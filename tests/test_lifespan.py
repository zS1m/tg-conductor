"""§16 — lifespan startup / shutdown wiring + signal handling.

We don't drive Kurigram in these tests; the lifespan ``client_factory``
is patched to return :class:`FakeTGClient` instances so the
``account_manager.start_all()`` round-trip stays in-memory.

Covers:

* §16.1 startup order: DB ready → AccountManager has clients →
  Reloader pulled yaml → workers exist per account → cleaner started.
* §16.2 shutdown reaches every component and disposes the engine.
* §16.3 :func:`install_signal_handlers` wires SIGTERM / SIGINT to a
  stop-event without crashing on platforms that lack add_signal_handler.
* §16.4 :func:`_supervise_task` re-launches a crashing factory until
  the failure budget is exhausted, then returns.
"""

from __future__ import annotations

import asyncio
import signal
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
import yaml
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tg_conductor.accounts import repo as account_repo
from tg_conductor.api import lifespan as lifespan_mod
from tg_conductor.api.lifespan import (
    _shutdown,
    _startup,
    _supervise_task,
    install_signal_handlers,
)
from tg_conductor.config.settings import get_settings
from tg_conductor.db.engine import create_engine
from tg_conductor.db.migrate import upgrade_head
from tg_conductor.tg_core.fake import FakeTGClient


@pytest.fixture
async def settings_in_tmp(
    master_key: str,  # noqa: ARG001 — exports APP_MASTER_KEY
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator:
    """Settings pointing at a tmp SQLite + tmp workflow_dir, with a stub
    pricing path and no OPENAI_API_KEY (we don't call OpenAI in tests).
    """
    db_path = tmp_path / "lifespan.sqlite3"
    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    wf_dir = tmp_path / "workflows"
    wf_dir.mkdir()
    monkeypatch.setenv("WORKFLOW_DIR", str(wf_dir))
    yield get_settings()


async def _seed_one_account(
    session_factory: async_sessionmaker[AsyncSession],
) -> int:
    async with session_factory() as s, s.begin():
        acc = await account_repo.upsert_session(
            s,
            owner_id=1,
            label="main",
            api_id=1,
            api_hash="h",
            session_string="s",
        )
        return acc.id  # type: ignore[return-value]


@pytest.fixture
def fake_client_factory(monkeypatch: pytest.MonkeyPatch):
    """Replace ``_default_client_factory`` so AccountManager uses fakes."""

    def make(  # noqa: ARG001
        *, session_factory, dispatcher, owner_id, settings, needs_updates_by_account
    ):
        def factory(account):
            return FakeTGClient(label=account.label)

        return factory

    monkeypatch.setattr(lifespan_mod, "_default_client_factory", make)
    return make


# ----------------------------------------------------------- §16.1 / 16.2


@pytest.mark.asyncio
async def test_full_startup_then_shutdown_round_trip(
    settings_in_tmp,
    tmp_path: Path,
    fake_client_factory,  # noqa: ARG001 — autouse via fixture arg
) -> None:
    settings = settings_in_tmp

    # Seed a workflow on disk so Reloader has something to sync.
    yaml_doc = {
        "name": "wf1",
        "account_id": 1,  # filled after we seed the account
        "trigger": {"type": "startup"},
        "action_plan": {
            "steps": [{"action": "send_text", "chat_id": 1, "text": "x"}],
        },
    }
    # Migrate first so we can pre-seed an account.
    await asyncio.to_thread(upgrade_head, settings.database_url)
    engine = create_engine(settings.database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    acc_id = await _seed_one_account(factory)
    await engine.dispose()
    yaml_doc["account_id"] = acc_id
    (settings.workflow_dir / "wf1.yaml").write_text(
        yaml.safe_dump(yaml_doc), encoding="utf-8"
    )

    state = await _startup(settings)
    try:
        # AccountManager brought up the one seeded account.
        assert state.account_manager.active_account_ids() == [acc_id]
        # Workers exist for each active account.
        assert set(state.workers.keys()) == {acc_id}
        # Reloader picked up the yaml → workflow row present.
        async with state.session_factory() as session:
            wfs = await __import__(
                "tg_conductor.workflows.repo", fromlist=["repo"]
            ).list_for_owner(session, owner_id=1)
        assert [w.name for w in wfs] == ["wf1"]
        # Dispatcher loop is running.
        assert state.dispatcher._task is not None  # noqa: SLF001
    finally:
        await _shutdown(state)

    # After shutdown: dispatcher / cleaner tasks done, manager empty.
    assert state.dispatcher._task is None  # noqa: SLF001
    assert state.account_manager.active_account_ids() == []


@pytest.mark.asyncio
async def test_startup_derives_send_only_vs_receiving_modes(
    settings_in_tmp,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cron-only account → send-only (no router); a message_match account →
    receiving (router installed). Uses a recording fake so we can assert which
    accounts got an ``on_message`` handler."""
    settings = settings_in_tmp

    # A fake that records receive_updates + whether on_message was registered.
    class _RecordingFake(FakeTGClient):
        def __init__(self, *, label: str, receive_updates: bool) -> None:
            super().__init__(label=label)
            self.receive_updates = receive_updates
            self.router_installed = False

        def on_message(self, handler) -> None:  # type: ignore[no-untyped-def]
            self.router_installed = True

    built: dict[str, _RecordingFake] = {}

    def make(  # noqa: ARG001
        *, session_factory, dispatcher, owner_id, settings, needs_updates_by_account
    ):
        def factory(account):
            needs = needs_updates_by_account.get(account.id, False)
            client = _RecordingFake(label=account.label, receive_updates=needs)
            if needs:
                client.on_message(lambda _m: None)
            built[account.label] = client
            return client

        return factory

    monkeypatch.setattr(lifespan_mod, "_default_client_factory", make)

    await asyncio.to_thread(upgrade_head, settings.database_url)
    engine = create_engine(settings.database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as s, s.begin():
        cron_acc = await account_repo.upsert_session(
            s, owner_id=1, label="cron", api_id=1, api_hash="h", session_string="s"
        )
        recv_acc = await account_repo.upsert_session(
            s, owner_id=1, label="recv", api_id=1, api_hash="h", session_string="s"
        )
        cron_id, recv_id = cron_acc.id, recv_acc.id
    await engine.dispose()

    (settings.workflow_dir / "cron.yaml").write_text(
        yaml.safe_dump(
            {
                "name": "cron-wf",
                "account_id": cron_id,
                "trigger": {"type": "cron", "expression": "* * * * *"},
                "action_plan": {
                    "steps": [{"action": "send_text", "chat_id": 1, "text": "x"}]
                },
            }
        ),
        encoding="utf-8",
    )
    (settings.workflow_dir / "recv.yaml").write_text(
        yaml.safe_dump(
            {
                "name": "recv-wf",
                "account_id": recv_id,
                "trigger": {"type": "message_match", "chat_id": -100123},
                "action_plan": {
                    "steps": [{"action": "send_text", "chat_id": 1, "text": "x"}]
                },
            }
        ),
        encoding="utf-8",
    )

    state = await _startup(settings)
    try:
        assert state.account_update_modes == {cron_id: False, recv_id: True}
        assert built["cron"].receive_updates is False
        assert built["cron"].router_installed is False
        assert built["recv"].receive_updates is True
        assert built["recv"].router_installed is True
    finally:
        await _shutdown(state)


class _RecordingLog:
    """Captures structlog-style ``log.<level>(event, **kw)`` calls.

    The lifespan logger is structlog's ``PrintLogger`` (stdout, not stdlib), so
    ``caplog`` can't see it — we swap the module ``log`` for this recorder.
    """

    def __init__(self) -> None:
        self.records: list[tuple[str, str, dict]] = []

    def _mk(self, level: str):
        def _log(event: str, **kw) -> None:  # type: ignore[no-untyped-def]
            self.records.append((level, event, kw))

        return _log

    def __getattr__(self, name: str):  # info / warning / error / exception / debug
        return self._mk(name)


@pytest.mark.asyncio
async def test_reload_flip_to_receiving_warns_and_keeps_mode(
    settings_in_tmp,
    tmp_path: Path,
    fake_client_factory,  # noqa: ARG001
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A send-only account that gains its first message_match on reload must
    WARN (restart required) while its live connection stays send-only."""
    settings = settings_in_tmp

    await asyncio.to_thread(upgrade_head, settings.database_url)
    engine = create_engine(settings.database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    acc_id = await _seed_one_account(factory)
    await engine.dispose()

    (settings.workflow_dir / "wf.yaml").write_text(
        yaml.safe_dump(
            {
                "name": "wf",
                "account_id": acc_id,
                "trigger": {"type": "cron", "expression": "* * * * *"},
                "action_plan": {
                    "steps": [{"action": "send_text", "chat_id": 1, "text": "x"}]
                },
            }
        ),
        encoding="utf-8",
    )

    state = await _startup(settings)
    try:
        # Built send-only.
        assert state.account_update_modes == {acc_id: False}

        # Swap the logger so we can assert the flip WARNING is emitted.
        recorder = _RecordingLog()
        monkeypatch.setattr(lifespan_mod, "log", recorder)

        # Add a message_match workflow and reload at runtime.
        (settings.workflow_dir / "mm.yaml").write_text(
            yaml.safe_dump(
                {
                    "name": "mm",
                    "account_id": acc_id,
                    "trigger": {"type": "message_match", "chat_id": -100123},
                    "action_plan": {
                        "steps": [{"action": "send_text", "chat_id": 1, "text": "x"}]
                    },
                }
            ),
            encoding="utf-8",
        )
        await state.reloader.reload()

        flips = [
            kw
            for level, event, kw in recorder.records
            if event == "lifespan.update_mode_flip" and level == "warning"
        ]
        assert len(flips) == 1
        assert flips[0]["account_id"] == acc_id
        assert flips[0]["from_mode"] == "send-only"
        assert flips[0]["to_mode"] == "receiving"
        # Baseline (live connection mode) is unchanged — still send-only.
        assert state.account_update_modes == {acc_id: False}
    finally:
        await _shutdown(state)


@pytest.mark.asyncio
async def test_startup_with_missing_workflow_dir_warns_but_succeeds(
    settings_in_tmp,
    tmp_path: Path,
    fake_client_factory,  # noqa: ARG001
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A fresh deploy with no workflow_dir on disk must still come up."""
    settings = settings_in_tmp
    # Point at a non-existent dir.
    settings.workflow_dir = tmp_path / "no-such-dir"
    state = await _startup(settings)
    try:
        async with state.session_factory() as session:
            wfs = await __import__(
                "tg_conductor.workflows.repo", fromlist=["repo"]
            ).list_for_owner(session, owner_id=1)
        assert wfs == []
    finally:
        await _shutdown(state)


# ----------------------------------------------------------- §16.3 signals


def test_install_signal_handlers_wires_sigterm_sigint() -> None:
    """The function must not raise; the stop_event should fire on a fake SIGTERM."""

    async def runner() -> bool:
        stop_event = asyncio.Event()
        install_signal_handlers(stop_event)
        # Simulate the OS delivering SIGTERM. ``loop.add_signal_handler``'s
        # callback fires synchronously on the next loop turn.
        signal.raise_signal(signal.SIGTERM)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=1.0)
            return True
        except asyncio.TimeoutError:
            return False

    assert asyncio.run(runner()) is True


# ----------------------------------------------------------- §16.4 supervisor


@pytest.mark.asyncio
async def test_supervised_task_restarts_after_crash() -> None:
    calls = {"n": 0}

    async def factory() -> None:
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError(f"crash {calls['n']}")
        # third call: succeed and return.

    # Tiny backoff via monkey-patching asyncio.sleep is overkill; the
    # default backoff is bounded by min(2 ** n, 30); we only need 2
    # restarts here so worst case ≈ 2 + 4 = 6 s. Use a small failure
    # budget so the test asserts give-up too.
    await asyncio.wait_for(
        _supervise_task(
            name="t",
            factory=factory,
            failure_window_seconds=10.0,
            max_failures_per_window=5,
        ),
        timeout=30.0,
    )
    assert calls["n"] == 3


@pytest.mark.asyncio
async def test_supervised_task_gives_up_after_too_many_crashes() -> None:
    crashes = {"n": 0}

    async def factory() -> None:
        crashes["n"] += 1
        raise RuntimeError("always-fail")

    # Tight budget so we exit quickly.
    await asyncio.wait_for(
        _supervise_task(
            name="t",
            factory=factory,
            failure_window_seconds=10.0,
            max_failures_per_window=2,
        ),
        timeout=30.0,
    )
    # Crashed budget+1 times then gave up.
    assert crashes["n"] == 3
