"""Process-level wiring for the ``serve`` command.

spec §16.1 startup order (ConfigLoader.reload moved *before* AccountManager so
each account's update mode can be derived from its workflows at connect time —
account-update-mode change / design D3)::

    configure_logging → DB engine → Dispatcher → ConfigLoader.reload
    → derive update modes → AccountManager → Scheduler tasks
    (Dispatcher loop, EventsTtlCleaner) → API ready.

spec §16.2 / §16.3 shutdown:

    SIGTERM / SIGINT (or the lifespan ``__aexit__``) →
    stop Dispatcher (cooperative) → stop AccountWorkers → stop
    AccountManager → stop EventsTtlCleaner → dispose DB engine.

spec §16.4 long-running tasks (Dispatcher loop, AccountWorker consumer,
AccountManager watchers, EventsTtlCleaner) are all built to be
exception-resilient: each catches its own per-tick failures and logs,
keeping the loop alive. The lifespan only adds one extra safety net —
a *task supervisor* that re-spawns any long-running task that crashes
unexpectedly (bounded by a per-task failure window).

The result of ``build_application_state(settings)`` is a fully-wired
``FastAPI`` plus a :class:`LifespanState` holder; the CLI ``serve``
command hands it to ``uvicorn.run`` directly.
"""

from __future__ import annotations

import asyncio
import signal
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from tg_conductor import __version__
from tg_conductor.accounts import repo as account_repo
from tg_conductor.accounts.manager import AccountManager
from tg_conductor.accounts.models import Account
from tg_conductor.actions.context import AIClient
from tg_conductor.ai.client import OpenAIClient
from tg_conductor.ai.pricing import PricingTable
from tg_conductor.api.app import create_app
from tg_conductor.config.settings import Settings, get_settings
from tg_conductor.config_loader.reload import Reloader, install_sighup_handler
from tg_conductor.db.engine import create_engine
from tg_conductor.db.migrate import upgrade_head
from tg_conductor.logging import (
    configure_logging,
    get_logger,
    log_startup_summary,
)
from tg_conductor.runs.cleanup import EventsTtlCleaner
from tg_conductor.runs.event_bus import EventBus, InMemoryEventBus
from tg_conductor.scheduler.account_worker import AccountWorker
from tg_conductor.scheduler.control import spawn_startup_jobs
from tg_conductor.scheduler.daily_expander import DailyExpander
from tg_conductor.scheduler.dispatcher import Dispatcher
from tg_conductor.scheduler.expander import expand_daily
from tg_conductor.scheduler.hooks import make_cancel_pending_jobs_hook
from tg_conductor.scheduler.message_router import MessageRouter
from tg_conductor.tg_core.kurigram_adapter import KurigramAdapter
from tg_conductor.tg_core.protocol import TGClient
from tg_conductor.tg_core.throttle import AccountThrottle
from tg_conductor.triggers.startup import StartupTracker
from tg_conductor.workflows import repo as workflow_repo
from tg_conductor.workflows.update_mode import account_needs_updates

log = get_logger("tg_conductor.lifespan")


# ---------------------------------------------------------- state holder


@dataclass
class LifespanState:
    """Everything created in startup that shutdown needs to tear down.

    Held on ``app.state.lifespan`` to make adhoc tools / tests reach into
    the wiring without touching globals.
    """

    settings: Settings
    engine: AsyncEngine
    session_factory: async_sessionmaker[AsyncSession]
    event_bus: EventBus
    pricing: PricingTable
    ai_client: AIClient
    account_manager: AccountManager
    reloader: Reloader
    dispatcher: Dispatcher
    cleaner: EventsTtlCleaner
    daily_expander: DailyExpander
    workers: dict[int, AccountWorker] = field(default_factory=dict)
    # account_id -> needs_updates? — the update mode each live connection was
    # built with. Baseline for the reload flip-warning; not hot-mutated.
    account_update_modes: dict[int, bool] = field(default_factory=dict)
    supervised_tasks: list[asyncio.Task[None]] = field(default_factory=list)


# ---------------------------------------------------------- factory


def _default_client_factory(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    dispatcher: Dispatcher,
    owner_id: int,
    settings: Settings,
    needs_updates_by_account: dict[int, bool],
):
    """Return a ``client_factory`` closure for :class:`AccountManager`.

    Each Account becomes a :class:`KurigramAdapter` wired with the same
    process-wide throttle config. The account's *update mode* is looked up in
    ``needs_updates_by_account`` (derived from its workflows before connecting —
    see :func:`_startup`): **receiving** accounts build with updates enabled and
    get a :class:`MessageRouter` ``on_message`` handler; **send-only** accounts
    build with ``receive_updates=False`` (no ``GetChannelDifference`` storm) and
    register no router. Unknown account ids default to send-only.
    """

    def factory(account: Account) -> TGClient:
        assert account.id is not None
        needs_updates = needs_updates_by_account.get(account.id, False)
        plaintext_session = account_repo.decrypt_session(account)
        throttle = AccountThrottle(
            min_interval_seconds=settings.tg_min_interval_seconds,
            max_floodwait_retries=settings.tg_floodwait_max_retries,
            floodwait_padding_seconds=settings.tg_floodwait_padding_seconds,
        )
        adapter = KurigramAdapter(
            api_id=account.api_id,
            api_hash=account.api_hash,
            session_string=plaintext_session,
            proxy=account.proxy,
            env_proxy=settings.tg_proxy,
            account_label=account.label,
            throttle=throttle,
            receive_updates=needs_updates,
        )
        if needs_updates:
            router = MessageRouter(
                session_factory=session_factory,
                dispatcher=dispatcher,
                owner_id=owner_id,
                account_id=account.id,
            )
            adapter.on_message(router.on_message)
        log.info(
            "lifespan.account_update_mode",
            account_id=account.id,
            label=account.label,
            mode="receiving" if needs_updates else "send-only",
        )
        return adapter

    return factory


# ---------------------------------------------------------- update-mode helpers


async def _compute_update_modes(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    owner_id: int,
) -> dict[int, bool]:
    """Map every non-disabled account → whether it needs Telegram updates.

    Must run *after* workflows are loaded (see :func:`_startup` ordering) so the
    derivation can see each account's workflows.
    """
    modes: dict[int, bool] = {}
    async with session_factory() as session:
        accounts = await account_repo.list_for_owner(session, owner_id=owner_id)
        for acc in accounts:
            assert acc.id is not None
            modes[acc.id] = await account_needs_updates(
                session, owner_id=owner_id, account_id=acc.id
            )
    return modes


def _make_mode_flip_hook(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    owner_id: int,
    baseline: dict[int, bool],
):
    """Post-reload hook: WARN when an account's derived mode flips vs ``baseline``.

    Update mode is a construct-time parameter — the live connection keeps its
    original mode until restart. ``baseline`` reflects what each connection was
    built with and is intentionally *not* mutated, so a still-pending flip keeps
    warning on every reload until the operator restarts.
    """

    async def hook() -> None:
        fresh = await _compute_update_modes(session_factory, owner_id=owner_id)
        async with session_factory() as session:
            accounts = {
                acc.id: acc
                for acc in await account_repo.list_for_owner(
                    session, owner_id=owner_id
                )
            }
        for account_id, new_needs in fresh.items():
            current = baseline.get(account_id, False)
            if new_needs != current:
                acc = accounts.get(account_id)
                log.warning(
                    "lifespan.update_mode_flip",
                    account_id=account_id,
                    label=getattr(acc, "label", None),
                    from_mode="receiving" if current else "send-only",
                    to_mode="receiving" if new_needs else "send-only",
                    hint=(
                        "update mode is set at connect time; restart required "
                        "for the new mode to take effect"
                    ),
                )

    return hook


# ---------------------------------------------------------- supervised tasks


async def _supervise_task(
    *,
    name: str,
    factory: Any,
    failure_window_seconds: float = 60.0,
    max_failures_per_window: int = 5,
) -> None:
    """spec §16.4 — restart ``factory()`` on unexpected death.

    If the task crashes more than ``max_failures_per_window`` times
    within ``failure_window_seconds``, give up (log + return). The
    caller decides whether that translates to process exit.
    """
    failures: list[float] = []
    while True:
        try:
            await factory()
            return  # graceful completion (e.g. shutdown signaled)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - we're the safety net
            log.exception("supervised_task.crashed", name=name)
            now = asyncio.get_event_loop().time()
            failures = [t for t in failures if now - t < failure_window_seconds]
            failures.append(now)
            if len(failures) > max_failures_per_window:
                log.error(
                    "supervised_task.giving_up",
                    name=name,
                    failures=len(failures),
                    window_s=failure_window_seconds,
                )
                return
            backoff = min(2 ** len(failures), 30)
            log.warning(
                "supervised_task.restarting",
                name=name,
                backoff_seconds=backoff,
            )
            await asyncio.sleep(backoff)


# ---------------------------------------------------------- startup


async def _startup(settings: Settings) -> LifespanState:
    """Build every long-lived component in the order spec §16.1 demands."""
    # 1) DB
    engine = create_engine(settings.database_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    await asyncio.to_thread(upgrade_head, settings.database_url)
    log.info("lifespan.db_ready", database_url=str(settings.database_url))

    # 2) Process-wide singletons (event bus, pricing, AI client)
    event_bus = InMemoryEventBus()
    pricing = PricingTable.load_default()
    ai_client = OpenAIClient(
        session_factory=session_factory,
        pricing=pricing,
        settings=settings,
    )

    # 3) Dispatcher (constructs first so client_factory can wire MessageRouter)
    dispatcher = Dispatcher(
        session_factory=session_factory,
        owner_id=settings.default_owner_id,
        cron_tz=settings.scheduler_tzinfo,
    )

    # 4) ConfigLoader / Reloader — pulls yaml workflows in *before* accounts
    #    connect, so the update-mode derivation (step 5) can see each account's
    #    workflows. validate_workflow only needs the account *row* (created at
    #    login), not a live connection, so this has no reverse dependency on
    #    AccountManager (design D3).
    reloader = Reloader(
        session_factory=session_factory,
        workflow_dir=settings.workflow_dir,
        owner_id=settings.default_owner_id,
        on_deleted=make_cancel_pending_jobs_hook(owner_id=settings.default_owner_id),
    )
    if settings.workflow_dir.exists():
        await reloader.reload()
    else:
        log.warning("lifespan.workflow_dir_missing", path=str(settings.workflow_dir))

    # 5) Derive each account's update mode from its workflows (now in DB).
    account_update_modes = await _compute_update_modes(
        session_factory, owner_id=settings.default_owner_id
    )

    # 6) AccountManager — builds + connects every non-disabled Account; the
    #    factory builds send-only accounts with updates disabled (no router).
    account_manager = AccountManager(
        session_factory=session_factory,
        client_factory=_default_client_factory(
            session_factory=session_factory,
            dispatcher=dispatcher,
            owner_id=settings.default_owner_id,
            settings=settings,
            needs_updates_by_account=account_update_modes,
        ),
        owner_id=settings.default_owner_id,
        initial_reconnect_seconds=settings.tg_reconnect_initial_seconds,
        max_reconnect_seconds=settings.tg_reconnect_max_seconds,
    )
    await account_manager.start_all()
    log.info(
        "lifespan.accounts_ready",
        accounts_total=len(account_manager.active_account_ids()),
    )

    # Now that connections exist with a known mode, arm the reload flip-warning
    # (only fires on runtime reloads, not the startup reload above).
    reloader.on_post_reload(
        _make_mode_flip_hook(
            session_factory,
            owner_id=settings.default_owner_id,
            baseline=account_update_modes,
        )
    )

    # 7) Startup-trigger spawn + today's time_window catch-up
    tracker = StartupTracker()
    async with session_factory() as session, session.begin():
        await spawn_startup_jobs(
            session, owner_id=settings.default_owner_id, tracker=tracker
        )
        tz = settings.scheduler_tzinfo
        await expand_daily(
            session,
            owner_id=settings.default_owner_id,
            target_date=datetime.now(tz).date(),
            tz=tz,
        )

    # 8) AccountWorkers — one per account
    workers: dict[int, AccountWorker] = {}
    for aid in account_manager.active_account_ids():
        client = account_manager.get(aid)
        if client is None:
            continue
        worker = AccountWorker(
            owner_id=settings.default_owner_id,
            account_id=aid,
            queue=dispatcher.queue_for(aid),
            session_factory=session_factory,
            tg_client=client,
            ai_client=ai_client,
            event_bus=event_bus,
        )
        await worker.start()
        workers[aid] = worker

    # 9) Dispatcher loop + SIGHUP-triggered reload
    await dispatcher.start()
    install_sighup_handler(reloader)

    # 10) Background TTL cleaner for run_events
    cleaner = EventsTtlCleaner(
        session_factory=session_factory,
        ttl_days=settings.run_events_ttl_days,
        interval_seconds=float(settings.run_events_cleanup_interval_seconds),
    )
    await cleaner.start()

    # 10b) Daily plan-expander — re-expands time_window Workflows each day at
    #      ``scheduler_expand_at``. Step 7 only covered the startup day; without
    #      this every later day stays silent (spec scheduler §"每日 plan 展开").
    daily_expander = DailyExpander(
        session_factory=session_factory,
        owner_id=settings.default_owner_id,
        expand_at=settings.scheduler_expand_time,
        tz=settings.scheduler_tzinfo,
    )
    await daily_expander.start()

    # 11) Final startup summary
    workflows_total = 0
    async with session_factory() as session:
        accounts_total = len(
            await account_repo.list_for_owner(
                session, owner_id=settings.default_owner_id
            )
        )
        workflows_total = len(
            await workflow_repo.list_for_owner(
                session, owner_id=settings.default_owner_id
            )
        )
    if accounts_total == 0:
        # Common first-run footgun, esp. in containers where the service
        # starts before ``account login`` is run: workflows referencing an
        # account fail validation and silently never load. Make the remedy
        # loud and actionable.
        log.warning(
            "lifespan.no_accounts",
            hint=(
                "No Telegram account is connected. Run "
                "`tg-conductor account login --owner <id>` then restart the "
                "service — workflows referencing an account stay inactive "
                "until an account exists at startup."
            ),
        )

    log_startup_summary(
        settings=settings,
        version=__version__,
        accounts_total=accounts_total,
        workflows_total=workflows_total,
        workflow_dir=str(settings.workflow_dir),
    )

    return LifespanState(
        settings=settings,
        engine=engine,
        session_factory=session_factory,
        event_bus=event_bus,
        pricing=pricing,
        ai_client=ai_client,
        account_manager=account_manager,
        reloader=reloader,
        dispatcher=dispatcher,
        cleaner=cleaner,
        daily_expander=daily_expander,
        workers=workers,
        account_update_modes=account_update_modes,
    )


async def _shutdown(state: LifespanState) -> None:
    """spec §16.2 — drain in flight then tear down outward-in.

    Order: dispatcher (so no new jobs queue) → workers (drain in-flight
    Run) → AccountManager (close TG clients) → cleaner → daily_expander →
    engine.dispose.
    """
    log.info("lifespan.shutdown_begin")
    try:
        await state.dispatcher.stop()
    except Exception:  # noqa: BLE001
        log.exception("lifespan.dispatcher_stop_failed")

    for aid, worker in state.workers.items():
        try:
            await worker.stop()
        except Exception:  # noqa: BLE001
            log.exception("lifespan.worker_stop_failed", account_id=aid)

    try:
        await state.account_manager.stop_all()
    except Exception:  # noqa: BLE001
        log.exception("lifespan.account_manager_stop_failed")

    try:
        await state.cleaner.stop()
    except Exception:  # noqa: BLE001
        log.exception("lifespan.cleaner_stop_failed")

    try:
        await state.daily_expander.stop()
    except Exception:  # noqa: BLE001
        log.exception("lifespan.daily_expander_stop_failed")

    for task in state.supervised_tasks:
        if not task.done():
            task.cancel()
    if state.supervised_tasks:
        await asyncio.gather(*state.supervised_tasks, return_exceptions=True)

    try:
        await state.engine.dispose()
    except Exception:  # noqa: BLE001
        log.exception("lifespan.engine_dispose_failed")
    log.info("lifespan.shutdown_done")


# ---------------------------------------------------------- public lifespan


def make_lifespan(settings: Settings | None = None):
    """Return a FastAPI ``lifespan`` callable bound to ``settings``."""
    resolved_settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        configure_logging()
        state = await _startup(resolved_settings)
        app.state.lifespan = state
        app.state.session_factory = state.session_factory
        app.state.reloader = state.reloader
        app.state.event_bus = state.event_bus
        app.state.account_manager = state.account_manager
        try:
            yield
        finally:
            await _shutdown(state)

    return lifespan


def build_app(settings: Settings | None = None) -> FastAPI:
    """The single entry point used by ``serve`` and by tests doing e2e.

    Constructs a :class:`FastAPI` whose lifespan does the full §16 wiring.
    """
    resolved = settings or get_settings()
    app = create_app(
        session_factory=_LazySessionFactory(),  # replaced inside lifespan
        default_owner_id=resolved.default_owner_id,
        cors_origins=list(resolved.cors_origins),
    )
    app.router.lifespan_context = make_lifespan(resolved)
    return app


class _LazySessionFactory:
    """Placeholder session_factory used until lifespan binds the real one.

    ``create_app`` requires a session_factory at construction time, but
    the real one only exists after we've created the DB engine inside
    the lifespan. We swap it in via ``app.state.session_factory`` at
    startup; any code path that tries to use this placeholder before
    then raises a clear error.
    """

    def __call__(self) -> Any:
        raise RuntimeError(
            "session_factory not yet bound — lifespan startup hasn't run; "
            "if you see this in a test, build the app inside the lifespan "
            "context."
        )


# ---------------------------------------------------------- signal handling


def install_signal_handlers(stop_event: asyncio.Event) -> None:
    """Wire SIGTERM / SIGINT → set ``stop_event`` (spec §16.3).

    The serve loop awaits ``stop_event.wait()`` and exits gracefully,
    which propagates into the lifespan's ``__aexit__`` and runs
    :func:`_shutdown`.
    """
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except (NotImplementedError, RuntimeError):
            # Windows / non-main-thread → fall back to ``signal.signal``.
            signal.signal(sig, lambda *_: stop_event.set())
