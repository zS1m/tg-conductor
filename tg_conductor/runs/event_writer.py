"""Strictly-ordered DB-then-bus event writer.

CLAUDE.md invariant (Run 事件双写顺序):
    写 run_events 表 → DB commit → event_bus.publish(...)，顺序不可调换。

If the bus publish happens before commit, SSE clients can see an event id
that doesn't yet exist in DB — and reconnects with ``?since=<seq>`` will
get a 404. This writer enforces the order: commit then publish.

Sequence numbers are managed by the caller (see :class:`ActionContext`'s
``next_seq``). With one writer per Run that's race-free without DB-side
coordination.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tg_conductor.runs.event_bus import EventBus, run_topic
from tg_conductor.runs.models import RunEventRow


class EventWriter:
    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        event_bus: EventBus,
    ) -> None:
        self._session_factory = session_factory
        self._event_bus = event_bus

    async def write_event(
        self,
        *,
        owner_id: int,
        run_id: int,
        seq: int,
        event_type: str,
        message: str = "",
        level: str = "INFO",
        attrs: dict[str, Any] | None = None,
    ) -> RunEventRow:
        """Persist the event row, then (and only then) publish to subscribers.

        Field names mirror ``specs/runs/spec.md``: ``type``, ``message``,
        ``level``, ``attrs``. The Python parameter is ``event_type`` to
        avoid shadowing the builtin; the DB column and the published
        message both use ``type``.
        """
        ts = datetime.now(UTC)
        async with self._session_factory() as session, session.begin():
            row = RunEventRow(
                owner_id=owner_id,
                run_id=run_id,
                seq=seq,
                type=event_type,
                message=message,
                level=level,
                attrs=attrs,
                ts=ts,
            )
            session.add(row)
            await session.flush()
        # Commit has happened. row.id / row.ts stay accessible because the
        # session_factory uses ``expire_on_commit=False``.
        self._event_bus.publish(
            run_topic(run_id),
            {
                "id": row.id,
                "owner_id": owner_id,
                "run_id": run_id,
                "seq": seq,
                "type": event_type,
                "message": message,
                "level": level,
                "attrs": attrs,
                "ts": ts.isoformat(),
            },
        )
        return row
