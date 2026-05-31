"""Cron next-fire-time calculation.

Cron syntax validity is enforced at ``Workflow`` construction time (see
``workflows.schema.CronTrigger``); :func:`next_fire_at` assumes ``expression``
is already valid.
"""

from __future__ import annotations

from datetime import UTC, datetime, tzinfo

from croniter import croniter


def next_fire_at(expression: str, after: datetime, *, tz: tzinfo = UTC) -> datetime:
    """First ``datetime`` strictly after ``after`` matching ``expression``.

    The expression is evaluated against wall-clock time in ``tz`` — so
    ``"0 9 * * *"`` with ``tz=ZoneInfo("Asia/Shanghai")`` means 09:00 北京
    时间, not 09:00 UTC. The returned datetime is always converted back to
    UTC for storage in the (UTC-based) jobs table. ``after`` may be naive
    (assumed UTC) or tz-aware.
    """
    if after.tzinfo is None:
        after = after.replace(tzinfo=UTC)
    local = after.astimezone(tz)
    it = croniter(expression, local)
    nxt: datetime = it.get_next(datetime)
    return nxt.astimezone(UTC)
