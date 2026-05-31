"""Random expansion of ``time_window`` workflows into per-day fire_at lists.

Algorithm — constructive sampling instead of rejection (spec says "1000 次
尝试"; the same outcome — drop to a feasible count + WARNING — is reached
deterministically in O(n log n) here, which keeps tests reproducible
under ``freezegun`` + a seeded ``random.Random``).

1. Resolve window start / end to ``datetime`` anchored on ``target_date``.
2. Effective start = ``max(window_start, now)`` so a workflow enabled
   mid-window only schedules into the remaining time (spec §9.7).
3. Sample N from ``count`` (CountRange → random in [min, max]).
4. Feasibility: window_seconds < (N-1) × min_gap_seconds → reduce N to
   the largest value that fits + WARNING.
5. Place N points by drawing N uniform offsets from ``[0, free_space]``
   where ``free_space = window_seconds − (N-1) × min_gap_seconds``;
   sort, then accumulate ``i × min_gap_seconds``. Result: N timestamps
   with consecutive gaps ≥ ``min_gap`` and otherwise uniformly random
   in the window.
"""

from __future__ import annotations

import logging
import random
from datetime import UTC, date, datetime, timedelta, tzinfo

from tg_conductor.triggers.duration import parse_duration
from tg_conductor.workflows.schema import (
    CountRange,
    TimeWindowTrigger,
    parse_window,
)

log = logging.getLogger(__name__)


def expand(
    trigger: TimeWindowTrigger,
    target_date: date,
    *,
    now: datetime | None = None,
    rng: random.Random | None = None,
    tz: tzinfo = UTC,
) -> list[datetime]:
    """Generate ``fire_at`` datetimes for one day.

    Empty list means "nothing to fire today" — either ``now`` is past the
    window or feasibility reduced N to zero.
    """
    if rng is None:
        rng = random.Random()

    (sh, sm), (eh, em) = parse_window(trigger.window)
    window_start = datetime(
        target_date.year, target_date.month, target_date.day, sh, sm, tzinfo=tz
    )
    window_end = datetime(
        target_date.year, target_date.month, target_date.day, eh, em, tzinfo=tz
    )

    eff_start = window_start
    if now is not None:
        if now >= window_end:
            return []
        if now > window_start:
            eff_start = now

    window_seconds = (window_end - eff_start).total_seconds()
    if window_seconds <= 0:
        return []

    n = _sample_count(trigger.count, rng)
    if n <= 0:
        return []

    min_gap_seconds = parse_duration(trigger.min_gap)
    required_total_gap = (n - 1) * min_gap_seconds

    if window_seconds < required_total_gap:
        feasible_n = int(window_seconds // min_gap_seconds) + 1
        feasible_n = max(0, feasible_n)
        log.warning(
            "time_window infeasible: requested %d in %ds window with min_gap %.0fs; "
            "reducing to %d",
            n,
            int(window_seconds),
            min_gap_seconds,
            feasible_n,
        )
        n = feasible_n
        if n == 0:
            return []
        required_total_gap = (n - 1) * min_gap_seconds

    free_space = window_seconds - required_total_gap
    offsets = sorted(rng.uniform(0.0, free_space) for _ in range(n))
    return [
        eff_start + timedelta(seconds=offsets[i] + i * min_gap_seconds)
        for i in range(n)
    ]


def _sample_count(count: int | CountRange, rng: random.Random) -> int:
    if isinstance(count, int):
        return count
    return rng.randint(count.min, count.max)
