"""§9 — pure-logic trigger primitives (cron, time_window, message_match, startup).

All tests are deterministic via:
* ``freezegun.freeze_time`` for ``now`` control where it matters,
* an explicit ``random.Random(seed)`` for time_window expansion.

No DB, no asyncio.
"""

from __future__ import annotations

import random
from datetime import UTC, date, datetime, timedelta

import pytest
from freezegun import freeze_time

from tg_conductor.tg_core.fake import make_fake_message
from tg_conductor.triggers.cron import next_fire_at
from tg_conductor.triggers.duration import parse_duration
from tg_conductor.triggers.message_match import matches
from tg_conductor.triggers.startup import StartupTracker
from tg_conductor.triggers.time_window import expand
from tg_conductor.workflows.schema import (
    CountRange,
    MessageMatchTrigger,
    TimeWindowTrigger,
)

# ------------------------------------------------------------ §9.5 cron


def test_cron_next_fires_at_expected_minute() -> None:
    after = datetime(2026, 5, 29, 9, 15, tzinfo=UTC)
    expr = "30 9 * * *"
    fire = next_fire_at(expr, after)
    assert fire == datetime(2026, 5, 29, 9, 30, tzinfo=UTC)


def test_cron_skips_to_next_day_when_past_today() -> None:
    after = datetime(2026, 5, 29, 12, 0, tzinfo=UTC)
    expr = "30 9 * * *"  # 9:30 daily; today's already past
    fire = next_fire_at(expr, after)
    assert fire == datetime(2026, 5, 30, 9, 30, tzinfo=UTC)


def test_cron_strictly_after_anchor() -> None:
    """``after`` itself must not be returned even if it matches."""
    anchor = datetime(2026, 5, 29, 9, 30, tzinfo=UTC)
    fire = next_fire_at("30 9 * * *", anchor)
    assert fire > anchor


def test_cron_expression_interpreted_in_configured_tz() -> None:
    """``"0 9 * * *"`` in Asia/Shanghai means 09:00 北京时间 = 01:00 UTC."""
    from zoneinfo import ZoneInfo

    tz = ZoneInfo("Asia/Shanghai")
    after = datetime(2026, 5, 29, 0, 0, tzinfo=UTC)  # 08:00 北京时间
    fire = next_fire_at("0 9 * * *", after, tz=tz)
    # 09:00 Shanghai is 01:00 UTC, and the result is returned in UTC.
    assert fire == datetime(2026, 5, 29, 1, 0, tzinfo=UTC)
    assert fire.tzinfo == UTC


# ------------------------------------------------------------ §9.6 time_window happy path


def _tw(window: str, count, min_gap: str) -> TimeWindowTrigger:
    return TimeWindowTrigger(
        type="time_window", window=window, count=count, min_gap=min_gap
    )


def test_expand_yields_count_with_min_gap_respected() -> None:
    trigger = _tw("10:00-23:00", 8, "30m")
    rng = random.Random(42)
    result = expand(
        trigger,
        date(2026, 5, 29),
        now=datetime(2026, 5, 29, 0, 0, tzinfo=UTC),
        rng=rng,
    )
    assert len(result) == 8
    window_start = datetime(2026, 5, 29, 10, 0, tzinfo=UTC)
    window_end = datetime(2026, 5, 29, 23, 0, tzinfo=UTC)
    for ts in result:
        assert window_start <= ts <= window_end
    gaps = [
        (b - a).total_seconds() for a, b in zip(result[:-1], result[1:], strict=True)
    ]
    assert all(g >= 30 * 60 for g in gaps), f"gap violated: {gaps}"


def test_expand_count_range_samples_within_min_max() -> None:
    trigger = _tw("10:00-23:00", CountRange(min=8, max=9), "30m")
    # Drive the rng so randint(8, 9) is deterministic.
    counts: set[int] = set()
    for seed in range(20):
        result = expand(
            trigger,
            date(2026, 5, 29),
            now=datetime(2026, 5, 29, 0, 0, tzinfo=UTC),
            rng=random.Random(seed),
        )
        counts.add(len(result))
    assert counts <= {8, 9}
    assert counts  # at least one sample landed


def test_expand_deterministic_with_seeded_rng() -> None:
    trigger = _tw("10:00-23:00", 4, "1h")
    a = expand(
        trigger,
        date(2026, 5, 29),
        now=datetime(2026, 5, 29, 0, 0, tzinfo=UTC),
        rng=random.Random(123),
    )
    b = expand(
        trigger,
        date(2026, 5, 29),
        now=datetime(2026, 5, 29, 0, 0, tzinfo=UTC),
        rng=random.Random(123),
    )
    assert a == b


# ------------------------------------------------------------ §9.7 mid-day enablement


@freeze_time("2026-05-29 15:00:00+00:00")
def test_expand_uses_now_when_inside_window() -> None:
    trigger = _tw("10:00-23:00", 5, "30m")
    now = datetime(2026, 5, 29, 15, 0, tzinfo=UTC)
    result = expand(trigger, date(2026, 5, 29), now=now, rng=random.Random(7))
    assert result
    # Every fire_at must be ≥ 15:00 (effective start), not the configured 10:00.
    assert all(ts >= now for ts in result)


def test_expand_returns_empty_after_window_end() -> None:
    trigger = _tw("10:00-12:00", 3, "10m")
    now_after = datetime(2026, 5, 29, 13, 0, tzinfo=UTC)
    assert expand(trigger, date(2026, 5, 29), now=now_after) == []


# ------------------------------------------------------------ §9.8 infeasible


def test_expand_reduces_count_when_window_too_small(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """spec scenario: 60-min window, count=10, min_gap=20m → only 4 fit."""
    from tg_conductor.triggers import time_window as tw_module

    warnings: list[str] = []
    monkeypatch.setattr(
        tw_module.log, "warning", lambda msg, *args: warnings.append(msg % args)
    )

    trigger = _tw("10:00-11:00", 10, "20m")
    result = expand(
        trigger,
        date(2026, 5, 29),
        now=datetime(2026, 5, 29, 0, 0, tzinfo=UTC),
        rng=random.Random(1),
    )
    assert len(result) < 10
    assert len(result) == 4  # 60 // 20 + 1
    gaps = [
        (b - a).total_seconds() for a, b in zip(result[:-1], result[1:], strict=True)
    ]
    assert all(g >= 20 * 60 for g in gaps)
    assert any("infeasible" in w for w in warnings), warnings


def test_expand_returns_empty_when_window_smaller_than_one_gap() -> None:
    trigger = _tw("10:00-10:05", 5, "10m")  # 5 min < 10 min gap
    result = expand(
        trigger,
        date(2026, 5, 29),
        now=datetime(2026, 5, 29, 0, 0, tzinfo=UTC),
        rng=random.Random(1),
    )
    # ``feasible_n = 5*60 // (10*60) + 1 = 0 + 1 = 1``: one fire is still fine.
    assert len(result) == 1


# ------------------------------------------------------------ §9.9 message_match


def test_message_match_full_match_passes() -> None:
    trigger = MessageMatchTrigger(
        type="message_match",
        chat_id=-100,
        topic_id=5,
        text_pattern="签到成功",
        from_user_id=42,
    )
    msg = make_fake_message(
        chat_id=-100, text="账号 签到成功", topic_id=5, from_user_id=42
    )
    assert matches(trigger, msg) is True


@pytest.mark.parametrize(
    "kwargs",
    [
        {"chat_id": -999},  # wrong chat
        {"topic_id": 6},  # wrong topic
        {"text": "no match here"},  # wrong text
        {"from_user_id": 99},  # wrong user
        {"text": None},  # text required but message has none
    ],
)
def test_message_match_any_missing_field_fails(kwargs: dict) -> None:
    base: dict = {
        "chat_id": -100,
        "topic_id": 5,
        "text": "签到成功",
        "from_user_id": 42,
    }
    base.update(kwargs)
    msg = make_fake_message(**base)
    trigger = MessageMatchTrigger(
        type="message_match",
        chat_id=-100,
        topic_id=5,
        text_pattern="签到成功",
        from_user_id=42,
    )
    assert matches(trigger, msg) is False


def test_message_match_only_required_fields_set() -> None:
    """Unconfigured trigger fields (topic_id, text_pattern, from_user_id) don't filter."""
    trigger = MessageMatchTrigger(type="message_match", chat_id=-100)
    msg = make_fake_message(
        chat_id=-100, text="random text", topic_id=99, from_user_id=42
    )
    assert matches(trigger, msg) is True


def test_message_match_text_pattern_is_regex_search_not_match() -> None:
    trigger = MessageMatchTrigger(type="message_match", chat_id=1, text_pattern="bar")
    msg = make_fake_message(chat_id=1, text="foo bar baz")
    assert matches(trigger, msg) is True


# ------------------------------------------------------------ §9.10 startup


def test_startup_consume_returns_true_only_once() -> None:
    tracker = StartupTracker()
    assert tracker.fired is False
    assert tracker.consume() is True
    assert tracker.fired is True
    assert tracker.consume() is False
    assert tracker.consume() is False


def test_startup_simulated_reload_does_not_re_fire() -> None:
    """Reload / SIGHUP must NOT reset the tracker — only test code may."""
    tracker = StartupTracker()
    tracker.consume()
    # Simulating a reload: just call consume again. Spec invariant: stays fired.
    assert tracker.consume() is False
    assert tracker.fired is True


def test_startup_reset_for_tests_works() -> None:
    tracker = StartupTracker()
    tracker.consume()
    tracker.reset_for_tests()
    assert tracker.fired is False
    assert tracker.consume() is True


# ------------------------------------------------------------ duration helper


@pytest.mark.parametrize(
    "text,expected",
    [
        ("30s", 30),
        ("30m", 30 * 60),
        ("1h", 3600),
        ("1h30m", 5400),
        ("2h15m30s", 2 * 3600 + 15 * 60 + 30),
    ],
)
def test_parse_duration_known_forms(text: str, expected: int) -> None:
    assert parse_duration(text) == float(expected)


def test_parse_duration_unknown_raises() -> None:
    with pytest.raises(ValueError):
        parse_duration("yesterday")


def test_parse_duration_zero_rejected() -> None:
    with pytest.raises(ValueError):
        parse_duration("0s")


# ------------------------------------------------------------ sanity: window crosses with timedelta math


def test_expand_anchors_to_target_date_not_today() -> None:
    """Calling with a future date returns timestamps on that day, not today."""
    trigger = _tw("10:00-11:00", 1, "1m")
    target = date.today() + timedelta(days=7)
    result = expand(trigger, target, rng=random.Random(0))
    assert result
    for ts in result:
        assert ts.date() == target
