"""§5.6 / §5.7 / §5.8 — AccountThrottle behaviour under concurrency."""

from __future__ import annotations

import asyncio

import pytest

from tg_conductor.tg_core.exceptions import TGFloodWait
from tg_conductor.tg_core.throttle import AccountThrottle

_MIN_INTERVAL = 0.05  # 50 ms — keeps the test fast but observable


@pytest.mark.asyncio
async def test_same_account_calls_are_serialised_with_min_interval() -> None:
    """3 concurrent calls on the same throttle run one-at-a-time, gaps ≥ min_interval."""
    throttle = AccountThrottle(min_interval_seconds=_MIN_INTERVAL)
    start_times: list[float] = []

    async def op() -> None:
        loop = asyncio.get_running_loop()
        start_times.append(loop.time())
        await asyncio.sleep(0)  # yield to scheduler so concurrency is real

    await asyncio.gather(
        throttle.call("op", op),
        throttle.call("op", op),
        throttle.call("op", op),
    )
    assert len(start_times) == 3
    gap1 = start_times[1] - start_times[0]
    gap2 = start_times[2] - start_times[1]
    # Allow a tiny scheduling fuzz; both gaps must respect min_interval.
    assert gap1 >= _MIN_INTERVAL - 0.005, f"gap1={gap1!r}"
    assert gap2 >= _MIN_INTERVAL - 0.005, f"gap2={gap2!r}"


@pytest.mark.asyncio
async def test_distinct_accounts_run_in_parallel() -> None:
    """Two throttles share no lock — total wall time ≈ one call, not two."""
    a = AccountThrottle(min_interval_seconds=_MIN_INTERVAL)
    b = AccountThrottle(min_interval_seconds=_MIN_INTERVAL)

    call_duration = 0.05

    async def slow_op() -> None:
        await asyncio.sleep(call_duration)

    loop = asyncio.get_running_loop()
    started = loop.time()
    await asyncio.gather(a.call("op", slow_op), b.call("op", slow_op))
    elapsed = loop.time() - started
    # Parallel execution: roughly one call_duration, definitely less than 2x.
    assert elapsed < call_duration * 1.8, f"elapsed={elapsed!r}"


@pytest.mark.asyncio
async def test_floodwait_retries_then_re_raises() -> None:
    """fn raises TGFloodWait every time — after max_retries we get the original."""
    throttle = AccountThrottle(
        min_interval_seconds=0.0,
        max_floodwait_retries=2,
        floodwait_padding_seconds=0.0,
    )
    call_count = 0

    async def always_flood() -> None:
        nonlocal call_count
        call_count += 1
        raise TGFloodWait(seconds=0.0, operation="boom")

    with pytest.raises(TGFloodWait):
        await throttle.call("boom", always_flood)
    # 1 initial + 2 retries = 3 invocations.
    assert call_count == 3


@pytest.mark.asyncio
async def test_floodwait_then_success() -> None:
    """fn fails once then succeeds — caller gets the success value, no exception."""
    throttle = AccountThrottle(
        min_interval_seconds=0.0,
        max_floodwait_retries=3,
        floodwait_padding_seconds=0.0,
    )
    attempts = 0

    async def flaky() -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise TGFloodWait(seconds=0.0, operation="flaky")
        return "ok"

    result = await throttle.call("flaky", flaky)
    assert result == "ok"
    assert attempts == 2
