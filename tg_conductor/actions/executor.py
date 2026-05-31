"""ActionPlan executor — variants picking, per-step timeout, inter-step delay.

Run lifecycle (orchestrated by :class:`AccountWorker`, not here):

1. Worker creates Run row + ``ActionContext`` for the Job.
2. Worker calls :meth:`ActionPlanExecutor.execute(ctx)`.
3. Executor:
   a. Selects steps (variants → ``bump_rr_counter`` / ``random.choice``
      from ``plan.variants``, or just ``plan.steps``).
   b. For each step:
      * emits no "started" event (the action itself emits its own
        ``action.<type>`` event on success);
      * wraps the action in ``asyncio.wait_for(timeout=resolved_timeout(step))``;
      * on ``asyncio.TimeoutError`` → emits ``action.failed`` with
        ``error="step_timeout"``;
      * on any other Exception → emits ``action.failed`` with the
        exception's message;
      * if step failed AND not ``continue_on_error`` → break out of the loop.
   c. After a successful ``wait_for`` step, executor stashes the matched
      message into ``ctx.extras[f"wait_for_step_{idx}"]`` so later steps
      can reference it via ``forward.source`` / ``click_button.target``.
   d. Between steps (not after the last), sleeps a uniform
      ``[inter_step_delay.min, inter_step_delay.max]`` jitter.
4. Worker wraps the whole ``executor.execute`` call in
   ``asyncio.wait_for(timeout=plan.job_timeout or settings.job_default_timeout_seconds)``;
   ``asyncio.TimeoutError`` becomes ``ExecutionResult(success=False, error="job_timeout")``.
"""

from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from tg_conductor.actions import (
    ai_reply,
    click_button,
    forward,
    send_dice,
    send_text,
    wait_for,
)
from tg_conductor.actions.context import ActionContext
from tg_conductor.workflows import repo as workflow_repo
from tg_conductor.workflows.schema import (
    ActionPlan,
    ActionStep,
    Variant,
    WaitForStep,
    _resolve_delay,  # noqa: PLC2701 - schema is the canonical source
    _resolved_timeout,  # noqa: PLC2701 - schema is the canonical source
)

log = logging.getLogger(__name__)


ActionExecutor = Callable[[Any, ActionContext], Awaitable[None]]


_DISPATCH: dict[str, ActionExecutor] = {
    "send_text": send_text.execute,
    "send_dice": send_dice.execute,
    "forward": forward.execute,
    "click_button": click_button.execute,
    "wait_for": wait_for.execute,
    "ai_reply": ai_reply.execute,
}


@dataclass
class ExecutionResult:
    success: bool
    error: str | None = None
    step_errors: list[tuple[int, str]] = field(default_factory=list)


class ActionPlanExecutor:
    def __init__(
        self,
        plan: ActionPlan,
        *,
        rng: random.Random | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        dispatch: dict[str, ActionExecutor] | None = None,
    ) -> None:
        self._plan = plan
        self._rng = rng or random.Random()
        self._sleep = sleep
        self._dispatch = dispatch or _DISPATCH

    async def execute(self, ctx: ActionContext) -> ExecutionResult:
        steps, variant_label = await self._select_steps(ctx)
        if variant_label is not None:
            ctx.extras["variant_id"] = variant_label

        result = ExecutionResult(success=True)
        for idx, step in enumerate(steps):
            ctx.step_index = idx

            # Pre-step delay runs *outside* the per-step asyncio.wait_for, so a
            # long delay_before is never killed by the step's own timeout. It
            # still counts toward the worker's job_timeout (which wraps this
            # whole loop) — schema validation guarantees job_timeout covers it.
            delay = _resolve_delay(step.delay_before, self._rng)
            if delay > 0:
                await self._sleep(delay)
                await ctx.emit(
                    "action.delay",
                    {"step_index": idx, "action": step.action, "seconds": delay},
                    message=f"delayed {delay:.3g}s before step {idx} ({step.action})",
                )

            timeout = _resolved_timeout(step)
            try:
                await asyncio.wait_for(self._run_step(step, ctx), timeout=timeout)
            except asyncio.TimeoutError:
                await ctx.emit(
                    "action.failed",
                    {
                        "step_index": idx,
                        "action": step.action,
                        "error": "step_timeout",
                        "timeout": timeout,
                    },
                    message=f"step {idx} ({step.action}) timed out after {timeout}s",
                    level="ERROR",
                )
                result.step_errors.append((idx, "step_timeout"))
                if not step.continue_on_error:
                    result.success = False
                    result.error = f"step {idx} ({step.action}) timed out"
                    break
            except Exception as exc:  # noqa: BLE001 - executor's job to translate
                err = f"{type(exc).__name__}: {exc}"
                await ctx.emit(
                    "action.failed",
                    {
                        "step_index": idx,
                        "action": step.action,
                        "error": err,
                    },
                    message=f"step {idx} ({step.action}) failed: {err}",
                    level="ERROR",
                )
                result.step_errors.append((idx, err))
                if not step.continue_on_error:
                    result.success = False
                    result.error = f"step {idx} ({step.action}) failed: {err}"
                    break
            else:
                # On success of a wait_for step, expose the matched message
                # under the named reference so later steps can use
                # ``forward.source="wait_for_step_<idx>"`` etc.
                if isinstance(step, WaitForStep) and ctx.last_matched_message:
                    ctx.extras[f"wait_for_step_{idx}"] = ctx.last_matched_message

            if idx < len(steps) - 1 and self._plan.inter_step_delay is not None:
                delay = self._rng.uniform(
                    self._plan.inter_step_delay.min,
                    self._plan.inter_step_delay.max,
                )
                await self._sleep(delay)

        return result

    async def _run_step(self, step: ActionStep, ctx: ActionContext) -> None:
        executor = self._dispatch.get(step.action)
        if executor is None:
            raise RuntimeError(f"no executor for action {step.action!r}")
        await executor(step, ctx)

    async def _select_steps(
        self, ctx: ActionContext
    ) -> tuple[list[ActionStep], str | None]:
        if not self._plan.variants:
            return list(self._plan.steps), None

        variants = self._plan.variants
        if self._plan.pick_variant == "round_robin":
            async with ctx.session_factory() as session, session.begin():
                counter = await workflow_repo.bump_rr_counter(
                    session,
                    workflow_id=ctx.workflow_id,
                    owner_id=ctx.owner_id,
                )
            if counter is None:
                # Workflow disappeared between dispatch and execution; fall
                # back to index 0 so we still produce *something*. The Run
                # will likely fail downstream when the row really is gone,
                # but that's a tighter loop than crashing here.
                counter = 1
            index = (counter - 1) % len(variants)
            return list(variants[index].steps), _variant_label(variants[index], index)

        if self._plan.pick_variant == "random":
            index = self._rng.randrange(len(variants))
            return list(variants[index].steps), _variant_label(variants[index], index)

        raise RuntimeError(
            f"unknown pick_variant: {self._plan.pick_variant!r} "
            "(schema should have rejected this)",
        )


def _variant_label(variant: Variant, index: int) -> str:
    return variant.id or variant.name or f"variant_{index}"
