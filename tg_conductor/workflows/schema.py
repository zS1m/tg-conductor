"""Pydantic v2 wire-schema for Workflows (YAML / HTTP / DB).

These models are the single source of truth for what a Workflow looks like.
- YAML loader parses files into ``Workflow``.
- DB stores ``trigger`` / ``action_plan`` as JSON via :class:`PydanticJSONType`,
  bound to ``TypeAdapter(Trigger)`` / ``TypeAdapter(ActionPlan)`` so unknown
  discriminator tags raise ``ValidationError`` on read.
- HTTP responses serialize these same models.

Validation that needs DB access (account_id existence, label uniqueness) is
*not* here — see ``workflows.validate``. This module only enforces what can
be checked from the values alone.
"""

from __future__ import annotations

import random
import re
from typing import Annotated, Literal

from croniter import croniter
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, model_validator

# ---------------------------------------------------------------- shared types


class CountRange(BaseModel):
    """Inclusive integer range used for ``time_window.count`` / ``pick_n``."""

    model_config = ConfigDict(extra="forbid")
    min: int = Field(ge=0)
    max: int = Field(ge=0)

    @model_validator(mode="after")
    def _check_order(self) -> CountRange:
        if self.max < self.min:
            raise ValueError(f"max ({self.max}) must be >= min ({self.min})")
        return self


class DelayRange(BaseModel):
    """Uniform[min, max] seconds. Used for ``inter_step_delay`` and the
    range form of a step's ``delay_before``."""

    model_config = ConfigDict(extra="forbid")
    min: float = Field(ge=0)
    max: float = Field(ge=0)

    @model_validator(mode="after")
    def _check_order(self) -> DelayRange:
        if self.max < self.min:
            raise ValueError(f"max ({self.max}) must be >= min ({self.min})")
        return self


_WINDOW_RE = re.compile(r"^(\d{2}):(\d{2})-(\d{2}):(\d{2})$")
_DURATION_RE = re.compile(r"^(\d+\s*(?:s|m|h))+$")


def parse_window(value: str) -> tuple[tuple[int, int], tuple[int, int]]:
    m = _WINDOW_RE.match(value.strip())
    if m is None:
        raise ValueError(f"window must match HH:MM-HH:MM, got {value!r}")
    start_h, start_m, end_h, end_m = (int(x) for x in m.groups())
    for h, mi, label in (
        (start_h, start_m, "start"),
        (end_h, end_m, "end"),
    ):
        if not (0 <= h <= 23 and 0 <= mi <= 59):
            raise ValueError(f"window {label} out of range: {value!r}")
    return (start_h, start_m), (end_h, end_m)


# ---------------------------------------------------------------- triggers


class CronTrigger(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: Literal["cron"]
    expression: str

    @model_validator(mode="after")
    def _validate_cron(self) -> CronTrigger:
        if not croniter.is_valid(self.expression):
            raise ValueError(f"invalid cron expression: {self.expression!r}")
        return self


class TimeWindowTrigger(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: Literal["time_window"]
    window: str
    count: int | CountRange
    min_gap: str  # duration like "30m" / "1h30m"; parsed by §9.2

    @model_validator(mode="after")
    def _validate(self) -> TimeWindowTrigger:
        start, end = parse_window(self.window)
        if end <= start:
            raise ValueError(
                f"window end {end} must be later than start {start} (no day-crossing)"
            )
        if isinstance(self.count, int) and self.count <= 0:
            raise ValueError(f"count must be positive, got {self.count}")
        if not _DURATION_RE.match(self.min_gap.replace(" ", "")):
            raise ValueError(
                f"min_gap must be a duration like '30m' or '1h30m', got {self.min_gap!r}"
            )
        return self


class MessageMatchTrigger(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: Literal["message_match"]
    # Must be an int: TG events arrive with already-resolved numeric chat ids.
    # A trigger ``chat_id: "@channel"`` would silently never match an inbound
    # message whose ``chat_id`` is, say, -1001234567890.
    chat_id: int
    topic_id: int | None = None
    text_pattern: str | None = None
    from_user_id: int | None = None

    @model_validator(mode="after")
    def _validate_regex(self) -> MessageMatchTrigger:
        if self.text_pattern is not None:
            try:
                re.compile(self.text_pattern)
            except re.error as exc:
                raise ValueError(f"text_pattern is not a valid regex: {exc}") from exc
        return self


class StartupTrigger(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: Literal["startup"]


Trigger = Annotated[
    CronTrigger | TimeWindowTrigger | MessageMatchTrigger | StartupTrigger,
    Field(discriminator="type"),
]
TriggerAdapter: TypeAdapter[Trigger] = TypeAdapter(Trigger)


# ---------------------------------------------------------------- action steps


_ALLOWED_DICE_EMOJI = frozenset({"🎲", "🎯", "🏀", "⚽", "🎳", "🎰"})


class _BaseStep(BaseModel):
    """Common fields all step types accept.

    Subclasses set their own ``model_config`` if they need ``extra="forbid"``.
    """

    timeout: float | None = Field(default=None, gt=0)
    continue_on_error: bool = False
    # Wait this long *before* entering the step (outside the step's own
    # ``timeout`` window — see executor). Bare number = fixed seconds;
    # ``{min, max}`` = uniform random seconds. None = no pre-delay.
    delay_before: float | DelayRange | None = None

    @model_validator(mode="after")
    def _check_delay_before(self) -> _BaseStep:
        if isinstance(self.delay_before, int | float) and self.delay_before < 0:
            raise ValueError("delay_before must be >= 0")
        return self


class SendTextStep(_BaseStep):
    model_config = ConfigDict(extra="forbid")
    action: Literal["send_text"]
    chat_id: int | str
    text: str | None = None
    text_pool: list[str] | None = None
    pick_n: int | CountRange | None = None
    shuffle: bool = False
    message_thread_id: int | None = None
    delete_after: int | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def _check_payload(self) -> SendTextStep:
        if isinstance(self.chat_id, str) and not self.chat_id.strip():
            raise ValueError("chat_id must not be empty")
        has_text = self.text is not None
        has_pool = self.text_pool is not None and len(self.text_pool) > 0
        if has_text and has_pool:
            raise ValueError("send_text: text and text_pool are mutually exclusive")
        if not has_text and not has_pool:
            raise ValueError("send_text: provide either text or text_pool")
        if has_pool:
            if self.pick_n is None:
                raise ValueError("send_text: pick_n is required when text_pool is set")
            assert self.text_pool is not None
            pool_size = len(self.text_pool)
            max_pick = self.pick_n if isinstance(self.pick_n, int) else self.pick_n.max
            if max_pick > pool_size:
                raise ValueError(
                    f"send_text: pick_n max ({max_pick}) exceeds text_pool size ({pool_size})"
                )
        return self


class SendDiceStep(_BaseStep):
    model_config = ConfigDict(extra="forbid")
    action: Literal["send_dice"]
    chat_id: int | str
    emoji: str = "🎲"
    message_thread_id: int | None = None
    delete_after: int | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def _validate(self) -> SendDiceStep:
        if self.emoji not in _ALLOWED_DICE_EMOJI:
            raise ValueError(
                f"send_dice: emoji must be one of {sorted(_ALLOWED_DICE_EMOJI)}, "
                f"got {self.emoji!r}"
            )
        if isinstance(self.chat_id, str) and not self.chat_id.strip():
            raise ValueError("chat_id must not be empty")
        return self


class ForwardStep(_BaseStep):
    model_config = ConfigDict(extra="forbid")
    action: Literal["forward"]
    to_chat_id: int | str
    to_message_thread_id: int | None = None
    source: str = "last_matched"  # context ref; see runs spec

    @model_validator(mode="after")
    def _validate(self) -> ForwardStep:
        if isinstance(self.to_chat_id, str) and not self.to_chat_id.strip():
            raise ValueError("to_chat_id must not be empty")
        return self


class ClickButtonMatch(BaseModel):
    """Discriminator-free union: exactly one of (text | text_regex | ai_image_prompt)."""

    model_config = ConfigDict(extra="forbid")
    text: str | None = None
    text_regex: str | None = None
    ai_image_prompt: str | None = None

    @model_validator(mode="after")
    def _exactly_one(self) -> ClickButtonMatch:
        provided = [
            name
            for name, val in (
                ("text", self.text),
                ("text_regex", self.text_regex),
                ("ai_image_prompt", self.ai_image_prompt),
            )
            if val is not None
        ]
        if len(provided) != 1:
            raise ValueError(
                "click_button.match: must specify exactly one of "
                f"text / text_regex / ai_image_prompt, got {provided!r}"
            )
        if self.text_regex is not None:
            try:
                re.compile(self.text_regex)
            except re.error as exc:
                raise ValueError(f"text_regex is not a valid regex: {exc}") from exc
        return self


class ClickButtonStep(_BaseStep):
    model_config = ConfigDict(extra="forbid")
    action: Literal["click_button"]
    match: ClickButtonMatch
    target: str = "last_matched"


class WaitForStep(_BaseStep):
    model_config = ConfigDict(extra="forbid")
    action: Literal["wait_for"]
    chat_id: int | str
    text_pattern: str | None = None
    from_user_id: int | None = None
    topic_id: int | None = None

    @model_validator(mode="after")
    def _validate(self) -> WaitForStep:
        if isinstance(self.chat_id, str) and not self.chat_id.strip():
            raise ValueError("chat_id must not be empty")
        if self.text_pattern is not None:
            try:
                re.compile(self.text_pattern)
            except re.error as exc:
                raise ValueError(f"text_pattern is not a valid regex: {exc}") from exc
        return self


class AIReplyStep(_BaseStep):
    model_config = ConfigDict(extra="forbid")
    action: Literal["ai_reply"]
    to_chat_id: int | str
    prompt_template: str
    model: str | None = None
    max_tokens: int | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def _validate(self) -> AIReplyStep:
        if isinstance(self.to_chat_id, str) and not self.to_chat_id.strip():
            raise ValueError("to_chat_id must not be empty")
        if not self.prompt_template.strip():
            raise ValueError("prompt_template must not be empty")
        return self


ActionStep = Annotated[
    SendTextStep
    | SendDiceStep
    | ForwardStep
    | ClickButtonStep
    | WaitForStep
    | AIReplyStep,
    Field(discriminator="action"),
]


# ---------------------------------------------------------------- variants & plan


class Variant(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: str | None = None
    name: str | None = None
    steps: list[ActionStep] = Field(default_factory=list)

    @model_validator(mode="after")
    def _non_empty(self) -> Variant:
        if not self.steps:
            raise ValueError("variant: steps must be non-empty")
        return self


PickVariant = Literal["random", "round_robin"]


class ActionPlan(BaseModel):
    model_config = ConfigDict(extra="forbid")
    steps: list[ActionStep] = Field(default_factory=list)
    variants: list[Variant] = Field(default_factory=list)
    pick_variant: PickVariant | None = None
    inter_step_delay: DelayRange | None = None
    job_timeout: float | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def _validate(self) -> ActionPlan:
        has_steps = bool(self.steps)
        has_variants = bool(self.variants)
        if has_steps and has_variants:
            raise ValueError("action_plan: steps and variants are mutually exclusive")
        if not has_steps and not has_variants:
            raise ValueError(
                "action_plan: must contain at least one step or one variant"
            )
        if has_variants and self.pick_variant is None:
            raise ValueError(
                "action_plan: pick_variant is required when variants are used"
            )
        if has_steps and self.pick_variant is not None:
            raise ValueError(
                "action_plan: pick_variant only applies when variants are used"
            )
        # job_timeout must cover each step's own budget: its resolved timeout
        # plus the worst-case delay_before (which runs outside the step's
        # timeout window but inside job_timeout). Enforce now so config
        # invalid-states never reach the DB.
        if self.job_timeout is not None:
            step_budgets = [
                _resolved_timeout(s) + _delay_upper_bound(s.delay_before)
                for s in self._all_steps()
            ]
            if step_budgets and self.job_timeout < max(step_budgets):
                raise ValueError(
                    f"action_plan: job_timeout ({self.job_timeout}) must be >= "
                    f"max step budget ({max(step_budgets)}, i.e. step timeout + "
                    f"delay_before upper bound)"
                )
        return self

    def _all_steps(self) -> list[ActionStep]:
        if self.variants:
            return [s for v in self.variants for s in v.steps]
        return list(self.steps)


_STEP_DEFAULT_TIMEOUTS: dict[str, float] = {
    "send_text": 30.0,
    "send_dice": 30.0,
    "forward": 30.0,
    "click_button": 30.0,
    "ai_reply": 60.0,
    "wait_for": 600.0,
}


def _resolved_timeout(step: ActionStep) -> float:
    """The effective timeout for a step — explicit value or per-type default."""
    explicit = getattr(step, "timeout", None)
    if explicit is not None:
        return float(explicit)
    return _STEP_DEFAULT_TIMEOUTS[step.action]


def _resolve_delay(delay: float | DelayRange | None, rng: random.Random) -> float:
    """Concrete pre-step delay in seconds: None→0, scalar→itself, range→uniform."""
    if delay is None:
        return 0.0
    if isinstance(delay, DelayRange):
        return rng.uniform(delay.min, delay.max)
    return float(delay)


def _delay_upper_bound(delay: float | DelayRange | None) -> float:
    """Worst-case pre-step delay used for job_timeout budgeting."""
    if delay is None:
        return 0.0
    if isinstance(delay, DelayRange):
        return delay.max
    return float(delay)


ActionPlanAdapter: TypeAdapter[ActionPlan] = TypeAdapter(ActionPlan)


# ---------------------------------------------------------------- top-level


class Workflow(BaseModel):
    """Wire-shape of a Workflow as found in YAML / HTTP bodies.

    ``id`` / ``owner_id`` / ``source`` / ``rr_counter`` / timestamps are
    persistence concerns and live on ``WorkflowRow`` instead.
    """

    model_config = ConfigDict(extra="forbid")
    name: str
    account_id: int
    enabled: bool = True
    trigger: Trigger
    action_plan: ActionPlan

    @model_validator(mode="after")
    def _validate(self) -> Workflow:
        if not self.name.strip():
            raise ValueError("name must not be empty")
        return self
