"""``click_button`` action — find a button on a target message and click it.

Match modes (mutually exclusive, enforced by schema):
* ``text`` — exact string match against ``button.text``.
* ``text_regex`` — ``re.search`` against ``button.text``.
* ``ai_image_prompt`` — **not implemented in C2**; §13 wires AI vision into
  this path. We raise ``NotImplementedError`` with a pointer to keep the
  surface honest.

Target message = :attr:`ActionContext.last_matched_message` by default; a
named reference can be specified via ``step.target``.
"""

from __future__ import annotations

import re

from tg_conductor.actions.context import ActionContext
from tg_conductor.tg_core.protocol import ButtonSpec, Message
from tg_conductor.workflows.schema import ClickButtonStep


async def execute(step: ClickButtonStep, ctx: ActionContext) -> None:
    target = _resolve_target(step, ctx)

    if step.match.ai_image_prompt is not None:
        raise NotImplementedError(
            "click_button.ai_image_prompt mode requires AI vision; "
            "§13 will implement this path by calling ctx.ai_client.vision()"
        )

    if not target.buttons:
        raise ValueError("click_button: target message has no inline keyboard")

    matched = _find_matching_button(target.buttons, step)
    if matched is None:
        raise ValueError(
            "click_button: no button matched the configured criteria "
            f"(text={step.match.text!r}, text_regex={step.match.text_regex!r})"
        )

    await ctx.tg_client.click_button(
        target.chat_id,
        target.id,
        text=matched.text,
    )
    await ctx.emit(
        "action.click_button",
        {
            "step_index": ctx.step_index,
            "chat_id": target.chat_id,
            "message_id": target.id,
            "matched_text": matched.text,
        },
        message=f"clicked {matched.text!r} on {target.chat_id}/{target.id}",
    )


def _resolve_target(step: ClickButtonStep, ctx: ActionContext) -> Message:
    if step.target == "last_matched":
        msg = ctx.last_matched_message
    else:
        msg = ctx.extras.get(step.target)
    if msg is None:
        raise ValueError(
            f"click_button: target {step.target!r} not available in run context"
        )
    return msg


def _find_matching_button(
    buttons: list[list[ButtonSpec]], step: ClickButtonStep
) -> ButtonSpec | None:
    for row in buttons:
        for btn in row:
            if step.match.text is not None and btn.text == step.match.text:
                return btn
            if step.match.text_regex is not None and re.search(
                step.match.text_regex, btn.text
            ):
                return btn
    return None
