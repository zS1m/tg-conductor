"""``forward`` action — relay a context message to ``to_chat_id``.

``step.source`` selects which message in the run context to forward:
* ``"last_matched"`` (default) — :attr:`ActionContext.last_matched_message`.
* Any other string — looked up in :attr:`ActionContext.extras` (a named
  reference written by an earlier step, e.g. ``wait_for_step_3``).

Missing source raises ``ValueError`` — the executor catches it and emits
``action.failed``.
"""

from __future__ import annotations

from tg_conductor.actions.context import ActionContext
from tg_conductor.tg_core.protocol import Message
from tg_conductor.workflows.schema import ForwardStep


async def execute(step: ForwardStep, ctx: ActionContext) -> None:
    source_msg = _resolve_source(step, ctx)
    msg = await ctx.tg_client.forward(
        from_chat_id=source_msg.chat_id,
        message_id=source_msg.id,
        to_chat_id=step.to_chat_id,
    )
    await ctx.emit(
        "action.forward",
        {
            "step_index": ctx.step_index,
            "from_chat_id": source_msg.chat_id,
            "from_message_id": source_msg.id,
            "to_chat_id": step.to_chat_id,
            "sent_message_id": msg.id,
        },
        message=(f"forwarded {source_msg.chat_id}/{source_msg.id} → {step.to_chat_id}"),
    )


def _resolve_source(step: ForwardStep, ctx: ActionContext) -> Message:
    if step.source == "last_matched":
        msg = ctx.last_matched_message
    else:
        msg = ctx.extras.get(step.source)
    if msg is None:
        raise ValueError(
            f"forward: source {step.source!r} not available in run context"
        )
    return msg
