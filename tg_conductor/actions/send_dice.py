"""``send_dice`` action — thin TGClient.send_dice wrapper + event emission."""

from __future__ import annotations

from tg_conductor.actions.context import ActionContext
from tg_conductor.workflows.schema import SendDiceStep


async def execute(step: SendDiceStep, ctx: ActionContext) -> None:
    msg = await ctx.tg_client.send_dice(step.chat_id, emoji=step.emoji)
    await ctx.emit(
        "action.send.dice",
        {
            "step_index": ctx.step_index,
            "chat_id": step.chat_id,
            "emoji": step.emoji,
            "sent_message_id": msg.id,
        },
        message=f"sent dice {step.emoji} to {step.chat_id}",
    )
