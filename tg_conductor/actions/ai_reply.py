"""``ai_reply`` action — feed context message to AIClient, send reply to chat.

Prompt template uses Python ``str.format`` with ``{message_text}`` (and any
other named keys present on :attr:`ActionContext.extras`). Model resolution:
``step.model`` wins if set, otherwise ``settings.openai_model_chat``.

The AI call goes through :attr:`ActionContext.ai_client` (the §13-supplied
:class:`AIClient` Protocol). Token usage from the response is recorded in
the emitted event payload so SSE / usage events can pick it up.
"""

from __future__ import annotations

from tg_conductor.actions.context import ActionContext, ChatMessage
from tg_conductor.config.settings import get_settings
from tg_conductor.workflows.schema import AIReplyStep


async def execute(step: AIReplyStep, ctx: ActionContext) -> None:
    source = ctx.last_matched_message
    if source is None:
        raise ValueError(
            "ai_reply: no source message in run context (need prior wait_for "
            "or message_match trigger)"
        )

    template_vars = {"message_text": source.text or "", **ctx.extras}
    prompt = step.prompt_template.format(**template_vars)
    model = step.model or get_settings().openai_model_chat

    response = await ctx.ai_client.chat(
        [ChatMessage(role="user", content=prompt)],
        model=model,
        max_tokens=step.max_tokens,
        owner_id=ctx.owner_id,
        run_id=ctx.run_id,
        workflow_id=ctx.workflow_id,
        account_id=ctx.account_id,
    )
    sent = await ctx.tg_client.send_text(step.to_chat_id, response.text)

    await ctx.emit(
        "action.ai_reply",
        {
            "step_index": ctx.step_index,
            "to_chat_id": step.to_chat_id,
            "model": response.model,
            "prompt_tokens": response.prompt_tokens,
            "completion_tokens": response.completion_tokens,
            "total_tokens": response.total_tokens,
            "latency_ms": response.latency_ms,
            "sent_message_id": sent.id,
        },
        message=f"ai_reply ({response.model}) → {step.to_chat_id}",
    )
