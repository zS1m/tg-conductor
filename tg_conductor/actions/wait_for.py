"""``wait_for`` action — block until a matching inbound message lands.

Delegates the actual subscription to :meth:`TGClient.wait_for`, which is
backed by ``FakeTGClient`` in tests and ``KurigramAdapter`` in production.
The successful message is stored on :attr:`ActionContext.last_matched_message`
so downstream ``click_button`` / ``forward`` / ``ai_reply`` can consume it.

Timeout: uses ``step.timeout`` directly if set; otherwise the type default
(600 s) lives in schema's ``_resolved_timeout``. The executor (C3) also
wraps every step in an ``asyncio.wait_for`` of the same value, so a hung
TGClient.wait_for ultimately raises from one side or the other.
"""

from __future__ import annotations

import re

from tg_conductor.actions.context import ActionContext
from tg_conductor.tg_core.protocol import Message
from tg_conductor.workflows.schema import WaitForStep, _resolved_timeout


async def execute(step: WaitForStep, ctx: ActionContext) -> None:
    timeout = _resolved_timeout(step)

    def predicate(msg: Message) -> bool:
        if step.text_pattern is not None:
            if msg.text is None:
                return False
            if re.search(step.text_pattern, msg.text) is None:
                return False
        if step.from_user_id is not None and msg.from_user_id != step.from_user_id:
            return False
        if step.topic_id is not None and msg.topic_id != step.topic_id:
            return False
        return True

    matched = await ctx.tg_client.wait_for(
        step.chat_id, predicate=predicate, timeout=timeout
    )
    ctx.last_matched_message = matched
    await ctx.emit(
        "action.wait_for",
        {
            "step_index": ctx.step_index,
            "chat_id": step.chat_id,
            "matched_message_id": matched.id,
            "matched_text": matched.text,
            "timeout": timeout,
        },
        message=f"matched message {matched.id} in {step.chat_id}",
    )
