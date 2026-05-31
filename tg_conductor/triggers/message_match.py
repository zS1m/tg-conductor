"""``message_match`` trigger evaluator.

All configured fields are AND-combined: a message matches iff **every**
non-``None`` field on the trigger matches the corresponding field on the
message. Missing message fields (e.g. ``from_user_id is None`` while the
trigger demanded one) count as non-matches.

The schema rejects string ``chat_id`` on ``MessageMatchTrigger`` so by the
time we get here both sides are numeric — direct equality is enough.
"""

from __future__ import annotations

import re

from tg_conductor.tg_core.protocol import Message
from tg_conductor.workflows.schema import MessageMatchTrigger


def matches(trigger: MessageMatchTrigger, message: Message) -> bool:
    if trigger.chat_id != message.chat_id:
        return False
    if trigger.topic_id is not None and trigger.topic_id != message.topic_id:
        return False
    if (
        trigger.from_user_id is not None
        and trigger.from_user_id != message.from_user_id
    ):
        return False
    if trigger.text_pattern is not None:
        if message.text is None:
            return False
        if re.search(trigger.text_pattern, message.text) is None:
            return False
    return True
