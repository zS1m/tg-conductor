"""Local patches for Kurigram (the pyrogram fork) bugs.

Each patch is a mixin: subclass it on top of ``pyrogram.Client`` so the
patched method wins via MRO. The classes here MUST be deletable once the
upstream issue is fixed — keep them small and self-contained.

Current patches
---------------
``SafeGetForumTopics`` — a defensive re-implementation of
``pyrogram.Client.get_forum_topics``. Kurigram 2.2.23's pagination
dereferences ``top_message.date`` on the trailing topic of every page,
which raises ``AttributeError`` for topics that carry no ``top_message``
(archived or empty topics). This version guards that dereference and stops
paging cleanly instead of crashing. Remove once upstream is fixed.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator

import pyrogram
from pyrogram import raw, types, utils

# Telegram caps a single ``messages.GetForumTopics`` request at 100 rows.
_MAX_PAGE = 100


class SafeGetForumTopics:
    """Drop-in replacement for ``pyrogram.Client.get_forum_topics`` paging."""

    async def get_forum_topics(
        self: pyrogram.Client,  # type: ignore[misc]
        chat_id: int | str,
        limit: int = 0,
    ) -> AsyncGenerator[types.ForumTopic, None]:
        peer = await self.resolve_peer(chat_id)

        # ``limit == 0`` means "every topic"; otherwise stop once we have
        # yielded ``limit`` of them.
        wanted = limit if limit > 0 else (1 << 31) - 1
        yielded = 0

        # Pagination cursor. Telegram walks forum topics by the trailing
        # topic's last-message coordinates plus that topic's own id.
        cursor_date = 0
        cursor_message_id = 0
        cursor_topic_id = 0
        seen_ids: set[int] = set()

        while yielded < wanted:
            response = await self.invoke(
                raw.functions.messages.GetForumTopics(
                    peer=peer,
                    offset_date=cursor_date,
                    offset_id=cursor_message_id,
                    offset_topic=cursor_topic_id,
                    limit=min(_MAX_PAGE, wanted - yielded),
                )
            )

            users = {u.id: u for u in response.users}
            chats = {c.id: c for c in response.chats}

            messages: dict[int, types.Message] = {}
            for raw_message in response.messages:
                if isinstance(raw_message, raw.types.MessageEmpty):
                    continue
                messages[raw_message.id] = await types.Message._parse(
                    self, raw_message, users, chats
                )

            page: list[types.ForumTopic] = []
            for raw_topic in response.topics:
                topic = types.ForumTopic._parse(self, raw_topic, messages, users, chats)
                if topic is None or topic.id in seen_ids:
                    continue
                seen_ids.add(topic.id)
                page.append(topic)

            # An empty page (after dedup) means the channel has no more
            # topics to hand out.
            if not page:
                return

            for topic in page:
                yield topic
                yielded += 1
                if yielded >= wanted:
                    return

            # Move the cursor onto the trailing topic for the next request.
            # If it has no ``top_message`` there is nothing left to page from
            # (and dereferencing it is the upstream crash), so finish here.
            tail = page[-1]
            if tail.top_message is None or tail.top_message.date is None:
                return
            cursor_date = utils.datetime_to_timestamp(tail.top_message.date)
            cursor_message_id = tail.top_message.id
            cursor_topic_id = tail.id
