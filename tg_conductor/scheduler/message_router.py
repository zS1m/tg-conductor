"""TGClient ``on_message`` handler that fires ``message_match`` workflows.

Construct one ``MessageRouter`` per (owner, account) pair; AccountManager
registers :meth:`on_message` as the TGClient handler at start-up. For each
inbound message we scan the owner's enabled ``message_match`` workflows
that target *this* account, and call
:meth:`Dispatcher.dispatch_immediate` for each match — the spec's "秒级
派发" semantic.
"""

from __future__ import annotations

import logging

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tg_conductor.scheduler.dispatcher import Dispatcher
from tg_conductor.tg_core.protocol import Message
from tg_conductor.triggers.message_match import matches
from tg_conductor.workflows import repo as workflow_repo
from tg_conductor.workflows.schema import MessageMatchTrigger

log = logging.getLogger(__name__)


class MessageRouter:
    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        dispatcher: Dispatcher,
        owner_id: int,
        account_id: int,
    ) -> None:
        self._session_factory = session_factory
        self._dispatcher = dispatcher
        self._owner_id = owner_id
        self._account_id = account_id

    async def on_message(self, msg: Message) -> list[int]:
        """Scan workflows, dispatch matches. Returns the list of Job ids created."""
        async with self._session_factory() as session:
            workflows = await workflow_repo.list_for_owner(
                session, owner_id=self._owner_id, enabled_only=True
            )

        created: list[int] = []
        for wf in workflows:
            if not isinstance(wf.trigger, MessageMatchTrigger):
                continue
            if wf.account_id != self._account_id:
                continue
            if not matches(wf.trigger, msg):
                continue
            assert wf.id is not None
            job_id = await self._dispatcher.dispatch_immediate(
                workflow_id=wf.id,
                account_id=self._account_id,
                trigger_message=msg,
            )
            if job_id is not None:
                created.append(job_id)

        if created:
            log.info(
                "message_router.matched owner_id=%d account_id=%d chat_id=%s jobs=%s",
                self._owner_id,
                self._account_id,
                msg.chat_id,
                created,
            )
        return created
