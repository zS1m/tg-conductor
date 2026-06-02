"""Derive an account's Telegram *update mode* from its workflows.

An account is **receiving** (must subscribe to Telegram updates) iff it owns at
least one enabled workflow that either:

* triggers on ``message_match`` (inbound message starts the run), or
* has an ``action_plan`` containing a ``wait_for`` step (the run blocks on an
  inbound message mid-plan).

Otherwise it is **send-only** — every trigger is ``cron`` / ``time_window`` /
``startup`` and every action only sends, so the connection never needs updates.

``ai_reply`` / ``forward`` consume the trigger message or a prior ``wait_for``
result; a workflow using them necessarily already contains ``message_match`` or
``wait_for``, so they don't widen the rule (design D2). See the ``accounts`` /
``tg-core`` specs for how the mode drives ``KurigramAdapter(receive_updates=...)``.
"""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from tg_conductor.workflows import repo as workflow_repo
from tg_conductor.workflows.models import WorkflowRow
from tg_conductor.workflows.schema import ActionPlan


def _plan_has_wait_for(plan: ActionPlan) -> bool:
    """True if any step (in ``steps`` or any ``variant``) is ``wait_for``."""
    steps = plan.steps if plan.steps else [s for v in plan.variants for s in v.steps]
    return any(step.action == "wait_for" for step in steps)


def _row_needs_updates(row: WorkflowRow) -> bool:
    if getattr(row.trigger, "type", None) == "message_match":
        return True
    return _plan_has_wait_for(row.action_plan)


async def account_needs_updates(
    session: AsyncSession,
    *,
    owner_id: int,
    account_id: int,
) -> bool:
    """Whether ``account_id`` must receive updates given its enabled workflows.

    Reuses ``workflow_repo.list_for_owner(enabled_only=True)`` (same call the
    expander uses) and aggregates in Python by ``account_id``; workflow counts
    are small so the cost is negligible (design D2). No enabled workflow → False.
    """
    rows = await workflow_repo.list_for_owner(session, owner_id, enabled_only=True)
    return any(
        row.account_id == account_id and _row_needs_updates(row) for row in rows
    )
