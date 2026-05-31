"""DB-aware validation that complements :mod:`workflows.schema`.

Pydantic validators in ``schema.py`` cover what's checkable from the values
alone (mutex fields, regex syntax, cron syntax, dice emoji enum).
This module covers what needs the DB:

* The referenced ``account_id`` exists for the same ``owner_id``.
* The account is not ``disabled``.

Schema-level errors are not silenced — callers should catch
``pydantic.ValidationError`` from ``Workflow.model_validate`` separately.
"""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from tg_conductor.accounts import repo as account_repo
from tg_conductor.accounts.models import AccountStatus
from tg_conductor.workflows.schema import Workflow


class WorkflowValidationError(ValueError):
    """Validation failed at the DB layer (referenced row missing / disabled)."""


async def validate_account_ref(
    session: AsyncSession,
    *,
    owner_id: int,
    account_id: int,
) -> None:
    """Raise if ``account_id`` is not a usable account for ``owner_id``."""
    account = await account_repo.get_by_id(session, account_id, owner_id=owner_id)
    if account is None:
        raise WorkflowValidationError(
            f"account_id={account_id} does not exist for owner_id={owner_id}"
        )
    if account.status == AccountStatus.disabled:
        raise WorkflowValidationError(
            f"account_id={account_id} is disabled and cannot be referenced "
            "by new workflows"
        )


async def validate_workflow(
    session: AsyncSession,
    *,
    owner_id: int,
    workflow: Workflow,
) -> None:
    """Run all DB-aware checks. Pydantic-level checks already happened during parse."""
    await validate_account_ref(
        session, owner_id=owner_id, account_id=workflow.account_id
    )
