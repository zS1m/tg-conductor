"""create jobs table

Revision ID: 0004_jobs
Revises: 0003_workflows
Create Date: 2026-05-29

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0004_jobs"
down_revision: str | Sequence[str] | None = "0003_workflows"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "jobs",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("owner_id", sa.Integer(), sa.ForeignKey("owners.id"), nullable=False),
        # No FK on workflow_id: workflows are physically deleted by the YAML
        # sync (spec §8.10), but spec §8.4 requires keeping the Job rows so
        # their ``canceled`` status remains queryable. A hard FK would block
        # the workflow delete; we keep workflow_id as a logical reference.
        sa.Column("workflow_id", sa.Integer(), nullable=False),
        sa.Column(
            "account_id",
            sa.Integer(),
            sa.ForeignKey("accounts.id"),
            nullable=False,
        ),
        sa.Column("fire_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("variant_id", sa.String(), nullable=True),
        sa.Column("resolved_payload", sa.JSON(), nullable=True),
        # run_id has no FK constraint; runs table lands in §11. Add FK there.
        sa.Column("run_id", sa.Integer(), nullable=True),
        sa.Column("skip_reason", sa.String(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expansion_date", sa.Date(), nullable=True),
    )
    op.create_index("ix_jobs_status_fire_at", "jobs", ["status", "fire_at"])
    op.create_index(
        "ix_jobs_owner_workflow_fire_at",
        "jobs",
        ["owner_id", "workflow_id", "fire_at"],
    )
    op.create_index(
        "ix_jobs_workflow_expansion_date",
        "jobs",
        ["workflow_id", "expansion_date"],
    )


def downgrade() -> None:
    op.drop_index("ix_jobs_workflow_expansion_date", table_name="jobs")
    op.drop_index("ix_jobs_owner_workflow_fire_at", table_name="jobs")
    op.drop_index("ix_jobs_status_fire_at", table_name="jobs")
    op.drop_table("jobs")
