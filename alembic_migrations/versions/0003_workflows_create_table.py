"""create workflows table

Revision ID: 0003_workflows
Revises: 0002_accounts
Create Date: 2026-05-29

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003_workflows"
down_revision: str | Sequence[str] | None = "0002_accounts"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "workflows",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "owner_id",
            sa.Integer(),
            sa.ForeignKey("owners.id"),
            nullable=False,
        ),
        sa.Column(
            "account_id",
            sa.Integer(),
            sa.ForeignKey("accounts.id"),
            nullable=False,
        ),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("source", sa.String(), nullable=False),
        sa.Column("trigger", sa.JSON(), nullable=False),
        sa.Column("action_plan", sa.JSON(), nullable=False),
        sa.Column("rr_counter", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("owner_id", "name", name="uq_workflows_owner_name"),
    )
    op.create_index("ix_workflows_owner_id", "workflows", ["owner_id"])
    op.create_index("ix_workflows_owner_enabled", "workflows", ["owner_id", "enabled"])
    op.create_index("ix_workflows_account_id", "workflows", ["account_id"])


def downgrade() -> None:
    op.drop_index("ix_workflows_account_id", table_name="workflows")
    op.drop_index("ix_workflows_owner_enabled", table_name="workflows")
    op.drop_index("ix_workflows_owner_id", table_name="workflows")
    op.drop_table("workflows")
