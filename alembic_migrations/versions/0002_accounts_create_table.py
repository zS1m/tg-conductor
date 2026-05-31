"""create accounts table

Revision ID: 0002_accounts
Revises: 0001_owners
Create Date: 2026-05-28

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002_accounts"
down_revision: str | Sequence[str] | None = "0001_owners"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "accounts",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "owner_id",
            sa.Integer(),
            sa.ForeignKey("owners.id"),
            nullable=False,
        ),
        sa.Column("label", sa.String(), nullable=False),
        sa.Column("api_id", sa.Integer(), nullable=False),
        sa.Column("api_hash", sa.String(), nullable=False),
        sa.Column("session_string_enc", sa.String(), nullable=True),
        sa.Column("proxy", sa.String(), nullable=True),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("floodwait_until", sa.DateTime(), nullable=True),
        sa.Column("last_error", sa.String(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("owner_id", "label", name="uq_accounts_owner_label"),
    )
    op.create_index("ix_accounts_owner_status", "accounts", ["owner_id", "status"])
    op.create_index("ix_accounts_owner_id", "accounts", ["owner_id"])


def downgrade() -> None:
    op.drop_index("ix_accounts_owner_id", table_name="accounts")
    op.drop_index("ix_accounts_owner_status", table_name="accounts")
    op.drop_table("accounts")
