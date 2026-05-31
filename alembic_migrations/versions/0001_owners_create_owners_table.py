"""create owners table

Revision ID: 0001_owners
Revises:
Create Date: 2026-05-28 19:45:16.636661

"""

from collections.abc import Sequence
from datetime import UTC, datetime

import sqlalchemy as sa
from alembic import op

revision: str = "0001_owners"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    owners = op.create_table(
        "owners",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )
    op.bulk_insert(
        owners,
        [{"id": 1, "name": "self", "created_at": datetime.now(UTC)}],
    )


def downgrade() -> None:
    op.drop_table("owners")
