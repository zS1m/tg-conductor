"""create usage_events table

Revision ID: 0006_ai_usage
Revises: 0005_runs
Create Date: 2026-05-30

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0006_ai_usage"
down_revision: str | Sequence[str] | None = "0005_runs"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # spec ai-usage §"每次调用写 usage_events": id, owner_id, ts, kind,
    # units, cost_micros (integer micro-yuan), run_id, workflow_id,
    # account_id, meta JSON. run_id / workflow_id / account_id are
    # logical references (no FK) so future cross-Run admin tooling can
    # insert rows even when the originating run row has been pruned.
    op.create_table(
        "usage_events",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("owner_id", sa.Integer(), sa.ForeignKey("owners.id"), nullable=False),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("kind", sa.String(), nullable=False),
        sa.Column("units", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("cost_micros", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("run_id", sa.Integer(), nullable=True),
        sa.Column("workflow_id", sa.Integer(), nullable=True),
        sa.Column("account_id", sa.Integer(), nullable=True),
        sa.Column("meta", sa.JSON(), nullable=True),
    )
    op.create_index("ix_usage_events_owner_ts", "usage_events", ["owner_id", "ts"])
    op.create_index(
        "ix_usage_events_owner_kind_ts", "usage_events", ["owner_id", "kind", "ts"]
    )


def downgrade() -> None:
    op.drop_index("ix_usage_events_owner_kind_ts", table_name="usage_events")
    op.drop_index("ix_usage_events_owner_ts", table_name="usage_events")
    op.drop_table("usage_events")
