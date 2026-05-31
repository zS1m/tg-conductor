"""create runs + run_events tables

Revision ID: 0005_runs
Revises: 0004_jobs
Create Date: 2026-05-29

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0005_runs"
down_revision: str | Sequence[str] | None = "0004_jobs"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "runs",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("owner_id", sa.Integer(), sa.ForeignKey("owners.id"), nullable=False),
        # Logical references — see model docstring.
        sa.Column("workflow_id", sa.Integer(), nullable=False),
        sa.Column(
            "account_id",
            sa.Integer(),
            sa.ForeignKey("accounts.id"),
            nullable=False,
        ),
        sa.Column("job_id", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("error", sa.String(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_runs_owner_workflow_started",
        "runs",
        ["owner_id", "workflow_id", "started_at"],
    )
    op.create_index("ix_runs_workflow_started", "runs", ["workflow_id", "started_at"])

    # run_events column shape follows specs/runs/spec.md Requirement
    # "结构化 run_events 流": id, run_id, owner_id, seq, ts, level, type,
    # message, attrs(JSON). owner_id duplicates runs.owner_id but lets us
    # build (owner_id, ts) indexes for cross-Run audit queries without a
    # join, and keeps the multi-tenant invariant (every business table
    # carries owner_id).
    op.create_table(
        "run_events",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("owner_id", sa.Integer(), sa.ForeignKey("owners.id"), nullable=False),
        sa.Column("run_id", sa.Integer(), sa.ForeignKey("runs.id"), nullable=False),
        sa.Column("seq", sa.Integer(), nullable=False),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("level", sa.String(), nullable=False, server_default="INFO"),
        sa.Column("type", sa.String(), nullable=False),
        sa.Column("message", sa.String(), nullable=False, server_default=""),
        sa.Column("attrs", sa.JSON(), nullable=True),
        sa.UniqueConstraint("run_id", "seq", name="uq_run_events_run_seq"),
    )
    op.create_index("ix_run_events_run_seq", "run_events", ["run_id", "seq"])
    op.create_index("ix_run_events_owner_ts", "run_events", ["owner_id", "ts"])


def downgrade() -> None:
    op.drop_index("ix_run_events_owner_ts", table_name="run_events")
    op.drop_index("ix_run_events_run_seq", table_name="run_events")
    op.drop_table("run_events")
    op.drop_index("ix_runs_workflow_started", table_name="runs")
    op.drop_index("ix_runs_owner_workflow_started", table_name="runs")
    op.drop_table("runs")
