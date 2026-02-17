"""Add engine_run_metadata for persisted run history.

Revision ID: 005
Revises: 004
Create Date: 2025-02-17 00:00:00

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "005"
down_revision: Union[str, None] = "004"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "engine_run_metadata",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("run_id", sa.String(), nullable=False),
        sa.Column("timestamp", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("mta_version", sa.String(), nullable=False, server_default=""),
        sa.Column("mmm_version", sa.String(), nullable=False, server_default=""),
        sa.Column("data_snapshot_id", sa.String(), nullable=False, server_default=""),
    )
    op.create_index("ix_engine_run_metadata_run_id", "engine_run_metadata", ["run_id"])
    op.create_index("ix_engine_run_metadata_timestamp", "engine_run_metadata", ["timestamp"])


def downgrade() -> None:
    op.drop_index("ix_engine_run_metadata_timestamp", "engine_run_metadata")
    op.drop_index("ix_engine_run_metadata_run_id", "engine_run_metadata")
    op.drop_table("engine_run_metadata")
