"""Add click_id/utm to orders and raw_ad_clicks for click-ID attribution.

Revision ID: 007
Revises: 006
Create Date: 2025-02-17 00:00:00

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "007"
down_revision: Union[str, None] = "006"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("raw_shopify_orders", sa.Column("click_id", sa.String(), nullable=True))
    op.add_column("raw_shopify_orders", sa.Column("utm_source", sa.String(), nullable=True))
    op.add_column("raw_shopify_orders", sa.Column("utm_medium", sa.String(), nullable=True))
    op.add_column("raw_shopify_orders", sa.Column("utm_campaign", sa.String(), nullable=True))

    op.add_column("raw_woocommerce_orders", sa.Column("click_id", sa.String(), nullable=True))
    op.add_column("raw_woocommerce_orders", sa.Column("utm_source", sa.String(), nullable=True))
    op.add_column("raw_woocommerce_orders", sa.Column("utm_medium", sa.String(), nullable=True))
    op.add_column("raw_woocommerce_orders", sa.Column("utm_campaign", sa.String(), nullable=True))

    op.create_table(
        "raw_ad_clicks",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("click_id", sa.String(), nullable=False),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("campaign_id", sa.String(), nullable=False),
        sa.Column("campaign_name", sa.String(), nullable=True),
        sa.Column("channel", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=True),
    )
    op.create_index("ix_raw_ad_clicks_click_id", "raw_ad_clicks", ["click_id"])


def downgrade() -> None:
    op.drop_index("ix_raw_ad_clicks_click_id", "raw_ad_clicks")
    op.drop_table("raw_ad_clicks")
    for col in ["utm_campaign", "utm_medium", "utm_source", "click_id"]:
        op.drop_column("raw_woocommerce_orders", col)
        op.drop_column("raw_shopify_orders", col)
