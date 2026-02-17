"""SQLModel definitions for all product-engine tables."""
from datetime import date, datetime
from typing import Any, Optional
from uuid import uuid4

from sqlalchemy import Column, JSON
from sqlmodel import Field, SQLModel


# ----- Raw (ingest-only) -----


class RawMetaAds(SQLModel, table=True):
    """Raw Meta ads data; schema mirrors CSV from data pipeline."""

    __tablename__ = "raw_meta_ads"
    id: Optional[int] = Field(default=None, primary_key=True)
    date: date
    campaign_id: str
    campaign_name: Optional[str] = None
    spend: float = 0.0
    impressions: Optional[int] = None
    clicks: Optional[int] = None
    created_at: Optional[datetime] = Field(default_factory=datetime.utcnow)


class RawGoogleAds(SQLModel, table=True):
    """Raw Google ads data; schema mirrors CSV."""

    __tablename__ = "raw_google_ads"
    id: Optional[int] = Field(default=None, primary_key=True)
    date: date
    campaign_id: str
    campaign_name: Optional[str] = None
    spend: float = 0.0
    impressions: Optional[int] = None
    clicks: Optional[int] = None
    created_at: Optional[datetime] = Field(default_factory=datetime.utcnow)


class RawBingAds(SQLModel, table=True):
    """Raw Bing ads data; schema mirrors CSV."""

    __tablename__ = "raw_bing_ads"
    id: Optional[int] = Field(default=None, primary_key=True)
    date: date
    campaign_id: str
    campaign_name: Optional[str] = None
    spend: float = 0.0
    impressions: Optional[int] = None
    clicks: Optional[int] = None
    created_at: Optional[datetime] = Field(default_factory=datetime.utcnow)


class RawPinterestAds(SQLModel, table=True):
    """Raw Pinterest ads data; schema mirrors CSV."""

    __tablename__ = "raw_pinterest_ads"
    id: Optional[int] = Field(default=None, primary_key=True)
    date: date
    campaign_id: str
    campaign_name: Optional[str] = None
    spend: float = 0.0
    impressions: Optional[int] = None
    clicks: Optional[int] = None
    created_at: Optional[datetime] = Field(default_factory=datetime.utcnow)


class RawShopifyOrders(SQLModel, table=True):
    """Raw Shopify orders; extended schema for real API (name, financial_status, net_revenue, etc.)."""

    __tablename__ = "raw_shopify_orders"
    id: Optional[int] = Field(default=None, primary_key=True)
    order_id: str
    name: Optional[str] = None
    order_date: date
    revenue: float = 0.0
    is_new_customer: Optional[bool] = None
    created_at: Optional[datetime] = Field(default_factory=datetime.utcnow)
    closed_at: Optional[datetime] = None
    cancelled_at: Optional[datetime] = None
    financial_status: Optional[str] = None
    fulfillment_status: Optional[str] = None
    total_price: Optional[float] = None
    subtotal_price: Optional[float] = None
    total_tax: Optional[float] = None
    currency: Optional[str] = None
    source_name: Optional[str] = None
    line_items_json: Optional[Any] = Field(default=None, sa_column=Column(JSON(), nullable=True))
    customer_id: Optional[int] = None
    is_test: bool = False
    net_revenue: Optional[float] = None
    click_id: Optional[str] = None  # gclid, fbclid, etc. for click-ID attribution
    utm_source: Optional[str] = None
    utm_medium: Optional[str] = None
    utm_campaign: Optional[str] = None


class RawShopifyTransactions(SQLModel, table=True):
    """Per-transaction: sale, refund, capture; used to compute order net_revenue."""

    __tablename__ = "raw_shopify_transactions"
    id: Optional[int] = Field(default=None, primary_key=True)
    order_id: int = Field(foreign_key="raw_shopify_orders.id")
    kind: str  # sale, refund, capture, authorization
    status: Optional[str] = None
    amount: Optional[float] = None
    currency: Optional[str] = None
    created_at: Optional[datetime] = None
    gateway: Optional[str] = None
    parent_id: Optional[int] = None
    source_name: Optional[str] = None


class RawWooCommerceOrders(SQLModel, table=True):
    """Raw WooCommerce orders; same shape as Shopify for unified attribution/MMM."""

    __tablename__ = "raw_woocommerce_orders"
    id: Optional[int] = Field(default=None, primary_key=True)
    order_id: str
    name: Optional[str] = None
    order_date: date
    revenue: float = 0.0
    is_new_customer: Optional[bool] = None
    created_at: Optional[datetime] = Field(default_factory=datetime.utcnow)
    net_revenue: Optional[float] = None
    click_id: Optional[str] = None
    utm_source: Optional[str] = None
    utm_medium: Optional[str] = None
    utm_campaign: Optional[str] = None


class RawAdClicks(SQLModel, table=True):
    """Ad clicks by click_id for click-ID attribution (gclid, fbclid, etc.)."""

    __tablename__ = "raw_ad_clicks"
    id: Optional[int] = Field(default=None, primary_key=True)
    click_id: str = Field(index=True)
    date: date
    campaign_id: str
    campaign_name: Optional[str] = None
    channel: str  # meta, google, bing, pinterest
    created_at: Optional[datetime] = Field(default_factory=datetime.utcnow)


class IngestAudit(SQLModel, table=True):
    """Audit log for order reconciliation: computed_net_revenue, diff, note."""

    __tablename__ = "ingest_audit"
    id: Optional[int] = Field(default=None, primary_key=True)
    order_id: str = Field(index=True)
    computed_net_revenue: Optional[float] = None
    diff: Optional[float] = None
    created_at: Optional[datetime] = Field(default_factory=datetime.utcnow)
    note: Optional[str] = None


# ----- Attribution -----


class AttributionEvent(SQLModel, table=True):
    """Per-order attribution: weight and allocated revenue by channel/campaign."""

    __tablename__ = "attribution_events"
    id: Optional[int] = Field(default=None, primary_key=True)
    order_id: str
    channel: str
    campaign_id: Optional[str] = None
    cost_center: Optional[str] = None
    weight: float  # fractional or Markov share
    allocated_revenue: float
    event_date: date
    run_id: str

    def __init__(self, **data: Any) -> None:
        # Coerce numpy scalars to Python float so psycopg2 doesn't emit "np" as schema
        if "weight" in data and data["weight"] is not None:
            data["weight"] = float(data["weight"])
        if "allocated_revenue" in data and data["allocated_revenue"] is not None:
            data["allocated_revenue"] = float(data["allocated_revenue"])
        super().__init__(**data)


# ----- Unified metrics -----


class UnifiedDailyMetrics(SQLModel, table=True):
    """Aggregated daily per-channel metrics."""

    __tablename__ = "unified_daily_metrics"
    id: Optional[int] = Field(default=None, primary_key=True)
    date: date
    channel: str
    spend: float = 0.0
    attributed_revenue: float = 0.0
    roas: Optional[float] = None
    mer: Optional[float] = None
    cac: Optional[float] = None
    revenue_new: Optional[float] = None
    revenue_returning: Optional[float] = None
    created_at: Optional[datetime] = Field(default_factory=datetime.utcnow)


# ----- MMM results -----


class MMMResults(SQLModel, table=True):
    """MMM run: coefficients, saturation params, goodness of fit."""

    __tablename__ = "mmm_results"
    id: Optional[int] = Field(default=None, primary_key=True)
    run_id: str
    created_at: datetime = Field(default_factory=datetime.utcnow)
    channel: str
    coefficient: float
    saturation_half_life: Optional[float] = None
    saturation_alpha: Optional[float] = None  # Hill alpha
    goodness_of_fit_r2: Optional[float] = None
    model_version: Optional[str] = None


# ----- Decision store -----


class DecisionStore(SQLModel, table=True):
    """Decisions produced by rules engine."""

    __tablename__ = "decision_store"
    decision_id: str = Field(default_factory=lambda: str(uuid4()), primary_key=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    entity_type: str
    entity_id: str
    decision_type: str
    reason_code: str
    explanation_text: Optional[str] = None
    projected_impact: Optional[float] = None
    confidence_score: float = 0.0  # 0-1
    status: str = "pending"


# ----- Copilot sessions (for chat history and revisit) -----


class CopilotSession(SQLModel, table=True):
    """One chat session with the Copilot."""

    __tablename__ = "copilot_sessions"
    id: Optional[int] = Field(default=None, primary_key=True)
    title: Optional[str] = None  # optional; can set from first question
    created_at: datetime = Field(default_factory=datetime.utcnow)


class CopilotMessage(SQLModel, table=True):
    """One message in a Copilot session (user or assistant)."""

    __tablename__ = "copilot_messages"
    id: Optional[int] = Field(default=None, primary_key=True)
    session_id: int = Field(foreign_key="copilot_sessions.id")
    role: str  # "user" | "assistant"
    content: str
    created_at: datetime = Field(default_factory=datetime.utcnow)


# ----- Engine run metadata (persisted for readiness / history) -----


class EngineRunMetadataRecord(SQLModel, table=True):
    """Persisted engine run metadata (run_id, versions, timestamp)."""

    __tablename__ = "engine_run_metadata"
    id: Optional[int] = Field(default=None, primary_key=True)
    run_id: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    mta_version: str = ""
    mmm_version: str = ""
    data_snapshot_id: str = ""


# ----- Store config (minimal placeholder for rules) -----


class StoreConfig(SQLModel, table=True):
    """Config for rules: thresholds, scaling windows."""

    __tablename__ = "store_config"
    id: Optional[int] = Field(default=None, primary_key=True)
    key: str
    value_json: Optional[str] = None
    value_float: Optional[float] = None
    value_int: Optional[int] = None
    updated_at: Optional[datetime] = Field(default_factory=datetime.utcnow)
