"""Pydantic/schema DTOs for API and shared use."""
from datetime import date, datetime
from typing import List, Optional

from pydantic import BaseModel


# ----- API: metrics -----


class MetricsQueryParams(BaseModel):
    """Query params for /metrics/unified."""

    start_date: Optional[date] = None
    end_date: Optional[date] = None
    channel: Optional[str] = None


class UnifiedMetricRow(BaseModel):
    """One row of unified daily metrics."""

    date: date
    channel: str
    spend: float
    attributed_revenue: float
    roas: Optional[float] = None
    mer: Optional[float] = None
    cac: Optional[float] = None
    revenue_new: Optional[float] = None
    revenue_returning: Optional[float] = None


class UnifiedMetricsResponse(BaseModel):
    """Response for GET /metrics/unified."""

    metrics: List[UnifiedMetricRow]
    start_date: Optional[date] = None
    end_date: Optional[date] = None


# ----- API: decisions -----


class DecisionRow(BaseModel):
    """One decision from decision_store."""

    decision_id: str
    created_at: datetime
    entity_type: str
    entity_id: str
    decision_type: str
    reason_code: str
    explanation_text: Optional[str] = None
    projected_impact: Optional[float] = None
    confidence_score: float
    status: str


class DecisionsResponse(BaseModel):
    """Response for GET /decisions."""

    decisions: List[DecisionRow]
    total: int


# ----- API: MMM -----


class MMMResultRow(BaseModel):
    """One MMM result row."""

    run_id: str
    created_at: datetime
    channel: str
    coefficient: float
    goodness_of_fit_r2: Optional[float] = None
    model_version: Optional[str] = None


class MMMStatusResponse(BaseModel):
    """Response for GET /model/mmm/status."""

    last_run_id: Optional[str] = None
    last_run_at: Optional[datetime] = None
    status: str = "unknown"


class MMMResultsResponse(BaseModel):
    """Response for GET /model/mmm/results."""

    run_id: Optional[str] = None
    results: List[MMMResultRow]


# ----- API: run -----


class RunTriggerResponse(BaseModel):
    """Response for POST /run."""

    run_id: str
    status: str = "accepted"
    message: str = "Pipeline run triggered."


# ----- API: simulate -----


class SimulateRequest(BaseModel):
    """Body for POST /simulate: fractional spend changes per channel."""

    meta_spend_change: Optional[float] = None
    google_spend_change: Optional[float] = None


class SimulateResponse(BaseModel):
    """Response for POST /simulate."""

    projected_revenue_delta: float
    current_spend: dict
    new_spend: dict


# ----- API: budget optimizer -----


class BudgetAllocationResponse(BaseModel):
    """Response for GET /optimizer/budget."""

    total_budget: float
    recommended_allocation: dict
    current_spend: dict
    predicted_revenue_at_recommended: float


# ----- API: attribution vs MMM report -----


class AttributionMMMReportResponse(BaseModel):
    """Response for GET /report/attribution-mmm-comparison."""

    channels: List[str]
    attribution_share: dict
    mmm_share: dict
    disagreement_score: float
    instability_flagged: bool


# ----- API: copilot -----


class CopilotContextResponse(BaseModel):
    """Response for GET /copilot/context: summary of data used for answers."""

    start_date: Optional[str] = None
    end_date: Optional[str] = None
    lookback_days: int = 30
    channels: List[str] = []
    total_spend: float = 0.0
    total_revenue: float = 0.0
    roas_overall: float = 0.0
    decisions_total: int = 0
    decisions_pending: int = 0
    mmm_last_run_id: Optional[str] = None
    instability_flagged: bool = False


class CopilotAskRequest(BaseModel):
    """Body for POST /copilot/ask and /copilot/ask/stream."""

    question: str
    session_id: Optional[int] = None
    start_date: Optional[str] = None  # YYYY-MM-DD; when set with end_date, use this range for context (matches "Data in scope")
    end_date: Optional[str] = None


class CopilotRecommendation(BaseModel):
    """One actionable recommendation from Copilot v2."""

    action: str  # e.g. reduce_budget, scale_up, reallocate
    entity: str  # e.g. channel/campaign name
    reason: str
    confidence: float = 0.0
    expected_impact: Optional[str] = None
    decision_id: Optional[str] = None


class CopilotRiskItem(BaseModel):
    """One risk item from Copilot v2."""

    title: str
    description: str
    confidence: float = 0.0
    entity_id: Optional[str] = None


class CopilotOpportunityItem(BaseModel):
    """One opportunity item from Copilot v2."""

    title: str
    description: str
    confidence: float = 0.0
    entity_id: Optional[str] = None
    expected_impact: Optional[str] = None


class CopilotAskResponse(BaseModel):
    """Response for POST /copilot/ask. answer/sources kept for backward compatibility."""

    answer: str
    sources: List[str] = []
    model_versions_used: Optional[dict] = None  # { mta_version, mmm_version }
    session_id: Optional[int] = None
    message_id: Optional[int] = None
    # Structured decision output (v2)
    recommendations: List[CopilotRecommendation] = []
    risks: List[CopilotRiskItem] = []
    opportunities: List[CopilotOpportunityItem] = []


class CopilotSessionListItem(BaseModel):
    id: int
    title: Optional[str] = None
    created_at: datetime


class CopilotSessionsResponse(BaseModel):
    sessions: List[CopilotSessionListItem]


class CopilotMessageRow(BaseModel):
    id: int
    role: str
    content: str
    created_at: datetime


class CopilotMessagesResponse(BaseModel):
    session_id: int
    messages: List[CopilotMessageRow]


class DecisionStatusUpdateRequest(BaseModel):
    """Body for POST /copilot/decision/{decision_id}/status."""

    status: str


# ----- Shared DTOs -----


class DateRange(BaseModel):
    """Date range filter."""

    start_date: date
    end_date: date
