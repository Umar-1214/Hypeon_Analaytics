"""
Attribution vs MMM comparison report: compare MTA-attributed share by channel
vs MMM-implied contribution share. Flag instability when they disagree heavily.
"""
from datetime import date
from typing import Dict, List, Optional, Tuple

from sqlmodel import Session, select

from packages.shared.src.models import (
    AttributionEvent,
    MMMResults,
    RawMetaAds,
    RawGoogleAds,
    RawBingAds,
    RawPinterestAds,
)
from packages.mmm.src.optimizer import _response_single_channel

DEFAULT_ADSTOCK_HALF_LIFE = 7.0
INSTABILITY_THRESHOLD = 0.25  # sum of absolute share differences above this => flag


def _attribution_revenue_by_channel(
    session: Session,
    start_date: date,
    end_date: date,
    run_id: Optional[str] = None,
) -> Dict[str, float]:
    """Total attributed revenue per channel in date range."""
    stmt = select(AttributionEvent).where(
        AttributionEvent.event_date >= start_date,
        AttributionEvent.event_date <= end_date,
    )
    if run_id:
        stmt = stmt.where(AttributionEvent.run_id == run_id)
    rows = list(session.exec(stmt).all())
    by_ch: Dict[str, float] = {}
    for r in rows:
        by_ch[r.channel] = by_ch.get(r.channel, 0.0) + r.allocated_revenue
    return by_ch


def _spend_by_channel(session: Session, start_date: date, end_date: date) -> Dict[str, float]:
    """Total spend per channel in date range."""
    by_ch: Dict[str, float] = {}
    for r in session.exec(
        select(RawMetaAds).where(RawMetaAds.date >= start_date, RawMetaAds.date <= end_date)
    ).all():
        by_ch["meta"] = by_ch.get("meta", 0.0) + r.spend
    for r in session.exec(
        select(RawGoogleAds).where(RawGoogleAds.date >= start_date, RawGoogleAds.date <= end_date)
    ).all():
        by_ch["google"] = by_ch.get("google", 0.0) + r.spend
    for r in session.exec(
        select(RawBingAds).where(RawBingAds.date >= start_date, RawBingAds.date <= end_date)
    ).all():
        by_ch["bing"] = by_ch.get("bing", 0.0) + r.spend
    for r in session.exec(
        select(RawPinterestAds).where(RawPinterestAds.date >= start_date, RawPinterestAds.date <= end_date)
    ).all():
        by_ch["pinterest"] = by_ch.get("pinterest", 0.0) + r.spend
    return by_ch


def _mmm_contribution_share(
    session: Session,
    spend_by_channel: Dict[str, float],
    mmm_run_id: Optional[str] = None,
    half_life: float = DEFAULT_ADSTOCK_HALF_LIFE,
) -> Tuple[Dict[str, float], Dict[str, float]]:
    """
    Get latest MMM coefficients, compute contribution = coef_j * response(spend_j), return shares.
    Returns (contribution_absolute, share).
    """
    stmt = select(MMMResults).order_by(MMMResults.created_at.desc())
    if mmm_run_id:
        stmt = stmt.where(MMMResults.run_id == mmm_run_id)
    rows = list(session.exec(stmt).all())
    if not rows:
        return {}, {}
    by_channel: Dict[str, float] = {}
    for r in rows:
        if r.channel not in by_channel:
            by_channel[r.channel] = r.coefficient
    contribution = {}
    for ch, coef in by_channel.items():
        spend = spend_by_channel.get(ch, 0.0)
        resp = _response_single_channel(spend, half_life)
        contribution[ch] = coef * resp
    total = sum(contribution.values()) or 1.0
    share = {ch: v / total for ch, v in contribution.items()}
    return contribution, share


def build_attribution_mmm_report(
    session: Session,
    start_date: date,
    end_date: date,
    attribution_run_id: Optional[str] = None,
    mmm_run_id: Optional[str] = None,
    instability_threshold: float = INSTABILITY_THRESHOLD,
) -> Dict:
    """
    Returns dict: attribution_share, mmm_share, disagreement_score (sum |attr_share - mmm_share|),
    instability_flagged (True if disagreement_score > threshold), channels.
    """
    attr_rev = _attribution_revenue_by_channel(
        session, start_date, end_date, run_id=attribution_run_id
    )
    total_attr = sum(attr_rev.values()) or 1.0
    attribution_share = {ch: v / total_attr for ch, v in attr_rev.items()}
    spend_by_ch = _spend_by_channel(session, start_date, end_date)
    _, mmm_share = _mmm_contribution_share(
        session, spend_by_ch, mmm_run_id=mmm_run_id
    )
    channels = sorted(set(list(attribution_share.keys()) + list(mmm_share.keys())))
    disagreement = 0.0
    for ch in channels:
        a = attribution_share.get(ch, 0.0)
        m = mmm_share.get(ch, 0.0)
        disagreement += abs(a - m)
    disagreement_score = disagreement
    instability_flagged = disagreement_score > instability_threshold
    return {
        "channels": channels,
        "attribution_share": attribution_share,
        "mmm_share": mmm_share,
        "disagreement_score": round(disagreement_score, 4),
        "instability_flagged": instability_flagged,
    }
