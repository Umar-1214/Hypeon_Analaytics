"""Unified daily metrics: aggregate spend, attributed revenue, ROAS, MER, CAC, new/returning splits."""
from datetime import date
from typing import List, Optional

import pandas as pd
from sqlmodel import Session, select

from packages.shared.src.models import (
    AttributionEvent,
    RawMetaAds,
    RawGoogleAds,
    RawBingAds,
    RawPinterestAds,
    UnifiedDailyMetrics,
)


def _spend_by_date_channel(session: Session, start: date, end: date) -> pd.DataFrame:
    rows = []
    for r in session.exec(
        select(RawMetaAds).where(RawMetaAds.date >= start, RawMetaAds.date <= end)
    ).all():
        rows.append({"date": r.date, "channel": "meta", "spend": r.spend})
    for r in session.exec(
        select(RawGoogleAds).where(RawGoogleAds.date >= start, RawGoogleAds.date <= end)
    ).all():
        rows.append({"date": r.date, "channel": "google", "spend": r.spend})
    for r in session.exec(
        select(RawBingAds).where(RawBingAds.date >= start, RawBingAds.date <= end)
    ).all():
        rows.append({"date": r.date, "channel": "bing", "spend": r.spend})
    for r in session.exec(
        select(RawPinterestAds).where(RawPinterestAds.date >= start, RawPinterestAds.date <= end)
    ).all():
        rows.append({"date": r.date, "channel": "pinterest", "spend": r.spend})
    if not rows:
        return pd.DataFrame(columns=["date", "channel", "spend"])
    df = pd.DataFrame(rows)
    return df.groupby(["date", "channel"], as_index=False).agg({"spend": "sum"})


def _attributed_revenue_by_date_channel(
    session: Session, start: date, end: date, run_id: Optional[str] = None
) -> pd.DataFrame:
    stmt = select(AttributionEvent).where(
        AttributionEvent.event_date >= start,
        AttributionEvent.event_date <= end,
    )
    if run_id:
        stmt = stmt.where(AttributionEvent.run_id == run_id)
    rows = []
    for r in session.exec(stmt).all():
        rows.append({
            "date": r.event_date,
            "channel": r.channel,
            "allocated_revenue": r.allocated_revenue,
        })
    if not rows:
        return pd.DataFrame(columns=["date", "channel", "allocated_revenue"])
    df = pd.DataFrame(rows)
    return df.groupby(["date", "channel"], as_index=False).agg({"allocated_revenue": "sum"})


def compute_unified_metrics(
    session: Session,
    start_date: date,
    end_date: date,
    attribution_run_id: Optional[str] = None,
) -> List[UnifiedDailyMetrics]:
    """
    Aggregate spend and attributed revenue by (date, channel), compute ROAS/MER/CAC and new/returning.
    Returns list of UnifiedDailyMetrics to upsert.
    """
    spend_df = _spend_by_date_channel(session, start_date, end_date)
    rev_df = _attributed_revenue_by_date_channel(
        session, start_date, end_date, run_id=attribution_run_id
    )
    if spend_df.empty and rev_df.empty:
        return []
    merged = spend_df.merge(
        rev_df, on=["date", "channel"], how="outer"
    ).fillna(0)
    if "allocated_revenue" not in merged.columns:
        merged["allocated_revenue"] = 0.0
    if "spend" not in merged.columns:
        merged["spend"] = 0.0
    out = []
    for _, row in merged.iterrows():
        d = row["date"]
        ch = row["channel"]
        spend = float(row["spend"])
        attr_rev = float(row["allocated_revenue"])
        roas = attr_rev / spend if spend > 0 else None
        total_rev = attr_rev
        mer = (total_rev / spend) if spend > 0 else None
        cac = (spend / 1) if total_rev > 0 else None
        out.append(
            UnifiedDailyMetrics(
                date=d,
                channel=ch,
                spend=spend,
                attributed_revenue=attr_rev,
                roas=roas,
                mer=mer,
                cac=cac,
                revenue_new=None,
                revenue_returning=None,
            )
        )
    return out


def run_metrics(
    session: Session,
    start_date: date,
    end_date: date,
    attribution_run_id: Optional[str] = None,
) -> int:
    """Compute unified metrics and write into unified_daily_metrics (replace range). Returns row count."""
    from sqlalchemy import delete
    session.execute(delete(UnifiedDailyMetrics).where(
        UnifiedDailyMetrics.date >= start_date,
        UnifiedDailyMetrics.date <= end_date,
    ))
    records = compute_unified_metrics(
        session, start_date, end_date, attribution_run_id=attribution_run_id
    )
    for rec in records:
        session.add(rec)
    session.commit()
    return len(records)
