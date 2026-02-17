"""FastAPI app: health, metrics, decisions, POST /run pipeline, MMM status/results, copilot."""
import os
from datetime import date, timedelta
from pathlib import Path

# Load .env so DATABASE_URL and GEMINI_* are set (try workspace root, then hypeon, then cwd)
from dotenv import load_dotenv
_app_dir = Path(__file__).resolve().parent
for _env_dir in [_app_dir.parent.parent.parent.parent, _app_dir.parent.parent.parent, Path.cwd()]:
    _env_file = _env_dir / ".env"
    if _env_file.exists():
        load_dotenv(_env_file)
        break

# Apply validated config to env so db/ingest and other consumers see it
from .config import get_settings
_settings = get_settings()
os.environ.setdefault("DATABASE_URL", _settings.database_url)
os.environ.setdefault("DATA_RAW_DIR", _settings.data_raw_dir)

import json

from fastapi import Depends, FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi import APIRouter
from sqlmodel import Session, select

from packages.shared.src.db import get_engine, get_session_fastapi
from packages.shared.src.models import (
    CopilotMessage,
    CopilotSession,
    DecisionStore,
    MMMResults,
    UnifiedDailyMetrics,
)
from packages.shared.src.schemas import (
    AttributionMMMReportResponse,
    BudgetAllocationResponse,
    CopilotAskRequest,
    CopilotAskResponse,
    CopilotContextResponse,
    CopilotMessageRow,
    CopilotMessagesResponse,
    CopilotSessionListItem,
    CopilotSessionsResponse,
    DecisionRow,
    DecisionsResponse,
    MMMResultRow,
    MMMResultsResponse,
    MMMStatusResponse,
    RunTriggerResponse,
    SimulateRequest,
    SimulateResponse,
    UnifiedMetricRow,
    UnifiedMetricsResponse,
)
from packages.shared.src.dates import parse_date_range
from packages.shared.src.ingest import run_ingest
from packages.attribution.src.runner import run_attribution, run_attribution_with_diagnostics
from packages.mmm.src.runner import run_mmm
from packages.metrics.src.aggregator import run_metrics
from packages.rules_engine.src.rules import run_rules
from packages.mmm.src.optimizer import (
    allocate_budget_greedy,
    predicted_revenue,
)
from packages.mmm.src.simulator import projected_revenue_delta
from packages.metrics.src.attribution_mmm_report import build_attribution_mmm_report
from packages.governance.src.metadata import record_run, get_latest_run
from packages.product_engine.src.reconciliation import compute_reconciliation
from packages.rules_engine.src.engine import enrich_decisions
from .envelope import envelope_success, envelope_error
from .middleware import get_correlation_id
from .copilot import generate_copilot_answer, get_copilot_context, stream_answer_with_gemini

# Last run diagnostics cache for /api/v1 (set only when pipeline runs via v1)
_last_mta_diagnostics: dict = {}
_last_mmm_diagnostics: dict = {}

app = FastAPI(title="HypeOn Product Engine API", version="1.0.0")


def _notify_pipeline_finished(run_id: str) -> None:
    """Notify SSE subscribers that a pipeline run finished."""
    loop = getattr(app.state, "event_loop", None)
    subs = getattr(app.state, "pipeline_subscribers", [])
    if loop and subs:
        msg = {"event": "pipeline_finished", "run_id": run_id}
        for q in subs:
            try:
                loop.call_soon_threadsafe(q.put_nowait, msg)
            except Exception:
                pass


@app.on_event("startup")
def ensure_copilot_tables():
    """Create copilot_sessions and copilot_messages if missing; init pipeline subscribers; start scheduler if configured."""
    engine = get_engine()
    for model in (CopilotSession, CopilotMessage):
        try:
            model.__table__.create(engine, checkfirst=True)
        except Exception:
            pass
    app.state.pipeline_subscribers = []
    interval = _settings.pipeline_run_interval_minutes
    if interval and interval > 0:
        from apscheduler.schedulers.background import BackgroundScheduler
        from packages.shared.src.db import get_session

        def scheduled_pipeline():
            try:
                with get_session() as session:
                    run_id = _run_pipeline(session, None, _default_data_dir())
                _notify_pipeline_finished(run_id)
            except Exception:
                pass

        scheduler = BackgroundScheduler()
        scheduler.add_job(scheduled_pipeline, "interval", minutes=interval)
        scheduler.start()
        app.state.scheduler = scheduler


from .middleware import CorrelationIdMiddleware, LoggingMiddleware, ApiKeyMiddleware

app.add_middleware(LoggingMiddleware)
app.add_middleware(CorrelationIdMiddleware)
app.add_middleware(ApiKeyMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=_settings.cors_origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(Exception)
def unhandled_exception_handler(request, exc):
    """Return 500 with error detail so frontend and logs show the real cause."""
    return JSONResponse(
        status_code=500,
        content={"detail": str(exc), "type": type(exc).__name__},
    )


def _current_spend_by_channel(session: Session, lookback_days: int = 30) -> dict:
    """Sum spend by channel over recent lookback from unified_daily_metrics."""
    start = date.today() - timedelta(days=lookback_days)
    end = date.today()
    stmt = select(UnifiedDailyMetrics).where(
        UnifiedDailyMetrics.date >= start,
        UnifiedDailyMetrics.date <= end,
    )
    rows = list(session.exec(stmt).all())
    by_ch: dict = {}
    for r in rows:
        by_ch[r.channel] = by_ch.get(r.channel, 0.0) + r.spend
    return by_ch or {"meta": 0.0, "google": 0.0}


def _latest_mmm_coefficients(session: Session) -> dict:
    """Latest MMM run coefficients by channel."""
    stmt = select(MMMResults).order_by(MMMResults.created_at.desc())
    rows = list(session.exec(stmt).all())
    if not rows:
        return {}
    rid = rows[0].run_id
    return {r.channel: r.coefficient for r in rows if r.run_id == rid}


def _health_db_ok() -> bool:
    """Lightweight DB readiness check (SELECT 1)."""
    from sqlalchemy import text
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


@app.get("/health")
def health():
    """Liveness and readiness: includes DB check."""
    db_ok = _health_db_ok()
    status = "ok" if db_ok else "degraded"
    if not db_ok:
        return JSONResponse(content={"status": status, "database": "unavailable"}, status_code=503)
    return {"status": status}


def _ensure_date(d) -> str:
    """Ensure date is JSON-serializable (ISO string)."""
    if hasattr(d, "isoformat"):
        return d.isoformat()
    if isinstance(d, str):
        return d
    return str(d)


@app.get("/metrics/unified")
def get_metrics_unified(
    session: Session = Depends(get_session_fastapi),
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
    channel: str | None = Query(None),
):
    """Query unified daily metrics (date range + optional channel filter)."""
    start, end = parse_date_range(start_date, end_date)
    stmt = select(UnifiedDailyMetrics).where(
        UnifiedDailyMetrics.date >= start,
        UnifiedDailyMetrics.date <= end,
    )
    if channel:
        stmt = stmt.where(UnifiedDailyMetrics.channel == channel)
    stmt = stmt.order_by(UnifiedDailyMetrics.date, UnifiedDailyMetrics.channel)
    rows = list(session.exec(stmt).all())
    # Build response with JSON-serializable types (dates as ISO strings for compatibility)
    metrics_data = [
        {
            "date": _ensure_date(r.date),
            "channel": str(r.channel),
            "spend": float(r.spend),
            "attributed_revenue": float(r.attributed_revenue),
            "roas": float(r.roas) if r.roas is not None else None,
            "mer": float(r.mer) if r.mer is not None else None,
            "cac": float(r.cac) if r.cac is not None else None,
            "revenue_new": float(r.revenue_new) if r.revenue_new is not None else None,
            "revenue_returning": float(r.revenue_returning) if r.revenue_returning is not None else None,
        }
        for r in rows
    ]
    return {
        "metrics": metrics_data,
        "start_date": _ensure_date(start),
        "end_date": _ensure_date(end),
    }


@app.get("/decisions", response_model=DecisionsResponse)
def list_decisions(
    session: Session = Depends(get_session_fastapi),
    status: str | None = Query(None, description="Filter by status"),
):
    """List decisions from decision_store; optional status filter."""
    stmt = select(DecisionStore)
    if status is not None:
        stmt = stmt.where(DecisionStore.status == status)
    stmt = stmt.order_by(DecisionStore.created_at.desc())
    rows = list(session.exec(stmt).all())
    return DecisionsResponse(
        decisions=[
            DecisionRow(
                decision_id=r.decision_id,
                created_at=r.created_at,
                entity_type=r.entity_type,
                entity_id=r.entity_id,
                decision_type=r.decision_type,
                reason_code=r.reason_code,
                explanation_text=r.explanation_text,
                projected_impact=r.projected_impact,
                confidence_score=r.confidence_score,
                status=r.status,
            )
            for r in rows
        ],
        total=len(rows),
    )


@app.get("/model/mmm/status", response_model=MMMStatusResponse)
def mmm_status(session: Session = Depends(get_session_fastapi)):
    """Last MMM run summary."""
    stmt = select(MMMResults).order_by(MMMResults.created_at.desc()).limit(1)
    r = session.exec(stmt).first()
    if not r:
        return MMMStatusResponse(status="no_runs")
    return MMMStatusResponse(
        last_run_id=r.run_id,
        last_run_at=r.created_at,
        status="completed",
    )


@app.get("/model/mmm/results", response_model=MMMResultsResponse)
def mmm_results(
    session: Session = Depends(get_session_fastapi),
    run_id: str | None = Query(None),
):
    """MMM results (optional run_id; else latest run)."""
    stmt = select(MMMResults).order_by(MMMResults.created_at.desc())
    if run_id:
        stmt = stmt.where(MMMResults.run_id == run_id)
    rows = list(session.exec(stmt).all())
    if not rows:
        return MMMResultsResponse(run_id=run_id, results=[])
    rid = rows[0].run_id
    by_run = [r for r in rows if r.run_id == rid]
    return MMMResultsResponse(
        run_id=rid,
        results=[
            MMMResultRow(
                run_id=r.run_id,
                created_at=r.created_at,
                channel=r.channel,
                coefficient=r.coefficient,
                goodness_of_fit_r2=r.goodness_of_fit_r2,
                model_version=r.model_version,
            )
            for r in by_run
        ],
    )


@app.post("/simulate", response_model=SimulateResponse)
def simulate(
    session: Session = Depends(get_session_fastapi),
    body: SimulateRequest = SimulateRequest(),
):
    """Projected revenue delta for given spend changes (e.g. meta +20%, google -10%)."""
    current = _current_spend_by_channel(session)
    spend_changes = {}
    if body.meta_spend_change is not None:
        spend_changes["meta"] = body.meta_spend_change
    if body.google_spend_change is not None:
        spend_changes["google"] = body.google_spend_change
    coefs = _latest_mmm_coefficients(session)
    if not coefs:
        return SimulateResponse(
            projected_revenue_delta=0.0,
            current_spend=current,
            new_spend=current,
        )
    delta = projected_revenue_delta(current, spend_changes, coefs)
    new_spend = {
        ch: current.get(ch, 0.0) * (1.0 + spend_changes.get(ch, 0.0))
        for ch in set(list(current.keys()) + list(spend_changes.keys()))
    }
    return SimulateResponse(
        projected_revenue_delta=round(delta, 2),
        current_spend=current,
        new_spend=new_spend,
    )


@app.get("/optimizer/budget", response_model=BudgetAllocationResponse)
def optimizer_budget(
    session: Session = Depends(get_session_fastapi),
    total_budget: float = Query(..., description="Total spend to allocate"),
):
    """Recommend channel allocation to maximize predicted revenue (greedy marginal ROAS)."""
    current = _current_spend_by_channel(session)
    coefs = _latest_mmm_coefficients(session)
    if not coefs:
        return BudgetAllocationResponse(
            total_budget=total_budget,
            recommended_allocation=current,
            current_spend=current,
            predicted_revenue_at_recommended=0.0,
        )
    recommended = allocate_budget_greedy(total_budget, coefs, current_spend=current)
    pred_rev = predicted_revenue(recommended, coefs)
    return BudgetAllocationResponse(
        total_budget=total_budget,
        recommended_allocation=recommended,
        current_spend=current,
        predicted_revenue_at_recommended=round(pred_rev, 2),
    )


@app.get("/report/attribution-mmm-comparison", response_model=AttributionMMMReportResponse)
def report_attribution_mmm(
    session: Session = Depends(get_session_fastapi),
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
):
    """Compare MTA attribution share vs MMM contribution share; flag instability if they disagree heavily."""
    start, end = parse_date_range(start_date, end_date)
    report = build_attribution_mmm_report(session, start, end)
    return AttributionMMMReportResponse(
        channels=report["channels"],
        attribution_share=report["attribution_share"],
        mmm_share=report["mmm_share"],
        disagreement_score=report["disagreement_score"],
        instability_flagged=report["instability_flagged"],
    )


def _run_pipeline(
    session: Session,
    seed: int | None,
    data_dir: Path | None,
) -> str:
    """Execute ingest -> attribution -> mmm -> metrics -> rules; return run_id."""
    import random
    if seed is not None:
        random.seed(seed)
    run_id = f"run-{seed if seed is not None else 'default'}"
    run_ingest(session, data_dir=data_dir)
    # Use 365-day lookback so sample data (e.g. Jan 2025) is included
    start = date.today() - timedelta(days=365)
    end = date.today()
    run_attribution(session, run_id=run_id, start_date=start, end_date=end)
    run_mmm(session, run_id=run_id, start_date=start, end_date=end)
    run_metrics(session, start_date=start, end_date=end, attribution_run_id=run_id)
    run_rules(session, start_date=start, end_date=end, mmm_run_id=run_id)
    record_run(run_id=run_id)
    return run_id


def _run_pipeline_v1(
    session: Session,
    seed: int | None,
    data_dir: Path | None,
) -> str:
    """Run pipeline with diagnostics; store MTA and MMM diagnostics for v1 endpoints."""
    import random
    global _last_mta_diagnostics, _last_mmm_diagnostics
    if seed is not None:
        random.seed(seed)
    run_id = f"run-{seed if seed is not None else 'default'}"
    run_ingest(session, data_dir=data_dir)
    start = date.today() - timedelta(days=365)
    end = date.today()
    _, mta_result = run_attribution_with_diagnostics(
        session, run_id=run_id, start_date=start, end_date=end
    )
    _last_mta_diagnostics.clear()
    _last_mta_diagnostics.update(mta_result)
    mmm_result = run_mmm(session, run_id=run_id, start_date=start, end_date=end)
    _last_mmm_diagnostics.clear()
    _last_mmm_diagnostics.update(mmm_result)
    run_metrics(session, start_date=start, end_date=end, attribution_run_id=run_id)
    run_rules(session, start_date=start, end_date=end, mmm_run_id=run_id)
    record_run(run_id=run_id)
    return run_id


# ----- API v1 (envelope) -----
router_v1 = APIRouter(prefix="/api/v1", tags=["v1"])


async def _stream_pipeline_events(request: Request):
    """SSE stream: emit pipeline_finished when a run completes (for UI auto-refresh)."""
    import asyncio
    app.state.event_loop = asyncio.get_running_loop()
    q = asyncio.Queue()
    app.state.pipeline_subscribers.append(q)
    try:
        while True:
            try:
                msg = await asyncio.wait_for(q.get(), timeout=60.0)
                yield f"data: {json.dumps(msg)}\n\n"
            except asyncio.TimeoutError:
                yield ": keepalive\n\n"
    finally:
        if q in app.state.pipeline_subscribers:
            app.state.pipeline_subscribers.remove(q)


@router_v1.get("/events/pipeline")
async def v1_events_pipeline(request: Request):
    """Server-Sent Events: pipeline_finished when a run completes. Use for dashboard auto-refresh."""
    return StreamingResponse(
        _stream_pipeline_events(request),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router_v1.get("/engine/health")
def v1_engine_health(request: Request):
    """Liveness/readiness. Returns envelope; 503 if DB unavailable."""
    meta = {"correlation_id": get_correlation_id(request)}
    db_ok = _health_db_ok()
    status = "ok" if db_ok else "degraded"
    if not db_ok:
        return JSONResponse(
            status_code=503,
            content=envelope_error(["database unavailable"], meta=meta),
        )
    return JSONResponse(content=envelope_success({"status": status}, meta=meta))


@router_v1.post("/engine/run")
def v1_engine_run(
    request: Request,
    session: Session = Depends(get_session_fastapi),
    seed: int | None = Query(None),
):
    """Run full pipeline with diagnostics; return envelope with run_id, timestamp, versions."""
    try:
        data_dir = _default_data_dir()
        run_id = _run_pipeline_v1(session, seed, data_dir)
        _notify_pipeline_finished(run_id)
        latest = get_latest_run()
        meta = {"correlation_id": get_correlation_id(request)}
        data = {
            "run_id": run_id,
            "timestamp": latest.timestamp.isoformat() if latest else None,
            "mta_version": latest.mta_version if latest else None,
            "mmm_version": latest.mmm_version if latest else None,
            "data_snapshot_id": latest.data_snapshot_id if latest else None,
        }
        return JSONResponse(content=envelope_success(data, meta=meta))
    except Exception as e:
        meta = {"correlation_id": get_correlation_id(request)}
        return JSONResponse(
            status_code=500,
            content=envelope_error([str(e)], meta=meta),
        )


@router_v1.get("/mta/diagnostics")
def v1_mta_diagnostics(request: Request):
    """MTA diagnostics from last v1 engine run. Returns envelope."""
    meta = {"correlation_id": get_correlation_id(request)}
    return JSONResponse(content=envelope_success(_last_mta_diagnostics, meta=meta))


@router_v1.get("/mmm/diagnostics")
def v1_mmm_diagnostics(request: Request):
    """MMM diagnostics from last v1 engine run. Returns envelope."""
    meta = {"correlation_id": get_correlation_id(request)}
    return JSONResponse(content=envelope_success(_last_mmm_diagnostics, meta=meta))


@router_v1.get("/reconciliation")
def v1_reconciliation(
    request: Request,
    session: Session = Depends(get_session_fastapi),
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
):
    """MTA vs MMM alignment. Returns envelope."""
    try:
        start, end = parse_date_range(start_date, end_date)
        report = build_attribution_mmm_report(session, start, end)
        rec = compute_reconciliation(
            report["attribution_share"],
            report["mmm_share"],
            alignment_confidence=1.0,
        )
        meta = {"correlation_id": get_correlation_id(request)}
        return JSONResponse(content=envelope_success(rec, meta=meta))
    except Exception as e:
        meta = {"correlation_id": get_correlation_id(request)}
        return JSONResponse(status_code=500, content=envelope_error([str(e)], meta=meta))


@router_v1.get("/decisions")
def v1_decisions(
    request: Request,
    session: Session = Depends(get_session_fastapi),
    status: str | None = Query(None),
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
):
    """Decisions with reasoning, risk_flags, model_versions, run_id. Returns envelope."""
    try:
        stmt = select(DecisionStore)
        if status is not None:
            stmt = stmt.where(DecisionStore.status == status)
        stmt = stmt.order_by(DecisionStore.created_at.desc())
        rows = list(session.exec(stmt).all())
        latest = get_latest_run()
        start_r, end_r = parse_date_range(start_date, end_date, default_days=365)
        report = build_attribution_mmm_report(session, start_r, end_r)
        rec = compute_reconciliation(report["attribution_share"], report["mmm_share"])
        mta_conf = _last_mta_diagnostics.get("confidence_score", 0.5) if _last_mta_diagnostics else 0.5
        mmm_conf = _last_mmm_diagnostics.get("confidence_score", 0.5) if _last_mmm_diagnostics else 0.5
        enriched = enrich_decisions(
            rows,
            run_id=latest.run_id if latest else None,
            mta_version=latest.mta_version if latest else None,
            mmm_version=latest.mmm_version if latest else None,
            mta_confidence=mta_conf,
            mmm_confidence=mmm_conf,
            alignment_score=rec["overall_alignment_score"],
            alignment_result=rec,
        )
        meta = {"correlation_id": get_correlation_id(request)}
        return JSONResponse(content=envelope_success({"decisions": enriched, "total": len(enriched)}, meta=meta))
    except Exception as e:
        meta = {"correlation_id": get_correlation_id(request)}
        return JSONResponse(status_code=500, content=envelope_error([str(e)], meta=meta))


@router_v1.get("/model-info")
def v1_model_info(request: Request):
    """Aggregate model versions and last run metadata. Returns envelope."""
    latest = get_latest_run()
    meta = {"correlation_id": get_correlation_id(request)}
    data = {}
    if latest:
        data = {
            "run_id": latest.run_id,
            "timestamp": latest.timestamp.isoformat(),
            "mta_version": latest.mta_version,
            "mmm_version": latest.mmm_version,
            "data_snapshot_id": latest.data_snapshot_id,
        }
    return JSONResponse(content=envelope_success(data, meta=meta))


app.include_router(router_v1)


# ----- Copilot (for founders / non-technical) -----


@app.get("/copilot/context", response_model=CopilotContextResponse)
def copilot_context(
    session: Session = Depends(get_session_fastapi),
    lookback_days: int = Query(90, ge=7, le=365),
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
):
    """Summary of current data used by Copilot (dashboard-aligned). Optional start_date/end_date."""
    ctx = get_copilot_context(
        session, lookback_days=lookback_days, start_date=start_date, end_date=end_date
    )
    report = ctx.get("attribution_mmm_report") or {}
    return CopilotContextResponse(
        start_date=ctx.get("start_date"),
        end_date=ctx.get("end_date"),
        lookback_days=ctx.get("lookback_days", 90),
        channels=ctx.get("channels", []),
        total_spend=ctx.get("total_spend", 0),
        total_revenue=ctx.get("total_revenue", 0),
        roas_overall=ctx.get("roas_overall", 0),
        decisions_total=ctx.get("decisions_total", 0),
        decisions_pending=ctx.get("decisions_pending", 0),
        mmm_last_run_id=ctx.get("mmm_last_run_id"),
        instability_flagged=report.get("instability_flagged", False),
    )


@app.get("/copilot/sessions", response_model=CopilotSessionsResponse)
def copilot_list_sessions(session: Session = Depends(get_session_fastapi)):
    """List all copilot sessions (newest first)."""
    stmt = select(CopilotSession).order_by(CopilotSession.created_at.desc()).limit(100)
    rows = list(session.exec(stmt).all())
    return CopilotSessionsResponse(
        sessions=[
            CopilotSessionListItem(id=r.id, title=r.title, created_at=r.created_at)
            for r in rows
        ]
    )


@app.post("/copilot/sessions", response_model=CopilotSessionListItem)
def copilot_create_session(session: Session = Depends(get_session_fastapi)):
    """Create a new copilot session."""
    s = CopilotSession()
    session.add(s)
    session.commit()
    session.refresh(s)
    return CopilotSessionListItem(id=s.id, title=s.title, created_at=s.created_at)


@app.get("/copilot/sessions/{session_id:int}/messages", response_model=CopilotMessagesResponse)
def copilot_get_messages(
    session_id: int,
    session: Session = Depends(get_session_fastapi),
):
    """Get all messages in a session (chronological)."""
    stmt = select(CopilotMessage).where(CopilotMessage.session_id == session_id).order_by(CopilotMessage.created_at)
    rows = list(session.exec(stmt).all())
    return CopilotMessagesResponse(
        session_id=session_id,
        messages=[
            CopilotMessageRow(id=m.id, role=m.role, content=m.content, created_at=m.created_at)
            for m in rows
        ],
    )


def _copilot_ensure_session(session: Session, session_id: int | None):
    """Create a session if session_id is None; return session id."""
    if session_id is not None:
        return session_id
    s = CopilotSession()
    session.add(s)
    session.commit()
    session.refresh(s)
    return s.id


def _get_versioned_copilot_context(session: Session) -> dict:
    """Build versioned context for Copilot: run_id, versions, stability, confidence, alignment (precomputed only)."""
    latest = get_latest_run()
    out = {
        "run_id": latest.run_id if latest else None,
        "mta_version": latest.mta_version if latest else None,
        "mmm_version": latest.mmm_version if latest else None,
        "stability_index": _last_mmm_diagnostics.get("stability_index"),
        "mta_confidence": _last_mta_diagnostics.get("confidence_score"),
        "mmm_confidence": _last_mmm_diagnostics.get("confidence_score"),
        "alignment_score": None,
    }
    try:
        start, end = parse_date_range(None, None, default_days=365)
        report = build_attribution_mmm_report(session, start, end)
        mta_share = report.get("attribution_share") or {}
        mmm_share = report.get("mmm_share") or {}
        recon = compute_reconciliation(mta_share, mmm_share)
        out["alignment_score"] = recon.get("overall_alignment_score")
    except Exception:
        pass
    return out


def _copilot_session_history(session: Session, session_id: int) -> list[dict[str, str]]:
    """Load previous messages in a copilot session for conversation context (user + assistant pairs)."""
    stmt = select(CopilotMessage).where(CopilotMessage.session_id == session_id).order_by(CopilotMessage.created_at)
    rows = list(session.exec(stmt).all())
    return [{"role": m.role, "content": m.content or ""} for m in rows]


def _parse_copilot_dates(start_date: str | None, end_date: str | None) -> tuple[date | None, date | None]:
    """Parse optional YYYY-MM-DD strings to dates for Copilot context."""
    if not start_date or not end_date:
        return None, None
    try:
        return date.fromisoformat(start_date), date.fromisoformat(end_date)
    except (ValueError, TypeError):
        return None, None


@app.post("/copilot/ask", response_model=CopilotAskResponse)
def copilot_ask(
    session: Session = Depends(get_session_fastapi),
    body: CopilotAskRequest = CopilotAskRequest(question=""),
):
    """Answer a natural-language question using dashboard data. Optionally save to a session."""
    question = (body.question or "").strip() or "How are we doing?"
    sid = body.session_id if body.session_id is not None else _copilot_ensure_session(session, None)
    versioned = _get_versioned_copilot_context(session)
    history = _copilot_session_history(session, sid)
    start_d, end_d = _parse_copilot_dates(body.start_date, body.end_date)
    answer, sources, model_versions_used = generate_copilot_answer(
        session, question, versioned, conversation_history=history, start_date=start_d, end_date=end_d
    )
    # Persist to session
    user_msg = CopilotMessage(session_id=sid, role="user", content=question)
    session.add(user_msg)
    session.commit()
    session.refresh(user_msg)
    assistant_msg = CopilotMessage(session_id=sid, role="assistant", content=answer)
    session.add(assistant_msg)
    session.commit()
    session.refresh(assistant_msg)
    # Optionally set session title from first question
    s = session.get(CopilotSession, sid)
    if s and not s.title:
        s.title = (question[:50] + "…") if len(question) > 50 else question
        session.add(s)
        session.commit()
    return CopilotAskResponse(
        answer=answer,
        sources=sources,
        model_versions_used=model_versions_used,
        session_id=sid,
        message_id=assistant_msg.id,
    )


@app.post("/copilot/ask/stream")
def copilot_ask_stream(
    session: Session = Depends(get_session_fastapi),
    body: CopilotAskRequest = CopilotAskRequest(question=""),
):
    """Stream answer as SSE. Uses session message history for follow-up questions. Dashboard data fetched when needed."""
    question = (body.question or "").strip() or "How are we doing?"
    sid = body.session_id if body.session_id is not None else _copilot_ensure_session(session, None)
    start_d, end_d = _parse_copilot_dates(body.start_date, body.end_date)
    if start_d is not None and end_d is not None:
        ctx = get_copilot_context(session, start_date=start_d, end_date=end_d)
    else:
        ctx = get_copilot_context(session, lookback_days=90)
    versioned = _get_versioned_copilot_context(session)
    history = _copilot_session_history(session, sid)

    def event_stream():
        full = []
        sources_list = []
        model_versions_used = None
        # Save user message
        user_msg = CopilotMessage(session_id=sid, role="user", content=question)
        session.add(user_msg)
        session.commit()
        session.refresh(user_msg)
        for delta, sources, mv in stream_answer_with_gemini(
            question, ctx, versioned, conversation_history=history
        ):
            if delta:
                full.append(delta)
                yield f"data: {json.dumps({'delta': delta})}\n\n"
            if sources is not None:
                sources_list = sources
            if mv is not None:
                model_versions_used = mv
            if sources is not None:
                payload = {"done": True, "sources": sources, "answer": "".join(full)}
                if model_versions_used is not None:
                    payload["model_versions_used"] = model_versions_used
                yield f"data: {json.dumps(payload)}\n\n"
        # Persist assistant message
        answer_text = "".join(full)
        assistant_msg = CopilotMessage(session_id=sid, role="assistant", content=answer_text)
        session.add(assistant_msg)
        session.commit()
        session.refresh(assistant_msg)
        s = session.get(CopilotSession, sid)
        if s and not s.title:
            s.title = (question[:50] + "…") if len(question) > 50 else question
            session.add(s)
            session.commit()

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


def _default_data_dir() -> Path:
    """Resolve data/raw from repo root (hypeon), regardless of process cwd."""
    # app.py lives at hypeon/apps/api/src/app.py -> repo root is 4 levels up
    repo_root = Path(__file__).resolve().parent.parent.parent.parent
    return repo_root / "data" / "raw"


@app.post("/run", response_model=RunTriggerResponse, status_code=202)
def trigger_run(
    session: Session = Depends(get_session_fastapi),
    seed: int | None = Query(None, description="Deterministic run seed"),
):
    """Trigger idempotent product-engine pipeline: ingest -> attribution -> mmm -> metrics -> rules."""
    data_dir = _default_data_dir()
    run_id = _run_pipeline(session, seed, data_dir)
    return RunTriggerResponse(run_id=run_id, status="accepted", message="Pipeline run triggered.")


@app.post("/run/sync", response_model=RunTriggerResponse)
def trigger_run_sync(
    session: Session = Depends(get_session_fastapi),
    seed: int | None = Query(None, description="Deterministic run seed"),
):
    """Run pipeline synchronously; returns when done (for UI 'Run pipeline' button)."""
    data_dir = _default_data_dir()
    run_id = _run_pipeline(session, seed, data_dir)
    _notify_pipeline_finished(run_id)
    return RunTriggerResponse(run_id=run_id, status="completed", message="Pipeline completed.")


# ----- Serve frontend static at / when dist exists (production Docker); API routes take precedence -----
_static_dir = Path(__file__).resolve().parent.parent.parent.parent / "apps" / "web" / "dist"
if _static_dir.exists():
    from fastapi.staticfiles import StaticFiles
    app.mount("/", StaticFiles(directory=str(_static_dir), html=True), name="static")
