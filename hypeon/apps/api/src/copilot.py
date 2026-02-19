"""
Copilot: answer natural-language questions using dashboard data.
Designed for founders and non-technical users; uses metrics, decisions, MMM, and reports.
v2: Intent routing, decision aggregator, structured recommendations. Never fabricate metrics;
only operate on prepared intelligence (DecisionStore, MMM, attribution report, unified metrics).
"""
from datetime import date, timedelta
import json
import os
import re
from pathlib import Path
from typing import Any, Optional

from sqlmodel import Session, select

from packages.shared.src.models import (
    DecisionStore,
    MMMResults,
    UnifiedDailyMetrics,
)
from packages.shared.src.dates import parse_date_range
from packages.metrics.src.attribution_mmm_report import build_attribution_mmm_report

from .copilot_intent_router import CopilotIntent, classify_intent
from .copilot_decision_engine import build_decision_context

_COPILOT_PROMPTS_DIR = Path(__file__).resolve().parent / "copilot_prompts"
_MAX_COPILOT_TOKENS = 512


def _load_copilot_templates() -> tuple[str, str]:
    """Load system and context_slot templates. Return (system_txt, context_slot_txt)."""
    system_path = _COPILOT_PROMPTS_DIR / "system.txt"
    slot_path = _COPILOT_PROMPTS_DIR / "context_slot.txt"
    system_txt = system_path.read_text(encoding="utf-8") if system_path.exists() else ""
    slot_txt = slot_path.read_text(encoding="utf-8") if slot_path.exists() else ""
    return system_txt, slot_txt


def _build_prompt_from_templates(
    question: str,
    ctx: dict[str, Any],
    versioned_context: Optional[dict[str, Any]] = None,
    conversation_history: Optional[list[dict[str, str]]] = None,
) -> str:
    """Build LLM prompt from templates; inject versioned context, decisions, and optional conversation history."""
    system_txt, slot_txt = _load_copilot_templates()
    decisions = ctx.get("decisions")
    decisions_json = json.dumps(decisions or {}, indent=2)
    risks_json = json.dumps((decisions or {}).get("risk_campaigns", []), indent=2)
    opportunities_json = json.dumps((decisions or {}).get("top_opportunities", []), indent=2)
    confidence_json = json.dumps((decisions or {}).get("confidence_summary", {}), indent=2)
    data_json = json.dumps(ctx, indent=2)

    if not slot_txt or versioned_context is None:
        hist = ""
        if conversation_history:
            hist = "\n\nConversation so far:\n" + "\n".join(
                f"{'User' if m.get('role') == 'user' else 'Assistant'}: {m.get('content', '')}"
                for m in conversation_history
            ) + "\n\n"
        return (
            "You are an analytics decision assistant. All analytical reasoning has already been computed. "
            "Use ONLY the data below. Never invent or compute metrics. Only explain, summarize, format, and prioritize.\n\n"
            f"Data:\n{data_json}\n\n"
            f"{hist}Current question: {question}\n\nAnswer:"
        )
    run_id = versioned_context.get("run_id") or "—"
    mta_version = versioned_context.get("mta_version") or "—"
    mmm_version = versioned_context.get("mmm_version") or "—"
    stability_index = versioned_context.get("stability_index")
    stability_str = f"{stability_index:.2f}" if stability_index is not None else "—"
    mta_conf = versioned_context.get("mta_confidence")
    mta_conf_str = f"{mta_conf:.2f}" if mta_conf is not None else "—"
    mmm_conf = versioned_context.get("mmm_confidence")
    mmm_conf_str = f"{mmm_conf:.2f}" if mmm_conf is not None else "—"
    align = versioned_context.get("alignment_score")
    align_str = f"{align:.2f}" if align is not None else "—"
    conversation_block = ""
    if conversation_history:
        conversation_block = "\n\nConversation so far:\n" + "\n".join(
            f"{'User' if m.get('role') == 'user' else 'Assistant'}: {m.get('content', '')}"
            for m in conversation_history
        ) + "\n\n"
    current_question = f"Current question: {question}"
    filled = (
        slot_txt.replace("{{ run_id }}", str(run_id))
        .replace("{{ mta_version }}", str(mta_version))
        .replace("{{ mmm_version }}", str(mmm_version))
        .replace("{{ stability_index }}", stability_str)
        .replace("{{ mta_confidence }}", mta_conf_str)
        .replace("{{ mmm_confidence }}", mmm_conf_str)
        .replace("{{ alignment_score }}", align_str)
        .replace("{{ decisions_json }}", decisions_json)
        .replace("{{ risks_json }}", risks_json)
        .replace("{{ opportunities_json }}", opportunities_json)
        .replace("{{ confidence_json }}", confidence_json)
        .replace("{{ data_json }}", data_json)
        .replace("{{ question }}", conversation_block + current_question)
    )
    return system_txt.strip() + "\n\n" + filled


def get_copilot_context(
    session: Session,
    lookback_days: int = 90,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    _extended: bool = False,
) -> dict[str, Any]:
    """
    Build a summary of current data for Copilot answers (same data as dashboard).
    If start_date/end_date are provided, use that range; else use lookback_days from today.
    When no explicit range is given and the default window has no data, retries with 365-day
    lookback so sample/historical data (e.g. from 2025-01-01) is included.
    """
    if start_date is not None and end_date is not None:
        start, end = start_date, end_date
    else:
        start, end = parse_date_range(
            start=date.today() - timedelta(days=lookback_days),
            end=date.today(),
            default_days=lookback_days,
        )
    # Unified metrics summary (dashboard-aligned)
    stmt = select(UnifiedDailyMetrics).where(
        UnifiedDailyMetrics.date >= start,
        UnifiedDailyMetrics.date <= end,
    )
    rows = list(session.exec(stmt).all())
    by_channel: dict[str, dict[str, float]] = {}
    total_spend = 0.0
    total_revenue = 0.0
    by_date: dict[str, dict[str, float]] = {}  # date -> {spend, revenue} for trend
    for r in rows:
        if r.channel not in by_channel:
            by_channel[r.channel] = {"spend": 0.0, "revenue": 0.0}
        by_channel[r.channel]["spend"] += r.spend
        by_channel[r.channel]["revenue"] += r.attributed_revenue
        total_spend += r.spend
        total_revenue += r.attributed_revenue
        dt = r.date.isoformat()
        if dt not in by_date:
            by_date[dt] = {"spend": 0.0, "revenue": 0.0}
        by_date[dt]["spend"] += r.spend
        by_date[dt]["revenue"] += r.attributed_revenue
    channel_list = sorted(by_channel.keys())
    roas_by_channel = {}
    for ch in channel_list:
        s = by_channel[ch]["spend"] or 1
        roas_by_channel[ch] = round(by_channel[ch]["revenue"] / s, 2)
    # Recent vs previous period trend (last 7 days vs prior 7)
    sorted_dates = sorted(by_date.keys())
    trend_text = None
    if len(sorted_dates) >= 14:
        recent_dates = sorted_dates[-7:]
        prior_dates = sorted_dates[-14:-7]
        recent_spend = sum(by_date[d]["spend"] for d in recent_dates)
        recent_rev = sum(by_date[d]["revenue"] for d in recent_dates)
        prior_spend = sum(by_date[d]["spend"] for d in prior_dates)
        prior_rev = sum(by_date[d]["revenue"] for d in prior_dates)
        trend_text = (
            f"Last 7 days: spend ${recent_spend:,.0f}, revenue ${recent_rev:,.0f}. "
            f"Previous 7 days: spend ${prior_spend:,.0f}, revenue ${prior_rev:,.0f}."
        )
    # Decisions
    decisions_stmt = select(DecisionStore).order_by(DecisionStore.created_at.desc()).limit(50)
    decisions = list(session.exec(decisions_stmt).all())
    pending = sum(1 for d in decisions if d.status == "pending")
    # MMM
    mmm_stmt = select(MMMResults).order_by(MMMResults.created_at.desc()).limit(20)
    mmm_rows = list(session.exec(mmm_stmt).all())
    mmm_run_id = mmm_rows[0].run_id if mmm_rows else None
    mmm_by_channel = {}
    for r in mmm_rows:
        if r.run_id == mmm_run_id:
            mmm_by_channel[r.channel] = r.coefficient
    r2 = mmm_rows[0].goodness_of_fit_r2 if mmm_rows else None
    # When using lookback (no explicit range), if this window has no data try a longer window
    # so sample/historical data (e.g. from 2025-01-01) is included instead of showing $0
    EXTENDED_LOOKBACK_DAYS = 400
    if (
        start_date is None
        and end_date is None
        and not _extended
        and total_spend == 0
        and not channel_list
        and lookback_days < EXTENDED_LOOKBACK_DAYS
    ):
        return get_copilot_context(
            session,
            lookback_days=EXTENDED_LOOKBACK_DAYS,
            start_date=None,
            end_date=None,
            _extended=True,
        )

    # Attribution vs MMM report
    report = build_attribution_mmm_report(session, start, end)
    # Full decisions list for decision aggregator (v2)
    decisions_list = [
        {
            "decision_id": d.decision_id,
            "entity_type": d.entity_type,
            "entity_id": d.entity_id,
            "decision_type": d.decision_type,
            "reason_code": d.reason_code,
            "explanation_text": d.explanation_text,
            "projected_impact": d.projected_impact,
            "confidence_score": d.confidence_score,
        }
        for d in decisions
    ]
    return {
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "lookback_days": lookback_days,
        "channels": channel_list,
        "spend_by_channel": {ch: round(by_channel[ch]["spend"], 2) for ch in channel_list},
        "revenue_by_channel": {ch: round(by_channel[ch]["revenue"], 2) for ch in channel_list},
        "roas_by_channel": roas_by_channel,
        "total_spend": round(total_spend, 2),
        "total_revenue": round(total_revenue, 2),
        "roas_overall": round(total_revenue / total_spend, 2) if total_spend else 0,
        "recent_vs_prior_trend": trend_text,
        "decisions_total": len(decisions),
        "decisions_pending": pending,
        "decisions_sample": [
            {
                "entity_type": d.entity_type,
                "decision_type": d.decision_type,
                "explanation_text": d.explanation_text,
                "confidence_score": d.confidence_score,
            }
            for d in decisions[:5]
        ],
        "decisions_list": decisions_list,
        "mmm_last_run_id": mmm_run_id,
        "mmm_coefficients": mmm_by_channel,
        "mmm_r2": round(r2, 4) if r2 is not None else None,
        "attribution_mmm_report": {
            "channels": report["channels"],
            "attribution_share": report["attribution_share"],
            "mmm_share": report["mmm_share"],
            "disagreement_score": report["disagreement_score"],
            "instability_flagged": report["instability_flagged"],
        },
    }


def _normalize(q: str) -> str:
    return re.sub(r"\s+", " ", q.lower().strip())


def _answer_from_templates(question: str, ctx: dict[str, Any]) -> tuple[str, list[str]]:
    """
    Match question intent and fill template with context.
    Returns (answer, list of source descriptions).
    """
    q = _normalize(question)
    sources = ["Unified metrics", "Decisions", "MMM results", "Attribution vs MMM report"]

    # How are we doing? / How's performance?
    if any(
        x in q
        for x in (
            "how are we doing",
            "how is performance",
            "how's performance",
            "overall performance",
            "summary",
            "high level",
        )
    ):
        rev = ctx["total_revenue"]
        spend = ctx["total_spend"]
        roas = ctx["roas_overall"]
        ch = ", ".join(ctx["channels"]) or "no channels"
        return (
            f"Over the last {ctx['lookback_days']} days, total ad spend was ${spend:,.2f} "
            f"and attributed revenue was ${rev:,.2f}, for an overall ROAS of {roas}. "
            f"Channels in the data: {ch}. "
            "Use the Dashboard for detailed metrics by channel and date."
        ), sources

    # Spend by channel
    if any(
        x in q
        for x in (
            "spend by channel",
            "spending per channel",
            "how much we spend",
            "where we spend",
            "channel spend",
        )
    ):
        parts = [
            f"{ch}: ${ctx['spend_by_channel'].get(ch, 0):,.2f}"
            for ch in ctx["channels"]
        ]
        return (
            f"Spend by channel ({ctx['start_date']} to {ctx['end_date']}): "
            + "; ".join(parts)
            + ". Check the Dashboard for daily breakdowns."
        ), sources

    # Revenue by channel
    if any(
        x in q
        for x in (
            "revenue by channel",
            "revenue per channel",
            "which channel drives",
            "revenue by channel",
        )
    ):
        parts = [
            f"{ch}: ${ctx['revenue_by_channel'].get(ch, 0):,.2f}"
            for ch in ctx["channels"]
        ]
        return (
            f"Attributed revenue by channel: " + "; ".join(parts) + "."
        ), sources

    # ROAS
    if any(x in q for x in ("roas", "return on ad spend", "efficiency")):
        roas = ctx["roas_overall"]
        return (
            f"Overall ROAS for the period is {roas} (attributed revenue / ad spend). "
            "Use the Dashboard to see ROAS by channel and over time."
        ), sources

    # Decisions / recommendations
    if any(
        x in q
        for x in (
            "decisions",
            "recommendations",
            "what should we do",
            "suggestions",
            "pending",
            "actions",
        )
    ):
        total = ctx["decisions_total"]
        pending = ctx["decisions_pending"]
        if total == 0:
            return (
                "There are no decisions in the system yet. Run the pipeline (Dashboard → Run pipeline) "
                "to generate recommendations based on your metrics and MMM model."
            ), sources
        sample = ctx.get("decisions_sample") or []
        lines = [f"You have {total} decisions ({pending} pending)."]
        for s in sample[:3]:
            lines.append(
                f"- {s['entity_type']} / {s['decision_type']}: {s.get('explanation_text') or s['reason_code']} "
                f"(confidence {s['confidence_score']:.0%})"
            )
        return " ".join(lines) + " See the Dashboard → Decisions for the full list.", sources

    # MMM / model
    if any(
        x in q
        for x in (
            "model",
            "mmm",
            "marketing mix",
            "coefficient",
            "contribution",
        )
    ):
        run_id = ctx.get("mmm_last_run_id")
        if not run_id:
            return (
                "No MMM run found. Run the pipeline from the Dashboard to train the model and get channel coefficients."
            ), sources
        coefs = ctx.get("mmm_coefficients") or {}
        r2 = ctx.get("mmm_r2")
        parts = [f"{ch}: {coefs.get(ch, 0):.4f}" for ch in ctx["channels"]]
        r2_str = f" Model fit (R²): {r2}." if r2 is not None else ""
        return (
            f"Latest MMM run: {run_id}. Channel coefficients: " + "; ".join(parts) + "." + r2_str
        ), sources

    # Best / top performing channel
    if any(
        x in q
        for x in (
            "best channel",
            "which channel performs",
            "top channel",
            "strongest channel",
        )
    ):
        channels = ctx["channels"]
        if not channels:
            return "No channel data yet. Run the pipeline from the Dashboard to load data.", sources
        spend_ch = ctx.get("spend_by_channel") or {}
        rev_ch = ctx.get("revenue_by_channel") or {}
        roas_ch = {}
        for ch in channels:
            s = spend_ch.get(ch, 0) or 1
            r = rev_ch.get(ch, 0)
            roas_ch[ch] = r / s if s else 0
        best = max(channels, key=lambda c: roas_ch.get(c, 0))
        best_roas = roas_ch.get(best, 0)
        return (
            f"Based on attributed revenue and spend, {best} has the highest ROAS ({best_roas:.2f}) in this period. "
            "Use the Dashboard → Metrics to compare channels over time, and → Optimizer for budget allocation."
        ), sources

    # Budget / optimize
    if any(
        x in q
        for x in (
            "budget",
            "optimize",
            "allocate",
            "how to spend",
        )
    ):
        return (
            "Use the Dashboard → Optimizer: enter your total budget and get a recommended split across channels "
            "based on the MMM model. You can also use the Simulator to see projected revenue for spend changes."
        ), sources

    # Attribution vs MMM
    if any(
        x in q
        for x in (
            "attribution",
            "mmm comparison",
            "disagree",
            "instability",
        )
    ):
        r = ctx.get("attribution_mmm_report") or {}
        flag = r.get("instability_flagged", False)
        score = r.get("disagreement_score", 0)
        if flag:
            return (
                f"Attribution and MMM are showing some disagreement (score {score:.2f}). "
                "This can happen when last-touch attribution and model-based contribution differ. "
                "Review the Dashboard → Attribution vs MMM report for details."
            ), sources
        return (
            f"Attribution vs MMM disagreement score is {score:.2f}; no major instability flagged. "
            "See the report in the Dashboard for channel-level comparison."
        ), sources

    # Scale / grow
    if any(x in q for x in ("scale", "grow", "increase spend", "should we spend more")):
        return (
            "Check the Dashboard → Decisions for recommendations (scale up/scale down by channel). "
            "Use → Optimizer to see how to allocate a larger budget, and → Simulator to test spend changes before committing."
        ), sources

    # Default
    return (
        "I can answer questions about your ad spend, revenue, ROAS, decisions, MMM model, and budget optimization. "
        "Try: \"How are we doing?\", \"Spend by channel\", \"Which channel performs best?\", \"What decisions do we have?\", or \"How do I optimize budget?\""
    ), ["General guidance"]


def _decision_context_to_response_structured(decision_ctx: dict[str, Any]) -> tuple[list, list, list]:
    """Convert decision aggregator output to recommendation/risk/opportunity dicts for API response."""
    recommendations = []
    for item in (decision_ctx.get("budget_waste") or [])[:10]:
        action = "reduce_budget" if (item.get("decision_type") or "").lower() in ("scale_down", "pause_campaign", "pause_product") else "reallocate"
        impact = item.get("projected_impact")
        recommendations.append({
            "action": action,
            "entity": item.get("entity_id") or "",
            "reason": item.get("reason") or "",
            "confidence": item.get("confidence", 0),
            "expected_impact": f"{impact:+.0%} efficiency" if impact is not None else None,
            "decision_id": item.get("decision_id"),
        })
    for item in (decision_ctx.get("scale_candidates") or [])[:10]:
        if (item.get("decision_type") or "").lower() == "scale_up":
            pi = item.get("projected_impact")
            recommendations.append({
                "action": "scale_up",
                "entity": item.get("entity_id") or "",
                "reason": item.get("reason") or "",
                "confidence": item.get("confidence", 0),
                "expected_impact": f"+{int((pi or 0.1) * 100)}% efficiency" if pi is not None else None,
                "decision_id": item.get("decision_id"),
            })
    risks = [
        {
            "title": r.get("entity_id") or "Risk",
            "description": r.get("reason") or "",
            "confidence": r.get("confidence", 0),
            "entity_id": r.get("entity_id"),
        }
        for r in (decision_ctx.get("risk_campaigns") or [])[:10]
    ]
    opportunities = [
        {
            "title": o.get("entity_id") or "Opportunity",
            "description": o.get("reason") or "",
            "confidence": o.get("confidence", 0),
            "entity_id": o.get("entity_id"),
            "expected_impact": f"+{int((o.get('projected_impact') or 0.1) * 100)}% efficiency",
        }
        for o in (decision_ctx.get("top_opportunities") or [])[:10]
    ]
    return recommendations, risks, opportunities


def generate_copilot_answer(
    session: Session,
    question: str,
    versioned_context: Optional[dict[str, Any]] = None,
    conversation_history: Optional[list[dict[str, str]]] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> tuple[str, list[str], dict[str, Any], list[dict], list[dict], list[dict]]:
    """
    Generate a plain-language answer from dashboard data. v2: intent routing, decision context.
    Returns (answer_text, sources, model_versions_used, recommendations, risks, opportunities).
    """
    if start_date is not None and end_date is not None:
        ctx = get_copilot_context(session, start_date=start_date, end_date=end_date)
    else:
        ctx = get_copilot_context(session)
    intent = classify_intent(question)
    decision_ctx = build_decision_context(ctx, intent)
    ctx["decisions"] = decision_ctx
    ctx["mmm_summary"] = {
        "run_id": ctx.get("mmm_last_run_id"),
        "coefficients": ctx.get("mmm_coefficients"),
        "r2": ctx.get("mmm_r2"),
    }
    ctx["attribution_summary"] = ctx.get("attribution_mmm_report")
    ctx["confidence_scores"] = decision_ctx.get("confidence_summary", {})

    q = question.strip()
    use_llm = len(q) > 10
    model_versions_used = {}
    if versioned_context:
        model_versions_used = {
            "mta_version": versioned_context.get("mta_version"),
            "mmm_version": versioned_context.get("mmm_version"),
        }

    if use_llm and os.environ.get("GEMINI_API_KEY"):
        try:
            answer, sources = _answer_with_gemini(question, ctx, versioned_context, conversation_history)
            recs, risks, opps = _decision_context_to_response_structured(decision_ctx)
            return answer, sources, model_versions_used, recs, risks, opps
        except Exception:
            pass

    if use_llm and os.environ.get("OPENAI_API_KEY"):
        try:
            answer, sources = _answer_with_openai(question, ctx, versioned_context, conversation_history)
            recs, risks, opps = _decision_context_to_response_structured(decision_ctx)
            return answer, sources, model_versions_used, recs, risks, opps
        except Exception:
            pass

    answer, sources = _answer_from_templates(question, ctx)
    recs, risks, opps = _decision_context_to_response_structured(decision_ctx)
    return answer, sources, model_versions_used, recs, risks, opps


def _answer_with_gemini(
    question: str,
    ctx: dict[str, Any],
    versioned_context: Optional[dict[str, Any]] = None,
    conversation_history: Optional[list[dict[str, str]]] = None,
) -> tuple[str, list[str]]:
    """Use Google Gemini API to generate answer from dashboard context and optional conversation history."""
    try:
        import google.generativeai as genai
    except ImportError:
        return _answer_from_templates(question, ctx)

    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        return _answer_from_templates(question, ctx)

    genai.configure(api_key=api_key)
    model_name = os.environ.get("GEMINI_MODEL", "gemini-1.5-flash")
    model = genai.GenerativeModel(model_name)
    prompt = _build_prompt_from_templates(question, ctx, versioned_context, conversation_history)

    response = model.generate_content(
        prompt,
        generation_config={"max_output_tokens": _MAX_COPILOT_TOKENS},
    )
    text = (response.text or "").strip()
    if not text:
        return _answer_from_templates(question, ctx)
    return text, ["Unified metrics", "Decisions", "MMM", "Attribution report"]


def stream_answer_with_gemini(
    question: str,
    ctx: dict[str, Any],
    versioned_context: Optional[dict[str, Any]] = None,
    conversation_history: Optional[list[dict[str, str]]] = None,
):
    """
    Yield (delta_text, sources, model_versions_used). For each content chunk yield (chunk, None, None);
    at the end yield (None, sources, model_versions_used). Uses dashboard context and optional conversation history for follow-ups.
    """
    model_versions_used = {}
    if versioned_context:
        model_versions_used = {
            "mta_version": versioned_context.get("mta_version"),
            "mmm_version": versioned_context.get("mmm_version"),
        }
    sources = ["Unified metrics", "Decisions", "MMM", "Attribution report"]

    try:
        import google.generativeai as genai
    except ImportError:
        full, _ = _answer_from_templates(question, ctx)
        yield full, None, None
        yield None, sources, model_versions_used
        return

    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        full, _ = _answer_from_templates(question, ctx)
        yield full, None, None
        yield None, sources, model_versions_used
        return

    genai.configure(api_key=api_key)
    model_name = os.environ.get("GEMINI_MODEL", "gemini-1.5-flash")
    model = genai.GenerativeModel(model_name)
    prompt = _build_prompt_from_templates(question, ctx, versioned_context, conversation_history)

    try:
        response = model.generate_content(
            prompt,
            stream=True,
            generation_config={"max_output_tokens": _MAX_COPILOT_TOKENS},
        )
        for chunk in response:
            if chunk.text:
                yield chunk.text, None, None
        yield None, sources, model_versions_used
    except Exception:
        full, _ = _answer_from_templates(question, ctx)
        yield full, None, None
        yield None, sources, model_versions_used


def _answer_with_openai(
    question: str,
    ctx: dict[str, Any],
    versioned_context: Optional[dict[str, Any]] = None,
    conversation_history: Optional[list[dict[str, str]]] = None,
) -> tuple[str, list[str]]:
    """Use OpenAI to generate a friendlier answer with same context and optional conversation history."""
    try:
        import openai
    except ImportError:
        return _answer_from_templates(question, ctx)

    client = openai.OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    prompt = _build_prompt_from_templates(question, ctx, versioned_context, conversation_history)
    resp = client.chat.completions.create(
        model=os.environ.get("OPENAI_MODEL", "gpt-4o-mini"),
        messages=[{"role": "user", "content": prompt}],
        max_tokens=_MAX_COPILOT_TOKENS,
    )
    text = (resp.choices[0].message.content or "").strip()
    if not text:
        return _answer_from_templates(question, ctx)
    return text, ["Unified metrics", "Decisions", "MMM", "Attribution report"]
