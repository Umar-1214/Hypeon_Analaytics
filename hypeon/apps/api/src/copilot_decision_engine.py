"""
Decision aggregator for Copilot v2: convert analytics outputs into structured decisions.
Deterministic logic only; no LLM, no raw warehouse queries. Operates on prepared context.
"""
from typing import Any

from .copilot_intent_router import CopilotIntent

# Thresholds for deterministic classification
ROAS_LOW_RISK = 0.5
CONFIDENCE_HIGH = 0.7
CONFIDENCE_LOW_RISK = 0.4


def build_decision_context(ctx: dict[str, Any], intent: CopilotIntent) -> dict[str, Any]:
    """
    Build structured decision context from precomputed Copilot context dict.
    Uses only DecisionStore-derived data, ROAS and spend from context; never queries raw tables.
    Returns: scale_candidates, budget_waste, risk_campaigns, top_opportunities, confidence_summary.
    """
    decisions_list = ctx.get("decisions_list") or []
    roas_by_channel = ctx.get("roas_by_channel") or {}
    spend_by_channel = ctx.get("spend_by_channel") or {}
    mmm_r2 = ctx.get("mmm_r2")
    mmm_coefficients = ctx.get("mmm_coefficients") or {}

    scale_candidates: list[dict[str, Any]] = []
    budget_waste: list[dict[str, Any]] = []
    risk_campaigns: list[dict[str, Any]] = []
    top_opportunities: list[dict[str, Any]] = []
    confidence_scores: list[float] = []

    for d in decisions_list:
        entity_id = d.get("entity_id") or ""
        decision_type = (d.get("decision_type") or "").lower()
        conf = float(d.get("confidence_score") or 0)
        confidence_scores.append(conf)

        item = {
            "entity_id": entity_id,
            "entity_type": d.get("entity_type"),
            "decision_type": decision_type,
            "reason": d.get("explanation_text") or d.get("reason_code") or "",
            "confidence": round(conf, 2),
            "projected_impact": d.get("projected_impact"),
            "decision_id": d.get("decision_id"),
        }

        if decision_type == "scale_up":
            scale_candidates.append(item)
            if conf >= CONFIDENCE_HIGH:
                top_opportunities.append(item)
        elif decision_type in ("scale_down", "pause_campaign", "pause_product"):
            budget_waste.append(item)
            if conf < CONFIDENCE_LOW_RISK or (roas_by_channel.get(entity_id) or 0) < ROAS_LOW_RISK:
                risk_campaigns.append(item)
        elif decision_type == "reallocate_budget":
            scale_candidates.append(item)

    # Risk: channels with very low ROAS and meaningful spend (from context only)
    for ch, roas in roas_by_channel.items():
        if roas < ROAS_LOW_RISK and (spend_by_channel.get(ch) or 0) > 0:
            if not any(r.get("entity_id") == ch for r in risk_campaigns):
                risk_campaigns.append({
                    "entity_id": ch,
                    "entity_type": "channel",
                    "decision_type": "risk",
                    "reason": f"Low ROAS ({roas}) with active spend; review efficiency.",
                    "confidence": 0.5,
                    "projected_impact": None,
                    "decision_id": None,
                })

    # Confidence summary from MMM and decisions
    avg_conf = sum(confidence_scores) / len(confidence_scores) if confidence_scores else 0.0
    confidence_summary = {
        "overall": round(avg_conf, 2),
        "by_source": {
            "decisions": round(avg_conf, 2),
            "mmm_r2": round(mmm_r2, 2) if mmm_r2 is not None else None,
        },
    }

    return {
        "scale_candidates": scale_candidates,
        "budget_waste": budget_waste,
        "risk_campaigns": risk_campaigns,
        "top_opportunities": top_opportunities,
        "confidence_summary": confidence_summary,
    }
