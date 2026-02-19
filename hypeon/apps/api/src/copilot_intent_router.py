"""
Intent router for Copilot v2: classify user question into a decision-intent category.
Lightweight rule + keyword classification only; no LLM.
"""
import re
from enum import Enum


class CopilotIntent(str, Enum):
    """Classified intent for Copilot question routing."""

    EXPLAIN_METRICS = "explain_metrics"
    OPTIMIZE_SPEND = "optimize_spend"
    FORECAST = "forecast"
    DEBUG_ATTRIBUTION = "debug_attribution"
    PERFORMANCE_SUMMARY = "performance_summary"


def _normalize(question: str) -> str:
    return re.sub(r"\s+", " ", question.lower().strip())


def classify_intent(question: str) -> CopilotIntent:
    """
    Classify user question into a CopilotIntent using keywords and rules.
    Order of checks matters: more specific patterns first.
    """
    q = _normalize(question)
    if not q:
        return CopilotIntent.PERFORMANCE_SUMMARY

    # "why", "what happened" -> EXPLAIN_METRICS
    if any(x in q for x in ("why", "what happened", "what caused", "explain the", "why did")):
        return CopilotIntent.EXPLAIN_METRICS

    # "where to spend", "optimize", "scale", "reduce" -> OPTIMIZE_SPEND
    if any(
        x in q
        for x in (
            "where to spend",
            "where should we spend",
            "optimize",
            "optimise",
            "scale",
            "reduce spend",
            "reduce budget",
            "cut spend",
            "reallocate",
            "reallocation",
            "increase spend",
            "should we scale",
        )
    ):
        return CopilotIntent.OPTIMIZE_SPEND

    # "forecast", "predict" -> FORECAST
    if any(x in q for x in ("forecast", "predict", "projection", "outlook", "next month", "next quarter")):
        return CopilotIntent.FORECAST

    # "attribution", "conversion source" -> DEBUG_ATTRIBUTION
    if any(
        x in q
        for x in (
            "attribution",
            "conversion source",
            "conversion source",
            "attribution model",
            "mta",
            "last touch",
        )
    ):
        return CopilotIntent.DEBUG_ATTRIBUTION

    # default
    return CopilotIntent.PERFORMANCE_SUMMARY
