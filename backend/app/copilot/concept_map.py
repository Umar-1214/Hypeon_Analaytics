"""
Concept-to-column mapping: map user terms (revenue, product id, etc.) to possible schema column names.
Used for synonym-aware ranking and SQL template building. Marts-first fallback to raw.
"""
from __future__ import annotations

import os
import re
from typing import List, Set

# User-facing concepts -> candidate column names (order: prefer marts-style names first)
CONCEPT_TO_COLUMNS: dict[str, List[str]] = {
    "revenue": ["revenue", "value", "item_revenue", "purchase_revenue", "total_revenue", "conversions", "sales"],
    "product_id": ["item_id", "product_id", "sku", "product_sku", "item_sku"],
    "product": ["item_id", "product_id", "sku", "product_name"],
    "roas": ["roas", "conversions", "cost", "revenue", "value"],
    "channel": ["channel", "utm_source", "source", "medium"],
    "campaign": ["campaign", "campaign_id", "campaign_name"],
    "sessions": ["sessions", "session_count", "event_count", "count"],
    "views": ["views", "view_count", "event_count", "count"],
    "conversion": ["conversions", "conversion_count", "purchase", "transaction"],
    "cost": ["cost", "spend", "ad_cost", "cpc"],
    "clicks": ["clicks", "click_count"],
    "customer": ["user_id", "client_id", "customer_id", "user_pseudo_id"],
    "city": ["city", "region", "geo_city"],
    "country": ["country", "geo_country", "region"],
    "device": ["device", "device_category", "platform"],
    "landing_page": ["page_location", "landing_page", "entry_page"],
    "date": ["date", "event_date", "event_time", "created_at"],
    "ltv": ["ltv", "total_revenue", "value", "revenue"],
    "basket": ["items", "product_id", "item_id"],
    "cohort": ["first_purchase_date", "user_id", "cohort"],
}


def expand_intent_tokens(question: str) -> Set[str]:
    """Return intent tokens plus synonym column names so ranking matches 'revenue' to tables with 'value'."""
    q = (question or "").strip().lower()
    tokens = set(re.findall(r"\w+", q))
    out: Set[str] = set(tokens)
    for concept, columns in CONCEPT_TO_COLUMNS.items():
        if concept in tokens or concept.replace("_", " ") in q:
            out.update(c.lower() for c in columns)
    return out


def resolve_metric_column(user_term: str, available_columns: Set[str]) -> str | None:
    """Pick best matching column for a metric (e.g. 'revenue' -> 'value' if 'value' in available_columns)."""
    available = { (c or "").lower() for c in available_columns }
    term = (user_term or "").strip().lower()
    if term in available:
        return term
    for concept, candidates in CONCEPT_TO_COLUMNS.items():
        if concept != term and term not in (concept.replace("_", " "), concept):
            continue
        for col in candidates:
            if col.lower() in available:
                return col.lower()
    return None


def resolve_product_id_column(available_columns: Set[str]) -> str | None:
    """Return first matching product/item identifier column."""
    available = { (c or "").lower() for c in available_columns }
    for col in CONCEPT_TO_COLUMNS.get("product_id", ["item_id", "product_id", "sku"]):
        if col.lower() in available:
            return col.lower()
    return None


def get_marts_datasets() -> Set[str]:
    """Datasets considered 'marts' (try first); rest are raw fallback."""
    marts = os.environ.get("MARTS_DATASET", "hypeon_marts").strip().lower()
    marts_ads = os.environ.get("MARTS_ADS_DATASET", "hypeon_marts_ads").strip().lower()
    return {marts, marts_ads}
