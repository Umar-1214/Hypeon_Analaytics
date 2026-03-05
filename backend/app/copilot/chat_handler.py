"""
Copilot chat: discover tables (marts first, raw fallback) → LLM generates SQL from schema + guide → run → validate → format.
Fully dynamic: no hardcoded SQL templates. Response: { answer, data, text, session_id }.
"""
from __future__ import annotations

import json
import logging
import re
import time
import uuid
from typing import Any, Generator, List, Optional

from .defaults import get_max_retries
from .tools import run_bigquery_sql, _serialize_rows
from .validator import validate as validate_result
from . import copilot_metrics

logger = logging.getLogger(__name__)

_SQL_GUIDE = """You are a BigQuery SQL expert for a read-only analytics warehouse. Your ONLY job is to output exactly one BigQuery SQL query that answers the user question.

## What TO do
- Use ONLY the tables and columns listed in the schema below. Table names must be backtick-quoted: `project.dataset.table`.
- Write a single SELECT statement (or WITH ... SELECT). No semicolon at the end.
- Always add a LIMIT (e.g. LIMIT 500) to avoid huge result sets.
- Prefer columns that match the question (e.g. revenue → value/item_revenue/revenue; product → item_id/product_id/sku; channel → channel/utm_source).
- Date filters: If the schema shows event_date or event_time with type STRING, the column is usually YYYYMMDD format. Use PARSE_DATE('%Y%m%d', event_date) for date comparison, or filter with: event_date >= FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)). Do NOT use CAST(event_date AS DATE) or compare a STRING column directly to a DATE. If the column type is DATE or TIMESTAMP, you may use event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY).
- If a table has client_id, filter by client_id = {client_id} when the question is about this client.
- For "days from first visit to first purchase" or "time lag" by channel: get first visit date (e.g. MIN(event_date) WHERE event_name = 'session_start') and first purchase date (e.g. MIN(event_date) WHERE event_name = 'purchase') per user; JOIN on user_pseudo_id only (do not require same utm_source on both); then attribute by the utm_source of the first visit. Use DATE_DIFF for days between dates.
- For ROAS or revenue vs cost: use tables from the schema below that contain revenue and cost/spend; join them by channel or campaign where columns exist; use COALESCE for missing keys. If no join key, use utm_source or campaign_id where present.
- Funnel (drop-off, checkout): when the schema has event_name, use event_name IN ('view_item','add_to_cart','begin_checkout','purchase','session_start'). Break down by utm_source or device if those columns exist.
- Landing page / entry page: when the schema has page_location and session/user identifiers, use the first event per session (e.g. ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp)); event_name can be session_start or page_view if present.

## What NOT to do
- Do NOT use INSERT, UPDATE, DELETE, MERGE, CREATE, DROP, ALTER, TRUNCATE, GRANT, REVOKE, EXPORT. Only SELECT is allowed.
- Do NOT reference tables or columns that are not in the schema below.
- Do NOT use CAST(string_date_column AS DATE) when the column holds YYYYMMDD strings; use PARSE_DATE('%Y%m%d', column) or FORMAT_DATE for comparison instead.
- Do NOT output explanation or markdown—only the raw SQL query. No ``` wrapper unless you put the SQL inside it; if you use a code block, use ```sql ... ``` so it can be extracted.
- Do NOT run multiple statements.

Output only the SQL query."""

_FORMAT_SYSTEM = """You are a marketing analytics assistant. Format the query result into a clear, readable answer.

Structure and formatting:
- Use **##** for main sections (e.g. ## Summary, ## Key Metrics, ## By Source, ## Insights).
- Use exactly one newline between a table header row and the separator row, and between the separator and data rows. Example:
  | Metric | Value |
  |--------|-------|
  | Sessions | 5,934 |
  Do NOT concatenate table rows without newlines (no "| A | B | |---||---|" on one line).
- Use bullet lists for key insights or short lists.
- For large result sets (many rows): write a concise summary with 1–2 small tables (max 10–15 rows each). Do not embed a full dump of all rows. Add a brief note like "Full result has N rows; top rows above."
- Do not invent data. Be concise.
- If the result has 0 rows: state that no data matches; suggest widening the time window or relaxing filters if relevant."""


def _is_simple_greeting(msg: str) -> bool:
    if not msg or len(msg) > 80:
        return False
    lower = msg.strip().lower()
    greetings = ("hi", "hello", "hey", "howdy", "hi there", "hello there", "yo", "sup", "good morning", "good afternoon", "good evening")
    return lower in greetings or lower.rstrip("!?.") in greetings


def _allow_empty_for_question(message: str) -> bool:
    """Allow 0 rows as valid for analytical/segment questions where empty is a valid answer."""
    if not message:
        return False
    lower = (message or "").strip().lower()
    phrases = (
        "days from first", "time lag", "first visit to first purchase",
        "churn", "went quiet", "45", "90 days", "days since last",
        "90-day ltv", "ltv by channel", "repeat purchase", "first buy",
        "top 10%", "top 10 percent", "spenders", "profile of",
        "landing page", "entry page", "drop off", "drop-off", "abandon",
        "funnel", "checkout", "no conversions", "no data",
    )
    return any(p in lower for p in phrases)


def _schema_block(candidates: List[dict]) -> str:
    """Build a single schema block for the prompt from candidate tables and columns."""
    lines = ["## Available schema (use only these tables and columns)\n"]
    for c in candidates[:10]:
        full = c.get("table") or ""
        cols = c.get("columns") or []
        if not full:
            continue
        lines.append(f"- Table: `{full}`")
        if cols:
            parts = []
            for x in cols:
                if isinstance(x, dict):
                    name = x.get("name") or ""
                    dtype = x.get("data_type")
                    parts.append(f"{name} ({dtype})" if dtype else name)
                else:
                    parts.append(str(x))
            lines.append(f"  Columns: {', '.join(parts)}")
        lines.append("")
    return "\n".join(lines)


def _build_sql_prompt(question: str, candidates: List[dict], client_id: int, previous_sql: Optional[str] = None, previous_error: Optional[str] = None) -> tuple[str, str]:
    """Return (system_prompt, user_message) for LLM SQL generation."""
    system = _SQL_GUIDE.format(client_id=client_id)
    schema = _schema_block(candidates)
    user_parts = [
        schema,
        "## User question",
        question.strip(),
        "",
    ]
    if previous_sql or previous_error:
        user_parts.append("## Previous attempt (do not repeat; try a different table or query)")
        if previous_sql:
            user_parts.append(f"SQL: {previous_sql[:500]}")
        if previous_error:
            user_parts.append(f"Result: {previous_error[:300]}")
        user_parts.append("")
    user_parts.append("Output only the single BigQuery SELECT query:")
    return system, "\n".join(user_parts)


def _extract_sql_from_response(text: str) -> Optional[str]:
    """Extract a single SQL query from LLM response (handles ```sql ... ``` or raw SQL)."""
    if not text or not isinstance(text, str):
        return None
    text = text.strip()
    # Code block
    match = re.search(r"```(?:sql)?\s*([\s\S]*?)```", text, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    # Whole text if it starts with SELECT/WITH (multi-line allowed)
    if text.upper().startswith("SELECT") or text.upper().startswith("WITH"):
        return text
    # Find first line starting with SELECT/WITH and take from there to end
    lines = text.split("\n")
    for i, line in enumerate(lines):
        if line.strip().upper().startswith("SELECT") or line.strip().upper().startswith("WITH"):
            return "\n".join(lines[i:]).strip()
    # Fallback: find SELECT or WITH anywhere (e.g. "The query is: SELECT ...")
    sel = re.search(r"(\bSELECT\b[\s\S]*?)(?=\s*$|\n\n|\n```|;\s*$)", text, re.IGNORECASE | re.DOTALL)
    if sel:
        return sel.group(1).strip().rstrip(";")
    with_match = re.search(r"(\bWITH\s+\w+\s+AS\s*\([\s\S]*)", text, re.IGNORECASE | re.DOTALL)
    if with_match:
        return with_match.group(1).strip().rstrip(";")
    return None


def _llm_generate_sql(system: str, user_content: str) -> Optional[str]:
    """Call LLM (Claude or Gemini) to generate SQL; return extracted SQL or None."""
    try:
        from ..llm_claude import is_claude_configured, chat_completion as claude_completion
        from ..llm_gemini import is_gemini_configured, chat_completion_with_tools as gemini_chat
    except Exception:
        return None
    msgs = [{"role": "user", "content": user_content}]
    raw = ""
    if is_claude_configured():
        try:
            raw = claude_completion(msgs, system=system)
        except Exception as e:
            logger.warning("Claude SQL generation failed: %s", e)
            if is_gemini_configured():
                try:
                    out = gemini_chat(msgs, [], system=system)
                    raw = (out.get("text") or "").strip()
                except Exception:
                    pass
    if not raw and is_gemini_configured():
        try:
            out = gemini_chat(msgs, [], system=system)
            raw = (out.get("text") or "").strip()
        except Exception as e:
            logger.warning("Gemini SQL generation failed: %s", e)
    return _extract_sql_from_response(raw) if raw else None


def _format_answer(message: str, sql_used: str, rows: list, organization_id: str, session_id: str, from_raw: bool = False) -> str:
    """One LLM call to format the result. Prefer Claude, fallback Gemini."""
    try:
        from ..llm_claude import is_claude_configured, chat_completion_with_tools as claude_chat
        from ..llm_gemini import is_gemini_configured, chat_completion_with_tools as gemini_chat
    except Exception:
        return _fallback_answer(rows, sql_used)
    # For large result sets, pass more rows for context but instruct the LLM to summarize
    preview_rows = rows[:50] if len(rows) > 30 else rows[:20]
    data_preview = json.dumps(preview_rows, default=str)[:6000]
    extra = " (Data from raw fallback.)" if from_raw else ""
    zero_row_note = ""
    if len(rows) == 0:
        zero_row_note = " The query returned 0 rows. State that no data matches; suggest widening the time window or relaxing filters if relevant."
    large_set_note = ""
    if len(rows) > 30:
        large_set_note = f" The result has {len(rows)} rows. Output a concise summary with clear sections (##) and at most 1–2 small markdown tables (max 10–15 rows each). Do not list every row."
    prompt = (
        f"User question: {message}\n\n"
        f"SQL used: {sql_used}\n\n"
        f"Result ({len(rows)} rows): {data_preview}\n\n"
        f"Format the above into a clear, well-formatted answer. Use markdown tables with proper newlines between header, separator, and rows. Use ## for sections. Do not invent data.{large_set_note}{zero_row_note}{extra}"
    )
    msgs = [{"role": "user", "content": prompt}]
    if is_claude_configured():
        try:
            res = claude_chat(msgs, [], system=_FORMAT_SYSTEM)
            return (res.get("text") or "").strip() or _fallback_answer(rows, sql_used)
        except Exception:
            if is_gemini_configured():
                res = gemini_chat(msgs, [], system=_FORMAT_SYSTEM)
                return (res.get("text") or "").strip() or _fallback_answer(rows, sql_used)
    if is_gemini_configured():
        try:
            res = gemini_chat(msgs, [], system=_FORMAT_SYSTEM)
            return (res.get("text") or "").strip() or _fallback_answer(rows, sql_used)
        except Exception:
            pass
    return _fallback_answer(rows, sql_used)


def _try_raw_fallback(message: str, organization_id: str, client_id: int) -> Optional[tuple[list, str]]:
    """When marts return 0 rows, try one query against raw GA4/Ads. Returns (rows, sql_used) or None."""
    try:
        from .knowledge_base import get_raw_schema_for_copilot
        from ..clients.bigquery import run_readonly_query_raw
    except Exception as e:
        logger.warning("Raw fallback import failed: %s", e)
        return None
    raw_schema = get_raw_schema_for_copilot(organization_id)
    if not raw_schema or "not available" in raw_schema.lower():
        return None
    system = (
        "You are a BigQuery SQL expert. Output exactly one SELECT query using ONLY the raw tables and columns listed below. "
        "Use backtick-quoted table names. Add LIMIT 500. For GA4 use event_date filter (e.g. event_date >= FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY))). "
        "No marts tables—only GA4 events_* or Ads tables from the schema below.\n\n"
        + raw_schema[:8000]
    )
    user_content = f"User question: {message.strip()}\n\nOutput only the single BigQuery SELECT query:"
    sql_used = _llm_generate_sql(system, user_content)
    if not sql_used:
        return None
    try:
        out = run_readonly_query_raw(
            sql_used,
            client_id=client_id,
            organization_id=organization_id,
            max_rows=500,
            timeout_sec=25.0,
        )
    except Exception as e:
        logger.warning("Raw fallback run failed: %s", e)
        return None
    if out.get("error"):
        logger.info("Raw fallback query error: %s", out.get("error")[:200])
        return None
    rows = out.get("rows") or []
    rows = _serialize_rows(rows)
    return (rows, sql_used)


def _fallback_answer(rows: list, sql_used: str) -> str:
    if not rows:
        return f"No rows returned. SQL: {sql_used[:200]}..."
    lines = [f"**Result ({len(rows)} rows)**", ""]
    if rows and isinstance(rows[0], dict):
        headers = list(rows[0].keys())
        lines.append("| " + " | ".join(headers) + " |")
        lines.append("|" + "|".join("---" for _ in headers) + "|")
        for r in rows[:20]:
            lines.append("| " + " | ".join(str(r.get(h, "")) for h in headers) + " |")
    else:
        for r in rows[:20]:
            lines.append(f"- {r}")
    return "\n".join(lines)


def chat(
    organization_id: str,
    message: str,
    *,
    session_id: Optional[str] = None,
    client_id: Optional[int] = None,
) -> dict[str, Any]:
    """
    One turn: discover tables → LLM generates SQL from schema + guide → run → validate → format. Retry on empty/error.
    Returns { answer, data, text, session_id }. Never raises.
    """
    try:
        from .session_memory import get_session_store
        from .planner import analyze as planner_analyze, replan as planner_replan
    except Exception as e:
        logger.exception("Copilot imports failed")
        sid = session_id or str(uuid.uuid4())
        return {"answer": f"Configuration error: {str(e)[:200]}", "data": [], "text": str(e)[:200], "session_id": str(sid)}

    store = get_session_store()
    sid = str(session_id or uuid.uuid4())
    try:
        cid = int(client_id) if client_id is not None else 1
    except (TypeError, ValueError):
        cid = 1

    msg_clean = (message or "").strip()
    if not msg_clean:
        out = {"answer": "Please type a message to get a response.", "data": [], "text": "Please type a message to get a response.", "session_id": sid}
        return out
    if len(msg_clean) > 32000:
        msg_clean = msg_clean[:32000] + "... [truncated]"
    message = msg_clean

    if _is_simple_greeting(message):
        reply = "Hi! I can help with marketing analytics. Ask about revenue, top products, channels, ROAS, sessions, conversions, or any metric we have in the warehouse."
        store.append(organization_id, sid, "user", message)
        store.append(organization_id, sid, "assistant", reply, meta=None)
        return {"answer": reply, "data": [], "text": reply, "session_id": sid}

    # When org has no BQ config, do not use shared env; tell user to configure datasets. ("default" org may use env.)
    o = (organization_id or "").strip()
    if o and o.lower() != "default":
        try:
            from ..auth.firestore_user import get_org_bq_context
            if get_org_bq_context(organization_id) is None:
                from ..clients.bigquery import MSG_ORG_DATASETS_NOT_CONFIGURED
                store.append(organization_id, sid, "user", message)
                store.append(organization_id, sid, "assistant", MSG_ORG_DATASETS_NOT_CONFIGURED, meta=None)
                return {"answer": MSG_ORG_DATASETS_NOT_CONFIGURED, "data": [], "text": MSG_ORG_DATASETS_NOT_CONFIGURED, "session_id": sid}
        except Exception:
            pass

    start_ms = time.perf_counter() * 1000
    max_retries = get_max_retries()
    copilot_metrics.increment("copilot.planner_attempts_total")
    plan = planner_analyze(message, context=None, client_id=cid, organization_id=organization_id)
    candidates = list(plan.get("candidates") or [])

    if not candidates:
        logger.info("Copilot no candidates | intent=%s", plan.get("intent", ""))
        copilot_metrics.increment("copilot.query_empty_results_total")
        final_text = (
            "I couldn't find any tables in the warehouse for that question. "
            "Check that BigQuery discovery is configured (project and datasets) and try again."
        )
        store.append(organization_id, sid, "user", message)
        store.append(organization_id, sid, "assistant", final_text, meta=None)
        return {"answer": final_text, "data": [], "text": final_text, "session_id": sid}

    valid_result = None
    sql_used = None
    tables_tried: list[str] = []
    attempt = 0
    previous_sql: Optional[str] = None
    previous_error: Optional[str] = None

    while attempt < max_retries:
        attempt += 1
        system, user_content = _build_sql_prompt(message, candidates, cid, previous_sql=previous_sql, previous_error=previous_error)
        sql_used = _llm_generate_sql(system, user_content)
        if not sql_used:
            previous_error = "LLM did not return a valid SQL query."
            logger.warning("Copilot LLM returned no SQL on attempt %s", attempt)
            continue
        tables_tried.append(sql_used[:500])
        try:
            out = run_bigquery_sql(sql_used, organization_id=organization_id, client_id=cid)
        except Exception as e:
            previous_sql = sql_used
            previous_error = str(e)[:300]
            logger.warning("Copilot run_bigquery_sql failed: %s", e)
            continue
        if out.get("error"):
            previous_sql = sql_used
            previous_error = (out.get("error") or "Unknown error")[:300]
            continue
        # Allow 0 rows for analytical/segment questions (churn, LTV, repeat purchase, funnel, etc.)
        allow_empty = _allow_empty_for_question(message)
        is_valid, _reason = validate_result(out, message, allow_empty=allow_empty)
        if is_valid:
            valid_result = out
            if attempt > 1:
                copilot_metrics.increment("copilot.fallback_success_total")
            break
        previous_sql = sql_used
        previous_error = (
            "Query returned no rows or invalid result. "
            "Try a different table from the schema, a wider time window, or fewer WHERE filters."
        )

    if valid_result is not None and sql_used:
        rows = valid_result.get("rows") or []
        execution_time_ms = int((time.perf_counter() * 1000) - start_ms)
        logger.info(
            "Copilot success | intent=%s sql_tried=%s row_count=%d execution_time_ms=%d",
            plan.get("intent", ""),
            len(tables_tried),
            len(rows),
            execution_time_ms,
        )
        final_text = _format_answer(message, sql_used, rows, organization_id, sid)
        store.append(organization_id, sid, "user", message)
        store.append(organization_id, sid, "assistant", final_text, meta=None)
        return {"answer": final_text, "data": rows, "text": final_text, "session_id": sid}

    # Explicit raw fallback: try one query against GA4/Ads raw when marts returned no valid result
    if tables_tried:
        raw_result = _try_raw_fallback(message, organization_id, cid)
        if raw_result is not None:
            raw_rows, raw_sql = raw_result
            logger.info("Copilot raw fallback success | row_count=%d", len(raw_rows))
            final_text = _format_answer(message, raw_sql, raw_rows, organization_id, sid, from_raw=True)
            store.append(organization_id, sid, "user", message)
            store.append(organization_id, sid, "assistant", final_text, meta=None)
            return {"answer": final_text, "data": raw_rows, "text": final_text, "session_id": sid}

    copilot_metrics.increment("copilot.query_empty_results_total")
    # Show first query in full (up to 500 chars) so WHERE clause is visible for debugging
    tables_msg = (tables_tried[0] + ("..." if len(tables_tried[0]) >= 500 else "")) if tables_tried else "none"
    logger.info("Copilot no valid result | intent=%s sql_tried=%s", plan.get("intent", ""), tables_tried)
    final_text = (
        f"I couldn't find relevant data for that question. Queries tried: {tables_msg}. "
        "Try rephrasing or ask about a different metric (e.g. revenue by product, top channels, ROAS)."
    )
    store.append(organization_id, sid, "user", message)
    store.append(organization_id, sid, "assistant", final_text, meta=None)
    return {"answer": final_text, "data": [], "text": final_text, "session_id": sid}


def _serialize_data_for_sse(raw_data: list) -> list:
    """Ensure data is JSON-serializable for SSE (e.g. date/datetime to string)."""
    out = []
    for r in raw_data or []:
        if not isinstance(r, dict):
            continue
        row = {}
        for k, v in r.items():
            row[k] = v.isoformat() if hasattr(v, "isoformat") else v
        out.append(row)
    return out


def chat_stream(
    organization_id: str,
    message: str,
    *,
    session_id: Optional[str] = None,
    client_id: Optional[int] = None,
) -> Generator[dict[str, Any], None, None]:
    """
    Same flow as chat() but yields SSE-style events: phase (status) then done or error.
    Events: {"phase": "analyzing", "message": "..."}, {"phase": "discovering", "message": "..."},
    {"phase": "generating_sql", "message": "..."}, {"phase": "running_query", "message": "..."},
    {"phase": "formatting", "message": "..."}, {"phase": "done", "answer", "data", "session_id"},
    or {"phase": "error", "error": "..."}.
    """
    try:
        from .session_memory import get_session_store
        from .planner import analyze as planner_analyze
    except Exception as e:
        logger.exception("Copilot imports failed")
        sid = session_id or str(uuid.uuid4())
        yield {"phase": "error", "error": str(e)[:200], "session_id": str(sid)}
        return

    store = get_session_store()
    sid = str(session_id or uuid.uuid4())
    try:
        cid = int(client_id) if client_id is not None else 1
    except (TypeError, ValueError):
        cid = 1

    msg_clean = (message or "").strip()
    if not msg_clean:
        yield {"phase": "done", "answer": "Please type a message to get a response.", "data": [], "session_id": sid}
        return
    if len(msg_clean) > 32000:
        msg_clean = msg_clean[:32000] + "... [truncated]"
    message = msg_clean

    if _is_simple_greeting(message):
        reply = "Hi! I can help with marketing analytics. Ask about revenue, top products, channels, ROAS, sessions, conversions, or any metric we have in the warehouse."
        store.append(organization_id, sid, "user", message)
        store.append(organization_id, sid, "assistant", reply, meta=None)
        yield {"phase": "done", "answer": reply, "data": [], "session_id": sid}
        return

    # When org has no BQ config, do not use shared env; tell user to configure datasets. ("default" org may use env.)
    o = (organization_id or "").strip()
    if o and o.lower() != "default":
        try:
            from ..auth.firestore_user import get_org_bq_context
            if get_org_bq_context(organization_id) is None:
                from ..clients.bigquery import MSG_ORG_DATASETS_NOT_CONFIGURED
                store.append(organization_id, sid, "user", message)
                store.append(organization_id, sid, "assistant", MSG_ORG_DATASETS_NOT_CONFIGURED, meta=None)
                yield {"phase": "done", "answer": MSG_ORG_DATASETS_NOT_CONFIGURED, "data": [], "session_id": sid}
                return
        except Exception:
            pass

    try:
        yield {"phase": "analyzing", "message": "Understanding your question…"}
        max_retries = get_max_retries()
        copilot_metrics.increment("copilot.planner_attempts_total")
        plan = planner_analyze(message, context=None, client_id=cid, organization_id=organization_id)
        candidates = list(plan.get("candidates") or [])

        yield {"phase": "discovering", "message": "Finding relevant tables…"}
        if not candidates:
            logger.info("Copilot no candidates | intent=%s", plan.get("intent", ""))
            copilot_metrics.increment("copilot.query_empty_results_total")
            final_text = (
                "I couldn't find any tables in the warehouse for that question. "
                "Check that BigQuery discovery is configured (project and datasets) and try again."
            )
            store.append(organization_id, sid, "user", message)
            store.append(organization_id, sid, "assistant", final_text, meta=None)
            yield {"phase": "done", "answer": final_text, "data": [], "session_id": sid}
            return

        valid_result = None
        sql_used = None
        tables_tried: list[str] = []
        attempt = 0
        previous_sql: Optional[str] = None
        previous_error: Optional[str] = None

        while attempt < max_retries:
            attempt += 1
            yield {"phase": "generating_sql", "message": "Writing SQL query…"}
            system, user_content = _build_sql_prompt(message, candidates, cid, previous_sql=previous_sql, previous_error=previous_error)
            sql_used = _llm_generate_sql(system, user_content)
            if not sql_used:
                previous_error = "LLM did not return a valid SQL query."
                logger.warning("Copilot LLM returned no SQL on attempt %s", attempt)
                continue
            tables_tried.append(sql_used[:500])
            yield {"phase": "running_query", "message": "Running query…"}
            try:
                out = run_bigquery_sql(sql_used, organization_id=organization_id, client_id=cid)
            except Exception as e:
                previous_sql = sql_used
                previous_error = str(e)[:300]
                logger.warning("Copilot run_bigquery_sql failed: %s", e)
                continue
            if out.get("error"):
                previous_sql = sql_used
                previous_error = (out.get("error") or "Unknown error")[:300]
                continue
            allow_empty = _allow_empty_for_question(message)
            is_valid, _reason = validate_result(out, message, allow_empty=allow_empty)
            if is_valid:
                valid_result = out
                if attempt > 1:
                    copilot_metrics.increment("copilot.fallback_success_total")
                break
            previous_sql = sql_used
            previous_error = (
                "Query returned no rows or invalid result. "
                "Try a different table from the schema, a wider time window, or fewer WHERE filters."
            )

        if valid_result is not None and sql_used:
            rows = valid_result.get("rows") or []
            yield {"phase": "formatting", "message": "Formatting results…"}
            final_text = _format_answer(message, sql_used, rows, organization_id, sid)
            store.append(organization_id, sid, "user", message)
            store.append(organization_id, sid, "assistant", final_text, meta=None)
            yield {"phase": "done", "answer": final_text, "data": _serialize_data_for_sse(rows), "session_id": sid}
            return

        if tables_tried:
            yield {"phase": "running_query", "message": "Trying raw data fallback…"}
            raw_result = _try_raw_fallback(message, organization_id, cid)
            if raw_result is not None:
                raw_rows, raw_sql = raw_result
                logger.info("Copilot raw fallback success | row_count=%d", len(raw_rows))
                yield {"phase": "formatting", "message": "Formatting results…"}
                final_text = _format_answer(message, raw_sql, raw_rows, organization_id, sid, from_raw=True)
                store.append(organization_id, sid, "user", message)
                store.append(organization_id, sid, "assistant", final_text, meta=None)
                yield {"phase": "done", "answer": final_text, "data": _serialize_data_for_sse(raw_rows), "session_id": sid}
                return

        copilot_metrics.increment("copilot.query_empty_results_total")
        tables_msg = (tables_tried[0] + ("..." if len(tables_tried[0]) >= 500 else "")) if tables_tried else "none"
        logger.info("Copilot no valid result | intent=%s sql_tried=%s", plan.get("intent", ""), tables_tried)
        final_text = (
            f"I couldn't find relevant data for that question. Queries tried: {tables_msg}. "
            "Try rephrasing or ask about a different metric (e.g. revenue by product, top channels, ROAS)."
        )
        store.append(organization_id, sid, "user", message)
        store.append(organization_id, sid, "assistant", final_text, meta=None)
        yield {"phase": "done", "answer": final_text, "data": [], "session_id": sid}
    except Exception as e:
        logger.exception("Copilot stream failed")
        yield {"phase": "error", "error": str(e)[:300], "session_id": sid}
        return
