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
from typing import Any, List, Optional

from .defaults import get_max_retries
from .tools import run_bigquery_sql
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

## What NOT to do
- Do NOT use INSERT, UPDATE, DELETE, MERGE, CREATE, DROP, ALTER, TRUNCATE, GRANT, REVOKE, EXPORT. Only SELECT is allowed.
- Do NOT reference tables or columns that are not in the schema below.
- Do NOT use CAST(string_date_column AS DATE) when the column holds YYYYMMDD strings; use PARSE_DATE('%Y%m%d', column) or FORMAT_DATE for comparison instead.
- Do NOT output explanation or markdown—only the raw SQL query. No ``` wrapper unless you put the SQL inside it; if you use a code block, use ```sql ... ``` so it can be extracted.
- Do NOT run multiple statements.

Output only the SQL query."""

_FORMAT_SYSTEM = """You are a marketing analytics assistant. Format the query result into a clear, accurate answer.
- Use markdown tables for tabular data (e.g. | Column A | Column B |).
- Use bullet lists for short lists.
- Include key numbers and which table/source was used.
- Do not invent data. Be concise."""


def _is_simple_greeting(msg: str) -> bool:
    if not msg or len(msg) > 80:
        return False
    lower = msg.strip().lower()
    greetings = ("hi", "hello", "hey", "howdy", "hi there", "hello there", "yo", "sup", "good morning", "good afternoon", "good evening")
    return lower in greetings or lower.rstrip("!?.") in greetings


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


def _format_answer(message: str, sql_used: str, rows: list, organization_id: str, session_id: str) -> str:
    """One LLM call to format the result. Prefer Claude, fallback Gemini."""
    try:
        from ..llm_claude import is_claude_configured, chat_completion_with_tools as claude_chat
        from ..llm_gemini import is_gemini_configured, chat_completion_with_tools as gemini_chat
    except Exception:
        return _fallback_answer(rows, sql_used)
    data_preview = json.dumps(rows[:15], default=str)[:4000]
    prompt = (
        f"User question: {message}\n\n"
        f"SQL used: {sql_used}\n\n"
        f"Result ({len(rows)} rows): {data_preview}\n\n"
        "Format the above into a clear, well-formatted answer (use markdown tables or lists where appropriate). Do not invent data."
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
        # Allow empty result for time-lag / first-visit-to-purchase questions (query can be valid but return 0 rows)
        msg_lower = (message or "").strip().lower()
        allow_empty = any(
            phrase in msg_lower
            for phrase in ("days from first", "time lag", "first visit to first purchase")
        )
        is_valid, _reason = validate_result(out, message, allow_empty=allow_empty)
        if is_valid:
            valid_result = out
            if attempt > 1:
                copilot_metrics.increment("copilot.fallback_success_total")
            break
        previous_sql = sql_used
        previous_error = "Query returned no rows or invalid result."

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
