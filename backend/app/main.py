"""
FastAPI backend: enterprise multi-tenant, insights (paginated/top), review/apply, copilot Q&A only.
All queries scoped by organization_id; no cross-client leakage.
Copilot uses Gemini when GEMINI_API_KEY or Vertex AI is configured.
No decision engine; analytics + attribution + Copilot only.
"""
from __future__ import annotations

try:
    from pathlib import Path
    from dotenv import load_dotenv
    _root = Path(__file__).resolve().parents[2]  # repo root when main.py is backend/app/main.py
    _env_file = _root / ".env"
    loaded = load_dotenv(_env_file)
    if not loaded and Path.cwd() != _root:
        loaded = load_dotenv(Path.cwd() / ".env")
except Exception as _e:
    import logging
    logging.basicConfig(level=logging.INFO)
    logging.getLogger(__name__).warning("Could not load .env: %s", _e)

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from contextlib import asynccontextmanager
from typing import Any, Optional

from .logging_config import configure_logging
configure_logging()

logger = logging.getLogger(__name__)

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from .config import get_api_key, get_bq_project, get_analytics_dataset, get_cors_origins
from .auth import (
    get_organization,
    get_organization_id,
    get_org_projects_flat,
    get_role_from_token as auth_get_role,
    get_user_id,
    init_firebase,
    parse_org_projects,
)
from .config_loader import get
from .copilot_synthesizer import (
    set_llm_client,
    synthesize as copilot_synthesize,
    prepare_copilot_prompt,
    _parse_llm_response,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """On startup: wire Claude or Gemini for Copilot. No analytics cache; Copilot queries hypeon_marts directly."""
    try:
        import os
        _has_anthropic = bool(os.environ.get("ANTHROPIC_API_KEY", "").strip())
        _has_gemini = bool(os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY"))
        logger.info("Copilot env: ANTHROPIC_API_KEY=%s GEMINI/GOOGLE_API_KEY=%s", "set" if _has_anthropic else "not set", "set" if _has_gemini else "not set")
        from .llm_claude import is_claude_configured, make_claude_copilot_client
        from .llm_gemini import is_gemini_configured, make_gemini_copilot_client
        if is_claude_configured():
            set_llm_client(make_claude_copilot_client())
            logger.info("Copilot LLM: Claude%s", " | Gemini as fallback" if is_gemini_configured() else "")
        elif is_gemini_configured():
            set_llm_client(make_gemini_copilot_client())
            logger.info("Copilot LLM: Gemini")
        else:
            logger.warning("Copilot: no LLM configured. Set ANTHROPIC_API_KEY or GEMINI_API_KEY for chat.")
    except Exception as e:
        logger.warning("Copilot LLM setup failed: %s", e, exc_info=True)
    import os
    if not os.environ.get("GOOGLE_CLOUD_PROJECT"):
        os.environ["GOOGLE_CLOUD_PROJECT"] = get_bq_project()
    if not os.environ.get("FIREBASE_PROJECT_ID") and os.environ.get("GOOGLE_CLOUD_PROJECT") == "braided-verve-459208-i6":
        os.environ["FIREBASE_PROJECT_ID"] = "hypeon-ai-prod"
    try:
        init_firebase()
    except Exception as e:
        logger.warning("Firebase init: %s", e)
    # Eagerly resolve session store so Firestore vs in-memory is fixed at startup
    try:
        from .copilot.session_memory import get_session_store
        get_session_store()
    except Exception as e:
        logger.debug("Session store init: %s", e)
    logger.info("Request logging active: every API request will be logged (METHOD path -> status | duration)")
    yield


app = FastAPI(title="HypeOn Analytics V1 API", version="2.0.0", lifespan=lifespan)


# ----- Global exception handlers (consistent JSON + logging) -----
@app.exception_handler(HTTPException)
def http_exception_handler(request: Request, exc: HTTPException):
    """Log 4xx/5xx and return consistent JSON."""
    if exc.status_code >= 500:
        logger.error(
            "HTTP %s %s -> %s | detail=%s",
            exc.status_code,
            request.method,
            request.url.path,
            exc.detail,
            exc_info=False,
        )
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})


@app.exception_handler(Exception)
def unhandled_exception_handler(request: Request, exc: Exception):
    """Log full traceback and return 500 with safe message."""
    logger.exception(
        "Unhandled exception: %s %s -> %s | %s",
        request.method,
        request.url.path,
        type(exc).__name__,
        str(exc)[:200],
    )
    return JSONResponse(
        status_code=500,
        content={
            "detail": {
                "code": "INTERNAL_ERROR",
                "message": "An unexpected error occurred. Check server logs.",
            }
        },
    )


app.add_middleware(CORSMiddleware, allow_origins=get_cors_origins(), allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# Copilot rate limit: 20 req/min per user
from .middleware.rate_limit import CopilotRateLimitMiddleware
app.add_middleware(CopilotRateLimitMiddleware)

# Request logging: outermost so every API request is logged (method, path, status, duration)
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request as StarletteRequest
from starlette.responses import Response


class RequestLogMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: StarletteRequest, call_next):
        path = request.url.path or ""
        if request.query_params:
            path = f"{path}?{request.query_params}"
        logger.info("%s %s ...", request.method, path)
        t0 = time.perf_counter()
        response = await call_next(request)
        elapsed_ms = (time.perf_counter() - t0) * 1000
        logger.info(
            "%s %s -> %s | %s ms",
            request.method,
            path,
            response.status_code,
            round(elapsed_ms, 1),
        )
        return response


app.add_middleware(RequestLogMiddleware)

# Dashboard API (cache; business-overview, campaign-performance, funnel)
try:
    from .api.dashboard import router as dashboard_router
    app.include_router(dashboard_router, prefix="/api/v1")
except ImportError:
    pass

# Analysis API (queries BigQuery for in-depth breakdowns; optional)
try:
    from .api.analysis import router as analysis_router
    app.include_router(analysis_router, prefix="/api/v1")
except ImportError:
    pass


# ----- Auth and tenant context (must be before routes that use them) -----
# get_organization_id and get_role_from_token imported from .auth (Firebase + Firestore when Bearer present)

def get_workspace_id(request: Request) -> Optional[str]:
    return request.headers.get("X-Workspace-Id") or None


def get_role_from_token(request: Request) -> str:
    """Role from Firebase user doc, or API key / Bearer / viewer fallback."""
    return auth_get_role(request, get_api_key)


def _has_any_auth(request: Request) -> bool:
    """True if request has Bearer token or valid API key (auth required for all protected routes, including local)."""
    if (request.headers.get("Authorization") or "").strip().startswith("Bearer "):
        return True
    if get_api_key() and request.headers.get("X-API-Key") == get_api_key():
        return True
    return False


def require_role(*allowed: str):
    def dep(request: Request):
        if not _has_any_auth(request):
            raise HTTPException(
                401,
                detail={"code": "UNAUTHORIZED", "message": "Authentication required. Use Bearer token (Firebase) or X-API-Key."},
            )
        role = get_role_from_token(request)
        if role not in allowed:
            raise HTTPException(403, detail={"code": "FORBIDDEN", "message": "Insufficient role"})
        return role
    return dep


# ----- Structured error -----
def api_error(code: str, message: str, status: int = 400):
    raise HTTPException(status, detail={"code": code, "message": message})


# ----- Schemas -----
class InsightReviewBody(BaseModel):
    status: str = Field(..., pattern="^(reviewed|rejected)$")


class InsightApplyBody(BaseModel):
    applied_by: Optional[str] = None
    outcome_metrics_7d: Optional[str] = None
    outcome_metrics_30d: Optional[str] = None


class CopilotQueryBody(BaseModel):
    insight_id: str


class CopilotChatBody(BaseModel):
    message: str = ""
    session_id: Optional[str] = None
    client_id: Optional[int] = None


# ----- Helpers -----
def _bq():
    from .clients.bigquery import get_client
    return get_client()


def _serialize_item(r: dict) -> dict:
    out = {}
    for k, v in r.items():
        if hasattr(v, "isoformat"):
            out[k] = v.isoformat()
        elif isinstance(v, (list, tuple)) and v and hasattr(v[0], "_fields"):
            out[k] = [dict(x) for x in v]
        else:
            out[k] = v
    return out


def _list_insights_scoped(
    organization_id: str,
    client_id: Optional[int],
    workspace_id: Optional[str],
    status: Optional[str],
    limit: int,
    offset: int,
) -> list[dict]:
    from .clients.bigquery import list_insights
    return list_insights(organization_id, client_id=client_id, workspace_id=workspace_id, status=status, limit=limit, offset=offset)


def _top_insights_scoped(organization_id: str, client_id: Optional[int], top_n: int) -> list[dict]:
    from .clients.bigquery import list_insights
    from .insight_ranker import top_per_client
    rows = list_insights(organization_id, client_id=client_id, status=None, limit=500, offset=0)
    ranked = top_per_client(rows, top_n=top_n)
    return ranked


def _update_insight_status(insight_id: str, organization_id: str, status: str, user_id: Optional[str]) -> None:
    from .clients.bigquery import get_client, get_analytics_dataset
    client = get_client()
    project = get_bq_project()
    dataset = get_analytics_dataset()
    user = (user_id or "unknown").replace("'", "''")
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    q = f"""
    UPDATE `{project}.{dataset}.analytics_insights`
    SET status = '{status}', applied_at = CURRENT_TIMESTAMP(), history = CONCAT(COALESCE(history, ''), '; applied_by={user} at {now}')
    WHERE insight_id = '{insight_id.replace("'", "''")}' AND organization_id = '{organization_id.replace("'", "''")}'
    """
    client.query(q).result()


# ----- Endpoints -----
@app.get("/api/v1/me")
def get_me(
    request: Request,
    _role: str = Depends(require_role("admin", "analyst", "viewer")),
):
    """Return current user's organization and dataset list from Firestore. Supports Option B: projects (bq_project + datasets per project)."""
    org_id = get_organization_id(request)
    org_doc = get_organization(org_id)
    if not org_doc:
        return {
            "organization_id": org_id,
            "name": None,
            "client_ids": [1],
            "ad_channels": [{"client_id": 1, "description": "Default"}],
            "projects": [],
        }
    # Option B: org has "projects" array (bq_project + datasets with bq_dataset, bq_location)
    projects_raw = parse_org_projects(org_doc)
    flat = get_org_projects_flat(org_doc)
    if flat:
        client_ids = [c["client_id"] for c in flat]
        ad_channels_list = [
            {
                "client_id": c["client_id"],
                "description": c.get("description", ""),
                "bq_project": c.get("bq_project"),
                "bq_dataset": c.get("bq_dataset"),
                "bq_location": c.get("bq_location"),
                "type": c.get("type"),
            }
            for c in flat
        ]
        # Raw Option B structure for clients that want project grouping
        projects_for_response = [
            {
                "bq_project": p.get("bq_project"),
                "datasets": [
                    {
                        "bq_dataset": d.get("bq_dataset"),
                        "bq_location": d.get("bq_location"),
                        "type": d.get("type"),
                    }
                    for d in (p.get("datasets") or [])
                ],
            }
            for p in projects_raw
        ]
        return {
            "organization_id": org_id,
            "name": org_doc.get("name"),
            "client_ids": client_ids,
            "ad_channels": ad_channels_list,
            "projects": projects_for_response,
        }
    # Legacy: ad_channels or datasets (no projects)
    raw_channels = org_doc.get("ad_channels") or org_doc.get("datasets")
    client_ids = []
    ad_channels_list = []
    if isinstance(raw_channels, list):
        for ch in raw_channels:
            if isinstance(ch, dict) and ch.get("client_id") is not None:
                cid = int(ch["client_id"])
                client_ids.append(cid)
                ad_channels_list.append({"client_id": cid, "description": ch.get("description", "")})
    elif isinstance(raw_channels, dict):
        for k, v in raw_channels.items():
            try:
                cid = int(k)
            except (TypeError, ValueError):
                continue
            client_ids.append(cid)
            desc = v.get("description", str(v)) if isinstance(v, dict) else str(v)
            ad_channels_list.append({"client_id": cid, "description": desc})
    if not client_ids:
        client_ids = [1]
        ad_channels_list = [{"client_id": 1, "description": "Default"}]
    return {
        "organization_id": org_id,
        "name": org_doc.get("name"),
        "client_ids": client_ids,
        "ad_channels": ad_channels_list,
        "projects": [],
    }


@app.get("/insights")
def get_insights(
    request: Request,
    client_id: Optional[int] = Query(None),
    status: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    _role: str = Depends(require_role("admin", "analyst", "viewer")),
):
    """Paginated insights scoped by organization. Returns empty list on error so UI can load."""
    org = get_organization_id(request)
    workspace = get_workspace_id(request)
    try:
        items = _list_insights_scoped(org, client_id, workspace, status, limit, offset)
    except Exception as e:
        logger.warning(
            "list_insights failed | org=%s client_id=%s error=%s",
            org, client_id, str(e)[:300],
            exc_info=True,
        )
        items = []
    return {"items": [_serialize_item(r) for r in items], "count": len(items), "organization_id": org}


@app.get("/insights/top")
def get_insights_top(
    request: Request,
    client_id: Optional[int] = Query(None),
    top_n: int = Query(None, ge=1, le=50),
    _role: str = Depends(require_role("admin", "analyst", "viewer")),
):
    """Top N actionable insights per client (default from config)."""
    org = get_organization_id(request)
    n = top_n or get("top_insights_per_client", 5)
    items = _top_insights_scoped(org, client_id, n)
    return {"items": [_serialize_item(r) for r in items], "count": len(items), "organization_id": org}


@app.post("/insights/{insight_id}/review")
def insight_review(
    insight_id: str,
    body: InsightReviewBody,
    request: Request,
    _role: str = Depends(require_role("admin", "analyst")),
):
    """Move insight to reviewed or rejected."""
    org = get_organization_id(request)
    _update_insight_status(insight_id, org, body.status, None)
    return {"ok": True, "insight_id": insight_id, "status": body.status}


@app.post("/insights/{insight_id}/apply")
def insight_apply(
    insight_id: str,
    body: InsightApplyBody,
    request: Request,
    _role: str = Depends(require_role("admin", "analyst")),
):
    """Mark insight as applied (status only; no decision store)."""
    org = get_organization_id(request)
    from .clients.bigquery import get_insight_by_id
    insight = get_insight_by_id(insight_id, org)
    if not insight:
        api_error("NOT_FOUND", "Insight not found", 404)
    _update_insight_status(insight_id, org, "applied", body.applied_by)
    from .audit_logger import log_decision_applied
    log_decision_applied(org, insight_id, body.applied_by)
    return {"ok": True, "insight_id": insight_id, "status": "applied"}


def _copilot_stream_gen(insight_id: str, org: str):
    """Generator yielding SSE events: phase loading | generating | chunk | done. Any exception yields error phase (no 500)."""
    def emit(ev: dict) -> str:
        return "data: " + json.dumps(ev) + "\n\n"

    try:
        yield emit({"phase": "loading", "message": "Accessing insights & metrics…"})
        prompt, err = prepare_copilot_prompt(insight_id, organization_id=org)
        if err is not None:
            yield emit({"phase": "error", "error": err.get("error", "Unknown error")})
            return

        yield emit({"phase": "generating", "message": "Generating analysis…"})
        from .llm_claude import is_claude_configured, stream_claude
        from .llm_gemini import is_gemini_configured, stream_gemini
        if is_claude_configured():
            stream_fn = stream_claude
        elif is_gemini_configured():
            stream_fn = stream_gemini
        else:
            yield emit({"phase": "error", "error": "No LLM configured. Set ANTHROPIC_API_KEY or GEMINI_API_KEY."})
            return
        acc = []
        for chunk in stream_fn(prompt):
            acc.append(chunk)
            yield emit({"phase": "chunk", "text": chunk})
        full = "".join(acc)
        out = _parse_llm_response(full)
        out["insight_id"] = insight_id
        out["provenance"] = out.get("provenance") or "analytics_insights, supporting_metrics_snapshot"
        yield emit({"phase": "done", "data": out})
    except Exception as e:
        logger.exception("Copilot stream failed")
        yield emit({"phase": "error", "error": str(e)[:300]})


@app.post("/copilot/query")
@app.post("/copilot_query")
def copilot_query(
    body: CopilotQueryBody,
    request: Request,
    _role: str = Depends(require_role("admin", "analyst", "viewer")),
):
    """Synthesized explanation from grounded sources only (insights + snapshot)."""
    org = get_organization_id(request)
    from .audit_logger import log_copilot_query
    log_copilot_query(org, body.insight_id)
    out = copilot_synthesize(insight_id=body.insight_id, organization_id=org)
    if "error" in out:
        raise HTTPException(404, detail={"code": "NOT_FOUND", "message": out["error"]})
    return out


@app.post("/copilot/stream")
def copilot_stream(
    body: CopilotQueryBody,
    request: Request,
    _role: str = Depends(require_role("admin", "analyst", "viewer")),
):
    """Stream Copilot response with phases: loading, generating, chunk, done. SSE."""
    org = get_organization_id(request)
    from .audit_logger import log_copilot_query
    log_copilot_query(org, body.insight_id)
    return StreamingResponse(
        _copilot_stream_gen(body.insight_id, org),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ----- V1 Copilot (chat only: LLM + run_sql) -----
def _copilot_safe_response(out: dict) -> JSONResponse:
    """Build a 200 JSONResponse from copilot output; ensure all values are JSON-serializable."""
    answer = str(out.get("answer") or out.get("text") or "")
    text = str(out.get("text") or out.get("answer") or "")
    raw_data = out.get("data")
    if not isinstance(raw_data, list):
        raw_data = []
    data = []
    for r in raw_data:
        if not isinstance(r, dict):
            continue
        row = {}
        for k, v in r.items():
            row[k] = v.isoformat() if hasattr(v, "isoformat") else v
        data.append(row)
    session_id = str(out.get("session_id") or "")
    return JSONResponse(
        status_code=200,
        content={"answer": answer, "text": text, "data": data, "session_id": session_id},
    )


@app.post("/api/v1/copilot/chat")
def copilot_chat(
    body: CopilotChatBody,
    request: Request,
    _role: str = Depends(require_role("admin", "analyst", "viewer")),
):
    """Chat: every query is handled by the LLM with tools. Always returns 200 (never 500)."""
    import uuid
    fallback_answer = (
        "I'm having trouble right now. Please try again, or ask e.g. \"Views count of item FT05B from Google\". "
        "If this persists, check that ANTHROPIC_API_KEY or GEMINI_API_KEY is set and that the analytics schema is available."
    )
    try:
        sid = str((body.session_id or uuid.uuid4()) if body else uuid.uuid4())
    except Exception:
        sid = str(uuid.uuid4())
    default_fail = {"answer": fallback_answer, "data": [], "text": fallback_answer, "session_id": sid}

    try:
        org = get_organization_id(request)
        uid = get_user_id(request)
        logger.info("Copilot chat | org=%s user_id=%s session_id=%s", org, uid or "(none)", getattr(body, "session_id", None) or "(new)")
        from .copilot.chat_handler import chat
        msg = (getattr(body, "message", None) or "").strip()
        if not msg:
            return _copilot_safe_response({"answer": "Please type a message to get a response.", "data": [], "text": "Please type a message to get a response.", "session_id": sid})
        out = chat(org, msg, session_id=sid, client_id=getattr(body, "client_id", None), user_id=uid)
        text = (out.get("text") or out.get("answer") or "").strip()
        if text and ("couldn't complete" in text.lower() or "couldnt complete" in text.lower()):
            fallback = "I'm having trouble right now. Please try again in a moment, or ask something like \"Views count of item FT05B from Google\"."
            out = {**out, "text": fallback, "answer": fallback, "data": out.get("data") or []}
        return _copilot_safe_response(out)
    except Exception as e:
        logger.exception("Copilot chat failed | session_id=%s error=%s", sid, str(e)[:200])
        try:
            msg = (getattr(body, "message", None) or "").strip().lower()
            if msg in ("hi", "hello", "hey", "howdy", "hi there", "hello there", "yo"):
                return _copilot_safe_response({
                    "answer": "Hi! How can I help with your marketing analytics today? Ask about views, campaigns, traffic, or item performance.",
                    "data": [],
                    "text": "Hi! How can I help with your marketing analytics today? Ask about views, campaigns, traffic, or item performance.",
                    "session_id": sid,
                })
        except Exception:
            pass
        return _copilot_safe_response({**default_fail, "session_id": sid})


def _copilot_chat_stream_gen(
    org: str, message: str, session_id: Optional[str], client_id: Optional[int], user_id: Optional[str] = None
):
    """Yield SSE lines from chat_stream. Each event: data: <json>\\n\\n. user_id scopes sessions to the logged-in user."""
    from .copilot.chat_handler import chat_stream
    for ev in chat_stream(org, message, session_id=session_id, client_id=client_id, user_id=user_id):
        yield "data: " + json.dumps(ev) + "\n\n"


@app.post("/api/v1/copilot/chat/stream")
def copilot_chat_stream(
    body: CopilotChatBody,
    request: Request,
    _role: str = Depends(require_role("admin", "analyst", "viewer")),
):
    """Stream copilot chat with status phases (analyzing, discovering, generating_sql, running_query, formatting) then done or error. Sessions scoped by user."""
    org = get_organization_id(request)
    uid = get_user_id(request)
    msg = (getattr(body, "message", None) or "").strip()
    sid = getattr(body, "session_id", None)
    cid = getattr(body, "client_id", None)
    return StreamingResponse(
        _copilot_chat_stream_gen(org, msg, sid, cid, uid),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/api/v1/copilot/chat/history")
def copilot_chat_history(
    request: Request,
    session_id: str = Query(..., description="Session to load"),
    _role: str = Depends(require_role("admin", "analyst", "viewer")),
):
    """Return message history for a chat session (for restoring UI after refresh). Scoped by user so they see only their chats."""
    org = get_organization_id(request)
    uid = get_user_id(request)
    from .copilot.session_memory import get_session_store
    store = get_session_store()
    messages = store.get_messages(org, session_id, user_id=uid)
    return {"session_id": session_id, "messages": messages}


@app.get("/api/v1/copilot/store-info")
def copilot_store_info(
    request: Request,
    _role: str = Depends(require_role("admin", "analyst", "viewer")),
):
    """Return which session store is used, current org, and user_id (for diagnostics). Sessions are scoped by user."""
    import os
    from .copilot.session_memory import get_session_store
    store = get_session_store()
    kind = "firestore" if type(store).__name__ == "FirestoreSessionStore" else "memory"
    db_id = os.environ.get("FIRESTORE_DATABASE_ID") if kind == "firestore" else None
    org = get_organization_id(request)
    uid = get_user_id(request)
    return {"store": kind, "database_id": db_id, "organization_id": org, "user_id": uid}


@app.get("/api/v1/copilot/sessions")
def copilot_sessions(
    request: Request,
    _role: str = Depends(require_role("admin", "analyst", "viewer")),
):
    """Return list of chat sessions for the current user (title, session_id, updated_at). User-scoped so they see their chats on re-login."""
    org = get_organization_id(request)
    uid = get_user_id(request)
    from .copilot.session_memory import get_session_store
    store = get_session_store()
    try:
        with ThreadPoolExecutor(max_workers=1) as ex:
            fut = ex.submit(store.get_sessions, org, uid)
            sessions = fut.result(timeout=10)
    except FuturesTimeoutError:
        logger.warning("Copilot GET /sessions timed out for org=%s user_id=%s", org, uid or "(none)")
        sessions = []
    logger.info("Copilot GET /sessions org=%s user_id=%s count=%d", org, uid or "(none)", len(sessions))
    return {"sessions": sessions}


@app.get("/health")
def health():
    """Liveness. Copilot queries hypeon_marts directly; no cache dependency."""
    return {"status": "ok"}


# Backward-compat alias
class RecommendationApplyBody(BaseModel):
    insight_id: str
    status: str = Field("applied", pattern="^(applied|rejected)$")
    user_id: Optional[str] = None


@app.post("/recommendations/apply")
def recommendations_apply_legacy(
    body: RecommendationApplyBody,
    request: Request,
    _role: str = Depends(require_role("admin", "analyst")),
):
    org = get_organization_id(request)
    _update_insight_status(body.insight_id, org, body.status, body.user_id)
    return {"ok": True, "insight_id": body.insight_id, "status": body.status}
