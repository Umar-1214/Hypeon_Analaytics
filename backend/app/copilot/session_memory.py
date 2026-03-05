"""
Copilot session memory store: Firestore (primary) or in-memory fallback for multi-turn context per session.
"""
from __future__ import annotations

import logging
import os
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Optional

MAX_MESSAGES_PER_SESSION = 20
MAX_SESSIONS = 100
MAX_SESSIONS_LIST = 50
SESSION_TITLE_MAX_LEN = 50

COPLIOT_SESSIONS_COLLECTION = "copilot_sessions"

logger = logging.getLogger(__name__)


@dataclass
class SessionMessage:
    role: str
    content: str
    meta: Optional[dict] = None


@dataclass
class SessionState:
    session_id: str
    organization_id: str
    messages: deque = field(default_factory=lambda: deque(maxlen=MAX_MESSAGES_PER_SESSION))
    context_summary: Optional[dict] = None
    title: Optional[str] = None
    updated_at: Optional[float] = None

    def append(self, role: str, content: str, meta: Optional[dict] = None) -> None:
        now = time.time()
        self.updated_at = now
        if role == "user" and (not self.title or not self.title.strip()):
            self.title = (content or "").strip()[:SESSION_TITLE_MAX_LEN] or "New chat"
        self.messages.append(SessionMessage(role=role, content=content, meta=meta))

    def get_messages(self) -> list[dict]:
        return [{"role": m.role, "content": m.content, **(m.meta or {})} for m in self.messages]


def _message_to_dict(role: str, content: str, meta: Optional[dict] = None) -> dict[str, Any]:
    """Serialize a message for Firestore (only JSON-safe values)."""
    out: dict[str, Any] = {"role": role, "content": content or ""}
    if meta and isinstance(meta, dict):
        out["meta"] = {k: v for k, v in meta.items() if isinstance(v, (str, int, float, bool, type(None), list, dict))}
    return out


class FirestoreSessionStore:
    """
    Persists copilot sessions and messages in Firestore.
    Collection: copilot_sessions. Document ID = session_id.
    Fields: organization_id, user_id (Firebase uid for per-user sessions), title, updated_at, context_summary?, messages.
    When user_id is set, list/history are scoped to that user so they see their chats when they log in again.
    """

    def __init__(self):
        self._db = None

    def _get_db(self):
        if self._db is None:
            try:
                from ..auth.firestore_user import _get_firestore
                self._db = _get_firestore()
            except Exception as e:
                logger.debug("Firestore session store: %s", e)
        return self._db

    def append(
        self,
        organization_id: str,
        session_id: str,
        role: str,
        content: str,
        meta: Optional[dict] = None,
        user_id: Optional[str] = None,
    ) -> None:
        db = self._get_db()
        if not db:
            logger.warning("FirestoreSessionStore.append: no db (Firestore client unavailable), session not persisted")
            return
        org = organization_id or "default"
        uid = (user_id or "").strip() or None
        now = time.time()
        title = None
        if role == "user" and content:
            title = (content or "").strip()[:SESSION_TITLE_MAX_LEN] or "New chat"
        ref = db.collection(COPLIOT_SESSIONS_COLLECTION).document(session_id)
        try:
            doc = ref.get()
            msg = _message_to_dict(role, content, meta)
            if not doc.exists:
                payload: dict[str, Any] = {
                    "organization_id": org,
                    "title": title or "New chat",
                    "updated_at": now,
                    "messages": [msg],
                }
                if uid:
                    payload["user_id"] = uid
                ref.set(payload)
                logger.info("FirestoreSessionStore.append: created session %s org=%s", session_id[:16], org)
            else:
                data = doc.to_dict() or {}
                if (data.get("organization_id") or "default") != org:
                    return
                doc_uid = data.get("user_id") or ""
                if doc_uid and doc_uid != (uid or ""):
                    return
                messages = list(data.get("messages") or [])
                messages.append(msg)
                messages = messages[-MAX_MESSAGES_PER_SESSION:]
                update: dict[str, Any] = {
                    "messages": messages,
                    "updated_at": now,
                }
                if title and (not data.get("title") or not str(data.get("title", "")).strip()):
                    update["title"] = title
                if uid and not doc_uid:
                    update["user_id"] = uid
                ref.update(update)
        except Exception as e:
            logger.warning("FirestoreSessionStore.append failed: %s", e, exc_info=True)

    def get_messages(
        self, organization_id: str, session_id: str, user_id: Optional[str] = None
    ) -> list[dict]:
        db = self._get_db()
        if not db:
            return []
        ref = db.collection(COPLIOT_SESSIONS_COLLECTION).document(session_id)
        try:
            doc = ref.get()
            if not doc.exists:
                return []
            data = doc.to_dict() or {}
            if (data.get("organization_id") or "default") != (organization_id or "default"):
                return []
            doc_uid = data.get("user_id") or ""
            if user_id and doc_uid and doc_uid != user_id:
                return []
            messages = data.get("messages") or []
            # Return last N in chronological order; merge meta into each message to match in-memory API
            out = []
            for m in messages[-MAX_MESSAGES_PER_SESSION:]:
                if not isinstance(m, dict):
                    continue
                msg = {"role": m.get("role", ""), "content": m.get("content", "")}
                if isinstance(m.get("meta"), dict):
                    msg.update(m["meta"])
                out.append(msg)
            return out
        except Exception as e:
            logger.warning("FirestoreSessionStore.get_messages failed: %s", e)
            return []

    def get_sessions(
        self, organization_id: str, user_id: Optional[str] = None
    ) -> list[dict]:
        """Sessions for the org (and user when user_id set), sorted by updated_at desc. Per-user so same user sees their chats on re-login."""
        db = self._get_db()
        if not db:
            return []
        org = organization_id or "default"
        uid = (user_id or "").strip() or None
        try:
            q = (
                db.collection(COPLIOT_SESSIONS_COLLECTION)
                .where("organization_id", "==", org)
                .limit(MAX_SESSIONS_LIST + 50)
            )
            out = []
            docs_list = list(q.stream())
            logger.info("FirestoreSessionStore.get_sessions: org=%s user_id=%s query_count=%d", org, uid or "(none)", len(docs_list))
            for doc in docs_list:
                d = doc.to_dict() or {}
                doc_uid = d.get("user_id") or ""
                if uid:
                    if doc_uid and doc_uid != uid:
                        continue
                else:
                    if doc_uid:
                        continue
                out.append({
                    "session_id": doc.id,
                    "title": d.get("title") or "New chat",
                    "updated_at": d.get("updated_at"),
                })
            out.sort(key=lambda x: (x["updated_at"] or 0), reverse=True)
            return out[:MAX_SESSIONS_LIST]
        except Exception as e:
            logger.warning("FirestoreSessionStore.get_sessions failed: %s", e)
            return []

    def set_context_summary(self, organization_id: str, session_id: str, summary: dict) -> None:
        db = self._get_db()
        if not db:
            return
        ref = db.collection(COPLIOT_SESSIONS_COLLECTION).document(session_id)
        try:
            ref.update({"context_summary": summary})
        except Exception as e:
            logger.debug("FirestoreSessionStore.set_context_summary failed: %s", e)

    def get_context_summary(self, organization_id: str, session_id: str) -> Optional[dict]:
        db = self._get_db()
        if not db:
            return None
        ref = db.collection(COPLIOT_SESSIONS_COLLECTION).document(session_id)
        try:
            doc = ref.get()
            if not doc.exists:
                return None
            data = doc.to_dict() or {}
            if (data.get("organization_id") or "default") != (organization_id or "default"):
                return None
            return data.get("context_summary")
        except Exception as e:
            logger.debug("FirestoreSessionStore.get_context_summary failed: %s", e)
            return None

    def clear_session(self, organization_id: str, session_id: str) -> bool:
        db = self._get_db()
        if not db:
            return False
        ref = db.collection(COPLIOT_SESSIONS_COLLECTION).document(session_id)
        try:
            doc = ref.get()
            if not doc.exists:
                return False
            if (doc.to_dict() or {}).get("organization_id") != (organization_id or "default"):
                return False
            ref.delete()
            return True
        except Exception as e:
            logger.warning("FirestoreSessionStore.clear_session failed: %s", e)
            return False


class SessionMemoryStore:
    def __init__(self, max_sessions: int = MAX_SESSIONS):
        self._store: dict[tuple[str, str], SessionState] = {}
        self._order: deque = deque(maxlen=max_sessions)

    def _key(self, organization_id: str, session_id: str) -> tuple[str, str]:
        return (organization_id or "default", session_id or "")

    def get_or_create_session(self, organization_id: str, session_id: Optional[str] = None) -> SessionState:
        sid = session_id or str(uuid.uuid4())
        key = self._key(organization_id, sid)
        if key not in self._store:
            if len(self._store) >= self._order.maxlen:
                old = self._order.popleft()
                self._store.pop(old, None)
            self._store[key] = SessionState(session_id=sid, organization_id=organization_id or "default")
            self._order.append(key)
        return self._store[key]

    def append(
        self,
        organization_id: str,
        session_id: str,
        role: str,
        content: str,
        meta: Optional[dict] = None,
        user_id: Optional[str] = None,
    ) -> None:
        self.get_or_create_session(organization_id, session_id).append(role, content, meta)

    def get_messages(
        self, organization_id: str, session_id: str, user_id: Optional[str] = None
    ) -> list[dict]:
        state = self._store.get(self._key(organization_id, session_id))
        return state.get_messages() if state else []

    def get_sessions(self, organization_id: str, user_id: Optional[str] = None) -> list[dict]:
        """Return sessions for the org as [{ session_id, title, updated_at }], sorted by updated_at desc. In-memory store is org-scoped only."""
        org = organization_id or "default"
        out = []
        for (o, sid), state in self._store.items():
            if o != org:
                continue
            out.append({
                "session_id": state.session_id,
                "title": state.title or "New chat",
                "updated_at": state.updated_at,
            })
        out.sort(key=lambda x: (x["updated_at"] or 0), reverse=True)
        return out[:MAX_SESSIONS_LIST]

    def set_context_summary(self, organization_id: str, session_id: str, summary: dict) -> None:
        self.get_or_create_session(organization_id, session_id).context_summary = summary

    def get_context_summary(self, organization_id: str, session_id: str) -> Optional[dict]:
        state = self._store.get(self._key(organization_id, session_id))
        return state.context_summary if state else None

    def clear_session(self, organization_id: str, session_id: str) -> bool:
        key = self._key(organization_id, session_id)
        if key in self._store:
            del self._store[key]
            return True
        return False


_session_store: Optional[SessionMemoryStore] = None
_firestore_store: Optional[FirestoreSessionStore] = None


def get_session_store() -> FirestoreSessionStore | SessionMemoryStore:
    """Return Firestore-backed store when Firestore is available, otherwise in-memory fallback."""
    global _firestore_store, _session_store
    try:
        from ..auth.firestore_user import _get_firestore
        db = _get_firestore()
        if db is not None:
            if _firestore_store is None:
                _firestore_store = FirestoreSessionStore()
                _db_id = os.environ.get("FIRESTORE_DATABASE_ID") or "(default)"
                logger.info("Copilot session store: Firestore (database=%s)", _db_id)
            return _firestore_store
    except Exception as e:
        logger.debug("Copilot session store: Firestore unavailable (%s), using in-memory", e)
    if _session_store is None:
        _session_store = SessionMemoryStore()
        logger.info("Copilot session store: in-memory (Firestore not available)")
    return _session_store
