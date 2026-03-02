"""
Schema cache for Copilot V2: cache discover_tables results.
Uses Redis if available (via cache_backend), else in-memory LRU with TTL.
"""
from __future__ import annotations

import json
import os
import threading
import time
from typing import Any, List


from .defaults import get_schema_cache_ttl_seconds

# In-memory: key -> (expiry_ts, value)
_memory: dict[str, tuple[float, Any]] = {}
_lock = threading.Lock()
_MAX_MEMORY_ENTRIES = 200
_PREFIX = "copilot:schema:"


_redis_client: Any = None
_redis_available: bool | None = None


def _get_redis():
    """Redis client when REDIS_URL is set; otherwise None (use in-memory)."""
    global _redis_client, _redis_available
    if _redis_available is False:
        return None
    if _redis_client is not None:
        return _redis_client
    url = os.environ.get("REDIS_URL")
    if not url:
        _redis_available = False
        return None
    try:
        import redis
        _redis_client = redis.from_url(url, decode_responses=True)
        _redis_client.ping()
        _redis_available = True
        return _redis_client
    except Exception:
        _redis_available = False
        return None


def _cache_key(intent: str) -> str:
    """Cache key for an intent (normalized)."""
    normalized = (intent or "").strip().lower()[:200]
    return f"{_PREFIX}{normalized}"


def schema_cache_get(intent: str) -> List[dict] | None:
    """
    Get cached discover_tables result for intent.
    Returns list of candidate table dicts or None if miss/expired.
    """
    key = _cache_key(intent)
    ttl = get_schema_cache_ttl_seconds()
    r = _get_redis()
    if r:
        try:
            raw = r.get(key)
            if raw is not None:
                return json.loads(raw)
        except Exception:
            pass
        return None
    with _lock:
        if key in _memory:
            expiry, val = _memory[key]
            if time.monotonic() < expiry:
                return val
            del _memory[key]
    return None


def schema_cache_set(intent: str, candidates: List[dict]) -> None:
    """Cache discover_tables result for intent."""
    key = _cache_key(intent)
    ttl = get_schema_cache_ttl_seconds()
    r = _get_redis()
    if r:
        try:
            r.set(key, json.dumps(candidates, default=str), ex=ttl)
        except Exception:
            pass
        return
    with _lock:
        # Evict oldest if at capacity
        if len(_memory) >= _MAX_MEMORY_ENTRIES and key not in _memory:
            oldest = min(_memory.keys(), key=lambda k: _memory[k][0])
            del _memory[oldest]
        _memory[key] = (time.monotonic() + ttl, candidates)


def schema_cache_refresh() -> None:
    """Force clear in-memory schema cache (call to refresh discovery). Redis keys are TTL-based."""
    with _lock:
        to_del = [k for k in _memory if k.startswith(_PREFIX)]
        for k in to_del:
            del _memory[k]
