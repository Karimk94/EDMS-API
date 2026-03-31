import time
from threading import Lock
from typing import Any, Dict, Optional, Tuple


class TTLCache:
    """Small in-memory TTL cache for single-process deployments."""

    def __init__(self, default_ttl_seconds: int = 300, max_items: int = 512):
        self.default_ttl_seconds = default_ttl_seconds
        self.max_items = max_items
        self._store: Dict[str, Tuple[float, Any]] = {}
        self._lock = Lock()

    def get(self, key: str) -> Optional[Any]:
        now = time.time()
        with self._lock:
            payload = self._store.get(key)
            if not payload:
                return None

            expires_at, value = payload
            if expires_at <= now:
                self._store.pop(key, None)
                return None

            return value

    def set(self, key: str, value: Any, ttl_seconds: Optional[int] = None) -> None:
        ttl = ttl_seconds if ttl_seconds is not None else self.default_ttl_seconds
        expires_at = time.time() + max(1, ttl)

        with self._lock:
            if len(self._store) >= self.max_items:
                self._evict_expired_or_oldest_locked()
            self._store[key] = (expires_at, value)

    def invalidate(self, key: str) -> None:
        with self._lock:
            self._store.pop(key, None)

    def invalidate_prefix(self, prefix: str) -> None:
        with self._lock:
            keys = [k for k in self._store if k.startswith(prefix)]
            for key in keys:
                self._store.pop(key, None)

    def _evict_expired_or_oldest_locked(self) -> None:
        now = time.time()
        expired_keys = [k for k, (expires_at, _) in self._store.items() if expires_at <= now]
        for key in expired_keys:
            self._store.pop(key, None)

        if len(self._store) < self.max_items:
            return

        oldest_key = min(self._store.items(), key=lambda item: item[1][0])[0]
        self._store.pop(oldest_key, None)
