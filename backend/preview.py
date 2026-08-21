from __future__ import annotations

from collections import OrderedDict, deque
from collections.abc import Callable, Collection, Sequence
from dataclasses import dataclass
import time
from typing import Any

from backend.reachability import participant_color

MAX_PREVIEW_ORIGINS = 6
PREVIEW_CACHE_TTL_SECONDS = 300.0
MAX_PREVIEW_CACHE_ENTRIES = 64
PREVIEW_RATE_LIMIT = 30
PREVIEW_RATE_WINDOW_SECONDS = 60.0
MAX_PREVIEW_LIMITER_KEYS = 1024


class PreviewValidationError(ValueError):
    pass


def normalize_preview_origins(
    raw_origins: Sequence[str],
    allowed_stops: Collection[str],
    *,
    reject_duplicates: bool,
) -> tuple[str, ...]:
    if isinstance(raw_origins, (str, bytes)) or not 1 <= len(raw_origins) <= MAX_PREVIEW_ORIGINS:
        raise PreviewValidationError("Choose between one and six starting stops.")
    allowed = set(allowed_stops)
    normalized: list[str] = []
    seen: set[str] = set()
    for value in raw_origins:
        if not isinstance(value, str):
            raise PreviewValidationError("Starting stops are invalid.")
        stop = value.strip()
        if not stop or stop not in allowed:
            raise PreviewValidationError("Choose stops from the Prague stop list.")
        if stop in seen:
            if reject_duplicates:
                raise PreviewValidationError("Choose each starting stop once.")
            continue
        seen.add(stop)
        normalized.append(stop)
    return tuple(normalized)


def build_preview_participants(origins: Sequence[str]) -> list[dict[str, object]]:
    return [
        {
            "id": index + 1,
            "name": stop,
            "color": participant_color(index),
            "start_stop": stop,
            "end_stop": "",
        }
        for index, stop in enumerate(origins)
    ]


@dataclass(frozen=True)
class _CacheEntry:
    expires_at: float
    payload: dict[str, Any]


class PreviewPayloadCache:
    def __init__(
        self,
        *,
        ttl_seconds: float = PREVIEW_CACHE_TTL_SECONDS,
        max_entries: int = MAX_PREVIEW_CACHE_ENTRIES,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._ttl_seconds = ttl_seconds
        self._max_entries = max_entries
        self._clock = clock
        self._entries: OrderedDict[tuple[str, ...], _CacheEntry] = OrderedDict()

    def get(self, key: tuple[str, ...]) -> dict[str, Any] | None:
        entry = self._entries.pop(key, None)
        if entry is None:
            return None
        if entry.expires_at <= self._clock():
            return None
        self._entries[key] = entry
        return entry.payload

    def set(self, key: tuple[str, ...], payload: dict[str, Any]) -> None:
        now = self._clock()
        self._prune_expired(now)
        self._entries.pop(key, None)
        self._entries[key] = _CacheEntry(now + self._ttl_seconds, payload)
        while len(self._entries) > self._max_entries:
            self._entries.popitem(last=False)

    def _prune_expired(self, now: float) -> None:
        for key, entry in list(self._entries.items()):
            if entry.expires_at <= now:
                del self._entries[key]


class PreviewRateLimiter:
    def __init__(
        self,
        *,
        limit: int = PREVIEW_RATE_LIMIT,
        window_seconds: float = PREVIEW_RATE_WINDOW_SECONDS,
        max_clients: int = MAX_PREVIEW_LIMITER_KEYS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._limit = limit
        self._window_seconds = window_seconds
        self._max_clients = max_clients
        self._clock = clock
        self._requests: OrderedDict[str, deque[float]] = OrderedDict()

    def allow(self, client_key: str) -> bool:
        now = self._clock()
        self._prune_inactive(now)
        accepted = self._requests.pop(client_key, None)
        if accepted is None:
            if len(self._requests) >= self._max_clients:
                return False
            accepted = deque()
        self._requests[client_key] = accepted
        if len(accepted) >= self._limit:
            return False
        accepted.append(now)
        return True

    def _prune_inactive(self, now: float) -> None:
        window_start = now - self._window_seconds
        for client_key, accepted in list(self._requests.items()):
            while accepted and accepted[0] <= window_start:
                accepted.popleft()
            if not accepted:
                del self._requests[client_key]
