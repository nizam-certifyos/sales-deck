from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict


@dataclass
class RateLimitResult:
    waited_seconds: float
    queue_depth: int


class SlidingWindowRateLimiter:
    """Simple thread-safe sliding-window limiter for RPM controls."""

    def __init__(self, requests_per_minute: int, max_wait_seconds: float):
        self.requests_per_minute = max(1, int(requests_per_minute))
        self.max_wait_seconds = max(0.1, float(max_wait_seconds))
        self._events: Deque[float] = deque()
        self._lock = threading.Lock()

    def _prune(self, now: float) -> None:
        cutoff = now - 60.0
        while self._events and self._events[0] <= cutoff:
            self._events.popleft()

    def acquire(self) -> RateLimitResult:
        started = time.monotonic()
        while True:
            now = time.monotonic()
            with self._lock:
                self._prune(now)
                queue_depth = max(0, len(self._events) - self.requests_per_minute + 1)
                if len(self._events) < self.requests_per_minute:
                    self._events.append(now)
                    waited = max(0.0, now - started)
                    return RateLimitResult(waited_seconds=waited, queue_depth=queue_depth)

                earliest = self._events[0]
                wait_for = max(0.01, (earliest + 60.0) - now)

            elapsed = now - started
            remaining = self.max_wait_seconds - elapsed
            if remaining <= 0:
                raise TimeoutError(
                    f"Rate limit exceeded ({self.requests_per_minute} RPM) and wait budget exhausted"
                )
            time.sleep(min(wait_for, remaining, 0.5))


_limiters: Dict[str, SlidingWindowRateLimiter] = {}
_limiters_lock = threading.Lock()


def limiter_for(name: str, requests_per_minute: int, max_wait_seconds: float) -> SlidingWindowRateLimiter:
    key = str(name or "default")
    with _limiters_lock:
        limiter = _limiters.get(key)
        if limiter is None:
            limiter = SlidingWindowRateLimiter(requests_per_minute=requests_per_minute, max_wait_seconds=max_wait_seconds)
            _limiters[key] = limiter
        else:
            limiter.requests_per_minute = max(1, int(requests_per_minute))
            limiter.max_wait_seconds = max(0.1, float(max_wait_seconds))
        return limiter
