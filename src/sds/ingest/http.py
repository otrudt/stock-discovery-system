"""Polite HTTP: a declared User-Agent, a rate limit, and backoff on throttling."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable

import httpx

RETRY_STATUS = {429, 500, 502, 503, 504}


class RateLimiter:
    """At most `per_second` calls per second, spaced evenly."""

    def __init__(
        self,
        per_second: float,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.interval = 1.0 / per_second
        self._clock = clock
        self._sleep = sleep
        self._next = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            now = self._clock()
            if now < self._next:
                self._sleep(self._next - now)
                now = self._next
            self._next = now + self.interval


def _raise_for_status(resp: httpx.Response) -> None:
    """Like httpx's raise_for_status, but the message drops the query string.

    Query strings can carry API tokens, and error text ends up in the run log and the
    quarantine table. `from None` keeps the original message out of tracebacks too.
    """
    try:
        resp.raise_for_status()
    except httpx.HTTPStatusError:
        url = resp.request.url.copy_with(query=None)
        raise httpx.HTTPStatusError(
            f"HTTP {resp.status_code} {resp.reason_phrase} for url '{url}'",
            request=resp.request,
            response=resp,
        ) from None


class PoliteClient:
    def __init__(
        self,
        user_agent: str,
        per_second: float,
        *,
        max_retries: int = 4,
        transport: httpx.BaseTransport | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.limiter = RateLimiter(per_second, sleep=sleep)
        self.max_retries = max_retries
        self._sleep = sleep
        self._client = httpx.Client(
            headers={"User-Agent": user_agent, "Accept-Encoding": "gzip, deflate"},
            timeout=30.0,
            follow_redirects=True,
            transport=transport,
        )

    def get(self, url: str, params: dict[str, str] | None = None) -> bytes:
        for attempt in range(self.max_retries + 1):
            self.limiter.wait()
            resp = self._client.get(url, params=params)
            if resp.status_code not in RETRY_STATUS:
                _raise_for_status(resp)
                return resp.content
            if attempt == self.max_retries:
                _raise_for_status(resp)
            retry_after = resp.headers.get("Retry-After", "")
            delay = float(retry_after) if retry_after.isdigit() else 2.0**attempt
            self._sleep(min(delay, 60.0))
        raise AssertionError("unreachable")

    def close(self) -> None:
        self._client.close()
