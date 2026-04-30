"""Shared HTTP client with polite defaults."""

from __future__ import annotations

import time
from typing import Any

import httpx

from .config import Settings

_settings = Settings.from_env()
USER_AGENT = (
    f"codebook-archive/0.1 (+https://github.com/evavogel/codebook-archive; "
    f"mailto:{_settings.polite_pool_email})"
)


def make_client(timeout: float = 30.0) -> httpx.Client:
    return httpx.Client(
        timeout=timeout,
        headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
        follow_redirects=True,
    )


class HTTPClientError(Exception):
    """Raised when a request fails with a non-retryable client error (4xx other than 429)."""


def get_json(
    client: httpx.Client,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    max_retries: int = 3,
    initial_delay: float = 0.5,
    max_delay: float = 8.0,
) -> dict[str, Any]:
    """GET a JSON resource.

    Retries with capped exponential backoff on 429 and 5xx.
    Fails fast (no retries) on 4xx errors other than 429 — those are
    typically permanent (deleted, private, not found).
    """
    delay = initial_delay
    last_exc: Exception | None = None
    for attempt in range(max_retries):
        try:
            r = client.get(url, params=params, headers=headers)
        except (httpx.TransportError, httpx.TimeoutException) as e:
            last_exc = e
            time.sleep(min(delay, max_delay))
            delay *= 2
            continue

        if r.status_code == 429 or r.status_code >= 500:
            retry_after_hdr = r.headers.get("Retry-After")
            try:
                retry_after = float(retry_after_hdr) if retry_after_hdr else delay
            except ValueError:
                retry_after = delay
            time.sleep(min(retry_after, max_delay))
            delay *= 2
            last_exc = httpx.HTTPStatusError(
                f"{r.status_code} on retry", request=r.request, response=r
            )
            continue

        if 400 <= r.status_code < 500:
            raise HTTPClientError(f"GET {url} -> {r.status_code}")

        return r.json()

    raise RuntimeError(f"GET {url} failed after {max_retries} retries") from last_exc
