import asyncio
import logging
import os
import random
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Optional

import httpx

# ---------- Config via env ----------
OZON_DOMAINS = tuple((os.getenv("OZON_DOMAINS") or "api-seller.ozon.ru").split(","))
# Requests-per-minute (устойчивые). 50 ~ 1 rps с запасом.
OZON_RPM = int(os.getenv("OZON_RPM", "50"))
# Разрешённый "всплеск" запросов. По умолчанию 1 — не допускаем два запроса подряд.
OZON_BURST = int(os.getenv("OZON_BURST", "1"))
# Поведение бэкоффа при 429, если сервер не дал Retry-After
OZON_MAX_RETRIES = int(os.getenv("OZON_MAX_RETRIES", "6"))
OZON_BACKOFF_BASE = float(os.getenv("OZON_BACKOFF_BASE", "1.5"))   # сек
OZON_BACKOFF_CAP = float(os.getenv("OZON_BACKOFF_CAP", "180"))     # сек

def _is_ozon_url(url: str) -> bool:
    try:
        return any(d.strip() and d.strip() in url for d in OZON_DOMAINS)
    except Exception:
        return "ozon.ru" in url

class _TokenBucket:
    def __init__(self, rpm: int, burst: int):
        self.capacity = max(1, burst)
        self.tokens = self.capacity
        self.refill_rate = max(1e-6, rpm / 60.0)  # tokens per second
        self.last_refill = time.monotonic()
        self._lock = asyncio.Lock()
        self._lock_sync = None  # lazily

    def _refill(self):
        now = time.monotonic()
        elapsed = now - self.last_refill
        self.last_refill = now
        add = elapsed * self.refill_rate
        self.tokens = min(self.capacity, self.tokens + add)

    async def acquire_async(self):
        async with self._lock:
            self._refill()
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return
            need = 1.0 - self.tokens
            wait_sec = need / self.refill_rate
            await asyncio.sleep(wait_sec)
            self._refill()
            self.tokens = max(0.0, self.tokens - 1.0)

    def acquire_sync(self):
        if self._lock_sync is None:
            import threading
            self._lock_sync = threading.Lock()
        with self._lock_sync:
            self._refill()
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return
            need = 1.0 - self.tokens
            wait_sec = need / self.refill_rate
            time.sleep(wait_sec)
            self._refill()
            self.tokens = max(0.0, self.tokens - 1.0)

_BUCKET = _TokenBucket(OZON_RPM, OZON_BURST)

def _parse_retry_after(headers) -> Optional[float]:
    ra = headers.get("Retry-After")
    if not ra:
        return None
    ra = ra.strip()
    if ra.isdigit():
        return float(ra)
    try:
        dt = parsedate_to_datetime(ra)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        delta = (dt - now).total_seconds()
        return max(0.0, delta)
    except Exception:
        return None

def _parse_reset_headers(headers) -> Optional[float]:
    for key in ("X-RateLimit-Reset", "X-RateLimit-Reset-After", "RateLimit-Reset"):
        val = headers.get(key)
        if not val:
            continue
        try:
            sec = float(val)
            if sec > 10_000:  # unix epoch
                now = time.time()
                return max(0.0, sec - now)
            return max(0.0, sec)
        except Exception:
            continue
    return None

def _compute_backoff(attempt: int) -> float:
    base = min(OZON_BACKOFF_CAP, OZON_BACKOFF_BASE * (2 ** attempt))
    jitter = random.uniform(0, min(1.0, OZON_BACKOFF_BASE))
    return min(OZON_BACKOFF_CAP, base + jitter)

async def _async_request_wrapper(self: httpx.AsyncClient, method: str, url: str, *args, **kwargs):
    if not _is_ozon_url(url):
        return await _orig_async(self, method, url, *args, **kwargs)

    await _BUCKET.acquire_async()
    attempt = 0
    while True:
        resp = await _orig_async(self, method, url, *args, **kwargs)
        if resp.status_code != 429:
            return resp

        retry_after = _parse_retry_after(resp.headers) or _parse_reset_headers(resp.headers)
        if retry_after is None:
            if attempt >= OZON_MAX_RETRIES:
                logging.warning("Ozon 429: max retries reached, returning response")
                return resp
            retry_after = _compute_backoff(attempt)
            attempt += 1

        logging.warning("Ozon 429 for %s %s -> wait %.2fs (attempt %d)", method, url, retry_after, attempt)
        await asyncio.sleep(retry_after)
        await _BUCKET.acquire_async()

def _sync_request_wrapper(self: httpx.Client, method: str, url: str, *args, **kwargs):
    if not _is_ozon_url(url):
        return _orig_sync(self, method, url, *args, **kwargs)

    _BUCKET.acquire_sync()
    attempt = 0
    while True:
        resp = _orig_sync(self, method, url, *args, **kwargs)
        if resp.status_code != 429:
            return resp

        retry_after = _parse_retry_after(resp.headers) or _parse_reset_headers(resp.headers)
        if retry_after is None:
            if attempt >= OZON_MAX_RETRIES:
                logging.warning("Ozon 429: max retries reached, returning response")
                return resp
            retry_after = _compute_backoff(attempt)
            attempt += 1

        logging.warning("Ozon 429 for %s %s -> wait %.2fs (attempt %d)", method, url, retry_after, attempt)
        time.sleep(retry_after)
        _BUCKET.acquire_sync()

# ---- Install patches once ----
if not getattr(httpx, "_ozon_rl_patched", False):
    _orig_async = httpx.AsyncClient.request
    _orig_sync = httpx.Client.request

    httpx.AsyncClient.request = _async_request_wrapper  # type: ignore[method-assign]
    httpx.Client.request = _sync_request_wrapper        # type: ignore[method-assign]

    setattr(httpx, "_ozon_rl_patched", True)
    logging.info(
        "httpx_rate_limit_patch: installed (rpm=%s, burst=%s, max_retries=%s, base=%.2fs, cap=%.2fs)",
        OZON_RPM, OZON_BURST, OZON_MAX_RETRIES, OZON_BACKOFF_BASE, OZON_BACKOFF_CAP
    )