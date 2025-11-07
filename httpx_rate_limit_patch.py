# -*- coding: utf-8 -*-
"""
httpx_rate_limit_patch
Анти-429 патч для httpx с «лёгкими» дефолтами и жёстким ограничением внутренних ожиданий.

Что делает:
- Каппирует любые внутренние ожидания (preflight, limiter, create-gate, avoid-second) RL_MAX_SLEEP_SEC (по умолчанию 1.0s).
- На 429 использует Retry-After, но с потолком RL_MAX_RETRY_AFTER_SEC (по умолчанию 6s) и выставляет пенальти у хоста.
- Поддерживает «create-gate» (межпроцессно синхронизированный слот) для эндпоинтов создания.
- «Секунду создателя» можно защищать от параллельных не-create запросов (опционально).
- По умолчанию поведение лёгкое: create-gate выключен, задержки минимальны.

Актуализации под наш флоу:
- В список «создающих» (CREATE_ENDPOINTS) добавлен /v1/cargoes/create и /v1/cargoes-label/create,
  чтобы эти вызовы могли использовать общий create-gate (если он включён).
- Опционально можно расширить список через ENV: OZON_EXTRA_CREATE_ENDPOINTS="/v1/foo,/v1/bar".
- Не включаем create-gate по умолчанию (RL_DISABLE_CREATE_GATE=True), чтобы не тормозить пайплайн.
  Интервалы между попытками cargoes/create остаются на уровне оркестратора (supply_watch), как и обсуждалось.

ENV ключи (важные):
- RL_DISABLE_CREATE_GATE (default: 1/True) — выключить межпроцессной create-gate.
- RL_MAX_SLEEP_SEC (default: 1.0) — кап на любые ожидания внутри патча.
- RL_MAX_CREATE_WAIT_MS (default: 750) — максимум для create-gate ожидания.
- RL_MAX_RETRY_AFTER_SEC (default: 6.0) — кап для Retry-After/пенальти.
- OZON_RPM (default: 8), OZON_BURST (default: 1) — базовые лимиты в секунду/окно.
- OZON_PER_SECOND_COOLDOWN_SEC (default: 0.5) — базовая прослойка между запросами.
- AVOID_CREATE_SECOND_FOR_OTHERS (default: False) — избегать «секунды создателя» для прочих запросов.
"""
from __future__ import annotations

import asyncio
import time
import os
import json
import random
import logging
from collections import deque
from pathlib import Path
from typing import Dict, Set, Any, Optional, Tuple, List
from urllib.parse import urlparse
from email.utils import parsedate_to_datetime

import httpx

try:
    import fcntl  # type: ignore
except Exception:
    fcntl = None

# ================== ENV HELPERS ==================

def _getenv_str(n: str, d: str = "") -> str:
    v = os.getenv(n)
    return v if v is not None else d

def _getenv_int(n: str, d: int) -> int:
    v = os.getenv(n)
    if v is None or v == "":
        return d
    try:
        return int(v)
    except Exception:
        try:
            return int(float(v))
        except Exception:
            return d

def _getenv_float(n: str, d: float) -> float:
    v = os.getenv(n)
    if v is None or v == "":
        return d
    try:
        return float(v)
    except Exception:
        return d

def _getenv_bool(n: str, d: bool = False) -> bool:
    v = os.getenv(n)
    if v is None or v == "":
        return d
    return str(v).strip().lower() in ("1", "true", "yes", "on")

# ================== CONFIG (с «лёгкими» дефолтами) ==================

DATA_DIR = Path(_getenv_str("DATA_DIR", "./data")).resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)

OZON_DOMAINS: Set[str] = set(
    s.strip() for s in _getenv_str("OZON_DOMAINS", "api-seller.ozon.ru").split(",") if s.strip()
)

RPM = _getenv_int("OZON_RPM", 8)
BURST = max(1, _getenv_int("OZON_BURST", 1))

# Базовые интервалы (облегчённые дефолты)
PER_SECOND_COOLDOWN = max(0.0, _getenv_float("OZON_PER_SECOND_COOLDOWN_SEC", 0.5))
CREATE_GAP = max(0.0, _getenv_float("OZON_CREATE_ENDPOINT_MIN_GAP_SEC", 0.0))

# Доп. параметры ожиданий
ALIGN_SECOND_BOUNDARY = _getenv_bool("OZON_ALIGN_SECOND_BOUNDARY", False)
JITTER_MS = max(0, _getenv_int("OZON_JITTER_MS", 0))

CREATE_SLOT_ENV = os.getenv("OZON_CREATE_SLOT_SEC", "").strip()
CREATE_SLOT_OFFSET_MS = max(0, _getenv_int("OZON_CREATE_SLOT_OFFSET_MS", 0))
CREATE_COLD_START_STAGGER_SEC = max(0, _getenv_int("OZON_CREATE_COLD_START_STAGGER_SEC", 0))

# Поведение после 429
ON429_SKIP_MINUTES = max(0, _getenv_int("OZON_CREATE_ON429_SKIP_MINUTES", 0))
ON429_BLACKLIST_TTL_SEC = max(0, _getenv_int("OZON_CREATE_ON429_BLACKLIST_TTL_SEC", 5))
FORCE_ROTATE_ON429 = _getenv_bool("OZON_CREATE_FORCE_ROTATE_ON429", False)

# Защита "секунды создающих" для прочих запросов (по умолчанию выключена)
AVOID_CREATE_SECOND_FOR_OTHERS = _getenv_bool("AVOID_CREATE_SECOND_FOR_OTHERS", False)
AVOID_MARGIN_MS = max(0, _getenv_int("AVOID_MARGIN_MS", 50))

# Новые ограничения ожиданий
RL_DISABLE_CREATE_GATE = _getenv_bool("RL_DISABLE_CREATE_GATE", True)
RL_MAX_CREATE_WAIT_MS = max(0, _getenv_int("RL_MAX_CREATE_WAIT_MS", 750))
RL_MAX_SLEEP_SEC = max(0, _getenv_float("RL_MAX_SLEEP_SEC", 1.0))
RL_MAX_RETRY_AFTER_SEC = max(0, _getenv_float("RL_MAX_RETRY_AFTER_SEC", 6.0))

# Эндпоинты "создания"
CREATE_ENDPOINTS: Set[str] = {
    "/v1/draft/create",
    "/v1/draft/supply/create",
    "/v1/cargoes/create",
    "/v1/cargoes-label/create",
}

# Расширяемый список create-эндпоинтов из ENV (через запятую)
_extra_create = [s.strip() for s in _getenv_str("OZON_EXTRA_CREATE_ENDPOINTS", "").split(",") if s.strip()]
if _extra_create:
    CREATE_ENDPOINTS |= set(_extra_create)

# ================== УТИЛИТЫ ==================

def _is_tracked_domain(host: str) -> bool:
    if not OZON_DOMAINS:
        return True
    return host in OZON_DOMAINS

def _path_of(url: str) -> str:
    try:
        return urlparse(url).path or "/"
    except Exception:
        try:
            return str(httpx.URL(url).path)
        except Exception:
            return "/"

def _host_of(url: str) -> str:
    try:
        return urlparse(url).hostname or ""
    except Exception:
        try:
            return str(httpx.URL(url).host or "")
        except Exception:
            return ""

def _clamped_sleep_seconds(wait: float) -> float:
    # Возвращаем фактическое время сна (с учётом RL_MAX_SLEEP_SEC)
    return max(0.0, min(float(wait), float(RL_MAX_SLEEP_SEC)))

def _retry_after_seconds(resp: httpx.Response) -> float:
    # Минимум — PER_SECOND_COOLDOWN, максимум — RL_MAX_RETRY_AFTER_SEC
    ra: Optional[float] = None
    try:
        v = resp.headers.get("Retry-After")
        if v and str(v).strip().isdigit():
            ra = float(int(v))
    except Exception:
        ra = None
    base = PER_SECOND_COOLDOWN
    if ra is None:
        return min(max(base, 0.0), RL_MAX_RETRY_AFTER_SEC)
    return min(max(ra, base), RL_MAX_RETRY_AFTER_SEC)

# ================== МЕЖПРОЦЕССНЫЙ CREATE-GATE ==================

class _CrossProcCreateGate:
    state_file = DATA_DIR / ".supw_create_gate.json"
    lock_file = DATA_DIR / ".supw_create_gate.lock"
    _first_create_done = False

    @classmethod
    def _ensure_files(cls):
        cls.state_file.parent.mkdir(parents=True, exist_ok=True)
        if not cls.state_file.exists():
            try:
                cls.state_file.write_text("{}", encoding="utf-8")
            except Exception:
                pass
        if not cls.lock_file.exists():
            try:
                cls.lock_file.write_text("", encoding="utf-8")
            except Exception:
                pass

    @classmethod
    def _load_state(cls) -> Dict[str, Any]:
        try:
            raw = cls.state_file.read_text(encoding="utf-8")
            js = json.loads(raw or "{}")
            return js if isinstance(js, dict) else {}
        except Exception:
            return {}

    @classmethod
    def _save_state(cls, st: Dict[str, Any]) -> None:
        tmp = cls.state_file.with_suffix(".tmp")
        tmp.write_text(json.dumps(st, ensure_ascii=False), encoding="utf-8")
        tmp.replace(cls.state_file)

    @classmethod
    def _calc_next_slot_time(cls, now: float, slot_sec: int, offset_ms: int) -> float:
        now_floor = int(now)
        sec_in_min = now_floor % 60
        base = now_floor - sec_in_min + slot_sec
        if base <= now:
            base += 60
        return base + (offset_ms / 1000.0)

    @classmethod
    def _blacklist_cleanup(cls, st: Dict[str, Any], now: float) -> None:
        bl = st.get("CREATE_blacklist") or {}
        if not isinstance(bl, dict):
            st["CREATE_blacklist"] = {}
            return
        changed = False
        for k in list(bl.keys()):
            try:
                if float(bl[k]) <= now:
                    del bl[k]
                    changed = True
            except Exception:
                del bl[k]
                changed = True
        if changed:
            st["CREATE_blacklist"] = bl

    @classmethod
    def _is_blacklisted(cls, st: Dict[str, Any], sec: int, now: float) -> bool:
        if ON429_BLACKLIST_TTL_SEC <= 0:
            return False
        bl = st.get("CREATE_blacklist") or {}
        if not isinstance(bl, dict):
            return False
        exp = bl.get(str(int(sec) % 60))
        try:
            return bool(exp and float(exp) > now)
        except Exception:
            return False

    @classmethod
    def _pick_slot_locked(cls, st: Dict[str, Any]) -> int:
        if CREATE_SLOT_ENV != "":
            try:
                slot = int(CREATE_SLOT_ENV) % 60
                st["CREATE_slot"] = slot
                return slot
            except Exception:
                pass
        if "CREATE_slot" in st:
            try:
                cand = int(st["CREATE_slot"]) % 60
                now = time.time()
                if not cls._is_blacklisted(st, cand, now):
                    return cand
            except Exception:
                pass
        now = time.time()
        choices = [s for s in range(60) if not cls._is_blacklisted(st, s, now)]
        slot = random.choice(choices) if choices else random.randint(0, 59)
        st["CREATE_slot"] = slot
        return slot

    @classmethod
    def rotate_slot(cls, st: Optional[Dict[str, Any]] = None, new_slot: Optional[int] = None) -> int:
        # Если слот зафиксирован, но FORCE_ROTATE_ON429=1 — разрешим ротацию; иначе — уважаем фикс.
        if CREATE_SLOT_ENV != "" and not FORCE_ROTATE_ON429:
            try:
                return int(CREATE_SLOT_ENV) % 60
            except Exception:
                return -1
        cls._ensure_files()
        if fcntl is None:
            return -1
        with open(cls.lock_file, "a+") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            try:
                if st is None:
                    st = cls._load_state()
                now = time.time()
                cls._blacklist_cleanup(st, now)
                try:
                    current = int(st.get("CREATE_slot", -1)) % 60
                except Exception:
                    current = -1
                if new_slot is None:
                    choices = [s for s in range(60) if s != current and not cls._is_blacklisted(st, s, now)]
                    if not choices:
                        choices = [s for s in range(60) if s != current]
                    new_slot = random.choice(choices) if choices else random.randint(0, 59)
                st["CREATE_slot"] = int(new_slot) % 60
                cls._save_state(st)
                logging.info("httpx_rate_limit_patch: rotated CREATE_slot to %s", st["CREATE_slot"])
                return int(st["CREATE_slot"])
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)

    @classmethod
    def on_429(cls, resp: httpx.Response):
        if fcntl is None:
            return
        with open(cls.lock_file, "a+") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            try:
                st = cls._load_state()
                now = time.time()
                cls._blacklist_cleanup(st, now)
                try:
                    if ON429_BLACKLIST_TTL_SEC > 0:
                        date_hdr = str(resp.headers.get("Date") or "").strip()
                        if date_hdr:
                            dt = parsedate_to_datetime(date_hdr)
                            sec = dt.second
                            bl = st.get("CREATE_blacklist") or {}
                            if not isinstance(bl, dict):
                                bl = {}
                            bl[str(int(sec) % 60)] = now + float(ON429_BLACKLIST_TTL_SEC)
                            st["CREATE_blacklist"] = bl
                            logging.info(
                                "httpx_rate_limit_patch: blacklist sec=%s for %ss",
                                sec,
                                ON429_BLACKLIST_TTL_SEC,
                            )
                except Exception:
                    pass
                cls.rotate_slot(st, None if CREATE_SLOT_ENV == "" else None)
                skip = int(st.get("CREATE_skip_minutes", 0) or 0)
                st["CREATE_skip_minutes"] = max(skip, int(ON429_SKIP_MINUTES))
                cls._save_state(st)
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)

    @classmethod
    def wait_sync(cls, group: str, gap: float):
        # Полностью отключаем gate, если так задано
        if RL_DISABLE_CREATE_GATE or gap <= 0:
            return
        cls._ensure_files()
        if fcntl is None:
            # Без межпроцессной блокировки — лишь минимальная пауза, с капом
            now = time.time()
            wait_gap = max(0.0, (0.0 + gap) - now)  # no-op по существу
            capped = _clamped_sleep_seconds(wait_gap)
            if capped > 0:
                time.sleep(capped)
            return

        with open(cls.lock_file, "a+") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            try:
                st = cls._load_state()
                now = time.time()
                cls._blacklist_cleanup(st, now)

                if not cls._first_create_done and CREATE_COLD_START_STAGGER_SEC > 0:
                    delay = random.uniform(0.0, float(CREATE_COLD_START_STAGGER_SEC))
                    time.sleep(_clamped_sleep_seconds(delay))
                    cls._first_create_done = True

                last = float(st.get(f"{group}_last_ts", 0.0) or 0.0)
                next_allowed_by_gap = last + float(gap)

                slot = cls._pick_slot_locked(st)
                target = cls._calc_next_slot_time(max(now, next_allowed_by_gap), slot, int(CREATE_SLOT_OFFSET_MS))

                skip_minutes = int(st.get("CREATE_skip_minutes", 0) or 0)
                if skip_minutes > 0:
                    target += 60.0 * float(skip_minutes)
                    st["CREATE_skip_minutes"] = 0
                    logging.info(
                        "httpx_rate_limit_patch: skip %d minute(s) after 429; next at slot=%d",
                        skip_minutes,
                        slot,
                    )

                wait = max(0.0, target - now)
                # Каппируем ожидание create-gate
                max_create_wait = max(0.0, RL_MAX_CREATE_WAIT_MS / 1000.0)
                if wait > max_create_wait:
                    logging.info(
                        "httpx_rate_limit_patch: create-gate wait %.2fs > cap %.2fs -> skip long wait",
                        wait,
                        max_create_wait,
                    )
                    # Пропускаем длинное ожидание, просто регистрируем момент
                    st[f"{group}_last_ts"] = time.time()
                    st["CREATE_slot"] = slot
                    cls._save_state(st)
                    return

                capped_wait = _clamped_sleep_seconds(wait)
                if capped_wait > 0:
                    logging.info(
                        "httpx_rate_limit_patch: create-gate wait %.2fs (slot=%s, gap=%.1fs)",
                        capped_wait,
                        slot,
                        gap,
                    )
                    time.sleep(capped_wait)

                st[f"{group}_last_ts"] = time.time()
                st["CREATE_slot"] = slot
                cls._save_state(st)
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)

    @classmethod
    async def wait(cls, group: str, gap: float):
        # Асинхронная обёртка над sync, чтобы не блокировать event loop
        await asyncio.to_thread(cls.wait_sync, group, gap)

    @classmethod
    def read_current_slot(cls) -> Optional[int]:
        try:
            raw = cls.state_file.read_text(encoding="utf-8")
            js = json.loads(raw or "{}")
            if isinstance(js, dict) and "CREATE_slot" in js:
                return int(js["CREATE_slot"]) % 60
        except Exception:
            pass
        if CREATE_SLOT_ENV != "":
            try:
                return int(CREATE_SLOT_ENV) % 60
            except Exception:
                return None
        return None

# ================== ХОСТ-ЛИМИТЕР (с капом ожиданий) ==================

class _HostLimiter:
    def __init__(self, rpm: int, burst: int):
        self.min_delta = 60.0 / max(1, rpm)
        self.burst = max(1, burst)
        self._deque: deque[float] = deque()
        self._penalty_until: float = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = time.time()
            # Пенальти: ждём, но с капом
            if now < self._penalty_until:
                wait = self._penalty_until - now
                capped = _clamped_sleep_seconds(wait)
                if capped > 0:
                    await asyncio.sleep(capped)
                # Не заставляем ждать остаток пенальти — снимаем его
                self._penalty_until = time.time()

            now = time.time()
            cutoff = now - self.min_delta
            while self._deque and self._deque[0] < cutoff:
                self._deque.popleft()

            if len(self._deque) >= self.burst:
                earliest = self._deque[0]
                wait = (earliest + self.min_delta) - now
                if wait > 0:
                    capped = _clamped_sleep_seconds(wait)
                    if capped > 0:
                        await asyncio.sleep(capped)
                    # После капа — очищаем устаревшие и продолжаем
                    now = time.time()
                    cutoff2 = now - self.min_delta
                    while self._deque and self._deque[0] < cutoff2:
                        self._deque.popleft()

            self._deque.append(time.time())

    def penalize(self, seconds: float):
        seconds = max(0.0, float(seconds))
        seconds = min(seconds, RL_MAX_RETRY_AFTER_SEC)  # кап Retry-After
        self._penalty_until = max(self._penalty_until, time.time() + seconds)

_host_limiters: Dict[str, _HostLimiter] = {}

def _get_limiter_for_host(host: str) -> _HostLimiter:
    if host not in _host_limiters:
        _host_limiters[host] = _HostLimiter(RPM, BURST)
    return _host_limiters[host]

# ================== PREFLIGHT ==================

async def _avoid_create_second_for_non_create(path: str):
    if not AVOID_CREATE_SECOND_FOR_OTHERS:
        return
    if path in CREATE_ENDPOINTS:
        return
    slot = _CrossProcCreateGate.read_current_slot()
    if slot is None:
        return
    now = time.time()
    if (int(now) % 60) == (int(slot) % 60):
        frac = now - int(now)
        margin = AVOID_MARGIN_MS / 1000.0
        wait = (1.0 - frac) + margin
        capped = _clamped_sleep_seconds(wait)
        if capped > 0:
            logging.info(
                "httpx_rate_limit_patch: avoid create-second=%s for non-create; sleep %.3fs",
                slot,
                capped,
            )
            await asyncio.sleep(capped)

async def _preflight_throttle(host: str, path: str):
    # Защита «секунды создателя» (опционально)
    await _avoid_create_second_for_non_create(path)

    # Тонкая подстройка начала секунды (опционально)
    if ALIGN_SECOND_BOUNDARY:
        now = time.time()
        frac = now - int(now)
        if frac < 0.02:
            capped = _clamped_sleep_seconds(0.02 - frac)
            if capped > 0:
                await asyncio.sleep(capped)

    # Небольшой джиттер (опционально)
    if JITTER_MS > 0:
        capped = _clamped_sleep_seconds(JITTER_MS / 1000.0)
        if capped > 0:
            await asyncio.sleep(capped)

    # Общий лимитер по хосту (burst+rpm)
    lim = _get_limiter_for_host(host)
    await lim.acquire()

    # Межпроцессное окно для create (если включено)
    if path in CREATE_ENDPOINTS and not RL_DISABLE_CREATE_GATE:
        if CREATE_COLD_START_STAGGER_SEC > 0 and not _CrossProcCreateGate._first_create_done:
            delay = random.uniform(0, float(CREATE_COLD_START_STAGGER_SEC))
            capped = _clamped_sleep_seconds(delay)
            if capped > 0:
                await asyncio.sleep(capped)
            _CrossProcCreateGate._first_create_done = True
        await _CrossProcCreateGate.wait("CREATE", CREATE_GAP)

# ================== ПАТЧИРОВАННЫЕ ЗАПРОСЫ ==================

_orig_async_request = httpx.AsyncClient.request
_orig_sync_request = httpx.Client.request

async def _patched_async_request(self: httpx.AsyncClient, method: str, url, *args, **kwargs):
    url_str = str(url)
    host = _host_of(url_str)
    path = _path_of(url_str)
    if _is_tracked_domain(host):
        await _preflight_throttle(host, path)

    resp = await _orig_async_request(self, method, url, *args, **kwargs)

    if _is_tracked_domain(host) and getattr(resp, "status_code", 0) == 429:
        cooldown = _retry_after_seconds(resp)
        _get_limiter_for_host(host).penalize(cooldown)
        if path in CREATE_ENDPOINTS:
            try:
                _CrossProcCreateGate.on_429(resp)
            except Exception:
                logging.exception("httpx_rate_limit_patch: on_429 failed")
        logging.warning("httpx_rate_limit_patch: penalize %.2fs (status 429)", cooldown)
    return resp

def _patched_sync_request(self: httpx.Client, method: str, url, *args, **kwargs):
    url_str = str(url)
    host = _host_of(url_str)
    path = _path_of(url_str)
    if _is_tracked_domain(host):
        lim = _get_limiter_for_host(host)

        # Пенальти с капом
        now = time.time()
        if now < lim._penalty_until:
            wait = lim._penalty_until - now
            capped = _clamped_sleep_seconds(wait)
            if capped > 0:
                time.sleep(capped)
            lim._penalty_until = time.time()

        # Burst лимиты
        now = time.time()
        cutoff = now - lim.min_delta
        while lim._deque and lim._deque[0] < cutoff:
            lim._deque.popleft()
        if len(lim._deque) >= lim.burst:
            earliest = lim._deque[0]
            wait = (earliest + lim.min_delta) - now
            if wait > 0:
                capped = _clamped_sleep_seconds(wait)
                if capped > 0:
                    time.sleep(capped)
                now = time.time()
                cutoff2 = now - lim.min_delta
                while lim._deque and lim._deque[0] < cutoff2:
                    lim._deque.popleft()
        lim._deque.append(time.time())

        # Прочие префлайты (опциональные)
        if ALIGN_SECOND_BOUNDARY:
            frac = time.time() - int(time.time())
            if frac < 0.02:
                capped = _clamped_sleep_seconds(0.02 - frac)
                if capped > 0:
                    time.sleep(capped)
        if JITTER_MS > 0:
            capped = _clamped_sleep_seconds(JITTER_MS / 1000.0)
            if capped > 0:
                time.sleep(capped)
        if path in CREATE_ENDPOINTS and not RL_DISABLE_CREATE_GATE:
            if _CrossProcCreateGate._first_create_done is False and CREATE_COLD_START_STAGGER_SEC > 0:
                delay = random.uniform(0, float(CREATE_COLD_START_STAGGER_SEC))
                capped = _clamped_sleep_seconds(delay)
                if capped > 0:
                    time.sleep(capped)
                _CrossProcCreateGate._first_create_done = True
            _CrossProcCreateGate.wait_sync("CREATE", CREATE_GAP)

    resp = _orig_sync_request(self, method, url, *args, **kwargs)
    if _is_tracked_domain(host) and getattr(resp, "status_code", 0) == 429:
        cooldown = _retry_after_seconds(resp)
        _get_limiter_for_host(host).penalize(cooldown)
        if path in CREATE_ENDPOINTS:
            try:
                _CrossProcCreateGate.on_429(resp)
            except Exception:
                logging.exception("httpx_rate_limit_patch: on_429 failed (sync)")
        logging.warning("httpx_rate_limit_patch: penalize %.2fs (status 429)", cooldown)
    return resp

# ================== ПУБЛИЧНЫЕ API ==================

def configure(
    *,
    rpm: Optional[int] = None,
    burst: Optional[int] = None,
    per_second_cooldown: Optional[float] = None,
    create_gap: Optional[float] = None,
    align: Optional[bool] = None,
    on429_skip_minutes: Optional[int] = None,
    bl_ttl: Optional[int] = None,
    force_rotate_on429: Optional[bool] = None,
    avoid_create_second_for_others: Optional[bool] = None,
    avoid_margin_ms: Optional[int] = None,
    disable_create_gate: Optional[bool] = None,
    max_create_wait_ms: Optional[int] = None,
    max_sleep_sec: Optional[float] = None,
    max_retry_after_sec: Optional[float] = None,
    extra_create_endpoints: Optional[List[str]] = None,
) -> None:
    """Опциональная конфигурация во время рантайма."""
    global RPM, BURST, PER_SECOND_COOLDOWN, CREATE_GAP, ALIGN_SECOND_BOUNDARY
    global ON429_SKIP_MINUTES, ON429_BLACKLIST_TTL_SEC, FORCE_ROTATE_ON429
    global AVOID_CREATE_SECOND_FOR_OTHERS, AVOID_MARGIN_MS
    global RL_DISABLE_CREATE_GATE, RL_MAX_CREATE_WAIT_MS, RL_MAX_SLEEP_SEC, RL_MAX_RETRY_AFTER_SEC
    global CREATE_ENDPOINTS

    if rpm is not None:
        RPM = int(rpm)
    if burst is not None:
        BURST = max(1, int(burst))
    if per_second_cooldown is not None:
        PER_SECOND_COOLDOWN = max(0.0, float(per_second_cooldown))
    if create_gap is not None:
        CREATE_GAP = max(0.0, float(create_gap))
    if align is not None:
        ALIGN_SECOND_BOUNDARY = bool(align)
    if on429_skip_minutes is not None:
        ON429_SKIP_MINUTES = max(0, int(on429_skip_minutes))
    if bl_ttl is not None:
        ON429_BLACKLIST_TTL_SEC = max(0, int(bl_ttl))
    if force_rotate_on429 is not None:
        FORCE_ROTATE_ON429 = bool(force_rotate_on429)
    if avoid_create_second_for_others is not None:
        AVOID_CREATE_SECOND_FOR_OTHERS = bool(avoid_create_second_for_others)
    if avoid_margin_ms is not None:
        AVOID_MARGIN_MS = max(0, int(avoid_margin_ms))
    if disable_create_gate is not None:
        RL_DISABLE_CREATE_GATE = bool(disable_create_gate)
    if max_create_wait_ms is not None:
        RL_MAX_CREATE_WAIT_MS = max(0, int(max_create_wait_ms))
    if max_sleep_sec is not None:
        RL_MAX_SLEEP_SEC = max(0.0, float(max_sleep_sec))
    if max_retry_after_sec is not None:
        RL_MAX_RETRY_AFTER_SEC = max(0.0, float(max_retry_after_sec))
    if extra_create_endpoints:
        CREATE_ENDPOINTS |= set(extra_create_endpoints)

def install():
    if getattr(install, "_installed", False):
        return
    httpx.AsyncClient.request = _patched_async_request  # type: ignore
    httpx.Client.request = _patched_sync_request        # type: ignore
    install._installed = True  # type: ignore
    logging.info(
        "httpx_rate_limit_patch: installed (rpm=%d, burst=%d, per_second_cooldown=%.1fs, create_gap=%.1fs, "
        "slot=%s, slot_offset_ms=%d, cold_start_stagger=%ds, jitter=%dms, align=%s, "
        "on429_skip_minutes=%d, bl_ttl=%ds, force_rotate_on429=%s, avoid_create_second_for_others=%s, avoid_margin_ms=%d, "
        "disable_create_gate=%s, max_create_wait_ms=%d, max_sleep_sec=%.2f, max_retry_after_sec=%.2f, "
        "create_endpoints=%s)",
        RPM, BURST, PER_SECOND_COOLDOWN, CREATE_GAP,
        (CREATE_SLOT_ENV if CREATE_SLOT_ENV != "" else "auto"),
        CREATE_SLOT_OFFSET_MS, CREATE_COLD_START_STAGGER_SEC, JITTER_MS, str(ALIGN_SECOND_BOUNDARY),
        ON429_SKIP_MINUTES, ON429_BLACKLIST_TTL_SEC, str(FORCE_ROTATE_ON429), str(AVOID_CREATE_SECOND_FOR_OTHERS), AVOID_MARGIN_MS,
        str(RL_DISABLE_CREATE_GATE), RL_MAX_CREATE_WAIT_MS, RL_MAX_SLEEP_SEC, RL_MAX_RETRY_AFTER_SEC,
        sorted(CREATE_ENDPOINTS),
    )