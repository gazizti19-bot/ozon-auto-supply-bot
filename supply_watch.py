# -*- coding: utf-8 -*-
"""
supply_watch.py
Оркестратор авто-заявок поставки для Ozon Seller API.

Ключевые изменения в этом выпуске:

- Немедленное пересоздание черновика при 404 от /v1/draft/timeslot/info.
  Если Ozon сообщает "Draft X doesn't exist", мы сразу помечаем черновик как отсутствующий,
  очищаем draft_id/operation_id, логируем причину и запускаем новый /v1/draft/create
  с прежними параметрами, затем продолжаем поиск тайм-слотов без каких-либо лимитов
  на количество таких реинициализаций (до истечения окна задачи).

- 404 от /v1/draft/timeslot/set по-прежнему НЕ приводит к пересозданию черновика:
  метод считаем недоступным и двигаемся дальше к созданию заявки (как и ранее).

- Для телеметрии добавлены/ведутся поля:
  stale_draft_recreates (счётчик пересозданий по 404 timeslot/info),
  last_draft_missing_ts (когда в последний раз увидели 404 на timeslot/info).
- Исправлен NameError (_epoch).
- Автоудаление “Создано”: по умолчанию включен немедленный снос (можно отключить).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
import uuid
import threading
import random
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Callable, Awaitable
from datetime import datetime, timedelta, timezone

import httpx
import fcntl
from contextlib import contextmanager

# ================== ENV helpers ==================

def _getenv_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return v if v is not None else default

def _getenv_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    try:
        return int(v)
    except Exception:
        try:
            return int(float(v))
        except Exception:
            return default

def _getenv_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "on")

def _getenv_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or v == "":
        return float(default)
    try:
        return float(v)
    except Exception:
        return float(default)

# ================== Paths / ENV ==================

DATA_DIR = Path(_getenv_str("DATA_DIR", "/app/data")).resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)
SUPPLY_TASK_FILE = DATA_DIR / _getenv_str("SUPPLY_TASK_FILE", "supply_tasks.json")

OZON_CLIENT_ID = _getenv_str("OZON_CLIENT_ID", "")
OZON_API_KEY = _getenv_str("OZON_API_KEY", "")

API_TIMEOUT_SECONDS = _getenv_int("API_TIMEOUT_SECONDS", 15)
OZON_HTTP_HARD_TIMEOUT_SECONDS = max(1, _getenv_int("OZON_HTTP_HARD_TIMEOUT_SECONDS", 8))
TASK_STEP_TIMEOUT_SECONDS = max(2, _getenv_int("TASK_STEP_TIMEOUT_SECONDS", 12))

SUPPLY_PURGE_AGE_DAYS = _getenv_int("SUPPLY_PURGE_AGE_DAYS", 7)

SLOT_POLL_INTERVAL_SECONDS = _getenv_int("SLOT_POLL_INTERVAL_SECONDS", 180)
OPERATION_POLL_INTERVAL_SECONDS = _getenv_int("OPERATION_POLL_INTERVAL_SECONDS", 25)
OPERATION_POLL_TIMEOUT_SECONDS = _getenv_int("OPERATION_POLL_TIMEOUT_SECONDS", 600)

SUPPLY_MAX_OPERATION_RETRIES = _getenv_int("SUPPLY_MAX_OPERATION_RETRIES", 25)

# Автосоздание грузов ОТКЛЮЧЕНО по умолчанию
AUTO_CREATE_CARGOES = _getenv_bool("AUTO_CREATE_CARGOES", False)
AUTO_CREATE_LABELS = _getenv_bool("AUTO_CREATE_LABELS", True)
AUTO_SEND_LABEL_PDF = _getenv_bool("AUTO_SEND_LABEL_PDF", True)

SUPPLY_TYPE_DEFAULT = _getenv_str("SUPPLY_TYPE_DEFAULT", "CREATE_TYPE_DIRECT")
SUPPLY_CLUSTER_IDS_RAW = _getenv_str("SUPPLY_CLUSTER_IDS", "").strip()
SUPPLY_WAREHOUSE_MAP_RAW = _getenv_str("SUPPLY_WAREHOUSE_MAP", "").strip()

RATE_LIMIT_DEFAULT_COOLDOWN = _getenv_int("RATE_LIMIT_DEFAULT_COOLDOWN", 10)
RATE_LIMIT_MAX_ON429 = _getenv_int("RATE_LIMIT_MAX_ON429", 60)
ON429_SHORT_RETRY_SEC = _getenv_int("ON429_SHORT_RETRY_SEC", 6)

CREATE_INITIAL_BACKOFF = _getenv_int("CREATE_INITIAL_BACKOFF", 2)
CREATE_MAX_BACKOFF = _getenv_int("CREATE_MAX_BACKOFF", 120)

SUPPLY_CREATE_STAGE_DELAY_SECONDS = _getenv_int("SUPPLY_CREATE_STAGE_DELAY_SECONDS", 15)
SUPPLY_CREATE_MIN_RETRY_SECONDS = _getenv_int("SUPPLY_CREATE_MIN_RETRY_SECONDS", 20)
SUPPLY_CREATE_MAX_RETRY_SECONDS = _getenv_int("SUPPLY_CREATE_MAX_RETRY_SECONDS", 90)

ORDER_FILL_POLL_INTERVAL_SECONDS = _getenv_int("ORDER_FILL_POLL_INTERVAL_SECONDS", 20)
ORDER_FILL_MAX_RETRIES = _getenv_int("ORDER_FILL_MAX_RETRIES", 150)

PROMPT_MIN_INTERVAL = _getenv_int("PROMPT_MIN_INTERVAL", 120)

DROP_OFF_WAREHOUSE_ID = _getenv_int("DROP_ID", 0)
SUPPLY_TIMESLOT_SEARCH_EXTRA_DAYS = _getenv_int("SUPPLY_TIMESLOT_SEARCH_EXTRA_DAYS", 7)

SUPPLY_MAX_DRAFTS_PER_TICK = _getenv_int("SUPPLY_MAX_DRAFTS_PER_TICK", 1)
SUPPLY_MAX_SUPPLY_CREATES_PER_TICK = _getenv_int("SUPPLY_MAX_SUPPLY_CREATES_PER_TICK", 1)

SELF_WAKEUP_THRESHOLD_SECONDS = _getenv_int("SELF_WAKEUP_THRESHOLD_SECONDS", 180)

# Глобальный пейсинг для draft/create
DRAFT_CREATE_MIN_SPACING_SECONDS = max(1, _getenv_int("DRAFT_CREATE_MIN_SPACING_SECONDS", 3))
DRAFT_CREATE_MAX_BACKOFF = _getenv_int("DRAFT_CREATE_MAX_BACKOFF", 120)

# «Тихий коридор» вокруг create-эндпоинтов
CREATE_QUIET_BEFORE_SEC = max(0.0, _getenv_float("CREATE_QUIET_BEFORE_SEC", 0.6))
CREATE_QUIET_AFTER_SEC = max(0.0, _getenv_float("CREATE_QUIET_AFTER_SEC", 1.2))

# Быстрый поллинг после установки тайм‑слота
ORDER_FAST_POLL_SECONDS = _getenv_int("ORDER_FAST_POLL_SECONDS", 12)
ORDER_FAST_POLL_WINDOW_SECONDS = _getenv_int("ORDER_FAST_POLL_WINDOW_SECONDS", 180)

# Тайм‑аут ожидания supply_id
ORDER_SUPPLY_ID_TIMEOUT_MIN = _getenv_int("ORDER_SUPPLY_ID_TIMEOUT_MIN", 20)

# Периодический re-try установки тайм‑слота
ORDER_TIMESLOT_REFRESH_SECONDS = _getenv_int("ORDER_TIMESLOT_REFRESH_SECONDS", 180)

# Ссылки ЛК: используем только портал (чтобы не ловить 404 в карточке заявки)
SELLER_PORTAL_URL = _getenv_str("SELLER_PORTAL_URL", "https://seller.ozon.ru").strip() or "https://seller.ozon.ru"

# Сколько минут держать заявку со статусом "Создано", затем удалить автоматически
AUTO_DELETE_CREATED_MINUTES = max(1, _getenv_int("AUTO_DELETE_CREATED_MINUTES", 10))
# Немедленное удаление после финального сообщения — по умолчанию включено
AUTO_DELETE_CREATED_IMMEDIATE = _getenv_bool("AUTO_DELETE_CREATED_IMMEDIATE", True)

# Внутренний гейт пейсинга (если True — отключить внутренний пейсинг черновиков)
SUPPLY_DISABLE_INTERNAL_GATE = _getenv_bool("SUPPLY_DISABLE_INTERNAL_GATE", False)

# Слоты
TIMESLOT_ALLOW_FALLBACK = _getenv_bool("TIMESLOT_ALLOW_FALLBACK", False)
TIMESLOT_FALLBACK_DELTA_MIN = max(1, _getenv_int("TIMESLOT_FALLBACK_DELTA_MIN", 120))

# Черновик: (грейс и макс ретраи оставлены для совместимости, но НE используются для timeslot/info 404)
DRAFT_NOT_FOUND_GRACE_MIN = max(1, _getenv_int("DRAFT_NOT_FOUND_GRACE_MIN", 180))  # legacy
DRAFT_NOT_FOUND_MAX_RETRIES = max(1, _getenv_int("DRAFT_NOT_FOUND_MAX_RETRIES", 999999))  # legacy

# ================== Status Constants ==================

ST_WAIT_WINDOW = "WAIT_WINDOW"
ST_DRAFT_CREATING = "DRAFT_CREATING"
ST_POLL_DRAFT = "POLL_DRAFT"
ST_TIMESLOT_SEARCH = "TIMESLOT_SEARCH"
ST_SUPPLY_CREATING = "SUPPLY_CREATING"
ST_POLL_SUPPLY = "POLL_SUPPLY"
ST_SUPPLY_ORDER_FETCH = "SUPPLY_ORDER_FETCH"
ST_ORDER_DATA_FILLING = "ORDER_DATA_FILLING"
ST_CARGO_PREP = "CARGO_PREP"
ST_CARGO_CREATING = "CARGO_CREATING"
ST_POLL_CARGO = "POLL_CARGO"
ST_LABELS_CREATING = "LABELS_CREATING"
ST_POLL_LABELS = "POLL_LABELS"
ST_DONE = "DONE"
ST_FAILED = "FAILED"
ST_CANCELED = "CANCELED"

# Визуальная метка для UI — заявка "Создано"
UI_STATUS_CREATED = "Создано"

TZ_YEKAT = timezone(timedelta(hours=5))

# ================== HTTP client and helpers ==================

_HTTP_CLIENT: Optional[httpx.AsyncClient] = None

def _get_client() -> httpx.AsyncClient:
    global _HTTP_CLIENT
    if _HTTP_CLIENT is None:
        _HTTP_CLIENT = httpx.AsyncClient(
            timeout=API_TIMEOUT_SECONDS,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
            http2=False,
            trust_env=True,
        )
    return _HTTP_CLIENT

def _parse_retry_after(headers: Dict[str, str]) -> int:
    ra = None
    try:
        v = headers.get("Retry-After")
        if v and str(v).strip().isdigit():
            ra = int(v)
    except Exception:
        ra = None
    if ra is None:
        ra = RATE_LIMIT_DEFAULT_COOLDOWN
    return max(1, min(int(ra), int(RATE_LIMIT_MAX_ON429)))

# «Тихий коридор» для всех запросов, кроме create-эндпоинтов
_CREATE_QUIET_UNTIL: float = 0.0
_CREATE_QUIET_LOCK = asyncio.Lock()

def _is_create_endpoint(path: str) -> bool:
    p = str(path or "").lower()
    return p.startswith("/v1/draft/create") or p.startswith("/v1/draft/supply/create")

async def _enter_quiet_before_create():
    global _CREATE_QUIET_UNTIL
    async with _CREATE_QUIET_LOCK:
        now = time.time()
        _CREATE_QUIET_UNTIL = max(_CREATE_QUIET_UNTIL, now + CREATE_QUIET_BEFORE_SEC + CREATE_QUIET_AFTER_SEC)
    if CREATE_QUIET_BEFORE_SEC > 0:
        await asyncio.sleep(CREATE_QUIET_BEFORE_SEC)

async def _wait_if_quiet(method: str, path: str):
    if _is_create_endpoint(path):
        return
    if CREATE_QUIET_BEFORE_SEC <= 0 and CREATE_QUIET_AFTER_SEC <= 0:
        return
    while True:
        until = _CREATE_QUIET_UNTIL
        if until <= 0:
            return
        now = time.time()
        if now >= until:
            return
        await asyncio.sleep(min(0.2, until - now))

class OzonApi:
    def __init__(self, client_id: str, api_key: str, timeout: int = 15):
        self.base = "https://api-seller.ozon.ru"
        self.client_id = client_id
        self.api_key = api_key
        self.timeout = timeout
        self.mock = not (client_id and api_key)

    def headers(self) -> Dict[str, str]:
        return {
            "Client-Id": self.client_id,
            "Api-Key": self.api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    async def request(self, method: str, path: str, payload: Optional[Dict[str, Any]] = None) -> Tuple[bool, Any, str, int, Dict[str, str]]:
        if self.mock:
            logging.info("MOCK %s %s payload_keys=%s", method, path, list((payload or {}).keys()))
            return True, {"mock": True, "path": path, "payload": payload}, "", 200, {}
        try:
            await _wait_if_quiet(method, path)
        except Exception:
            pass

        client = _get_client()
        url = f"{self.base}{path}"
        try:
            r = await asyncio.wait_for(
                client.request(method=method, url=url, headers=self.headers(), json=payload),
                timeout=float(OZON_HTTP_HARD_TIMEOUT_SECONDS),
            )
            status = r.status_code
            headers = {k: v for k, v in r.headers.items()}
            text = r.text or ""
            logging.info("HTTP %s %s -> status=%s, resp=%s", method, path, status, text[:300])
            try:
                data = r.json()
            except Exception:
                data = {"text": text}

            if status == 429:
                ra = _parse_retry_after(headers)
                return False, data, f"rate_limit:{ra}", status, headers

            ok = 200 <= status < 300
            if ok:
                return True, data, "", status, headers
            return False, data, f"http_status:{status}", status, headers

        except asyncio.TimeoutError:
            logging.warning("HTTP %s %s hard-timeout after %ss", method, path, OZON_HTTP_HARD_TIMEOUT_SECONDS)
            return False, {"error": "hard_timeout"}, "http_timeout", 0, {}
        except Exception as e:
            logging.exception("HTTP %s %s failed: %s", method, path, e)
            return False, {"error": str(e)}, f"http_error:{e}", 0, {}

    async def post(self, path: str, payload: Dict[str, Any]) -> Tuple[bool, Any, str, int, Dict[str, str]]:
        return await self.request("POST", path, payload)

    async def get_pdf(self, path: str) -> Tuple[bool, bytes, str]:
        if self.mock:
            logging.info("MOCK GET %s (PDF)", path)
            return True, b"%PDF-1.4\n% Fake PDF\n", ""
        try:
            await _wait_if_quiet("GET", path)
        except Exception:
            pass
        client = _get_client()
        url = f"{self.base}{path}"
        try:
            r = await asyncio.wait_for(
                client.get(url, headers=self.headers()),
                timeout=float(OZON_HTTP_HARD_TIMEOUT_SECONDS),
            )
            r.raise_for_status()
            return True, r.content, ""
        except asyncio.TimeoutError:
            logging.warning("HTTP GET %s hard-timeout after %ss", path, OZON_HTTP_HARD_TIMEOUT_SECONDS)
            return False, b"", "http_timeout"
        except httpx.HTTPError as e:
            logging.exception("HTTP GET %s failed: %s", path, e)
            return False, b"", f"http_error:{e}"

# ================== Task storage ==================

_tasks_loaded = False
_tasks: List[Dict[str, Any]] = []
_lock = asyncio.Lock()
_sync_lock = threading.RLock()

ID_FIELDS = ("id", "task_id", "uuid", "pk", "external_id")
STATUS_FIELDS = ["status","state","phase","stage","step","pipeline_status","current_status","progress","phase_name"]
BOOKING_FIELDS = ("draft_id","order_id","order_number","number","supply_order_number","supply_id","from_in_timezone","to_in_timezone","warehouse_id","drop_off_id","timeslot","result")

def now_ts() -> int:
    return int(time.time())

def short(uid: str) -> str:
    try:
        return uid.split("-")[0]
    except Exception:
        return str(uid)[:8]

def ensure_loaded():
    global _tasks_loaded, _tasks
    if _tasks_loaded:
        return
    if SUPPLY_TASK_FILE.exists():
        try:
            _tasks = json.loads(SUPPLY_TASK_FILE.read_text(encoding="utf-8"))
            if not isinstance(_tasks, list):
                _tasks = []
        except Exception:
            _tasks = []
    else:
        _tasks = []
    _tasks_loaded = True

def save_tasks():
    SUPPLY_TASK_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = SUPPLY_TASK_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(_tasks, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(SUPPLY_TASK_FILE)

def add_task(task: Dict[str, Any]):
    _tasks.append(task)
    save_tasks()

def delete_task(task_id: str) -> bool:
    ensure_loaded()
    removed = False
    with _sync_lock:
        before = len(_tasks)
        _tasks[:] = [t for t in _tasks if str(t.get("id")) != str(task_id)]
        removed = len(_tasks) < before
        if removed:
            save_tasks()
    return removed

def purge_all_supplies_now() -> int:
    ensure_loaded()
    with _sync_lock:
        count = len(_tasks)
        _tasks.clear()
        save_tasks()
        logging.info("purge_all_supplies_now: removed=%s", count)
        return count

def _normalize_id(v: Any) -> Optional[str]:
    if v is None:
        return None
    try:
        return str(v)
    except Exception:
        return None

def _extract_any_id(obj: Dict[str, Any]) -> Optional[str]:
    for k in ID_FIELDS:
        if k in obj and obj[k] not in (None, ""):
            nid = _normalize_id(obj[k])
            if nid:
                return nid
    return None

def _find_task_by_any_id(any_id: Any) -> Optional[Dict[str, Any]]:
    any_id_norm = _normalize_id(any_id)
    if not any_id_norm:
        return None
    for t in _tasks:
        if _normalize_id(t.get("id")) == any_id_norm:
            return t
        for k in ID_FIELDS:
            if _normalize_id(t.get(k)) == any_id_norm:
                return t
    return None

def _apply_status_fields(t: Dict[str, Any], payload: Dict[str, Any]) -> bool:
    changed = False
    for f in STATUS_FIELDS:
        if f in payload and t.get(f) != payload[f]:
            t[f] = payload[f]
            changed = True
    return changed

def _apply_booking_fields(t: Dict[str, Any], payload: Dict[str, Any]) -> bool:
    changed = False
    for k in BOOKING_FIELDS:
        if k in payload and t.get(k) != payload[k]:
            t[k] = payload[k]
            changed = True
    return changed

def _apply_generic_fields(t: Dict[str, Any], payload: Dict[str, Any]) -> bool:
    changed = False
    for k, v in payload.items():
        if k in ID_FIELDS or k in STATUS_FIELDS or k in BOOKING_FIELDS:
            continue
        if t.get(k) != v:
            t[k] = v
            changed = True
    return changed

def _touch(t: Dict[str, Any]) -> None:
    t["updated_ts"] = now_ts()

def update_task(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    ensure_loaded()
    with _sync_lock:
        payload: Dict[str, Any] = {}
        if args:
            if len(args) == 1 and isinstance(args[0], dict):
                payload.update(args[0])
            else:
                tid = _normalize_id(args[0])
                if tid:
                    payload.setdefault("id", tid)
                    payload.setdefault("task_id", tid)
                    payload.setdefault("uuid", tid)
                if len(args) >= 2 and isinstance(args[1], dict):
                    payload.update(args[1])
                if kwargs:
                    payload.update(kwargs)
        else:
            payload.update(kwargs)

        target: Optional[Dict[str, Any]] = None
        if len(args) == 1 and isinstance(args[0], dict) and args[0] in _tasks:
            target = args[0]
        if target is None:
            tid = _extract_any_id(payload)
            if tid:
                target = _find_task_by_any_id(tid)
        if target is None:
            tid = _extract_any_id(payload) or str(uuid.uuid4())
            target = {"id": tid, "status": ST_TIMESLOT_SEARCH, "created_ts": now_ts(), "updated_ts": now_ts()}
            for k in ("task_id", "uuid"):
                target.setdefault(k, tid)
            _tasks.append(target)

        changed = False
        changed |= _apply_status_fields(target, payload)
        changed |= _apply_booking_fields(target, payload)
        changed |= _apply_generic_fields(target, payload)

        has_order = bool(target.get("order_id"))
        has_supply = bool(target.get("supply_id"))
        is_verified = bool(target.get("supply_id_verified"))
        cur = str(target.get("status") or "").upper()

        if has_supply and is_verified and cur not in (ST_CARGO_PREP, ST_CARGO_CREATING, ST_POLL_CARGO, ST_LABELS_CREATING, ST_POLL_LABELS, ST_DONE):
            target["status"] = ST_CARGO_PREP
            target["next_attempt_ts"] = now_ts()
            target["retry_after_ts"] = 0
            target["retry_after_jitter"] = 0.0
            changed = True
        elif has_order and (not has_supply) and cur not in (ST_SUPPLY_ORDER_FETCH, ST_ORDER_DATA_FILLING, ST_CARGO_PREP, ST_CARGO_CREATING, ST_POLL_CARGO, ST_LABELS_CREATING, ST_POLL_LABELS, ST_DONE, ST_FAILED, ST_CANCELED):
            target["status"] = ST_SUPPLY_ORDER_FETCH
            target["next_attempt_ts"] = now_ts()
            changed = True

        if changed:
            _touch(target)
            save_tasks()

        return dict(target)

def get_task(task_id: str) -> Optional[Dict[str, Any]]:
    ensure_loaded()
    for t in _tasks:
        if t.get("id") == task_id:
            return t
    return None

def list_all_tasks() -> List[Dict[str, Any]]:
    ensure_loaded()
    return [dict(t) for t in _tasks]

def list_tasks() -> List[Dict[str, Any]]:
    ensure_loaded()
    active = []
    for t in _tasks:
        st = str(t.get("status") or "").upper()
        if st in (ST_DONE, ST_FAILED, ST_CANCELED):
            continue
        active.append(dict(t))
    return active

def purge_tasks(days: int = SUPPLY_PURGE_AGE_DAYS) -> int:
    ensure_loaded()
    if days is None or int(days) <= 0:
        removed = len(_tasks)
        _tasks.clear()
        save_tasks()
        logging.info("purge_tasks: bulldozer mode, removed=%s", removed)
        return removed

    cutoff = now_ts() - int(days) * 86400
    before = len(_tasks)
    remain = []
    removed_ids = []
    for t in _tasks:
        st = str(t.get("status") or "").upper()
        upd = int(t.get("updated_ts") or 0)
        if st not in (ST_DONE, ST_FAILED, ST_CANCELED) and upd > 0 and upd < cutoff:
            removed_ids.append(t.get("id"))
            continue
        remain.append(t)
    _tasks[:] = remain
    save_tasks()
    logging.info("purge_stale_nonfinal: hours=%s removed=%s ids=%s", 48, before - len(remain), removed_ids[:10])
    return before - len(remain)

def purge_all_tasks() -> int:
    return purge_tasks(0)

def purge_stale_nonfinal(hours: int = 48) -> int:
    ensure_loaded()
    cutoff = now_ts() - max(1, int(hours)) * 3600
    before = len(_tasks)
    remain = []
    removed_ids = []
    for t in _tasks:
        st = str(t.get("status") or "").upper()
        upd = int(t.get("updated_ts") or 0)
        if st not in (ST_DONE, ST_FAILED, ST_CANCELED) and upd > 0 and upd < cutoff:
            removed_ids.append(t.get("id"))
            continue
        remain.append(t)
    _tasks[:] = remain
    save_tasks()
    logging.info("purge_stale_nonfinal: hours=%s removed=%s ids=%s", hours, before - len(remain), removed_ids[:10])
    return before - len(remain)

# ================== Template parsing and slots helpers ==================

DASH_CLASS = r"[\-\u2010\u2011\u2012\u2013\u2014\u2015\u2212]"

HEADER_RE = re.compile(
    rf"^\s*На\s+(\d{{2}})\.(\d{{2}})\.(\д{{4}})\s*,?\s*(\д{{2}}:\д{{2}})\s*{DASH_CLASS}\s*(\д{{2}}:\д{{2}})\s*$".replace("д", "d"),
    re.IGNORECASE
)

LINE_RE = re.compile(
    rf"""
    ^\s*
    (?P<sku>[^\s{DASH_CLASS}]+?)\s*{DASH_CLASS}\s*
    (?:кол(?:-?\s*во|ичество)?|qty|quantity)\s*(?P<qty>\d+)\s*[,;]?\s*
    (?P<boxes>\d+)\s*
    (?:короб(?:к(?:а|и|ок))?|кор\.?\b|box(?:es)?)\s*[,;]?\s*
    (?:в\s*каждой\s*коробке\s*по|по)\s*(?P<per>\d+)\s*
    (?:шт(?:\.|ук)?|штук|штуки|штука)?\s*[,;]?\s*
    (?P<wh>.+?)\s*\.?\s*$
    """,
    re.IGNORECASE | re.VERBOSE
)

def _canon_spaces_and_dashes(s: str) -> str:
    if not isinstance(s, str):
        return s
    s = s.replace("\u00A0", " ").replace("\u2009", " ").replace("\u202F", " ")
    for ch in ["\u2010", "\u2011", "\u2012", "\u2013", "\u2014", "\u2015", "\u2212"]:
        s = s.replace(ch, "-")
    s = re.sub(r"\s*-\s*", " - ", s)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s

def _scan_header(lines: List[str]) -> Tuple[int, str, str, str]:
    for idx, raw in enumerate(lines
        ):
        line = _canon_spaces_and_dashes(raw)
        m = HEADER_RE.search(line)
        if m:
            dd, mm, yyyy, start_hhmm, end_hhmm = m.groups()
            date_iso = f"{yyyy}-{mm}-{dd}"
            return idx, date_iso, start_hhmm, end_hhmm
    raise ValueError("Заголовок должен быть вида: 'На DD.MM.YYYY, HH:MM-HH:MM ...'")

def _parse_item_line(raw_line: str) -> Optional[Dict[str, Any]]:
    line = _canon_spaces_and_dashes(raw_line)
    m2 = LINE_RE.match(line)
    if m2:
        sku = m2.group("sku").strip()
        qty = int(m2.group("qty"))
        boxes = int(m2.group("boxes"))
        per_box = int(m2.group("per"))
        wh = (m2.group("wh") or "").strip().rstrip(".")
        return {"sku": sku, "total_qty": qty, "boxes": boxes, "per_box": per_box, "warehouse_name": wh}

    parts = [p.strip() for p in re.split(rf"{DASH_CLASS}", line, maxsplit=1)]
    if len(parts) == 2:
        sku_guess, rest = parts[0].strip(), parts[1].strip()
        flags = re.IGNORECASE
        m_qty = re.search(r"(?:кол(?:-?\s*во|ичество)?|qty|quantity)\s*(\d+)", rest, flags)
        m_boxes = re.search(r"(\d+)\s*(?:короб(?:к(?:а|и|ок))?|кор\.?\b|box(?:es)?)", rest, flags)
        m_per = re.search(r"(?:в\s*каждой\s*коробке\s*по|по)\s*(\d+)", rest, flags)
        m_wh = re.search(r"(?:шт(?:\.|ук)?|штук|штуки|штука)[,;\s]*(.*)$", rest, flags)
        wh = None
        if m_wh and m_wh.group(1).strip():
            wh = m_wh.group(1).strip().rstrip(".")
        else:
            cut = re.split(r"[,;]", rest)
            if len(cut) >= 2 and cut[-1].strip():
                wh = cut[-1].strip().rstrip(".")
            else:
                tail = rest.split()
                if tail:
                    wh = tail[-1].strip().rstrip(".,;")
        if m_qty and m_boxes and m_per and wh:
            try:
                qty = int(m_qty.group(1))
                boxes = int(m_boxes.group(1))
                per_box = int(m_per.group(1))
                return {"sku": sku_guess, "total_qty": qty, "boxes": boxes, "per_box": per_box, "warehouse_name": wh}
            except Exception:
                pass
    logging.error("Line parse failed (fallback too): %r", raw_line)
    return None

def to_iso_local(date_iso: str, hhmm: str) -> str:
    y, m, d = [int(x) for x in date_iso.split("-")]
    hh, mm = [int(x) for x in hhmm.split(":")]
    dt = datetime(y, m, d, hh, mm, tzinfo=TZ_YEKAT)
    return dt.isoformat()

def to_ts_local(date_iso: str, hhmm: str) -> int:
    y, m, d = [int(x) for x in date_iso.split("-")]
    hh, mm = [int(x) for x in hhmm.split(":")]
    dt = datetime(y, m, d, hh, mm, tzinfo=TZ_YEKAT)
    return int(dt.timestamp())

def day_range_local(date_iso: str) -> Tuple[str, str]:
    y, m, d = [int(x) for x in date_iso.split("-")]
    start = datetime(y, m, d, 0, 0, 0, tzinfo=TZ_YEKAT)
    end = start.replace(hour=23, minute=59, second=59)
    return start.isoformat(), end.isoformat()

def add_days_iso(date_iso: str, days: int) -> str:
    y, m, d = [int(x) for x in date_iso.split("-")]
    dt = datetime(y, m, d, tzinfo=TZ_YEKAT) + timedelta(days=int(days))
    return dt.strftime("%Y-%m-%d")

def to_local_iso(iso_str: str) -> Optional[str]:
    if not iso_str or not isinstance(iso_str, str):
        return None
    s = iso_str.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except Exception:
        return None
    return dt.astimezone(TZ_YEKAT).isoformat()

def _to_z_iso(iso_str: str) -> Optional[str]:
    try:
        dt = datetime.fromisoformat(iso_str)
        return dt.astimezone(timezone.utc).replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception:
        return None

# === FIX: helpers for epoch conversion ===
def _parse_iso_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    ss = s.strip()
    if ss.endswith("Z"):
        ss = ss[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(ss)
    except Exception:
        return None

def _epoch(iso_str: Optional[str]) -> Optional[int]:
    dt = _parse_iso_dt(iso_str)
    return int(dt.timestamp()) if dt else None
# =========================================

def parse_pairs_or_json(raw: str) -> Dict[str, Any]:
    raw = (raw or "").strip()
    if not raw:
        return {}
    if raw.startswith("{"):
        try:
            obj = json.loads(raw)
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}
    parts = [p.strip() for p in re.split(r"[;,]", raw) if p.strip()]
    out: Dict[str, Any] = {}
    for p in parts:
        if "=" in p:
            k, v = p.split("=", 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            try:
                out[k] = int(v)
            except Exception:
                out[k] = v
    return out

WAREHOUSE_MAP = {str(k).lower(): v for k, v in parse_pairs_or_json(SUPPLY_WAREHOUSE_MAP_RAW).items()}

REVERSE_WAREHOUSE_MAP: Dict[int, str] = {}
for name_lc, val in WAREHOUSE_MAP.items():
    try:
        wid = int(val)
        pretty = name_lc.strip() if name_lc else name_lc
        REVERSE_WAREHOUSE_MAP[wid] = pretty
    except Exception:
        continue

def resolve_warehouse_id(name: str) -> Optional[int]:
    lname = (name or "").lower()
    if lname in WAREHOUSE_MAP:
        try:
            return int(WAREHOUSE_MAP[lname])
        except Exception:
            return None
    return None

def warehouse_display(task: Dict[str, Any]) -> str:
    try:
        items = task.get("sku_list") or []
        if items:
            wn = (items[0] or {}).get("warehouse_name")
            if wn:
                wid = task.get("chosen_warehouse_id")
                if wid:
                    return f"{wn} (ID: {wid})"
                return str(wn)
    except Exception:
        pass

    wid = task.get("chosen_warehouse_id") or task.get("warehouse_id")
    if wid:
        try:
            wid_int = int(wid)
            nm = REVERSE_WAREHOUSE_MAP.get(wid_int)
            if nm:
                return f"{nm} (ID: {wid_int})"
            return f"ID: {wid_int}"
        except Exception:
            return f"ID: {wid}"
    return "—"

def to_int_or_str(v: Any) -> Any:
    try:
        return int(str(v).strip())
    except Exception:
        return str(v).strip()

def _extract_dropoff_id(task: Dict[str, Any]) -> Optional[int]:
    for k in ("drop_off_point_warehouse_id", "dropoff_warehouse_id", "dropoffWarehouseId", "drop_off_id", "warehouse_id"):
        v = task.get(k)
        if v not in (None, "", 0, "0"):
            try:
                return int(str(v).strip())
            except Exception:
                pass
    return int(DROP_OFF_WAREHOUSE_ID) if DROP_OFF_WAREHOUSE_ID else None

# ================== API calls (Ozon) ==================

async def api_draft_create(api: OzonApi, task: Dict[str, Any]) -> Tuple[bool, Optional[str], Optional[str], int]:
    items = [{"sku": str(line["sku"]), "quantity": int(line["total_qty"])} for line in task["sku_list"]]
    drop_id = _extract_dropoff_id(task)
    used_type = task.get("supply_type") or ("CREATE_TYPE_CROSSDOCK" if drop_id else SUPPLY_TYPE_DEFAULT)
    payload: Dict[str, Any] = {"items": items, "type": used_type}
    if drop_id:
        payload["drop_off_point_warehouse_id"] = drop_id
        payload.setdefault("dropoff_warehouse_id", drop_id)
        payload.setdefault("dropoffWarehouseId", drop_id)
    if SUPPLY_CLUSTER_IDS_RAW:
        try:
            ids = [int(x) for x in SUPPLY_CLUSTER_IDS_RAW.split(",") if x.strip()]
            if ids:
                payload["cluster_ids"] = ids
        except Exception:
            pass
    task["supply_type"] = used_type
    update_task(task)

    try:
        await _enter_quiet_before_create()
    except Exception:
        pass

    ok, data, err, status, headers = await api.post("/v1/draft/create", payload)
    if status == 429 or (err and str(err).startswith("rate_limit:")):
        msg = ""
        try:
            if isinstance(data, dict):
                msg = str(data.get("message") or data.get("error") or "").lower()
        except Exception:
            msg = ""
        try:
            base = int(str(err).split(":", 1)[1]) if err else ON429_SHORT_RETRY_SEC
        except Exception:
            base = ON429_SHORT_RETRY_SEC

        if "per second" in msg or "per-second" in msg or "per second limit" in msg:
            wait_sec = max(base + 10, 30) + random.randint(0, 15)
        else:
            attempts = int(task.get("draft_rl_attempts") or 0) + 1
            task["draft_rl_attempts"] = attempts
            wait_sec = min(DRAFT_CREATE_MAX_BACKOFF, max(ON429_SHORT_RETRY_SEC, base) + attempts * 3)

        _set_retry_after(task, wait_sec, jitter_max=1.5)
        task["last_error"] = "429 Too Many Requests (draft/create)"
        try:
            task["last_trace_id"] = headers.get("x-o3-trace-id", "")
        except Exception:
            pass
        update_task(task)
        _set_global_draft_cooldown(wait_sec + 1.0)
        return False, None, f"rate_limit:{wait_sec}", 429

    if task.get("draft_rl_attempts"):
        task.pop("draft_rl_attempts", None)

    if not ok:
        return False, None, f"draft_create_error:{err}|{data}", status
    op_id = data.get("operation_id") or data.get("result", {}).get("operation_id")
    if not op_id:
        return False, None, f"draft_create_no_operation_id:{data}", status
    return True, op_id, None, status

def _extract_clusters_warehouses(js: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    clusters = js.get("clusters") or js.get("result", {}).get("clusters") or []
    if not isinstance(clusters, list):
        return out
    for cl in clusters:
        wlist = (cl or {}).get("warehouses") or []
        for w in wlist:
            sup = (w or {}).get("supply_warehouse") or {}
            wid = sup.get("warehouse_id")
            try:
                wid = int(wid) if wid is not None else None
            except Exception:
                wid = None
            bundle_id = None
            bids = (w or {}).get("bundle_ids") or []
            if isinstance(bids, list) and bids:
                bd = (bids[0] or {})
                bundle_id = bd.get("bundle_id") or bd.get("id")
            status = (w or {}).get("status") or {}
            if wid:
                out.append({"warehouse_id": wid, "bundle_id": bundle_id, "status": status})
    return out

def _is_available(w: Dict[str, Any]) -> bool:
    st = (w or {}).get("status") or {}
    return bool(st.get("is_available", True)) or ((st.get("state") or "").upper().endswith("AVAILABLE"))

def choose_warehouse(warehouses: List[Dict[str, Any]]) -> Optional[int]:
    if not warehouses:
        return None
    normalized = []
    for w in warehouses:
        wid = (w or {}).get("warehouse_id")
        try:
            wid = int(wid) if wid is not None else None
        except Exception:
            wid = None
        if wid:
            normalized.append(w if isinstance(w, dict) else {"warehouse_id": wid})
    if not normalized:
        return None
    avail = [w for w in normalized if _is_available(w)]
    pick = (avail[0] if avail else normalized[0])
    return int(pick["warehouse_id"]) if pick and pick.get("warehouse_id") is not None else None

def _tokens(s: str) -> List[str]:
    s = (s or "").lower()
    s = re.sub(r"[^a-zа-я0-9\s\-]+", " ", s, flags=re.IGNORECASE)
    s = s.replace("_", " ").replace("-", " ")
    return [t for t in s.split() if t]

def _name_match_score(desired: str, candidate: str) -> int:
    dt = set(_tokens(desired))
    ct = set(_tokens(candidate))
    if not dt or not ct:
        return 0
    return len(dt & ct)

def choose_warehouse_smart(task: Dict[str, Any], warehouses: List[Dict[str, Any]]) -> Optional[int]:
    pre = task.get("chosen_warehouse_id")
    if pre:
        try:
            pre_i = int(pre)
        except Exception:
            pre_i = None
        if pre_i:
            for w in (warehouses or []):
                try:
                    wid = int((w.get("supply_warehouse") or {}).get("warehouse_id") or w.get("warehouse_id") or 0)
                except Exception:
                    wid = 0
                if wid == pre_i:
                    return pre_i
    desired_name = ""
    try:
        desired_name = ((task.get("sku_list") or [{}])[0]).get("warehouse_name") or ""
    except Exception:
        desired_name = ""
    best = None
    best_score = -1
    for w in (warehouses or []):
        wid = None
        name = ""
        try:
            wid = int((w.get("supply_warehouse") or {}).get("warehouse_id") or w.get("warehouse_id") or 0)
        except Exception:
            wid = None
        name = (w.get("supply_warehouse") or {}).get("name") or w.get("name") or ""
        if wid:
            sc = _name_match_score(desired_name, name)
            if sc > best_score and _is_available(w):
                best = wid
                best_score = sc
    if best:
        return best
    return choose_warehouse(warehouses)

async def api_draft_create_info(api: OzonApi, operation_id: str) -> Tuple[bool, Optional[str], List[Dict[str, Any]], Optional[str], int]:
    payload = {"operation_id": operation_id}
    ok, data, err, status, headers = await api.post("/v1/draft/create/info", payload)
    if status == 429:
        ra = _parse_retry_after(headers)
        return False, None, [], f"rate_limit:{min(RATE_LIMIT_MAX_ON429, max(ON429_SHORT_RETRY_SEC, ra))}", status
    if not ok:
        return False, None, [], f"draft_info_error:{err}|{data}", status
    draft_id = data.get("draft_id") or data.get("result", {}).get("draft_id")
    if not draft_id:
        s = (data.get("status") or (data.get("result") or {}).get("status") or "")
        if str(s).upper() in ("IN_PROGRESS", "PENDING", ""):
            return False, None, [], None, status
        return False, None, [], f"draft_info_no_draft_id:{data}", status
    warehouses = data.get("warehouses") or data.get("result", {}).get("warehouses") or []
    if not warehouses:
        warehouses = _extract_clusters_warehouses(data)
    return True, draft_id, warehouses, None, status

def _normalize_timeslots_response(js: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    flat = js.get("timeslots") or js.get("result", {}).get("timeslots") or []
    if isinstance(flat, list):
        for s in flat:
            if not isinstance(s, dict):
                continue
            fr = s.get("from_in_timezone") or s.get("from")
            to = s.get("to_in_timezone") or s.get("to")
            fr_loc = to_local_iso(fr) if fr else None
            to_loc = to_local_iso(to) if to else None
            slot = dict(s)
            if fr_loc:
                slot.setdefault("from_in_timezone", fr_loc)
            if to_loc:
                slot.setdefault("to_in_timezone", to_loc)
            slot["from_epoch"] = _epoch(slot.get("from_in_timezone"))
            slot["to_epoch"] = _epoch(slot.get("to_in_timezone"))
            out.append(slot)
    drop = js.get("drop_off_warehouse_timeslots") or js.get("result", {}).get("drop_off_warehouse_timeslots") or []
    if isinstance(drop, list):
        for entry in drop:
            drop_id = entry.get("drop_off_warehouse_id")
            days = (entry or {}).get("days") or []
            for day in days:
                tss = (day or {}).get("timeslots") or []
                for s in tss:
                    if not isinstance(s, dict):
                        continue
                    fr = s.get("from_in_timezone") or s.get("from")
                    to = s.get("to_in_timezone") or s.get("to")
                    fr_loc = to_local_iso(fr) if fr else None
                    to_loc = to_local_iso(to) if to else None
                    slot = dict(s)
                    if fr_loc:
                        slot["from_in_timezone"] = fr_loc
                    if to_loc:
                        slot["to_in_timezone"] = to_loc
                    slot["from_epoch"] = _epoch(slot.get("from_in_timezone"))
                    slot["to_epoch"] = _epoch(slot.get("to_in_timezone"))
                    if drop_id is not None:
                        slot.setdefault("drop_off_point_warehouse_id", drop_id)
                    out.append(slot)
    return out

async def api_timeslot_info(api: OzonApi, draft_id: str, warehouse_ids: List[int], date_iso: str, bundle_id: Optional[str] = None) -> Tuple[bool, List[Dict[str, Any]], str, int]:
    def _build_payload(draft_id: str, wids: List[int], from_z: str, to_z: str, bundle_id: Optional[str]) -> Dict[str, Any]:
        base = {
            "draft_id": draft_id,
            "warehouse_ids": [int(w) for w in wids],
            "date_from": from_z,
            "date_to": to_z
        }
        if bundle_id:
            base["bundle_id"] = bundle_id
        if DROP_OFF_WAREHOUSE_ID:
            base.setdefault("drop_off_point_warehouse_id", int(DROP_OFF_WAREHOUSE_ID))
        return base

    day_start_local, day_end_local = day_range_local(date_iso)
    from_z = _to_z_iso(day_start_local) or day_start_local
    to_z = _to_z_iso(day_end_local) or day_end_local

    pl = _build_payload(draft_id, warehouse_ids, from_z, to_z, bundle_id)
    ok, data, err, status, headers = await api.post("/v1/draft/timeslot/info", pl)
    logging.info("timeslot_info(v1): status=%s keys=%s", status, list(pl.keys()))
    if status == 429:
        ra = _parse_retry_after(headers)
        return False, [], f"rate_limit:{min(RATE_LIMIT_MAX_ON429, max(ON429_SHORT_RETRY_SEC, ra))}", status
    if status == 404:
        return False, [], "not_found_404", 404
    if ok:
        slots = _normalize_timeslots_response(data)
        if slots:
            return True, slots, "", status
        return False, [], "", status

    last_err = f"timeslot_info_error:{err}|{data}"
    last_status = status

    extra_days = max(0, int(SUPPLY_TIMESLOT_SEARCH_EXTRA_DAYS))
    if extra_days > 0:
        ext_to_local = day_range_local(add_days_iso(date_iso, extra_days))[1]
        ext_to_z = _to_z_iso(ext_to_local) or ext_to_local
        pl2 = _build_payload(draft_id, warehouse_ids, from_z, ext_to_z, bundle_id)
        ok2, data2, err2, status2, headers2 = await api.post("/v1/draft/timeslot/info", pl2)
        logging.info("timeslot_info(v1,ext): status=%s keys=%s", status2, list(pl2.keys()))
        if status2 == 429:
            ra = _parse_retry_after(headers2)
            return False, [], f"rate_limit:{min(RATE_LIMIT_MAX_ON429, max(ON429_SHORT_RETRY_SEC, ra))}", status2
        if status2 == 404:
            return False, [], "not_found_404", 404
        if ok2:
            slots2 = _normalize_timeslots_response(data2)
            if slots2:
                return True, slots2, "", status2
        else:
            last_err = f"timeslot_info_error:{err2}|{data2}"
            last_status = status2

    return False, [], (last_err or "timeslot_info_empty"), (last_status or 200)

async def api_draft_timeslot_set(
    api: OzonApi,
    draft_id: int,
    drop_off_point_warehouse_id: int,
    timeslot: Dict[str, Any],
) -> Tuple[bool, Optional[str], int]:
    body = {
        "id": int(draft_id),
        "drop_off_point_warehouse_id": int(drop_off_point_warehouse_id),
        "timeslot": dict(timeslot),
    }
    ok, data, err, status, headers = await api.post("/v1/draft/timeslot/set", body)
    if status == 429:
        ra = _parse_retry_after(headers)
        return False, f"rate_limit:{min(RATE_LIMIT_MAX_ON429, max(ON429_SHORT_RETRY_SEC, ra))}", status
    if status == 404:
        return False, "not_found_404", 404
    if not ok:
        return False, f"http_status:{status}", status
    return True, None, status

def _drop_id_from_task(task: Dict[str, Any]) -> Optional[int]:
    for k in ("dropoff_warehouse_id", "drop_off_point_warehouse_id", "drop_off_id"):
        v = task.get(k)
        if v not in (None, "", 0, "0"):
            try:
                return int(v)
            except Exception:
                pass
    return int(DROP_OFF_WAREHOUSE_ID) if DROP_OFF_WAREHOUSE_ID else None

async def api_supply_create(api: OzonApi, task: Dict[str, Any]) -> Tuple[bool, Optional[str], Optional[str], int]:
    if api.mock:
        return True, f"op-supply-{short(task['id'])}", None, 200

    try:
        await _enter_quiet_before_create()
    except Exception:
        pass

    draft_id = task["draft_id"]
    wid = int(task.get("chosen_warehouse_id") or 0)
    fr = task["desired_from_iso"]
    to = task["desired_to_iso"]
    slot_id = task.get("slot_id")
    bundle_id = task.get("bundle_id")
    drop_id = _drop_id_from_task(task)

    if wid <= 0:
        return False, None, "supply_create_error:warehouse_id_is_zero", 400

    base = {"draft_id": draft_id, "from_in_timezone": fr, "to_in_timezone": to}
    variants: List[Dict[str, Any]] = []
    variants.append(dict(base, warehouse_id=wid))
    if drop_id:
        variants.append(dict(base, warehouse_id=wid, drop_off_point_warehouse_id=int(drop_id)))
    if slot_id is not None:
        variants.append(dict(base, warehouse_id=wid, timeslot_id=slot_id))
    if bundle_id:
        variants.append(dict(base, warehouse_id=wid, bundle_id=bundle_id))

    uniq: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for p in variants:
        key = json.dumps(p, sort_keys=True, ensure_ascii=False)
        if key in seen:
            continue
        seen.add(key)
        uniq.append(p)

    last_data: Any = None
    last_err: str = ""
    last_status: int = 400

    for pl in uniq:
        task["last_supply_http_attempt_ts"] = now_ts()
        update_task(task)

        ok, data, err, status, headers = await api.post("/v1/draft/supply/create", pl)
        logging.info("supply_create: status=%s keys=%s", status, list(pl.keys()))
        if status == 429 or (err and str(err).startswith("rate_limit:")):
            try:
                ra = int(str(err).split(":", 1)[1]) if err else ON429_SHORT_RETRY_SEC
            except Exception:
                ra = ON429_SHORT_RETRY_SEC
            wait_sec = min(RATE_LIMIT_MAX_ON429, max(ON429_SHORT_RETRY_SEC, ra))
            _set_retry_after(task, wait_sec, jitter_max=1.0)
            task["last_error"] = "429 Too Many Requests (supply/create)"
            try:
                task["last_trace_id"] = headers.get("x-o3-trace-id", "")
            except Exception:
                pass
            update_task(task)
            return False, None, f"rate_limit:{wait_sec}", 429

        if 200 <= status < 300 and ok:
            op_id = data.get("operation_id") or data.get("result", {}).get("operation_id")
            if not op_id:
                return False, None, f"supply_create_no_operation_id:{data}", status
            return True, op_id, None, status

        last_data, last_err, last_status = data, err, status

    return False, None, f"supply_create_error:{last_err}|{last_data}", last_status

async def api_supply_create_status(api: OzonApi, operation_id: str) -> Tuple[bool, Optional[str], Optional[str], int]:
    payload = {"operation_id": operation_id}
    ok, data, err, status, headers = await api.post("/v1/draft/supply/create/status", payload)
    if status == 429:
        ra = _parse_retry_after(headers)
        return False, None, f"rate_limit:{min(RATE_LIMIT_MAX_ON429, max(ON429_SHORT_RETRY_SEC, ra))}", status
    if not ok:
        return False, None, f"supply_status_error:{err}|{data}", status
    raw_status = str(data.get("status") or (data.get("result") or {}).get("status") or "").lower()
    def _extract_order_id_from_supply_status(data: Dict[str, Any]) -> Optional[str]:
        direct = data.get("order_id")
        if direct not in (None, "", []):
            try:
                return str(int(direct))
            except Exception:
                return str(direct)
        res = data.get("result") or {}
        res_direct = res.get("order_id")
        if res_direct not in (None, "", []):
            try:
                return str(int(res_direct))
            except Exception:
                return str(res_direct)
        arr = data.get("order_ids")
        if isinstance(arr, list) and arr:
            try:
                return str(int(arr[0]))
            except Exception:
                return str(arr[0])
        res_arr = (data.get("result") or {}).get("order_ids")
        if isinstance(res_arr, list) and res_arr:
            try:
                return str(int(res_arr[0]))
            except Exception:
                return str(res_arr[0])
        return None
    order_id = _extract_order_id_from_supply_status(data)
    if "success" in raw_status or order_id:
        if not order_id:
            return False, None, f"supply_status_no_order_id:{data}", status
        return True, order_id, None, status
    if "progress" in raw_status or raw_status in ("in_progress", "pending", "processing", ""):
        return False, None, None, status
    errors = data.get("error_messages") or (data.get("result") or {}).get("error_messages") or []
    if errors:
        return False, None, f"supply_status_error_messages:{errors}", status
    if order_id:
        return True, order_id, None, status
    return False, None, f"supply_status_unrecognized:{raw_status}", status

def _first_str_in(o: Dict[str, Any], keys: List[str]) -> Optional[str]:
    for k in keys:
        v = o.get(k)
        if isinstance(v, str) and v:
            return v
        if isinstance(v, int):
            return str(v)
    return None

def _deep_find_supply_id(obj: Any) -> Optional[str]:
    try:
        def walk(o: Any) -> Optional[str]:
            if isinstance(o, dict):
                for k, v in o.items():
                    kl = str(k).lower()
                    if "supply" in kl and "id" in kl:
                        if isinstance(v, bool):
                            pass
                        elif isinstance(v, int) and v > 0:
                            return str(v)
                        elif isinstance(v, str) and v.strip() and v.strip().isdigit():
                            return v.strip()
                    r = walk(v)
                    if r:
                        return r
            elif isinstance(o, list):
                for it in o:
                    r = walk(it)
                    if r:
                        return r
            return None
        return walk(obj)
    except Exception:
        return None

def _extract_supply_order_info_from_response(js: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    for scope in [js, js.get("result") or {}]:
        if isinstance(scope, dict):
            sid = scope.get("supply_id")
            if sid not in (None, "", []):
                return str(sid), _first_str_in(scope, ["supply_order_number", "order_number", "number"])
    list_keys = ["orders", "supply_orders", "supplyOrders", "items", "data", "order_list", "supply_orders_list"]
    for scope in [js, js.get("result") or {}]:
        if not isinstance(scope, dict):
            continue
        for lk in list_keys:
            arr = scope.get(lk)
            if isinstance(arr, list) and arr:
                first = arr[0] if isinstance(arr[0], dict) else None
                if not isinstance(first, dict):
                    continue
                supplies = first.get("supplies") or []
                if isinstance(supplies, list):
                    for s in supplies:
                        if isinstance(s, dict) and s.get("supply_id") not in (None, "", 0):
                            sid = str(s.get("supply_id"))
                            num = _first_str_in(first, ["supply_order_number", "order_number", "number"])
                            return sid, num
                direct_sid = first.get("supply_id")
                if direct_sid not in (None, "", 0):
                    sid = str(direct_sid)
                    num = _first_str_in(first, ["supply_order_number", "order_number", "number"])
                    return sid, num
                num = _first_str_in(first, ["supply_order_number", "order_number", "number"])
                if num:
                    return None, num
    deep_sid = _deep_find_supply_id(js)
    if deep_sid:
        return deep_sid, _first_str_in(js.get("result") or {}, ["supply_order_number", "order_number", "number"])
    return None, None

def _extract_order_meta(js: Dict[str, Any]) -> Dict[str, Any]:
    meta = {
        "state": "",
        "timeslot": {"required": False, "can_set": False, "from": None, "to": None},
        "vehicle": {"required": False, "can_set": False},
        "contact": {"required": False, "can_set": False},
        "dropoff_warehouse_id": None,
    }
    candidate = None
    for scope in [js, js.get("result") or {}]:
        for key in ("orders", "supply_orders", "supplyOrders", "items", "data", "order_list", "supply_orders_list"):
            arr = scope.get(key)
            if isinstance(arr, list) and arr and isinstance(arr[0], dict):
                candidate = arr[0]
                break
        if candidate:
            break
    if not candidate and isinstance(js, dict):
        candidate = js.get("result") or js

    if not isinstance(candidate, dict):
        return meta

    meta["state"] = str(candidate.get("state") or candidate.get("status") or "")

    ts = candidate.get("timeslot") or {}
    meta["timeslot"]["can_set"] = bool(ts.get("can_set"))
    meta["timeslot"]["required"] = bool(ts.get("is_required") or ts.get("required"))
    value = ts.get("value") or {}
    val_ts = value.get("timeslot") or {}
    meta["timeslot"]["from"] = val_ts.get("from")
    meta["timeslot"]["to"] = val_ts.get("to")

    veh = candidate.get("vehicle") or {}
    meta["vehicle"]["can_set"] = bool(veh.get("can_set"))
    meta["vehicle"]["required"] = bool(veh.get("is_required") or veh.get("required"))

    contact = candidate.get("contact") or candidate.get("contact_info") or candidate.get("responsible") or {}
    if isinstance(contact, dict):
        meta["contact"]["can_set"] = bool(contact.get("can_set", False))
        meta["contact"]["required"] = bool(contact.get("is_required", False) or contact.get("required", False))

    for k in ("dropoff_warehouse_id", "dropoffWarehouseId", "warehouse_id", "warehouseId"):
        v = candidate.get(k)
        if v is not None:
            try:
                meta["dropoff_warehouse_id"] = int(v)
            except Exception:
                pass
            break

    return meta

async def api_supply_order_get(api: OzonApi, order_id: str) -> Tuple[bool, Optional[str], Optional[str], Optional[str], int, Dict[str, Any]]:
    """
    DISABLED per requirements: non-public /v1|v2/supply-order/get endpoints removed from operational flows.
    This function now returns empty/non-fatal result to avoid breaking any legacy code paths.
    """
    logger.warning("api_supply_order_get called but disabled - returning empty result (order_id=%s)", order_id)
    return False, None, None, "supply_order_get_disabled", 404, {}

async def api_supply_order_timeslot_set(
    api: OzonApi,
    order_id: str,
    from_in_timezone: str,
    to_in_timezone: str,
    slot_id: Optional[Any] = None,
    dropoff_warehouse_id: Optional[int] = None,
    supply_id: Optional[str] = None,
    chosen_warehouse_id: Optional[int] = None,
) -> Tuple[bool, Optional[str], int, Optional[str]]:
    try:
        oid = int(str(order_id))
    except Exception:
        oid = int(order_id)

    variants: List[Dict[str, Any]] = []

    v1 = {"supply_order_id": oid, "from_in_timezone": from_in_timezone, "to_in_timezone": to_in_timezone}
    if slot_id:
        v1["timeslot_id"] = slot_id
    variants.append(v1)

    v2 = {"supply_order_id": oid, "timeslot": {"from": from_in_timezone, "to": to_in_timezone}}
    variants.append(v2)

    v3 = {"order_id": oid, "from_in_timezone": from_in_timezone, "to_in_timezone": to_in_timezone}
    if slot_id:
        v3["timeslot_id"] = slot_id
    variants.append(v3)

    v4 = {"order_id": oid, "timeslot": {"from": from_in_timezone, "to": to_in_timezone}}
    variants.append(v4)

    last_err = None
    last_status = 400

    for body in variants:
        ok, data, err, status, headers = await api.post("/v1/supply-order/timeslot/update", body)
        if status == 404:
            return False, "not_supported_404", 404, None
        if status == 429:
            ra = _parse_retry_after(headers)
            return False, f"rate_limit:{ra}", 429, None

        if 200 <= status < 300 and ok:
            op_id = None
            try:
                op_id = (data or {}).get("operation_id") or (data or {}).get("result", {}).get("operation_id")
            except Exception:
                op_id = None
            return True, None, status, (str(op_id) if op_id else None)

        last_err, last_status = (err or f"http_status:{status}"), status

    return False, (last_err or "http_status:400"), last_status, None

async def api_operation_get(api: OzonApi, operation_id: str) -> Tuple[bool, Optional[str], Optional[str], int]:
    payload = {"operation_id": operation_id}
    ok, data, err, status, headers = await api.post("/v1/operation/get", payload)
    if status == 429:
        ra = _parse_retry_after(headers)
        return False, None, f"rate_limit:{min(RATE_LIMIT_MAX_ON429, max(ON429_SHORT_RETRY_SEC, ra))}", status
    if not ok:
        return False, None, f"operation_get_error:{err}|{data}", status
    s = (data.get("status") or (data.get("result") or {}).get("status") or "").upper()
    return True, s or "", None, status

def _canon_cargo_type(raw: Any) -> str:
    v = str(raw or "").strip().upper()
    return v if v in ("BOX", "PALLET") else "BOX"

def _build_cargoes_v1_payload(task: Dict[str, Any]) -> Dict[str, Any]:
    supply_id_raw = task.get("supply_id")
    if not supply_id_raw:
        raise RuntimeError("supply_id is missing; wait for /v2/supply-order/get -> orders.supplies.supply_id")
    try:
        supply_id = int(str(supply_id_raw).strip())
    except Exception:
        raise RuntimeError("supply_id must be int64-compatible")

    ctype = _canon_cargo_type(task.get("type") or task.get("cargo_type") or "BOX")
    cargoes: List[Dict[str, Any]] = []

    for line in task["sku_list"]:
        offer_id = str(line.get("sku"))
        boxes = int(line["boxes"])
        per_box = int(line["per_box"])

        for _ in range(boxes):
            item = {"offer_id": offer_id, "quant": per_box, "quantity": per_box}
            value = {"type": ctype, "items": [item]}
            cargoes.append({"key": str(uuid.uuid4()), "value": value})

    body = {"supply_id": supply_id, "delete_current_version": True, "cargoes": cargoes}
    return body

async def api_cargoes_create_v1(api: OzonApi, payload: Dict[str, Any]) -> Tuple[bool, Optional[str], Optional[str], int]:
    ok, data, err, status, headers = await api.post("/v1/cargoes/create", payload)
    if status == 429:
        ra = _parse_retry_after(headers)
        return False, None, f"rate_limit:{min(RATE_LIMIT_MAX_ON429, max(ON429_SHORT_RETRY_SEC, ra))}", status
    if not ok:
        return False, None, f"cargoes_create_v1_error:{err}|{data}", status
    op_id = data.get("operation_id") or data.get("result", {}).get("operation_id")
    if not op_id:
        return False, None, f"cargoes_create_v1_no_operation_id:{data}", status
    return True, op_id, None, status

async def api_cargoes_create_info_v2(api: OzonApi, operation_id: str) -> Tuple[bool, List[Dict[str, Any]], Optional[str], int]:
    payload = {"operation_id": operation_id}
    ok, data, err, status, headers = await api.post("/v2/cargoes/create/info", payload)
    if status == 429:
        ra = _parse_retry_after(headers)
        return False, [], f"rate_limit:{min(RATE_LIMIT_MAX_ON429, max(ON429_SHORT_RETRY_SEC, ra))}", status
    if not ok:
        return False, [], f"cargoes_info_v2_error:{err}|{data}", status

    raw_status = str(data.get("status") or (data.get("result") or {}).get("status") or "").upper()
    if raw_status in ("", "STATUS_IN_PROGRESS", "IN_PROGRESS", "PENDING"):
        return False, [], None, status

    if raw_status == "FAILED":
        try:
            reason_json = json.dumps(data, ensure_ascii=False)
        except Exception:
            reason_json = str(data)
        return False, [], f"cargoes_failed:{reason_json}", status

    cargo_ids: List[str] = []
    try:
        result = data.get("result") or {}
        arr = result.get("cargoes") or []
        if isinstance(arr, list):
            for entry in arr:
                if not isinstance(entry, dict):
                    continue
                val = entry.get("value") or {}
                cid = val.get("cargo_id")
                if cid not in (None, "", 0):
                    cargo_ids.append(str(cid))
    except Exception:
        pass

    if cargo_ids:
        return True, [{"cargo_id": cid} for cid in cargo_ids], None, status

    return False, [], f"cargoes_info_v2_no_cargoes:{data}", status

async def api_cargoes_create_info_v1(api: OzonApi, operation_id: str) -> Tuple[bool, List[Dict[str, Any]], Optional[str], int]:
    payload = {"operation_id": operation_id}
    ok, data, err, status, headers = await api.post("/v1/cargoes/create/info", payload)
    if status == 429:
        ra = _parse_retry_after(headers)
        return False, [], f"rate_limit:{min(RATE_LIMIT_MAX_ON429, max(ON429_SHORT_RETRY_SEC, ra))}", status
    if not ok:
        return False, [], f"cargoes_info_v1_error:{err}|{data}", status
    s = (data.get("status") or (data.get("result") or {}).get("status") or "").upper()
    if s in ("IN_PROGRESS", "PENDING", ""):
        return False, [], None, status
    if s not in ("SUCCESS", "OK", "DONE"):
        return False, [], f"cargoes_status_fail:{s}|{data}", status
    cargoes = data.get("cargoes") or data.get("result", {}).get("cargoes") or []
    if not cargoes:
        return False, [], f"cargoes_info_v1_no_cargoes:{data}", status
    return True, cargoes, None, status

async def api_labels_create(api: OzonApi, supply_id: str, cargo_ids: Optional[List[str]] = None) -> Tuple[bool, Optional[str], Optional[str], int]:
    payload: Dict[str, Any] = {"supply_id": supply_id}
    if cargo_ids:
        payload["cargo_ids"] = cargo_ids
    ok, data, err, status, headers = await api.post("/v1/cargoes-label/create", payload)
    if status == 429:
        ra = _parse_retry_after(headers)
        return False, None, f"rate_limit:{min(RATE_LIMIT_MAX_ON429, max(ON429_SHORT_RETRY_SEC, ra))}", status
    if not ok:
        return False, None, f"labels_create_error:{err}|{data}", status
    op_id = data.get("operation_id") or data.get("result", {}).get("operation_id")
    if not op_id:
        return False, None, f"labels_create_no_operation_id:{data}", status
    return True, op_id, None, status

async def api_labels_get(api: OzonApi, operation_id: str) -> Tuple[bool, Optional[str], Optional[str], int]:
    payload = {"operation_id": operation_id}
    ok, data, err, status, headers = await api.post("/v1/cargoes-label/get", payload)
    if status == 429:
        ra = _parse_retry_after(headers)
        return False, None, f"rate_limit:{min(RATE_LIMIT_MAX_ON429, max(ON429_SHORT_RETRY_SEC, ra))}", status
    if not ok:
        return False, None, f"labels_get_error:{err}|{data}", status
    s = (data.get("status") or (data.get("result") or {}).get("status") or "").upper()
    if s in ("IN_PROGRESS", "PENDING", ""):
        return False, None, None, status
    if s not in ("SUCCESS", "OK", "DONE"):
        return False, None, f"labels_status_fail:{s}|{data}", status
    file_guid = data.get("file_guid") or data.get("result", {}).get("file_guid")
    if not file_guid:
        return False, None, f"labels_no_file_guid:{data}", status
    return True, file_guid, None, status

async def api_labels_file(api: OzonApi, file_guid: str) -> Tuple[bool, Optional[bytes], Optional[str]]:
    ok, content, err = await api.get_pdf(f"/v1/cargoes-label/file/{file_guid}")
    if not ok:
        return False, None, err
    return True, content, None

# ================== Draft cooldown files ==================

_DRAFT_RATE_LOCK = threading.RLock()
_DRAFT_RATE_FILE = DATA_DIR / ".draft_create_next_allowed"
_DRAFT_COOLDOWN_FILE = DATA_DIR / ".draft_create_cooldown_until"
_DRAFT_RATE_LOCKFILE = DATA_DIR / ".draft_create_next_allowed.lock"

def _read_int_from_file(p: Path) -> int:
    try:
        s = p.read_text(encoding="utf-8").strip()
        return int(s) if s else 0
    except Exception:
        return 0

def _write_int_to_file(p: Path, v: int):
    try:
        p.write_text(str(int(v)), encoding="utf-8")
    except Exception:
        pass

@contextmanager
def _interprocess_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(str(path), "a+b") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        try:
            yield f
        finally:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)

def _read_next_allowed_ts() -> int:
    return _read_int_from_file(_DRAFT_RATE_FILE)

def _write_next_allowed_ts(ts: int):
    _write_int_to_file(_DRAFT_RATE_FILE, ts)

def _read_draft_cooldown_until() -> int:
    return _read_int_from_file(_DRAFT_COOLDOWN_FILE)

def _write_draft_cooldown_until(ts: int):
    _write_int_to_file(_DRAFT_COOLDOWN_FILE, ts)

def _reserve_draft_slot_or_wait() -> int:
    with _interprocess_lock(_DRAFT_RATE_LOCKFILE):
        with _DRAFT_RATE_LOCK:
            n = now_ts()
            next_allowed = _read_next_allowed_ts()
            if n < next_allowed:
                return max(1, next_allowed - n)
            _write_next_allowed_ts(n + DRAFT_CREATE_MIN_SPACING_SECONDS)
            return 0

def _draft_global_wait() -> int:
    n = now_ts()
    until = _read_draft_cooldown_until()
    if until > n:
        return until - n
    return 0

def _set_global_draft_cooldown(base_wait: float):
    sec = int(max(1.0, float(base_wait) + random.uniform(0.2, 1.1)))
    until = now_ts() + sec
    _write_draft_cooldown_until(until)
    logging.info("draft cooldown set: %ss (until=%s)", sec, until)

# ================== Timeslot + prompts helpers ==================

def _nearest_slot_within_delta(slots: List[Dict[str, Any]], desired_from_iso: str, delta_min: int, drop_id: Optional[int]=None) -> Optional[Dict[str, Any]]:
    try:
        target = datetime.fromisoformat(desired_from_iso.replace("Z", "+00:00")).timestamp()
    except Exception:
        return None
    best_after = None
    best_after_d = None
    best_any = None
    best_any_d = None
    for s in slots:
        if drop_id is not None:
            sid = s.get("drop_off_point_warehouse_id") or s.get("drop_off_warehouse_id")
            try:
                if sid is not None and int(sid) != int(drop_id):
                    continue
            except Exception:
                pass
        fr = s.get("from_in_timezone") or s.get("from")
        if not fr:
            continue
        try:
            ts = datetime.fromisoformat(fr.replace("Z", "+00:00")).timestamp()
        except Exception:
            continue
        d = abs(ts - target)
        if d > delta_min * 60:
            continue
        if ts >= target:
            if best_after_d is None or d < best_after_d:
                best_after = s
                best_after_d = d
        if best_any_d is None or d < best_any_d:
            best_any = s
            best_any_d = d
    return best_after or best_any

async def _auto_fill_timeslot_if_needed(task: Dict[str, Any], api: OzonApi, meta: Dict[str, Any], notify_text) -> bool:
    ts_meta = meta.get("timeslot") or {}
    can_set = bool(ts_meta.get("can_set"))
    has_from = ts_meta.get("from")
    has_to = ts_meta.get("to")

    if not can_set:
        return True

    fr = task.get("from_in_timezone") or task.get("desired_from_iso") or task.get("slot_from") or task.get("desired_from_iso")
    to = task.get("to_in_timezone") or task.get("desired_to_iso") or task.get("slot_to") or task.get("desired_to_iso")
    if not (fr and to):
        return False

    if has_from and has_to:
        def _eq(a, b):
            try:
                return int(datetime.fromisoformat(a.replace("Z","+00:00")).timestamp()) == int(datetime.fromisoformat(b.replace("Z","+00:00")).timestamp())
            except Exception:
                return a == b
        zf = _to_z_iso(fr) or fr
        zt = _to_z_iso(to) or to
        if _eq(has_from, zf) and _eq(has_to, zt):
            return True

    if task.get("order_timeslot_set_ok"):
        return True
    if now_ts() - int(task.get("order_timeslot_set_ts") or 0) < 10:
        return True

    ok, err, status, op_id = await api_supply_order_timeslot_set(
        api,
        task["order_id"],
        fr,
        to,
        task.get("slot_id"),
        task.get("dropoff_warehouse_id"),
        task.get("supply_id"),
        task.get("chosen_warehouse_id"),
    )
    task["order_timeslot_set_attempts"] = int(task.get("order_timeslot_set_attempts") or 0) + 1
    task["order_timeslot_set_ts"] = now_ts()

    if ok:
        task["order_timeslot_set_ok"] = True
        if op_id:
            task["order_timeslot_op_id"] = str(op_id)
        task["order_fast_poll_until_ts"] = now_ts() + int(max(ORDER_FAST_POLL_WINDOW_SECONDS, 300))
        task.pop("last_error", None)
        update_task(task)
        try:
            await notify_text(task["chat_id"], f"🟦 [{short(task['id'])}] Тайм‑слот заявки отправлен на установку (operation_id={op_id or '—'}).")
        except Exception:
            logging.exception("notify_text failed on timeslot set OK")
        return True

    if err == "not_supported_404" or status == 404:
        task["order_timeslot_set_ok"] = True
        task["order_timeslot_set_note"] = "timeslot API not available; relying on supply create"
        task["order_fast_poll_until_ts"] = now_ts() + int(ORDER_FAST_POLL_WINDOW_SECONDS)
        task["status"] = ST_ORDER_DATA_FILLING
        task["retry_after_ts"] = 0
        task["retry_after_jitter"] = 0.0
        task["next_attempt_ts"] = now_ts()
        task.pop("last_error", None)
        update_task(task)
        try:
            await notify_text(task["chat_id"], f"🟦 [{short(task['id'])}] Тайм‑слот API заявки недоступен (404). Продолжаем.")
        except Exception:
            logging.exception("notify_text failed on promote after 404")
        return True

    task["last_error"] = f"timeslot_set:{err or status}"
    update_task(task)
    last_warn = int(task.get("order_timeslot_warn_ts") or 0)
    if now_ts() - last_warn >= PROMPT_MIN_INTERVAL:
        task["order_timeslot_warn_ts"] = now_ts()
        update_task(task)
        try:
            await notify_text(task["chat_id"], f"🟨 [{short(task['id'])}] Не удалось проставить тайм‑слот в заявке (повторим).")
        except Exception:
            logging.exception("notify_text failed on timeslot warn")
    return False

# ================== Helpers ==================

def _inc_backoff(task: Dict[str, Any]) -> int:
    b = int(task.get("create_backoff_sec") or CREATE_INITIAL_BACKOFF)
    b = min(max(b, CREATE_INITIAL_BACKOFF), CREATE_MAX_BACKOFF)
    task["create_backoff_sec"] = min(max(b * 2, CREATE_INITIAL_BACKOFF), CREATE_MAX_BACKOFF)
    return b

def _set_retry_after(task: Dict[str, Any], seconds: int, jitter_max: float = 0.3):
    seconds = max(1, int(seconds))
    jitter = random.uniform(0.0, float(jitter_max))
    task["retry_after_ts"] = now_ts() + seconds
    task["next_attempt_ts"] = task["retry_after_ts"]
    task["retry_after_jitter"] = jitter

def _fmt_local_range_short(fr_iso: Optional[str], to_iso: Optional[str]) -> str:
    def _p(s: Optional[str]) -> Optional[datetime]:
        if not s:
            return None
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        except Exception:
            return None
        return dt.astimezone(TZ_YEKAT)
    a = _p(fr_iso)
    b = _p(to_iso)
    if not a or not b:
        return "—"
    return f"{a.strftime('%d.%m %H:%M')}–{b.strftime('%H:%M')}"

# ================== Self-wakeup ==================

_pending_wakeup_ts: Optional[float] = None

async def _wakeup_runner(target_ts: float, notify_text, notify_file):
    global _pending_wakeup_ts
    try:
        while True:
            delay = max(0.0, target_ts - time.time())
            if delay > 0:
                await asyncio.sleep(min(delay, 0.5))
                continue
            if _lock.locked():
                logging.debug("self-wakeup: locked, retry in 0.5s")
                await asyncio.sleep(0.5)
                continue
            _pending_wakeup_ts = None
            try:
                await process_tasks(notify_text, notify_file)
            except Exception:
                logging.exception("self-wakeup process_tasks failed")

            try:
                n = now_ts()
                soon: Optional[float] = None
                for t in list_tasks():
                    nt = int(t.get("next_attempt_ts") or 0)
                    if nt and nt > n:
                        d = nt - n + float(t.get("retry_after_jitter") or 0.0)
                        if d > 0:
                            soon = d if soon is None else min(soon, d)
                if soon is not None and soon <= max(SELF_WAKEUP_THRESHOLD_SECONDS, 600):
                    logging.debug("self-wakeup: chain schedule in %.2fs", soon)
                    _schedule_self_wakeup(soon, notify_text, notify_file)
            except Exception:
                logging.exception("self-wakeup: chain scheduling failed")
            break
    except Exception:
        _pending_wakeup_ts = None
        logging.exception("self-wakeup runner crashed")

def _schedule_self_wakeup(delay: float, notify_text, notify_file):
    global _pending_wakeup_ts
    delay = max(0.05, float(delay))
    target_ts = time.time() + delay
    if _pending_wakeup_ts is not None and _pending_wakeup_ts <= target_ts:
        logging.debug("self-wakeup: earlier wakeup already scheduled, skip new")
        return
    _pending_wakeup_ts = target_ts
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_wakeup_runner(target_ts, notify_text, notify_file))
        logging.debug("self-wakeup: scheduled in %.2fs", delay)
    except RuntimeError:
        _pending_wakeup_ts = None

# ================== Sanitize legacy ==================

def sanitize_tasks_legacy_cyrillic():
    ensure_loaded()
    changed = False
    for t in _tasks:
        st = str(t.get("status") or "")
        st_norm = (
            st.replace("PREП", "PREP")
              .replace("CARGO_PREП", "CARGO_PREP")
              .replace("ST_CARGO_PREП", "ST_CARGO_PREP")
        )
        if st_norm != st:
            t["status"] = st_norm
            t["next_attempt_ts"] = 0
            t["retry_after_ts"] = 0
            t.pop("creating", None)
            changed = True

        err = str(t.get("last_error") or "")
        if "ST_CARGO_PREП" in err or "CARGO_PREП" in err:
            t["last_error"] = ""
            t["next_attempt_ts"] = 0
            t["retry_after_ts"] = 0
            changed = True

        sid = str(t.get("supply_id") or "")
        for num_key in ("supply_order_number", "order_number", "number"):
            num = str(t.get(num_key) or "")
            if sid and num and sid == num:
                t["supply_id"] = None
                t["supply_id_verified"] = False
                t["status"] = ST_ORDER_DATA_FILLING if t.get("order_id") else ST_SUPPLY_ORDER_FETCH
                t["next_attempt_ts"] = 0
                t["retry_after_ts"] = 0
                changed = True
                break

        if not t.get("supply_id_verified"):
            try:
                wid = int(t.get("chosen_warehouse_id") or 0)
            except Exception:
                wid = 0
            try:
                did = int(t.get("dropoff_warehouse_id") or t.get("drop_off_point_warehouse_id") or t.get("drop_off_id") or 0)
            except Exception:
                did = 0
            try:
                sid_int = int(sid) if sid else 0
            except Exception:
                sid_int = 0

            if sid_int and (sid_int == wid or sid_int == did):
                t["supply_id"] = None
                t["supply_id_verified"] = False
                if t.get("order_id"):
                    t["status"] = ST_ORDER_DATA_FILLING
                else:
                    t["status"] = ST_SUPPLY_ORDER_FETCH
                t["next_attempt_ts"] = 0
                t["retry_after_ts"] = 0
                changed = True

    if changed:
        save_tasks()
        logging.debug("sanitize_tasks_legacy_cyrillic: normalized statuses and reset timings")

# ================== Create/Cancel/Retry/Delete (public) ==================

def create_tasks_from_template(raw_text: str, mode: str, chat_id: int) -> List[Dict[str, Any]]:
    ensure_loaded()
    date_iso, start_hhmm, end_hhmm, items = parse_template(raw_text)
    out: List[Dict[str, Any]] = []
    for it in items:
        task: Dict[str, Any] = {
            "id": str(uuid.uuid4()),
            "created_ts": now_ts(),
            "updated_ts": now_ts(),
            "status": ST_WAIT_WINDOW,
            "mode": (mode or "").upper() or "FBO",
            "chat_id": int(chat_id),

            "date": date_iso,
            "desired_from_iso": to_iso_local(date_iso, start_hhmm),
            "desired_to_iso": to_iso_local(date_iso, end_hhmm),
            "window_start_ts": to_ts_local(date_iso, start_hhmm),
            "window_end_ts": to_ts_local(date_iso, end_hhmm),

            "sku_list": [{
                "sku": it["sku"],
                "total_qty": int(it["total_qty"]),
                "boxes": int(it["boxes"]),
                "per_box": int(it["per_box"]),
                "warehouse_name": it["warehouse_name"]
            }],
            "chosen_warehouse_id": resolve_warehouse_id(it["warehouse_name"]),
            "next_attempt_ts": 0,
            "retry_after_ts": 0,
            "creating": False,
            "last_error": ""
        }
        if not task.get("drop_off_id") and DROP_OFF_WAREHOUSE_ID:
            task["drop_off_id"] = int(DROP_OFF_WAREHOUSE_ID)

        add_task(task)
        out.append(task)
    return out

def cancel_task(task_id: str) -> bool:
    ensure_loaded()
    t = get_task(task_id)
    if not t:
        return False
    t["status"] = ST_CANCELED
    update_task(t)
    return True

def retry_task(task_id: str) -> Tuple[bool, str]:
    ensure_loaded()
    t = get_task(task_id)
    if not t:
        return False, "Задача не найдена"
    for k in list(t.keys()):
        if k.endswith("_operation_id") or k in (
            "draft_id", "order_id", "supply_id", "supply_order_number",
            "cargo_payload", "cargo_ids", "labels_file_guid", "labels_pdf_path",
            "slot_from", "slot_to", "slot_id", "last_error", "op_started_ts", "op_retries",
            "retry_after_ts", "next_attempt_ts", "creating", "create_attempts", "create_backoff_sec",
            "order_timeslot_set_ok", "order_timeslot_set_attempts", "order_timeslot_set_ts",
            "need_vehicle", "order_vehicle_text", "need_contact", "order_contact_phone", "order_contact_name",
            "need_vehicle_prompt_ts", "need_contact_prompt_ts", "dropoff_warehouse_id", "bundle_id", "supply_type",
            "retry_after_jitter", "draft_info_rl_attempts", "timeslot_rl_attempts",
            "supply_status_rl_attempts", "order_get_rl_attempts", "order_timeslot_warn_ts",
            "draft_last_try_ts", "draft_rl_attempts", "cargo_payload_variants", "cargo_variant_idx",
            "last_draft_http_attempt_ts", "__draft_pacing_scheduled_ts", "last_trace_id", "cargo_api_ver",
            "supply_id_verified", "order_fast_poll_until_ts", "supply_id_wait_started_ts", "supply_id_timeout_alert_ts",
            "order_timeslot_op_unsupported", "order_timeslot_refresh_ts", "last_supply_http_attempt_ts",
            "cargo_prep_prompted", "autodelete_ts", "created_message_ts",
            "draft_created_ts", "draft_404_count", "draft_first_404_ts", "stale_draft_recreates", "last_draft_missing_ts"
        ):
            t.pop(k, None)
    t["status"] = ST_WAIT_WINDOW
    t["next_attempt_ts"] = 0
    t["retry_after_ts"] = 0
    t["creating"] = False
    update_task(t)
    return True, "OK"

# ================== Parse template (public) ==================

def parse_template(text: str) -> Tuple[str, str, str, List[Dict[str, Any]]]:
    if not text or not isinstance(text, str):
        raise ValueError("Пустой шаблон.")
    text = "\n".join(_canon_spaces_and_dashes(line) for line in text.splitlines())
    lines = [l.rstrip() for l in text.splitlines() if l.strip()]
    if not lines:
        raise ValueError("Пустой шаблон.")
    header_idx, date_iso, start_hhmm, end_hhmm = _scan_header(lines)
    items: List[Dict[str, Any]] = []
    for raw_line in lines[header_idx + 1:]:
        if not raw_line.strip():
            continue
        parsed = _parse_item_line(raw_line)
        if not parsed:
            logging.error("Line parse failed: %r", raw_line)
            raise ValueError(f"Не удалось распарсить строку: '{raw_line}'")
        if parsed["total_qty"] != parsed["boxes"] * parsed["per_box"]:
            raise ValueError(f"Строка '{raw_line}': {parsed['total_qty']} != {parsed['boxes']}*{parsed['per_box']}")
        items.append(parsed)
    if not items:
        raise ValueError("Нет позиций.")
    return date_iso, start_hhmm, end_hhmm, items

# ================== Auto-delete helpers ==================

def _cleanup_created_tasks() -> int:
    """
    Агрессивная очистка:
    - Удаляет финальные задачи (ST_DONE, ST_FAILED, ST_CANCELED) немедленно.
    - Удаляет задачи со статусом "Создано" сразу при AUTO_DELETE_CREATED_IMMEDIATE=1
      или после наступления их autodelete_ts.
    """
    ensure_loaded()
    now = now_ts()
    to_keep: List[Dict[str, Any]] = []
    removed = 0
    for t in _tasks:
        st = str(t.get("status") or "").upper()
        if st in (ST_DONE, ST_FAILED, ST_CANCELED):
            removed += 1
            continue
        if st == UI_STATUS_CREATED.upper():
            ad = int(t.get("autodelete_ts") or 0)
            if AUTO_DELETE_CREATED_IMMEDIATE:
                removed += 1
                continue
            if ad and now >= ad:
                removed += 1
                continue
        to_keep.append(t)
    if removed:
        _tasks[:] = to_keep
        save_tasks()
    return removed

# ================== Main stepper ==================

async def advance_task(task: Dict[str, Any], api: OzonApi, notify_text: Callable[[int, str], Awaitable[None]], notify_file: Callable[[int, str, Optional[str]], Awaitable[None]]) -> None:
    # Пропуск финальных и визуально "Создано"
    if task.get("status") in (ST_DONE, ST_FAILED, ST_CANCELED, UI_STATUS_CREATED):
        return

    n = now_ts()
    if n > task.get("window_end_ts", n + 1_000_000_000):
        task["status"] = ST_FAILED
        task["last_error"] = "Окно истекло"
        update_task(task)
        try:
            await notify_text(task["chat_id"], f"🟥 [{short(task['id'])}] Окно истекло.")
        except Exception:
            logging.exception("notify_text failed on window expired")
        return

    st = str(task.get("status") or "").upper()
    if task.get("supply_id_verified") and task.get("supply_id") and st not in (ST_CARGO_PREP, ST_CARGO_CREATING, ST_POLL_CARGO, ST_LABELS_CREATING, ST_POLL_LABELS, ST_DONE):
        task["status"] = ST_CARGO_PREP
        task["next_attempt_ts"] = now_ts()
        task["retry_after_ts"] = 0
        task["retry_after_jitter"] = 0.0
        update_task(task)

    st = str(task.get("status") or "").upper()

    if st not in (ST_CARGO_PREP, ST_CARGO_CREATING, ST_POLL_CARGO, ST_LABELS_CREATING, ST_POLL_LABELS):
        next_ts = int(task.get("next_attempt_ts") or 0)
        if n < next_ts:
            return
        rat = int(task.get("retry_after_ts") or 0)
        if rat and n < rat:
            return

    def schedule(sec: int, mark: Optional[str] = None):
        task["next_attempt_ts"] = now_ts() + max(1, int(sec))
        if mark:
            task[mark] = now_ts()
        update_task(task)

    if task.get("order_id") and st in (ST_TIMESLOT_SEARCH, "RATE_LIMIT", "BOOKED", "booked", ""):
        task["status"] = ST_SUPPLY_ORDER_FETCH
        task["op_started_ts"] = now_ts()
        task["op_retries"] = 0
        update_task(task)
        schedule(1)
        return

    try:
        if st == ST_WAIT_WINDOW:
            task["status"] = ST_DRAFT_CREATING
            task["creating"] = False
            task.pop("create_backoff_sec", None)
            # Сброс счётчиков 404 по драфту
            task.pop("draft_404_count", None)
            task.pop("draft_first_404_ts", None)
            task.pop("draft_created_ts", None)
            update_task(task)
            schedule(1)
            return

        if st == ST_DRAFT_CREATING:
            if not SUPPLY_DISABLE_INTERNAL_GATE:
                cd = _draft_global_wait()
                if cd > 0:
                    schedule(cd, mark="__draft_pacing_scheduled_ts")
                    return

                wait = _reserve_draft_slot_or_wait()
                if wait > 0:
                    schedule(wait, mark="__draft_pacing_scheduled_ts")
                    return

            if task.get("creating"):
                return

            task["creating"] = True
            task["draft_last_try_ts"] = now_ts()
            task["last_draft_http_attempt_ts"] = now_ts()
            task["last_supply_http_attempt_ts"] = task.get("last_supply_http_attempt_ts") or 0
            update_task(task)

            ok, op_id, err, status = await api_draft_create(api, task)
            task["creating"] = False

            if status == 429 or (err and str(err).startswith("rate_limit:")):
                try:
                    delay = int(str(err).split(":", 1)[1]) + 1
                except Exception:
                    delay = max(2, int(task.get("retry_after_ts", 0) - now_ts()) + 1)
                schedule(max(1, delay))
                return

            if not ok:
                if str(err).startswith("http_status:5") or str(err).startswith("http_error:") or status >= 500 or status == 0 or (err == "http_timeout"):
                    delay = max(_inc_backoff(task), SUPPLY_CREATE_MIN_RETRY_SECONDS)
                    task["last_error"] = f"server_or_timeout_error:{err}"
                    update_task(task)
                    schedule(delay)
                    return
                task["status"] = ST_FAILED
                task["last_error"] = str(err)
                update_task(task)
                try:
                    await notify_text(task["chat_id"], f"🟥 [{short(task['id'])}] draft/create: {err}")
                except Exception:
                    logging.exception("notify_text failed on draft error")
                return

            task.pop("draft_rl_attempts", None)
            task["draft_operation_id"] = op_id
            task["op_started_ts"] = now_ts()
            task["op_retries"] = 0
            task.pop("draft_created_ts", None)
            update_task(task)
            task["status"] = ST_POLL_DRAFT
            update_task(task)
            schedule(OPERATION_POLL_INTERVAL_SECONDS)
            return

        if st == ST_POLL_DRAFT:
            if now_ts() - int(task.get("op_started_ts") or now_ts()) > OPERATION_POLL_TIMEOUT_SECONDS:
                task["status"] = ST_FAILED
                task["last_error"] = "draft timeout"
                update_task(task)
                try:
                    await notify_text(task["chat_id"], f"🟥 [{short(task['id'])}] Таймаут draft/info")
                except Exception:
                    logging.exception("notify_text failed on draft timeout")
                return

            ok, draft_id, warehouses, err, status = await api_draft_create_info(api, task["draft_operation_id"])
            if status == 429 or (err and str(err or "").startswith("rate_limit:")):
                attempts = int(task.get("draft_info_rl_attempts") or 0) + 1
                task["draft_info_rl_attempts"] = attempts
                try:
                    ra = int(str(err).split(":", 1)[1]) if err else ON429_SHORT_RETRY_SEC
                except Exception:
                    ra = ON429_SHORT_RETRY_SEC
                base = min(RATE_LIMIT_MAX_ON429, max(ON429_SHORT_RETRY_SEC, ra))
                wait_sec = min(60, base + attempts * 2)
                _set_retry_after(task, wait_sec, jitter_max=1.5)
                task["last_error"] = "429 Too Many Requests (draft/info)"
                update_task(task)
                schedule(wait_sec + 1)
                return

            if task.get("draft_info_rl_attempts"):
                task.pop("draft_info_rl_attempts", None)

            if err:
                task["op_retries"] = int(task.get("op_retries") or 0) + 1
                if task["op_retries"] > SUPPLY_MAX_OPERATION_RETRIES:
                    task["status"] = ST_FAILED
                    task["last_error"] = str(err)
                    update_task(task)
                    try:
                        await notify_text(task["chat_id"], f"🟥 [{short(task['id'])}] draft/info: {err}")
                    except Exception:
                        logging.exception("notify_text failed on draft/info retries")
                    return
                schedule(OPERATION_POLL_INTERVAL_SECONDS)
                return

            if not ok:
                schedule(OPERATION_POLL_INTERVAL_SECONDS)
                return

            task["draft_id"] = draft_id
            task["draft_created_ts"] = now_ts()  # метка возраста черновика
            task.pop("draft_404_count", None)
            task.pop("draft_first_404_ts", None)

            chosen_wid = choose_warehouse_smart(task, warehouses) or task.get("chosen_warehouse_id")
            task["chosen_warehouse_id"] = chosen_wid

            if chosen_wid:
                for w in warehouses:
                    try:
                        wid = int((w or {}).get("warehouse_id"))
                    except Exception:
                        wid = None
                    if wid == int(chosen_wid):
                        bndl = (w or {}).get("bundle_id")
                        if bndl:
                            task["bundle_id"] = bndl
                        break

            task["status"] = ST_TIMESLOT_SEARCH
            update_task(task)
            schedule(1)
            return

        if st == ST_TIMESLOT_SEARCH:
            wid = int(task.get("chosen_warehouse_id") or 0)
            if wid <= 0:
                task["status"] = ST_FAILED
                task["last_error"] = "warehouse not resolved"
                update_task(task)
                try:
                    await notify_text(task["chat_id"], f"🟥 [{short(task['id'])}] Не удалось выбрать склад.")
                except Exception:
                    logging.exception("notify_text failed on no warehouse")
                return

            ok, slots, err, status = await api_timeslot_info(api, task["draft_id"], [wid], task["date"], task.get("bundle_id") if task.get("bundle_id") else None)
            if status == 429 or (err and str(err or "").startswith("rate_limit:")):
                attempts = int(task.get("timeslot_rl_attempts") or 0) + 1
                task["timeslot_rl_attempts"] = attempts
                try:
                    ra = int(str(err).split(":", 1)[1]) if err else ON429_SHORT_RETRY_SEC
                except Exception:
                    ra = ON429_SHORT_RETRY_SEC
                base = min(RATE_LIMIT_MAX_ON429, max(ON429_SHORT_RETRY_SEC, ra))
                wait_sec = min(45, base + attempts * 2)
                _set_retry_after(task, wait_sec, jitter_max=1.5)
                task["last_error"] = "429 Too Many Requests (timeslot)"
                update_task(task)
                schedule(wait_sec + 1)
                return

            if status == 404 or err == "not_found_404":
                # Новая логика: немедленно пересоздаём черновик без лимитов
                try:
                    logging.info(
                        "draft %s missing; recreate draft after 404 timeslot/info (task=%s)",
                        str(task.get("draft_id") or "—"),
                        short(task.get("id") or "")
                    )
                except Exception:
                    pass
                # Телеметрия
                task["stale_draft_recreates"] = int(task.get("stale_draft_recreates") or 0) + 1
                task["last_draft_missing_ts"] = now_ts()
                # Сброс черновика
                task.pop("draft_id", None)
                task.pop("draft_operation_id", None)
                task.pop("draft_created_ts", None)
                task.pop("draft_404_count", None)
                task.pop("draft_first_404_ts", None)
                task["status"] = ST_DRAFT_CREATING
                update_task(task)
                schedule(1)
                return

            if task.get("timeslot_rl_attempts"):
                task.pop("timeslot_rl_attempts", None)

            if not ok or not slots:
                schedule(SLOT_POLL_INTERVAL_SECONDS)
                return

            desired_from = task["desired_from_iso"]
            desired_to = task["desired_to_iso"]
            matched = None
            for s in slots:
                if (s.get("from_in_timezone") == desired_from and s.get("to_in_timezone") == desired_to):
                    matched = s
                    break

            if not matched and TIMESLOT_ALLOW_FALLBACK:
                drop_id = int(DROP_OFF_WAREHOUSE_ID) if DROP_OFF_WAREHOUSE_ID else None
                near = _nearest_slot_within_delta(slots, desired_from, TIMESLOT_FALLBACK_DELTA_MIN, drop_id=drop_id)
                if near:
                    matched = near
                    desired_from = near.get("from_in_timezone") or near.get("from") or desired_from
                    desired_to = near.get("to_in_timezone") or near.get("to") or desired_to

            if not matched:
                schedule(SLOT_POLL_INTERVAL_SECONDS)
                return

            task["slot_from"] = matched.get("from_in_timezone") or matched.get("from")
            task["slot_to"] = matched.get("to_in_timezone") or matched.get("to")
            task["slot_id"] = matched.get("id") or matched.get("timeslot_id") or matched.get("slot_id")
            if matched.get("drop_off_point_warehouse_id"):
                task["dropoff_warehouse_id"] = int(matched["drop_off_point_warehouse_id"])
                update_task(task)

            # Пытаемся закрепить слот в черновике — 404 на set НЕ вызывает пересоздание
            if task.get("slot_id"):
                ts_payload = {"id": task["slot_id"]}
            else:
                z_from = _to_z_iso(task["slot_from"]) or task["slot_from"]
                z_to = _to_z_iso(task["slot_to"]) or task["slot_to"]
                ts_payload = {"start_time": z_from, "end_time": z_to}

            drop_id_for_set = task.get("dropoff_warehouse_id") or _extract_dropoff_id(task)
            if drop_id_for_set:
                ok_set, err_set, status_set = await api_draft_timeslot_set(api, int(task["draft_id"]), int(drop_id_for_set), ts_payload)
                if status_set == 429 or (err_set and str(err_set).startswith("rate_limit:")):
                    try:
                        ra = int(str(err_set).split(":", 1)[1]) if err_set else ON429_SHORT_RETRY_SEC
                    except Exception:
                        ra = ON429_SHORT_RETRY_SEC
                    _set_retry_after(task, min(RATE_LIMIT_MAX_ON429, max(1, ra)), jitter_max=1.0)
                    task["last_error"] = "429 Too Many Requests (draft/timeslot/set)"
                    update_task(task)
                    schedule(min(RATE_LIMIT_MAX_ON429, max(1, ra)) + 1)
                    return
                if status_set == 404 or err_set == "not_found_404":
                    # Просто пометим, что закрепление черновика недоступно, и поедем дальше
                    task["draft_timeslot_set_unsupported"] = True
                    update_task(task)
                elif not ok_set:
                    logging.warning("draft/timeslot/set failed (non-blocking), proceeding")

            # Закрепляем актуальное окно для supply/create
            task["desired_from_iso"] = desired_from
            task["desired_to_iso"] = desired_to
            update_task(task)

            task["status"] = ST_SUPPLY_CREATING
            update_task(task)
            schedule(SUPPLY_CREATE_STAGE_DELAY_SECONDS)
            return

        if st == ST_SUPPLY_CREATING:
            if task.get("creating"):
                return
            task["creating"] = True
            update_task(task)

            ok, op_id, err, status = await api_supply_create(api, task)
            task["creating"] = False

            if status == 429 or (err and str(err or "").startswith("rate_limit:")):
                try:
                    ra = int(str(err).split(":", 1)[1]) if err else ON429_SHORT_RETRY_SEC
                except Exception:
                    ra = ON429_SHORT_RETRY_SEC
                _set_retry_after(task, min(RATE_LIMIT_MAX_ON429, max(1, ra)), jitter_max=1.5)
                task["last_error"] = "429 Too Many Requests (supply/create)"
                update_task(task)
                schedule(min(RATE_LIMIT_MAX_ON429, max(1, ra)) + 1)
                return

            if not ok:
                if str(err).startswith("http_status:5") or str(err).startswith("http_error:") or status >= 500 or status == 0 or (err == "http_timeout"):
                    delay = max(_inc_backoff(task), SUPPLY_CREATE_MIN_RETRY_SECONDS)
                    task["last_error"] = f"supply_create_server_or_timeout:{err}"
                    update_task(task)
                    schedule(delay)
                    return
                task["last_error"] = str(err)
                update_task(task)
                schedule(SLOT_POLL_INTERVAL_SECONDS)
                task["status"] = ST_TIMESLOT_SEARCH
                update_task(task)
                return

            task.pop("supply_create_rl_count", None)
            task["supply_operation_id"] = op_id
            task["op_started_ts"] = now_ts()
            task["op_retries"] = 0
            task["status"] = ST_POLL_SUPPLY
            update_task(task)
            schedule(OPERATION_POLL_INTERVAL_SECONDS)
            return

        if st == ST_POLL_SUPPLY:
            ok, order_id, err, status = await api_supply_create_status(api, task["supply_operation_id"])
            if status == 429 or (err and str(err or "").startswith("rate_limit:")):
                attempts = int(task.get("supply_status_rl_attempts") or 0) + 1
                task["supply_status_rl_attempts"] = attempts
                try:
                    ra = int(str(err).split(":", 1)[1]) if err else ON429_SHORT_RETRY_SEC
                except Exception:
                    ra = ON429_SHORT_RETRY_SEC
                base = min(RATE_LIMIT_MAX_ON429, max(ON429_SHORT_RETRY_SEC, ra))
                wait_sec = min(45, base + attempts * 2)
                _set_retry_after(task, wait_sec, jitter_max=1.5)
                task["last_error"] = "429 Too Many Requests (supply/status)"
                update_task(task)
                schedule(wait_sec + 1)
                return
            if task.get("supply_status_rl_attempts"):
                task.pop("supply_status_rl_attempts", None)

            if err:
                task["op_retries"] = int(task.get("op_retries") or 0) + 1
                if task["op_retries"] > SUPPLY_MAX_OPERATION_RETRIES:
                    task["status"] = ST_FAILED
                    task["last_error"] = str(err)
                    update_task(task)
                    try:
                        await notify_text(task["chat_id"], f"🟥 [{short(task['id'])}] supply/status: {err}")
                    except Exception:
                        logging.exception("notify_text failed on supply status")
                    return
                schedule(OPERATION_POLL_INTERVAL_SECONDS)
                return

            if not ok:
                schedule(OPERATION_POLL_INTERVAL_SECONDS)
            else:
                task["order_id"] = order_id
                task["status"] = ST_SUPPLY_ORDER_FETCH
                task["op_started_ts"] = now_ts()
                task["op_retries"] = 0
                task.setdefault("supply_id_wait_started_ts", now_ts())
                update_task(task)
                schedule(1)
            return

        if st in (ST_SUPPLY_ORDER_FETCH, ST_ORDER_DATA_FILLING):
            task.setdefault("supply_id_wait_started_ts", now_ts())

            # Removed api_supply_order_get per requirements - avoid non-public /v1|v2/supply-order/get
            # Instead, proceed directly to ORDER_DATA_FILLING/CREATED status
            # Metadata like supply_id, dropoff_warehouse_id can be filled from earlier create/status result if available
            
            # Extract any metadata we already have from task
            supply_id = task.get("supply_id")
            number = task.get("supply_order_number") or ""
            
            # If we don't have supply_id yet, that's okay - proceed anyway
            # User will complete details in UI
            
            # Auto-fill timeslot if needed (uses available task data, no API call required here)
            meta = {}
            if task.get("dropoff_warehouse_id"):
                meta["dropoff_warehouse_id"] = task["dropoff_warehouse_id"]
            
            await _auto_fill_timeslot_if_needed(task, api, meta, notify_text)

            if not task.get("cargo_prep_prompted"):
                link_main = SELLER_PORTAL_URL
                rng = _fmt_local_range_short(task.get("desired_from_iso") or task.get("slot_from"),
                                             task.get("desired_to_iso") or task.get("slot_to"))
                num = task.get("supply_order_number") or "—"
                wh_disp = warehouse_display(task)

                msg = (
                    "🟦 Заявка создана\n"
                    f"• Номер: {num}\n"
                    f"• Окно приёмки: {rng}\n"
                    f"• Склад: {wh_disp}\n"
                    f"• Личный кабинет: {link_main}\n\n"
                    "Действия: откройте ЛК, укажите грузоместа и количество, затем сгенерируйте этикетки."
                )
                try:
                    await notify_text(task["chat_id"], msg)
                except Exception:
                    logging.exception("notify_text failed on ORDER_DATA_FILLING prompt")

                task["cargo_prep_prompted"] = True
                task["created_message_ts"] = now_ts()
                task["autodelete_ts"] = now_ts() + int(AUTO_DELETE_CREATED_MINUTES) * 60

            if AUTO_DELETE_CREATED_IMMEDIATE:
                delete_task(task["id"])
                return

            task["status"] = UI_STATUS_CREATED
            task["updated_ts"] = now_ts()
                update_task(task)
                return

            else:
                task["op_retries"] = int(task.get("op_retries") or 0) + 1
                if task["op_retries"] > max(SUPPLY_MAX_OPERATION_RETRIES, ORDER_FILL_MAX_RETRIES):
                    task["status"] = ST_FAILED
                    task["last_error"] = str(err)
                    update_task(task)
                    try:
                        await notify_text(task["chat_id"], f"🟥 [{short(task['id'])}] supply-order/get: {err}")
                    except Exception:
                        logging.exception("notify_text failed on supply-order/get")
                    return

            task["status"] = ST_ORDER_DATA_FILLING
            update_task(task)

            fast_until = int(task.get("order_fast_poll_until_ts") or 0)
            if fast_until and now_ts() < fast_until and not task.get("supply_id"):
                schedule(max(3, int(ORDER_FAST_POLL_SECONDS)))
            else:
                schedule(max(5, int(ORDER_FILL_POLL_INTERVAL_SECONDS)))
            return

    except Exception as e:
        logging.exception("advance_task exception")
        task["status"] = ST_FAILED
        task["last_error"] = f"exception:{e}"
        update_task(task)
        try:
            await notify_text(task["chat_id"], f"🟥 [{short(task['id'])}] Исключение: {e}")
        except Exception:
            logging.exception("notify_text failed on exception")

# ================== Scheduler wiring ==================

_SCHEDULER_JOB_ID = "supply_process_tasks"

async def process_tasks(notify_text: Callable[[int, str], Awaitable[None]], notify_file: Callable[[int, str, Optional[str]], Awaitable[None]]):
    ensure_loaded()
    sanitize_tasks_legacy_cyrillic()

    try:
        removed = _cleanup_created_tasks()
        if removed:
            logging.info("auto-clean tasks: removed=%s", removed)
    except Exception:
        logging.exception("cleanup_created_tasks failed")

    if _lock.locked():
        logging.info("process_tasks: SKIP (already running)")
        return

    logging.info("process_tasks: START")
    async with _lock:
        api = OzonApi(OZON_CLIENT_ID, OZON_API_KEY, timeout=API_TIMEOUT_SECONDS)

        drafts_done = 0
        supplies_done = 0

        active_list = list_tasks()
        for task in active_list:
            if task.get("status") == ST_CANCELED:
                continue

            st = str(task.get("status") or "")
            if st == UI_STATUS_CREATED:
                continue

            st_u = st.upper()

            if st_u == ST_DRAFT_CREATING and drafts_done >= SUPPLY_MAX_DRAFTS_PER_TICK:
                task["next_attempt_ts"] = now_ts() + 30
                update_task(task)
                logging.info("draft quota reached; defer task=%s", short(task["id"]))
                continue

            if st_u == ST_SUPPLY_CREATING and supplies_done >= SUPPLY_MAX_SUPPLY_CREATES_PER_TICK:
                task["next_attempt_ts"] = now_ts() + SUPPLY_CREATE_STAGE_DELAY_SECONDS
                update_task(task)
                logging.info("supply-create quota reached; defer task=%s", short(task["id"]))
                continue

            prev_status = st_u
            prev_draft_http_ts = int(task.get("last_draft_http_attempt_ts") or 0)
            prev_supply_http_ts = int(task.get("last_supply_http_attempt_ts") or 0)

            try:
                await asyncio.wait_for(
                    advance_task(task, api, notify_text, notify_file),
                    timeout=float(TASK_STEP_TIMEOUT_SECONDS)
                )
            except asyncio.TimeoutError:
                logging.warning("advance_task hard-timeout for task=%s after %ss", short(task["id"]), TASK_STEP_TIMEOUT_SECONDS)
                try:
                    tnow = get_task(task["id"]) or task
                    tnow["next_attempt_ts"] = now_ts() + 5
                    update_task(tnow)
                except Exception:
                    pass
            except Exception:
                logging.exception("advance_task crashed for task=%s", short(task["id"]))

            tnow = get_task(task["id"]) or task
            new_draft_http_ts = int(tnow.get("last_draft_http_attempt_ts") or 0)
            new_supply_http_ts = int(tnow.get("last_supply_http_attempt_ts") or 0)
            if prev_status == ST_DRAFT_CREATING and new_draft_http_ts > prev_draft_http_ts:
                drafts_done += 1
            if prev_status == ST_SUPPLY_CREATING and new_supply_http_ts > prev_supply_http_ts:
                supplies_done += 1

        next_delay: Optional[float] = None
        n = now_ts()
        for t in list_tasks():
            if str(t.get("status") or "") == UI_STATUS_CREATED:
                continue
            nt = int(t.get("next_attempt_ts") or 0)
            if nt and nt > n:
                d = nt - n + float(t.get("retry_after_jitter") or 0.0)
                if d > 0:
                    next_delay = d if next_delay is None else min(next_delay, d)

        if next_delay is None and list_tasks():
            next_delay = 15.0

        if next_delay is not None:
            logging.info("process_tasks: schedule next wakeup in %.2fs", next_delay)
            _schedule_self_wakeup(float(next_delay), notify_text, notify_file)

        try:
            if random.random() < 0.1:
                purge_tasks(SUPPLY_PURGE_AGE_DAYS)
        except Exception:
            logging.exception("purge_tasks error")

    logging.info("process_tasks: END (drafts=%d supplies=%d active=%d)", drafts_done, supplies_done, len(list_tasks()))

def register_supply_scheduler(
    scheduler: Any,
    notify_text: Callable[[int, str], Awaitable[None]],
    notify_file: Callable[[int, str, Optional[str]], Awaitable[None]],
    interval_seconds: int = 15,
):
    async def _tick():
        try:
            await process_tasks(notify_text, notify_file)
        except Exception:
            logging.exception("register_supply_scheduler: tick failed")

    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_tick())
    except RuntimeError:
        pass

    try:
        if scheduler and hasattr(scheduler, "add_job"):
            try:
                scheduler.add_job(
                    _tick,
                    "interval",
                    seconds=max(5, int(interval_seconds)),
                    id=_SCHEDULER_JOB_ID,
                    replace_existing=True,
                    coalesce=True,
                    max_instances=1,
                )
                scheduler.add_job(
                    _tick,
                    "date",
                    run_date=datetime.utcnow() + timedelta(seconds=1),
                    id=f"{_SCHEDULER_JOB_ID}_boot",
                    replace_existing=True,
                )
                logging.info("register_supply_scheduler: APScheduler jobs registered (interval=%ss)", interval_seconds)
                return
            except Exception:
                logging.exception("register_supply_scheduler: scheduler.add_job failed, fallback to self-wakeup")
    except Exception:
        logging.exception("register_supply_scheduler: scheduler probing failed, fallback")

    try:
        _schedule_self_wakeup(5.0, notify_text, notify_file)
        logging.info("register_supply_scheduler: internal self-wakeup armed")
    except Exception:
        logging.exception("register_supply_scheduler: self-wakeup scheduling failed")

async def close_http_client():
    global _HTTP_CLIENT
    if _HTTP_CLIENT is not None:
        try:
            await _HTTP_CLIENT.aclose()
        except Exception:
            pass
        _HTTP_CLIENT = None

__all__ = [
    "register_supply_scheduler",
    "process_tasks",
    "create_tasks_from_template",
    "cancel_task",
    "retry_task",
    "delete_task",
    "purge_all_supplies_now",
    "list_tasks",
    "list_all_tasks",
    "purge_tasks",
    "purge_all_tasks",
    "purge_stale_nonfinal",
]