# -*- coding: utf-8 -*-
"""
supply_watch.py
Оркестратор авто-заявок поставки для Ozon Seller API.

Основные этапы пайплайна:
- DRAFT_CREATING -> POLL_DRAFT -> TIMESLOT_SEARCH -> SUPPLY_CREATING -> POLL_SUPPLY ->
  SUPPLY_ORDER_FETCH/ORDER_DATA_FILLING -> CARGO_PREP -> CARGO_CREATING -> POLL_CARGO ->
  LABELS_CREATING -> POLL_LABELS -> DONE

Антизалипание на DRAFT_CREATING:
- Inline-ожидание по умолчанию отключено (чтобы не застревать в “inline‑wait … then attempt”).
- Если есть пейсинг (min-spacing) — планируем next_attempt_ts и выходим, помечая флагом __draft_pacing_scheduled_ts.
- Процесс-тактовая квота учитывает пейсинг как «занятую» попытку в текущем тике.
- Реальная попытка draft/create делается только когда пейсинг дал «зелёный свет».

Дополнительно:
- Межпроцессная синхронизация слота на /v1/draft/create (file-lock в DATA_DIR).
- «Тихий коридор» вокруг /v1/draft/create: подавляем прочие запросы к Ozon в ± небольшое окно.
- Увеличенный backoff при 429 с текстом "per second"; сохраняем x-o3-trace-id для обращения в поддержку.
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
from typing import Any, Dict, List, Optional, Tuple
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

AUTO_CREATE_CARGOES = _getenv_bool("AUTO_CREATE_CARGOES", True)
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
MIN_CREATE_RETRY_SECONDS = _getenv_int("MIN_CREATE_RETRY_SECONDS", 180)

SUPPLY_CREATE_STAGE_DELAY_SECONDS = _getenv_int("SUPPLY_CREATE_STAGE_DELAY_SECONDS", 15)
SUPPLY_CREATE_MIN_RETRY_SECONDS = _getenv_int("SUPPLY_CREATE_MIN_RETRY_SECONDS", 20)
SUPPLY_CREATE_MAX_RETRY_SECONDS = _getenv_int("SUPPLY_CREATE_MAX_RETRY_SECONDS", 90)

ORDER_FILL_POLL_INTERVAL_SECONDS = _getenv_int("ORDER_FILL_POLL_INTERVAL_SECONDS", 60)
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

# Инлайн ожидание ВЫКЛ по умолчанию (ставьте >0, если хотите вернуть старое поведение)
DRAFT_INLINE_WAIT_MAX_SEC = _getenv_int("DRAFT_INLINE_WAIT_MAX_SEC", 0)
DRAFT_INLINE_WAIT_MAX_LOOPS = _getenv_int("DRAFT_INLINE_WAIT_MAX_LOOPS", 0)

# Значение cargo_type по умолчанию
OZON_CARGO_TYPE_DEFAULT = _getenv_str("OZON_CARGO_TYPE_DEFAULT", "CARGO_TYPE_BOX")

# Внутренний гейт в supply_watch (можно отключить через ENV)
SUPPLY_DISABLE_INTERNAL_GATE = _getenv_bool("SUPPLY_DISABLE_INTERNAL_GATE", False)

# «Тихий коридор» вокруг draft/create
CREATE_QUIET_BEFORE_SEC = max(0.0, _getenv_float("CREATE_QUIET_BEFORE_SEC", 0.6))
CREATE_QUIET_AFTER_SEC = max(0.0, _getenv_float("CREATE_QUIET_AFTER_SEC", 1.2))

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

# «Тихий коридор» для всех запросов, кроме /v1/draft/create
_CREATE_QUIET_UNTIL: float = 0.0
_CREATE_QUIET_LOCK = asyncio.Lock()

def _is_create_endpoint(path: str) -> bool:
    p = str(path or "").lower()
    # только /v1/draft/create — самое «жёсткое» окно
    return p.startswith("/v1/draft/create")

async def _enter_quiet_before_create():
    global _CREATE_QUIET_UNTIL
    # Раздвигаем «тихий коридор» на период ДО и ПОСЛЕ вызова create
    async with _CREATE_QUIET_LOCK:
        now = time.time()
        _CREATE_QUIET_UNTIL = max(_CREATE_QUIET_UNTIL, now + CREATE_QUIET_BEFORE_SEC + CREATE_QUIET_AFTER_SEC)
    if CREATE_QUIET_BEFORE_SEC > 0:
        await asyncio.sleep(CREATE_QUIET_BEFORE_SEC)

async def _wait_if_quiet(method: str, path: str):
    # Пропускаем только сам create; остальным — подождать до конца «тихого окна»
    if _is_create_endpoint(path):
        return
    if CREATE_QUIET_BEFORE_SEC <= 0 and CREATE_QUIET_AFTER_SEC <= 0:
        return
    # Ждём, пока «тихий коридор» не истечёт
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
        # «Тихий коридор»: все, кроме create, ждут окончания окна
        try:
            await _wait_if_quiet(method, path)
        except Exception:
            # не прерываем из-за ошибок ожидания
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
        cur = str(target.get("status") or "").upper()

        if has_supply and cur not in (ST_CARGO_PREP, ST_CARGO_CREATING, ST_POLL_CARGO, ST_LABELS_CREATING, ST_POLL_LABELS, ST_DONE):
            target["status"] = ST_CARGO_PREP if AUTO_CREATE_CARGOES else ST_DONE
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
    for t in _tasks:
        st = str(t.get("status") or "").upper()
        if st in (ST_DONE, ST_FAILED, ST_CANCELED) and t.get("updated_ts", 0) < cutoff:
            continue
        remain.append(t)
    _tasks[:] = remain
    save_tasks()
    removed = before - len(remain)
    logging.info("purge_tasks: final-only mode days=%s removed=%s", days, removed)
    return removed

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

# Корректный паттерн заголовка "На DD.MM.YYYY, HH:MM-HH:MM"
HEADER_RE = re.compile(
    rf"\bНа\s+(\d{{2}})\.(\d{{2}})\.(\d{{4}}),\s*(\d{{2}}:\d{{2}})\s*{DASH_CLASS}\s*(\д{{2}}:\д{{2}})\b".replace("д", "d"),
    re.IGNORECASE
)

LINE_RE = re.compile(
    rf"""
    ^\s*
    (?P<sku>.+?)\s*{DASH_CLASS}\s*
    (?:количеств(?:о|а)|кол-?во|колво|qty|quantity)\s*
    (?P<qty>\d+)\s*[,;]?\s*
    (?P<boxes>\d+)\s*
    (?:короб(?:к(?:а|и|ок))?|кор\b|box(?:es)?)\s*[,;]?\s*
    (?:в\s*каждой\s*коробке\s*по|по)\s*
    (?P<per>\d+)\s*
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
    for idx, raw in enumerate(lines):
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
        m_qty = re.search(r"(?:количеств(?:о|а)|кол-?во|колво|qty|quantity)\s*(\d+)", rest, re.IGNORECASE)
        m_boxes = re.search(r"(\d+)\s*(?:короб(?:к(?:а|и|ок))?|кор\b|box(?:es)?)", rest, re.IGNORECASE)
        m_per = re.search(r"(?:в\s*каждой\s*коробке\s*по|по)\s*(\d+)", rest, re.IGNORECASE)
        wh = None
        if "," in rest or ";" in rest:
            tail = re.split(r"[;,]", rest)
            wh = tail[-1].strip().rstrip(".")
        else:
            m_wh = re.search(r"(?:шт(?:\.|ук)?|штук|штуки|штука)\s*(.*)$", rest, re.IGNORECASE)
            if m_wh:
                wh = m_wh.group(1).strip().rstrip(".")
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

def resolve_warehouse_id(name: str) -> Optional[int]:
    lname = (name or "").lower()
    if lname in WAREHOUSE_MAP:
        try:
            return int(WAREHOUSE_MAP[lname])
        except Exception:
            return None
    return None

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

# ================== API calls ==================

async def api_draft_create(api: OzonApi, task: Dict[str, Any]) -> Tuple[bool, Optional[str], Optional[str], int]:
    items = [{"sku": to_int_or_str(line["sku"]), "quantity": int(line["total_qty"])} for line in task["sku_list"]]
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

    # «Тихий коридор» перед create
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

        # При пер‑секундном лимите — длиннее пауза, чтобы выйти из окна
        if "per second" in msg or "per-second" in msg or "per second limit" in msg:
            wait_sec = max(base + 10, 30) + random.randint(0, 15)
        else:
            attempts = int(task.get("draft_rl_attempts") or 0) + 1
            task["draft_rl_attempts"] = attempts
            wait_sec = min(DRAFT_CREATE_MAX_BACKOFF, max(ON429_SHORT_RETRY_SEC, base) + attempts * 3)

        _set_retry_after(task, wait_sec, jitter_max=1.5)
        task["last_error"] = "429 Too Many Requests (draft/create)"
        task["create_attempts"] = int(task.get("create_attempts") or 0) + 1
        # Сохраним trace id для обращения в поддержку
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
        s = (data.get("status") or data.get("result") or {}).get("status") or ""
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
            out.append(slot)
    drop = js.get("drop_off_warehouse_timeslots") or js.get("result", {}).get("drop_off_warehouse_timeslots") or []
    if isinstance(drop, list):
        for entry in drop:
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
                    slot.setdefault("capacity_status", "")
                    if fr_loc:
                        slot["from_in_timezone"] = fr_loc
                    if to_loc:
                        slot["to_in_timezone"] = to_loc
                    out.append(slot)
    return out

async def api_timeslot_info(api: OzonApi, draft_id: str, warehouse_ids: List[int], date_iso: str, bundle_id: Optional[str] = None) -> Tuple[bool, List[Dict[str, Any]], str, int]:
    def _make_payloads(draft_id: str, wids: List[int], from_iso: str, to_iso: str, bundle_id: Optional[str]) -> List[Tuple[str, Dict[str, Any]]]:
        endpoints = ["/v1/draft/timeslot/info", "/v2/draft/timeslot/info"]
        base = {"draft_id": draft_id, "warehouse_ids": [int(w) for w in wids]}
        pl_variants: List[Dict[str, Any]] = [
            dict(base, date_from=from_iso, date_to=to_iso),
            dict(base, from_in_timezone=from_iso, to_in_timezone=to_iso),
        ]
        if bundle_id:
            pl_variants += [
                dict(base, date_from=from_iso, date_to=to_iso, bundle_id=bundle_id),
                dict(base, from_in_timezone=from_iso, to_in_timezone=to_iso, bundle_id=bundle_id),
            ]
        payloads: List[Tuple[str, Dict[str, Any]]] = []
        for ep in endpoints:
            for pl in pl_variants:
                payloads.append((ep, pl))
        return payloads

    day_start_local, day_end_local = day_range_local(date_iso)
    extra_days = max(0, int(SUPPLY_TIMESLOT_SEARCH_EXTRA_DAYS))
    ext_to_local = day_range_local(add_days_iso(date_iso, extra_days))[1] if extra_days > 0 else day_end_local

    last_err = ""
    last_status = 0

    for ep, pl in _make_payloads(draft_id, warehouse_ids, day_start_local, day_end_local, bundle_id):
        ok, data, err, status, headers = await api.post(ep, pl)
        logging.info("timeslot_info: ep=%s status=%s keys=%s", ep, status, list(pl.keys()))
        if status == 429:
            ra = _parse_retry_after(headers)
            return False, [], f"rate_limit:{min(RATE_LIMIT_MAX_ON429, max(ON429_SHORT_RETRY_SEC, ra))}", status
        if ok:
            slots = _normalize_timeslots_response(data)
            if slots:
                return True, slots, "", status
        else:
            last_err = f"timeslot_info_error:{err}|{data}"
            last_status = status

    if extra_days > 0:
        for ep, pl in _make_payloads(draft_id, warehouse_ids, day_start_local, ext_to_local, bundle_id):
            ok, data, err, status, headers = await api.post(ep, pl)
            logging.info("timeslot_info(ext): ep=%s status=%s keys=%s", ep, status, list(pl.keys()))
            if status == 429:
                ra = _parse_retry_after(headers)
                return False, [], f"rate_limit:{min(RATE_LIMIT_MAX_ON429, max(ON429_SHORT_RETRY_SEC, ra))}", status
            if ok:
                slots = _normalize_timeslots_response(data)
                if slots:
                    return True, slots, "", status
            else:
                last_err = f"timeslot_info_error:{err}|{data}"
                last_status = status

    return False, [], (last_err or "timeslot_info_empty"), (last_status or 200)

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

    raw_status = str(data.get("status") or (data.get("result") or {}).get("status") or "").strip()
    s_norm = raw_status.lower()

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
    if "success" in s_norm or (order_id and not s_norm):
        if not order_id:
            return False, None, f"supply_status_no_order_id:{data}", status
        return True, order_id, None, status
    if "progress" in s_norm or s_norm in ("in_progress", "pending", "processing"):
        return False, None, None, status

    errors = data.get("error_messages") or (data.get("result") or {}).get("error_messages") or []
    if errors:
        return False, None, f"supply_status_error_messages:{errors}", status
    if order_id:
        return True, order_id, None, status
    return False, None, f"supply_status_unrecognized:{raw_status}", status

def _extract_supply_order_info_from_response(js: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    def _first_str(o: Dict[str, Any], keys: List[str]) -> Optional[str]:
        for k in keys:
            v = o.get(k)
            if isinstance(v, str) and v:
                return v
            if isinstance(v, int):
                s = str(v)
                if s:
                    return s
        return None

    def _first_supply_like_id(o: Dict[str, Any]) -> Optional[str]:
        for k in ("supply_id", "supplyId"):
            v = o.get(k)
            if v not in (None, "", []):
                return str(v)
        for k in ("supply_order_id", "supplyOrderId"):
            v = o.get(k)
            if v not in (None, "", []):
                return str(v)
        for k in ("supply", "supply_order", "supplyOrder"):
            v = o.get(k)
            if isinstance(v, dict):
                for kk in ("id", "supply_id", "supplyId", "supply_order_id", "supplyOrderId"):
                    vv = v.get(kk)
                    if vv not in (None, "", []):
                        return str(vv)
        return None

    for scope in [js, js.get("result") or {}]:
        if isinstance(scope, dict):
            sid = _first_supply_like_id(scope)
            num = _first_str(scope, ["supply_order_number", "supplyOrderNumber", "order_number", "orderNumber", "number"])
            if sid or num:
                return sid, num

    list_keys = ["orders", "supply_orders", "supplyOrders", "items", "data", "order_list", "supply_orders_list"]
    for scope in [js, js.get("result") or {}]:
        if not isinstance(scope, dict):
            continue
        for lk in list_keys:
            arr = scope.get(lk)
            if isinstance(arr, list) and arr:
                first = arr[0]
                if isinstance(first, dict):
                    sid = _first_supply_like_id(first)
                    num = _first_str(first, ["supply_order_number", "supplyOrderNumber", "order_number", "orderNumber", "number"])
                    if sid or num:
                        return sid, num
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
    try:
        oid = int(str(order_id).strip())
    except Exception:
        oid = str(order_id).strip()
    payloads_v2 = [{"order_ids": [oid]}, {"supply_order_ids": [oid]}, {"order_id": oid}, {"supply_order_id": oid}]
    payloads_v1 = [{"order_id": oid}, {"supply_order_id": oid}]
    last_err = None
    last_status = 0

    for pl in payloads_v2:
        ok, data, err, status, headers = await api.post("/v2/supply-order/get", pl)
        if status == 429:
            ra = _parse_retry_after(headers)
            return False, None, None, f"rate_limit:{min(RATE_LIMIT_MAX_ON429, max(ON429_SHORT_RETRY_SEC, ra))}", status, {}
        if ok:
            sid, num = _extract_supply_order_info_from_response(data)
            meta = _extract_order_meta(data)
            if sid or num:
                return True, sid, (num or ""), None, status, meta

    for pl in payloads_v1:
        ok, data, err, status, headers = await api.post("/v1/supply-order/get", pl)
        if status == 429:
            ra = _parse_retry_after(headers)
            return False, None, None, f"rate_limit:{min(RATE_LIMIT_MAX_ON429, max(ON429_SHORT_RETRY_SEC, ra))}", status, {}
        if ok:
            sid, num = _extract_supply_order_info_from_response(data)
            meta = _extract_order_meta(data)
            return True, sid, (num or ""), None, status, meta
        last_err, last_status = f"supply_order_get_error:{err}|{data}", status

    return False, None, None, (last_err or "supply_order_get_error:unknown"), (last_status or 400), {}

async def api_supply_order_timeslot_set(
    api: OzonApi,
    order_id: Any,
    from_iso: str,
    to_iso: str,
    slot_id: Optional[Any] = None,
    dropoff_warehouse_id: Optional[int] = None,
    supply_id: Optional[Any] = None,
    warehouse_id: Optional[int] = None,
) -> Tuple[bool, Optional[str], int]:
    def _num_or_str(x: Any) -> Any:
        try:
            return int(str(x).strip())
        except Exception:
            return str(x).strip()

    oid = _num_or_str(order_id)
    wid = int(warehouse_id) if warehouse_id not in (None, "", 0, "0") else None
    drop = int(dropoff_warehouse_id) if dropoff_warehouse_id not in (None, "", 0, "0") else None

    id_variants: List[Dict[str, Any]] = [{"order_id": oid}, {"supply_order_id": oid}, {"id": oid}]
    z_from, z_to = _to_z_iso(from_iso), _to_z_iso(to_iso)

    base_ts_variants = [
        {"from_in_timezone": from_iso, "to_in_timezone": to_iso},
        {"from": z_from or from_iso, "to": z_to or to_iso},
    ]
    if slot_id is not None:
        base_ts_variants.append({"id": slot_id})

    def _with_optional(p: Dict[str, Any]) -> List[Dict[str, Any]]:
        variants = [dict(p)]
        if drop:
            variants.append(dict(p, drop_off_point_warehouse_id=drop))
        if wid:
            variants.append(dict(p, warehouse_id=wid))
        if drop and wid:
            variants.append(dict(p, drop_off_point_warehouse_id=drop, warehouse_id=wid))
        return variants

    attempts: List[Tuple[str, str, Dict[str, Any], str]] = []

    for idv in id_variants:
        for ts in base_ts_variants:
            upd = {"update": {"timeslot": dict(ts)}}
            for pl in _with_optional(dict(idv, **upd)):
                attempts.append(("/v2/supply-order/update", "update.timeslot", pl, "POST"))
                attempts.append(("/v1/supply-order/update", "update.timeslot", pl, "POST"))
            flat = {"timeslot": dict(ts)}
            for pl in _with_optional(dict(idv, **flat)):
                attempts.append(("/v2/supply-order/update", "flat.timeslot", pl, "POST"))
                attempts.append(("/v1/supply-order/update", "flat.timeslot", pl, "POST"))

    for idv in id_variants:
        for ts in base_ts_variants:
            flat = {"timeslot": dict(ts)}
            for pl in _with_optional(dict(idv, **flat)):
                for ep in ("/v2/supply-order/set-timeslot", "/v1/supply-order/set-timeslot"):
                    for m in ("POST", "PUT", "PATCH"):
                        attempts.append((ep, "set-timeslot.flat", pl, m))
            if "id" in ts:
                for pl in _with_optional(dict(idv, timeslot_id=ts["id"])):
                    for ep in ("/v2/supply-order/set-timeslot", "/v1/supply-order/set-timeslot"):
                        for m in ("POST", "PUT", "PATCH"):
                            attempts.append((ep, "set-timeslot.id", pl, m))

    for idv in id_variants:
        for ts in base_ts_variants:
            flat = {"timeslot": dict(ts)}
            for pl in _with_optional(dict(idv, **flat)):
                for ep in ("/v2/supply-order/timeslot/set", "/v1/supply-order/timeslot/set"):
                    for m in ("POST", "PUT", "PATCH"):
                        attempts.append((ep, "timeslot.set.flat", pl, m))

    errors: List[str] = []
    saw_404 = False
    saw_429 = False
    saw_ok = False
    saw_other = False

    for ep, tag, pl, method in attempts:
        logging.info("timeslot_set: %s %s payload_keys=%s", method, ep, list(pl.keys()))
        ok, data, err, status, headers = await api.request(method, ep, pl)

        body_snip = ""
        try:
            body_snip = (data.get("text") if isinstance(data, dict) else str(data))[:160]
        except Exception:
            pass

        if status == 404:
            saw_404 = True
            errors.append(f"{method} {ep} 404:{body_snip}")
            continue
        if status == 429:
            saw_429 = True
            ra = _parse_retry_after(headers)
            errors.append(f"{method} {ep} 429:rate_limit:{ra}")
            continue

        if ok:
            saw_ok = True
            return True, None, status

        saw_other = True
        errors.append(f"{method} {ep} {status}:{body_snip}")

    if (saw_404 or saw_429) and not saw_ok and not saw_other:
        logging.warning("timeslot_set: not_supported_404/429 only — пропускаем установку тайм-слота.")
        return False, "not_supported_404", 404

    msg = "timeslot_set_failed:all_endpoints_rejected_or_missing"
    logging.error("timeslot_set: %s; attempts:\n%s", msg, "\n\n".join(errors))
    return False, msg, 400

async def api_cargoes_create(api: OzonApi, payload: Dict[str, Any]) -> Tuple[bool, Optional[str], Optional[str], int]:
    ok, data, err, status, headers = await api.post("/v1/cargoes/create", payload)
    if status == 429:
        ra = _parse_retry_after(headers)
        return False, None, f"rate_limit:{min(RATE_LIMIT_MAX_ON429, max(ON429_SHORT_RETRY_SEC, ra))}", status
    if not ok:
        return False, None, f"cargoes_create_error:{err}|{data}", status
    op_id = data.get("operation_id") or data.get("result", {}).get("operation_id")
    if not op_id:
        return False, None, f"cargoes_create_no_operation_id:{data}", status
    return True, op_id, None, status

async def api_cargoes_create_info(api: OzonApi, operation_id: str) -> Tuple[bool, List[Dict[str, Any]], Optional[str], int]:
    payload = {"operation_id": operation_id}
    ok, data, err, status, headers = await api.post("/v1/cargoes/create/info", payload)
    if status == 429:
        ra = _parse_retry_after(headers)
        return False, [], f"rate_limit:{min(RATE_LIMIT_MAX_ON429, max(ON429_SHORT_RETRY_SEC, ra))}", status
    if not ok:
        return False, [], f"cargoes_info_error:{err}|{data}", status
    s = (data.get("status") or data.get("result") or {}).get("status", "").upper()
    if s in ("IN_PROGRESS", "PENDING", ""):
        return False, [], None, status
    if s not in ("SUCCESS", "OK", "DONE"):
        return False, [], f"cargoes_status_fail:{s}|{data}", status
    cargoes = data.get("cargoes") or data.get("result", {}).get("cargoes") or []
    if not cargoes:
        return False, [], f"cargoes_info_no_cargoes:{data}", status
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
    s = (data.get("status") or data.get("result") or {}).get("status", "").upper()
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

# ================== Timeslot helpers inside order ==================

async def _auto_fill_timeslot_if_needed(task: Dict[str, Any], api: OzonApi, meta: Dict[str, Any], notify_text) -> bool:
    ts_meta = meta.get("timeslot") or {}
    required = bool(ts_meta.get("required"))
    can_set = bool(ts_meta.get("can_set"))
    has_from = ts_meta.get("from")
    has_to = ts_meta.get("to")

    if has_from and has_to:
        return True
    if not required or not can_set:
        return True

    fr = task.get("from_in_timezone") or task.get("desired_from_iso") or task.get("slot_from")
    to = task.get("to_in_timezone") or task.get("desired_to_iso") or task.get("slot_to")
    if not (fr and to):
        return False

    if task.get("order_timeslot_set_ok"):
        return True
    if now_ts() - int(task.get("order_timeslot_set_ts") or 0) < 10:
        return True

    ok, err, status = await api_supply_order_timeslot_set(
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
        task.pop("last_error", None)
        update_task(task)
        try:
            await notify_text(task["chat_id"], f"🟦 [{short(task['id'])}] Тайм‑слот проставлен в заявке.")
        except Exception:
            logging.exception("notify_text failed on timeslot set OK")
        return True

    if err == "not_supported_404" or status == 404:
        task["order_timeslot_set_ok"] = True
        task["order_timeslot_set_note"] = "timeslot API not available; relying on supply create"
        if task.get("order_id") and not task.get("supply_id"):
            task["supply_id"] = str(task["order_id"])
        task["status"] = ST_CARGO_PREП if AUTO_CREATE_CARGOES else ST_DONE
        task["retry_after_ts"] = 0
        task["retry_after_jitter"] = 0.0
        task["next_attempt_ts"] = now_ts()
        task.pop("last_error", None)
        update_task(task)
        logging.info("timeslot_set: not_supported_404 -> promoted to %s (supply_id=%s)", task["status"], task.get("supply_id"))
        try:
            await notify_text(task["chat_id"], f"🟦 [{short(task['id'])}] Тайм‑слот пропущен (404). Переход к подготовке грузов.")
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
            await notify_text(task["chat_id"], f"🟨 [{short(task['id'])}] Не удалось проставить тайм‑слот (повторим).")
        except Exception:
            logging.exception("notify_text failed on timeslot warn")
    return False

async def _prompt_missing_fields(task: Dict[str, Any], meta: Dict[str, Any], notify_text) -> bool:
    asked = False
    veh = meta.get("vehicle") or {}
    cnt = meta.get("contact") or {}

    nowv = now_ts()
    is_crossdock = (str(task.get("supply_type") or "").upper() == "CREATE_TYPE_CROSSDOCK") or bool(
        task.get("dropoff_warehouse_id") or task.get("drop_off_point_warehouse_id") or task.get("drop_off_id") or DROP_OFF_WAREHOUSE_ID
    )

    if veh.get("required") and not is_crossdock and not task.get("order_vehicle_text"):
        if nowv - int(task.get("need_vehicle_prompt_ts") or 0) >= PROMPT_MIN_INTERVAL:
            task["need_vehicle"] = True
            task["need_vehicle_prompt_ts"] = nowv
            update_task(task)
            try:
                await notify_text(
                    task["chat_id"],
                    "🚚 Для заявки требуется транспорт. Ответьте сообщением:\n"
                    f"ТРАНСПОРТ {short(task['id'])} <госномер и описание>\n"
                    f"пример: ТРАНСПОРТ {short(task['id'])} А123ВС 116 RUS Газель тент"
                )
            except Exception:
                logging.exception("notify_text failed on vehicle prompt")
            asked = True
    else:
        if task.get("need_vehicle"):
            task["need_vehicle"] = False
            update_task(task)

    if cnt.get("required") and not (task.get("order_contact_phone") or task.get("order_contact_name")):
        if nowv - int(task.get("need_contact_prompt_ts") or 0) >= PROMPT_MIN_INTERVAL:
            task["need_contact"] = True
            task["need_contact_prompt_ts"] = nowv
            update_task(task)
            try:
                await notify_text(
                    task["chat_id"],
                    "📱 Для заявки требуется контакт. Ответьте сообщением:\n"
                    f"КОНТАКТ {short(task['id'])} +79990000000 [Имя]\n"
                    f"пример: КОНТАКТ {short(task['id'])} +79991234567 Иван"
                )
            except Exception:
                logging.exception("notify_text failed on contact prompt")
            asked = True
    else:
        if task.get("need_contact"):
            task["need_contact"] = False
            update_task(task)

    return asked

# ================== Cargo payload builders and variants ==================

def _cargo_type_candidates() -> List[str]:
    return ["BOX", "CARGO_TYPE_BOX"]

def build_cargoes_payload_variants(task: Dict[str, Any]) -> List[Dict[str, Any]]:
    supply_id = task.get("supply_id")
    if not supply_id:
        raise RuntimeError("supply_id is not ready yet")

    base_items: List[Dict[str, Any]] = []
    for line in task["sku_list"]:
        sku = to_int_or_str(line["sku"])
        boxes = int(line["boxes"])
        per_box = int(line["per_box"])
        for _ in range(boxes):
            base_items.append({"sku": sku, "quantity": per_box})

    def make_payload(per_keys: List[str], top_keys: List[str], value: str) -> Dict[str, Any]:
        cargos: List[Dict[str, Any]] = []
        for it in base_items:
            c: Dict[str, Any] = {"key": str(uuid.uuid4()), "items": [dict(it)]}
            for k in per_keys:
                c[k] = value
            cargos.append(c)
        payload: Dict[str, Any] = {"supply_id": supply_id, "delete_current_version": True, "cargoes": cargos}
        for k in top_keys:
            payload[k] = value
        return payload

    variants: List[Dict[str, Any]] = []
    per_key_sets: List[List[str]] = [
        ["CargoType"], ["cargoType"], ["cargo_type"], ["type"],
        ["CargoType", "type"], ["cargoType", "type"],
    ]

    for val in _cargo_type_candidates():
        for per in per_key_sets[:4]:
            variants.append(make_payload(per, [], val))
        variants.append(make_payload(["CargoType"], ["CargoType"], val))
        variants.append(make_payload(["cargoType"], ["cargoType"], val))
        variants.append(make_payload(["cargo_type"], ["cargo_type"], val))
        variants.append(make_payload(["type"], ["type"], val))
        variants.append(make_payload(["CargoType"], ["type"], val))
        variants.append(make_payload(["cargoType"], ["type"], val))

    dedup: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for pl in variants:
        key = json.dumps(pl, sort_keys=True, ensure_ascii=False)
        if key in seen:
            continue
        seen.add(key)
        dedup.append(pl)
    return dedup

def build_cargoes_from_task(task: Dict[str, Any]) -> Dict[str, Any]:
    supply_id = task.get("supply_id")
    if not supply_id:
        raise RuntimeError("supply_id is not ready yet")
    cargoes: List[Dict[str, Any]] = []
    for line in task["sku_list"]:
        sku = to_int_or_str(line["sku"])
        boxes = int(line["boxes"])
        per_box = int(line["per_box"])
        for _ in range(boxes):
            cargoes.append({
                "key": str(uuid.uuid4()),
                "CargoType": "BOX",
                "cargoType": "BOX",
                "cargo_type": OZON_CARGO_TYPE_DEFAULT,
                "type": "BOX",
                "items": [{"sku": sku, "quantity": per_box}]
            })
    return {"supply_id": supply_id, "delete_current_version": True, "cargoes": cargoes}

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

# ================== Self-wakeup (robust) ==================

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
        logging.debug("self-wakeup: earlier wakeup already scheduled (%.2fs), skip new", _pending_wakeup_ts - time.time())
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

    if changed:
        save_tasks()
        logging.info("sanitize_tasks_legacy_cyrillic: normalized statuses and reset timings")

# ================== Create/Cancel/Retry ==================

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
            "create_attempts": 0,
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
            "last_draft_http_attempt_ts", "__draft_pacing_scheduled_ts", "last_trace_id"
        ):
            t.pop(k, None)
    t["status"] = ST_WAIT_WINDOW
    t["next_attempt_ts"] = 0
    t["retry_after_ts"] = 0
    t["creating"] = False
    t["create_attempts"] = 0
    update_task(t)
    return True, "OK"

# ================== Глобальный пейсинг и cooldown для draft/create ==================

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
    # Межпроцессная и межпоточная синхронизация доступа к окну create
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

# ================== Main stepper ==================

async def advance_task(task: Dict[str, Any], api: OzonApi, notify_text, notify_file) -> None:
    if task.get("status") in (ST_DONE, ST_FAILED, ST_CANCELED):
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

    if task.get("supply_id") and task.get("status") not in (ST_CARGO_PREP, ST_CARGO_CREATING, ST_POLL_CARGO, ST_LABELS_CREATING, ST_POLL_LABELS, ST_DONE):
        task["status"] = ST_CARGO_PREP if AUTO_CREATE_CARGOES else ST_DONE
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
            task.setdefault("create_attempts", 0)
            task.pop("create_backoff_sec", None)
            update_task(task)
            schedule(1)
            return

        if st == ST_DRAFT_CREATING:
            if not SUPPLY_DISABLE_INTERNAL_GATE:
                cd = _draft_global_wait()
                if cd > 0:
                    logging.info("draft cooldown active: %ss left", cd)
                    schedule(cd, mark="__draft_pacing_scheduled_ts")
                    _schedule_self_wakeup(cd + 0.2, notify_text, notify_file)
                    return

                wait = _reserve_draft_slot_or_wait()
                if wait > 0:
                    logging.info(
                        "draft pacing: wait %ss (next_allowed=%s, now=%s) before /v1/draft/create for task=%s",
                        wait, _read_next_allowed_ts(), now_ts(), short(task["id"])
                    )
                    schedule(wait, mark="__draft_pacing_scheduled_ts")
                    _schedule_self_wakeup(wait + 0.2, notify_text, notify_file)
                    return

            if task.get("creating"):
                return

            logging.info("draft/create: attempting now for task=%s", short(task["id"]))
            task["creating"] = True
            task["draft_last_try_ts"] = now_ts()
            task["last_draft_http_attempt_ts"] = now_ts()
            update_task(task)

            ok, op_id, err, status = await api_draft_create(api, task)

            task["creating"] = False

            if status == 429 or (err and str(err).startswith("rate_limit:")):
                try:
                    delay = int(str(err).split(":", 1)[1]) + 1
                except Exception:
                    delay = max(2, int(task.get("retry_after_ts", 0) - now_ts()) + 1)
                _schedule_self_wakeup(max(1, delay), notify_text, notify_file)
                return

            if not ok:
                if str(err).startswith("http_status:5") or str(err).startswith("http_error:") or status >= 500 or status == 0 or (err == "http_timeout"):
                    delay = max(_inc_backoff(task), SUPPLY_CREATE_MIN_RETRY_SECONDS)
                    task["last_error"] = f"server_or_timeout_error:{err}"
                    task["create_attempts"] = int(task.get("create_attempts") or 0) + 1
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
                _schedule_self_wakeup(wait_sec + 1.0, notify_text, notify_file)
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
            chosen_wid = task.get("chosen_warehouse_id") or choose_warehouse(warehouses)
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

            logging.info("Draft ready task=%s draft_id=%s chosen_warehouse_id=%s bundle_id=%s",
                         short(task["id"]), draft_id, task.get("chosen_warehouse_id"), task.get("bundle_id"))

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

            ok, slots, err, status = await api_timeslot_info(api, task["draft_id"], [wid], task["date"], task.get("bundle_id"))
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
                _schedule_self_wakeup(wait_sec + 1.0, notify_text, notify_file)
                return
            if task.get("timeslot_rl_attempts"):
                task.pop("timeslot_rl_attempts", None)

            if not ok or not slots:
                logging.info("No timeslots yet for task=%s date=%s wid=%s", short(task["id"]), task["date"], wid)
                schedule(SLOT_POLL_INTERVAL_SECONDS)
                return

            desired_from = task["desired_from_iso"]
            desired_to = task["desired_to_iso"]
            matched = None
            for s in slots:
                if (s.get("from_in_timezone") == desired_from and s.get("to_in_timezone") == desired_to):
                    matched = s
                    break
            if not matched and slots:
                for s in slots:
                    cs = (s.get("capacity_status") or "").upper()
                    if cs in ("AVAILABLE", "FREE", "OK", ""):
                        matched = s
                        break
            if not matched:
                schedule(SLOT_POLL_INTERVAL_SECONDS)
                return

            task["slot_from"] = matched.get("from_in_timezone")
            task["slot_to"] = matched.get("to_in_timezone")
            task["slot_id"] = matched.get("id") or matched.get("timeslot_id") or matched.get("slot_id")

            task["desired_from_iso"] = task["slot_from"]
            task["desired_to_iso"] = task["slot_to"]
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
                _set_retry_after(task, min(RATE_LIMIT_MAX_ON429, max(1, ra)), jitter_max=1.0)
                task["last_error"] = "429 Too Many Requests (supply/create)"
                update_task(task)
                _schedule_self_wakeup(min(RATE_LIMIT_MAX_ON429, max(1, ra)) + 1.0, notify_text, notify_file)
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
                _schedule_self_wakeup(wait_sec + 1.0, notify_text, notify_file)
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
                return

            task["order_id"] = order_id
            task["status"] = ST_SUPPLY_ORDER_FETCH
            task["op_started_ts"] = now_ts()
            task["op_retries"] = 0
            update_task(task)
            schedule(1)
            return

        if st in (ST_SUPPLY_ORDER_FETCH, ST_ORDER_DATA_FILLING):
            ok, supply_id, number, err, status, meta = await api_supply_order_get(api, task["order_id"])
            if status == 429 or (err and str(err or "").startswith("rate_limit:")):
                attempts = int(task.get("order_get_rl_attempts") or 0) + 1
                task["order_get_rl_attempts"] = attempts
                try:
                    ra = int(str(err).split(":", 1)[1]) if err else ON429_SHORT_RETRY_SEC
                except Exception:
                    ra = ON429_SHORT_RETRY_SEC
                base = min(RATE_LIMIT_MAX_ON429, max(ON429_SHORT_RETRY_SEC, ra))
                wait_sec = min(45, base + attempts * 2)
                _set_retry_after(task, wait_sec, jitter_max=1.5)
                task["last_error"] = "429 Too Many Requests (supply-order/get)"
                update_task(task)
                _schedule_self_wakeup(wait_sec + 1.0, notify_text, notify_file)
                return
            if task.get("order_get_rl_attempts"):
                task.pop("order_get_rl_attempts", None)

            if not ok:
                task["op_retries"] = int(task.get("op_retries") or 0) + 1
                if task["оп_retries"] > max(SUPPLY_MAX_OPERATION_RETRIES, ORDER_FILL_MAX_RETRIES):
                    task["status"] = ST_FAILED
                    task["last_error"] = str(err)
                    update_task(task)
                    try:
                        await notify_text(task["chat_id"], f"🟥 [{short(task['id'])}] supply-order/get: {err}")
                    except Exception:
                        logging.exception("notify_text failed on supply-order/get")
                    return
                schedule(ORDER_FILL_POLL_INTERVAL_SECONDS)
                return

            task["supply_order_number"] = number or task.get("supply_order_number") or ""
            if meta.get("dropoff_warehouse_id"):
                task["dropoff_warehouse_id"] = int(meta["dropoff_warehouse_id"])
                update_task(task)

            if supply_id and not task.get("supply_id"):
                task["supply_id"] = str(supply_id)
                update_task(task)

            if not task.get("supply_id") and task.get("order_id"):
                task["supply_id"] = str(task["order_id"])
                update_task(task)

            await _auto_fill_timeslot_if_needed(task, api, meta, notify_text)
            await _prompt_missing_fields(task, meta, notify_text)

            task["status"] = ST_ORDER_DATA_FILLING
            update_task(task)
            schedule(ORDER_FILL_POLL_INTERVAL_SECONDS)
            return

        if st == ST_CARGO_PREP:
            logging.info("enter CARGO_PREP task=%s supply_id=%s next=%s retry_after=%s",
                         short(task["id"]), task.get("supply_id"),
                         task.get("next_attempt_ts"), task.get("retry_after_ts"))
            variants = build_cargoes_payload_variants(task)
            task["cargo_payload_variants"] = variants
            task["cargo_variant_idx"] = 0
            task["cargo_payload"] = variants[0]
            update_task(task)
            task["status"] = ST_CARGO_CREATING
            task["next_attempt_ts"] = now_ts()
            task["retry_after_ts"] = 0
            task["retry_after_jitter"] = 0.0
            update_task(task)
            return

        if st == ST_CARGO_CREATING:
            if task.get("creating"):
                return
            if not task.get("cargo_payload"):
                variants = task.get("cargo_payload_variants") or build_cargoes_payload_variants(task)
                task["cargo_payload_variants"] = variants
                task["cargo_variant_idx"] = int(task.get("cargo_variant_idx") or 0)
                if task["cargo_variant_idx"] >= len(variants):
                    task["cargo_variant_idx"] = 0
                task["cargo_payload"] = variants[task["cargo_variant_idx"]]
                update_task(task)

            task["creating"] = True
            update_task(task)

            ok, op_id, err, status = await api_cargoes_create(api, task["cargo_payload"])
            task["creating"] = False

            if status == 429 or (err and str(err or "").startswith("rate_limit:")):
                try:
                    ra = int(str(err).split(":", 1)[1]) if err else ON429_SHORT_RETRY_SEC
                except Exception:
                    ra = ON429_SHORT_RETRY_SEC
                _set_retry_after(task, min(RATE_LIMIT_MAX_ON429, max(1, ra)), jitter_max=1.5)
                task["last_error"] = "429 Too Many Requests (cargoes/create)"
                update_task(task)
                _schedule_self_wakeup(min(RATE_LIMIT_MAX_ON429, max(1, ra)) + 1.0, notify_text, notify_file)
                return

            if not ok:
                err_text = str(err or "")
                if "CargoType must be set" in err_text:
                    variants = task.get("cargo_payload_variants") or []
                    cur = int(task.get("cargo_variant_idx") or 0)
                    nxt = cur + 1
                    if variants and nxt < len(variants):
                        task["cargo_variant_idx"] = nxt
                        task["cargo_payload"] = variants[nxt]
                        task["last_error"] = "switch_cargo_type_variant"
                        task["next_attempt_ts"] = now_ts()
                        task["retry_after_ts"] = 0
                        update_task(task)
                        logging.warning("cargoes/create: switching variant %s -> %s due to 'CargoType must be set'", cur, nxt)
                        return
                    else:
                        msg = "cargoes_create_error: CargoType must be set (all variants tried). "\
                              "Попробуйте OZON_CARGO_TYPE_DEFAULT=BOX или CARGO_TYPE_BOX."
                        task["status"] = ST_FAILED
                        task["last_error"] = msg
                        update_task(task)
                        try:
                            await notify_text(task["chat_id"], f"🟥 [{short(task['id'])}] {msg}")
                        except Exception:
                            logging.exception("notify_text failed on cargoes/create variants exhausted")
                        return

                if str(err).startswith("http_status:5") or str(err).startswith("http_error:") or status >= 500 or status == 0 or (err == "http_timeout"):
                    delay = max(_inc_backoff(task), SUPPLY_CREATE_MIN_RETRY_SECONDS)
                    task["last_error"] = f"cargoes_create_server_or_timeout:{err}"
                    update_task(task)
                    schedule(delay)
                    return
                task["status"] = ST_FAILED
                task["last_error"] = str(err)
                update_task(task)
                try:
                    await notify_text(task["chat_id"], f"🟥 [{short(task['id'])}] cargoes/create: {err}")
                except Exception:
                    logging.exception("notify_text failed on cargoes/create")
                return

            task["cargo_operation_id"] = op_id
            task["op_started_ts"] = now_ts()
            task["оп_retries"] = 0
            task["status"] = ST_POLL_CARGO
            update_task(task)
            schedule(OPERATION_POLL_INTERVAL_SECONDS)
            return

        if st == ST_POLL_CARGO:
            ok, cargoes, err, status = await api_cargoes_create_info(api, task["cargo_operation_id"])
            if status == 429 or (err and str(err or "").startswith("rate_limit:")):
                try:
                    ra = int(str(err).split(":", 1)[1]) if err else ON429_SHORT_RETRY_SEC
                except Exception:
                    ra = ON429_SHORT_RETRY_SEC
                _set_retry_after(task, min(RATE_LIMIT_MAX_ON429, max(1, ra)), jitter_max=1.5)
                task["last_error"] = "429 Too Many Requests (cargoes/info)"
                update_task(task)
                _schedule_self_wakeup(min(RATE_LIMIT_MAX_ON429, max(1, ra)) + 1.0, notify_text, notify_file)
                return

            if err:
                task["op_retries"] = int(task.get("op_retries") or 0) + 1
                if task["op_retries"] > SUPPLY_MAX_OPERATION_RETRIES:
                    task["status"] = ST_FAILED
                    task["last_error"] = str(err)
                    update_task(task)
                    try:
                        await notify_text(task["chat_id"], f"🟥 [{short(task['id'])}] cargoes/info: {err}")
                    except Exception:
                        logging.exception("notify_text failed on cargoes/info")
                    return
                schedule(OPERATION_POLL_INTERVAL_SECONDS)
                return

            if not ok:
                schedule(OPERATION_POLL_INTERVAL_SECONDS)
            else:
                task["cargo_ids"] = [c.get("cargo_id") for c in cargoes if c.get("cargo_id")]
                task["status"] = ST_LABELS_CREATING if AUTO_CREATE_LABELS else ST_DONE
                update_task(task)
                schedule(1)
            return

        if st == ST_LABELS_CREATING:
            if task.get("creating"):
                return
            task["creating"] = True
            update_task(task)

            ok, op_id, err, status = await api_labels_create(api, task["supply_id"], task.get("cargo_ids"))
            task["creating"] = False

            if status == 429 or (err and str(err or "").startswith("rate_limit:")):
                try:
                    ra = int(str(err).split(":", 1)[1]) if err else ON429_SHORT_RETRY_SEC
                except Exception:
                    ra = ON429_SHORT_RETRY_SEC
                _set_retry_after(task, min(RATE_LIMIT_MAX_ON429, max(1, ra)), jitter_max=1.5)
                task["last_error"] = "429 Too Many Requests (labels/create)"
                update_task(task)
                _schedule_self_wakeup(min(RATE_LIMIT_MAX_ON429, max(1, ra)) + 1.0, notify_text, notify_file)
                return

            if not ok:
                if str(err).startswith("http_status:5") or str(err).startswith("http_error:") or status >= 500 or status == 0 or (err == "http_timeout"):
                    delay = max(_inc_backoff(task), SUPPLY_CREATE_MIN_RETRY_SECONDS)
                    task["last_error"] = f"labels_create_server_or_timeout:{err}"
                    update_task(task)
                    schedule(delay)
                    return

            task["labels_operation_id"] = op_id
            task["op_started_ts"] = now_ts()
            task["op_retries"] = 0
            task["status"] = ST_POLL_LABELS
            update_task(task)
            schedule(OPERATION_POLL_INTERVAL_SECONDS)
            return

        if st == ST_POLL_LABELS:
            ok, file_guid, err, status = await api_labels_get(api, task["labels_operation_id"])
            if status == 429 or (err and str(err or "").startswith("rate_limit:")):
                try:
                    ra = int(str(err).split(":", 1)[1]) if err else ON429_SHORT_RETRY_SEC
                except Exception:
                    ra = ON429_SHORT_RETRY_SEC
                _set_retry_after(task, min(RATE_LIMIT_MAX_ON429, max(1, ra)), jitter_max=1.5)
                task["last_error"] = "429 Too Many Requests (labels/get)"
                update_task(task)
                _schedule_self_wakeup(min(RATE_LIMIT_MAX_ON429, max(1, ra)) + 1.0, notify_text, notify_file)
                return

            if err:
                task["op_retries"] += 1
                if task["op_retries"] > SUPPLY_MAX_OPERATION_RETRIES:
                    task["status"] = ST_FAILED
                    task["last_error"] = str(err)
                    update_task(task)
                    try:
                        await notify_text(task["chat_id"], f"🟥 [{short(task['id'])}] labels/get: {err}")
                    except Exception:
                        logging.exception("notify_text failed on labels/get")
                    return

            if not ok:
                schedule(OPERATION_POLL_INTERVAL_SECONDS)
                return

            task["labels_file_guid"] = file_guid
            ok2, pdf_bytes, err2 = await api_labels_file(api, file_guid)
            if ok2 and pdf_bytes:
                pdf_path = DATA_DIR / f"labels_{task['id']}.pdf"
                pdf_path.write_bytes(pdf_bytes)
                task["labels_pdf_path"] = str(pdf_path)
                update_task(task)
                if AUTO_SEND_LABEL_PDF and pdf_path.exists():
                    try:
                        await notify_file(task["chat_id"], str(pdf_path), f"Этикетки supply_id={task.get('supply_id')} №{task.get('supply_order_number','')}".strip())
                    except Exception:
                        logging.exception("notify_file failed on labels pdf")

            task["status"] = ST_DONE
            update_task(task)
            try:
                await notify_text(task["chat_id"], f"🟩 [{short(task['id'])}] Готово. supply_id={task.get('supply_id')} cargo={len(task.get('cargo_ids',[]))}")
            except Exception:
                logging.exception("notify_text failed on DONE")
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

def register_supply_scheduler(scheduler, notify_text, notify_file, interval_seconds: int = 45):
    try:
        existing = scheduler.get_job(_SCHEDULER_JOB_ID)
    except Exception:
        existing = None

    if existing is not None:
        logging.info("Scheduler job '%s' already registered. Skip duplicate.", _SCHEDULER_JOB_ID)
        return

    async def _async_job():
        try:
            if _lock.locked():
                logging.info("process_tasks: SKIP (already running) [scheduler]")
                return
            await process_tasks(notify_text, notify_file)
        except Exception:
            logging.exception("Scheduler async job failed")

    try:
        scheduler.add_job(
            _async_job,
            "interval",
            seconds=max(5, int(interval_seconds)),
            id=_SCHEDULER_JOB_ID,
            coalesce=True,
            max_instances=1,
            replace_existing=False,
            misfire_grace_time=10,
        )
        logging.info("Scheduler job '%s' registered (interval=%ss).", _SCHEDULER_JOB_ID, interval_seconds)
    except Exception:
        logging.exception("Failed to register scheduler job '%s'", _SCHEDULER_JOB_ID)

async def process_tasks(notify_text, notify_file):
    ensure_loaded()
    sanitize_tasks_legacy_cyrillic()

    if _lock.locked():
        logging.info("process_tasks: SKIP (already running)")
        return

    logging.info("process_tasks: START")
    async with _lock:
        api = OzonApi(OZON_CLIENT_ID, OZON_API_KEY, timeout=API_TIMEOUT_SECONDS)

        drafts_done = 0
        supplies_done = 0

        for task in list_tasks():
            if task.get("status") == ST_CANCELED:
                continue

            st = str(task.get("status") or "").upper()

            if st == ST_DRAFT_CREATING and drafts_done >= SUPPLY_MAX_DRAFTS_PER_TICK:
                task["next_attempt_ts"] = now_ts() + 30
                update_task(task)
                logging.info("draft quota reached; defer task=%s", short(task["id"]))
                continue

            if st == ST_SUPPLY_CREATING and supplies_done >= SUPPLY_MAX_SUPPLY_CREATES_PER_TICK:
                task["next_attempt_ts"] = now_ts() + SUPPLY_CREATE_STAGE_DELAY_SECONDS
                update_task(task)
                logging.info("supply-create quota reached; defer task=%s", short(task["id"]))
                continue

            prev_status = st
            prev_creating = bool(task.get("creating"))
            prev_draft_http_ts = int(task.get("last_draft_http_attempt_ts") or 0)

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

            tnow = get_task(task["id"]) or task
            new_status = str(tnow.get("status") or "").upper()
            new_draft_http_ts = int(tnow.get("last_draft_http_attempt_ts") or 0)
            draft_http_called = (new_draft_http_ts > prev_draft_http_ts)

            pacing_flag = tnow.get("__draft_pacing_scheduled_ts")
            if pacing_flag and prev_status == ST_DRAFT_CREATING:
                try:
                    tnow.pop("__draft_pacing_scheduled_ts", None)
                    update_task(tnow)
                except Exception:
                    pass
                drafts_done += 1

            if prev_status == ST_DRAFT_CREATING:
                if new_status != ST_DRAFT_CREATING or draft_http_called or (prev_creating and not bool(tnow.get("creating"))):
                    drafts_done += 1

            if prev_status == ST_SUPPLY_CREATING:
                if new_status != ST_SUPPLY_CREATING or (prev_creating and not bool(tnow.get("creating"))):
                    supplies_done += 1

            if drafts_done >= SUPPLY_MAX_DRAFTS_PER_TICK and supplies_done >= SUPPLY_MAX_SUPPLY_CREATES_PER_TICK:
                logging.info("Reached per-tick quotas; stop this tick")
                break

        try:
            n = now_ts()
            threshold = max(1, int(SELF_WAKEUP_THRESHOLD_SECONDS))
            soon: Optional[float] = None
            for t in list_tasks():
                nt = int(t.get("next_attempt_ts") or 0)
                if nt and nt > n:
                    d = nt - n
                    if d <= threshold:
                        jit = float(t.get("retry_after_jitter") or 0.0)
                        candidate = max(0.05, d + jit)
                        soon = candidate if soon is None else min(soon, candidate)
            if soon is not None and soon > 0:
                _schedule_self_wakeup(soon, notify_text, notify_file)
        except Exception:
            logging.exception("process_tasks: self-wakeup scheduling failed")
    logging.info("process_tasks: END")