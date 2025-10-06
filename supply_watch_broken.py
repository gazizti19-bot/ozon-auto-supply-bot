# -*- coding: utf-8 -*-
"""
supply_watch.py (UNIFIED, FULL, aiogram v3-ready + v2 fallback)

Оркестратор автопоставок для Ozon Seller API.
Поддержка:
- Полный цикл: WAIT_WINDOW -> DRAFT_CREATING -> POLL_DRAFT -> TIMESLOT_SEARCH -> SUPPLY_CREATING ->
  POLL_SUPPLY -> SUPPLY_ORDER_FETCH -> (ORDER_DATA_FILLING) -> CARGO_PREP -> CARGO_CREATING ->
  POLL_CARGO -> LABELS_CREATING -> POLL_LABELS -> DONE/FAILED/CANCELED
- Устойчивый парсер шаблона, включая отдельную строку "Склад: <имя>" и строки позиций без склада
- Draft variant matrix с приоритетом вариантов под режим (CROSSDOCK/XDOCK -> xdock, FBO/DIRECT -> direct)
- Быстрые ретраи вариантов драфта при ошибке supply type is unknown
- Рейтлимиты и бэк-оффы
- Тайм-слоты, создание supply, карго, ярлыков
- Команды Telegram: /supply_new, /supply_list, /supply_dump <id>, /supply_cancel <id>, /supply_retry <id>,
  /supply_tick, /supply_purge, /supply_purge_all, /supply_debug
- Обертки для интеграции: schedule_supply(), purge_all_supplies_now()
- aiogram v3 (Router + dp.include_router) и v2 fallback (dp.register_message_handler)
- Фоновый воркер: ensure_worker_running(), worker_status(), stop_worker()

ENV (основные):
- SUPPLY_DRAFT_ENABLE_VARIANTS=1/0
- DRAFT_VARIANT_FAST_DELAY=3
- DRAFT_VARIANT_NORMAL_DELAY=12
- DRAFT_VARIANT_MAX=30
- SUPPLY_DRAFT_FORCE_WAREHOUSE=0
- SUPPLY_DRAFT_LOG_PAYLOAD=1
- SUPPLY_DRAFT_INJECT_TEST_FIELDS=0
- SUPPLY_WORKER_INTERVAL_SECONDS=45 — период фонового тика ensure_worker_running
- SUPPLY_WAREHOUSE_MAP='УФА_РФЦ=12345;склад=111' — сопоставление имени склада к ID
- DROP_ID=<int> — дефолтный drop_off_point_warehouse_id для CROSSDOCK, если не укажется из данных
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
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta, timezone

import httpx

log = logging.getLogger(__name__)

# ================== ENV ==================

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
        return default

def _getenv_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "on")

DATA_DIR = Path(_getenv_str("DATA_DIR", "./data")).resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)

SUPPLY_TASK_FILE = DATA_DIR / _getenv_str("SUPPLY_TASK_FILE", "supply_tasks.json")

OZON_CLIENT_ID = _getenv_str("OZON_CLIENT_ID", "")
OZON_API_KEY = _getenv_str("OZON_API_KEY", "")

API_TIMEOUT_SECONDS = _getenv_int("API_TIMEOUT_SECONDS", 15)

SUPPLY_PURGE_AGE_DAYS = _getenv_int("SUPPLY_PURGE_AGE_DAYS", 7)

SLOT_POLL_INTERVAL_SECONDS = _getenv_int("SLOT_POLL_INTERVAL_SECONDS", 180)
OPERATION_POLL_INTERVAL_SECONDS = _getenv_int("OPERATION_POLL_INTERVAL_SECONDS", 25)
OPERATION_POLL_TIMEOUT_SECONDS = _getenv_int("OPERATION_POLL_TIMEOUT_SECONDS", 600)

SUPPLY_MAX_OPERATION_RETRIES = _getenv_int("SUPPLY_MAX_OPERATION_RETRIES", 25)

AUTO_CREATE_CARGOES = _getenv_bool("AUTO_CREATE_CARGOES", True)
AUTO_CREATE_LABELS = _getenv_bool("AUTO_CREATE_LABELS", True)
AUTO_SEND_LABEL_PDF = _getenv_bool("AUTO_SEND_LABEL_PDF", True)

SUPPLY_TYPE_DEFAULT = _getenv_str("SUPPLY_TYPE_DEFAULT", "CREATE_TYPE_DIRECT")
SUPPLY_WAREHOUSE_MAP_RAW = _getenv_str("SUPPLY_WAREHOUSE_MAP", "").strip()

RATE_LIMIT_DEFAULT_COOLDOWN = _getenv_int("RATE_LIMIT_DEFAULT_COOLDOWN", 60)
CREATE_INITIAL_BACKOFF = _getenv_int("CREATE_INITIAL_BACKOFF", 2)
CREATE_MAX_BACKOFF = _getenv_int("CREATE_MAX_BACKOFF", 120)
MIN_CREATE_RETRY_SECONDS = _getenv_int("MIN_CREATE_RETRY_SECONDS", 180)

ORDER_FILL_POLL_INTERVAL_SECONDS = _getenv_int("ORDER_FILL_POLL_INTERVAL_SECONDS", 60)
ORDER_FILL_MAX_RETRIES = _getenv_int("ORDER_FILL_MAX_RETRIES", 150)

PROMPT_MIN_INTERVAL = _getenv_int("PROMPT_MIN_INTERVAL", 120)
SUPPLY_ALLOW_LEGACY_SET_TIMESLOT = _getenv_bool("SUPPLY_ALLOW_LEGACY_SET_TIMESLOT", False)
DROP_OFF_WAREHOUSE_ID = _getenv_int("DROP_ID", 0)

# Новые ENV для draft‑variant логики
SUPPLY_DRAFT_ENABLE_VARIANTS = _getenv_bool("SUPPLY_DRAFT_ENABLE_VARIANTS", True)
DRAFT_VARIANT_FAST_DELAY = _getenv_int("DRAFT_VARIANT_FAST_DELAY", 3)
DRAFT_VARIANT_NORMAL_DELAY = _getenv_int("DRAFT_VARIANT_NORMAL_DELAY", 12)
DRAFT_VARIANT_MAX = _getenv_int("DRAFT_VARIANT_MAX", 30)
SUPPLY_DRAFT_FORCE_WAREHOUSE = _getenv_bool("SUPPLY_DRAFT_FORCE_WAREHOUSE", False)
SUPPLY_DRAFT_LOG_PAYLOAD = _getenv_bool("SUPPLY_DRAFT_LOG_PAYLOAD", True)
SUPPLY_DRAFT_INJECT_TEST_FIELDS = _getenv_bool("SUPPLY_DRAFT_INJECT_TEST_FIELDS", False)

# Интервал фонового воркера
SUPPLY_WORKER_INTERVAL_SECONDS = _getenv_int("SUPPLY_WORKER_INTERVAL_SECONDS", 45)

# ================== Статусы ==================

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

# ================== Хранилище задач ==================

_tasks_loaded = False
_tasks: List[Dict[str, Any]] = []
_lock = asyncio.Lock()
_sync_lock = threading.RLock()

# dp-ссылка и фоновый воркер
_dp_ref: Any = None
_worker_task: Optional[asyncio.Task] = None
_worker_started: bool = False

ID_FIELDS = ("id", "task_id", "uuid", "pk", "external_id")
STATUS_FIELDS = ["status", "state", "phase", "stage", "step", "pipeline_status", "current_status", "progress", "phase_name"]
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
            _tasks = json.loads(SUPPLY_TASK_FILE.read_text("utf-8"))
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
    tmp.write_text(json.dumps(_tasks, ensure_ascii=False, indent=2), "utf-8")
    tmp.replace(SUPPLY_TASK_FILE)

def add_task(task: Dict[str, Any]):
    _tasks.append(task)
    save_tasks()

def purge_all_tasks() -> int:
    ensure_loaded()
    with _sync_lock:
        removed = len(_tasks)
        _tasks.clear()
        try:
            if SUPPLY_TASK_FILE.exists():
                SUPPLY_TASK_FILE.unlink()
        except Exception:
            try:
                SUPPLY_TASK_FILE.write_text("[]", encoding="utf-8")
            except Exception:
                pass
        return removed

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
            t[f] = payload[f]; changed = True
    return changed

def _apply_booking_fields(t: Dict[str, Any], payload: Dict[str, Any]) -> bool:
    changed = False
    for k in BOOKING_FIELDS:
        if k in payload and t.get(k) != payload[k]:
            t[k] = payload[k]; changed = True
    return changed

def _apply_generic_fields(t: Dict[str, Any], payload: Dict[str, Any]) -> bool:
    changed = False
    for k, v in payload.items():
        if k in ID_FIELDS or k in STATUS_FIELDS or k in BOOKING_FIELDS:
            continue
        if t.get(k) != v:
            t[k] = v; changed = True
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
                    payload.setdefault("id", tid); payload.setdefault("task_id", tid); payload.setdefault("uuid", tid)
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
            target["next_attempt_ts"] = now_ts() + 1
            changed = True
        elif has_order and cur not in (ST_SUPPLY_ORDER_FETCH, ST_ORDER_DATA_FILLING, ST_CARGO_PREP, ST_DONE, ST_FAILED, ST_CANCELED):
            target["status"] = ST_SUPPLY_ORDER_FETCH
            target["next_attempt_ts"] = now_ts() + 1
            changed = True

        if changed:
            _touch(target); save_tasks()

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
    cutoff = now_ts() - days * 86400
    before = len(_tasks)
    remain = []
    for t in _tasks:
        if t.get("status") in (ST_DONE, ST_FAILED, ST_CANCELED) and t.get("updated_ts", 0) < cutoff:
            continue
        remain.append(t)
    _tasks[:] = remain
    save_tasks()
    return before - len(remain)

# ================== Время/форматы и парсинг ==================

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

DASH_CLASS = r"[\-\u2010\u2011\u2012\u2013\u2014\u2015\u2212]"

HEADER_RE = re.compile(
    rf"\bНа\s+(\d{{2}})\.(\d{{2}})\.(\d{{4}}),\s*(\d{{2}}:\d{{2}})\s*{DASH_CLASS}\s*(\d{{2}}:\d{{2}})\b",
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

# Вариант без склада в строке
LINE_RE_NO_WH = re.compile(
    rf"""
    ^\s*
    (?P<sku>.+?)\s*{DASH_CLASS}\s*
    (?:количеств(?:о|а)|кол-?во|колво|qty|quantity)\s*
    (?P<qty>\d+)\s*[,;]?\s*
    (?P<boxes>\d+)\s*
    (?:короб(?:к(?:а|и|ок))?|кор\b|box(?:es)?)\s*[,;]?\s*
    (?:в\s*каждой\s*коробке\s*по|по)\s*
    (?P<per>\d+)\s*
    (?:шт(?:\.|ук)?|штук|штуки|штука)?\s*$
    """,
    re.IGNORECASE | re.VERBOSE
)

WAREHOUSE_LINE_RE = re.compile(
    r"^\s*(?:склад|warehouse)\s*[:\-]\s*(?P<wh>.+?)\s*$",
    re.IGNORECASE
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
    """
    Ищем строку заголовка среди всех строк.
    Возвращает (idx, date_iso, start_hhmm, end_hhmm).
    """
    for idx, raw in enumerate(lines):
        line = _canon_spaces_and_dashes(raw)
        m = HEADER_RE.search(line)
        if m:
            dd, mm, yyyy, start_hhmm, end_hhmm = m.groups()
            date_iso = f"{yyyy}-{mm}-{dd}"
            return idx, date_iso, start_hhmm, end_hhmm
    raise ValueError("Заголовок должен быть вида: 'На DD.MM.YYYY, HH:MM-HH:MM ...'")

def _parse_item_line_strict(raw_line: str) -> Optional[Dict[str, Any]]:
    line = _canon_spaces_and_dashes(raw_line)
    m2 = LINE_RE.match(line)
    if not m2:
        return None
    sku = m2.group("sku").strip()
    qty = int(m2.group("qty"))
    boxes = int(m2.group("boxes"))
    per_box = int(m2.group("per"))
    wh = (m2.group("wh") or "").strip().rstrip(".")
    return {"sku": sku, "total_qty": qty, "boxes": boxes, "per_box": per_box, "warehouse_name": wh}

def _parse_item_line_no_wh(raw_line: str) -> Optional[Dict[str, Any]]:
    line = _canon_spaces_and_dashes(raw_line)
    m2 = LINE_RE_NO_WH.match(line)
    if not m2:
        return None
    sku = m2.group("sku").strip()
    qty = int(m2.group("qty"))
    boxes = int(m2.group("boxes"))
    per_box = int(m2.group("per"))
    return {"sku": sku, "total_qty": qty, "boxes": boxes, "per_box": per_box}

def parse_template(text: str) -> Tuple[str, str, str, List[Dict[str, Any]]]:
    if not text or not isinstance(text, str):
        raise ValueError("Пустой шаблон.")
    text = "\n".join(_canon_spaces_and_dashes(line) for line in text.splitlines())
    lines = [l.rstrip() for l in text.splitlines() if l.strip()]
    if not lines:
        raise ValueError("Пустой шаблон.")
    header_idx, date_iso, start_hhmm, end_hhmm = _scan_header(lines)

    default_wh: Optional[str] = None
    items: List[Dict[str, Any]] = []
    for raw_line in lines[header_idx + 1:]:
        if not raw_line.strip():
            continue

        # Линия "Склад: ..."
        mwh = WAREHOUSE_LINE_RE.match(raw_line)
        if mwh:
            default_wh = mwh.group("wh").strip()
            continue

        # Позиция: сначала строгий парсинг со складом
        parsed = _parse_item_line_strict(raw_line)
        if not parsed:
            # Позиция без склада -> подставим default_wh
            parsed = _parse_item_line_no_wh(raw_line)
            if parsed and default_wh:
                parsed["warehouse_name"] = default_wh

        if not parsed:
            log.error("Line parse failed: %r", raw_line)
            raise ValueError(f"Не удалось распарсить строку: '{raw_line}'")

        if "warehouse_name" not in parsed or not parsed["warehouse_name"]:
            # все равно нет склада — не можем продолжать
            raise ValueError(f"Строка '{raw_line}': не указан склад (и нет строки 'Склад: ...' выше)")

        if parsed["total_qty"] != parsed["boxes"] * parsed["per_box"]:
            raise ValueError(f"Строка '{raw_line}': {parsed['total_qty']} != {parsed['boxes']}*{parsed['per_box']}")

        items.append(parsed)

    if not items:
        raise ValueError("Нет позиций.")
    return date_iso, start_hhmm, end_hhmm, items

# ================== HTTP ==================

def _parse_retry_after(headers: Dict[str, str]) -> int:
    try:
        ra = headers.get("Retry-After")
        if ra and str(ra).isdigit():
            return max(1, int(ra))
    except Exception:
        pass
    try:
        xrlr = headers.get("X-RateLimit-Reset")
        if xrlr and str(xrlr).isdigit():
            return max(1, int(xrlr))
    except Exception:
        pass
    return RATE_LIMIT_DEFAULT_COOLDOWN

class OzonApi:
    def __init__(self, client_id: str, api_key: str, timeout: int = 15):
        self.base = "https://api-seller.ozon.ru"
        self.client_id = client_id
        self.api_key = api_key
        self.timeout = timeout
        self.mock = not (client_id and api_key)

    def headers(self) -> Dict[str, str]:
        return {"Client-Id": self.client_id, "Api-Key": self.api_key, "Content-Type": "application/json"}

    async def request(self, method: str, path: str, payload: Dict[str, Any]) -> Tuple[bool, Any, str, int, Dict[str, str]]:
        if self.mock:
            log.info("MOCK %s %s payload_keys=%s", method, path, list(payload.keys()))
            return True, {"mock": True, "path": path, "payload": payload}, "", 200, {}
        url = f"{self.base}{path}"
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            try:
                r = await client.request(method=method, url=url, headers=self.headers(), json=payload)
                status = r.status_code
                headers = {k: v for k, v in r.headers.items()}
                snippet = (r.text or "")[:400]
                log.info("HTTP %s %s -> %s, status=%s, resp=%r", method, path, url, status, snippet)
                try:
                    data = r.json()
                except Exception:
                    data = {"text": r.text}
                ok = 200 <= status < 300
                if ok:
                    return True, data, "", status, headers
                return False, data, f"http_status:{status}", status, headers
            except Exception as e:
                log.exception("HTTP %s %s failed: %s", method, path, e)
                return False, {"error": str(e)}, f"http_error:{e}", 0, {}

    async def post(self, path: str, payload: Dict[str, Any]) -> Tuple[bool, Any, str, int, Dict[str, str]]:
        return await self.request("POST", path, payload)

    async def get_pdf(self, path: str) -> Tuple[bool, bytes, str]:
        if self.mock:
            log.info("MOCK GET %s (PDF)", path)
            return True, b"%PDF-1.4\n% Fake PDF\n", ""
        url = f"{self.base}{path}"
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            try:
                r = await client.get(url, headers=self.headers()); r.raise_for_status()
                return True, r.content, ""
            except httpx.HTTPError as e:
                log.exception("HTTP GET %s failed: %s", path, e)
                return False, b"", f"http_error:{e}"

# ================== DRAFT: create/info (с variant matrix) ==================

def _extract_dropoff_id(task: Dict[str, Any]) -> Optional[int]:
    keys = ["drop_off_point_warehouse_id","dropoff_warehouse_id","dropoffWarehouseId","drop_off_id","warehouse_id"]
    for k in keys:
        v = task.get(k)
        if v not in (None, "", 0, "0"):
            try:
                return int(str(v).strip())
            except Exception:
                pass
    if DROP_OFF_WAREHOUSE_ID:
        return int(DROP_OFF_WAREHOUSE_ID)
    return None

def _mode_prefers_xdock(task: Dict[str, Any]) -> bool:
    return str(task.get("mode") or "").upper() in ("CROSSDOCK", "XDOCK")

def _init_draft_variants(task: Dict[str, Any]) -> None:
    if task.get("draft_type_variants"):
        return
    variants: List[Dict[str, Any]] = [
        {"name": "v_supply_type_direct", "supply_type": "direct"},
        {"name": "v_supply_type_xdock", "supply_type": "xdock"},
        {"name": "v_type_direct", "type": "direct"},
        {"name": "v_type_xdock", "type": "xdock"},
        {"name": "v_type_DIRECT", "type": "DIRECT"},
        {"name": "v_type_XDOCK", "type": "XDOCK"},
        {"name": "v_both_direct", "type": "direct", "supply_type": "direct"},
        {"name": "v_both_xdock", "type": "xdock", "supply_type": "xdock"},
        {"name": "v_old_CREATE_TYPE_FBO", "type": "CREATE_TYPE_FBO"},
        {"name": "v_old_CREATE_TYPE_CROSSDOCK", "type": "CREATE_TYPE_CROSSDOCK"},
        {"name": "v_old_both_fbo_direct", "type": "CREATE_TYPE_FBO", "supply_type": "direct"},
        {"name": "v_old_both_cross_xdock", "type": "CREATE_TYPE_CROSSDOCK", "supply_type": "xdock"},
        {"name": "v_type_fbo", "type": "fbo"},
        {"name": "v_type_crossdock", "type": "crossdock"},
        {"name": "v_supplyType_direct", "supplyType": "direct"},
        {"name": "v_supplyType_xdock", "supplyType": "xdock"},
        {"name": "v_nested_supply_direct", "__nested_supply": {"supply_type": "direct"}},
        {"name": "v_nested_supply_xdock", "__nested_supply": {"supply_type": "xdock"}},
    ]
    prefers_xdock = _mode_prefers_xdock(task)
    def _score(v: Dict[str, Any]) -> int:
        blob = json.dumps(v, ensure_ascii=False).lower()
        if prefers_xdock:
            return 0 if "xdock" in blob else 1
        else:
            return 0 if "direct" in blob else 1
    variants.sort(key=_score)

    task["preferred_supply_type"] = "xdock" if prefers_xdock else "direct"
    task["draft_type_variants"] = variants
    task["draft_type_variant_index"] = 0
    task["draft_type_variants_tried"] = []
    task["draft_variant_events"] = []
    task["draft_variant_cycle_done"] = False
    task["draft_variants_total_attempts"] = 0
    update_task(task)

def _log_variant_event(task: Dict[str, Any], ev: Dict[str, Any]):
    buf = task.get("draft_variant_events") or []
    buf.append({"ts": now_ts(), **ev})
    if len(buf) > 50:
        buf = buf[-50:]
    task["draft_variant_events"] = buf

def _select_current_variant(task: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    variants = task.get("draft_type_variants") or []
    idx = int(task.get("draft_type_variant_index") or 0)
    if 0 <= idx < len(variants):
        return variants[idx]
    return None

async def api_draft_create(api: OzonApi, task: Dict[str, Any]) -> Tuple[bool, Optional[str], Optional[str], int]:
    if not SUPPLY_DRAFT_ENABLE_VARIANTS:
        return await _legacy_api_draft_create(api, task)

    _init_draft_variants(task)

    total_attempts = int(task.get("draft_variants_total_attempts") or 0)
    if total_attempts >= DRAFT_VARIANT_MAX:
        task["draft_variant_cycle_done"] = True
        update_task(task)
        return False, None, "draft_variants_exhausted", 400

    variant = _select_current_variant(task)
    if not variant:
        task["draft_variant_cycle_done"] = True
        update_task(task)
        return False, None, "draft_variants_exhausted", 400

    if "sku_list" not in task or not task["sku_list"]:
        return False, None, "no_items_for_draft", 400
    try:
        items = [{"sku": to_int_or_str(line["sku"]), "quantity": int(line.get("total_qty") or line.get("quantity") or 0)}
                 for line in task["sku_list"] if (line.get("total_qty") or line.get("quantity") or 0) > 0]
    except Exception as e:
        return False, None, f"items_build_error:{e}", 400
    if not items:
        return False, None, "empty_items", 400

    drop_id = _extract_dropoff_id(task)
    payload: Dict[str, Any] = {"items": items}

    # Слияние вариантов
    for k, v in variant.items():
        if k in ("name", "__nested_supply", "use_drop"):
            continue
        payload[k] = v

    if variant.get("__nested_supply"):
        payload["supply"] = dict(variant["__nested_supply"])

    # Если предпочтение xdock — при наличии drop_id подставим его
    needs_xdock = ("xdock" in json.dumps(variant, ensure_ascii=False).lower())
    if drop_id and needs_xdock:
        payload["drop_off_point_warehouse_id"] = drop_id

    if SUPPLY_DRAFT_FORCE_WAREHOUSE:
        chosen = task.get("chosen_warehouse_id")
        if chosen:
            payload.setdefault("warehouse_id", chosen)

    if SUPPLY_DRAFT_INJECT_TEST_FIELDS:
        payload["debug_mark"] = f"var_{variant['name']}"

    if SUPPLY_DRAFT_LOG_PAYLOAD:
        log.info("draft_create VAR=%s task=%s payload=%s", variant["name"], short(task["id"]), json.dumps(payload, ensure_ascii=False))
    else:
        log.info("draft_create VAR=%s task=%s payload_keys=%s", variant["name"], short(task["id"]), list(payload.keys()))

    attempt_local = 0
    backoff = 1.3
    while True:
        ok, data, err, status, headers = await api.post("/v1/draft/create", payload)
        if status == 429:
            attempt_local += 1
            if attempt_local >= 3:
                _log_variant_event(task, {"variant": variant["name"], "status": status, "info": "429_final"})
                ra = _parse_retry_after(headers)
                return False, None, f"rate_limit:{ra}", status
            log.warning("draft_create 429 variant=%s attempt=%s sleep=%.1fs", variant["name"], attempt_local, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 8.0)
            continue
        break

    total_attempts += 1
    task["draft_variants_total_attempts"] = total_attempts

    if ok and 200 <= status < 300:
        op_id = (data or {}).get("operation_id") or (data or {}).get("result", {}).get("operation_id")
        if not op_id:
            _log_variant_event(task, {"variant": variant["name"], "status": status, "info": "no_operation_id"})
            task["draft_type_variants_tried"].append(variant["name"])
            task["draft_type_variant_index"] += 1
            update_task(task)
            return False, None, "variant_retry:no_operation_id", 400
        task["winning_draft_variant"] = variant["name"]
        # Запишем определившийся тип
        task["supply_type"] = payload.get("supply_type") or payload.get("type") or task.get("supply_type")
        _log_variant_event(task, {"variant": variant["name"], "status": status, "info": "SUCCESS"})
        update_task(task)
        return True, op_id, None, status

    # Ошибки
    msg = ""
    if isinstance(data, dict):
        msg = str(data.get("message") or "")

    if "supply type is unknown" in (msg.lower() if msg else ""):
        task["draft_type_variants_tried"].append(variant["name"])
        task["draft_type_variant_index"] += 1
        task["draft_variant_last_error"] = msg
        _log_variant_event(task, {"variant": variant["name"], "status": status, "info": "supply_type_unknown"})
        variants = task.get("draft_type_variants") or []
        if task["draft_type_variant_index"] >= len(variants):
            task["draft_variant_cycle_done"] = True
            update_task(task)
            return False, None, "draft_variants_exhausted", 400
        update_task(task)
        return False, None, "variant_retry:supply_type_unknown", 400

    task["draft_variant_last_error"] = msg or f"status={status}"
    _log_variant_event(task, {"variant": variant["name"], "status": status, "info": "other_error", "msg": (msg or "")[:160]})
    update_task(task)
    return False, None, f"draft_create_error:{status}:{msg}", status

async def _legacy_api_draft_create(api: OzonApi, task: Dict[str, Any]) -> Tuple[bool, Optional[str], Optional[str], int]:
    items = [{"sku": to_int_or_str(x.get("sku")), "quantity": int(x.get("total_qty") or 0)} for x in task.get("sku_list", []) if (x.get("total_qty") or 0) > 0]
    if not items:
        return False, None, "empty_items", 400
    drop_id = _extract_dropoff_id(task)
    used_type = task.get("supply_type") or ("CREATE_TYPE_CROSSDOCK" if drop_id else SUPPLY_TYPE_DEFAULT)
    payload = {"items": items, "type": used_type}
    if drop_id:
        payload["drop_off_point_warehouse_id"] = drop_id
    ok, data, err, status, headers = await api.post("/v1/draft/create", payload)
    if not ok:
        return False, None, f"legacy_draft_error:{err}|{data}", status
    op_id = data.get("operation_id") or data.get("result", {}).get("operation_id")
    if not op_id:
        return False, None, "draft_create_no_operation_id", status
    task["supply_type"] = used_type
    update_task(task)
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
                bd = bids[0] or {}
                bundle_id = bd.get("bundle_id") or bd.get("id")
            status = (w or {}).get("status") or {}
            if wid:
                out.append({"warehouse_id": wid, "bundle_id": bundle_id, "status": status})
    return out

def _is_available(w: Dict[str, Any]) -> bool:
    st = w.get("status") or {}
    return bool(st.get("is_available", True)) or ((st.get("state") or "").upper().endswith("AVAILABLE"))

def choose_warehouse(warehouses: List[Dict[str, Any]]) -> Optional[int]:
    if not warehouses:
        return None
    normalized = []
    for w in warehouses:
        wid = w.get("warehouse_id") if isinstance(w, dict) else None
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
        return False, None, [], f"rate_limit:{ra}", status
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

# ================== Timeslot info ==================

async def api_timeslot_info(api: OzonApi, draft_id: str, warehouse_ids: List[int], date_iso: str, bundle_id: Optional[str] = None) -> Tuple[bool, List[Dict[str, Any]], str, int]:
    start_local, end_local = day_range_local(date_iso)
    endpoints = ["/v1/draft/timeslot/info","/v2/draft/timeslot/info"]
    base = {"draft_id": draft_id, "warehouse_ids": [int(w) for w in warehouse_ids]}
    variants: List[Dict[str, Any]] = [dict(base, date_from=start_local, date_to=end_local), dict(base, from_in_timezone=start_local, to_in_timezone=end_local)]
    if bundle_id:
        variants += [
            dict(base, date_from=start_local, date_to=end_local, bundle_id=bundle_id),
            dict(base, date_from=start_local, date_to=end_local, bundle_ids=[bundle_id]),
            dict(base, from_in_timezone=start_local, to_in_timezone=end_local, bundle_id=bundle_id),
            dict(base, from_in_timezone=start_local, to_in_timezone=end_local, bundle_ids=[bundle_id]),
        ]

    def normalize_slots(js: Dict[str, Any]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        flat = js.get("timeslots") or js.get("result", {}).get("timeslots") or []
        if isinstance(flat, list):
            for s in flat:
                if not isinstance(s, dict): continue
                fr = s.get("from_in_timezone") or s.get("from")
                to = s.get("to_in_timezone") or s.get("to")
                fr_loc = to_local_iso(fr) if fr else None
                to_loc = to_local_iso(to) if to else None
                slot = dict(s)
                if fr_loc: slot.setdefault("from_in_timezone", fr_loc); slot.setdefault("from", fr_loc)
                if to_loc: slot.setdefault("to_in_timezone", to_loc); slot.setdefault("to", to_loc)
                out.append(slot)
        drop = js.get("drop_off_warehouse_timeslots") or js.get("result", {}).get("drop_off_warehouse_timeslots") or []
        if isinstance(drop, list):
            for entry in drop:
                days = (entry or {}).get("days") or []
                for day in days:
                    tss = (day or {}).get("timeslots") or []
                    for s in tss:
                        if not isinstance(s, dict): continue
                        fr = s.get("from_in_timezone") or s.get("from")
                        to = s.get("to_in_timezone") or s.get("to")
                        fr_loc = to_local_iso(fr) if fr else None
                        to_loc = to_local_iso(to) if to else None
                        slot = dict(s)
                        slot.setdefault("capacity_status", "")
                        if fr_loc: slot["from_in_timezone"] = fr_loc; slot.setdefault("from", fr_loc)
                        if to_loc: slot["to_in_timezone"] = to_loc; slot.setdefault("to", to_loc)
                        out.append(slot)
        return out

    last_err = ""
    last_status = 0
    for ep in endpoints:
        for pl in variants:
            ok, data, err, status, headers = await api.post(ep, pl)
            log.info("timeslot_info: ep=%s status=%s keys=%s", ep, status, list(pl.keys()))
            if status == 429:
                ra = _parse_retry_after(headers)
                return False, [], f"rate_limit:{ra}", status
            if ok:
                slots = normalize_slots(data)
                return True, slots, "", status
            last_err = f"timeslot_info_error:{err}|{data}"
            last_status = status
    return False, [], (last_err or "timeslot_info_error:unknown"), (last_status or 400)

# ================== Supply create / status ==================

def _drop_id_from_task(task: Dict[str, Any]) -> Optional[int]:
    for k in ("dropoff_warehouse_id", "drop_off_point_warehouse_id", "drop_off_id"):
        v = task.get(k)
        if v not in (None, "", 0, "0"):
            try:
                return int(v)
            except Exception:
                pass
    if DROP_OFF_WAREHOUSE_ID:
        return int(DROP_OFF_WAREHOUSE_ID)
    return None

async def api_supply_create(api: OzonApi, task: Dict[str, Any]) -> Tuple[bool, Optional[str], Optional[str], int]:
    if api.mock:
        return True, f"op-supply-{short(task['id'])}", None, 200

    draft_id = task["draft_id"]
    wid = int(task.get("chosen_warehouse_id") or 0)
    fr = task["desired_from_iso"]; to = task["desired_to_iso"]
    slot_id = task.get("slot_id"); bundle_id = task.get("bundle_id")
    drop_id = _drop_id_from_task(task)

    if wid <= 0:
        return False, None, "supply_create_error:warehouse_id_is_zero", 400

    base = {"draft_id": draft_id, "from_in_timezone": fr, "to_in_timezone": to}
    variants: List[Dict[str, Any]] = [
        dict(base, warehouse_id=wid),
        dict(base, warehouse_ids=[wid]),
    ]
    if drop_id:
        variants += [
            dict(base, warehouse_id=wid, drop_off_point_warehouse_id=int(drop_id)),
            dict(base, warehouse_ids=[wid], drop_off_point_warehouse_id=int(drop_id)),
        ]
    if slot_id is not None:
        variants += [
            dict(base, warehouse_id=wid, timeslot_id=slot_id),
        ]
        if drop_id:
            variants += [
                dict(base, warehouse_id=wid, timeslot_id=slot_id, drop_off_point_warehouse_id=int(drop_id)),
            ]
    if bundle_id:
        variants += [
            dict(base, warehouse_id=wid, bundle_id=bundle_id),
            dict(base, warehouse_id=wid, bundle_ids=[bundle_id]),
        ]
        if drop_id:
            variants += [
                dict(base, warehouse_id=wid, bundle_id=bundle_id, drop_off_point_warehouse_id=int(drop_id)),
            ]

    uniq: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for p in variants:
        key = json.dumps(p, sort_keys=True, ensure_ascii=False)
        if key in seen:
            continue
        seen.add(key); uniq.append(p)

    attempt_429 = 0; backoff = 1.7
    last_data: Any = None; last_err: str = ""; last_status: int = 400

    for pl in uniq:
        while True:
            ok, data, err, status, headers = await api.post("/v1/draft/supply/create", pl)
            log.info("supply_create: status=%s keys=%s warehouse_id=%s",
                     status, list(pl.keys()), pl.get("warehouse_id") or (pl.get("warehouse_ids") or [None])[0])
            if status == 429:
                attempt_429 += 1
                if attempt_429 > 3:
                    ra = _parse_retry_after(headers)
                    return False, None, f"rate_limit:{ra}", status
                log.warning("Ozon 429 for POST /v1/draft/supply/create -> wait %.2fs (attempt %s)", backoff, attempt_429)
                await asyncio.sleep(backoff); backoff = min(backoff * 2, 12.0); continue
            break

        if 200 <= status < 300 and ok:
            op_id = data.get("operation_id") or data.get("result", {}).get("operation_id")
            if not op_id:
                return False, None, f"supply_create_no_operation_id:{data}", status
            return True, op_id, None, status

        last_data, last_err, last_status = data, err, status

        if status == 400 and isinstance(data, dict):
            msg = str(data.get("message") or "")
            if "WarehouseId" in msg and "greater than 0" in msg:
                continue

    return False, None, f"supply_create_error:{last_err}|{last_data}", last_status

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
    res_arr = res.get("order_ids")
    if isinstance(res_arr, list) and res_arr:
        try:
            return str(int(res_arr[0]))
        except Exception:
            return str(res_arr[0])
    return None

async def api_supply_create_status(api: OzonApi, operation_id: str) -> Tuple[bool, Optional[str], Optional[str], int]:
    payload = {"operation_id": operation_id}
    ok, data, err, status, headers = await api.post("/v1/draft/supply/create/status", payload)
    if status == 429:
        ra = _parse_retry_after(headers); return False, None, f"rate_limit:{ra}", status
    if not ok:
        return False, None, f"supply_status_error:{err}|{data}", status

    raw_status = str(data.get("status") or (data.get("result") or {}).get("status") or "").strip()
    s_norm = raw_status.lower()
    order_id = _extract_order_id_from_supply_status(data)

    if "success" in s_norm or (order_id and not s_norm):
        if not order_id:
            return False, None, f"supply_status_no_order_id:{data}", status
        return True, order_id, None, status

    if "progress" in s_norm or s_norm in ("in_progress","pending","processing"):
        return False, None, None, status

    errors = data.get("error_messages") or (data.get("result") or {}).get("error_messages") or []
    if errors:
        return False, None, f"supply_status_error_messages:{errors}", status

    if order_id:
        return True, order_id, None, status

    return False, None, f"supply_status_unrecognized:{raw_status}", status

# ================== supply-order/get ==================

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

    def _first_supply_id(o: Dict[str, Any]) -> Optional[str]:
        for k in ("supply_id", "supplyId"):
            v = o.get(k)
            if v:
                return str(v)
        for k in ("supply", "supply_order", "supplyOrder"):
            v = o.get(k)
            if isinstance(v, dict):
                for kk in ("id", "supply_id", "supplyId"):
                    vv = v.get(kk)
                    if vv:
                        return str(vv)
        return None

    for scope in [js, js.get("result") or {}]:
        if isinstance(scope, dict):
            sid = _first_supply_id(scope)
            num = _first_str(scope, ["supply_order_number","supplyOrderNumber","order_number","orderNumber","number"])
            if sid or num:
                return sid, num

    list_keys = ["orders","supply_orders","supplyOrders","items","data","order_list","supply_orders_list"]
    for scope in [js, js.get("result") or {}]:
        if not isinstance(scope, dict):
            continue
        for lk in list_keys:
            arr = scope.get(lk)
            if isinstance(arr, list) and arr:
                first = arr[0]
                if isinstance(first, dict):
                    sid = _first_supply_id(first)
                    num = _first_str(first, ["supply_order_number","supplyOrderNumber","order_number","orderNumber","number"])
                    if sid or num:
                        return sid, num
    return None, None

def _extract_order_meta(js: Dict[str, Any]) -> Dict[str, Any]:
    meta = {"state":"", "timeslot":{"required":False,"can_set":False,"from":None,"to":None}, "vehicle":{"required":False,"can_set":False}, "contact":{"required":False,"can_set":False}, "dropoff_warehouse_id":None}
    candidate = None
    for scope in [js, js.get("result") or {}]:
        for key in ("orders","supply_orders","supplyOrders","items","data","order_list","supply_orders_list"):
            arr = scope.get(key)
            if isinstance(arr, list) and arr and isinstance(arr[0], dict):
                candidate = arr[0]; break
        if candidate: break
    if not candidate and isinstance(js, dict):
        candidate = js.get("result") or js
    if not isinstance(candidate, dict): return meta

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

    for k in ("dropoff_warehouse_id","dropoffWarehouseId","warehouse_id","warehouseId"):
        v = candidate.get(k)
        if v is not None:
            try: meta["dropoff_warehouse_id"] = int(v)
            except Exception: pass
            break
    return meta

async def api_supply_order_get(api: OzonApi, order_id: str) -> Tuple[bool, Optional[str], Optional[str], Optional[str], int, Dict[str, Any]]:
    endpoints = ["/v2/supply-order/get","/v1/supply-order/get"]
    try: oid = int(str(order_id).strip())
    except Exception: oid = str(order_id).strip()

    payloads_v2 = [{"order_ids":[oid]},{"supply_order_ids":[oid]},{"order_id":oid},{"supply_order_id":oid}]
    payloads_v1 = [{"order_id":oid},{"supply_order_id":oid}]

    last_err = None; last_status = 0

    for pl in payloads_v2:
        ok, data, err, status, headers = await api.post(endpoints[0], pl)
        if status == 429:
            ra = _parse_retry_after(headers); return False, None, None, f"rate_limit:{ra}", status, {}
        if ok:
            sid, num = _extract_supply_order_info_from_response(data)
            meta = _extract_order_meta(data)
            return True, sid, (num or ""), None, status, meta
        else:
            last_err, last_status = f"supply_order_get_error:{err}|{data}", status

    for pl in payloads_v1:
        ok, data, err, status, headers = await api.post(endpoints[1], pl)
        if status == 429:
            ra = _parse_retry_after(headers); return False, None, None, f"rate_limit:{ra}", status, {}
        if ok:
            sid, num = _extract_supply_order_info_from_response(data)
            meta = _extract_order_meta(data)
            return True, sid, (num or ""), None, status, meta
        else:
            last_err, last_status = f"supply_order_get_error:{err}|{data}", status

    return False, None, None, (last_err or "supply_order_get_error:unknown"), (last_status or 400), {}

# ================== DATA_FILLING actions ==================

async def api_supply_order_timeslot_set(api: OzonApi, order_id: Any, from_iso: str, to_iso: str, slot_id: Optional[Any] = None, dropoff_warehouse_id: Optional[int] = None, supply_id: Optional[Any] = None) -> Tuple[bool, Optional[str], int]:
    def _to_num_or_str(x: Any) -> Any:
        try:
            return int(str(x).strip())
        except Exception:
            return str(x).strip()

    oid = _to_num_or_str(order_id)
    sid = _to_num_or_str(supply_id) if supply_id is not None else None

    endpoints = [
        "/v1/supply-order/appointment/set","/v2/supply-order/appointment/set",
        "/v1/draft/supply/appointment/set","/v2/draft/supply/appointment/set",
        "/v1/supply-order/update","/v2/supply-order/update",
    ]
    if sid is not None:
        endpoints = ["/v1/supply/appointment/set","/v2/supply/appointment/set"] + endpoints

    if SUPPLY_ALLOW_LEGACY_SET_TIMESLOT:
        endpoints += ["/v2/supply-order/timeslot/set","/v1/supply-order/timeslot/set","/v1/supply-order/set-timeslot"]

    methods = ["POST","PUT","PATCH"]

    from_variants = [from_iso]; to_variants = [to_iso]
    zf, zt = _to_z_iso(from_iso), _to_z_iso(to_iso)
    if zf and zf not in from_variants: from_variants.append(zf)
    if zt and zt not in to_variants: to_variants.append(zt)

    base_bodies: List[Dict[str, Any]] = []
    for fr in from_variants:
        for to in to_variants:
            base_bodies.extend([
                {"timeslot":{"from":fr,"to":to}},
                {"timeslot":{"from_in_timezone":fr,"to_in_timezone":to}},
                {"value":{"timeslot":{"from":fr,"to":to}}},
                {"update":{"timeslot":{"from":fr,"to":to}}},
                {"from_in_timezone":fr,"to_in_timezone":to},
                {"appointment":{"from":fr,"to":to}},
                {"appointment":{"from_in_timezone":fr,"to_in_timezone":to}},
            ])
    if slot_id is not None:
        base_bodies.extend([
            {"timeslot":{"id":slot_id}},
            {"value":{"timeslot":{"id":slot_id}}},
            {"update":{"timeslot":{"id":slot_id}}},
            {"timeslot_id":slot_id},
            {"appointment":{"id":slot_id}},
        ])

    bodies: List[Dict[str, Any]] = []
    for b in base_bodies:
        bodies.append(dict(b))
        if dropoff_warehouse_id:
            for top_key in ("dropoff_warehouse_id","warehouse_id","drop_off_point_warehouse_id"):
                bb = dict(b); bb[top_key] = dropoff_warehouse_id; bodies.append(bb)
            bb = dict(b); bb.setdefault("update", {}); bb["update"]["dropoff_warehouse_id"] = dropoff_warehouse_id; bodies.append(bb)
            bb = dict(b); bb.setdefault("update", {}); bb["update"]["warehouse_id"] = dropoff_warehouse_id; bodies.append(bb)

    id_variants: List[Dict[str, Any]] = [{"order_id":oid},{"supply_order_id":oid},{"id":oid}]
    if sid is not None:
        id_variants = [{"supply_id":sid},{"id":sid}] + id_variants

    payloads: List[Dict[str, Any]] = []
    for idv in id_variants:
        for b in bodies:
            p = dict(idv); p.update(b); payloads.append(p)

    total_attempts = 0; MAX_ATTEMPTS_PER_CALL = 60

    for ep in endpoints:
        endpoint_route_missing = False
        for method in methods:
            for pl in payloads:
                total_attempts += 1
                if total_attempts > MAX_ATTEMPTS_PER_CALL:
                    return False, "timeslot_set_too_many_attempts", 429
                ok, data, err, status, headers = await api.request(method, ep, pl)
                log.info("timeslot_set: method=%s ep=%s status=%s keys=%s", method, ep, status, list(pl.keys()))
                if status == 404 and isinstance(data, dict) and "text" in data and "page not found" in str(data["text"]).lower():
                    endpoint_route_missing = True
                    log.info("timeslot_set: skip endpoint %s due to 404 page not found", ep)
                    break
                if status == 429:
                    ra = _parse_retry_after(headers)
                    return False, f"rate_limit:{ra}", status
                if ok:
                    return True, None, status
            if endpoint_route_missing:
                break
        if endpoint_route_missing:
            continue

    return False, "timeslot_set_failed:all_endpoints_rejected_or_missing", 404

async def api_supply_order_set_vehicle(api: OzonApi, order_id: Any, vehicle_text: str) -> Tuple[bool, Optional[str], int]:
    try:
        oid_int = int(str(order_id).strip()); oid = oid_int
    except Exception:
        oid = str(order_id).strip()

    endpoints = ["/v2/supply-order/update","/v1/supply-order/update","/v2/supply-order/vehicle/set","/v1/supply-order/vehicle/set","/v1/supply-order/set-vehicle"]
    methods = ["POST","PUT","PATCH"]
    variants = [
        {"vehicle":{"number":vehicle_text}},
        {"vehicle":{"plate":vehicle_text}},
        {"vehicle":{"text":vehicle_text}},
        {"update":{"vehicle":{"number":vehicle_text}}},
        {"update":{"vehicle":{"plate":vehicle_text}}},
        {"update":{"vehicle":{"text":vehicle_text}}},
        {"transport":{"text":vehicle_text}},
        {"update":{"transport":{"text":vehicle_text}}},
    ]
    id_variants = [{"order_id":oid},{"supply_order_id":oid},{"id":oid}]
    payloads: List[Dict[str, Any]] = []
    for idv in id_variants:
        for b in variants:
            p = dict(idv); p.update(b); payloads.append(p)

    for ep in endpoints:
        for method in methods:
            for pl in payloads:
                ok, data, err, status, headers = await api.request(method, ep, pl)
                if status == 429:
                    ra = _parse_retry_after(headers); return False, f"rate_limit:{ra}", status
                if ok:
                    return True, None, status
    return False, "vehicle_set_failed", 404

async def api_supply_order_set_contact(api: OzonApi, order_id: Any, phone: str, name: str = "") -> Tuple[bool, Optional[str], int]:
    try:
        oid_int = int(str(order_id).strip()); oid = oid_int
    except Exception:
        oid = str(order_id).strip()

    endpoints = ["/v2/supply-order/update","/v1/supply-order/update","/v2/supply-order/contact/set","/v1/supply-order/contact/set","/v1/supply-order/set-contact"]
    methods = ["POST","PUT","PATCH"]
    variants = [
        {"contact":{"phone":phone,"name":name}},
        {"responsible":{"phone":phone,"name":name}},
        {"update":{"contact":{"phone":phone,"name":name}}},
        {"update":{"responsible":{"phone":phone,"name":name}}},
        {"phone":phone,"name":name},
    ]
    id_variants = [{"order_id":oid},{"supply_order_id":oid},{"id":oid}]
    payloads: List[Dict[str, Any]] = []
    for idv in id_variants:
        for b in variants:
            p = dict(idv); p.update(b); payloads.append(p)

    for ep in endpoints:
        for method in methods:
            for pl in payloads:
                ok, data, err, status, headers = await api.request(method, ep, pl)
                if status == 429:
                    ra = _parse_retry_after(headers); return False, f"rate_limit:{ra}", status
                if ok:
                    return True, None, status
    return False, "contact_set_failed", 404

# ================== LABELS/CARGO API ==================

async def api_cargoes_create(api: OzonApi, payload: Dict[str, Any]) -> Tuple[bool, Optional[str], Optional[str], int]:
    ok, data, err, status, headers = await api.post("/v1/cargoes/create", payload)
    if status == 429:
        ra = _parse_retry_after(headers); return False, None, f"rate_limit:{ra}", status
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
        ra = _parse_retry_after(headers); return False, [], f"rate_limit:{ra}", status
    if not ok:
        return False, [], f"cargoes_info_error:{err}|{data}", status
    s = (data.get("status") or data.get("result", {}).get("status") or "").upper()
    if s in ("IN_PROGRESS","PENDING",""):
        return False, [], None, status
    if s not in ("SUCCESS","OK","DONE"):
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
        ra = _parse_retry_after(headers); return False, None, f"rate_limit:{ra}", status
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
        ra = _parse_retry_after(headers); return False, None, f"rate_limit:{ra}", status
    if not ok:
        return False, None, f"labels_get_error:{err}|{data}", status
    s = (data.get("status") or data.get("result", {}).get("status") or "").upper()
    if s in ("IN_PROGRESS","PENDING",""):
        return False, None, None, status
    if s not in ("SUCCESS","OK","DONE"):
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

# ================== Логика пайплайна ==================

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
            cargoes.append({"key": str(uuid.uuid4()), "type": "BOX", "items": [{"sku": sku, "quantity": per_box}]})
    return {"supply_id": supply_id, "delete_current_version": True, "cargoes": cargoes}

def _inc_backoff(task: Dict[str, Any]) -> int:
    b = int(task.get("create_backoff_sec") or CREATE_INITIAL_BACKOFF)
    b = min(max(b, CREATE_INITIAL_BACKOFF), CREATE_MAX_BACKOFF)
    task["create_backoff_sec"] = min(max(b * 2, CREATE_INITIAL_BACKOFF), CREATE_MAX_BACKOFF)
    return b

def _set_retry_after(task: Dict[str, Any], seconds: int):
    seconds = max(1, int(seconds))
    task["retry_after_ts"] = now_ts() + seconds
    task["next_attempt_ts"] = task["retry_after_ts"]

async def _auto_fill_timeslot_if_needed(task: Dict[str, Any], api: OzonApi, meta: Dict[str, Any], notify_text) -> bool:
    ts_meta = meta.get("timeslot") or {}
    required = bool(ts_meta.get("required"))
    can_set = bool(ts_meta.get("can_set"))
    has_from = ts_meta.get("from")
    has_to = ts_meta.get("to")

    if has_from and has_to:
        return True
    if not required or not can_set:
        return False

    fr = task.get("from_in_timezone") or task.get("desired_from_iso")
    to = task.get("to_in_timezone") or task.get("desired_to_iso")
    if not (fr and to):
        return False

    if task.get("order_timeslot_set_ok"):
        return True
    if now_ts() - int(task.get("order_timeslot_set_ts") or 0) < 20:
        return True

    ok, err, status = await api_supply_order_timeslot_set(api, task["order_id"], fr, to, task.get("slot_id"), task.get("dropoff_warehouse_id"), task.get("supply_id"))
    task["order_timeslot_set_attempts"] = int(task.get("order_timeslot_set_attempts") or 0) + 1
    task["order_timeslot_set_ts"] = now_ts()
    if ok:
        task["order_timeslot_set_ok"] = True
        update_task(task)
        try:
            await notify_text(task["chat_id"], f"🟦 [{short(task['id'])}] Тайм‑слот проставлен в заявке.")
        except Exception:
            log.exception("notify_text failed on timeslot set")
        return True
    else:
        task["last_error"] = f"timeslot_set:{err or status}"
        update_task(task)
        try:
            await notify_text(task["chat_id"], f"🟨 [{short(task['id'])}] Не удалось проставить тайм‑слот (попробуем позже).")
        except Exception:
            log.exception("notify_text failed on timeslot fail")
        return False

async def _prompt_missing_fields(task: Dict[str, Any], meta: Dict[str, Any], notify_text) -> bool:
    asked = False
    veh = meta.get("vehicle") or {}
    cnt = meta.get("contact") or {}
    now = now_ts()
    is_crossdock = (str(task.get("supply_type") or "").upper() == "CREATE_TYPE_CROSSDOCK") or bool(
        task.get("dropoff_warehouse_id") or task.get("drop_off_point_warehouse_id") or task.get("drop_off_id") or DROP_OFF_WAREHOUSE_ID
    )
    if veh.get("required") and not is_crossdock and not task.get("order_vehicle_text"):
        if now - int(task.get("need_vehicle_prompt_ts") or 0) >= PROMPT_MIN_INTERVAL:
            task["need_vehicle"] = True
            task["need_vehicle_prompt_ts"] = now
            update_task(task)
            try:
                await notify_text(task["chat_id"],
                    "🚚 Для заявки требуется транспорт. Ответьте сообщением:\n"
                    f"ТРАНСПОРТ {short(task['id'])} <госномер и описание>\n"
                    f"пример: ТРАНСПОРТ {short(task['id'])} А123ВС 116 RUS Газель тент"
                )
            except Exception:
                log.exception("notify_text failed on vehicle prompt")
            asked = True
    else:
        if task.get("need_vehicle"):
            task["need_vehicle"] = False
            update_task(task)

    if cnt.get("required") and not (task.get("order_contact_phone") or task.get("order_contact_name")):
        if now - int(task.get("need_contact_prompt_ts") or 0) >= PROMPT_MIN_INTERVAL:
            task["need_contact"] = True
            task["need_contact_prompt_ts"] = now
            update_task(task)
            try:
                await notify_text(
                    task["chat_id"],
                    "📱 Для заявки требуется контакт. Ответьте сообщением:\n"
                    f"КОНТАКТ {short(task['id'])} +79990000000 [Имя]\n"
                    f"пример: КОНТАКТ {short(task['id'])} +79991234567 Иван"
                )
            except Exception:
                log.exception("notify_text failed on contact prompt")
            asked = True
    else:
        if task.get("need_contact"):
            task["need_contact"] = False
            update_task(task)

    return asked

async def _scheduled_tick():
    bot = getattr(_dp_ref, "bot", None)
    async def _notify_text(chat_id: int, text: str):
        if bot:
            await bot.send_message(chat_id, text)
    async def _notify_file(chat_id: int, file_path: str, caption: str = ""):
        if not bot:
            return
        try:
            with open(file_path, "rb") as f:
                await bot.send_document(chat_id, f, caption=caption or None)
        except Exception:
            log.exception("notify_file failed in scheduler")
    await process_tasks(_notify_text, _notify_file)

async def _delayed_tick(sec: int = 3):
    try:
        await asyncio.sleep(max(1, int(sec)))
        await _scheduled_tick()
    except Exception:
        log.exception("delayed_tick failed")

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
            log.exception("notify_text failed on window expired")
        return

    if task.get("supply_id") and task.get("status") not in (ST_CARGO_PREP, ST_CARGO_CREATING, ST_POLL_CARGO, ST_LABELS_CREATING, ST_POLL_LABELS, ST_DONE):
        task["status"] = ST_CARGO_PREP if AUTO_CREATE_CARGOES else ST_DONE
        update_task(task)
        task["next_attempt_ts"] = now_ts() + 1
        update_task(task)
        return

    next_ts = int(task.get("next_attempt_ts") or 0)
    if n < next_ts:
        return

    rat = int(task.get("retry_after_ts") or 0)
    if rat and n < rat:
        if task.get("status") != "RATE_LIMIT":
            task["status"] = "RATE_LIMIT"
            update_task(task)
        return

    def schedule(sec: int):
        task["next_attempt_ts"] = now_ts() + max(1, int(sec))
        update_task(task)

    if task.get("order_id") and task.get("status") in (ST_TIMESLOT_SEARCH, "RATE_LIMIT", "BOOKED", "booked", ""):
        task["status"] = ST_SUPPLY_ORDER_FETCH
        task["op_started_ts"] = now_ts()
        task["op_retries"] = 0
        update_task(task)
        schedule(1)
        return

    try:
        st = task["status"]

        if st == ST_WAIT_WINDOW:
            task["status"] = ST_DRAFT_CREATING
            task["creating"] = False
            task.setdefault("create_attempts", 0)
            task.pop("create_backoff_sec", None)
            update_task(task)

        if task["status"] == ST_DRAFT_CREATING:
            last_upd = int(task.get("updated_ts") or 0)
            if n - last_upd < MIN_CREATE_RETRY_SECONDS and task.get("create_attempts", 0) > 0 and not SUPPLY_DRAFT_ENABLE_VARIANTS:
                schedule(MIN_CREATE_RETRY_SECONDS - (n - last_upd))
                return

            if task.get("creating"):
                return

            task["creating"] = True
            update_task(task)

            ok, op_id, err, status = await api_draft_create(api, task)

            task["creating"] = False

            if status == 429 or (err and str(err).startswith("rate_limit:")):
                try:
                    ra = int(str(err).split(":", 1)[1])
                except Exception:
                    ra = RATE_LIMIT_DEFAULT_COOLDOWN
                wait_sec = max(ra, 15)
                _set_retry_after(task, wait_sec)
                task["status"] = "RATE_LIMIT"
                task["last_error"] = "429 Too Many Requests"
                task["create_attempts"] = int(task.get("create_attempts") or 0) + 1
                update_task(task)
                try:
                    await notify_text(task["chat_id"], f"⏱️ [{short(task['id'])}] Лимит Ozon (429). Повтор через {wait_sec} сек.")
                except Exception:
                    log.exception("notify_text failed on RATE_LIMIT draft")
                return

            if not ok and err and err.startswith("variant_retry:supply_type_unknown"):
                task["status"] = ST_DRAFT_CREATING
                task["last_error"] = "variant:supply_type_unknown"
                update_task(task)
                schedule(DRAFT_VARIANT_FAST_DELAY)
                # Быстрый пинок воркера
                asyncio.create_task(_delayed_tick(DRAFT_VARIANT_FAST_DELAY))
                return

            if not ok and err and err.startswith("variant_retry:no_operation_id"):
                task["status"] = ST_DRAFT_CREATING
                task["last_error"] = "variant:no_operation_id"
                update_task(task)
                schedule(DRAFT_VARIANT_FAST_DELAY)
                asyncio.create_task(_delayed_tick(DRAFT_VARIANT_FAST_DELAY))
                return

            if not ok and err in ("draft_variants_exhausted","no_items_for_draft","empty_items"):
                task["status"] = ST_FAILED
                task["last_error"] = err
                update_task(task)
                try:
                    await notify_text(task["chat_id"], f"🟥 [{short(task['id'])}] Draft variants exhausted.")
                except Exception:
                    log.exception("notify_text variant exhausted")
                return

            if not ok and err and err.startswith("draft_create_error:"):
                task["status"] = ST_FAILED
                task["last_error"] = err
                update_task(task)
                try:
                    await notify_text(task["chat_id"], f"🟥 [{short(task['id'])}] draft error: {err}")
                except Exception:
                    log.exception("notify_text draft error")
                return

            if not ok and err and (err.startswith("http_status:5") or err.startswith("http_error:") or status >= 500 or status == 0) and not SUPPLY_DRAFT_ENABLE_VARIANTS:
                delay = max(_inc_backoff(task), MIN_CREATE_RETRY_SECONDS)
                task["last_error"] = f"server_error:{err}"
                task["create_attempts"] = int(task.get("create_attempts") or 0) + 1
                update_task(task)
                schedule(delay)
                return

            if not ok:
                task["status"] = ST_FAILED
                task["last_error"] = err or "draft_create_fail"
                update_task(task)
                return

            task["draft_operation_id"] = op_id
            task["op_started_ts"] = now_ts()
            task["op_retries"] = 0
            task["status"] = ST_POLL_DRAFT
            task["last_error"] = ""
            update_task(task)
            schedule(OPERATION_POLL_INTERVAL_SECONDS)
            return

        if task["status"] == ST_POLL_DRAFT:
            if now_ts() - task.get("op_started_ts", n) > OPERATION_POLL_TIMEOUT_SECONDS:
                task["status"] = ST_FAILED
                task["last_error"] = "draft timeout"
                update_task(task)
                try:
                    await notify_text(task["chat_id"], f"🟥 [{short(task['id'])}] Таймаут draft/info")
                except Exception:
                    log.exception("notify_text failed on draft timeout")
                return
            ok, draft_id, warehouses, err, status = await api_draft_create_info(api, task["draft_operation_id"])

            if status == 429 or (err and str(err).startswith("rate_limit:")):
                try:
                    ra = int(str(err).split(":", 1)[1])
                except Exception:
                    ra = RATE_LIMIT_DEFAULT_COOLDOWN
                wait_sec = max(ra, MIN_CREATE_RETRY_SECONDS)
                _set_retry_after(task, wait_sec)
                task["status"] = "RATE_LIMIT"
                task["last_error"] = "429 Too Many Requests (draft/info)"
                update_task(task)
                schedule(wait_sec)
                return

            if err:
                task["op_retries"] += 1
                if task["op_retries"] > SUPPLY_MAX_OPERATION_RETRIES:
                    task["status"] = ST_FAILED
                    task["last_error"] = str(err)
                    update_task(task)
                    try:
                        await notify_text(task["chat_id"], f"🟥 [{short(task['id'])}] draft/info: {err}")
                    except Exception:
                        log.exception("notify_text failed on draft/info retries")
                    return
                schedule(OPERATION_POLL_INTERVAL_SECONDS)
                return
            if not ok:
                schedule(OPERATION_POLL_INTERVAL_SECONDS)
                return

            task["draft_id"] = draft_id

            chosen_wid = task.get("chosen_warehouse_id")
            if not chosen_wid:
                chosen_wid = choose_warehouse(warehouses)
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

            log.info("Draft ready task=%s draft_id=%s chosen_warehouse_id=%s bundle_id=%s",
                     short(task["id"]), draft_id, task.get("chosen_warehouse_id"), task.get("bundle_id"))

            task["status"] = ST_TIMESLOT_SEARCH
            update_task(task)
            schedule(1)
            return

        if task["status"] == ST_TIMESLOT_SEARCH:
            wid = int(task.get("chosen_warehouse_id") or 0)
            if wid <= 0:
                task["status"] = ST_FAILED
                task["last_error"] = "warehouse not resolved"
                update_task(task)
                try:
                    await notify_text(task["chat_id"], f"🟥 [{short(task['id'])}] Не удалось выбрать склад.")
                except Exception:
                    log.exception("notify_text failed on no warehouse")
                return

            ok, slots, err, status = await api_timeslot_info(api, task["draft_id"], [wid], task["date"], task.get("bundle_id"))

            if status == 429 or (err and str(err).startswith("rate_limit:")):
                try:
                    ra = int(str(err).split(":", 1)[1])
                except Exception:
                    ra = RATE_LIMIT_DEFAULT_COOLDOWN
                wait_sec = max(ra, MIN_CREATE_RETRY_SECONDS)
                _set_retry_after(task, wait_sec)
                task["status"] = "RATE_LIMIT"
                task["last_error"] = "429 Too Many Requests (timeslot)"
                update_task(task)
                schedule(wait_sec)
                return

            if not ok:
                schedule(SLOT_POLL_INTERVAL_SECONDS)
                return

            desired_from = task["desired_from_iso"]; desired_to = task["desired_to_iso"]
            matched = None
            for s in slots:
                if (s.get("from_in_timezone") == desired_from and s.get("to_in_timezone") == desired_to) or \
                   (s.get("from") == desired_from and s.get("to") == desired_to):
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

            task["slot_from"] = matched.get("from_in_timezone") or matched.get("from")
            task["slot_to"] = matched.get("to_in_timezone") or matched.get("to")
            task["slot_id"] = matched.get("id") or matched.get("timeslot_id") or matched.get("slot_id")

            last_upd = int(task.get("updated_ts") or 0)
            if now_ts() - last_upd < MIN_CREATE_RETRY_SECONDS and task.get("supply_create_attempts", 0) > 0:
                schedule(MIN_CREATE_RETRY_SECONDS - (now_ts() - last_upd))
                return
            if task.get("creating"):
                return
            task["creating"] = True
            update_task(task)

            ok, op_id, err, status = await api_supply_create(api, task)
            task["creating"] = False

            if status == 429 or (err and str(err).startswith("rate_limit:")):
                try:
                    ra = int(str(err).split(":", 1)[1])
                except Exception:
                    ra = RATE_LIMIT_DEFAULT_COOLDOWN
                wait_sec = max(ra, MIN_CREATE_RETRY_SECONDS)
                _set_retry_after(task, wait_sec)
                task["status"] = "RATE_LIMIT"
                task["last_error"] = "429 Too Many Requests (supply/create)"
                task["supply_create_attempts"] = int(task.get("supply_create_attempts") or 0) + 1
                update_task(task)
                schedule(wait_sec)
                return

            if not ok:
                if str(err).startswith("http_status:5") or str(err).startswith("http_error:") or status >= 500 or status == 0:
                    delay = max(_inc_backoff(task), MIN_CREATE_RETRY_SECONDS)
                    task["last_error"] = f"supply_create_server_error:{err}"
                    update_task(task)
                    schedule(delay)
                    return
                task["last_error"] = str(err)
                update_task(task)
                schedule(SLOT_POLL_INTERVAL_SECONDS)
                return

            task["supply_operation_id"] = op_id
            task["op_started_ts"] = now_ts()
            task["op_retries"] = 0
            task["status"] = ST_POLL_SUPPLY
            update_task(task)
            schedule(OPERATION_POLL_INTERVAL_SECONDS)
            return

        if task["status"] == ST_POLL_SUPPLY:
            ok, order_id, err, status = await api_supply_create_status(api, task["supply_operation_id"])

            if status == 429 or (err and str(err).startswith("rate_limit:")):
                try:
                    ra = int(str(err).split(":", 1)[1])
                except Exception:
                    ra = RATE_LIMIT_DEFAULT_COOLDOWN
                wait_sec = max(ra, MIN_CREATE_RETRY_SECONDS)
                _set_retry_after(task, wait_sec)
                task["status"] = "RATE_LIMIT"
                task["last_error"] = "429 Too Many Requests (supply/status)"
                update_task(task)
                schedule(wait_sec)
                return

            if err:
                task["op_retries"] = int(task.get("op_retries") or 0) + 1
                if task["op_retries"] > SUPPLY_MAX_OPERATION_RETRIES:
                    task["status"] = ST_FAILED
                    task["last_error"] = str(err)
                    update_task(task)
                    try:
                        await notify_text(task["chat_id"], f"🟥 [{short(task['id'])}] supply/status: {err}")
                    except Exception:
                        log.exception("notify_text failed on supply status")
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

        if task["status"] in (ST_SUPPLY_ORDER_FETCH, ST_ORDER_DATA_FILLING):
            ok, supply_id, number, err, status, meta = await api_supply_order_get(api, task["order_id"])

            if status == 429 or (err and str(err or "").startswith("rate_limit:")):
                try:
                    ra = int(str(err).split(":", 1)[1]) if err else RATE_LIMIT_DEFAULT_COOLDOWN
                except Exception:
                    ra = RATE_LIMIT_DEFAULT_COOLDOWN
                _set_retry_after(task, max(ra, ORDER_FILL_POLL_INTERVAL_SECONDS))
                task["status"] = "RATE_LIMIT"
                task["last_error"] = "429 Too Many Requests (supply-order/get)"
                update_task(task)
                return

            if not ok:
                task["op_retries"] = int(task.get("op_retries") or 0) + 1
                if task["op_retries"] > max(SUPPLY_MAX_OPERATION_RETRIES, ORDER_FILL_MAX_RETRIES):
                    task["status"] = ST_FAILED
                    task["last_error"] = str(err)
                    update_task(task)
                    try:
                        await notify_text(task["chat_id"], f"🟥 [{short(task['id'])}] supply-order/get: {err}")
                    except Exception:
                        log.exception("notify_text failed on supply-order/get")
                    return
                schedule(ORDER_FILL_POLL_INTERVAL_SECONDS)
                return

            task["supply_order_number"] = number or task.get("supply_order_number") or ""
            if meta.get("dropoff_warehouse_id"):
                task["dropoff_warehouse_id"] = int(meta["dropoff_warehouse_id"])
                update_task(task)

            if supply_id or task.get("supply_id"):
                if supply_id:
                    task["supply_id"] = supply_id
                log.info("Supply ready task=%s supply_id=%s order_no=%s", short(task["id"]), task.get("supply_id"), task["supply_order_number"])
                task["status"] = ST_CARGO_PREP if AUTO_CREATE_CARGOES else ST_DONE
                update_task(task)
                schedule(1)
                return

            task["status"] = ST_ORDER_DATA_FILLING
            update_task(task)

            await _auto_fill_timeslot_if_needed(task, api, meta, notify_text)
            await _prompt_missing_fields(task, meta, notify_text)

            schedule(ORDER_FILL_POLL_INTERVAL_SECONDS)
            return

        if task["status"] == ST_CARGO_PREP:
            task["cargo_payload"] = build_cargoes_from_task(task)
            task["status"] = ST_CARGO_CREATING
            update_task(task)
            schedule(1)
            return

        if task["status"] == ST_CARGO_CREATING:
            if task.get("creating"):
                return
            task["creating"] = True
            update_task(task)

            ok, op_id, err, status = await api_cargoes_create(api, task["cargo_payload"])
            task["creating"] = False

            if status == 429 or (err and str(err).startswith("rate_limit:")):
                try:
                    ra = int(str(err).split(":", 1)[1])
                except Exception:
                    ra = RATE_LIMIT_DEFAULT_COOLDOWN
                wait_sec = max(ra, MIN_CREATE_RETRY_SECONDS)
                _set_retry_after(task, wait_sec)
                task["status"] = "RATE_LIMIT"
                task["last_error"] = "429 Too Many Requests (cargoes/create)"
                update_task(task)
                schedule(wait_sec)
                return

            if not ok:
                if str(err).startswith("http_status:5") or str(err).startswith("http_error:") or status >= 500 or status == 0:
                    delay = max(_inc_backoff(task), MIN_CREATE_RETRY_SECONDS)
                    task["last_error"] = f"cargoes_create_server_error:{err}"
                    update_task(task)
                    schedule(delay)
                    return
                task["status"] = ST_FAILED
                task["last_error"] = str(err)
                update_task(task)
                try:
                    await notify_text(task["chat_id"], f"🟥 [{short(task['id'])}] cargoes/create: {err}")
                except Exception:
                    log.exception("notify_text failed on cargoes/create")
                return

            task["cargo_operation_id"] = op_id
            task["op_started_ts"] = now_ts()
            task["op_retries"] = 0
            task["status"] = ST_POLL_CARGO
            update_task(task)
            schedule(OPERATION_POLL_INTERVAL_SECONDS)
            return

        if task["status"] == ST_POLL_CARGO:
            ok, cargoes, err, status = await api_cargoes_create_info(api, task["cargo_operation_id"])

            if status == 429 or (err and str(err).startswith("rate_limit:")):
                try:
                    ra = int(str(err).split(":", 1)[1])
                except Exception:
                    ra = RATE_LIMIT_DEFAULT_COOLDOWN
                wait_sec = max(ra, MIN_CREATE_RETRY_SECONDS)
                _set_retry_after(task, wait_sec)
                task["status"] = "RATE_LIMIT"
                task["last_error"] = "429 Too Many Requests (cargoes/info)"
                update_task(task)
                schedule(wait_sec)
                return

            if err:
                task["op_retries"] += 1
                if task["op_retries"] > SUPPLY_MAX_OPERATION_RETRIES:
                    task["status"] = ST_FAILED
                    task["last_error"] = str(err)
                    update_task(task)
                    try:
                        await notify_text(task["chat_id"], f"🟥 [{short(task['id'])}] cargoes/info: {err}")
                    except Exception:
                        log.exception("notify_text failed on cargoes/info")
                    return
                schedule(OPERATION_POLL_INTERVAL_SECONDS)
                return
            if not ok:
                schedule(OPERATION_POLL_INTERVAL_SECONDS)
                return
            task["cargo_ids"] = [c.get("cargo_id") for c in cargoes if c.get("cargo_id")]
            task["status"] = ST_LABELS_CREATING if AUTO_CREATE_LABELS else ST_DONE
            update_task(task)
            schedule(1)
            return

        if task["status"] == ST_LABELS_CREATING:
            if task.get("creating"):
                return
            task["creating"] = True
            update_task(task)

            ok, op_id, err, status = await api_labels_create(api, task["supply_id"], task.get("cargo_ids"))
            task["creating"] = False

            if status == 429 or (err and str(err).startswith("rate_limit:")):
                try:
                    ra = int(str(err).split(":", 1)[1])
                except Exception:
                    ra = RATE_LIMIT_DEFAULT_COOLDOWN
                wait_sec = max(ra, MIN_CREATE_RETRY_SECONDS)
                _set_retry_after(task, wait_sec)
                task["status"] = "RATE_LIMIT"
                task["last_error"] = "429 Too Many Requests (labels/create)"
                update_task(task)
                schedule(wait_sec)
                return

            if not ok:
                if str(err).startswith("http_status:5") or str(err).startswith("http_error:") or status >= 500 or status == 0:
                    delay = max(_inc_backoff(task), MIN_CREATE_RETRY_SECONDS)
                    task["last_error"] = f"labels_create_server_error:{err}"
                    update_task(task)
                    schedule(delay)
                    return
                task["status"] = ST_FAILED
                task["last_error"] = str(err)
                update_task(task)
                try:
                    await notify_text(task["chat_id"], f"🟥 [{short(task['id'])}] labels/create: {err}")
                except Exception:
                    log.exception("notify_text failed on labels/create")
                return

            task["labels_operation_id"] = op_id
            task["op_started_ts"] = now_ts()
            task["op_retries"] = 0
            task["status"] = ST_POLL_LABELS
            update_task(task)
            schedule(OPERATION_POLL_INTERVAL_SECONDS)
            return

        if task["status"] == ST_POLL_LABELS:
            ok, file_guid, err, status = await api_labels_get(api, task["labels_operation_id"])

            if status == 429 or (err and str(err).startswith("rate_limit:")):
                try:
                    ra = int(str(err).split(":", 1)[1])
                except Exception:
                    ra = RATE_LIMIT_DEFAULT_COOLDOWN
                wait_sec = max(ra, MIN_CREATE_RETRY_SECONDS)
                _set_retry_after(task, wait_sec)
                task["status"] = "RATE_LIMIT"
                task["last_error"] = "429 Too Many Requests (labels/get)"
                update_task(task)
                schedule(wait_sec)
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
                        log.exception("notify_text failed on labels/get")
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
                        log.exception("notify_file failed on labels pdf")
            task["status"] = ST_DONE
            update_task(task)
            try:
                await notify_text(task["chat_id"], f"🟩 [{short(task['id'])}] Готово. supply_id={task.get('supply_id')} cargo={len(task.get('cargo_ids',[]))}")
            except Exception:
                log.exception("notify_text failed on DONE")
            return

    except Exception as e:
        log.exception("advance_task exception")
        task["status"] = ST_FAILED
        task["last_error"] = f"exception:{e}"
        update_task(task)
        try:
            await notify_text(task["chat_id"], f"🟥 [{short(task['id'])}] Исключение: {e}")
        except Exception:
            log.exception("notify_text failed on exception")

# ================== Публичное API ==================

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
        add_task(task); out.append(task)
    return out

# Удобные обертки (возможно вызываются supply_integration)
def purge_all_supplies_now() -> int:
    return purge_all_tasks()

def schedule_supply(raw_text: str, chat_id: int, mode: str = "FBO") -> List[Dict[str, Any]]:
    # Создать задачи из текста (с учетом "Склад: ..."), без немедленного запуска воркера
    return create_tasks_from_template(raw_text, mode, chat_id)

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
        if k.endswith("_operation_id") or k in ("draft_id","order_id","supply_id","supply_order_number","cargo_payload","cargo_ids","labels_file_guid","labels_pdf_path","slot_from","slot_to","slot_id","last_error","op_started_ts","op_retries","retry_after_ts","next_attempt_ts","creating","create_attempts","create_backoff_sec","order_timeslot_set_ok","order_timeslot_set_attempts","order_timeslot_set_ts","need_vehicle","order_vehicle_text","need_contact","order_contact_phone","order_contact_name","need_vehicle_prompt_ts","need_contact_prompt_ts","dropoff_warehouse_id","bundle_id","supply_type","draft_type_variants","draft_type_variant_index","draft_type_variants_tried","winning_draft_variant","draft_variant_last_error","draft_variant_events","draft_variants_total_attempts","draft_variant_cycle_done"):
            t.pop(k, None)
    t["status"] = ST_WAIT_WINDOW
    t["next_attempt_ts"] = 0
    t["retry_after_ts"] = 0
    t["creating"] = False
    t["create_attempts"] = 0
    update_task(t)
    return True, "OK"

# ================== Планировщик / процесс ==================

async def process_tasks(notify_text, notify_file):
    ensure_loaded()
    log.info("process_tasks: START")
    async with _lock:
        api = OzonApi(OZON_CLIENT_ID, OZON_API_KEY, timeout=API_TIMEOUT_SECONDS)
        for task in list_tasks():
            if task.get("status") == ST_CANCELED:
                continue
            await advance_task(task, api, notify_text, notify_file)
    log.info("process_tasks: END")

# ================== HANDLERS REGISTRATION (aiogram v3 + v2 fallback) ==================

def setup_supply_handlers(dp, scheduler=None):
    """
    Регистрирует команды/хендлеры для Telegram (aiogram v3 + v2 fallback)
    """
    global _dp_ref
    _dp_ref = dp

    import re as _re
    from datetime import datetime as _dt, timedelta as _td
    HEADER_PREFIX_RE = _re.compile(r"^\s*На\s+\d{2}\.\d{2}\.\d{4},", _re.IGNORECASE)

    async def supply_debug(message):
        all_tasks = list_all_tasks(); active = list_tasks()
        done = [t for t in all_tasks if t.get("status") == ST_DONE]
        failed = [t for t in all_tasks if t.get("status") == ST_FAILED]
        await message.answer(
            "Supply debug:\n"
            f" total={len(all_tasks)} active={len(active)} done={len(done)} failed={len(failed)}\n"
            f" file={SUPPLY_TASK_FILE}"
        )

    def _fmt_task_line(t: Dict[str, Any]) -> str:
        sid = short(t.get("id", "?")); st = t.get("status")
        sku_part = ""
        lst = t.get("sku_list") or []
        if lst: sku_part = f" sku={lst[0].get('sku')}"
        return f"{sid}: {st}{sku_part}"

    def _dump_task(t: Dict[str, Any]) -> str:
        keys_interest = [
            "id","status","date","desired_from_iso","desired_to_iso",
            "draft_operation_id","draft_id","chosen_warehouse_id","bundle_id",
            "winning_draft_variant","draft_type_variant_index","draft_type_variants_tried",
            "draft_variant_last_error","supply_type",
            "supply_operation_id","order_id","supply_id","supply_order_number",
            "slot_id","slot_from","slot_to",
            "cargo_ids","labels_pdf_path","last_error","next_attempt_ts","retry_after_ts"
        ]
        lines = []
        for k in keys_interest:
            if k in t: lines.append(f"{k}: {t[k]}")
        if t.get("draft_variant_events"):
            lines.append(f"draft_variant_events: {len(t['draft_variant_events'])} entries")
        return "\n".join(lines)

    async def supply_list_cmd(message):
        active = list_tasks()
        if not active:
            await message.answer("Нет активных задач."); return
        lines = ["Активные задачи:"]
        for t in active[:60]:
            lines.append(_fmt_task_line(t))
        await message.answer("\n".join(lines))

    async def supply_dump_cmd(message):
        parts = (message.text or "").strip().split()
        if len(parts) < 2:
            await message.answer("Usage: /supply_dump <task_id|prefix>"); return
        q = parts[1].lower()
        target = None
        for t in list_all_tasks():
            tid = str(t.get("id", "")).lower()
            if tid == q or tid.startswith(q):
                target = t; break
        if not target:
            await message.answer("Не найдено."); return
        await message.answer("Dump:\n" + _dump_task(target))

    async def supply_cancel_cmd(message):
        parts = (message.text or "").strip().split()
        if len(parts) < 2:
            await message.answer("Usage: /supply_cancel <task_id|prefix>"); return
        prefix = parts[1].lower()
        target = None
        for t in list_all_tasks():
            tid = str(t.get("id","")).lower()
            if tid == prefix or tid.startswith(prefix):
                target = t; break
        if not target:
            await message.answer("Не найдено."); return
        cancel_task(target["id"])
        await message.answer(f"Отменено: {short(target['id'])}")

    async def supply_retry_cmd(message):
        parts = (message.text or "").strip().split()
        if len(parts) < 2:
            await message.answer("Usage: /supply_retry <task_id|prefix>"); return
        prefix = parts[1].lower()
        target = None
        for t in list_all_tasks():
            tid = str(t.get("id","")).lower()
            if tid == prefix or tid.startswith(prefix):
                target = t; break
        if not target:
            await message.answer("Не найдено."); return
        ok, info = retry_task(target["id"])
        await message.answer(f"retry: {info} ({short(target['id'])})")

    async def supply_purge_cmd(message):
        removed = purge_tasks()
        await message.answer(f"Пург старых: {removed}")

    async def supply_purge_all_cmd(message):
        removed = purge_all_tasks()
        await message.answer(f"Удалено полностью: {removed}")

    async def supply_new_cmd(message):
        now_local = _dt.now(TZ_YEKAT)
        start_local = (now_local + _td(minutes=5)).strftime("%H:%M")
        end_local = (now_local + _td(hours=1)).strftime("%H:%M")
        template = f"На {now_local.strftime('%d.%m.%Y')}, {start_local}-{end_local}\nСклад: Склад\n2625768907 — количество 10, 1 коробка, по 10 шт"
        tasks = create_tasks_from_template(template, "FBO", message.chat.id)
        await message.answer(f"Создано {len(tasks)} задач. Первая: {short(tasks[0]['id'])}")

    async def supply_tick_cmd(message):
        bot = message.bot
        async def _notify_text(chat_id: int, text: str):
            await bot.send_message(chat_id, text)
        async def _notify_file(chat_id: int, file_path: str, caption: str = ""):
            try:
                with open(file_path, "rb") as f:
                    await bot.send_document(chat_id, f, caption=caption or None)
            except Exception:
                log.exception("notify_file failed")
        await process_tasks(_notify_text, _notify_file)
        await message.answer("tick done")

    async def text_fallback(message):
        txt = message.text or ""
        if HEADER_PREFIX_RE.search(txt) or WAREHOUSE_LINE_RE.search(txt):
            try:
                tasks = create_tasks_from_template(txt, "FBO", message.chat.id)
                await message.answer(f"Создано задач: {len(tasks)}. Первый id={short(tasks[0]['id'])}")
                bot = message.bot
                async def _notify_text(chat_id: int, text: str):
                    await bot.send_message(chat_id, text)
                async def _notify_file(chat_id: int, file_path: str, caption: str = ""):
                    try:
                        with open(file_path, "rb") as f:
                            await bot.send_document(chat_id, f, caption=caption or None)
                    except Exception:
                        log.exception("notify_file failed")
                await process_tasks(_notify_text, _notify_file)
            except Exception as e:
                await message.answer(f"Ошибка парсинга: {e}")

    if hasattr(dp, "include_router"):
        # aiogram v3
        from aiogram import Router, F
        from aiogram.filters import Command
        router = Router()
        router.message.register(supply_debug, Command("supply_debug"))
        router.message.register(supply_list_cmd, Command("supply_list"))
        router.message.register(supply_dump_cmd, Command("supply_dump"))
        router.message.register(supply_cancel_cmd, Command("supply_cancel"))
        router.message.register(supply_retry_cmd, Command("supply_retry"))
        router.message.register(supply_purge_cmd, Command("supply_purge"))
        router.message.register(supply_purge_all_cmd, Command("supply_purge_all"))
        router.message.register(supply_new_cmd, Command("supply_new"))
        router.message.register(supply_tick_cmd, Command("supply_tick"))
        router.message.register(text_fallback, F.text.regexp(HEADER_PREFIX_RE))
        dp.include_router(router)
    else:
        # aiogram v2 fallback
        try:
            dp.register_message_handler(supply_debug, commands=["supply_debug"])
            dp.register_message_handler(supply_list_cmd, commands=["supply_list"])
            dp.register_message_handler(supply_dump_cmd, commands=["supply_dump"])
            dp.register_message_handler(supply_cancel_cmd, commands=["supply_cancel"])
            dp.register_message_handler(supply_retry_cmd, commands=["supply_retry"])
            dp.register_message_handler(supply_purge_cmd, commands=["supply_purge"])
            dp.register_message_handler(supply_purge_all_cmd, commands=["supply_purge_all"])
            dp.register_message_handler(supply_new_cmd, commands=["supply_new"])
            dp.register_message_handler(supply_tick_cmd, commands=["supply_tick"])
            dp.register_message_handler(text_fallback, content_types=["text"])
        except Exception:
            log.exception("aiogram v2 registration failed")

    # Планировщик (опционально)
    if scheduler:
        try:
            existing = [j for j in scheduler.get_jobs() if j.id == "supply_process"]
            if not existing:
                scheduler.add_job(
                    lambda: asyncio.create_task(_scheduled_tick()),
                    "interval",
                    seconds=SUPPLY_WORKER_INTERVAL_SECONDS,
                    id="supply_process",
                    replace_existing=True,
                    max_instances=1,
                    coalesce=True
                )
                log.info("setup_supply_handlers: добавлен job supply_process (%ss)", SUPPLY_WORKER_INTERVAL_SECONDS)
            else:
                log.info("setup_supply_handlers: job supply_process уже существует")
        except Exception:
            log.exception("scheduler setup failed")

# ================== Фоновый воркер (ensure_worker_running / worker_status / stop_worker) ==================

async def _worker_loop():
    global _worker_started
    _worker_started = True
    log.info("supply_watch worker: started, interval=%ss", SUPPLY_WORKER_INTERVAL_SECONDS)
    try:
        while True:
            try:
                await _scheduled_tick()
            except Exception:
                log.exception("worker tick failed")
            await asyncio.sleep(max(2, SUPPLY_WORKER_INTERVAL_SECONDS))
    except asyncio.CancelledError:
        log.info("supply_watch worker: cancelled")
        raise
    except Exception:
        log.exception("supply_watch worker: crashed")
    finally:
        _worker_started = False
        log.info("supply_watch worker: stopped")

def ensure_worker_running():
    """
    Запускает фоновый воркер process_tasks, если он еще не запущен.
    Вызывать один раз при старте (bot.py так и делает).
    """
    global _worker_task
    loop = asyncio.get_running_loop()
    if _worker_task and not _worker_task.done():
        log.info("supply_watch worker: already running")
        return
    _worker_task = loop.create_task(_worker_loop())
    log.info("supply_watch worker: created")

def worker_status() -> str:
    """
    Возвращает строковый статус фонового воркера для логирования:
    'not-started' | 'running' | 'cancelled' | 'done' | 'done:<exc>'.
    """
    t = _worker_task
    if t is None:
        return "not-started"
    if t.cancelled():
        return "cancelled"
    if t.done():
        try:
            exc = t.exception()
        except Exception:
            exc = None
        return "done" if exc is None else f"done:{exc}"
    return "running"

async def stop_worker():
    """
    Останавливает фоновый воркер (если запущен).
    """
    global _worker_task
    if _worker_task and not _worker_task.done():
        _worker_task.cancel()
        try:
            await _worker_task
        except asyncio.CancelledError:
            pass
    _worker_task = None

# ================== Standalone (опционально) ==================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    async def _main():
        async def _notify_text(cid, txt):
            print(f"[notify {cid}] {txt}")
        async def _notify_file(cid, path, cap=""):
            print(f"[notify_file {cid}] {path} {cap}")
        now_local = datetime.now(TZ_YEKAT)
        template = f"На {now_local.strftime('%d.%m.%Y')}, 10:00-11:00\nСклад: УФА_РФЦ\n2625768907 — количество 10, 1 коробка, по 10 шт"
        tasks = create_tasks_from_template(template, "CROSSDOCK", 0)
        print("Created:", [short(t["id"]) for t in tasks])
        await process_tasks(_notify_text, _notify_file)

    asyncio.run(_main())