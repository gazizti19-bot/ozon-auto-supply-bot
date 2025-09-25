# -*- coding: utf-8 -*-
"""
supply_watch.py
Оркестратор автопоставок для Ozon Seller API:
WAIT_WINDOW -> DRAFT_CREATING -> POLL_DRAFT -> TIMESLOT_SEARCH -> SUPPLY_CREATING ->
POLL_SUPPLY -> SUPPLY_ORDER_FETCH -> (ORDER_DATA_FILLING) -> CARGO_PREP -> CARGO_CREATING ->
POLL_CARGO -> LABELS_CREATING -> POLL_LABELS -> DONE/FAILED/CANCELED

Ключевые улучшения:
- Парсер шаблона устойчив к разным тире/пробелам/словоформам; добавлен fallback‑парсер.
- Заголовок матчится даже при пробелах вокруг тире (08:00 - 09:00).
- Сохраняем тип поставки (supply_type). При наличии drop‑off используем CREATE_TYPE_CROSSDOCK.
- Для crossdock не запрашиваем транспорт.
- Тайм‑слот: приоритет appointment/set; быстрый скип 404 page not found; альтернативные пути по supply_id;
  лимит попыток на один вызов.
- Если supply_id уже есть — немедленный переход в CARGO_PREP.
- Груз: на каждую коробку отдельный BOX с количеством per_box.
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
SUPPLY_CLUSTER_IDS_RAW = _getenv_str("SUPPLY_CLUSTER_IDS", "").strip()
SUPPLY_WAREHOUSE_MAP_RAW = _getenv_str("SUPPLY_WAREHOUSE_MAP", "").strip()

RATE_LIMIT_DEFAULT_COOLDOWN = _getenv_int("RATE_LIMIT_DEFAULT_COOLDOWN", 60)
CREATE_INITIAL_BACKOFF = _getenv_int("CREATE_INITIAL_BACKOFF", 2)
CREATE_MAX_BACKOFF = _getenv_int("CREATE_MAX_BACKOFF", 120)
MIN_CREATE_RETRY_SECONDS = _getenv_int("MIN_CREATE_RETRY_SECONDS", 180)

# Для блока DATA_FILLING
ORDER_FILL_POLL_INTERVAL_SECONDS = _getenv_int("ORDER_FILL_POLL_INTERVAL_SECONDS", 60)
ORDER_FILL_MAX_RETRIES = _getenv_int("ORDER_FILL_MAX_RETRIES", 150)

# Антиспам подсказок пользователю (сек)
PROMPT_MIN_INTERVAL = _getenv_int("PROMPT_MIN_INTERVAL", 120)

# Можно включить старые пути сеттера таймслота (обычно не нужно)
SUPPLY_ALLOW_LEGACY_SET_TIMESLOT = _getenv_bool("SUPPLY_ALLOW_LEGACY_SET_TIMESLOT", False)

# Drop-off warehouse id из окружения
DROP_OFF_WAREHOUSE_ID = _getenv_int("DROP_ID", 0)

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

# Asia/Yekaterinburg (UTC+5)
TZ_YEKAT = timezone(timedelta(hours=5))

# ================== Хранилище задач ==================

_tasks_loaded = False
_tasks: List[Dict[str, Any]] = []
_lock = asyncio.Lock()
_sync_lock = threading.RLock()

ID_FIELDS = ("id", "task_id", "uuid", "pk", "external_id")

STATUS_FIELDS = [
    "status", "state", "phase", "stage", "step",
    "pipeline_status", "current_status", "progress", "phase_name",
]

BOOKING_FIELDS = (
    "draft_id", "order_id", "order_number", "number", "supply_order_number",
    "supply_id", "from_in_timezone", "to_in_timezone", "warehouse_id", "drop_off_id",
    "timeslot", "result",
)

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
    """
    Полностью очистить список задач и удалить файл.
    """
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
            target = {
                "id": tid,
                "status": ST_TIMESLOT_SEARCH,
                "created_ts": now_ts(),
                "updated_ts": now_ts(),
            }
            for k in ("task_id", "uuid"):
                target.setdefault(k, tid)
            _tasks.append(target)

        changed = False
        changed |= _apply_status_fields(target, payload)
        changed |= _apply_booking_fields(target, payload)
        changed |= _apply_generic_fields(target, payload)

        # Автопереходы (резервная защита)
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
    """
    Возвращает только активные задачи (без DONE/FAILED/CANCELED).
    """
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

# ================== Утилиты (время/форматы) ==================

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
    """
    Z/UTC -> локальная TZ (TZ_YEKAT) в isoformat().
    """
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

# ================== Утилиты (парсинг шаблона) ==================

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

# Класс “тире/минусов”, включающий распространённые варианты
DASH_CLASS = r"[\-\u2010\u2011\u2012\u2013\u2014\u2015\u2212]"

# Заголовок допускает пробелы вокруг тире между временем
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

def _canon_spaces_and_dashes(s: str) -> str:
    """
    Нормализация:
    - NBSP/узкие пробелы -> обычный пробел
    - разные виды “тире” -> обычный '-'
    - схлопывание пробелов/окружения тире
    """
    if not isinstance(s, str):
        return s
    # заменяем неразрывные/узкие пробелы
    s = s.replace("\u00A0", " ").replace("\u2009", " ").replace("\u202F", " ")
    # разные тире -> '-'
    for ch in ["\u2010", "\u2011", "\u2012", "\u2013", "\u2014", "\u2015", "\u2212"]:
        s = s.replace(ch, "-")
    # пробелы вокруг '-'
    s = re.sub(r"\s*-\s*", " - ", s)
    # схлопываем множественные пробелы
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

def _parse_item_line(raw_line: str) -> Optional[Dict[str, Any]]:
    """
    Пытаемся распарсить строку позиции. Сначала основным правилом, затем fallback‑эвристикой.
    """
    line = _canon_spaces_and_dashes(raw_line)
    m2 = LINE_RE.match(line)
    if m2:
        sku = m2.group("sku").strip()
        qty = int(m2.group("qty"))
        boxes = int(m2.group("boxes"))
        per_box = int(m2.group("per"))
        wh = (m2.group("wh") or "").strip().rstrip(".")
        return {"sku": sku, "total_qty": qty, "boxes": boxes, "per_box": per_box, "warehouse_name": wh}

    # Fallback: более свободный парсинг
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

def parse_template(text: str) -> Tuple[str, str, str, List[Dict[str, Any]]]:
    """
    Ожидаемый формат:
    На DD.MM.YYYY, HH:MM-HH:MM
    <SKU> — количество <QTY>, <BOXES> коробки/коробок, по <PER> шт, <СКЛАД>
    Поддерживает ; вместо , и разные варианты тире и словоформ.
    Заголовок ищется в любом месте текста.
    """
    if not text or not isinstance(text, str):
        raise ValueError("Пустой шаблон.")

    # Нормализуем весь текст
    text = "\n".join(_canon_spaces_and_dashes(line) for line in text.splitlines())
    lines = [l.rstrip() for l in text.splitlines() if l.strip()]
    if not lines:
        raise ValueError("Пустой шаблон.")

    # Ищем заголовок среди всех строк
    header_idx, date_iso, start_hhmm, end_hhmm = _scan_header(lines)

    # Позиции — после строки заголовка
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
        return {
            "Client-Id": self.client_id,
            "Api-Key": self.api_key,
            "Content-Type": "application/json"
        }

    async def request(self, method: str, path: str, payload: Dict[str, Any]) -> Tuple[bool, Any, str, int, Dict[str, str]]:
        if self.mock:
            logging.info("MOCK %s %s payload_keys=%s", method, path, list(payload.keys()))
            return True, {"mock": True, "path": path, "payload": payload}, "", 200, {}

        url = f"{self.base}{path}"
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            try:
                r = await client.request(method=method, url=url, headers=self.headers(), json=payload)
                status = r.status_code
                headers = {k: v for k, v in r.headers.items()}
                snippet = (r.text or "")[:400]
                logging.info("HTTP %s %s -> %s, status=%s, resp=%r", method, path, url, status, snippet)
                try:
                    data = r.json()
                except Exception:
                    data = {"text": r.text}
                ok = 200 <= status < 300
                if ok:
                    return True, data, "", status, headers
                return False, data, f"http_status:{status}", status, headers
            except Exception as e:
                logging.exception("HTTP %s %s failed: %s", method, path, e)
                return False, {"error": str(e)}, f"http_error:{e}", 0, {}

    async def post(self, path: str, payload: Dict[str, Any]) -> Tuple[bool, Any, str, int, Dict[str, str]]:
        return await self.request("POST", path, payload)

    async def get_pdf(self, path: str) -> Tuple[bool, bytes, str]:
        if self.mock:
            logging.info("MOCK GET %s (PDF)", path)
            return True, b"%PDF-1.4\n% Fake PDF\n", ""
        url = f"{self.base}{path}"
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            try:
                r = await client.get(url, headers=self.headers())
                r.raise_for_status()
                return True, r.content, ""
            except httpx.HTTPError as e:
                logging.exception("HTTP GET %s failed: %s", path, e)
                return False, b"", f"http_error:{e}"

# ================== DRAFT: create/info ==================

def _extract_dropoff_id(task: Dict[str, Any]) -> Optional[int]:
    keys = [
        "drop_off_point_warehouse_id",
        "dropoff_warehouse_id",
        "dropoffWarehouseId",
        "drop_off_id",
        "warehouse_id",
    ]
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

async def api_draft_create(api: OzonApi, task: Dict[str, Any]) -> Tuple[bool, Optional[str], Optional[str], int]:
    """
    Создание драфта. Определяем тип поставки и сохраняем его в задаче (task['supply_type']).
    """
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

    # Сохраним тип для логики DATA_FILLING (crossdock => не просим транспорт)
    task["supply_type"] = used_type
    update_task(task)

    attempt = 0
    backoff = 1.9
    while True:
        ok, data, err, status, headers = await api.post("/v1/draft/create", payload)
        if status == 429:
            attempt += 1
            if attempt >= 3:
                ra = _parse_retry_after(headers)
                return False, None, f"rate_limit:{ra}", status
            logging.warning("Ozon 429 for POST /v1/draft/create -> wait %.2fs (attempt %s)", backoff, attempt)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 8.0)
            continue
        break

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

async def api_timeslot_info(
    api: OzonApi,
    draft_id: str,
    warehouse_ids: List[int],
    date_iso: str,
    bundle_id: Optional[str] = None
) -> Tuple[bool, List[Dict[str, Any]], str, int]:
    start_local, end_local = day_range_local(date_iso)

    endpoints = [
        "/v1/draft/timeslot/info",
        "/v2/draft/timeslot/info",
    ]

    base = {
        "draft_id": draft_id,
        "warehouse_ids": [int(w) for w in warehouse_ids],
    }
    variants: List[Dict[str, Any]] = [
        dict(base, date_from=start_local, date_to=end_local),
        dict(base, from_in_timezone=start_local, to_in_timezone=end_local),
    ]
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
                if not isinstance(s, dict):
                    continue
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
            logging.info("timeslot_info: ep=%s status=%s keys=%s", ep, status, list(pl.keys()))
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
    """
    Универсальный supply/create: пробуем несколько схем тела, ретраи 429, дедупликация payload.
    """
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
    variants.append(dict(base, warehouse_ids=[wid]))

    if drop_id:
        variants.append(dict(base, warehouse_id=wid, drop_off_point_warehouse_id=int(drop_id)))
        variants.append(dict(base, warehouse_ids=[wid], drop_off_point_warehouse_id=int(drop_id)))

    if slot_id is not None:
        variants.append(dict(base, warehouse_id=wid, timeslot_id=slot_id))
        if drop_id:
            variants.append(dict(base, warehouse_id=wid, timeslot_id=slot_id, drop_off_point_warehouse_id=int(drop_id)))

    if bundle_id:
        variants.append(dict(base, warehouse_id=wid, bundle_id=bundle_id))
        variants.append(dict(base, warehouse_id=wid, bundle_ids=[bundle_id]))
        if drop_id:
            variants.append(dict(base, warehouse_id=wid, bundle_id=bundle_id, drop_off_point_warehouse_id=int(drop_id)))

    uniq: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for p in variants:
        key = json.dumps(p, sort_keys=True, ensure_ascii=False)
        if key in seen:
            continue
        seen.add(key)
        uniq.append(p)

    attempt_429 = 0
    backoff = 1.7

    last_data: Any = None
    last_err: str = ""
    last_status: int = 400

    for pl in uniq:
        while True:
            ok, data, err, status, headers = await api.post("/v1/draft/supply/create", pl)
            logging.info("supply_create: status=%s keys=%s warehouse_id=%s",
                         status, list(pl.keys()), pl.get("warehouse_id") or (pl.get("warehouse_ids") or [None])[0])
            if status == 429:
                attempt_429 += 1
                if attempt_429 > 3:
                    ra = _parse_retry_after(headers)
                    return False, None, f"rate_limit:{ra}", status
                logging.warning("Ozon 429 for POST /v1/draft/supply/create -> wait %.2fs (attempt %s)", backoff, attempt_429)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 12.0)
                continue
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
        ra = _parse_retry_after(headers)
        return False, None, f"rate_limit:{ra}", status

    if not ok:
        return False, None, f"supply_status_error:{err}|{data}", status

    raw_status = str(data.get("status") or (data.get("result") or {}).get("status") or "").strip()
    s_norm = raw_status.lower()

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
                    sid = _first_supply_id(first)
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
    endpoints = ["/v2/supply-order/get", "/v1/supply-order/get"]
    try:
        oid = int(str(order_id).strip())
    except Exception:
        oid = str(order_id).strip()

    payloads_v2 = [{"order_ids": [oid]},
                   {"supply_order_ids": [oid]},
                   {"order_id": oid}, {"supply_order_id": oid}]
    payloads_v1 = [{"order_id": oid}, {"supply_order_id": oid}]

    last_err = None
    last_status = 0

    for pl in payloads_v2:
        ok, data, err, status, headers = await api.post(endpoints[0], pl)
        if status == 429:
            ra = _parse_retry_after(headers)
            return False, None, None, f"rate_limit:{ra}", status, {}
        if ok:
            sid, num = _extract_supply_order_info_from_response(data)
            meta = _extract_order_meta(data)
            return True, sid, (num or ""), None, status, meta
        else:
            last_err, last_status = f"supply_order_get_error:{err}|{data}", status

    for pl in payloads_v1:
        ok, data, err, status, headers = await api.post(endpoints[1], pl)
        if status == 429:
            ra = _parse_retry_after(headers)
            return False, None, None, f"rate_limit:{ra}", status, {}
        if ok:
            sid, num = _extract_supply_order_info_from_response(data)
            meta = _extract_order_meta(data)
            return True, sid, (num or ""), None, status, meta
        else:
            last_err, last_status = f"supply_order_get_error:{err}|{data}", status

    return False, None, None, (last_err or "supply_order_get_error:unknown"), (last_status or 400), {}

# ================== DATA_FILLING actions ==================

async def api_supply_order_timeslot_set(
    api: OzonApi,
    order_id: Any,
    from_iso: str,
    to_iso: str,
    slot_id: Optional[Any] = None,
    dropoff_warehouse_id: Optional[int] = None,
    supply_id: Optional[Any] = None
) -> Tuple[bool, Optional[str], int]:
    """
    Ставит тайм-слот на заявке/поставке.
    - Приоритет appointment/set; затем draft/supply/appointment/set; затем update и (опц.) legacy timeslot/set.
    - Быстрый скип 404 page not found по каждому эндпоинту.
    - Если есть supply_id, пробуем пути /v1|v2/supply/appointment/set (без -order).
    - Ограничение числа попыток за один вызов.
    """
    def _to_num_or_str(x: Any) -> Any:
        try:
            return int(str(x).strip())
        except Exception:
            return str(x).strip()

    oid = _to_num_or_str(order_id)
    sid = _to_num_or_str(supply_id) if supply_id is not None else None

    endpoints = [
        "/v1/supply-order/appointment/set",
        "/v2/supply-order/appointment/set",
        "/v1/draft/supply/appointment/set",
        "/v2/draft/supply/appointment/set",
        "/v1/supply-order/update",
        "/v2/supply-order/update",
    ]
    # Альтернативные пути (по supply_id)
    if sid is not None:
        endpoints = [
            "/v1/supply/appointment/set",
            "/v2/supply/appointment/set",
        ] + endpoints

    if SUPPLY_ALLOW_LEGACY_SET_TIMESLOT:
        endpoints += [
            "/v2/supply-order/timeslot/set",
            "/v1/supply-order/timeslot/set",
            "/v1/supply-order/set-timeslot",
        ]

    methods = ["POST", "PUT", "PATCH"]

    from_variants = [from_iso]
    to_variants = [to_iso]
    zf, zt = _to_z_iso(from_iso), _to_z_iso(to_iso)
    if zf and zf not in from_variants: from_variants.append(zf)
    if zt and zt not in to_variants: to_variants.append(zt)

    base_bodies: List[Dict[str, Any]] = []
    for fr in from_variants:
        for to in to_variants:
            base_bodies.extend([
                {"timeslot": {"from": fr, "to": to}},
                {"timeslot": {"from_in_timezone": fr, "to_in_timezone": to}},
                {"value": {"timeslot": {"from": fr, "to": to}}},
                {"update": {"timeslot": {"from": fr, "to": to}}},
                {"from_in_timezone": fr, "to_in_timezone": to},
                {"appointment": {"from": fr, "to": to}},
                {"appointment": {"from_in_timezone": fr, "to_in_timezone": to}},
            ])
    if slot_id is not None:
        base_bodies.extend([
            {"timeslot": {"id": slot_id}},
            {"value": {"timeslot": {"id": slot_id}}},
            {"update": {"timeslot": {"id": slot_id}}},
            {"timeslot_id": slot_id},
            {"appointment": {"id": slot_id}},
        ])

    bodies: List[Dict[str, Any]] = []
    for b in base_bodies:
        bodies.append(dict(b))
        if dropoff_warehouse_id:
            for top_key in ("dropoff_warehouse_id", "warehouse_id", "drop_off_point_warehouse_id"):
                bb = dict(b); bb[top_key] = dropoff_warehouse_id; bodies.append(bb)
            bb = dict(b); bb.setdefault("update", {}); bb["update"]["dropoff_warehouse_id"] = dropoff_warehouse_id; bodies.append(bb)
            bb = dict(b); bb.setdefault("update", {}); bb["update"]["warehouse_id"] = dropoff_warehouse_id; bodies.append(bb)

    id_variants: List[Dict[str, Any]] = [{"order_id": oid}, {"supply_order_id": oid}, {"id": oid}]
    if sid is not None:
        id_variants = [{"supply_id": sid}, {"id": sid}] + id_variants

    payloads: List[Dict[str, Any]] = []
    for idv in id_variants:
        for b in bodies:
            p = dict(idv); p.update(b); payloads.append(p)

    total_attempts = 0
    MAX_ATTEMPTS_PER_CALL = 60

    for ep in endpoints:
        endpoint_route_missing = False
        for method in methods:
            for pl in payloads:
                total_attempts += 1
                if total_attempts > MAX_ATTEMPTS_PER_CALL:
                    return False, "timeslot_set_too_many_attempts", 429
                ok, data, err, status, headers = await api.request(method, ep, pl)
                logging.info("timeslot_set: method=%s ep=%s status=%s keys=%s", method, ep, status, list(pl.keys()))
                if status == 404 and isinstance(data, dict) and "text" in data and "page not found" in str(data["text"]).lower():
                    endpoint_route_missing = True
                    logging.info("timeslot_set: skip endpoint %s due to 404 page not found", ep)
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
        oid_int = int(str(order_id).strip())
        oid = oid_int
    except Exception:
        oid = str(order_id).strip()

    endpoints = [
        "/v2/supply-order/update",
        "/v1/supply-order/update",
        "/v2/supply-order/vehicle/set",
        "/v1/supply-order/vehicle/set",
        "/v1/supply-order/set-vehicle",
    ]
    methods = ["POST", "PUT", "PATCH"]
    variants = [
        {"vehicle": {"number": vehicle_text}},
        {"vehicle": {"plate": vehicle_text}},
        {"vehicle": {"text": vehicle_text}},
        {"update": {"vehicle": {"number": vehicle_text}}},
        {"update": {"vehicle": {"plate": vehicle_text}}},
        {"update": {"vehicle": {"text": vehicle_text}}},
        {"transport": {"text": vehicle_text}},
        {"update": {"transport": {"text": vehicle_text}}},
    ]
    id_variants = [{"order_id": oid}, {"supply_order_id": oid}, {"id": oid}]
    payloads: List[Dict[str, Any]] = []
    for idv in id_variants:
        for b in variants:
            p = dict(idv); p.update(b); payloads.append(p)

    for ep in endpoints:
        for method in methods:
            for pl in payloads:
                ok, data, err, status, headers = await api.request(method, ep, pl)
                if status == 429:
                    ra = _parse_retry_after(headers)
                    return False, f"rate_limit:{ra}", status
                if ok:
                    return True, None, status
    return False, "vehicle_set_failed", 404

async def api_supply_order_set_contact(api: OzonApi, order_id: Any, phone: str, name: str = "") -> Tuple[bool, Optional[str], int]:
    try:
        oid_int = int(str(order_id).strip())
        oid = oid_int
    except Exception:
        oid = str(order_id).strip()

    endpoints = [
        "/v2/supply-order/update",
        "/v1/supply-order/update",
        "/v2/supply-order/contact/set",
        "/v1/supply-order/contact/set",
        "/v1/supply-order/set-contact",
    ]
    methods = ["POST", "PUT", "PATCH"]
    variants = [
        {"contact": {"phone": phone, "name": name}},
        {"responsible": {"phone": phone, "name": name}},
        {"update": {"contact": {"phone": phone, "name": name}}},
        {"update": {"responsible": {"phone": phone, "name": name}}},
        {"phone": phone, "name": name},
    ]
    id_variants = [{"order_id": oid}, {"supply_order_id": oid}, {"id": oid}]
    payloads: List[Dict[str, Any]] = []
    for idv in id_variants:
        for b in variants:
            p = dict(idv); p.update(b); payloads.append(p)

    for ep in endpoints:
        for method in methods:
            for pl in payloads:
                ok, data, err, status, headers = await api.request(method, ep, pl)
                if status == 429:
                    ra = _parse_retry_after(headers)
                    return False, f"rate_limit:{ra}", status
                if ok:
                    return True, None, status
    return False, "contact_set_failed", 404

# ================== LABELS/CARGO API ==================

async def api_cargoes_create(api: OzonApi, payload: Dict[str, Any]) -> Tuple[bool, Optional[str], Optional[str], int]:
    ok, data, err, status, headers = await api.post("/v1/cargoes/create", payload)
    if status == 429:
        ra = _parse_retry_after(headers)
        return False, None, f"rate_limit:{ra}", status
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
        return False, [], f"rate_limit:{ra}", status
    if not ok:
        return False, [], f"cargoes_info_error:{err}|{data}", status
    s = (data.get("status") or data.get("result", {}).get("status") or "").upper()
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
        return False, None, f"rate_limit:{ra}", status
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
        return False, None, f"rate_limit:{ra}", status
    if not ok:
        return False, None, f"labels_get_error:{err}|{data}", status
    s = (data.get("status") or data.get("result", {}).get("status") or "").upper()
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

# ================== Логика ==================

def build_cargoes_from_task(task: Dict[str, Any]) -> Dict[str, Any]:
    """
    Формируем грузовой список:
    - На каждую коробку отдельный BOX
    - Внутри BOX — позиция SKU с количеством per_box
    """
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
                "type": "BOX",
                "items": [{"sku": sku, "quantity": per_box}]
            })
    return {
        "supply_id": supply_id,
        "delete_current_version": True,
        "cargoes": cargoes
    }

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

    ok, err, status = await api_supply_order_timeslot_set(
        api,
        task["order_id"],
        fr,
        to,
        task.get("slot_id"),
        task.get("dropoff_warehouse_id"),
        task.get("supply_id")
    )
    task["order_timeslot_set_attempts"] = int(task.get("order_timeslot_set_attempts") or 0) + 1
    task["order_timeslot_set_ts"] = now_ts()
    if ok:
        task["order_timeslot_set_ok"] = True
        update_task(task)
        try:
            await notify_text(task["chat_id"], f"🟦 [{short(task['id'])}] Тайм‑слот проставлен в заявке.")
        except Exception:
            logging.exception("notify_text failed on timeslot set")
        return True
    else:
        task["last_error"] = f"timeslot_set:{err or status}"
        update_task(task)
        try:
            await notify_text(task["chat_id"], f"🟨 [{short(task['id'])}] Не удалось проставить тайм‑слот (попробуем позже).")
        except Exception:
            logging.exception("notify_text failed on timeslot fail")
        return False

async def _prompt_missing_fields(task: Dict[str, Any], meta: Dict[str, Any], notify_text) -> bool:
    """
    Подсказываем пользователю недостающие данные.
    Для crossdock транспорт НЕ запрашиваем.
    """
    asked = False
    veh = meta.get("vehicle") or {}
    cnt = meta.get("contact") or {}

    now = now_ts()
    is_crossdock = (str(task.get("supply_type") or "").upper() == "CREATE_TYPE_CROSSDOCK") or bool(
        task.get("dropoff_warehouse_id") or task.get("drop_off_point_warehouse_id") or task.get("drop_off_id") or DROP_OFF_WAREHOUSE_ID
    )

    # Транспорт: только если НЕ crossdock
    if veh.get("required") and not is_crossdock and not task.get("order_vehicle_text"):
        if now - int(task.get("need_vehicle_prompt_ts") or 0) >= PROMPT_MIN_INTERVAL:
            task["need_vehicle"] = True
            task["need_vehicle_prompt_ts"] = now
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
        # crossdock или не требуется — гарантированно не просим
        if task.get("need_vehicle"):
            task["need_vehicle"] = False
            update_task(task)

    # Контакт — только если реально требуется
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
                logging.exception("notify_text failed on contact prompt")
            asked = True
    else:
        if task.get("need_contact"):
            task["need_contact"] = False
            update_task(task)

    return asked

async def advance_task(task: Dict[str, Any], api: OzonApi,
                       notify_text, notify_file) -> None:
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

    # ВАЖНО: Немедленный автопереход в CARGO_PREP при наличии supply_id
    if task.get("supply_id") and task.get("status") not in (ST_CARGO_PREP, ST_CARGO_CREATING, ST_POLL_CARGO, ST_LABELS_CREATING, ST_POLL_LABELS, ST_DONE):
        task["status"] = ST_CARGO_PREP if AUTO_CREATE_CARGOES else ST_DONE
        update_task(task)
        task["next_attempt_ts"] = now_ts() + 1
        update_task(task)
        return

    # Если supply_id ещё нет — учитываем таймеры
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

    # Если уже есть order_id — сразу в SUPPLY_ORDER_FETCH (на всякий)
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
            if n - last_upd < MIN_CREATE_RETRY_SECONDS and task.get("create_attempts", 0) > 0:
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
                wait_sec = max(ra, MIN_CREATE_RETRY_SECONDS)
                _set_retry_after(task, wait_sec)
                task["status"] = "RATE_LIMIT"
                task["last_error"] = "429 Too Many Requests"
                task["create_attempts"] = int(task.get("create_attempts") or 0) + 1
                update_task(task)
                try:
                    await notify_text(task["chat_id"], f"⏱️ [{short(task['id'])}] Лимит Ozon (429). Повтор через {wait_sec} сек.")
                except Exception:
                    logging.exception("notify_text failed on RATE_LIMIT draft")
                return

            if not ok:
                if str(err).startswith("http_status:5") or str(err).startswith("http_error:") or status >= 500 or status == 0:
                    delay = max(_inc_backoff(task), MIN_CREATE_RETRY_SECONDS)
                    task["last_error"] = f"server_error:{err}"
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

            task["draft_operation_id"] = op_id
            task["op_started_ts"] = now_ts()
            task["op_retries"] = 0
            task["status"] = ST_POLL_DRAFT
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
                    logging.exception("notify_text failed on draft timeout")
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
                        logging.exception("notify_text failed on draft/info retries")
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

            logging.info("Draft ready task=%s draft_id=%s chosen_warehouse_id=%s bundle_id=%s",
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
                    logging.exception("notify_text failed on no warehouse")
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

            desired_from = task["desired_from_iso"]
            desired_to = task["desired_to_iso"]
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

        if task["status"] == ST_SUPPLY_ORDER_FETCH or task["status"] == ST_ORDER_DATA_FILLING:
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
                        logging.exception("notify_text failed on supply-order/get")
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
                logging.info("Supply ready task=%s supply_id=%s order_no=%s", short(task["id"]), task.get("supply_id"), task["supply_order_number"])
                task["status"] = ST_CARGO_PREP if AUTO_CREATE_CARGOES else ST_DONE
                update_task(task)
                schedule(1)
                return

            # Иначе продолжаем DATA_FILLING: пытаемся автозаполнить timeslot и даём подсказки
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
                    logging.exception("notify_text failed on cargoes/create")
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
                        logging.exception("notify_text failed on cargoes/info")
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
                    logging.exception("notify_text failed on labels/create")
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
        # Пытаемся сразу проставить drop_off_id из окружения, если есть
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
        if k.endswith("_operation_id") or k in ("draft_id","order_id","supply_id","supply_order_number",
                                                "cargo_payload","cargo_ids","labels_file_guid","labels_pdf_path",
                                                "slot_from","slot_to","slot_id","last_error","op_started_ts","op_retries",
                                                "retry_after_ts","next_attempt_ts","creating","create_attempts","create_backoff_sec",
                                                "order_timeslot_set_ok","order_timeslot_set_attempts","order_timeslot_set_ts",
                                                "need_vehicle","order_vehicle_text","need_contact","order_contact_phone","order_contact_name",
                                                "need_vehicle_prompt_ts","need_contact_prompt_ts","dropoff_warehouse_id","bundle_id","supply_type"):
            t.pop(k, None)
    t["status"] = ST_WAIT_WINDOW
    t["next_attempt_ts"] = 0
    t["retry_after_ts"] = 0
    t["creating"] = False
    t["create_attempts"] = 0
    update_task(t)
    return True, "OK"

# ================== Планировщик ==================

async def process_tasks(notify_text, notify_file):
    ensure_loaded()
    logging.info("process_tasks: START")
    async with _lock:
        api = OzonApi(OZON_CLIENT_ID, OZON_API_KEY, timeout=API_TIMEOUT_SECONDS)
        for task in list_tasks():  # только активные
            if task.get("status") == ST_CANCELED:
                continue
            await advance_task(task, api, notify_text, notify_file)
    logging.info("process_tasks: END")