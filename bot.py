#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ozon FBO Telegram Bot + GigaChat + Auto-supplies + Расширенные отчёты

VERSION: stable-grounded-1.4.23-final + ACL usernames/ids
"""

import os
os.environ["AUTO_BOOK"] = os.getenv("AUTO_BOOK", "0")

# ===================== ACL MIDDLEWARE (NEW) =====================
from typing import Callable, Dict, Any, Awaitable, Set, Optional
from aiogram import BaseMiddleware
from aiogram.types import TelegramObject, Message, CallbackQuery

def _parse_ids_env(key: str) -> Set[int]:
    raw = os.getenv(key, "") or ""
    ids: Set[int] = set()
    for part in raw.replace(" ", "").split(","):
        if part.isdigit():
            try:
                ids.add(int(part))
            except Exception:
                pass
    return ids

def _parse_usernames_env(key: str) -> Set[str]:
    raw = os.getenv(key, "") or ""
    names: Set[str] = set()
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        if part.startswith("@"):
            part = part[1:]
        names.add(part.lower())
    return names

class ACLMiddleware(BaseMiddleware):
    """
    Пропускаем только пользователей из ALLOWED_USER_IDS или ALLOWED_USERNAMES.
    Остальных глушим (или отправляем ACL_DENY_MESSAGE, если задан).
    """
    def __init__(self,
                 allowed_ids: Set[int],
                 allowed_usernames: Set[str],
                 deny_message: Optional[str] = None) -> None:
        super().__init__()
        self.allowed_ids = allowed_ids or set()
        self.allowed_usernames = {u for u in (allowed_usernames or set()) if u}
        self.deny_message = (deny_message or "").strip() or None

    def _is_allowed(self, user) -> bool:
        try:
            if int(user.id) in self.allowed_ids:
                return True
        except Exception:
            pass
        uname = (getattr(user, "username", None) or "").strip()
        if uname:
            if uname.lower().lstrip("@") in self.allowed_usernames:
                return True
        return False

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any],
    ) -> Any:
        user = data.get("event_from_user")
        if not user:
            return
        if self._is_allowed(user):
            return await handler(event, data)
        if self.deny_message:
            bot = data.get("bot")
            if bot:
                try:
                    # Сообщение пользователю – тихо обрабатываем любые ошибки
                    if isinstance(event, Message):
                        await bot.send_message(chat_id=user.id, text=self.deny_message)
                    elif isinstance(event, CallbackQuery):
                        await bot.send_message(chat_id=user.id, text=self.deny_message)
                except Exception:
                    pass
        return
# =================== END ACL MIDDLEWARE (NEW) ===================

import asyncio
import logging
import json
import math
import time
import re
import uuid
import hashlib
import signal
import tempfile
import inspect
from typing import Dict, List, Tuple, Optional, Any
from pathlib import Path
from zoneinfo import ZoneInfo
import html
import sys as _sys, os as _os

_ROOT_DIR = _os.path.dirname(_os.path.abspath(__file__))
if _ROOT_DIR not in _sys.path:
    _sys.path.insert(0, _ROOT_DIR)

from dotenv import load_dotenv
load_dotenv()

_raw_days = os.getenv("DAYS", "").strip()
if not _raw_days or _raw_days == "0":
    os.environ["DAYS"] = "3"
if int(os.getenv("DAYS", "3")) <= 0:
    os.environ["DAYS"] = "3"

# Внешние модули поставок
try:
    import supply_integration as si
except Exception as _e:
    si = None
    logging.getLogger("ozon-bot").warning("supply_integration not available: %s", _e)

try:
    import supply_watch as sw
except Exception as _e:
    sw = None
    logging.getLogger("ozon-bot").warning("supply_watch module not available: %s", _e)

try:
    from supply_watch import register_supply_scheduler
except Exception:
    def register_supply_scheduler(*args, **kwargs):
        logging.getLogger("ozon-bot").warning("supply_watch.register_supply_scheduler not available.")
        return None

try:
    from supply_watch import purge_tasks, purge_all_tasks, purge_stale_nonfinal
except Exception:
    purge_tasks = purge_all_tasks = purge_stale_nonfinal = None

AUTOBOOK_ENABLED = False
_AUTOBOOK_IMPORT_ERROR: Optional[str] = None
try:
    import flows.autobook_flow as abf
    autobook_router = abf.router
    AUTOBOOK_ENABLED = True
except Exception as e:
    autobook_router = None
    AUTOBOOK_ENABLED = False
    _AUTOBOOK_IMPORT_ERROR = f"{e.__class__.__name__}: {e}"

VERSION = "stable-grounded-1.4.23-final"

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile
)
from aiogram.filters import Command
from aiogram.filters import StateFilter
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.exceptions import TelegramRetryAfter

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
OZON_CLIENT_ID = os.getenv("OZON_CLIENT_ID", "").strip()
OZON_API_KEY = os.getenv("OZON_API_KEY", "").strip()

DEFAULT_DROPOFF_ID = (os.getenv("DEFAULT_DROPOFF_ID") or os.getenv("DROP_ID") or "").strip()
DEFAULT_DROPOFF_NAME = (
    os.getenv("DEFAULT_DROPOFF_NAME")
    or os.getenv("DROP_NAME")
    or os.getenv("DEFAULT_DROP_OFF_NAME")
    or ""
).strip()

TIMEWINDOWS_RAW = os.getenv("TIMEWINDOWS", "09:00-12:00;12:00-15:00;15:00-18:00")
DAYS_ENV = int(os.getenv("DAYS", "3"))
if DAYS_ENV <= 0:
    DAYS_ENV = 3
DISABLE_TS_FALLBACK = os.getenv("DISABLE_TS_FALLBACK", "0") == "1"

MIN_STOCK = int(os.getenv("MIN_STOCK", "100"))
TARGET_MULTIPLIER = float(os.getenv("TARGET_MULTIPLIER", "2"))
HISTORY_RETENTION_DAYS = int(os.getenv("HISTORY_RETENTION_DAYS", "120"))
HISTORY_LOOKBACK_DAYS = int(os.getenv("HISTORY_LOOKBACK_DAYS", "90"))
MIN_HISTORY_HOURS = float(os.getenv("MIN_HISTORY_HOURS", "6"))
MAX_HISTORY_POINTS = int(os.getenv("MAX_HISTORY_POINTS", "300"))
MAX_HISTORY_SNAPSHOTS = int(os.getenv("MAX_HISTORY_SNAPSHOTS", "5000"))

SNAPSHOT_INTERVAL_MINUTES = int(os.getenv("SNAPSHOT_INTERVAL_MINUTES", "30"))
SNAPSHOT_STALE_MINUTES = int(os.getenv("SNAPSHOT_STALE_MINUTES", "15"))
SNAPSHOT_MIN_REUSE_SECONDS = int(os.getenv("SNAPSHOT_MIN_REUSE_SECONDS", "120"))
HISTORY_PRUNE_EVERY_MINUTES = int(os.getenv("HISTORY_PRUNE_EVERY_MINUTES", "360"))

DAILY_NOTIFY_HOUR = int(os.getenv("DAILY_NOTIFY_HOUR", "9"))
DAILY_NOTIFY_MINUTE = int(os.getenv("DAILY_NOTIFY_MINUTE", "0"))
TZ_NAME = os.getenv("TZ", "UTC")

API_TIMEOUT_SECONDS = int(os.getenv("API_TIMEOUT_SECONDS", "15"))
HEALTH_WARN_LATENCY_MS = int(os.getenv("HEALTH_WARN_LATENCY_MS", "4000"))
SAVE_BUFFER_FLUSH_SECONDS = int(os.getenv("SAVE_BUFFER_FLUSH_SECONDS", "30"))

DEFAULT_VIEW_MODE = "FULL"

LLM_PROVIDER = os.getenv("LLM_PROVIDER", "").lower().strip()

def _clean(v: str) -> str:
    return (v or "").strip().strip('"').strip("'")

GIGACHAT_CLIENT_ID = _clean(os.getenv("GIGACHAT_CLIENT_ID", ""))
GIGACHAT_CLIENT_SECRET = _clean(os.getenv("GIGACHAT_CLIENT_SECRET", ""))
GIGACHAT_SCOPE = _clean(os.getenv("GIGACHAT_SCOPE", "GIGACHAT_API_B2B"))
GIGACHAT_TOKEN_URL = _clean(os.getenv("GIGACHAT_TOKEN_URL", "https://ngw.devices.sberbank.ru:9443/api/v2/oauth"))
GIGACHAT_API_URL = _clean(os.getenv("GIGACHAT_API_URL", "https://gigachat.devices.sberbank.ru/api/v1/chat/completions"))
GIGACHAT_MODEL = _clean(os.getenv("GIGACHAT_MODEL", "GigaChat"))
GIGACHAT_TEMPERATURE = float(os.getenv("GIGACHAT_TEMPERATURE", "0.3"))
GIGACHAT_MAX_TOKENS = int(os.getenv("GIGACHAT_MAX_TOKENS", "800"))
GIGACHAT_TIMEOUT_SECONDS = int(os.getenv("GIGACHAT_TIMEOUT_SECONDS", "40"))
GIGACHAT_VERIFY_SSL = os.getenv("GIGACHAT_VERIFY_SSL", "1") != "0"
GIGACHAT_SSL_MODE = _clean(os.getenv("GIGACHAT_SSL_MODE", "auto")).lower()
GIGACHAT_CA_CERT = _clean(os.getenv("GIGACHAT_CA_CERT", "/app/ca/gigachat_ca.pem"))
GIGACHAT_TOKEN_CACHE_ENV = _clean(os.getenv("GIGACHAT_TOKEN_CACHE", "keys/gigachat_token_cache.json"))

LLM_FORCE_FACT_MODE = os.getenv("LLM_FORCE_FACT_MODE", "1") == "1"
AI_MIN_INTERVAL_SECONDS = int(os.getenv("AI_MIN_INTERVAL_SECONDS", "5"))
LLM_TOP_DEFICITS = int(os.getenv("LLM_TOP_DEFICITS", "20"))
LLM_TOP_WAREHOUSES = int(os.getenv("LLM_TOP_WAREHOUSES", "8"))
LLM_TOP_CLUSTERS = int(os.getenv("LLM_TOP_CLUSTERS", "8"))
LLM_MAX_CONTEXT_SKU = int(os.getenv("LLM_MAX_CONTEXT_SKU", "10"))
LLM_MAX_CONTEXT_WAREHOUSE = int(os.getenv("LLM_MAX_CONTEXT_WAREHOUSE", "6"))
LLM_INVENTORY_SAMPLE_SKU = int(os.getenv("LLM_INVENTORY_SAMPLE_SKU", "50"))
LLM_FULL_DETAIL_SKU = int(os.getenv("LLM_FULL_DETAIL_SKU", "25"))
LLM_FULL_DETAIL_WAREHOUSES = int(os.getenv("LLM_FULL_DETAIL_WAREHOUSES", "6"))
LLM_FACT_SOFT_LIMIT_CHARS = int(os.getenv("LLM_FACT_SOFT_LIMIT_CHARS", "18000"))
LLM_ENABLE_ANSWER_CACHE = os.getenv("LLM_ENABLE_ANSWER_CACHE", "1") == "1"
LLM_STYLE_ENABLED = os.getenv("LLM_STYLE_ENABLED", "1") == "1"

LLM_GENERAL_TEMPERATURE = float(os.getenv("LLM_GENERAL_TEMPERATURE", "0.7"))
GENERAL_HISTORY_MAX = int(os.getenv("GENERAL_HISTORY_MAX", "12"))
DEFAULT_CHAT_MODE = _clean(os.getenv("DEFAULT_CHAT_MODE", "fact")).lower()

DIAG_TOP_DEFICITS = int(os.getenv("DIAG_TOP_DEFICITS", "8"))
DIAG_TOP_WAREHOUSES = int(os.getenv("DIAG_TOP_WAREHOUSES", "6"))
DIAG_TOP_CLUSTERS = int(os.getenv("DIAG_TOP_CLUSTERS", "6"))

WAREHOUSE_CLUSTERS_ENV = os.getenv("WAREHOUSE_CLUSTERS", "").strip()

STOCK_PAGE_SIZE = min(25, max(5, int(os.getenv("STOCK_PAGE_SIZE", "40"))))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
SUPPLY_JOB_INTERVAL = int(os.getenv("SUPPLY_JOB_INTERVAL", "45"))

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s:%(name)s: %(message)s"
)
log = logging.getLogger("ozon-bot")
if not AUTOBOOK_ENABLED and _AUTOBOOK_IMPORT_ERROR:
    log.warning("Autobook external disabled: %s", _AUTOBOOK_IMPORT_ERROR)

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
STATE_FILE = DATA_DIR / "bot_state.json"
CACHE_FILE = DATA_DIR / "sku_cache.json"
HISTORY_FILE = DATA_DIR / "stock_history.json"
KEYS_DIR = DATA_DIR / "keys"; KEYS_DIR.mkdir(exist_ok=True)
GIGACHAT_TOKEN_CACHE_FILE = (DATA_DIR / GIGACHAT_TOKEN_CACHE_ENV).resolve()
SUPPLY_EVENTS_FILE = DATA_DIR / "supply_events.json"

if not TELEGRAM_BOT_TOKEN:
    raise SystemExit("Missing TELEGRAM_BOT_TOKEN")

MOCK_MODE = not (OZON_CLIENT_ID and OZON_API_KEY)
GIGACHAT_ENABLED = (LLM_PROVIDER == "gigachat")

bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# ========= Attach ACL from ENV (NEW) =========
ALLOWED_USER_IDS = _parse_ids_env("ALLOWED_USER_IDS")
ALLOWED_USERNAMES = _parse_usernames_env("ALLOWED_USERNAMES")
ACL_DENY_MESSAGE = os.getenv("ACL_DENY_MESSAGE", "").strip()
dp.update.middleware(ACLMiddleware(ALLOWED_USER_IDS, ALLOWED_USERNAMES, ACL_DENY_MESSAGE))
# ============================================

ADMIN_ID: Optional[int] = None
SKU_NAME_CACHE: Dict[int, str] = {}
BOT_STATE: Dict[str, Any] = {}
LAST_DEFICIT_CACHE: Dict[int, Dict[str, Any]] = {}
HISTORY_CACHE: List[dict] = []
LAST_SNAPSHOT_TS = 0
ANALYZE_LOCK = asyncio.Lock()
FACT_BUILD_LOCK = asyncio.Lock()
LAST_API_LATENCY_MS = 0.0
LAST_ANALYZE_MS = 0.0
LAST_ANALYZE_ERROR: Optional[str] = None
_HISTORY_DIRTY = False
_LAST_SAVE_FLUSH = 0.0
_GIGACHAT_TOKEN_MEM: Dict[str, Any] = {}
_LAST_AI_CALL = 0.0
FACT_INDEX: Dict[str, Any] = {}
ANSWER_CACHE: Dict[str, str] = {}
GENERAL_HISTORY: Dict[int, List[Dict[str, str]]] = {}
SUPPLY_EVENTS: Dict[str, List[Dict[str, Any]]] = {}
TASKS_CACHE: Dict[int, List[Dict[str, Any]]] = {}
WAREHOUSE_CB_MAP: Dict[str, Tuple[str,str]] = {}
LAST_PURGE_TS: Dict[int, float] = {}

ENV_SKU = os.getenv("SKU_LIST", "")
if ENV_SKU:
    try:
        SKU_LIST = [int(s.strip()) for s in ENV_SKU.replace(";", ",").split(",") if s.strip()]
    except Exception:
        SKU_LIST = []
else:
    SKU_LIST = []

# ===== Кластеры паттерны =====
CLUSTER_MAP: Dict[str, str] = {}
RAW_CLUSTER_PATTERNS: Dict[str, List[str]] = {
    "Санкт-Петербург и СЗО": [r"санкт", r"питер", r"\bспб\b", r"\bсзо\b", r"ленингр"],
    "Казань": [r"казан"],
    "Самара": [r"самар"],
    "Уфа": [r"\буфа\b"],
    "Юг": [r"\bюг\b", r"южн", r"ростов", r"краснодар", r"астрахан"],
    "Воронеж": [r"воронеж"],
    "Саратов": [r"саратов"],
    "Кавказ": [r"кавказ", r"черкес", r"ставроп", r"дагест", r"осет", r"ингуш", r"чеч", r"махачкал"],
    "Красноярск": [r"краснояр"],
    "Сибирь": [r"сибир", r"томск", r"омск", r"новосиб", r"кемеров", r"барнаул", r"иркут", r"кузбас"],
    "Урал": [r"урал", r"екатерин", r"челяб", r"пермь", r"свердлов"],
    "Тюмень": [r"тюмень"],
    "Дальний Восток": [r"дальн(ий)?\s*вост", r"владивост", r"хабаров", r"камчат", r"сахал", r"магадан", r"якут", r"примор"],
    "Калининград": [r"калининград"],
    "Ярославль": [r"ярослав"],
    "Беларусь": [r"беларус", r"минск", r"\bрб\b"],
    "Казахстан": [r"казахстан", r"алматы", r"астан", r"\bкз\b", r"караганда"],
    "Армения": [r"армени", r"ереван"],
}
CLUSTER_PATTERN_MAP: Dict[str, List[re.Pattern]] = {
    cname: [re.compile(p, re.IGNORECASE) for p in pats]
    for cname, pats in RAW_CLUSTER_PATTERNS.items()
}

def parse_cluster_env():
    global CLUSTER_MAP
    raw = WAREHOUSE_CLUSTERS_ENV
    if not raw:
        CLUSTER_MAP = {}
        return
    try:
        obj=json.loads(raw)
        if isinstance(obj,dict):
            CLUSTER_MAP={str(k):str(v) for k,v in obj.items()}
            return
    except Exception:
        pass
    mapping={}
    for part in raw.split(";"):
        part=part.strip()
        if not part: continue
        if "=" in part:
            k,v=part.split("=",1)
            mapping[str(k.strip())]=v.strip().strip('"').strip("'")
    CLUSTER_MAP=mapping
parse_cluster_env()

# ===== UI =====
GR_FILL = {"red":"🟥","orange":"🟧","yellow":"🟨","green":"🟩"}
EMPTY_SEG="▫"
BAR_LEN=12
SEP_THIN="─"*60
SEP_BOLD="═"*60

EMOJI_OK="✅"; EMOJI_WARN="⚠"; EMOJI_ANALYZE="🔍"; EMOJI_NOTIFY="📣"; EMOJI_BOX="📦"
EMOJI_WH="🏬"; EMOJI_CLUSTER="🗺"; EMOJI_REFRESH="🔄"; EMOJI_TARGET="🎯"
EMOJI_INFO="ℹ"; EMOJI_DIAG="🧪"; EMOJI_AI="🤖"; EMOJI_CLOUD="☁"
EMOJI_CHAT="💬"; EMOJI_LIST="📄"; EMOJI_TASKS="📋"

LEGEND_TEXT="Легенда: 🟥 <25%  🟧 <50%  🟨 <80%  🟩 ≥80%"
AI_MAX_RENDER_LINES=420

# ===== FSM =====
class AIChatState(StatesGroup):
    waiting=State()

class AutobookStates(StatesGroup):
    picking_sku=State()
    enter_qty=State()
    choose_warehouse=State()
    choose_date=State()
    choose_timeslot=State()
    confirm=State()
    creating=State()

# ===== Formatting helpers =====
def bold(txt:str)->str:
    return f"§§B§§{txt}§§EB§§"

def build_html(lines:List[str])->str:
    text="\n".join(lines)
    text=html.escape(text)
    text=text.replace("§§B§§","<b>").replace("§§EB§§","</b>")
    text=text.replace("§§I§§","<i>").replace("§§EI§§","</i>")
    text=text.replace("§§U§§","<u>").replace("§§EU§§","</u>")
    return text

def _atomic_write(path:Path, text:str):
    fd,tmp=tempfile.mkstemp(dir=str(path.parent), prefix=path.name, suffix=".tmp")
    try:
        with os.fdopen(fd,"w",encoding="utf-8") as f:
            f.write(text); f.flush(); os.fsync(f.fileno())
        os.replace(tmp,path)
    except Exception:
        try: os.unlink(tmp)
        except Exception: pass

# ===== State persistence =====
def load_state():
    global BOT_STATE, SUPPLY_EVENTS
    if STATE_FILE.exists():
        try:
            BOT_STATE=json.loads(STATE_FILE.read_text("utf-8"))
        except Exception:
            BOT_STATE={}
    BOT_STATE.setdefault("view_mode", DEFAULT_VIEW_MODE)
    BOT_STATE.setdefault("style_enabled", LLM_STYLE_ENABLED)
    BOT_STATE.setdefault("chat_mode", "fact" if DEFAULT_CHAT_MODE not in ("fact","general") else DEFAULT_CHAT_MODE)
    BOT_STATE.setdefault("cluster_view_mode", "full")
    if SUPPLY_EVENTS_FILE.exists():
        try:
            SUPPLY_EVENTS.update(json.loads(SUPPLY_EVENTS_FILE.read_text("utf-8")))
        except Exception:
            pass

def save_state():
    try: _atomic_write(STATE_FILE, json.dumps(BOT_STATE, ensure_ascii=False, indent=2))
    except Exception as e: log.warning("save_state error: %s", e)

def load_cache():
    global SKU_NAME_CACHE
    if CACHE_FILE.exists():
        try:
            data=json.loads(CACHE_FILE.read_text("utf-8"))
            SKU_NAME_CACHE={int(k):v for k,v in data.items()}
        except Exception:
            SKU_NAME_CACHE={}

def save_cache_if_needed(prev:int):
    if len(SKU_NAME_CACHE)>prev:
        try: _atomic_write(CACHE_FILE, json.dumps(SKU_NAME_CACHE, ensure_ascii=False, indent=2))
        except Exception as e: log.warning("cache save error: %s", e)

def load_history():
    global HISTORY_CACHE, LAST_SNAPSHOT_TS
    if HISTORY_FILE.exists():
        try:
            arr=json.loads(HISTORY_FILE.read_text("utf-8"))
            if isinstance(arr,list): HISTORY_CACHE[:]=arr
        except Exception as e:
            log.warning("history load error: %s", e)
    if HISTORY_CACHE:
        LAST_SNAPSHOT_TS=max(s.get("ts",0) for s in HISTORY_CACHE)

def mark_history_dirty():
    global _HISTORY_DIRTY
    _HISTORY_DIRTY=True

async def flush_history_if_needed(force=False):
    global _HISTORY_DIRTY, _LAST_SAVE_FLUSH
    if not _HISTORY_DIRTY and not force: return
    now=time.time()
    if force or (now-_LAST_SAVE_FLUSH>SAVE_BUFFER_FLUSH_SECONDS):
        try:
            await asyncio.to_thread(_atomic_write, HISTORY_FILE, json.dumps(HISTORY_CACHE, ensure_ascii=False))
            _HISTORY_DIRTY=False
            _LAST_SAVE_FLUSH=now
        except Exception as e:
            log.warning("history flush error: %s", e)

def prune_history():
    cutoff=int(time.time())-HISTORY_RETENTION_DAYS*86400
    before=len(HISTORY_CACHE)
    if not before: return
    pruned=[s for s in HISTORY_CACHE if s.get("ts",0)>=cutoff]
    if len(pruned)>MAX_HISTORY_SNAPSHOTS:
        pruned=pruned[-MAX_HISTORY_SNAPSHOTS:]
    if len(pruned)!=before:
        HISTORY_CACHE[:]=прuned
        mark_history_dirty()
        log.info("History pruned %d -> %d", before, len(pruned))

def append_snapshot(rows:List[Dict]):
    global LAST_SNAPSHOT_TS
    ts=int(time.time()); nr=[]
    for r in rows:
        try:
            sku=int(r.get("sku") or 0)
            if not sku: continue
            wid_raw=r.get("warehouse_id")
            wname=(r.get("warehouse_name") or (r.get("warehouse") or {}).get("name") or (str(wid_raw) if wid_raw else "Склад"))
            qty=int(r.get("free_to_sell_amount") or 0)
            if qty<0: qty=0
            wkey=str(wid_raw) if wid_raw not in (None,"") else f"name:{wname}"
            nr.append({"sku":sku,"warehouse_key":wkey,"warehouse_name":wname,"qty":qty})
        except Exception:
            continue
    HISTORY_CACHE.append({"ts":ts,"rows":nr})
    LAST_SNAPSHOT_TS=ts
    mark_history_dirty()

def _supply_log_append(chat_id:int, entry:Dict[str,Any]):
    arr=SUPPLY_EVENTS.setdefault(str(chat_id), [])
    arr.append(entry)
    if len(arr)>300:
        del arr[0:len(arr)-300]
    try:
        _atomic_write(SUPPLY_EVENTS_FILE, json.dumps(SUPPLY_EVENTS, ensure_ascii=False, indent=2))
    except Exception as e:
        log.warning("supply events save error: %s", e)

# ===== API layer (Ozon) =====
async def ozon_stock_fbo(skus:List[int])->Tuple[List[Dict],Optional[str]]:
    if not skus: return [], "SKU_LIST пуст"
    if MOCK_MODE:
        demo_wh=[(1,"Санкт-Петербург ФБО"),(2,"Казань"),(3,"Самара"),(4,"Уфа"),(5,"Ростов-на-Дону"),
                 (6,"Воронеж"),(7,"Саратов"),(8,"Махачкала"),(9,"Красноярск"),(10,"Новосибирск")]
        rows=[]
        for sku in skus:
            base=(sku%50)+20
            for wid,name in demo_wh:
                rows.append({"sku":sku,"warehouse_id":wid,"warehouse_name":name,
                             "free_to_sell_amount":max(0, base - (wid*2) + (sku%7))})
        return rows, None
    url="https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses"
    payload={"sku":skus,"limit":1000,"offset":0}
    headers={"Client-Id":OZON_CLIENT_ID,"Api-Key":OZON_API_KEY,"Content-Type":"application/json"}
    start=time.time()
    try:
        async with httpx.AsyncClient(timeout=API_TIMEOUT_SECONDS) as client:
            resp=await client.post(url,json=payload,headers=headers)
    except Exception as e:
        return [], f"HTTP error: {e}"
    finally:
        global LAST_API_LATENCY_MS
        LAST_API_LATENCY_MS=(time.time()-start)*1000
    if resp.status_code!=200:
        try: data=resp.json(); msg=data.get("message") or data.get("error") or resp.text
        except Exception: msg=resp.text
        return [], f"Ozon API {resp.status_code}: {msg}"
    try: data=resp.json()
    except Exception: return [], "Non-JSON response"
    rows=[]
    if isinstance(data,dict):
        res=data.get("result")
        if isinstance(res,dict) and isinstance(res.get("rows"),list):
            rows=res["rows"]
        elif isinstance(data.get("rows"),list):
            rows=data["rows"]
        else:
            for v in data.values():
                if isinstance(v,list) and v and isinstance(v[0],dict):
                    rows=v; break
    return rows or [], None

async def ozon_product_names_by_sku(skus:List[int])->Tuple[Dict[int,str],Optional[str]]:
    if not skus: return {}, None
    if MOCK_MODE:
        return {s:f"Demo SKU {s}" for s in skus}, None
    headers={"Client-Id":OZON_CLIENT_ID,"Api-Key":OZON_API_KEY,"Content-Type":"application/json"}
    mapping={}
    endpoints=[
        ("https://api-seller.ozon.ru/v3/product/info/list", {"sku": skus, "limit": len(skus)}),
        ("https://api-seller.ozon.ru/v3/product/info/list", {"sku": skus, "limit": len(skus)}),
        ("https://api-seller.ozon.ru/v3/product/info/list", {"offer_id":[str(s) for s in skus],"product_id":[],"sku":[]}),
    ]
    for url,payload in endpoints:
        need=[s for s in skus if s not in mapping]
        if not need: break
        if "offer_id" in payload: payload["offer_id"]=[str(s) for s in need]
        if "sku" in payload: payload["sku"]=need
        try:
            async with httpx.AsyncClient(timeout=API_TIMEOUT_SECONDS) as client:
                resp=await client.post(url, json=payload, headers=headers)
            if resp.status_code!=200: continue
            data=resp.json(); res=data.get("result") or {}
            items=[]
            if isinstance(res,list): items=res
            else:
                for k in ("items","products"):
                    if isinstance(res.get(k),list):
                        items=res[k]; break
            for it in items or []:
                try:
                    sku_v=int(it.get("sku") or it.get("offer_id") or it.get("id"))
                except Exception:
                    continue
                name_v=it.get("name") or it.get("title") or it.get("display_name") or it.get("product_name")
                if name_v: mapping[sku_v]=name_v
        except Exception:
            pass
    for s in skus:
        mapping.setdefault(s,f"SKU {s}")
    return mapping, None

def skus_needing_names()->List[int]:
    return [s for s in SKU_LIST if (s not in SKU_NAME_CACHE) or SKU_NAME_CACHE[s].startswith("SKU ") or SKU_NAME_CACHE[s].lower().startswith("demo sku")]

# ===== Consumption / indexing helpers =====
def build_consumption_cache()->Dict[Tuple[int,str],Dict[str,Any]]:
    now=int(time.time()); cutoff=now-HISTORY_LOOKBACK_DAYS*86400
    series={}
    for snap in HISTORY_CACHE:
        ts=snap.get("ts",0)
        if ts<cutoff: continue
        for r in snap.get("rows",[]):
            sku=r.get("sku"); wkey=r.get("warehouse_key"); qty=r.get("qty")
            if sku is None or wkey is None: continue
            try: sku_i=int(sku); qty_i=int(qty)
            except Exception: continue
            series.setdefault((sku_i,wkey),[]).append((ts,qty_i))
    cache={}
    for key,arr in series.items():
        arr.sort(key=lambda x:x[0])
        if MAX_HISTORY_POINTS>0 and len(arr)>MAX_HISTORY_POINTS:
            arr=arr[-MAX_HISTORY_POINTS:]
        points=len(arr); total_decrease=0
        if points>=2:
            span=arr[-1][0]-arr[0][0]
            if span>0:
                span_hours=span/3600
                for i in range(1,points):
                    p=arr[i-1][1]; c=arr[i][1]
                    if p>c: total_decrease+=p-c
                if span_hours>=MIN_HISTORY_HOURS and total_decrease>0:
                    avg_per_hour=total_decrease/span_hours
                    monthly=avg_per_hour*24*30
                    norm=max(1, math.ceil(monthly))
                    target=max(norm+1, math.ceil(norm*TARGET_MULTIPLIER))
                    cache[key]={"norm":norm,"target":target,"history_used":True}
                    continue
        norm=MIN_STOCK; target=int(MIN_STOCK*TARGET_MULTIPLIER)
        cache[key]={"norm":norm,"target":target,"is_low":False,"history_used":False}
    return cache

def evaluate_position_cached(sku:int,wkey:str,qty:int,ccache:Dict[Tuple[int,str],Dict[str,Any]])->Dict[str,Any]:
    meta=ccache.get((sku,wkey))
    if not meta:
        norm=MIN_STOCK; target=int(MIN_STOCK*TARGET_MULTIPLIER)
        return {"norm":norm,"target":target,"is_low":qty<norm,"need":max(0,norm-qty) if qty<norm else 0,"history_used":False}
    norm=meta["norm"]; target=meta["target"]; is_low=qty<norm
    return {"norm":norm,"target":target,"is_low":is_low,"need":max(0,norm-qty) if is_low else 0,"history_used":meta["history_used"]}

def aggregate_rows(rows:List[Dict])->Dict[int,Dict[str,Dict[str,Any]]]:
    agg={}
    for r in rows:
        try:
            sku=int(r.get("sku") or 0)
            if sku==0: continue
            qty=int(r.get("free_to_sell_amount") or r.get("qty") or 0)
            if qty<0: qty=0
            wid_raw=r.get("warehouse_id")
            wname=r.get("warehouse_name") or (r.get("warehouse") or {}).get("name") or (str(wid_raw) if wid_raw else "Склад")
            wkey=str(wid_raw) if wid_raw not in (None,"") else f"name:{wname}"
        except Exception:
            continue
        agg.setdefault(sku,{})
        agg[sku].setdefault(wkey,{"qty":0,"warehouse_name":wname})
        agg[sku][wkey]["qty"]+=qty
    return agg

def coverage_bar(r:float)->Tuple[str,str]:
    if r<0: r=0
    if r<0.25: c=GR_FILL["red"]; sev="Критично"
    elif r<0.5: c=GR_FILL["orange"]; sev="Критично"
    elif r<0.8: c=GR_FILL["yellow"]; sev="Ниже нормы"
    else: c=GR_FILL["green"]; sev="Нормально"
    filled=min(BAR_LEN,max(0,round(r*BAR_LEN)))
    bar=c*filled+EMPTY_SEG*(BAR_LEN-filled)
    return f"{bar} {int(r*100):02d}%", sev

def calc_need_pct(qty:int, norm:int, target:int)->Tuple[int,int]:
    p_norm=int(round(max(0, (norm-qty))/norm*100)) if norm>0 else 0
    p_target=int(round(max(0, (target-qty))/target*100)) if target>0 else 0
    return max(0,min(100,p_norm)), max(0,min(100,p_target))

def need_pct_text(qty:int, norm:int, target:int)->str:
    pn, pt = calc_need_pct(qty, norm, target)
    return f"{pn}% до нормы / {pt}% до цели"

# ===== Cluster mapping / detail =====
def resolve_cluster_for_warehouse(wkey:str, wname:str)->str:
    if CLUSTER_MAP:
        raw_id=None if wkey.startswith("name:") else wkey
        if raw_id and raw_id in CLUSTER_MAP: return CLUSTER_MAP[raw_id]
        if wname in CLUSTER_MAP: return CLUSTER_MAP[wname]
        return "Прочие"
    lname=(wname or "").lower()
    for cname, pats in CLUSTER_PATTERN_MAP.items():
        for p in pats:
            if p.search(lname):
                return cname
    return "Прочие"

def aggregate_clusters_from_fact(sku_section:Dict[int,Any])->Dict[str,Any]:
    clusters={}
    for sku,data in sku_section.items():
        for w in data.get("warehouses", []):
            cname=resolve_cluster_for_warehouse(w["wkey"], w["name"])
            c=clusters.setdefault(cname,{
                "name":cname,"total_qty":0,"total_need_target":0,"deficit_need":0,"sku_set":set(),
                "critical_sku":0,"mid_sku":0,"ok_sku":0,"warehouses":set()
            })
            qty=w["qty"]; gap_target=max(0, w["target"]-w["qty"])
            c["total_qty"]+=qty; c["total_need_target"]+=gap_target; c["deficit_need"]+=w["need"]
            c["warehouses"].add(w["name"])
            c["sku_set"].add(sku)
            cov=w["coverage"]
            if cov<0.5: c["critical_sku"]+=1
            elif cov<0.8: c["mid_sku"]+=1
            else: c["ok_sku"]+=1
    out={}
    for cname, meta in clusters.items():
        out[cname]={
            "name":cname,
            "total_qty":meta["total_qty"],
            "total_need_target":meta["total_need_target"],
            "deficit_need":meta["deficit_need"],
            "total_sku":len(meta["sku_set"]),
            "critical_sku":meta["critical_sku"],
            "mid_sku":meta["mid_sku"],
            "ok_sku":meta["ok_sku"],
            "warehouses":sorted(meta["warehouses"])
        }
    return out

def small_cov_bar(cov:float, length:int=12)->str:
    cov=max(0.0,min(1.0,cov))
    if cov<0.25: color=GR_FILL["red"]
    elif cov<0.5: color=GR_FILL["orange"]
    elif cov<0.8: color=GR_FILL["yellow"]
    else: color=GR_FILL["green"]
    filled=max(1,round(cov*length))
    return color*filled+EMPTY_SEG*(length-filled)

def build_cluster_detail(name:str, cluster_section:Dict[str,Any], sku_section:Dict[int,Any], short:bool=False)->str:
    cl=cluster_section.get(name)
    if not cl:
        return build_html([f"{EMOJI_CLUSTER} Кластер не найден."])
    # Сводка по складам
    wh_stats: Dict[str, Dict[str, Any]] = {}
    for sku, skud in sku_section.items():
        for w in skud.get("warehouses", []):
            if resolve_cluster_for_warehouse(w["wkey"], w["name"]) != name:
                continue
            ws=wh_stats.setdefault(w["wkey"], {
                "name": w["name"],
                "total_qty": 0,
                "need_norm": 0,
                "need_target": 0,
                "critical_sku": 0,
                "mid_sku": 0,
                "ok_sku": 0,
                "sku_set": set()
            })
            ws["total_qty"]+=w["qty"]
            ws["need_norm"]+=w["need"]
            ws["need_target"]+=max(0, w["target"]-w["qty"])
            ws["sku_set"].add(sku)
            cov=w["coverage"]
            if cov<0.5: ws["critical_sku"]+=1
            elif cov<0.8: ws["mid_sku"]+=1
            else: ws["ok_sku"]+=1

    # Товары по складам (дефициты)
    wh_items: Dict[str, List[Dict[str, Any]]] = {}
    for sku, skud in sku_section.items():
        for w in skud.get("warehouses", []):
            if resolve_cluster_for_warehouse(w["wkey"], w["name"]) != name:
                continue
            if w["need"] <= 0:
                continue
            arr=wh_items.setdefault(w["wkey"], [])
            arr.append({
                "sku": sku,
                "name": skud["name"],
                "qty": w["qty"],
                "norm": w["norm"],
                "target": w["target"],
                "need": w["need"],
                "coverage": w["coverage"]
            })
    for wk in wh_items:
        wh_items[wk].sort(key=lambda x:(x["coverage"], -x["need"]))

    cov_worst=[]
    for sku, skud in sku_section.items():
        worst=1.0; inside=False
        for w in skud.get("warehouses", []):
            if resolve_cluster_for_warehouse(w["wkey"], w["name"])==name:
                worst=min(worst, w["coverage"]); inside=True
        if inside: cov_worst.append(worst)
    total_present=len(cov_worst)
    cluster_worst=min(cov_worst) if cov_worst else 0.0
    cluster_avg=sum(cov_worst)/total_present if total_present else 0.0

    lines=[f"🗺 §§B§§Кластер: {name}§§EB§§", SEP_THIN]
    lines.append(f"SKU всего: {cl['total_sku']}")
    lines.append(f"Суммарный остаток: {cl['total_qty']}")
    lines.append(f"Потребность до цели: {cl['total_need_target']}")
    lines.append(f"Дефицит (ниже нормы): {cl['deficit_need']}")
    lines.append("")
    lines.append("Покрытие:")
    lines.append(f"  Худшее: {small_cov_bar(cluster_worst,20)} {int(cluster_worst*100):02d}%")
    lines.append(f"  Среднее: {small_cov_bar(cluster_avg,20)} {int(cluster_avg*100):02d}%")
    lines.append(SEP_THIN)

    wh_sorted=sorted(wh_stats.values(), key=lambda x:x["need_target"], reverse=True)
    if not short:
        lines.append("§§B§§Сводка по складам§§EB§§")
        if wh_sorted:
            header=f"{'Склад':<30} {'SKU':>4} {'Остаток':>8} {'Дефицит':>8} {'До цели':>8} {'Критичных':>10}"
            lines.append(header); lines.append("-"*len(header))
            for ws in wh_sorted[:40]:
                lines.append(f"{ws['name'][:30]:<30} {len(ws['sku_set']):>4} {ws['total_qty']:>8} {ws['need_norm']:>8} {ws['need_target']:>8} {ws['critical_sku']:>10}")
        else:
            lines.append("Нет данных.")
        lines.append(SEP_THIN)
    else:
        lines.append("§§B§§Склады (коротко)§§EB§§")
        if wh_sorted:
            for ws in wh_sorted[:12]:
                lines.append(f"• {ws['name']}: дефицит {ws['need_norm']}, до цели {ws['need_target']}, критичных SKU {ws['critical_sku']}")
        else:
            lines.append("Нет данных.")
        lines.append(SEP_THIN)

    lines.append("§§B§§Товары по складам (дефицит)§§EB§§" + (" (коротко)" if short else ""))
    if not wh_sorted:
        lines.append("Нет складов в кластере.")
    else:
        per_wh_limit = 6 if short else 12
        for ws in wh_sorted:
            wkey=None
            for k,meta in wh_stats.items():
                if meta is ws:
                    wkey=k; break
            if wkey is None:
                for k,meta in wh_stats.items():
                    if meta["name"]==ws["name"]:
                        wkey=k; break
            lines.append(f"{EMOJI_WH} {bold(ws['name'])} — Остаток {ws['total_qty']} | Дефицит {ws['need_norm']} | До цели {ws['need_target']} | Критичных SKU {ws['critical_sku']}")
            items=wh_items.get(wkey,[])
            if not items:
                lines.append("  • Дефицитов не найдено.")
            else:
                for it in items[:per_wh_limit]:
                    cov=it["qty"]/it["norm"] if it["norm"] else 0
                    bar, sev=coverage_bar(cov)
                    badge=need_pct_text(it["qty"], it["norm"], it["target"])
                    lines.append(f"  • {bold(it['name'])} (SKU {it['sku']})")
                    lines.append(f"    Остаток {it['qty']} / Норма {it['norm']} / Цель {it['target']} → +{it['need']} · {badge}")
                    lines.append(f"    {bar} {sev}")
            lines.append(SEP_THIN)

    return build_html(lines)

# ===== FACT_INDEX building =====
def build_fact_index(rows:List[dict], flat:List[dict], ccache:Dict[Tuple[int,str],Dict[str,Any]]):
    agg=aggregate_rows(rows)
    sku_section={}
    wh_agg={}
    for sku,wmap in agg.items():
        name=SKU_NAME_CACHE.get(sku,f"SKU {sku}")
        entry={"name":name,"total_qty":0,"total_need":0,"deficit_need":0,"worst_coverage":1.0,"warehouses":[]}
        for wkey,info in wmap.items():
            qty=info["qty"]
            st=evaluate_position_cached(sku,wkey,qty,ccache)
            norm=st["norm"] or 1
            coverage=qty/norm if norm else 0
            need_def=st["need"] if st["is_low"] else 0
            gap_target=max(0, st["target"]-qty)
            entry["total_qty"]+=qty
            entry["deficit_need"]+=need_def
            entry["total_need"]+=gap_target
            entry["worst_coverage"]=min(entry["worst_coverage"], coverage)
            entry["warehouses"].append({
                "wkey":wkey,"name":info["warehouse_name"],"qty":qty,"norm":st["norm"],"target":st["target"],
                "need":need_def,"coverage":round(coverage,4),"history_used":st["history_used"]
            })
            wm=wh_agg.setdefault(wkey,{"name":info["warehouse_name"],"total_qty":0,"total_need":0,"deficit_need":0,
                                       "sku_set":set(),"critical_sku":0,"mid_sku":0,"ok_sku":0})
            wm["total_qty"]+=qty; wm["total_need"]+=gap_target; wm["deficit_need"]+=need_def
            wm["sku_set"].add(sku)
            if coverage<0.5: wm["critical_sku"]+=1
            elif coverage<0.8: wm["mid_sku"]+=1
            else: wm["ok_sku"]+=1
        entry["warehouses"].sort(key=lambda x:x["coverage"])
        sku_section[sku]=entry
    top_deficits=sorted(
        ({"sku":s,"name":v["name"],"coverage":round(v["worst_coverage"],4),"deficit_need":v["deficit_need"]}
         for s,v in sku_section.items()),
        key=lambda x:x["coverage"]
    )[:LLM_TOP_DEFICITS]
    wh_section={}
    for k,meta in wh_agg.items():
        wh_section[k]={
            "name":meta["name"],"total_qty":meta["total_qty"],
            "total_need":meta["total_need"],"deficit_need":meta["deficit_need"],
            "total_sku":len(meta["sku_set"]),
            "critical_sku":meta["critical_sku"],
            "mid_sku":meta["mid_sku"],
            "ok_sku":meta["ok_sku"]
        }
    cluster_section=aggregate_clusters_from_fact(sku_section)
    top_clusters=sorted(
        ({"cluster":c,"name":v["name"],"total_need":v["total_need_target"],"deficit_need":v["deficit_need"]}
         for c,v in cluster_section.items()),
        key=lambda x:x["total_need"], reverse=True
    )[:LLM_TOP_CLUSTERS]
    top_warehouses=sorted(
        ({"wkey":k,"name":v["name"],"total_need":v["total_need"],"deficit_need":v["deficit_need"]}
         for k,v in wh_section.items()),
        key=lambda x:x["total_need"], reverse=True
    )[:LLM_TOP_WAREHOUSES]
    sample=[f"{sku}:{sku_section[sku]['name'][:50]}" for sku in sorted(sku_section.keys())[:LLM_INVENTORY_SAMPLE_SKU]]
    FACT_INDEX.clear()
    FACT_INDEX.update({
        "updated_ts":int(time.time()),
        "snapshot_ts":LAST_SNAPSHOT_TS,
        "sku":sku_section,
        "warehouse":wh_section,
        "cluster":cluster_section,
        "top_deficits":top_deficits,
        "top_warehouses":top_warehouses,
        "top_clusters":top_clusters,
        "inventory_overview":{"total_sku":len(sku_section),"sample_skus":sample}
    })

# ===== Silent index builder =====
async def ensure_fact_index(force:bool=False, silent:bool=True):
    async with FACT_BUILD_LOCK:
        if not force and FACT_INDEX:
            return
        rows, err = await ozon_stock_fbo(SKU_LIST)
        if err:
            log.warning("ensure_fact_index: Ozon error: %s", err)
            return
        # snapshot refresh if stale
        if time.time()-LAST_SNAPSHOT_TS>SNAPSHOT_MIN_REUSE_SECONDS:
            append_snapshot(rows)
        # names
        to_fetch=skus_needing_names()
        if to_fetch:
            prev=len(SKU_NAME_CACHE); mp,_=await ozon_product_names_by_sku(to_fetch)
            SKU_NAME_CACHE.update(mp); save_cache_if_needed(prev)
        ccache=build_consumption_cache()
        # Build index (flat deficits optional)
        try:
            build_fact_index(rows, [], ccache)
        except Exception as e:
            log.exception("ensure_fact_index build error: %s", e)
        await flush_history_if_needed(force=True)
        if not silent:
            await send_safe_message(ADMIN_ID or list(SUPPLY_EVENTS.keys())[0], "Индекс обновлён.", disable_web_page_preview=True)

# ===== Report generation (deficit) =====
def generate_deficit_report(rows:List[Dict], name_map:Dict[int,str], ccache:Dict[Tuple[int,str],Dict[str,Any]])->Tuple[str,List[dict]]:
    agg=aggregate_rows(rows)
    deficits={}; flat=[]
    for sku,wmap in agg.items():
        for wkey,info in wmap.items():
            qty=info["qty"]
            st=evaluate_position_cached(sku,wkey,qty,ccache)
            if st["is_low"]:
                cov=qty/st["norm"] if st["norm"] else 0
                d={"sku":sku,"name":name_map.get(sku,f"SKU {sku}"),"warehouse_key":wkey,"warehouse_name":info["warehouse_name"],
                   "qty":qty,"norm":st["norm"],"target":st["target"],"need":st["need"],
                   "coverage":cov,"history_used":st["history_used"]}
                deficits.setdefault(sku,[]).append(d)
                flat.append(d)
    if not deficits:
        return f"{EMOJI_OK} Нет товаров ниже нормы.", []
    sku_order=sorted(deficits.keys(), key=lambda s: min(x["coverage"] for x in deficits[s]))
    view_mode=BOT_STATE.get("view_mode", DEFAULT_VIEW_MODE)
    full=(view_mode=="FULL")
    crit=mid=hi=0
    lines=[f"{EMOJI_ANALYZE} §§B§§Дефицит по товарам§§EB§§", LEGEND_TEXT, SEP_BOLD]
    for sku in sku_order:
        items=deficits[sku]; items.sort(key=lambda x:x["coverage"])
        pname=items[0]["name"]; worst=min(i["coverage"] for i in items)
        head="🔥" if worst<0.25 else (EMOJI_WARN if worst<0.5 else "➤")
        lines.append(f"{head} §§B§§{pname} (SKU {sku})§§EB§§")
        total_qty=sum(i["qty"] for i in items); total_need=sum(i["need"] for i in items)
        for i in items:
            bar,sev=coverage_bar(i["coverage"])
            if i["coverage"]<0.5: crit+=1
            elif i["coverage"]<0.8: mid+=1
            else: hi+=1
            hist="(история)" if i["history_used"] else "(мин. порог)"
            badge=need_pct_text(i["qty"], i["norm"], i["target"])
            wh_b = bold(i['warehouse_name'])
            if full:
                lines.append(f"• {wh_b}: Остаток {i['qty']} / Норма {i['norm']} / Цель {i['target']} → +{i['need']}\n  {bar} {sev} {hist} · {badge}")
            else:
                lines.append(f"• {wh_b}: Остаток {i['qty']} → +{i['need']}  {bar} · {badge}")
        lines.append(f"  Σ Остаток={total_qty}, Потребность (до нормы)={total_need}")
        lines.append(SEP_THIN)
    lines.append(f"{EMOJI_TARGET} Итоги: товаров={len(deficits)}, строк={len(flat)} | <50%={crit} | 50–80%={mid} | ≥80% но ниже нормы={hi} | режим={view_mode}")
    return build_html(lines), flat

# ===== AI highlight patterns & rendering =====
HIGHLIGHT_PATTERNS=[
    (r"(\b\d{1,3}(?:[\s.,]\d{3})+|\b\d+)\b","num"),
    (r"\b\d{1,3}%\b","pct"),
    (r"\b(SKU\s*\d+)\b","sku"),
    (r"\b(дефицит\w*)\b","kw"),
    (r"\b(склад\w*)\b","kw"),
    (r"\b(кластер\w*)\b","kw"),
    (r"\b(норм[аиы]?)\b","kw"),
    (r"\b(цель|target|целевая)\b","kw"),
    (r"\b(покрыти[ея]|coverage)\b","kw"),
]

def _html_highlight(text:str)->str:
    def repl_bold(m): return f"<b>{html.escape(m.group(1))}</b>"
    out=text
    for pat,_ in HIGHLIGHT_PATTERNS:
        out=re.sub(pat,repl_bold,out,flags=re.IGNORECASE)
    return out

def style_ai_answer(question:str, raw:str, mode:str, fact_mode:bool)->str:
    raw=(raw or "").strip() or "Нет ответа."
    header=f"{EMOJI_AI} <b>Ответ ассистента</b> · режим: <u>{'FACT' if fact_mode else 'GENERAL'}</u> · {time.strftime('%H:%M:%S')}"
    qline=f"<i>Вопрос:</i> {html.escape(question)}"
    src=[ln.rstrip() for ln in raw.splitlines()]
    out=[]; blank=False
    for ln in src:
        if not ln.strip():
            if not blank: out.append("")
            blank=True; continue
        blank=False
        if re.match(r"^[-*•—]\s", ln) or re.match(r"^\d+[\).]\s", ln):
            ln="• "+re.sub(r"^[-*•—]\s*","",ln)
        out.append(ln)
    body=html.escape("\n".join(out))
    body=_html_highlight(body)
    return f"{header}\n{SEP_THIN}\n{qline}\n{SEP_THIN}\n{body}"

def _gigachat_verify_param():
    mode=os.getenv("GIGACHAT_SSL_MODE","auto").lower().strip()
    verify=os.getenv("GIGACHAT_VERIFY_SSL","1")!="0"
    ca=os.getenv("GIGACHAT_CA_CERT","/app/ca/gigachat_ca.pem").strip()
    if mode=="insecure": return False
    if mode=="custom":
        if not os.path.isfile(ca):
            log.warning("GigaChat CA не найден (%s)", ca)
            return verify
        return ca
    return verify

def _read_token_cache_file():
    if not GIGACHAT_TOKEN_CACHE_FILE.exists(): return None
    try: return json.loads(GIGACHAT_TOKEN_CACHE_FILE.read_text("utf-8"))
    except Exception: return None

def _write_token_cache_file(data:dict):
    try: _atomic_write(GIGACHAT_TOKEN_CACHE_FILE, json.dumps(data, ensure_ascii=False, indent=2))
    except Exception as e: log.warning("token cache write error: %s", e)

def _token_valid(tok:dict)->bool:
    if not tok: return False
    exp=tok.get("expires_epoch")
    if not exp and tok.get("obtained_at") and tok.get("expires_in"):
        exp=tok["obtained_at"]+int(tok["expires_in"])
    if not exp: return False
    return (exp-time.time())>120

async def get_gigachat_token(force=False)->str:
    if not GIGACHAT_ENABLED: raise RuntimeError("LLM_PROVIDER != gigachat")
    cid=GIGACHAT_CLIENT_ID; sec=GIGACHAT_CLIENT_SECRET
    if not cid or not sec: raise RuntimeError("Пустые CLIENT_ID/SECRET")
    global _GIGACHAT_TOKEN_MEM
    if not force and _token_valid(_GIGACHAT_TOKEN_MEM):
        return _GIGACHAT_TOKEN_MEM["access_token"]
    if not force:
        cache=_read_token_cache_file()
        if _token_valid(cache):
            _GIGACHAT_TOKEN_MEM=cache
            return cache["access_token"]
    headers={"RqUID":str(uuid.uuid4()),"Content-Type":"application/x-www-form-urlencoded","Accept":"application/json"}
    data={"scope":GIGACHAT_SCOPE}
    async with httpx.AsyncClient(verify=_gigachat_verify_param(), timeout=GIGACHAT_TIMEOUT_SECONDS, trust_env=True) as client:
        resp=await client.post(GIGACHAT_TOKEN_URL,data=data,headers=headers,auth=(cid,sec))
    if resp.status_code>=400:
        raise RuntimeError(f"OAuth {resp.status_code}: {resp.text}")
    js=resp.json(); obtained=int(time.time())
    exp_epoch=js.get("expires_at")
    if not exp_epoch and js.get("expires_in"):
        try: exp_epoch=obtained+int(js["expires_in"])
        except Exception: pass
    if not exp_epoch: exp_epoch=obtained+1800
    token_obj={"access_token":js.get("access_token"),"obtained_at":obtained,"expires_in":js.get("expires_in"),"expires_epoch":exp_epoch}
    if not token_obj["access_token"]: raise RuntimeError("Ответ без access_token")
    _GIGACHAT_TOKEN_MEM=token_obj; _write_token_cache_file(token_obj)
    return token_obj["access_token"]

FULL_DUMP_PATTERNS=["весь объем","весь объём","все данные","полный список","полный перечень","full dump","все sku","весь ассортимент","доступные товары","все товары"]
PRODUCT_LIST_PATTERNS=["какие товары","список товаров","перечень товаров","ассортимент","какие у нас товары","что за товары"]

def extract_skus_from_question(q:str)->List[int]:
    return [int(m.group()) for m in re.finditer(r"\b\d{3,}\b", q)]

def is_full_dump_question(q:str)->bool:
    ql=q.lower(); return any(p in ql for p in FULL_DUMP_PATTERNS)

def is_list_products_question(q:str)->bool:
    ql=q.lower(); return any(p in ql for p in PRODUCT_LIST_PATTERNS)

def _trim_facts(text:str)->str:
    if len(text)<=LLM_FACT_SOFT_LIMIT_CHARS: return text
    out=[]; total=0
    limit=LLM_FACT_SOFT_LIMIT_CHARS-300
    for ln in text.splitlines():
        if total+len(ln)+1>limit:
            out.append("...(усечено)")
            break
        out.append(ln); total+=len(ln)+1
    return "\n".join(out)

def build_facts_block(question:str)->Tuple[str,str]:
    if not FACT_INDEX: return "NO_DATA_INDEX","empty"
    q=question.strip(); skus_in=extract_skus_from_question(q)
    sku_data=FACT_INDEX.get("sku",{}); inv=FACT_INDEX.get("inventory_overview",{})
    mode="general"
    if is_full_dump_question(q):
        mode="full_dump"
        lines=[f"snapshot_ts={FACT_INDEX['snapshot_ts']} TOTAL_SKU={inv.get('total_sku')}"]
        for sku, entry in list(sku_data.items())[:LLM_FULL_DETAIL_SKU]:
            lines.append(f"SKU {sku} '{entry['name']}' worst_cov={round(entry['worst_coverage'],3)} total_qty={entry['total_qty']} deficit_need={entry['deficit_need']}")
            for w in entry["warehouses"][:LLM_FULL_DETAIL_WAREHOUSES]:
                lines.append(f"  WH '{w['name']}' qty={w['qty']} norm={w['norm']} target={w['target']} need_norm={w['need']} cov={w['coverage']}")
        return _trim_facts("\n".join(lines)), mode
    if skus_in:
        mode="specific"
        lines=[f"snapshot_ts={FACT_INDEX['snapshot_ts']} TOTAL_SKU={inv.get('total_sku')}"]
        for sku in skus_in[:LLM_MAX_CONTEXT_SKU]:
            entry=sku_data.get(sku)
            if not entry:
                lines.append(f"SKU {sku}: NO_DATA"); continue
            lines.append(f"SKU {sku} '{entry['name']}' worst_cov={round(entry['worst_coverage'],3)} total_qty={entry['total_qty']} deficit_need={entry['deficit_need']}")
            for w in entry["warehouses"][:LLM_MAX_CONTEXT_WAREHOUSE]:
                lines.append(f"  WH '{w['name']}' qty={w['qty']} norm={w['norm']} target={w['target']} need_norm={w['need']} cov={w['coverage']}")
        return _trim_facts("\n".join(lines)), mode
    if is_list_products_question(q):
        mode="list"
        lines=[f"snapshot_ts={FACT_INDEX['snapshot_ts']} TOTAL_SKU={inv.get('total_sku')}", "SAMPLE_SKUS:"]
        for s in inv.get("sample_skus",[])[:LLM_INVENTORY_SAMPLE_SKU]:
            lines.append(f"  {s}")
        return _trim_facts("\n".join(lines)), mode
    lines=[f"snapshot_ts={FACT_INDEX['snapshot_ts']} TOTAL_SKU={inv.get('total_sku')}"]
    for td in FACT_INDEX.get("top_deficits",[])[:LLM_MAX_CONTEXT_SKU]:
        lines.append(f"TOP_DEFICIT SKU {td['sku']} '{td['name']}' cov={td['coverage']} need_def={td['deficit_need']}")
    return _trim_facts("\n".join(lines)), mode

def build_messages_fact(question:str)->Tuple[List[Dict[str,str]], str]:
    facts, mode=build_facts_block(question)
    if facts=="NO_DATA_INDEX":
        return [
            {"role":"system","content":"Ты отвечаешь только данными из FACTS. Если данных нет — 'Нет данных'."},
            {"role":"user","content":question}
        ], mode
    system="Ты аналитик остатков. Используй ТОЛЬКО данные из FACTS; не выдумывай."
    return [
        {"role":"system","content":system},
        {"role":"user","content":f"Вопрос:\n{question}\n\nFACTS:\n{facts}"}
    ], mode

GENERAL_WORK_KEYWORDS=["sku","склад","склады","дефицит","норм","target","покрыт","остат","товар","ozon","озон","кластер"]
def looks_like_work_question(q:str)->bool:
    ql=q.lower()
    if re.search(r"\b\d{5,}\b", ql): return True
    return any(k in ql for k in GENERAL_WORK_KEYWORDS)

def add_general_history(chat_id:int, role:str, content:str):
    arr=GENERAL_HISTORY.setdefault(chat_id,[])
    arr.append({"role":role,"content":content})
    if len(arr)>GENERAL_HISTORY_MAX:
        del arr[0:len(arr)-GENERAL_HISTORY_MAX]

def build_general_messages(chat_id:int, question:str)->List[Dict[str,str]]:
    history=GENERAL_HISTORY.get(chat_id,[])
    sys="Ты дружелюбный помощник. Если вопрос про остатки/склады — предложи /ai."
    msgs=[{"role":"system","content":sys}]
    for msg in history[-(GENERAL_HISTORY_MAX-1):]:
        msgs.append(msg)
    msgs.append({"role":"user","content":question})
    if looks_like_work_question(question):
        msgs.append({"role":"system","content":"Рабочий вопрос — предложи /ai."})
    return msgs

async def llm_fact_answer(question:str)->Tuple[str,str]:
    if not GIGACHAT_ENABLED: return "LLM отключён.", "off"
    q=question.strip()
    if not q: return "Пустой запрос.","empty"
    global _LAST_AI_CALL
    now=time.time()
    if (now-_LAST_AI_CALL)<AI_MIN_INTERVAL_SECONDS:
        return f"Слишком часто. Подождите {AI_MIN_INTERVAL_SECONDS-int(now-_LAST_AI_CALL)} сек.","rate"
    await ensure_fact_index()
    messages, mode=build_messages_fact(q)
    key=hashlib.sha1(f"{mode}|{FACT_INDEX.get('snapshot_ts')}|{q.lower()}".encode()).hexdigest()
    if ANSWER_CACHE.get(key):
        return "(из кэша)\n"+ANSWER_CACHE[key], mode
    _LAST_AI_CALL=now
    try:
        token=await get_gigachat_token()
    except Exception as e:
        return f"Не удалось получить токен: {e}", "auth"
    payload={"model":GIGACHAT_MODEL,"messages":messages,"temperature":min(0.2,GIGACHAT_TEMPERATURE),"max_tokens":GIGACHAT_MAX_TOKENS}
    try:
        async with httpx.AsyncClient(verify=_gigachat_verify_param(), timeout=GIGACHAT_TIMEOUT_SECONDS, trust_env=True) as client:
            r=await client.post(GIGACHAT_API_URL,json=payload,headers={"Authorization":f"Bearer {token}","Content-Type":"application/json"})
            if r.status_code==401:
                _GIGACHAT_TOKEN_MEM={}
                token=await get_gigachat_token(force=True)
                r=await client.post(GIGACHAT_API_URL,json=payload,headers={"Authorization":f"Bearer {token}","Content-Type":"application/json"})
            if r.status_code>=400:
                return f"GigaChat HTTP {r.status_code}: {r.text[:250]}", "http"
            data=r.json()
    except Exception as e:
        return f"Ошибка сети: {e}", "net"
    ch=data.get("choices")
    if not ch: return f"Пустой ответ: {data}","empty"
    text=(ch[0].get("message",{}).get("content") or "").strip()
    ANSWER_CACHE[key]=text
    return text, mode

async def llm_general_answer(chat_id:int, question:str)->Tuple[str,str]:
    if not GIGACHAT_ENABLED: return "LLM отключён.", "off"
    q=question.strip()
    if not q: return "Пустой запрос.","empty"
    global _LAST_AI_CALL
    now=time.time()
    if (now-_LAST_AI_CALL)<AI_MIN_INTERVAL_SECONDS:
        return f"Слишком часто. Подождите {AI_MIN_INTERVAL_SECONDS-int(now-_LAST_AI_CALL)} сек.","rate"
    _LAST_AI_CALL=now
    messages=build_general_messages(chat_id,q)
    try:
        token=await get_gigachat_token()
    except Exception as e:
        return f"Не удалось получить токен: {e}", "auth"
    payload={"model":GIGACHAT_MODEL,"messages":messages,"temperature":LLM_GENERAL_TEMPERATURE,"max_tokens":GIGACHAT_MAX_TOKENS}
    try:
        async with httpx.AsyncClient(verify=_gigachat_verify_param(), timeout=GIGACHAT_TIMEOUT_SECONDS, trust_env=True) as client:
            r=await client.post(GIGACHAT_API_URL,json=payload,headers={"Authorization":f"Bearer {token}","Content-Type":"application/json"})
            if r.status_code>=400:
                return f"GigaChat HTTP {r.status_code}: {r.text[:250]}", "http"
            data=r.json()
    except Exception as e:
        return f"Ошибка сети: {e}", "net"
    ch=data.get("choices")
    if not ch: return f"Пустой ответ: {data}","empty"
    text=(ch[0].get("message",{}).get("content") or "").strip()
    add_general_history(chat_id,"user",q)
    add_general_history(chat_id,"assistant",text)
    return text,"general"

# ===== Messaging helpers =====
async def send_safe_message(chat_id:int, text:str, **kwargs):
    if not text: text="\u200b"
    try: return await bot.send_message(chat_id, text, **kwargs)
    except TelegramRetryAfter as e:
        await asyncio.sleep(e.retry_after)
        return await bot.send_message(chat_id, text, **kwargs)
    except Exception as e:
        log.warning("send fail: %s", e)

async def send_long(chat_id:int, text:str, kb:Optional[InlineKeyboardMarkup]=None):
    max_len=3900
    parts=[]; buf=[]; ln=0
    for line in (text or "").split("\n"):
        L=len(line)+1
        if buf and ln+L>max_len:
            parts.append("\n".join(buf)); buf=[line]; ln=L
        else:
            buf.append(line); ln+=L
    if buf: parts.append("\n".join(buf))
    if not parts: parts=["\u200b"]
    for i,chunk in enumerate(parts):
        await send_safe_message(chat_id, chunk.rstrip() or "\u200B",
                                parse_mode="HTML",
                                disable_web_page_preview=True,
                                reply_markup=kb if (kb and i==len(parts)-1) else None)
        await asyncio.sleep(0.02)

# ===== Tasks logic =====
DRAFT_STATUSES = {"DRAFT","NEW","CREATED","WAIT_WINDOW","CALCULATION_STATUS_PENDING","INITIAL"}
WAIT_STATUSES = {"WAIT","WAITING","PENDING","IN_PROGRESS","ACTIVE","QUEUED","CALCULATION_STATUS_SUCCESS"}
SLOT_STATUSES = {"BOOKED","RESERVED","SCHEDULED","WINDOW_SET","SLOT_SET","SLOT_BOOKED"}
CREATING_STATUSES = {"CREATING","CREATING_SUPPLY","SUPPLY_CREATING","SUPPLY_CREATE","CREATING_DRAFT"}
DONE_STATUSES = {"DONE","SUCCESS","FINISHED","COMPLETED","SUPPLY_CREATED"}
ERROR_STATUSES = {"ERROR","FAILED"}
CANCEL_STATUSES = {"CANCELLED","CANCELED"}

DEFAULT_STAGE_EMOJI_RU = {
    "Черновик":"📝","Ожидание":"⏳","Слот":"🕘","Создание заявки":"🛠",
    "Готово":"✅","Ошибка":"❌","Отменено":"🚫"
}

def resolve_abf_stage_emoji_map()->Dict[str,str]:
    try:
        if AUTOBOOK_ENABLED and 'abf' in globals() and abf:
            for name in dir(abf):
                obj=getattr(abf,name)
                if isinstance(obj,dict):
                    keys=set(DEFAULT_STAGE_EMOJI_RU.keys())
                    if keys.issубset(set(obj.keys())):
                        return {str(k):str(v) for k,v in obj.items()}
    except Exception:
        pass
    return DEFAULT_STAGE_EMOJI_RU

STAGE_EMOJI_RU=resolve_abf_stage_emoji_map()

def classify_task_stage(task:Dict[str,Any])->Tuple[str,str]:
    status=(task.get("status") or "").upper()
    creating=bool(task.get("creating"))
    desired_from_iso=task.get("desired_from_iso") or ""
    last_error=(task.get("last_error") or "").strip()
    if status in ERROR_STATUSES or last_error: return (STAGE_EMOJI_RU.get("Ошибка","❌"),"Ошибка")
    if status in CANCEL_STATUSES: return (STAGE_EMОJI_RU.get("Отменено","🚫"),"Отменено")
    if creating or status in CREATING_STATUSES: return (STAGE_EMОJI_RU.get("Создание заявки","🛠"),"Создание заявки")
    if status in DONE_STATUSES: return (STAGE_EMОJI_RU.get("Готово","✅"),"Готово")
    if status in SLOT_STATUSES: return (STAGE_EMОJI_RU.get("Слот","🕘"),"Слот")
    if status in DRAFT_STATUSES:
        if desired_from_iso and status=="WAIT_WINDOW":
            return (STAGE_EMОJI_RU.get("Ожидание","⏳"),"Ожидание")
        return (STAGE_EMОJI_RU.get("Черновик","📝"),"Черновик")
    if status in WAIT_STATUSES or status.startswith("WAIT_"):
        return (STAGE_EMОJI_RU.get("Ожидание","⏳"),"Ожидание")
    if desired_from_iso and not creating: return (STAGE_EMОJI_RU.get("Слот","🕘"),"Слот")
    return (STAGE_EMОJI_RU.get("Черновик","📝"),"Черновик")

def _first_time_or_dash(task:Dict[str,Any])->str:
    ts=task.get("timeslot") or ""
    if ts: return ts
    f=task.get("desired_from_iso"); t=task.get("desired_to_iso")
    try:
        if f and t and "T" in f and "T" in t:
            return f"{f.split('T')[1][:5]}-{t.split('T')[1][:5]}"
    except Exception: pass
    return "-"

def _sum_qty(task:Dict[str,Any])->int:
    total=0
    sl=task.get("sku_list")
    if isinstance(sl,list):
        for it in sl:
            try: total+=int(it.get("total_qty") or it.get("qty") or 0)
            except Exception: pass
        return total
    try: return int(task.get("qty") or 0)
    except Exception: return 0

def _first_sku(task:Dict[str,Any])->Optional[int]:
    sl=task.get("sku_list")
    if isinstance(sl,list) and sl:
        try: return int(sl[0].get("sku") or 0)
        except Exception: return None
    try: return int(task.get("sku") or 0)
    except Exception: return None

def _task_warehouse_name(task:Dict[str,Any])->str:
    for key in ("warehouse_name","chosen_warehouse_name","drop_off_name"):
        if task.get(key): return str(task[key])
    sl=task.get("sku_list")
    if isinstance(sl,list) and sl:
        w=sl[0].get("warehouse_name")
        if w: return str(w)
    for key in ("chosen_warehouse_id","drop_off_id","warehouse_id"):
        if task.get(key): return f"id:{task[key]}"
    return "-"

async def call_func_flexible(fn, chat_id:int):
    try:
        sig=inspect.signature(fn)
        params=sig.parameters
        if "chat_id" in params: res=fn(chat_id=chat_id)
        elif "user_id" in params: res=fn(user_id=chat_id)
        elif "uid" in params: res=fn(uid=chat_id)
        elif "chat" in params: res=fn(chat=chat_id)
        else: res=fn()
        if inspect.isawaitable(res): res=await res
        return res,None
    except Exception as e:
        return None,e

def normalize_tasks_result(res:Any, chat_id:int)->List[Dict[str,Any]]:
    if not res: return []
    if isinstance(res, dict):
        for key in ("tasks","items","result"):
            v=res.get(key)
            if isinstance(v,list) and (not v or isinstance(v[0],dict)):
                return [t for t in v if t.get("chat_id") in (chat_id,str(chat_id)) or "chat_id" not in t]
        if str(chat_id) in res and isinstance(res[str(chat_id)],list):
            return [x for x in res[str(chat_id)] if isinstance(x,dict)]
        vals=[]
        for v in res.values():
            if isinstance(v,list) and v and isinstance(v[0],dict):
                vals.extend(v)
        if vals:
            return [t for t in vals if t.get("chat_id") in (chat_id,str(chat_id)) or "chat_id" not in t]
        if "id" in res: return [res]
        return []
    if isinstance(res,list) and res and isinstance(res[0],dict):
        if any("chat_id" in x for x in res):
            return [t for t in res if t.get("chat_id") in (chat_id,str(chat_id))]
        return res
    return []

def fallback_tasks_from_events(chat_id:int)->List[Dict[str,Any]]:
    arr=SUPPLY_EVENTS.get(str(chat_id)) or []
    out=[]; seen=set()
    for e in reversed(arr):
        payload=e.get("payload") or {}
        tid=payload.get("id") or payload.get("task_id")
        if not tid:
            m=re.search(r"([a-f0-9]{8}-[a-f0-9-]{13,})",(e.get("text") or ""),re.I)
            if m: tid=m.group(1)
        if not tid or tid in seen: continue
        seen.add(tid)
        t={
            "id":tid,
            "status":(e.get("status") or "DRAFT").upper(),
            "date":payload.get("date") or "",
            "timeslot":payload.get("timeslot") or "",
            "chat_id":chat_id,
            "creating":payload.get("creating") or False,
            "desired_from_iso":payload.get("desired_from_iso") or "",
            "desired_to_iso":payload.get("desired_to_iso") or "",
            "last_error":payload.get("last_error") or "",
            "sku_list":payload.get("sku_list") or [],
            "warehouse_name":payload.get("warehouse_name") or "",
        }
        if not t["sku_list"]:
            t["sku_list"]=[{"sku":payload.get("sku"),"total_qty":payload.get("qty") or 0,"warehouse_name":payload.get("warehouse_name") or ""}]
        out.append(t)
    out.reverse()
    clean=[]
    for t in out:
        if _sum_qty(t)==0 and _task_warehouse_name(t)=="-" and not _first_sku(t):
            continue
        clean.append(t)
    return clean

async def fetch_tasks_for_chat(chat_id:int)->List[Dict[str,Any]]:
    recent=(time.time()-LAST_PURGE_TS.get(chat_id,0))<120
    candidates=[]
    if sw: candidates += [(sw,n) for n in ("get_tasks_for_chat","list_tasks_for_chat","list_tasks","get_tasks","tasks_for_chat","dump_tasks")]
    if AUTOBOOK_ENABLED and 'abf' in globals() and abf: candidates += [(abf,n) for n in ("get_tasks_for_chat","list_tasks_for_chat","list_tasks","get_tasks","tasks_for_chat")]
    if si: candidates += [(si,n) for n in ("get_tasks_for_chat","list_tasks_for_chat","list_tasks","get_tasks","tasks_for_chat")]
    for mod,name in candidates:
        fn=getattr(mod,name,None)
        if not fn: continue
        res,err=await call_func_flexible(fn,chat_id)
        if err: continue
        tasks=normalize_tasks_result(res,chat_id)
        if tasks: return tasks
    if recent: return []
    return fallback_tasks_from_events(chat_id)

def build_tasks_list_text(tasks:List[Dict[str,Any]])->str:
    if not tasks:
        return build_html(["§§B§§Заявки (0)§§EB§§",SEP_THIN,"Активных задач нет.","","Кнопки ниже: обновить / закрыть."])
    lines=[f"§§B§§Заявки ({len(tasks)})§§EB§§",SEP_THIN]
    for i,t in enumerate(tasks,1):
        em,stage=classify_task_stage(t)
        qty=_sum_qty(t)
        date=t.get("date") or (t.get("desired_from_iso","")[:10] if t.get("desired_from_iso") else "-")
        slot=_first_time_or_dash(t)
        sku=_first_sku(t) or "-"
        tid=t.get("id") or "-"
        wh=_task_warehouse_name(t)
        lines.append(f"{i}) {em} {stage} | {qty} шт | {date} {slot} | Склад: {wh} | SKU {sku} | Task {tid}")
    lines.append(""); lines.append("Нажмите номер для деталей. Ниже — обновить / удалить все / закрыть.")
    return build_html(lines)

def build_tasks_kb(n:int)->InlineKeyboardMarkup:
    rows=[]; buf=[]
    for i in range(1,n+1):
        buf.append(InlineKeyboardButton(text=str(i),callback_data=f"tasks:detail:{i}"))
        if len(buf)==5: rows.append(buf); buf=[]
    if buf: rows.append(buf)
    rows.append([
        InlineKeyboardButton(text=f"{EMOJI_REFRESH} Обновить",callback_data="tasks:refresh"),
        InlineKeyboardButton(text="🗑 Удалить все",callback_data="tasks:purge_all"),
        InlineKeyboardButton(text="✖ Закрыть",callback_data="tasks:close"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)

async def render_tasks_list(chat_id:int, edit_message:Optional[Message]=None):
    tasks=await fetch_tasks_for_chat(chat_id)
    TASKS_CACHE[chat_id]=tasks
    text=build_tasks_list_text(tasks)
    kb=build_tasks_kb(len(tasks))
    if edit_message:
        try:
            await edit_message.edit_text(text,parse_mode="HTML",reply_markup=kb,disable_web_page_preview=True)
            return
        except Exception: pass
    await send_safe_message(chat_id,text,parse_mode="HTML",reply_markup=kb,disable_web_page_preview=True)

def build_task_detail_text(t:Dict[str,Any])->str:
    em,stage=classify_task_stage(t); qty=_sum_qty(t)
    date=t.get("date") or (t.get("desired_from_iso","")[:10] if t.get("desired_from_iso") else "-")
    slot=_first_time_or_dash(t); tid=t.get("id") or "-"; wh=_task_warehouse_name(t)
    lines=["§§B§§Детали заявки§§EB§§",
           f"ID: {tid}",
           f"Стадия: {em} {stage}",
           f"Дата: {date} | Окно: {slot}",
           f"Склад поставки: {wh}",
           f"Итого: {qty} шт",
           ""]
    sl=t.get("sku_list")
    if isinstance(sl,list):
        lines.append("Позиции:")
        for i,it in enumerate(sl,1):
            sku=it.get("sku"); q=it.get("total_qty") or it.get("qty") or 0
            name_w=it.get("warehouse_name") or "-"
            lines.append(f"{i}. SKU {sku} — {q} шт | {name_w}")
        lines.append("")
    if t.get("last_error"): lines.append(f"Ошибка: {t['last_error']}")
    return build_html(lines)

def task_detail_kb()->InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅ Назад",callback_data="tasks:refresh")],
        [InlineKeyboardButton(text="✖ Закрыть",callback_data="tasks:close")]
    ])

# ===== Full analyze =====
async def handle_analyze(chat_id:int, verbose:bool=True):
    global LAST_ANALYZE_MS,LAST_ANALYZE_ERROR
    async with ANALYZE_LOCK:
        start=time.time(); LAST_ANALYZE_ERROR=None; temp=None
        try:
            if verbose: temp=await send_safe_message(chat_id,"⚙ Анализ запасов…")
            need_snapshot=(time.time()-LAST_SNAPSHOT_TS>SNAPSHOT_STALE_MINUTES*60)
            rows,err=await ozon_stock_fbo(SKU_LIST)
            if err:
                LAST_ANALYZE_ERROR=err
                await send_safe_message(chat_id,f"Ошибка Ozon API: {html.escape(err)}")
                if temp:
                    try: await temp.delete()
                    except Exception: pass
                return
            if need_snapshot and time.time()-LAST_SNAPSHOT_TS>SNAPSHOT_MIN_REUSE_SECONDS:
                append_snapshot(rows); await flush_history_if_needed(force=True)
            to_fetch=skus_needing_names()
            if to_fetch:
                prev=len(SKU_NAME_CACHE); mp,_=await ozon_product_names_by_sku(to_fetch)
                SKU_NAME_CACHE.update(mp); save_cache_if_needed(prev)
            ccache=build_consumption_cache()
            report,flat=generate_deficit_report(rows,SKU_NAME_CACHE,ccache)
            LAST_DEFICIT_CACHE[chat_id]={"flat":flat,"timestamp":int(time.time()),"report":report,"raw_rows":rows,"consumption_cache":ccache}
            try: build_fact_index(rows,flat,ccache)
            except Exception as e: log.warning("FACT_INDEX build error: %s", e)
            kb=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Все",callback_data="filter:all"),
                 InlineKeyboardButton(text="Критично",callback_data="filter:crit"),
                 InlineKeyboardButton(text="50–80%",callback_data="filter:mid")],
                [InlineKeyboardButton(text=f"{EMOJI_REFRESH} Обновить",callback_data="action:reanalyze")],
                [InlineKeyboardButton(text="Автобронирование",callback_data="menu_autobook")],
            ])
            await send_long(chat_id,report,kb=kb if flat else None)
            if temp:
                try: await temp.delete()
                except Exception: pass
        except Exception as e:
            LAST_ANALYZE_ERROR=str(e)
            log.exception("Analyze error")
            await send_safe_message(chat_id,f"❌ Ошибка анализа: {html.escape(str(e))}")
        finally:
            LAST_ANALYZE_MS=(time.time()-start)*1000
            await flush_history_if_needed()

# ===== Snapshot jobs =====
async def snapshot_job():
    if time.time()-LAST_SNAPSHOT_TS<SNAPSHOT_MIN_REUSE_SECONDS: return
    rows,err=await ozon_stock_fbo(SKU_LIST)
    if err or not rows: return
    append_snapshot(rows)
    to_fetch=skus_needing_names()
    if to_fetch:
        prev=len(SKU_NAME_CACHE); mp,_=await ozon_product_names_by_sku(to_fetch)
        SKU_NAME_CACHE.update(mp); save_cache_if_needed(prev)
    try:
        ccache=build_consumption_cache(); build_fact_index(rows,[],ccache)
    except Exception as e: log.warning("snapshot index build fail: %s", e)
    await flush_history_if_needed(force=True)

async def daily_notify_job():
    if ADMIN_ID is None: return
    await ensure_fact_index()
    async with ANALYZE_LOCK:
        rows,err=await ozon_stock_fbo(SKU_LIST)
        if err:
            await send_safe_message(ADMIN_ID,f"Ошибка Ozon API: {html.escape(err)}"); return
        if time.time()-LAST_SNAPSHOT_TS>SNAPSHOT_MIN_REUSE_SECONDS:
            append_snapshot(rows); await flush_history_if_needed(force=True)
        ccache=build_consumption_cache()
        to_fetch=skus_needing_names()
        if to_fetch:
            prev=len(SKU_NAME_CACHE); mp,_=await ozon_product_names_by_sku(to_fetch)
            SKU_NAME_CACHE.update(mp); save_cache_if_needed(prev)
        report,flat=generate_deficit_report(rows,SKU_NAME_CACHE,ccache)
        LAST_DEFICIT_CACHE[ADMIN_ID]={"flat":flat,"timestamp":int(time.time()),"report":report,"raw_rows":rows,"consumption_cache":ccache}
        try: build_fact_index(rows,flat,ccache)
        except Exception as e: log.warning("FACT_INDEX daily build fail: %s", e)
        header=f"{EMOJI_NOTIFY} §§B§§Ежедневный отчёт {DAILY_NOTIFY_HOUR:02d}:{DAILY_NOTIFY_MINUTE:02d}§§EB§§\n"
        kb=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Все",callback_data="filter:all"),
             InlineKeyboardButton(text="Критично",callback_data="filter:crit"),
             InlineKeyboardButton(text="50–80%",callback_data="filter:mid")],
            [InlineKeyboardButton(text=f"{EMOJI_REFRESH} Обновить",callback_data="action:reanalyze")],
        ])
        await send_long(ADMIN_ID, header+report, kb=kb)
        await flush_history_if_needed()

async def maintenance_job():
    prune_history()
    await flush_history_if_needed()

async def init_snapshot():
    rows,err=await ozon_stock_fbo(SKU_LIST)
    if err or not rows: return
    append_snapshot(rows)
    try:
        ccache=build_consumption_cache(); build_fact_index(rows,[],ccache)
    except Exception as e: log.warning("init index build fail: %s", e)
    await flush_history_if_needed(force=True)

# ===== Cluster keyboard =====
def cluster_view_kb(cname:str, short:bool)->InlineKeyboardMarkup:
    rows=[]
    rows.append([
        InlineKeyboardButton(text=("✅ Коротко" if short else "Коротко"), callback_data=f"cluster_view:short:{cname}"),
        InlineKeyboardButton(text=("✅ Подробно" if not short else "Подробно"), callback_data=f"cluster_view:full:{cname}"),
    ])
    rows.append([InlineKeyboardButton(text="⬅ К списку кластеров", callback_data="clusters:list")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

# ===== Callbacks (filters etc.) remain same except ensure_fact_index where needed =====
@dp.callback_query(F.data.startswith("stockpage:"))
async def cb_stock_page(c:CallbackQuery):
    ensure_admin(c.from_user.id)
    try: page=int(c.data.split(":")[1])
    except Exception: await c.answer(); return
    to_fetch=skus_needing_names()
    if to_fetch:
        prev=len(SKU_NAME_CACHE); mp,_=await ozon_product_names_by_sku(to_fetch)
        SKU_NAME_CACHE.update(mp); save_cache_if_needed(prev)
    total=len(SKU_LIST)
    if total==0:
        kb=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Нет SKU",callback_data="noop")]])
    else:
        pages=(total+STOCK_PAGE_SIZE-1)//STOCK_PAGE_SIZE
        page=max(0,min(page,pages-1))
        start=page*STOCK_PAGE_SIZE; end=min(start+STOCK_PAGE_SIZE,total)
        buttons=[]
        for sku in SKU_LIST[start:end]:
            nm=SKU_NAME_CACHE.get(sku,f"SKU {sku}")
            buttons.append([InlineKeyboardButton(text=f"{nm[:48]} (SKU {sku})",callback_data=f"sku:{sku}")])
        nav=[]
        if page>0: nav.append(InlineKeyboardButton(text="«",callback_data=f"stockpage:{page-1}"))
        nav.append(InlineKeyboardButton(text=f"{page+1}/{pages}",callback_data="noop"))
        if page<pages-1: nav.append(InlineKeyboardButton(text="»",callback_data=f"stockpage:{page+1}"))
        buttons.append(nav)
        buttons.append([InlineKeyboardButton(text="Автобронирование",callback_data="menu_autobook")])
        kb=InlineKeyboardMarkup(inline_keyboard=buttons)
    try: await c.message.edit_reply_markup(reply_markup=kb)
    except Exception: await c.message.answer("Товары:",reply_markup=kb)
    await c.answer()

@dp.callback_query(F.data=="noop")
async def cb_noop(c:CallbackQuery): await c.answer()

@dp.callback_query(F.data=="action:reanalyze")
async def cb_reanalyze(c:CallbackQuery):
    ensure_admin(c.from_user.id)
    await handle_analyze(c.message.chat.id, verbose=False)
    await c.answer()

@dp.callback_query(F.data.startswith("filter:"))
async def cb_filter(c:CallbackQuery):
    ensure_admin(c.from_user.id)
    mode=c.data.split(":")[1]
    cache=LAST_DEFICIT_CACHE.get(c.message.chat.id)
    if not cache:
        await c.answer("Анализ..."); await handle_analyze(c.message.chat.id, verbose=False); return
    flat=cache["flat"]
    if not flat:
        await c.message.answer(f"{EMOJI_OK} Дефициты не найдены."); await c.answer(); return
    view_mode=BOT_STATE.get("view_mode",DEFAULT_VIEW_MODE); full=view_mode=="FULL"
    def pick(lst,mode):
        if mode=="crit": return [d for d in lst if d["coverage"]<0.5]
        if mode=="mid": return [d for d in lst if 0.5<=d["coverage"]<0.8]
        return lst
    f2=pick(flat,mode)
    if not f2:
        rep=build_html([f"{EMOJI_OK} Нет позиций категории {mode}."])
    else:
        per={}
        for d in f2: per.setdefault(d["sku"],[]).append(d)
        sku_order=sorted(per.keys(), key=lambda s:min(x["coverage"] for x in per[s]))
        lines=[f"{EMOJI_ANALYZE} §§B§§Фильтр: {mode}§§EB§§",SEP_BOLD]
        for sku in sku_order:
            items=sorted(per[sku], key=lambda x:x["coverage"])
            pname=items[0]["name"]
            lines.append(f"§§B§§{pname} (SKU {sku})§§EB§§")
            total_need=sum(i["need"] for i in items); total_qty=sum(i["qty"] for i in items)
            for i in items:
                bar,sev=coverage_bar(i["coverage"])
                badge=need_pct_text(i["qty"], i["norm"], i["target"])
                wh_b=bold(i['warehouse_name'])
                if full:
                    lines.append(f"• {wh_b}: Остаток {i['qty']} / Норма {i['norm']} / Цель {i['target']} → +{i['need']}\n  {bar} {sev} · {badge}")
                else:
                    lines.append(f"• {wh_b}: Остаток {i['qty']} → +{i['need']} {bar} · {badge}")
            lines.append(f"  Σ Остаток={total_qty}, Потребность={total_need}")
            lines.append(SEP_THIN)
        lines.append(f"{EMOJI_TARGET} Показано товаров={len(per)}, строк={len(f2)}, режим={view_mode}")
        rep=build_html(lines)
    kb=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Все",callback_data="filter:all"),
         InlineKeyboardButton(text="Критично",callback_data="filter:crit"),
         InlineKeyboardButton(text="50–80%",callback_data="filter:mid")],
        [InlineKeyboardButton(text=f"{EMOJI_REFRESH} Обновить",callback_data="action:reanalyze")],
        [InlineKeyboardButton(text="Автобронирование",callback_data="menu_autobook")],
    ])
    await send_long(c.message.chat.id, rep, kb=kb)
    await c.answer()

@dp.callback_query(F.data.startswith("sku:"))
async def cb_sku(c:CallbackQuery):
    ensure_admin(c.from_user.id)
    try: sku=int(c.data.split(":")[1])
    except Exception: await c.answer(); return
    rows, err=await ozon_stock_fbo(SKU_LIST)
    if err:
        await c.message.answer(f"Ошибка Ozon API: {html.escape(err)}"); await c.answer(); return
    ccache=build_consumption_cache()
    agg=aggregate_rows(rows)
    if sku not in agg:
        await c.message.answer("Нет данных по этому товару."); await c.answer(); return
    if sku not in SKU_NAME_CACHE or SKU_NAME_CACHE[sku].startswith("SKU "):
        prev=len(SKU_NAME_CACHE); mp,_=await ozon_product_names_by_sku([sku]); SKU_NAME_CACHE.update(mp); save_cache_if_needed(prev)
    name=SKU_NAME_CACHE.get(sku,f"SKU {sku}")
    lines=[f"{EMOJI_BOX} §§B§§{name} (SKU {sku})§§EB§§", SEP_THIN]
    for wkey, info in sorted(agg[sku].items()):
        qty=info["qty"]; st=evaluate_position_cached(sku,wkey,qty,ccache)
        cov=qty/st["norm"] if st["norm"] else 0
        bar, sev=coverage_bar(cov)
        status=EMOJI_WARN if st["is_low"] else EMOJI_OK
        hist="(история)" if st["history_used"] else "(минимум)"
        badge=need_pct_text(qty, st["norm"], st["target"])
        lines.append(f"• {bold(info['warehouse_name'])}: Остаток {qty} / Норма {st['norm']} / Цель {st['target']} {status}\n  {bar} {sev} {hist} · {badge}")
    await send_long(c.message.chat.id, build_html(lines))
    await c.answer()

@dp.callback_query(F.data.startswith("whid:"))
async def cb_whid(c:CallbackQuery):
    ensure_admin(c.from_user.id)
    hid=c.data.split(":",1)[1]
    pair=WAREHOUSE_CB_MAP.get(hid)
    if not pair:
        await c.answer("Обновляю…")
        await cmd_warehouses(c.message)
        return
    wkey, wname = pair
    rows, err=await ozon_stock_fbo(SKU_LIST)
    if err:
        await c.message.answer(f"Ошибка Ozon API: {html.escape(err)}"); await c.answer(); return
    ccache=build_consumption_cache()
    agg=aggregate_rows(rows)
    lines=[f"{EMOJI_WH} §§B§§Склад {wname}§§EB§§", SEP_THIN]
    present=False
    items=[]
    for sku in agg.keys():
        if wkey in agg[sku]:
            present=True
            info=agg[sku][wkey]
            if sku not in SKU_NAME_CACHE or SKU_NAME_CACHE[sku].startswith("SKU "):
                prev=len(SKU_NAME_CACHE); mp,_=await ozon_product_names_by_sku([sku]); SKU_NAME_CACHE.update(mp); save_cache_if_needed(prev)
            nm=SKU_NAME_CACHE.get(sku,f"SKU {sku}")
            qty=info["qty"]; st=evaluate_position_cached(sku,wkey,qty,ccache)
            cov=qty/st["norm"] if st["norm"] else 0
            items.append((cov, st["need"], sku, nm, qty, st))
    items.sort(key=lambda x:(x[0], -x[1]))
    for cov, need, sku, nm, qty, st in items:
        bar, sev=coverage_bar(cov)
        badge=need_pct_text(qty, st["norm"], st["target"])
        status=EMOJI_WARN if st["is_low"] else EMOJI_OK
        lines.append(f"{bold(nm)} (SKU {sku}): Остаток {qty} / Норма {st['norm']} / Цель {st['target']} {status}\n  {bar} {sev} · {badge}")
    if not present: lines.append("Нет данных по складу.")
    await send_long(c.message.chat.id, build_html(lines))
    await c.answer()

@dp.callback_query(F.data.startswith("cluster:"))
async def cb_cluster(c:CallbackQuery):
    ensure_admin(c.from_user.id)
    cname=c.data.split(":",1)[1]
    await ensure_fact_index()
    cl_sec=FACT_INDEX.get("cluster",{}); sku_sec=FACT_INDEX.get("sku",{})
    if not cl_sec:
        await c.message.answer("Нет данных кластеров. Запустите /analyze.")
        await c.answer(); return
    short = BOT_STATE.get("cluster_view_mode","full")=="short"
    rep=build_cluster_detail(cname,cl_sec,sku_sec, short=short)
    kb=cluster_view_kb(cname, short)
    await send_long(c.message.chat.id, rep, kb=kb)
    await c.answer()

@dp.callback_query(F.data.startswith("cluster_view:"))
async def cb_cluster_view(c:CallbackQuery):
    ensure_admin(c.from_user.id)
    try:
        _, mode, cname = c.data.split(":", 2)
    except ValueError:
        await c.answer(); return
    BOT_STATE["cluster_view_mode"] = "short" if mode=="short" else "full"
    save_state()
    cl_sec=FACT_INDEX.get("cluster",{}); sku_sec=FACT_INDEX.get("sku",{})
    rep=build_cluster_detail(cname, cl_sec, sku_sec, short=(mode=="short"))
    kb=cluster_view_kb(cname, short=(mode=="short"))
    try:
        await c.message.edit_text(rep, parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True)
    except Exception:
        await send_long(c.message.chat.id, rep, kb=kb)
    await c.answer("Режим изменён")

@dp.callback_query(F.data=="clusters:list")
async def cb_clusters_list(c:CallbackQuery):
    ensure_admin(c.from_user.id)
    await cmd_clusters(c.message)
    await c.answer()

@dp.callback_query(F.data=="chatmode:toggle")
async def cb_chatmode_toggle(c:CallbackQuery):
    ensure_admin(c.from_user.id)
    current=BOT_STATE.get("chat_mode","fact")
    BOT_STATE["chat_mode"]="general" if current=="fact" else "fact"
    save_state()
    await c.answer(f"Режим: {BOT_STATE['chat_mode'].upper()}")

# ===== Автобронирование fallback/external =====
if AUTOBOOK_ENABLED:
    @dp.message(Command("autobook"))
    async def cmd_autobook_external(m:Message):
        ensure_admin(m.from_user.id)
        kb=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Начать",callback_data="menu_autobook")]])
        await m.answer("🧩 Внешний мастер автобронирования.", reply_markup=kb)

# ===== Main menu / commands =====
def main_menu_kb()->ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🔧 Автобронирование"),KeyboardButton(text=f"{EMOJI_LIST} Заявки"),KeyboardButton(text=f"{EMOJI_TASKS} Задачи")],
        [KeyboardButton(text="🔍 Анализ"),KeyboardButton(text="📣 Отчёт сейчас")],
        [KeyboardButton(text="📦 Товары"),KeyboardButton(text="🏬 Склады"),KeyboardButton(text="🗺 Кластеры")],
        [KeyboardButton(text="⚙ Режим отображения"),KeyboardButton(text="🧪 Диагностика"),KeyboardButton(text="🔄 Сброс кэша")],
        [KeyboardButton(text="🤖 AI чат"),KeyboardButton(text="❌ Отмена")],
    ], resize_keyboard=True)

def start_overview()->str:
    rows=[
        ("🔍 Анализ","Пересчёт дефицитов"),
        ("📣 Отчёт сейчас","Быстрый срез"),
        ("📦 Товары","Список SKU"),
        ("🏬 Склады","Остатки / дефицит"),
        ("🗺 Кластеры","Группы складов"),
        ("📄 Заявки","Последние заявки"),
        ("📋 Задачи","Автобронирование"),
        ("⚙ Режим отображения","FULL / COMPACT"),
        ("🧪 Диагностика","Состояние / топы"),
        ("🔄 Сброс кэша","Очистить имена SKU"),
        ("🤖 AI чат","FACT / GENERAL"),
        ("🔧 Автобронирование","Мастер поставки"),
        ("❌ Отмена","Выход из AI чата"),
    ]
    lines=[]
    header=f"{'═'*22}  {EMOJI_INFO} ОБЗОР  {'═'*22}"
    lines.append(header)
    lines.append(f"Версия: {VERSION} | ChatMode={BOT_STATE.get('chat_mode','?').upper()} | Style={'ON' if BOT_STATE.get('style_enabled') else 'OFF'} | ClusterView={BOT_STATE.get('cluster_view_mode')}")
    lines.append(SEP_THIN)
    cmds=["autobook","analyze","stock","warehouses","clusters","supplies","diag","ai","ask","chat_mode","style_toggle","cluster_map","ai_reset_token"]
    lines.append("Команды: "+ " /".join(f"/{c}" for c in cmds))
    lines.append(SEP_THIN)
    ml=max(len(k) for k,_ in rows)
    for k,d in rows:
        lines.append(f"{k}{' '*(ml-len(k))} │ {d}")
    lines.append(SEP_THIN)
    lines.append(f"Автобронирование: {'external' if AUTOBOOK_ENABLED else 'fallback'}")
    lines.append("AI: FACT (по данным) / GENERAL (общий).")
    lines.append("═"*len(header))
    return build_html(lines)

def version_info()->str:
    import sys
    return (f"Версия: {VERSION}\nPython: {sys.version.split()[0]}\nSnapshots: {len(HISTORY_CACHE)}\n"
            f"SKU index: {len(FACT_INDEX.get('sku', {}))}\nClusters: {len(FACT_INDEX.get('cluster', {}))}\n"
            f"ChatMode: {BOT_STATE.get('chat_mode')} Style:{BOT_STATE.get('style_enabled')} ClusterView:{BOT_STATE.get('cluster_view_mode')} "
            f"Autobook={'external' if AUTOBOOK_ENABLED else 'fallback'}")

def build_diag_report()->str:
    inv=FACT_INDEX.get("inventory_overview",{})
    sku_section=FACT_INDEX.get("sku",{})
    top_def=FACT_INDEX.get("top_deficits",[])
    top_wh=FACT_INDEX.get("top_warehouses",[])
    top_cl=FACT_INDEX.get("top_clusters",[])
    cov={"<25":0,"25-50":0,"50-80":0,"80-100":0,"100+":0}
    for _,info in sku_section.items():
        cv=info["worst_coverage"]
        if cv<0.25: cov["<25"]+=1
        elif cv<0.5: cov["25-50"]+=1
        elif cv<0.8: cov["50-80"]+=1
        elif cv<1: cov["80-100"]+=1
        else: cov["100+"]+=1
    s=lambda t:f"§§B§§{t}§§EB§§"
    lines=[f"{EMOJI_DIAG} {s('Диагностика')} ({time.strftime('%H:%M:%S')})",SEP_BOLD,
           f"{EMOJI_INFO} Версия: {VERSION}",
           f"{EMOJI_CLOUD} Снимок: {FACT_INDEX.get('snapshot_ts','-')} | SKU: {inv.get('total_sku','-')} | Кластеров: {len(FACT_INDEX.get('cluster',{}))}",
           f"{EMOJI_CHAT} ChatMode={BOT_STATE.get('chat_mode')} | Style={'ON' if BOT_STATE.get('style_enabled') else 'OFF'} | Autobook={'external' if AUTOBOOK_ENABLED else 'fallback'}",
           "", s("Покрытие (худшее по SKU)"), SEP_THIN,
           f"<25%: {cov['<25']} | 25–50%: {cov['25-50']} | 50–80%: {cov['50-80']} | 80–100%: {cov['80-100']} | ≥100%: {cov['100+']}",
           "", s("Топ дефицитных SKU"), SEP_THIN]
    if top_def:
        for td in top_def[:DIAG_TOP_DEFICITS]:
            lines.append(f"• {td['name'][:40]} (SKU {td['sku']}) покрытие {td['coverage']:.2f} потребность {td['deficit_need']}")
    else:
        lines.append("Нет дефицита.")
    lines.append("")
    lines+=[s("Склады с потребностью"), SEP_THIN]
    if top_wh:
        for w in top_wh[:DIAG_TOP_WAREHOUSES]:
            lines.append(f"• {bold(w['name'][:40])}: потребность до цели {w['total_need']}, дефицит {w['deficit_need']}")
    else:
        lines.append("Нет данных.")
    lines.append("")
    lines+=[s("Кластеры с потребностью"), SEP_THIN]
    if top_cl:
        for c in top_cl[:DIAG_TOP_CLUSTERS]:
            lines.append(f"• {bold(c['name'][:40])}: потребность до цели {c['total_need']}, дефицит {c['deficit_need']}")
    else:
        lines.append("Нет данных.")
    lines.append("")
    lines+=[s("Производительность"), SEP_THIN,
            f"API: {LAST_API_LATENCY_MS:.0f} мс | Анализ: {LAST_ANALYZE_MS:.0f} мс | Ошибка: {LAST_ANALYЗЕ_ERROR or '—'}",
            f"Snapshots: {len(HISTORY_CACHE)} | Кэш AI: {len(ANSWER_CACHE)}"]
    return build_html(lines)

def build_supplies_last_created(chat_id:int, limit:int=2)->str:
    arr=SUPPLY_EVENTS.get(str(chat_id)) or []
    if not arr: return build_html([f"{EMOJI_LIST} Нет событий по заявкам."])
    created=[]
    for e in reversed(arr):
        text=(e.get("text") or "")
        status=(e.get("status") or "")
        if "создан" in text.lower() or status.lower() in ("создано","created","done"):
            created.append(e)
            if len(created)>=limit: break
    if not created: return build_html([f"{EMOJI_LIST} Пока нет созданных заявок."])
    lines=[f"{EMOJI_LIST} §§B§§Последние заявки ({len(created)})§§EB§§",SEP_THIN]
    for e in created:
        ts=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(e.get("ts",int(time.time()))))
        txt=(e.get("text") or "").strip()
        payload=e.get("payload") or {}; tid=payload.get("id") or payload.get("task_id") or "-"
        lines.append(f"[{ts}] {txt} | ID {tid}")
    return build_html(lines)

# ===== Commands =====
def ensure_admin(uid:int):
    global ADMIN_ID
    if ADMIN_ID is None:
        ADMIN_ID=uid

@dp.message(Command("version"))
async def cmd_version(m:Message):
    ensure_admin(m.from_user.id)
    await m.answer(version_info())

@dp.message(Command("help"))
@dp.message(Command("start"))
async def cmd_start(m:Message):
    ensure_admin(m.from_user.id)
    # тихо строим индекс, чтобы /clusters сразу работал
    await ensure_fact_index()
    await m.answer(f"{EMOJI_OK} Бот активен. Версия {VERSION}.", reply_markup=main_menu_kb())
    kb=[[InlineKeyboardButton(text="Автобронирование",callback_data="menu_autobook")]]
    await send_long(m.chat.id, start_overview(), kb=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.message(Command("supplies"))
async def cmd_supplies(m:Message):
    ensure_admin(m.from_user.id)
    rep=build_supplies_last_created(m.chat.id,2)
    await send_long(m.chat.id, rep)

@dp.message(Command("cluster_map"))
async def cmd_cluster_map(m:Message):
    ensure_admin(m.from_user.id)
    if CLUSTER_MAP:
        lines=["§§B§§Карта кластеров (ENV)§§EB§§",SEP_THIN]
        for k,v in CLUSTER_MAP.items(): lines.append(f"{k} → {v}")
    else:
        lines=["§§B§§ENV не задан — эвристика§§EB§§","Неопознанные склады → 'Прочие'"]
    await send_long(m.chat.id, build_html(lines))

@dp.message(Command("health"))
async def cmd_health(m:Message):
    ensure_admin(m.from_user.id)
    warn=[]
    if LAST_API_LATENCY_MS>HEALTH_WARN_LATENCY_MS: warn.append("API медленно")
    if LAST_ANALYZE_MS>HEALTH_WARN_LATENCY_MS: warn.append("Анализ медленно")
    status="OK" if not warn else " | ".join(warn)
    lines=["§§B§§Health§§EB§§",
           f"API {LAST_API_LATENCY_MS:.0f} мс · Анализ {LAST_ANALYZE_MS:.0f} мс",
           f"Snapshots={len(HISTORY_CACHE)} SKU_index={len(FACT_INDEX.get('sku',{}))} Clusters={len(FACT_INDEX.get('cluster',{}))}",
           f"ChatMode={BOT_STATE.get('chat_mode')} Style={'ON' if BOT_STATE.get('style_enabled') else 'OFF'} ClusterView={BOT_STATE.get('cluster_view_mode')} Autobook={'external' if AUTOBOOK_ENABLED else 'fallback'}",
           f"Статус: {status}"]
    await send_long(m.chat.id, build_html(lines))

@dp.message(Command("view_mode"))
async def cmd_view_mode(m:Message):
    ensure_admin(m.from_user.id)
    BOT_STATE["view_mode"]="COMPACT" if BOT_STATE.get("view_mode")=="FULL" else "FULL"; save_state()
    await m.answer(f"Режим: {BOT_STATE['view_mode']}")

@dp.message(Command("style_toggle"))
async def cmd_style_toggle(m:Message):
    ensure_admin(m.from_user.id)
    BOT_STATE["style_enabled"]=not BOT_STATE.get("style_enabled",True); save_state()
    await m.answer(f"Стилизация AI: {'ON' if BOT_STATE['style_enabled'] else 'OFF'}")

@dp.message(Command("chat_mode"))
async def cmd_chat_mode(m:Message):
    ensure_admin(m.from_user.id); mode=BOT_STATE.get("chat_mode","fact").upper()
    kb=[[InlineKeyboardButton(text="🔁 Переключить",callback_data="chatmode:toggle")]]
    await m.answer(build_html(["§§B§§Режим чата§§EB§§",f"Текущий: {mode}","/fact /general или кнопка."]), reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.message(Command("fact"))
async def cmd_fact(m:Message):
    ensure_admin(m.from_user.id); BOT_STATE["chat_mode"]="fact"; save_state(); await m.answer("FACT")

@dp.message(Command("general"))
async def cmd_general(m:Message):
    ensure_admin(m.from_user.id); BOT_STATE["chat_mode"]="general"; save_state(); await м.answer("GENERAL")

@dp.message(Command("chat"))
async def cmd_chat(m:Message):
    ensure_admin(m.from_user.id); q=m.text.partition(" ")[2].strip()
    if not q: await m.answer("Формат: /chat <сообщение>"); return
    await m.answer(f"{EMOJI_CHAT} Общаюсь…")
    raw,mode=await llm_general_answer(m.chat.id,q); styled=style_ai_answer(q,raw,mode,False)
    await send_long(m.chat.id, styled)

@dp.message(Command("refresh"))
async def cmd_refresh(m:Message):
    ensure_admin(m.from_user.id); SKU_NAME_CACHE.clear(); save_cache_if_needed(0); await m.answer("Кэш SKU очищён.")

@dp.message(Command("analyze"))
async def cmd_analyze(m:Message):
    ensure_admin(m.from_user.id)
    await handle_analyze(m.chat.id)

@dp.message(Command("force_notify"))
async def cmd_force_notify(m:Message):
    ensure_admin(m.from_user.id); await m.answer("Отчёт…"); await daily_notify_job(); await m.answer("Готово.")

@dp.message(Command("diag"))
async def cmd_diag(m:Message):
    ensure_admin(m.from_user.id)
    await ensure_fact_index()
    rep=build_diag_report(); await send_long(m.chat.id, rep)

@dp.message(Command("diag_env"))
async def cmd_diag_env(m:Message):
    ensure_admin(m.from_user.id)
    def mask(v:str)->str:
        if not v: return "(empty)"
        if len(v)<8: return v[0]+"***"
        return v[:4]+"****"+v[-4:]
    lines=["§§B§§ENV ключевые§§EB§§",
           f"VERSION={VERSION}",
           f"OZON_CLIENT_ID={'yes' if OZON_CLIENT_ID else 'no'}",
           f"OZON_API_KEY={mask(OZON_API_KEY)}",
           f"LLM_PROVIDER={LLM_PROVIDER}",
           f"GIGACHAT_SCOPE={GIGACHAT_SCOPE}",
           f"CHAT_MODE={BOT_STATE.get('chat_mode')}",
           f"STYLE={'ON' if BOT_STATE.get('style_enabled') else 'OFF'}",
           f"CLUSTER_VIEW={BOT_STATE.get('cluster_view_mode')}",
           f"INVENTORY_SAMPLE={LLM_INVENTORY_SAMPLE_SKU}",
           f"FACT_SOFT_LIMIT={LLM_FACT_SOFT_LIMIT_CHARS}",
           f"STOCK_PAGE_SIZE={STOCK_PAGE_SIZE}",
           f"CLUSTER_COUNT={len(FACT_INDEX.get('cluster',{}))}",
           f"AUTOBOOK={'external' if AUTOBOOK_ENABLED else 'fallback'}"]
    await send_long(m.chat.id, build_html(lines))

@dp.message(Command("stock"))
async def cmd_stock(m:Message):
    ensure_admin(m.from_user.id)
    to_fetch=skus_needing_names()
    if to_fetch:
        prev=len(SKU_NAME_CACHE); mp,_=await ozon_product_names_by_sku(to_fetch)
        SKU_NAME_CACHE.update(mp); save_cache_if_needed(prev)
    total=len(SKU_LIST)
    if total==0:
        kb=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Нет SKU",callback_data="noop")]])
    else:
        pages=(total+STOCK_PAGE_SIZE-1)//STOCK_PAGE_SIZE
        page=0; start=0; end=min(start+STOCK_PAGE_SIZE,total)
        btn=[]
        for sku in SKU_LIST[start:end]:
            nm=SKU_NAME_CACHE.get(sku,f"SKU {sku}")
            btn.append([InlineKeyboardButton(text=f"{nm[:48]} (SKU {sku})",callback_data=f"sku:{sku}")])
        nav=[InlineKeyboardButton(text=f"1/{pages}",callback_data="noop")]
        btn.append(nav)
        btn.append([InlineKeyboardButton(text="Автобронирование",callback_data="menu_autobook")])
        kb=InlineKeyboardMarkup(inline_keyboard=btn)
    await m.answer(f"{EMOJI_BOX} Товары:", reply_markup=kb)

@dp.message(Command("warehouses"))
async def cmd_warehouses(m:Message):
    ensure_admin(m.from_user.id)
    rows, err=await ozon_stock_fbo(SKU_LIST)
    if err: await m.answer(f"Ошибка Ozon API: {html.escape(err)}"); return
    agg=aggregate_rows(rows)
    wh_map={}
    for wmap in agg.values():
        for wk,info in wmap.items():
            wh_map.setdefault(wk,info["warehouse_name"])
    if not wh_map: await m.answer("Нет данных."); return
    kb_rows=[]
    for wk,nm in sorted(wh_map.items(), key=lambda x:x[1].lower()):
        hid=hashlib.sha1(str(wk).encode()).hexdigest()[:10]
        WAREHOUSE_CB_MAP[hid]=(wk,nm)
        kb_rows.append([InlineKeyboardButton(text=nm[:60],callback_data=f"whid:{hid}")])
    kb_rows.append([InlineKeyboardButton(text="Автобронирование",callback_data="menu_autobook")])
    kb=InlineKeyboardMarkup(inline_keyboard=kb_rows)
    await m.answer(f"{EMOJI_WH} Склады:", reply_markup=kb)

@dp.message(Command("clusters"))
async def cmd_clusters(m:Message):
    ensure_admin(m.from_user.id)
    await ensure_fact_index()
    if not FACT_INDEX.get("cluster"):
        await m.answer("Нет данных кластеров. Запустите /analyze.")
        return
    kb=[]
    # Список кластеров без подробностей
    for cname in sorted(FACT_INDEX["cluster"].keys(), key=lambda c: FACT_INDEX["cluster"][c]["deficit_need"], reverse=True):
        kb.append([InlineKeyboardButton(text=cname[:40],callback_data=f"cluster:{cname}")])
    kb.append([InlineKeyboardButton(text="Автобронирование",callback_data="menu_autobook")])
    await m.answer(f"{EMOJI_CLUSTER} Кластеры:", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.message(Command("ai"))
async def cmd_ai(m:Message):
    ensure_admin(m.from_user.id)
    q=m.text.partition(" ")[2].strip() or "Покажи картину по дефициту"
    await ensure_fact_index()
    await m.answer(f"{EMOJI_AI} Анализирую…")
    raw,mode=await llm_fact_answer(q)
    styled=style_ai_answer(q,raw,mode,True)
    await send_long(m.chat.id, styled)

@dp.message(Command("ask"))
async def cmd_ask(m:Message):
    ensure_admin(m.from_user.id)
    q=m.text.partition(" ")[2].strip()
    if not q: await m.answer("Формат: /ask <вопрос>"); return
    await ensure_fact_index()
    await m.answer(f"{EMOJI_AI} Анализ фактов…")
    raw,mode=await llm_fact_answer(q); styled=style_ai_answer(q,raw,mode,True)
    await send_long(m.chat.id, styled)

@dp.message(Command("ask_raw"))
async def cmd_ask_raw(m:Message):
    ensure_admin(m.from_user.id)
    await ensure_fact_index()
    dump=json.dumps(FACT_INDEX, ensure_ascii=False)
    if len(dump)>3900: dump=dump[:3900]+"...(усечено)"
    await send_long(m.chat.id, build_html(["FACT_INDEX (усечено):", dump]))

@dp.message(Command("ai_scope"))
async def cmd_ai_scope(m:Message):
    ensure_admin(m.from_user.id)
    tok=_GIGACHAT_TOKEN_MEM; ttl=int(tok["expires_epoch"]-time.time()) if tok and tok.get("expires_epoch") else -1
    insecure=_gigachat_verify_param() is False
    lines=["§§B§§GigaChat статус§§EB§§",
           f"Enabled={GIGACHAT_ENABLED} ChatMode={BOT_STATE.get('chat_mode')}",
           f"CID_len={len(GIGACHAT_CLIENT_ID)} SEC_len={len(GIGACHAT_CLIENT_SECRET)} Scope={GIGACHAT_SCOPE}",
           f"Token={'yes' if tok else 'no'} TTL={ttl if ttl>=0 else '-'}",
           f"SSL_mode={os.getenv('GIGACHAT_SSL_MODE','auto')}{' (INSECURE)' if insecure else ''}",
           f"Index SKU={len(FACT_INDEX.get('sku',{}))} Clusters={len(FACT_INDEX.get('cluster',{}))}",
           f"Answer cache={len(ANSWER_CACHE)}"]
    await send_long(m.chat.id, build_html(lines))

@dp.message(Command("ai_reset_token"))
async def cmd_ai_reset_token(m:Message):
    ensure_admin(m.from_user.id)
    global _GIGACHAT_TOKEN_MEM
    _GIGACHAT_TOKEN_MEM={}
    if GIGACHAT_TOKEN_CACHE_FILE.exists():
        try: GIGACHAT_TOKEN_CACHE_FILE.unlink()
        except Exception: pass
    await m.answer("Токен сброшен.")

# ===== FSM AI чат =====
@dp.message(F.text=="🤖 AI чат")
async def btn_ai_chat(m:Message,state:FSMContext):
    ensure_admin(m.from_user.id); await ensure_fact_index()
    await state.set_state(AIChatState.waiting)
    mode=BOT_STATE.get("chat_mode","fact")
    await m.answer(f"AI чат включён. Режим: {mode.upper()}.\nНапишите вопрос.\nКоманды: /fact /general /cancel", reply_markup=main_menu_kb())

@dp.message(AIChatState.waiting)
async def ai_chat_waiting(m:Message,state:FSMContext):
    ensure_admin(m.from_user.id)
    q=(m.text or "").strip()
    if not q: await m.answer("Пришлите текст."); return
    if q.lower()=="/cancel" or q=="❌ Отмена":
        await state.clear(); await m.answer("AI чат закрыт.", reply_markup=main_menu_kb()); return
    if q.lower()=="/fact":
        BOT_STATE["chat_mode"]="fact"; save_state(); await m.answer("FACT режим."); return
    if q.lower()=="/general":
        BOT_STATE["chat_mode"]="general"; save_state(); await m.answer("GENERAL режим."); return
    mode=BOT_STATE.get("chat_mode","fact")
    try:
        if mode=="fact":
            await ensure_fact_index()
            raw,ai_mode=await llm_fact_answer(q)
            styled=style_ai_answer(q,raw,ai_mode,True)
        else:
            raw,ai_mode=await llm_general_answer(m.chat.id,q)
            styled=style_ai_answer(q,raw,ai_mode,False)
        await send_long(m.chat.id, styled)
    except Exception as e:
        log.exception("ai_chat error"); await m.answer(f"Ошибка AI: {e}")

# ===== Task buttons =====
@dp.message(F.text==f"{EMOJI_TASKS} Задачи")
async def btn_tasks_with_emoji(m:Message):
    ensure_admin(m.from_user.id)
    await render_tasks_list(m.chat.id)

@dp.message(F.text=="Задачи")
async def btn_tasks_plain(m:Message):
    ensure_admin(m.from_user.id)
    await render_tasks_list(m.chat.id)

@dp.message(F.text==f"{EMOJI_LIST} Заявки")
async def btn_supplies_button(m:Message):
    await cmd_supplies(m)

@dp.message(Command("cancel"))
@dp.message(F.text=="❌ Отмена")
async def cmd_cancel(m:Message,state:FSMContext):
    ensure_admin(m.from_user.id)
    await state.clear()
    await m.answer("Завершено.", reply_markup=main_menu_kb())

# Кнопочные алиасы
BUTTON_ALIASES={
    "анализ":"cmd_analyze","отчёт сейчас":"cmd_analyze","товары":"cmd_stock","склады":"cmd_warehouses",
    "кластеры":"cmd_clusters","заявки":"cmd_supplies","задачи":"tasks","режим отображения":"cmd_view_mode",
    "диагностика":"cmd_diag","сброс кэша":"cmd_refresh"
}

@dp.message(StateFilter(None), F.text.regexp(r'^(?!\/).+'))
async def text_buttons(m:Message,state:FSMContext):
    raw=(m.text or "").lower()
    for em in ["🔧","🔍","📣","📦","🏬","🗺","⚙","🧪","🔄","🤖","❌","📄","📋"]:
        raw=raw.replace(em.lower(),"")
    raw=raw.strip()
    if "автобронир" in raw:
        ensure_admin(m.from_user.id)
        if AUTOBOOK_ENABLED:
            kb=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Начать",callback_data="menu_autobook")]])
            await m.answer("🧩 Внешний мастер автобронирования.", reply_markup=kb)
        else:
            await m.answer("Fallback автобронирование отключено в этой сборке.")
        return
    if raw in BUTTON_ALIASES:
        ensure_admin(m.from_user.id); key=BUTTON_ALIASES[raw]
        if key=="cmd_analyze": await cmd_analyze(m)
        elif key=="cmd_stock": await cmd_stock(m)
        elif key=="cmd_warehouses": await cmd_warehouses(m)
        elif key=="cmd_clusters": await cmd_clusters(m)
        elif key=="cmd_supplies": await cmd_supplies(m)
        elif key=="tasks": await render_tasks_list(m.chat.id)
        elif key=="cmd_view_mode": await cmd_view_mode(m)
        elif key=="cmd_diag": await cmd_diag(m)
        elif key=="cmd_refresh": await cmd_refresh(m)

# ===== Task callbacks =====
@dp.callback_query(F.data=="tasks:refresh")
async def cb_tasks_refresh(c:CallbackQuery):
    ensure_admin(c.from_user.id)
    await render_tasks_list(c.message.chat.id, edit_message=c.message)
    await c.answer("Обновлено")

@dp.callback_query(F.data=="tasks:close")
async def cb_tasks_close(c:CallbackQuery):
    ensure_admin(c.from_user.id)
    try:
        await c.message.edit_reply_markup(reply_markup=None)
        await c.message.edit_text(build_html(["Список задач закрыт."]))
    except Exception:
        pass
    await c.answer()

@dp.callback_query(F.data=="tasks:purge_all")
async def cb_tasks_purge_all(c:CallbackQuery):
    ensure_admin(c.from_user.id)
    chat_id=c.message.chat.id; done=False; msg="Удалено."
    try:
        if purge_all_tasks:
            cnt=purge_all_tasks(); done=True; msg=f"Удалено задач: {cnt}"
    except Exception as e: log.warning("purge_all_tasks error: %s", e)
    if not done and sw and hasattr(sw,"purge_all_tasks"):
        try:
            cnt=sw.purge_all_tasks(); done=True; msg=f"Удалено задач: {cnt}"
        except Exception as e: log.warning("sw purge error: %s", e)
    LAST_PURGE_TS[chat_id]=time.time()
    TASKS_CACHE[chat_id]=[]
    SUPPLY_EVENTS[str(chat_id)]=[]
    try: _atomic_write(SUPPLY_EVENTS_FILE, json.dumps(SUPPLY_EVENTS, ensure_ascii=False, indent=2))
    except Exception: pass
    if not done: msg="Удаление недоступно."
    await render_tasks_list(chat_id, edit_message=c.message)
    await c.answer(msg, show_alert=not done)

@dp.callback_query(F.data.startswith("tasks:detail:"))
async def cb_tasks_detail(c:CallbackQuery):
    ensure_admin(c.from_user.id)
    try: idx=int(c.data.rsplit(":",1)[1])-1
    except Exception: await c.answer(); return
    tasks=TASKS_CACHE.get(c.message.chat.id) or []
    if idx<0 or idx>=len(tasks):
        await c.answer("Нет задачи."); await render_tasks_list(c.message.chat.id, edit_message=c.message); return
    t=tasks[idx]; text=build_task_detail_text(t); kb=task_detail_kb()
    try:
        await c.message.edit_text(text,parse_mode="HTML",reply_markup=kb,disable_web_page_preview=True)
    except Exception:
        await send_safe_message(c.message.chat.id,text,parse_mode="HTML",reply_markup=kb,disable_web_page_preview=True)
    await c.answer()

# ===== Supply notifications hooks =====
async def supply_notify_text(chat_id:int,text:str):
    _supply_log_append(chat_id,{"ts":int(time.time()),"type":"text","text":text})
    await send_safe_message(chat_id,text,parse_mode="HTML",disable_web_page_preview=True)

async def supply_notify_file(chat_id:int,file_path:str,caption:str=""):
    _supply_log_append(chat_id,{"ts":int(time.time()),"type":"file","file":file_path,"caption":caption})
    try:
        await bot.send_document(chat_id, FSInputFile(file_path), caption=caption)
    except Exception as e:
        await send_safe_message(chat_id,f"Файл не отправлен: {html.escape(str(e))}\n{caption}")

def _try_register_supply_scheduler(scheduler:AsyncIOScheduler):
    try:
        register_supply_scheduler(scheduler,notify_text=supply_notify_text,notify_file=supply_notify_file,interval=SUPPLY_JOB_INTERVAL)
        log.info("Supply scheduler registered (interval).")
    except TypeError:
        try:
            register_supply_scheduler(scheduler,notify_text=supply_notify_text,notify_file=supply_notify_file)
            log.info("Supply scheduler registered (no interval).")
        except Exception as e:
            log.warning("register_supply_scheduler failed: %s", e)

# ===== Scheduler & main =====
def setup_scheduler()->AsyncIOScheduler:
    tz=ZoneInfo(TZ_NAME)
    scheduler=AsyncIOScheduler(timezone=tz)
    scheduler.add_job(snapshot_job,"interval",minutes=max(1,SNAPSHOT_INTERVAL_MINUTES),
                      id="snapshot_job",max_instances=1,coalesce=True,misfire_grace_time=60)
    scheduler.add_job(maintenance_job,"interval",minutes=max(5,HISTORY_PRUNE_EVERY_MINUTES),
                      id="maintenance_job",max_instances=1,coalesce=True,misfire_grace_time=60)
    scheduler.add_job(daily_notify_job,"cron",hour=DAILY_NOTIFY_HOUR,minute=DAILY_NOTIFY_MINUTE,
                      id="daily_notify_job",max_instances=1,coalesce=True,misfire_grace_time=600)
    return scheduler

def _register_signal_handlers(loop:asyncio.AbstractEventLoop,scheduler:AsyncIOScheduler):
    def _grace():
        try: scheduler.shutdown(wait=False)
        except Exception: pass
        for t in asyncio.all_tasks(loop):
            if t is not asyncio.current_task(loop): t.cancel()
        try: loop.stop()
        except Exception: pass
    for sig in (signal.SIGINT,signal.SIGTERM):
        try: loop.add_signal_handler(sig,_grace)
        except NotImplementedError: pass

async def main():
    load_state(); load_cache(); load_history()
    if not HISTORY_CACHE: await init_snapshot()
    else:
        # тихо построим индекс из последнего снапшота если пуст
        if not FACT_INDEX:
            ccache=build_consumption_cache()
            try:
                rows=[]
                if HISTORY_CACHE:
                    last=HISTORY_CACHE[-1]
                    for r in last.get("rows",[]):
                        rows.append({"sku":r["sku"],"warehouse_id":r.get("warehouse_key"),"warehouse_name":r.get("warehouse_name"),"free_to_sell_amount":r.get("qty")})
                build_fact_index(rows, [], ccache)
            except Exception:
                pass
    if AUTOBOOK_ENABLED and autobook_router is not None:
        try: dp.include_router(autobook_router); log.info("Autobook router включён.")
        except Exception as e: log.warning("include_router fail: %s", e)
    scheduler=setup_scheduler(); _try_register_supply_scheduler(scheduler); scheduler.start()
    _register_signal_handlers(asyncio.get_running_loop(),scheduler)
    log.info("Starting polling... Version=%s MOCK_MODE=%s ALLOWED_IDS=%s ALLOWED_USERS=%s",
             VERSION, MOCK_MODE,
             ",".join(str(x) for x in sorted(ALLOWED_USER_IDS)) or "-",
             ",".join(sorted(ALLOWED_USERNAMES)) or "-")
    try:
        await dp.start_polling(bot, allowed_updates=None)
    finally:
        try: scheduler.shutdown(wait=False)
        except Exception: pass

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass