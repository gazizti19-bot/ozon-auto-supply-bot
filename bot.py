#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SellMate | Escanor — Ozon FBO Telegram Bot
Version stable-grounded-1.7.7

Изменения в этой версии:
- Кнопки фильтров "Критично" и "50–80%" теперь корректно работают в ежедневной рассылке:
  • В daily_notify_job кэш LAST_DEFICIT_CACHE заполняется для каждого получателя.
  • В обработчике cb_filter добавлен фолбэк: если нет данных — выполняется быстрый пересчёт.
- В списке “📋 Задачи” и в списке “📄 Заявки” явно показываются обе стороны: «Склад поставки» и «Кроссдок».
- В “📄 Заявки” добавлен явный статус: «Статус: ✅ Создано» для ORDER_DATA_FILLING/CREATED и «Статус: ✅ Готово» для DONE.
- build_supplies_last_created добавляет строку «Статус: ✅ Создано» и отображает обе стороны.
- Уведомление о создании заявки: при сканировании задач/заявок определяем «созданные» (включая ORDER_DATA_FILLING),
  отправляем красиво оформленное уведомление один раз на заявку (c защитой от дублей).
- ORDER_DATA_FILLING исключён из «📋 Задачи» (перенесён в «📄 Заявки»), плюс немедленное уведомление.
- Исправлен приоритет классификации стадий: финальные/созданные стадии обрабатываются раньше «Ошибка»,
  чтобы готовые заявки не отображались как «Ошибка».
"""

import os
os.environ["AUTO_BOOK"] = os.getenv("AUTO_BOOK", "0")

from typing import Callable, Dict, Any, Awaitable, Set, Optional, List, Tuple
from aiogram import BaseMiddleware
from aiogram.types import TelegramObject, Message, CallbackQuery

# ===================== ACL MIDDLEWARE =====================
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
            # Если списки пустые — пускаем всех
            if (not self.allowed_ids) and (not self.allowed_usernames):
                return True
        except Exception:
            pass
        try:
            # Если пользователь уже запускал /start — он в KNOWN_USERS
            if 'KNOWN_USERS' in globals() and hasattr(user, 'id'):
                if int(user.id) in globals()['KNOWN_USERS']:
                    return True
        except Exception:
            pass
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
                    if isinstance(event, Message):
                        await bot.send_message(chat_id=user.id, text=self.deny_message)
                    elif isinstance(event, CallbackQuery):
                        await bot.send_message(chat_id=user.id, text=self.deny_message)
                except Exception:
                    pass
        return
# =================== END ACL MIDDLEWARE ===================

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
from pathlib import Path
from zoneinfo import ZoneInfo
import html
import sys as _sys, os as _os
import datetime  # важно для _human_window

_ROOT_DIR = _os.path.dirname(_os.path.abspath(__file__))
if _ROOT_DIR not in _sys.path:
    _sys.path.insert(0, _ROOT_DIR)

from dotenv import load_dotenv
load_dotenv()

# ===== External modules (supply) =====
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
        logging.getLogger("ozon-bot").warning("register_supply_scheduler not available.")
        return None

try:
    from supply_watch import purge_tasks, purge_all_tasks
except Exception:
    purge_tasks = None
    purge_all_tasks = None

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

VERSION = "stable-grounded-1.7.7"

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile
)
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.exceptions import TelegramRetryAfter

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
OZON_CLIENT_ID = os.getenv("OZON_CLIENT_ID", "").strip()
OZON_API_KEY = os.getenv("OZON_API_KEY", "").strip()

DEFAULT_DROPOFF_ID = (os.getenv("OZON_DROP_OFF_ID") or os.getenv("DEFAULT_DROPOFF_ID") or os.getenv("DROP_ID") or "").strip()
DEFAULT_DROPOFF_NAME = (
    os.getenv("OZON_DROP_OFF_NAME")
    or os.getenv("DEFAULT_DРОПОFF_NAME")  # поддержка исторической опечатки ключа в окружении (возможны кирилл. буквы)
    or os.getenv("DEFAULT_DROPOFF_NAME")
    or os.getenv("DROP_NAME")
    or os.getenv("DEFAULT_DROP_OFF_NAME")
    or ""
).strip()

TIMEWINDOWS_RAW = os.getenv("TIMEWINDOWS", "09:00-12:00;12:00-15:00;15:00-18:00")
DAYS_ENV = int(os.getenv("DAYS", "3"));  DAYS_ENV = 3 if DAYS_ENV <= 0 else DAYS_ENV
DISABLE_TS_FALLBACK = (os.getenv("DISABLE_TS_FALLBACK") or os.getenv("OZON_TIMESLOT_DISABLE_FALLBACK") or "0") in ("1","true","True","TRUE")

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

# ==== GigaChat config ====
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
SUPPLY_JOB_INTERVAL = int(os.getenv("SUPPLY_JOB_INTERVAL", os.getenv("SUPPLY_JOB_INTERVAL_MINUTES", "45")))

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
CACHE_FILE = Path(os.getenv("SKU_CACHE_FILE", DATA_DIR / "sku_cache.json"))
HISTORY_FILE = DATA_DIR / "stock_history.json"
KEYS_DIR = DATA_DIR / "keys"; KEYS_DIR.mkdir(exist_ok=True)
GIGACHAT_TOKEN_CACHE_FILE = (DATA_DIR / GIGACHAT_TOKEN_CACHE_ENV).resolve()
SUPPLY_EVENTS_FILE = DATA_DIR / "supply_events.json"
KNOWN_USERS_FILE = DATA_DIR / "known_users.json"

if not TELEGRAM_BOT_TOKEN:
    raise SystemExit("Missing TELEGRAM_BOT_TOKEN")

MOCK_MODE = not (OZON_CLIENT_ID and OZON_API_KEY)
GIGACHAT_ENABLED = (LLM_PROVIDER == "gigachat")

bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

ALLOWED_USER_IDS = _parse_ids_env("ALLOWED_USER_IDS")
ALLOWED_USERNAMES = _parse_usernames_env("ALLOWED_USERNAMES")
ACL_DENY_MESSAGE = os.getenv("ACL_DENY_MESSAGE", "").strip()
dp.update.middleware(ACLMiddleware(ALLOWED_USER_IDS, ALLOWED_USERNAMES, ACL_DENY_MESSAGE))

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
APPS_CACHE: Dict[int, List[Dict[str, Any]]] = {}
WAREHOUSE_CB_MAP: Dict[str, Tuple[str,str]] = {}
LAST_PURGE_TS: Dict[int, float] = {}
KNOWN_USERS: Set[int] = set()
CROSSDOCK_SELECTED: Dict[int, Dict[str, str]] = {}  # chat_id -> {id,name}
NOTIFIED_CREATED: Set[str] = set()  # task_id, чтобы не слать уведомления повторно

ENV_SKU = os.getenv("SKU_LIST", "")
if ENV_SKU:
    try:
        SKU_LIST = [int(s.strip()) for s in ENV_SKU.replace(";", ",").split(",") if s.strip()]
    except Exception:
        SKU_LIST = []
else:
    SKU_LIST = []

# ==== Cluster patterns ====
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
    raw = WAREHOUSE_CLUSTERS_ENV
    global CLUSTER_MAP
    if not raw:
        CLUSTER_MAP = {}
        return
    try:
        obj = json.loads(raw)
        if isinstance(obj, dict):
            CLUSTER_MAP = {str(k): str(v) for k, v in obj.items()}
            return
    except Exception:
        pass
    mapping = {}
    for part in re.split(r"[;\n]+", raw):
        part = part.strip()
        if not part:
            continue
        if "=" in part or ":" in part:
            k, v = (part.split(":", 1) if ":" in part else part.split("=", 1))
            mapping[str(k).strip()] = str(v).strip().strip('"').strip("'")
    CLUSTER_MAP = mapping

parse_cluster_env()

# ==== UI constants ====
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

# ==== FSM ====
class AIChatState(StatesGroup):
    waiting=State()

class AutobookStates(StatesGroup):
    choose_crossdock=State()
    after_crossdock=State()
    # далее шаги внешнего мастера

# ==== Formatting helpers ====
def bold(txt:str)->str:
    return f"§§B§§{txt}§§EB§§"

def build_html(lines:List[str])->str:
    text="\n".join(lines)
    text=html.escape(text)
    return (text.replace("§§B§§","<b>").replace("§§EB§§","</b>")
                .replace("§§I§§","<i>").replace("§§EI§§","</i>")
                .replace("§§U§§","<u>").replace("§§EU§§","</u>"))

def _atomic_write(path:Path, text:str):
    fd,tmp=tempfile.mkstemp(dir=str(path.parent), prefix=path.name, suffix=".tmp")
    try:
        with os.fdopen(fd,"w",encoding="utf-8") as f:
            f.write(text); f.flush(); os.fsync(f.fileno())
        os.replace(tmp,path)
    except Exception:
        try: os.unlink(tmp)
        except Exception: pass

# ==== Known users ====
def load_known_users():
    if KNOWN_USERS_FILE.exists():
        try:
            arr=json.loads(KNOWN_USERS_FILE.read_text("utf-8"))
            if isinstance(arr,list):
                for v in arr:
                    try: KNOWN_USERS.add(int(v))
                    except Exception: pass
        except Exception: pass

def save_known_users():
    try:
        tmp=str(KNOWN_USERS_FILE)+".tmp"
        with open(tmp,"w",encoding="utf-8") as f:
            json.dump(sorted(KNOWN_USERS),f,ensure_ascii=False,indent=2)
        os.replace(tmp, KNOWN_USERS_FILE)
    except Exception: pass

# ==== State persistence ====
def load_state():
    global BOT_STATE, SUPPLY_EVENTS, NOTIFIED_CREATED
    if STATE_FILE.exists():
        try:
            BOT_STATE=json.loads(STATE_FILE.read_text("utf-8"))
        except Exception:
            BOT_STATE={}
    BOT_STATE.setdefault("view_mode", DEFAULT_VIEW_MODE)
    BOT_STATE.setdefault("style_enabled", LLM_STYLE_ENABLED)
    BOT_STATE.setdefault("chat_mode", DEFAULT_CHAT_MODE if DEFAULT_CHAT_MODE in ("fact","general") else "fact")
    BOT_STATE.setdefault("cluster_view_mode", "full")
    BOT_STATE.setdefault("notified_created_ids", [])
    # загружаем уже уведомлённые заявки
    try:
        ids = BOT_STATE.get("notified_created_ids") or []
        for tid in ids:
            if tid:
                NOTIFIED_CREATED.add(str(tid))
    except Exception:
        pass
    if SUPPLY_EVENTS_FILE.exists():
        try:
            SUPPLY_EVENTS.update(json.loads(SUPPLY_EVENTS_FILE.read_text("utf-8")))
        except Exception:
            pass
    SUPPLY_EVENTS.setdefault("*", [])

def save_state():
    try:
        BOT_STATE["notified_created_ids"] = sorted(list(NOTIFIED_CREATED))
        _atomic_write(STATE_FILE, json.dumps(BOT_STATE, ensure_ascii=False, indent=2))
    except Exception as e: log.warning("save_state error: %s", e)

def load_cache():
    global SKU_NAME_CACHE
    if Path(CACHE_FILE).exists():
        try:
            data=json.loads(Path(CACHE_FILE).read_text("utf-8"))
            SKU_NAME_CACHE={int(k):v for k,v in data.items()}
        except Exception:
            SKU_NAME_CACHE={}

def save_cache_if_needed(prev:int):
    if len(SKU_NAME_CACHE)>prev:
        try: _atomic_write(Path(CACHE_FILE), json.dumps(SKU_NAME_CACHE, ensure_ascii=False, indent=2))
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
    if not _HISTORY_DIRTY and not force:
        return
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
        HISTORY_CACHE[:]=pruned
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

def _persist_supply_events():
    try:
        _atomic_write(SUPPLY_EVENTS_FILE, json.dumps(SUPPLY_EVENTS, ensure_ascii=False, indent=2))
    except Exception as e:
        log.warning("supply events save error: %s", e)

def _supply_log_append(chat_id:int, entry:Dict[str,Any]):
    arr=SUPPLY_EVENTS.setdefault(str(chat_id), [])
    arr.append(entry)
    SUPPLY_EVENTS.setdefault("*", []).append(entry)
    for k in (str(chat_id),"*"):
        a=SUPPLY_EVENTS.get(k,[])
        if len(a)>1000:
            del a[0:len(a)-1000]
    # Авто-слот: если задача на WAIT_WINDOW/CREATING — включаем auto_watch и ставим в очередь
    try:
        payload=entry.get("payload") or {}
        status=(entry.get("status") or "DRAFT").upper()
        tid=payload.get("id") or payload.get("task_id")
        if tid and status in ("WAIT_WINDOW","CREATING","CREATED","NEW","INITIAL"):
            try:
                import supply_watch as swm
                if hasattr(swm, "set_auto_watch_for_task"):
                    swm.set_auto_watch_for_task(tid, True)
                if hasattr(swm, "enqueue_task_watch"):
                    swm.enqueue_task_watch(tid)
            except Exception:
                pass
    except Exception:
        pass
    _persist_supply_events()

# ==== Name fetch / API ====
async def ozon_stock_fbo(skus:List[int])->Tuple[List[Dict],Optional[str]]:
    if not skus: return [], "SKU_LIST пуст"
    if MOCK_MODE:
        demo_wh=[(1,"Санкт-Петербург ФБО"),(2,"Казань")]
        rows=[]
        for sku in skus:
            for wid,name in demo_wh:
                rows.append({"sku":sku,"warehouse_id":wid,"warehouse_name":name,"free_to_sell_amount":(sku%37)+wid*5})
        return rows,None
    url="https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses"
    payload={"sku":skus,"limit":1000,"offset":0}
    headers={"Client-Id":OZON_CLIENT_ID,"Api-Key":OZON_API_KEY,"Content-Type":"application/json"}
    start=time.time()
    try:
        async with httpx.AsyncClient(timeout=API_TIMEOUT_SECONDS, trust_env=True) as client:
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

async def _fetch_names_batch(skus:List[int])->Dict[int,str]:
    headers={"Client-Id":OZON_CLIENT_ID,"Api-Key":OZON_API_KEY,"Content-Type":"application/json"}
    out={}
    if not skus: return out
    url="https://api-seller.ozon.ru/v3/product/info/list"
    payload={"sku": skus}
    try:
        async with httpx.AsyncClient(timeout=API_TIMEOUT_SECONDS, trust_env=True) as client:
            resp=await client.post(url,json=payload,headers=headers)
        if resp.status_code!=200:
            log.warning("Name batch fetch status=%s body=%s", resp.status_code, resp.text[:300])
            return out
        js=resp.json()
        items=(js.get("result") or {}).get("items") or js.get("items") or (js.get("result") if isinstance(js.get("result"),list) else [])
        if not isinstance(items,list):
            items=[]
        for it in items:
            try:
                sku=int(it.get("sku") or it.get("offer_id") or 0)
            except Exception:
                continue
            nm=it.get("name") or it.get("title") or it.get("display_name") or it.get("product_name") or f"SKU {sku}"
            out[sku]=nm
    except Exception as e:
        log.warning("Batch name error: %s", e)
    return out

async def _fetch_name_single(sku:int)->str:
    headers={"Client-Id":OZON_CLIENT_ID,"Api-Key":OZON_API_KEY,"Content-Type":"application/json"}
    # Try v2 by offer_id
    url="https://api-seller.ozon.ru/v2/product/info"
    payload={"offer_id": str(sku)}
    try:
        async with httpx.AsyncClient(timeout=API_TIMEOUT_SECONDS, trust_env=True) as client:
            r=await client.post(url,json=payload,headers=headers)
        if r.status_code==200:
            js=r.json()
            nm=js.get("result",{}).get("name") or js.get("name")
            if nm: return nm
    except Exception:
        pass
    # Try v3 with single sku list
    payload={"sku": [sku]}
    try:
        async with httpx.AsyncClient(timeout=API_TIMEOUT_SECONDS, trust_env=True) as client:
            r=await client.post("https://api-seller.ozon.ru/v3/product/info/list",json=payload,headers=headers)
        if r.status_code==200:
            js=r.json()
            items=(js.get("result") or {}).get("items") or []
            for it in items:
                if int(it.get("sku") or 0)==sku:
                    nm=it.get("name") or it.get("title")
                    if nm: return nm
    except Exception:
        pass
    return f"SKU {sku}"

async def ozon_product_names_by_sku(skus:List[int])->Tuple[Dict[int,str],Optional[str]]:
    if not skus: return {}, None
    if MOCK_MODE:
        return {s:f"Demo SKU {s}" for s in skus}, None
    unique=[s for s in skus if s>0]
    mapping={}
    CHUNK=100
    for i in range(0,len(unique),CHUNK):
        chunk=unique[i:i+CHUNK]
        batch=await _fetch_names_batch(chunk)
        mapping.update(batch)
    missing=[s for s in unique if s not in mapping]
    for sku in missing[:80]:
        mapping[sku]=await _fetch_name_single(sku)
    for s in unique:
        mapping.setdefault(s,f"SKU {s}")
    return mapping, None

def skus_needing_names()->List[int]:
    return [s for s in SKU_LIST if (s not in SKU_NAME_CACHE) or SKU_NAME_CACHE[s].startswith("SKU ") or SKU_NAME_CACHE[s].lower().startswith("demo sku")]

async def ensure_sku_names(force:bool=False):
    to_fetch = SKU_LIST if force else skus_needing_names()
    if to_fetch:
        prev=len(SKU_NAME_CACHE)
        mp,_=await ozon_product_names_by_sku(to_fetch)
        SKU_NAME_CACHE.update(mp)
        save_cache_if_needed(prev)

def get_sku_name_local(sku:int)->str:
    return SKU_NAME_CACHE.get(sku, f"SKU {sku}")

# Ленивый резолвер имён для внешнего мастера: если имени нет — подкачиваем в фоне
def get_or_fetch_sku_name_lazy(sku: int) -> str:
    name = SKU_NAME_CACHE.get(sku)
    if name and not name.lower().startswith("sku "):
        return name

    async def _fetch_one(_sku: int):
        try:
            mp, _ = await ozon_product_names_by_sku([_sku])
            if mp and _sku in mp:
                prev = len(SKU_NAME_CACHE)
                SKU_NAME_CACHE[_sku] = mp[_sku]
                save_cache_if_needed(prev)
        except Exception as e:
            log.warning("lazy name fetch failed for %s: %s", _sku, e)

    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_fetch_one(int(sku)))
    except Exception:
        pass
    return f"SKU {sku}"

def try_mount_external_name_resolver():
    # Подключаем во внешний модуль ленивый резолвер имён
    if not AUTOBOOK_ENABLED:
        return

    def resolver(s):
        try:
            sk = int(s)
        except Exception:
            return ""
        return get_or_fetch_sku_name_lazy(sk)

    candidates = [
        "set_name_resolver", "set_sku_name_resolver", "set_sku_title_provider",
        "set_title_resolver", "register_name_resolver"
    ]
    mounted=False
    for fn in candidates:
        if hasattr(abf, fn):
            try:
                getattr(abf, fn)(resolver)
                log.info("Autobook: name resolver mounted via %s", fn)
                mounted=True
                break
            except Exception as e:
                log.warning("Autobook: mount resolver failed (%s): %s", fn, e)
    if not mounted:
        # Попробуем как атрибут
        for attr in ("SKU_TITLE_PROVIDER","NAME_RESOLVER","TITLE_RESOLVER"):
            try:
                setattr(abf, attr, resolver)
                log.info("Autobook: resolver set attr %s", attr)
                mounted=True
                break
            except Exception:
                pass
    if not mounted:
        log.info("Autobook: no compatible name resolver interface exposed; fallback to SKU will remain in external UI.")

# ==== Consumption / Index helpers ====
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

# ==== Cluster functions ====
def resolve_cluster_for_warehouse(wkey:str,wname:str)->str:
    if CLUSTER_MAP:
        raw_id=None if wkey.startswith("name:") else wkey
        if raw_id and raw_id in CLUSTER_MAP: return CLUSTER_MAP[raw_id]
        if wname in CLUSTER_MAP: return CLUSTER_MAP[wname]
        return "Прочие"
    lname=(wname or "").lower()
    for cname,pats in CLUSTER_PATTERN_MAP.items():
        for p in pats:
            if p.search(lname): return cname
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
            qty=w["qty"]; gap_target=max(0,w["target"]-w["qty"])
            c["total_qty"]+=qty; c["total_need_target"]+=gap_target; c["deficit_need"]+=w["need"]
            c["warehouses"].add(w["name"]); c["sku_set"].add(sku)
            cov=w["coverage"]
            if cov<0.5: c["critical_sku"]+=1
            elif cov<0.8: c["mid_sku"]+=1
            else: c["ok_sku"]+=1
    out={}
    for cname,meta in clusters.items():
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

def small_cov_bar(cov:float,length:int=12)->str:
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
    wh_stats={}
    for sku, skud in sku_section.items():
        for w in skud.get("warehouses", []):
            if resolve_cluster_for_warehouse(w["wkey"], w["name"]) != name:
                continue
            ws=wh_stats.setdefault(w["wkey"], {
                "name": w["name"],"total_qty":0,"need_norm":0,"need_target":0,
                "critical_sku":0,"mid_sku":0,"ok_sku":0,"sku_set":set()
            })
            ws["total_qty"]+=w["qty"]; ws["need_norm"]+=w["need"]; ws["need_target"]+=max(0,w["target"]-w["qty"])
            ws["sku_set"].add(sku)
            cov=w["coverage"]
            if cov<0.5: ws["critical_sku"]+=1
            elif cov<0.8: ws["mid_sku"]+=1
            else: ws["ok_sku"]+=1

    wh_items={}
    for sku, skud in sku_section.items():
        for w in skud.get("warehouses", []):
            if resolve_cluster_for_warehouse(w["wkey"], w["name"]) != name:
                continue
            if w["need"]<=0: continue
            arr=wh_items.setdefault(w["wkey"], [])
            arr.append({
                "sku":sku,"name":skud["name"],"qty":w["qty"],
                "norm":w["norm"],"target":w["target"],"need":w["need"],"coverage":w["coverage"]
            })
    for wk in wh_items:
        wh_items[wk].sort(key=lambda x:(x["coverage"], -x["need"]))

    cov_worst=[]
    for sku, skud in sku_section.items():
        worst=1.0; inside=False
        for w in skud.get("warehouses", []):
            if resolve_cluster_for_warehouse(w["wkey"], w["name"])==name:
                worst=min(worst,w["coverage"]); inside=True
        if inside: cov_worst.append(worst)
    cluster_worst=min(cov_worst) if cov_worst else 0.0
    cluster_avg=(sum(cov_worst)/len(cov_worst)) if cov_worst else 0.0

    lines=[f"🗺 §§B§§Кластер: {name}§§EB§§", SEP_THIN,
           f"SKU всего: {cl['total_sku']}",
           f"Суммарный остаток: {cl['total_qty']}",
           f"Потребность до цели: {cl['total_need_target']}",
           f"Дефицит (ниже нормы): {cl['deficit_need']}",
           "", "Покрытие:",
           f"  Худшее: {small_cov_bar(cluster_worst,20)} {int(cluster_worst*100):02d}%",
           f"  Среднее: {small_cov_bar(cluster_avg,20)} {int(cluster_avg*100):02d}%",
           SEP_THIN]

    wh_sorted=sorted(wh_stats.values(), key=lambda x:x["need_target"], reverse=True)
    if not short:
        lines+=["§§B§§Сводка по складам§§EB§§"]
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
                lines.append(f"• {ws['name']}: дефицит {ws['need_norm']}, до цели {ws['need_target']}, критичных {ws['critical_sku']}")
        else: lines.append("Нет данных.")
        lines.append(SEP_THIN)

    lines.append("§§B§§Товары по складам (дефицит)§§EB§§"+(" (коротко)" if short else ""))
    if not wh_sorted:
        lines.append("Нет складов в кластере.")
    else:
        per_wh_limit=12 if not short else 6
        for ws in wh_sorted:
            wkey=None
            for k,meta in wh_stats.items():
                if meta is ws:
                    wkey=k; break
            if wkey is None:
                for k,meta in wh_stats.items():
                    if meta["name"]==ws["name"]:
                        wkey=k; break
            lines.append(f"{EMOJI_WH} {bold(ws['name'])} — Остаток {ws['total_qty']} | Дефицит {ws['need_norm']} | До цели {ws['need_target']}")
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

# ==== FACT INDEX ====
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

async def ensure_fact_index(force:bool=False, silent:bool=True):
    async with FACT_BUILD_LOCK:
        if not force and FACT_INDEX:
            return
        if not SKU_LIST:
            return
        rows, err = await ozon_stock_fbo(SKU_LIST)
        if err:
            log.warning("ensure_fact_index: Ozon error: %s", err)
            return
        if time.time()-LAST_SNAPSHOT_TS>SNAPSHOT_MIN_REUSE_SECONDS:
            append_snapshot(rows)
        await ensure_sku_names(force=True)  # имена перед индексом
        ccache=build_consumption_cache()
        try:
            build_fact_index(rows, [], ccache)
        except Exception as e:
            log.exception("ensure_fact_index build error: %s", e)
        await flush_history_if_needed(force=True)
        if not silent and ADMIN_ID:
            await send_safe_message(ADMIN_ID, "Индекс обновлён.", disable_web_page_preview=True)

# ==== Deficit report ====
def generate_deficit_report(rows:List[Dict], name_map:Dict[int,str], ccache:Dict[Tuple[int,str],Dict[str,Any]])->Tuple[str,List[dict]]:
    agg=aggregate_rows(rows)
    deficits={}; flat=[]
    for sku,wmap in agg.items():
        for wkey,info in wmap.items():
            qty=info["qty"]; st=evaluate_position_cached(sku,wkey,qty,ccache)
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
            wh_b=bold(i['warehouse_name'])
            if full:
                lines.append(f"• {wh_b}: Остаток {i['qty']} / Норма {i['norm']} / Цель {i['target']} → +{i['need']}\n  {bar} {sev} {hist} · {badge}")
            else:
                lines.append(f"• {wh_b}: Остаток {i['qty']} → +{i['need']}  {bar} · {badge}")
        lines.append(f"  Σ Остаток={total_qty}, Потребность (до нормы)={total_need}")
        lines.append(SEP_THIN)
    lines.append(f"{EMOJI_TARGET} Итоги: товаров={len(deficits)}, строк={len(flat)} | <50%={crit} | 50–80%={mid} | ≥80% но ниже нормы={hi} | режим={view_mode}")
    return build_html(lines), flat

# ==== AI highlight & answer ====
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
    out=[]; total=0; limit=LLM_FACT_SOFT_LIMIT_CHARS-300
    for ln in text.splitlines():
        if total+len(ln)+1>limit:
            out.append("...(усечено)"); break
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
    for td in FACT_INDEX.get("top_deficits",[])[:LLM_TOP_DEFICITS]:
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
    if not GIGACHAT_ENABLED: return "LLM отключён.","off"
    q=question.strip()
    if not q: return "Пустой запрос.","empty"
    global _LAST_AI_CALL
    now=time.time()
    if now-_LAST_AI_CALL<AI_MIN_INTERVAL_SECONDS:
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
        return f"Не удалось получить токен: {e}","auth"
    payload={"model":GIGACHAT_MODEL,"messages":messages,"temperature":min(0.2,GIGACHAT_TEMPERATURE),"max_tokens":GIGACHAT_MAX_TOKENS}
    try:
        async with httpx.AsyncClient(verify=_gigachat_verify_param(),timeout=GIGACHAT_TIMEOUT_SECONDS,trust_env=True) as client:
            r=await client.post(GIGACHAT_API_URL,json=payload,headers={"Authorization":f"Bearer {token}","Content-Type":"application/json"})
            if r.status_code==401:
                _GIGACHAT_TOKEN_MEM={}
                token=await get_gigachat_token(force=True)
                r=await client.post(GIGACHAT_API_URL,json=payload,headers={"Authorization":f"Bearer {token}","Content-Type":"application/json"})
            if r.status_code>=400:
                return f"GigaChat HTTP {r.status_code}: {r.text[:250]}","http"
            data=r.json()
    except Exception as e:
        return f"Ошибка сети: {e}","net"
    ch=data.get("choices")
    if not ch: return f"Пустой ответ: {data}","empty"
    text=(ch[0].get("message",{}).get("content") or "").strip()
    ANSWER_CACHE[key]=text
    return text, mode

async def llm_general_answer(chat_id:int, question:str)->Tuple[str,str]:
    if not GIGACHAT_ENABLED: return "LLM отключён.","off"
    q=question.strip()
    if not q: return "Пустой запрос.","empty"
    global _LAST_AI_CALL
    now=time.time()
    if now-_LAST_AI_CALL<AI_MIN_INTERVAL_SECONDS:
        return f"Слишком часто. Подождите {AI_MIN_INTERVAL_SECONDS-int(now-_LAST_AI_CALL)} сек.","rate"
    _LAST_AI_CALL=now
    messages=build_general_messages(chat_id,q)
    try:
        token=await get_gigachat_token()
    except Exception as e:
        return f"Не удалось получить токен: {e}","auth"
    payload={"model":GIGACHAT_MODEL,"messages":messages,"temperature":LLM_GENERAL_TEMPERATURE,"max_tokens":GIGACHAT_MAX_TOKENS}
    try:
        async with httpx.AsyncClient(verify=_gigachat_verify_param(),timeout=GIGACHAT_TIMEOUT_SECONDS,trust_env=True) as client:
            r=await client.post(GIGACHAT_API_URL,json=payload,headers={"Authorization":f"Bearer {token}","Content-Type":"application/json"})
            if r.status_code>=400:
                return f"GigaChat HTTP {r.status_code}: {r.text[:250]}","http"
            data=r.json()
    except Exception as e:
        return f"Ошибка сети: {e}","net"
    ch=data.get("choices")
    if not ch: return f"Пустой ответ: {data}","empty"
    text=(ch[0].get("message",{}).get("content") or "").strip()
    add_general_history(chat_id,"user",q); add_general_history(chat_id,"assistant",text)
    return text,"general"

# ==== Messaging helpers ====
async def send_safe_message(chat_id:int,text:str,**kwargs):
    if not text: text="\u200b"
    try: return await bot.send_message(chat_id,text,**kwargs)
    except TelegramRetryAfter as e:
        await asyncio.sleep(e.retry_after)
        return await bot.send_message(chat_id,text,**kwargs)
    except Exception as e:
        log.warning("send fail: %s", e)

async def send_long(chat_id:int,text:str,kb:Optional[InlineKeyboardMarkup]=None):
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
        await send_safe_message(chat_id,chunk.rstrip() or "\u200B",
                                parse_mode="HTML",
                                disable_web_page_preview=True,
                                reply_markup=kb if (kb and i==len(parts)-1) else None)
        await asyncio.sleep(0.02)

# ==== Tasks logic ====
# Стадии (расширено под реальные статусы supply_watch/Ozon)
DRAFT_STATUSES={"DRAFT","NEW","INITIAL","CALCULATION_STATUS_PENDING"}
WAIT_STATUSES={"WAIT","WAITING","PENDING","IN_PROGRESS","ACTIVE","QUEUED",
               "CALCULATION_STATUS_SUCCESS","SUPPLY_ORDER_FETCH","POLL_SUPPLY"}
SLOT_STATUSES={"BOOKED","RESERVED","SCHEDULED","WINDOW_SET","SLOT_SET","SLOT_BOOKED"}
CREATING_STATUSES={"CREATING","CREATING_SUPPLY","SUPPLY_CREATING","SUPPLY_CREATE","CREATING_DRAFT"}
APPLICATION_FILL_STATUSES={"ORDER_DATA_FILLING"}  # стадия заполнения заявки
DONE_STATUSES={"DONE","SUCCESS","FINISHED","COMPLETED","SUPPLY_CREATED"}
CREATED_STATUSES={"CREATED","UI_STATUS_CREATED","СОЗДАНО"}
ERROR_STATUSES={"ERROR","FAILED"}
CANCEL_STATUSES={"CANCELLED","CANCELED"}

DEFAULT_STAGE_EMOJI_RU={
    "Черновик":"📝","Ожидание":"⏳","Слот":"🕘","Заполнение заявки":"📝",
    "Создание заявки":"🛠","Готово":"✅","Ошибка":"❌","Отменено":"🚫","Ожидание supply":"🔄"
}

def resolve_abf_stage_emoji_map()->Dict[str,str]:
    try:
        if AUTOBOOK_ENABLED and 'abf' in globals() and abf:
            for name in dir(abf):
                obj=getattr(abf,name)
                if isinstance(obj,dict):
                    keys=set(DEFAULT_STAGE_EMOJI_RU.keys())
                    if keys.issubset(set(obj.keys())):
                        return {str(k):str(v) for k,v in obj.items()}
    except Exception:
        pass
    return DEFAULT_STAGE_EMOJI_RU

# Основная карта эмодзи стадий
STAGE_EMOJI_RU = resolve_abf_stage_emoji_map()

def classify_task_stage(task:Dict[str,Any])->Tuple[str,str]:
    """
    ВАЖНО: финальные/созданные стадии обрабатываются раньше ошибок, чтобы не метить готовые заявки как «Ошибка».
    """
    status=(task.get("status") or task.get("state") or "").upper()
    creating=bool(task.get("creating"))
    desired_from_iso=task.get("desired_from_iso") or ""
    last_error=(task.get("last_error") or "").strip()

    # Отменено
    if status in CANCEL_STATUSES:
        return (STAGE_EMOJI_RU.get("Отменено","🚫"),"Отменено")
    # Готово
    if status in DONE_STATUSES:
        return (STAGE_EMOJI_RU.get("Готово","✅"),"Готово")
    # Создано (включая UI-метки создано)
    if status in CREATED_STATUSES:
        return (STAGE_EMOJI_RU.get("Готово","✅"),"Создано")
    # Заполнение заявки считаем «создано» с точки зрения пользователю
    if status in APPLICATION_FILL_STATUSES or status=="CREATING_DRAFT" or creating:
        # В стадии заполнения всё равно показываем как «Заполнение заявки», но не «Ошибка»
        return (STAGE_EMOJI_RU.get("Заполнение заявки","📝"),"Заполнение заявки")
    # Слот
    if status in SLOT_STATUSES:
        return (STAGE_EMOJI_RU.get("Слот","🕘"),"Слот")
    # Ожидание supply
    if "SUPPLY_ORDER_FETCH" in status or "POLL_SUPPLY" in status:
        return (STAGE_EMOJI_RU.get("Ожидание supply","🔄"),"Ожидание supply")
    # Черновики/ожидания
    if status in DRAFT_STATUSES:
        if desired_from_iso and status=="WAIT_WINDOW":
            return (STAGE_EMOJI_RU.get("Ожидание","⏳"),"Ожидание")
        return (STAGE_EMOJI_RU.get("Черновик","📝"),"Черновик")
    if status in WAIT_STATUSES or status.startswith("WAIT_"):
        return (STAGE_EMOJI_RU.get("Ожидание","⏳"),"Ожидание")

    # Ошибка — обрабатываем после финальных стадий
    if status in ERROR_STATUSES or last_error:
        return (STAGE_EMOJI_RU.get("Ошибка","❌"),"Ошибка")

    # По умолчанию — черновик
    return (STAGE_EMOJI_RU.get("Черновик","📝"),"Черновик")

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

def normalize_tasks_result(res:Any)->List[Dict[str,Any]]:
    if not res: return []
    if isinstance(res, dict):
        out=[]
        for key in ("tasks","items","result"):
            v=res.get(key)
            if isinstance(v,list) and (not v or isinstance(v[0],dict)):
                out.extend([t for t in v if isinstance(t,dict)])
        if out: return out
        for v in res.values():
            if isinstance(v,list) and v and isinstance(v[0],dict):
                out.extend(v)
        if out: return out
        if "id" in res: return [res]
        return []
    if isinstance(res,list) and res and isinstance(res[0],dict):
        return res
    return []

def fallback_tasks_from_events()->List[Dict[str,Any]]:
    arr=SUPPLY_EVENTS.get("*") or []
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
            "id":tid,"status":(e.get("status") or "DRAFT").upper(),"date":payload.get("date") or "",
            "timeslot":payload.get("timeslot") or "","creating":payload.get("creating") or False,
            "desired_from_iso":payload.get("desired_from_iso") or "","desired_to_iso":payload.get("desired_to_iso") or "",
            "last_error":payload.get("last_error") or "","sku_list":payload.get("sku_list") or [],
            "warehouse_name":payload.get("warehouse_name") or payload.get("drop_off_name") or "",
            "crossdock_id":payload.get("crossdock_id") or "",
            "crossdock_name":payload.get("crossdock_name") or "",
        }
        if not t["sku_list"]:
            qty=payload.get("qty") or 0
            sku=payload.get("sku")
            t["sku_list"]=[{"sku":sku,"total_qty":qty,"warehouse_name":t["warehouse_name"]}]
        out.append(t)
    out.reverse()
    clean=[t for t in out if not (_sum_qty(t)==0 and _task_warehouse_name(t)=="-" and not _first_sku(t))]
    return clean

async def fetch_tasks_global()->List[Dict[str,Any]]:
    tasks=[]
    try:
        import supply_watch as swm
        for name in ("list_tasks","list_all_tasks","get_tasks","dump_tasks"):
            fn=getattr(swm,name,None)
            if not fn: continue
            res=fn()
            if inspect.isawaitable(res): res=await res
            tasks=normalize_tasks_result(res)
            if tasks: break
    except Exception: pass
    if not tasks:
        try:
            import flows.autobook_flow as abfm
            for name in ("list_tasks","get_tasks"):
                fn=getattr(abfm,name,None)
                if not fn: continue
                res=fn()
                if inspect.isawaitable(res): res=await res
                tasks=normalize_tasks_result(res)
                if tasks: break
        except Exception: pass
    if not tasks:
        try:
            import supply_integration as sim
            for name in ("list_tasks","get_tasks"):
                fn=getattr(sim,name,None)
                if not fn: continue
                res=fn()
                if inspect.isawaitable(res): res=await res
                tasks=normalize_tasks_result(res)
                if tasks: break
        except Exception: pass
    if not tasks:
        tasks=fallback_tasks_from_events()
    return tasks

def _human_window(timeslot:str="", from_iso:str="", to_iso:str="")->str:
    ts_raw=(timeslot or "").strip()
    iso_pat=r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:Z|[+\-]\d{2}:\d{2})'
    iso=re.findall(iso_pat, ts_raw)
    if len(iso)>=2:
        from_iso=iso[0]; to_iso=iso[1]
    def _p(v):
        if not v: return None
        v=v.replace("Z","+00:00")
        try: return datetime.datetime.fromisoformat(v)
        except Exception: return None
    df=_p(from_iso); dt=_p(to_iso)
    if df and dt:
        mons=["янв","фев","мар","апр","май","июн","июл","авг","сен","окт","ноя","дек"]
        wds=["Пн","Вт","Ср","Чт","Пт","Сб","Вс"]
        tz=df.strftime("%z"); tz_fmt=f"(UTC{tz[:3]}:{tz[3:]})" if tz else ""
        if df.date()==dt.date():
            return f"{df.day} {mons[df.month-1]} ({wds[df.weekday()]}) {df:%H:%M}–{dt:%H:%M} {tz_fmt}"
        return f"{df.day} {mons[df.month-1]} ({wds[df.weekday()]}) {df:%H:%M} → {dt.day} {mons[dt.month-1]} ({wds[dt.weekday()]}) {dt:%H:%M} {tz_fmt}"
    # Fallback: HH:MM-HH:MM
    if ts_raw and re.match(r'^\d{2}:\d{2}-\d{2}:\d{2}$', ts_raw):
        return ts_raw.replace('-', '–')
    return ts_raw or from_iso or to_iso or "-"

# ==== Application status helpers ====
def _application_status_text(status:str)->str:
    s=status.upper().strip()
    if s in APPLICATION_FILL_STATUSES or s in CREATED_STATUSES:
        return "✅ Создано"
    if s in DONE_STATUSES:
        return "✅ Готово"
    if s in CREATING_STATUSES:
        return "🛠 Создание"
    if s in CANCEL_STATUSES:
        return "🚫 Отменено"
    if s in ERROR_STATUSES:
        return "❌ Ошибка"
    return "—"

# ==== Lists renderers (Tasks/Applications) ====
def build_tasks_list_text(tasks:List[Dict[str,Any]], chat_id:int)->str:
    """
    Build tasks list with full product names (resolved from SKU) and improved formatting.
    Shows: stage emoji + stage + date + time window; product name + SKU + qty + ID; warehouse + crossdock.
    """
    if not tasks:
        return build_html(["§§B§§📋 Активные задачи (0)§§EB§§",SEP_THIN,"Сейчас нет активных (незавершённых) задач."])
    lines=[f"§§B§§📋 Активные задачи ({len(tasks)})§§EB§§",SEP_THIN]
    for i,t in enumerate(tasks,1):
        em,stage=classify_task_stage(t)
        qty=_sum_qty(t)
        date=t.get("date") or (t.get("desired_from_iso","")[:10] if t.get("desired_from_iso") else "-")
        slot=_human_window(t.get("timeslot") or "", t.get("desired_from_iso") or "", t.get("desired_to_iso") or "")
        sku=_first_sku(t)
        tid=t.get("id") or "-"
        wh=_task_warehouse_name(t)
        cd=_resolve_crossdock_name_warehouses(t, chat_id)
        # Resolve product name
        if sku and sku > 0:
            product_name = get_sku_name_local(sku)
            # If name is just "SKU {n}", try lazy fetch
            if product_name.startswith("SKU "):
                product_name = get_or_fetch_sku_name_lazy(sku)
            sku_display = str(sku)
        else:
            product_name = "-"
            sku_display = "-"
        # Format: stage + date + time on first line; product + SKU + qty + ID on second; warehouse + crossdock on third
        lines.append(f"{i}) {em} {stage} | {date} {slot}")
        lines.append(f"   §§B§§{html.escape(product_name)}§§EB§§ (SKU {sku_display}) | {qty} шт | ID {tid}")
        lines.append(f"   Склад поставки: §§B§§{html.escape(wh)}§§EB§§ | Кроссдок: §§B§§{html.escape(cd)}§§EB§§")
    return build_html(lines)

def _last_created_tasks(limit:int=3)->List[Dict[str,Any]]:
    """
    Scan SUPPLY_EVENTS["*"] in reverse chronological order and collect unique applications.
    Treats as "created": CREATED, DONE, SUCCESS, FINISHED, COMPLETED, SUPPLY_CREATED, 
    ORDER_DATA_FILLING, and textual mentions like "создан".
    """
    arr=SUPPLY_EVENTS.get("*") or []
    created=[]
    seen=set()
    for e in reversed(arr):
        payload=e.get("payload") or {}
        tid=payload.get("id") or payload.get("task_id") or ""
        if not tid or tid in seen:
            continue
        txt=(e.get("text") or "").lower()
        st=(e.get("status") or "").lower()
        # Treat as created: explicit states + ORDER_DATA_FILLING + textual mentions
        if "создан" in txt or st in ("created","done","success","finished","completed","supply_created","supply created","order_data_filling"):
            seen.add(tid)
            created.append(payload)
            if len(created)>=limit:
                break
    return created

def build_last_created_tasks_text(limit:int=3)->str:
    tasks=_last_created_tasks(limit)
    if not tasks:
        return build_html(["<b>Последние созданные задачи (0)</b>", "Нет созданных заявок.", SEP_THIN])
    lines=[f"<b>Последние созданные задачи ({len(tasks)})</b>", SEP_THIN]
    for i,p in enumerate(tasks,1):
        tid=p.get("id") or p.get("task_id") or "-"
        sku_list=p.get("sku_list") or []
        qty=sum(int(it.get("qty") or it.get("total_qty") or 0) for it in sku_list)
        warehouse=p.get("warehouse_name") or p.get("drop_off_name") or "-"
        cd = p.get("crossdock_name") or p.get("crossdock_id") or ""
        slot=_human_window(p.get("timeslot") or "", p.get("desired_from_iso") or "", p.get("desired_to_iso") or "")
        lines.append(f"{i}. ID {tid} | {qty} шт | Слот: {slot}")
        lines.append(f"   Статус: ✅ Создано")
        lines.append(f"   Склад поставки: §§B§§{html.escape(warehouse)}§§EB§§ | Кроссдок: §§B§§{html.escape(cd)}§§EB§§")
    lines.append(SEP_THIN)
    return build_html(lines)

# ==== Crossdocks ENV parsers (для имён кроссдоков в списке задач) ====
CROSSDOCKS_MAP = {}

def _load_crossdocks_warehouses_env() -> None:
    raw = (os.environ.get("CROSSDOCK_WAREHOUSES") or os.environ.get("CROSSDOCKS_WAREHOUSES") or "").strip()
    if not raw:
        return
    # strip surrounding quotes if present
    if (raw.startswith('"') and raw.endswith('"')) or (raw.startswith("'") and raw.endswith("'")):
        raw = raw[1:-1]
    # normalize: replace commas with newlines
    text = raw.replace(",", "\n")
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    m = {}
    for item in lines:
        tokens = item.split()
        if not tokens:
            continue
        id_token = tokens[-1]
        name = " ".join(tokens[:-1]).strip()
        id_digits = "".join(ch for ch in id_token if ch.isdigit())
        if id_digits and name:
            m[id_digits] = name
    if m:
        globals()["CROSSDOCKS_MAP"] = m

def _load_crossdocks_env() -> None:
    """
    Загружает карту кроссдоков из ENV:
    - CROSSDOCKS_JSON: JSON-строка, например [{"id":"102000..","name":"ХАБАРОВСК_2_РФЦ_КРОССДОКИНГ"}, ...]
    - CROSSDOCKS: CSV-строка по строкам, формат "id;name" (одна пара на строку)
    """
    import json as _json
    raw_json = os.environ.get("CROSSDOCKS_JSON", "").strip()
    raw_csv = os.environ.get("CROSSDOCKS", "").strip()
    m = {}
    # JSON сначала
    if raw_json:
        try:
            arr = _json.loads(raw_json)
            if isinstance(arr, list):
                for it in arr:
                    cid = str((it.get("id") or it.get("code") or "")).strip()
                    name = str((it.get("name") or it.get("title") or "")).strip()
                    if cid and name:
                        m[cid] = name
        except Exception:
            pass
    # CSV "id;name"
    if raw_csv and not m:
        for line in raw_csv.splitlines():
            line=line.strip()
            if not line or line.startswith("#"): continue
            parts=line.split(";")
            if len(parts)>=2:
                cid=parts[0].strip()
                name=";".join(parts[1:]).strip()
                if cid and name:
                    m[cid] = name
    if m:
        globals()["CROSSDOCKS_MAP"] = m

def _resolve_crossdock_name_warehouses(t:dict, chat_id:int) -> str:
    """
    Имя кроссдока (предпочтительно из ENV-карт).
    Порядок:
      1) crossdock_name
      2) drop_off_name
      3) по crossdock_id/drop_off_id из CROSSDOCKS_MAP (CROSSDOCK_WAREHOUSES / CROSSDOCKS / CROSSDOCKS_JSON)
      4) '—'
    """
    name = (t.get("crossdock_name") or "").strip()
    if name:
        return name
    dname = (t.get("drop_off_name") or "").strip()
    cid = str(t.get("crossdock_id") or "").strip()
    did = str(t.get("drop_off_id") or "").strip()
    # Гарантия загрузки карты
    try:
        if not globals().get("CROSSDOCKS_MAP"):
            _load_crossdocks_warehouses_env()
            _load_crossdocks_env()
    except Exception:
        pass
    mp = globals().get("CROSSDOCKS_MAP") or {}
    def norm(v: str) -> str:
        return "".join(ch for ch in v if ch.isdigit())
    if dname:
        return dname
    cidn = norm(cid) if cid else ""
    didn = norm(did) if did else ""
    if cidn and cidn in mp:
        return mp[cidn]
    if didn and didn in mp:
        return mp[didn]
    return dname or "—"

def build_task_detail_text(t:Dict[str,Any], chat_id:int)->str:
    em,stage=classify_task_stage(t); qty=_sum_qty(t)
    date=t.get("date") or (t.get("desired_from_iso","")[:10] if t.get("desired_from_iso") else "-")
    slot=_human_window(t.get("timeslot") or "", t.get("desired_from_iso") or "", t.get("desired_to_iso") or "")
    tid=t.get("id") or "-"; wh=_task_warehouse_name(t)
    cd = _resolve_crossdock_name_warehouses(t, chat_id)
    lines=["§§B§§Детали заявки§§EB§§",
           f"ID: {tid}",
           f"Стадия: {em} {stage}",
           f"Дата: {date} | Окно: {slot}",
           f"Кроссдок: {html.escape(cd or '—')}",
           f"Склад поставки: {html.escape(wh)}",
           f"Итого: {qty} шт",
           ""]
    sl=t.get("sku_list")
    if isinstance(sl,list) and sl:
        lines.append("Позиции:")
        for i,it in enumerate(sl,1):
            sku=it.get("sku"); q=it.get("total_qty") or it.get("qty") or 0
            # Validate and convert SKU
            try:
                sku_int = int(sku) if sku else 0
            except (ValueError, TypeError):
                sku_int = 0
            
            if sku_int > 0:
                sname=get_sku_name_local(sku_int)
                # If name is just "SKU {n}", try lazy fetch
                if sname.startswith("SKU "):
                    sname = get_or_fetch_sku_name_lazy(sku_int)
                sku_display = str(sku_int)
            else:
                sname = "-"
                sku_display = "-"
            wname=it.get("warehouse_name") or "-"
            lines.append(f"{i}. §§B§§{html.escape(sname)}§§EB§§ (SKU {sku_display}) — {q} шт | {html.escape(wname)}")
        lines.append("")
    if t.get("last_error"):
        lines.append(f"Ошибка: {html.escape(t['last_error'])}")
    return build_html(lines)

# ==== Удаление задач (single/all) ====
async def _try_delete_task_in_module(mod, tid:str) -> bool:
    if not mod: return False
    fn_candidates = [
        ("cancel_task", (tid,)),
        ("cancel_supply", (tid,)),
        ("delete_task", (tid,)),
        ("remove_task", (tid,)),
        ("drop_task", (tid,)),
        ("purge_task", (tid,)),
        ("purge_tasks", ([tid],)),
    ]
    for fn_name, args in fn_candidates:
        fn = getattr(mod, fn_name, None)
        if not fn:
            continue
        try:
            res = fn(*args)
            if inspect.isawaitable(res):
                await res
            return True
        except Exception as e:
            log.debug("delete %s in %s failed: %s", tid, getattr(mod, "__name__", mod), e)
            continue
    return False

async def delete_task_by_id(tid:str) -> bool:
    # Пытаемся удалить в supply_watch, потом в внешнем мастере, потом в интеграции
    for mod in (sw, abf if AUTOBOOK_ENABLED else None, si):
        if await _try_delete_task_in_module(mod, tid):
            return True
    # Фолбэк: попытка через purge_tasks, если импортирован как глобальный
    try:
        if purge_tasks:
            res = purge_tasks([tid])
            if inspect.isawaitable(res):
                await res
            return True
    except Exception:
        pass
    return False

def _remove_task_from_caches(chat_id:int, tid:str):
    """
    Remove task from caches. Preserves application events (created/done states) 
    to keep last 3 created applications visible.
    """
    # Удаляем из TASKS_CACHE
    lst = TASKS_CACHE.get(chat_id) or []
    TASKS_CACHE[chat_id] = [t for t in lst if str(t.get("id") or "") != str(tid)]
    # Удаляем только события задач (не заявок) из SUPPLY_EVENTS
    try:
        for key in list(SUPPLY_EVENTS.keys()):
            events = SUPPLY_EVENTS.get(key) or []
            filtered = []
            for e in events:
                payload = e.get("payload") or {}
                event_tid = str(payload.get("id") or payload.get("task_id") or "")
                if event_tid != str(tid):
                    # Не этот task - сохраняем
                    filtered.append(e)
                else:
                    # Это событие для данного task - проверяем, заявка ли это
                    text = (e.get("text") or "").lower()
                    status = (e.get("status") or "").lower()
                    # Сохраняем события о созданных заявках
                    if "создан" in text or status in ("создано","created","done","success","finished","completed","supply_created","order_data_filling"):
                        filtered.append(e)
                    # Иначе удаляем (это событие обычной задачи)
            SUPPLY_EVENTS[key] = filtered
        _persist_supply_events()
    except Exception as e:
        log.warning("SUPPLY_EVENTS purge for %s failed: %s", tid, e)

async def delete_all_tasks_for_chat(chat_id:int) -> int:
    """
    Delete all tasks for a chat. Applications data (SUPPLY_EVENTS, APPS_CACHE, NOTIFIED_CREATED) 
    is preserved so the last 3 created applications remain visible.
    """
    lst = TASKS_CACHE.get(chat_id) or []
    count = 0
    # Если доступно purge_all_tasks — используем
    try:
        if purge_all_tasks:
            res = purge_all_tasks()
            if inspect.isawaitable(res):
                await res
            count = len(lst)
            TASKS_CACHE[chat_id] = []
            # НЕ чистим SUPPLY_EVENTS, APPS_CACHE, NOTIFIED_CREATED — сохраняем заявки
            return count
    except Exception as e:
        log.warning("purge_all_tasks failed: %s", e)
    # Иначе — удаляем каждую по ID
    for t in lst:
        tid = str(t.get("id") or "")
        try:
            if tid:
                ok = await delete_task_by_id(tid)
                if ok:
                    count += 1
                    _remove_task_from_caches(chat_id, tid)
        except Exception:
            pass
    return count

# ==== Фильтры задач/заявок ====
def is_active_task(t:dict)->bool:
    """
    Активная (незавершённая) задача: пред-заявочные стадии,
    исключая ошибки/отмены и исключая заполнение/создание заявок.
    """
    status=(t.get("status") or t.get("state") or "").upper().strip()
    if not status:
        return True
    if status in ERROR_STATUSES or status in CANCEL_STATUSES:
        return False
    # Исключаем стадии заявок из списка задач:
    if status in APPLICATION_FILL_STATUSES:  # ORDER_DATA_FILLING
        return False
    if status in CREATING_STATUSES or status in CREATED_STATUSES or status in DONE_STATUSES:
        return False
    # Оставляем DRAFT/WAIT/SLOT/..., включая SUPPLY_ORDER_FETCH/POLL_SUPPLY
    return True

def is_application_task(t:dict)->bool:
    """
    Заявка: начиная с заполнения (ORDER_DATA_FILLING), создание (CREATING_*),
    создано (CREATED/UI_STATUS_CREATED/СОЗДАНО) и финальные DONE/SUCCESS/COMPLETED.
    Ошибки/отмены исключены.
    """
    status=(t.get("status") or t.get("state") or "").upper().strip()
    if not status:
        return False
    if status in ERROR_STATUSES or status in CANCEL_STATUSES:
        return False
    if status in APPLICATION_FILL_STATUSES:
        return True
    if status in CREATING_STATUSES:
        return True
    if status in CREATED_STATUSES:
        return True
    if status in DONE_STATUSES:
        return True
    return False

def is_created_like(t:dict)->bool:
    """
    Что считаем событием «создания» для уведомления:
    - ORDER_DATA_FILLING (начало заполнения) — по бизнес-логике это уже «создано»;
    - CREATED/UI_STATUS_CREATED/СОЗДАНО;
    - DONE/SUCCESS/COMPLETED/SUPPLY_CREATED.
    Ошибки/отмены — нет.
    """
    s=(t.get("status") or t.get("state") or "").upper().strip()
    if not s: return False
    if s in CANCEL_STATUSES or s in ERROR_STATUSES: return False
    return (s in APPLICATION_FILL_STATUSES) or (s in CREATED_STATUSES) or (s in DONE_STATUSES)

# ==== Keyboards (задачи/заявки) ====
def build_tasks_kb(n:int)->InlineKeyboardMarkup:
    rows=[]; buf=[]
    # Кнопки выбора по номеру
    for i in range(1,n+1):
        buf.append(InlineKeyboardButton(text=str(i),callback_data=f"tasks:detail:{i}"))
        if len(buf)==5: rows.append(buf); buf=[]
    if buf: rows.append(buf)
    # Управляющие кнопки
    ctrl_row = [
        InlineKeyboardButton(text=f"{EMOJI_REFRESH} Обновить",callback_data="tasks:refresh"),
        InlineKeyboardButton(text="🗑 Удалить все",callback_data="tasks:delete_all"),
        InlineKeyboardButton(text="✖ Закрыть",callback_data="tasks:close"),
    ]
    rows.append(ctrl_row)
    return InlineKeyboardMarkup(inline_keyboard=rows)

def task_detail_kb(tid:str)->InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗑 Удалить",callback_data=f"tasks:delete_id:{tid}")],
        [InlineKeyboardButton(text="⬅ Назад",callback_data="tasks:refresh")],
        [InlineKeyboardButton(text="✖ Закрыть",callback_data="tasks:close")]
    ])

def build_apps_kb(n:int)->InlineKeyboardMarkup:
    rows=[]; buf=[]
    for i in range(1,n+1):
        buf.append(InlineKeyboardButton(text=str(i),callback_data=f"apps:detail:{i}"))
        if len(buf)==6: rows.append(buf); buf=[]
    if buf: rows.append(buf)
    rows.append([
        InlineKeyboardButton(text=f"{EMOJI_REFRESH} Обновить",callback_data="apps:refresh"),
        InlineKeyboardButton(text="⬅ Меню",callback_data="back:menu"),
        InlineKeyboardButton(text="✖ Закрыть",callback_data="apps:close"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def apps_detail_kb()->InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅ Назад к заявкам",callback_data="apps:refresh")],
        [InlineKeyboardButton(text="✖ Закрыть",callback_data="apps:close")]
    ])

# ==== Notifications (Created application) ====
def build_created_notification_text(t:Dict[str,Any], chat_id:int)->str:
    tid=t.get("id") or "-"
    date=t.get("date") or (t.get("desired_from_iso","")[:10] if t.get("desired_from_iso") else "-")
    slot=_human_window(t.get("timeslot") or "", t.get("desired_from_iso") or "", t.get("desired_to_iso") or "")
    wh=_task_warehouse_name(t)
    cd=_resolve_crossdock_name_warehouses(t, chat_id)
    qty=_sum_qty(t)
    lines=[
        f"📄 §§B§§Заявка создана§§EB§§",
        SEP_THIN,
        f"ID: §§B§§{tid}§§EB§§",
        f"Статус: ✅ Создано",
        f"Дата: {date} | Окно: {slot}",
        f"Склад поставки: §§B§§{html.escape(wh)}§§EB§§",
        f"Кроссдок: §§B§§{html.escape(cd)}§§EB§§",
        f"Итого: §§B§§{qty} шт§§EB§§",
        ""
    ]
    sl=t.get("sku_list") or []
    if isinstance(sl,list) and sl:
        lines.append("Позиции:")
        for it in sl[:30]:
            sku=it.get("sku")
            q=it.get("total_qty") or it.get("qty") or 0
            sname=get_sku_name_local(int(sku)) if sku else "-"
            lines.append(f"• §§B§§{html.escape(sname)}§§EB§§ (SKU {sku}) — {q} шт")
        lines.append("")
    if lines and lines[-1]=="":
        lines.pop()
    lines.append(SEP_THIN)
    return build_html(lines)

async def scan_and_notify_created(chat_id:int, tasks:List[Dict[str,Any]]):
    """
    Находит «созданные» заявки и отправляет уведомление один раз.
    """
    sent=0
    for t in tasks:
        try:
            tid=str(t.get("id") or "")
            if not tid:
                continue
            if not is_created_like(t):
                continue
            if tid in NOTIFIED_CREATED:
                continue
            # Отправляем уведомление
            text=build_created_notification_text(t, chat_id)
            await send_long(chat_id, text)
            NOTIFIED_CREATED.add(tid)
            # лог-событие
            try:
                _supply_log_append(chat_id, {
                    "status": "CREATED",
                    "text": "Уведомление о создании заявки",
                    "payload": t
                })
            except Exception:
                pass
            sent+=1
        except Exception as e:
            log.warning("notify created failed: %s", e)
    if sent:
        save_state()

# ==== Analyze / snapshot / daily notify ====
async def _ensure_deficit_cache_for_chat(chat_id: int) -> Dict[str, Any]:
    """
    Ensure LAST_DEFICIT_CACHE has valid data for the given chat.
    If missing, performs a fast recomputation and stores the result.
    Returns the cache dict with keys: flat, timestamp, report, raw_rows, consumption_cache.
    """
    cache = LAST_DEFICIT_CACHE.get(chat_id)
    if cache and cache.get("flat"):
        return cache
    
    # Cache missing or empty - recompute
    log.info("Deficit cache missing for chat %s, recomputing...", chat_id)
    try:
        await ensure_sku_names(force=True)
        rows, err = await ozon_stock_fbo(SKU_LIST)
        if err:
            log.warning("Failed to fetch stock for deficit cache: %s", err)
            return {}
        ccache = build_consumption_cache()
        report, flat = generate_deficit_report(rows, SKU_NAME_CACHE, ccache)
        cache = {
            "flat": flat,
            "timestamp": int(time.time()),
            "report": report,
            "raw_rows": rows,
            "consumption_cache": ccache
        }
        LAST_DEFICIT_CACHE[chat_id] = cache
        log.info("Deficit cache recomputed for chat %s: %d items", chat_id, len(flat))
        return cache
    except Exception as e:
        log.exception("Failed to ensure deficit cache for chat %s: %s", chat_id, e)
        return {}

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
            await ensure_sku_names(force=True)
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

async def snapshot_job():
    if time.time()-LAST_SNAPSHOT_TS<SNAPSHOT_MIN_REUSE_SECONDS: return
    rows,err=await ozon_stock_fbo(SKU_LIST)
    if err or not rows: return
    append_snapshot(rows)
    await ensure_sku_names(force=True)
    try:
        ccache=build_consumption_cache(); build_fact_index(rows,[],ccache)
    except Exception as e: log.warning("snapshot index build fail: %s", e)
    await flush_history_if_needed(force=True)

async def daily_notify_job():
    await ensure_fact_index()
    async with ANALYZE_LOCK:
        rows,err=await ozon_stock_fbo(SKU_LIST)
        if err:
            if ADMIN_ID:
                await send_safe_message(ADMIN_ID,f"Ошибка Ozon API: {html.escape(err)}")
            return
        if time.time()-LAST_SNAPSHOT_TS>SNAPSHOT_MIN_REUSE_SECONDS:
            append_snapshot(rows); await flush_history_if_needed(force=True)
        await ensure_sku_names(force=True)
        ccache=build_consumption_cache()
        report,flat=generate_deficit_report(rows,SKU_NAME_CACHE,ccache)
        try: build_fact_index(rows,flat,ccache)
        except Exception as e: log.warning("FACT_INDEX daily build fail: %s", e)
        header=f"{EMOJI_NOTIFY} <b>Ежедневный отчёт {DAILY_NOTIFY_HOUR:02d}:{DAILY_NOTIFY_MINUTE:02d}</b>\n"
        kb=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Все",callback_data="filter:all"),
             InlineKeyboardButton(text="Критично",callback_data="filter:crit"),
             InlineKeyboardButton(text="50–80%",callback_data="filter:mid")],
            [InlineKeyboardButton(text=f"{EMOJI_REFRESH} Обновить",callback_data="action:reanalyze")],
        ])
        targets=list(KNOWN_USERS) or ([ADMIN_ID] if ADMIN_ID else [])
        for uid in targets:
            # Заполняем кэш для фильтров у каждого получателя
            LAST_DEFICIT_CACHE[uid]={"flat":flat,"timestamp":int(time.time()),"report":report,"raw_rows":rows,"consumption_cache":ccache}
            try:
                await send_long(uid, header+report, kb=kb)
            except Exception as e:
                log.warning("daily notify fail to %s: %s", uid, e)
        await flush_history_if_needed()

async def maintenance_job():
    prune_history()
    await flush_history_if_needed()

async def init_snapshot():
    rows,err=await ozon_stock_fbo(SKU_LIST)
    if err or not rows: return
    append_snapshot(rows)
    await ensure_sku_names(force=True)
    try:
        ccache=build_consumption_cache(); build_fact_index(rows,[],ccache)
    except Exception as e: log.warning("init index build fail: %s", e)
    await flush_history_if_needed(force=True)

# ==== Crossdock selection with robust parsing ====
def _parse_crossdock_env(raw:str)->Dict[str,str]:
    """
    Поддерживает форматы:
     - 'ID:NAME,ID2:NAME2'
     - 'NAME ID' или 'ID NAME'
     - Строки через запятую/точку с запятой/перенос строки
    Возвращает dict[id] = name
    """
    m: Dict[str,str] = {}
    if not raw:
        return m
    parts = re.split(r"[,\n;]+", raw)
    for p in parts:
        s=p.strip()
        if not s: continue
        if ":" in s:
            left,right=s.split(":",1)
            left=left.strip(); right=right.strip()
            if re.fullmatch(r"\d{10,}", left):
                m[left]=right
            elif re.fullmatch(r"\d{10,}", right):
                m[right]=left
        else:
            toks=re.split(r"\s+", s)
            if len(toks)>=2:
                if re.fullmatch(r"\d{10,}", toks[0]):
                    _id=toks[0]; name=" ".join(toks[1:])
                    m[_id]=name
                elif re.fullmatch(r"\d{10,}", toks[-1]):
                    _id=toks[-1]; name=" ".join(toks[:-1])
                    m[_id]=name
    return m

CROSSDOCK_RAW = os.getenv("CROSSDOCK_WAREHOUSES","").strip()
CROSSDOCK_MAP: Dict[str,str] = _parse_crossdock_env(CROSSDOCK_RAW)
if DEFAULT_DROPOFF_ID:
    CROSSDOCK_MAP.setdefault(DEFAULT_DROPOFF_ID, DEFAULT_DROPOFF_NAME or "DROP_OFF")

def crossdock_kb()->InlineKeyboardMarkup:
    rows=[]
    if CROSSDOCK_MAP:
        for wid,name in CROSSDOCK_MAP.items():
            title = f"{name} ({wid})" if name else str(wid)
            rows.append([InlineKeyboardButton(text=title[:64], callback_data=f"cdsel:{wid}")])
    else:
        rows.append([InlineKeyboardButton(text="Нет кроссдок-складов (добавьте CROSSDOCK_WAREHOUSES в .env)", callback_data="noop")])
    rows.append([InlineKeyboardButton(text="Пропустить", callback_data="cdsel:skip")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

@dp.callback_query(F.data=="menu_autobook")
async def cb_menu_autobook(c:CallbackQuery,state:FSMContext):
    ensure_admin(c.from_user.id)
    await state.set_state(AutobookStates.choose_crossdock)
    await c.message.answer("Выберите склад отправления (кроссдок):", reply_markup=crossdock_kb())
    await c.answer()

@dp.callback_query(F.data.startswith("cdsel:"))
async def cb_crossdock_pick(c:CallbackQuery,state:FSMContext):
    ensure_admin(c.from_user.id)
    choice=c.data.split(":",1)[1]
    if choice=="skip":
        cd_id=""; cd_name=""
        await c.answer("Кроссдок пропущен")
    else:
        cd_id=choice; cd_name=CROSSDOCK_MAP.get(choice,"")
        await c.answer(f"Кроссдок: {cd_name or cd_id}")
    CROSSDOCK_SELECTED[c.message.chat.id]={"id":cd_id,"name":cd_name}
    await state.update_data(crossdock_id=cd_id, crossdock_name=cd_name)
    if sw and hasattr(sw, "set_global_crossdock"):
        try:
            sw.set_global_crossdock(cd_id or None, cd_name or None)
            log.info("Global crossdock set in supply_watch: %s (%s)", cd_name, cd_id)
        except Exception as e:
            log.warning("set_global_crossdock failed: %s", e)
    if AUTOBOOK_ENABLED and abf:
        for fn_name in ("set_crossdock","set_crossdock_context","set_crossdock_for_chat","set_dropoff","set_drop_off","set_drop_off_id","set_global_crossdock"):
            if hasattr(abf, fn_name):
                try:
                    getattr(abf, fn_name)(c.message.chat.id, cd_id, cd_name)
                    log.info("Crossdock passed to external via %s", fn_name)
                    break
                except Exception as e:
                    log.warning("Crossdock external setter error: %s", e)
    await state.set_state(AutobookStates.after_crossdock)
    kb=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Начать мастер", callback_data="ab_start")],
        [InlineKeyboardButton(text="Отмена", callback_data="ab_cancel")]
    ])
    await c.message.answer(f"Готово. Кроссдок: {cd_name or '—'}.\nНажмите 'Начать мастер' чтобы перейти к бронированию.", reply_markup=kb)

@dp.callback_query(F.data=="ab_cancel")
async def cb_autobook_cancel(c:CallbackQuery,state:FSMContext):
    await state.clear()
    await c.message.answer("Автобронирование отменено.")
    await c.answer()

@dp.callback_query(F.data=="ab_start")
async def cb_autobook_start(c:CallbackQuery,state:FSMContext):
    ensure_admin(c.from_user.id)
    data=await state.get_data()
    cd_id=data.get("crossdock_id",""); cd_name=data.get("crossdock_name","")
    await ensure_sku_names(force=True)
    try_mount_external_name_resolver()
    if sw and hasattr(sw, "set_global_crossdock"):
        try:
            sw.set_global_crossdock(cd_id or None, cd_name or None)
        except Exception:
            pass
    if AUTOBOOK_ENABLED and autobook_router is not None and hasattr(abf,"start_autobook"):
        try:
            await abf.start_autobook(chat_id=c.message.chat.id, crossdock_id=cd_id, crossdock_name=cd_name)
            await c.message.answer("Внешний мастер запущен.")
            # Включаем авто-наблюдение при создании задач мастером
            try:
                import supply_watch as swm
                if hasattr(swm, "enable_auto_watch_for_chat"):
                    swm.enable_auto_watch_for_chat(c.message.chat.id, True)
            except Exception:
                pass
        except Exception as e:
            await c.message.answer(f"Не удалось запустить внешний мастер: {e}\nИспользуйте /autobook для fallback.")
    else:
        await c.message.answer("Fallback мастер: используйте /autobook для создания заявки (внутренние шаги).")
    await state.clear()
    await c.answer()

# ==== Main menu keyboard ====
def main_menu_kb()->ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🔧 Автобронирование"),
         KeyboardButton(text=f"{EMOJI_LIST} Заявки"),
         KeyboardButton(text=f"{EMOJI_TASKS} Задачи")],
        [KeyboardButton(text="🔍 Анализ"),
         KeyboardButton(text="🤖 AI чат")],
        [KeyboardButton(text="📦 Товары"),
         KeyboardButton(text="🏬 Склады"),
         KeyboardButton(text="🗺 Кластеры")],
        [KeyboardButton(text="❌ Отмена")],
    ], resize_keyboard=True)

def start_overview()->str:
    rows=[
        ("🔍 Анализ","Пересчёт дефицитов"),
        ("📦 Товары","Список SKU (имена)"),
        ("🏬 Склады","Остатки по складам"),
        ("🗺 Кластеры","Группы складов"),
        ("📄 Заявки","Последние заявки"),
        ("📋 Задачи","Активные задачи (до заполнения)"),
        ("🤖 AI чат","FACT / GENERAL диалог"),
        ("🔧 Автобронирование","Выбор кроссдока + мастер"),
        ("❌ Отмена","Сброс FSM / AI"),
    ]
    lines=[]
    header=f"{'═'*22}  {EMOJI_INFO} ОБЗОР  {'═'*22}"
    lines.append(header)
    lines.append(f"Версия: {VERSION} | ChatMode={BOT_STATE.get('chat_mode','?').upper()} | Style={'ON' if BOT_STATE.get('style_enabled') else 'OFF'} | ClusterView={BOT_STATE.get('cluster_view_mode')}")
    lines.append(SEP_THIN)
    cmds=["tasks","all_tasks","autobook","analyze","stock","warehouses","clusters","supplies","ai","ask","chat_mode","ai_reset_token"]
    lines.append("Команды: "+ " /".join(f"/{c}" for c in cmds))
    lines.append(SEP_THIN)
    ml=max(len(k) for k,_ in rows)
    for k,d in rows:
        lines.append(f"{k}{' '*(ml-len(k))} │ {d}")
    lines.append(SEP_THIN)
    lines.append(f"Autobook: {'external' if AUTOBOOK_ENABLED else 'fallback'} | Кроссдоков: {len(CROSSDOCK_MAP)}")
    lines.append("═"*len(header))
    return build_html(lines)

def version_info()->str:
    import sys
    return (f"Версия: {VERSION}\nPython: {sys.version.split()[0]}\nSnapshots: {len(HISTORY_CACHE)}\n"
            f"SKU index: {len(FACT_INDEX.get('sku', {}))}\nClusters: {len(FACT_INDEX.get('cluster', {}))}\n"
            f"ChatMode: {BOT_STATE.get('chat_mode')} Style:{BOT_STATE.get('style_enabled')} "
            f"ClusterView:{BOT_STATE.get('cluster_view_mode')} Autobook={'external' if AUTOBOOK_ENABLED else 'fallback'}")

# ==== Diagnostics & supplies summary ====
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
        for td in top_def[:8]:
            lines.append(f"• {td['name'][:40]} (SKU {td['sku']}) покрытие {td['coverage']:.2f} потребность {td['deficit_need']}")
    else:
        lines.append("Нет дефицита.")
    lines.append("")
    lines+=[s("Склады с потребностью"), SEP_THIN]
    if top_wh:
        for w in top_wh[:6]:
            lines.append(f"• {bold(w['name'][:40])}: потребность {w['total_need']}, дефицит {w['deficit_need']}")
    else:
        lines.append("Нет данных.")
    lines.append("")
    lines+=[s("Кластеры с потребностью"), SEP_THIN]
    if top_cl:
        for c in top_cl[:6]:
            lines.append(f"• {bold(c['name'][:40])}: потребность {c['total_need']}, дефицит {c['deficit_need']}")
    else:
        lines.append("Нет данных.")
    lines.append("")
    lines+=[s("Производительность"), SEP_THIN,
            f"API: {LAST_API_LATENCY_MS:.0f} мс | Анализ: {LAST_ANALYZE_MS:.0f} мс | Ошибка: {LAST_ANALYZE_ERROR or '—'}",
            f"Snapshots: {len(HISTORY_CACHE)} | Кэш AI: {len(ANSWER_CACHE)}"]
    return build_html(lines)

def build_supplies_last_created(limit:int=3)->str:
    """
    Build text for last created applications. Shows last 3 created entries with both 
    sides (warehouse + crossdock) and explicit status labels.
    """
    arr=SUPPLY_EVENTS.get("*") or []
    try:
        loop=asyncio.get_running_loop()
        if loop.is_running():
            loop.create_task(ensure_sku_names(force=True))
    except Exception:
        pass
    if not arr:
        return build_html([f"{EMOJI_LIST} Нет событий по заявкам."])
    created=[]; seen=set()
    for e in reversed(arr):
        payload=e.get("payload") or {}
        tid=payload.get("id") or payload.get("task_id") or ""
        if not tid or tid in seen: continue
        text=(e.get("text") or "")
        status=(e.get("status") or "")
        # Include ORDER_DATA_FILLING as created state
        if "создан" in text.lower() or status.lower() in ("создано","created","done","success","finished","completed","supply_created","order_data_filling"):
            seen.add(tid); created.append(e)
            if len(created)>=limit: break
    if not created:
        return build_html([f"{EMOJI_LIST} Пока нет созданных заявок."])
    lines=[f"{EMOJI_LIST} §§B§§Последние заявки ({len(created)})§§EB§§", SEP_THIN]
    for e in created:
        payload=e.get("payload") or {}
        tid=payload.get("id") or payload.get("task_id") or "-"
        sku_list=payload.get("sku_list") or []
        qty=sum(int(it.get("qty") or it.get("total_qty") or 0) for it in sku_list)
        wh=payload.get("warehouse_name") or payload.get("drop_off_name") or "-"
        cd=payload.get("crossdock_name") or payload.get("crossdock_id") or ""
        slot=_human_window(payload.get("timeslot") or "", payload.get("desired_from_iso") or "", payload.get("desired_to_iso") or "")
        # Determine status label
        status_text = (e.get("status") or "").lower()
        if status_text in ("done", "success", "finished", "completed"):
            status_label = "Статус: ✅ Готово"
        else:
            status_label = "Статус: ✅ Создано"
        lines.append(f"ID: §§B§§{tid}§§EB§§ | §§B§§{qty} шт§§EB§§ | Окно: {html.escape(slot)}")
        lines.append(status_label)
        lines.append(f"Склад поставки: §§B§§{html.escape(wh)}§§EB§§ | Кроссдок: §§B§§{html.escape(cd)}§§EB§§")
        if sku_list:
            lines.append("Позиции:")
            for it in sku_list[:30]:
                sku=it.get("sku"); q=it.get("qty") or it.get("total_qty") or 0
                sname=get_sku_name_local(int(sku)) if sku else "-"
                lines.append(f"• §§B§§{html.escape(sname)}§§EB§§ (SKU {sku}) — {q} шт")
        lines.append("")
    if lines and lines[-1]=="":
        lines.pop()
    lines.append(SEP_THIN)
    return build_html(lines)

# ==== Utility: ensure_admin ====
def ensure_admin(uid:int):
    global ADMIN_ID
    if ADMIN_ID is None:
        ADMIN_ID=uid
    KNOWN_USERS.add(uid); save_known_users()

# ==== Stock/Warehouses/Clusters render helpers ====
async def render_stock_list(chat_id:int, edit_message:Optional[Message]=None):
    await ensure_sku_names(force=True)
    await ensure_fact_index()
    total=len(SKU_LIST)
    if total==0:
        kb=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Нет SKU",callback_data="noop")],
                                                 [InlineKeyboardButton(text="⬅ Назад", callback_data="back:menu")]])
        text=f"{EMOJI_BOX} Товары:"
        if edit_message:
            try:
                await edit_message.edit_text(text, reply_markup=kb)
                return
            except Exception:
                pass
        await send_safe_message(chat_id, text, reply_markup=kb)
        return

    start=0; end=min(start+STOCK_PAGE_SIZE,total)
    btn=[]
    for sku in SKU_LIST[start:end]:
        nm=SKU_NAME_CACHE.get(sku,f"SKU {sku}")
        btn.append([InlineKeyboardButton(text=f"{nm[:48]} (SKU {sku})",callback_data=f"sku:{sku}")])
    nav=[InlineKeyboardButton(text=f"1/{(total+STOCK_PAGE_SIZE-1)//STOCK_PAGE_SIZE}",callback_data="noop")]
    btn.append(nav)
    btn.append([InlineKeyboardButton(text="⬅ Назад", callback_data="back:menu")])
    kb=InlineKeyboardMarkup(inline_keyboard=btn)
    text=f"{EMOJI_BOX} Товары:"
    if edit_message:
        try:
            await edit_message.edit_text(text, reply_markup=kb)
            return
        except Exception:
            pass
    await send_safe_message(chat_id, text, reply_markup=kb)

async def render_warehouses_list(chat_id:int, edit_message:Optional[Message]=None):
    await ensure_sku_names(force=True)
    rows, err=await ozon_stock_fbo(SKU_LIST)
    if err:
        await send_safe_message(chat_id, f"Ошибка Ozon API: {html.escape(err)}")
        return
    agg=aggregate_rows(rows); wh_map={}
    for wmap in agg.values():
        for wk,info in wmap.items():
            wh_map.setdefault(wk,info["warehouse_name"])
    if not wh_map:
        kb=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅ Назад", callback_data="back:menu")]])
        text=f"{EMOJI_WH} Нет данных по складам."
        if edit_message:
            try: await edit_message.edit_text(text, reply_markup=kb)
            except Exception: pass
        else:
            await send_safe_message(chat_id, text, reply_markup=kb)
        return
    kb_rows=[]
    WAREHOUSE_CB_MAP.clear()
    for wk,nm in sorted(wh_map.items(), key=lambda x:x[1].lower()):
        hid=hashlib.sha1(str(wk).encode()).hexdigest()[:10]
        WAREHOUSE_CB_MAP[hid]=(wk,nm)
        kb_rows.append([InlineKeyboardButton(text=nm[:60],callback_data=f"whid:{hid}")])
    kb_rows.append([InlineKeyboardButton(text="⬅ Назад", callback_data="back:menu")])
    kb=InlineKeyboardMarkup(inline_keyboard=kb_rows)
    text=f"{EMOJI_WH} Склады:"
    if edit_message:
        try: await edit_message.edit_text(text, reply_markup=kb)
        except Exception: pass
    else:
        await send_safe_message(chat_id, text, reply_markup=kb)

async def render_clusters_list(chat_id:int, edit_message:Optional[Message]=None):
    await ensure_fact_index()
    if not FACT_INDEX.get("cluster"):
        kb=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅ Назад", callback_data="back:menu")]])
        text="Нет данных кластеров. Запустите /analyze."
        if edit_message:
            try: await edit_message.edit_text(text)
            except Exception: pass
        else:
            await send_safe_message(chat_id, text)
        return
    kb=[]
    for cname in sorted(FACT_INDEX["cluster"].keys(), key=lambda c: FACT_INDEX["cluster"][c]["deficit_need"], reverse=True):
        kb.append([InlineKeyboardButton(text=cname[:40],callback_data=f"cluster:{cname}")])
    kb.append([InlineKeyboardButton(text="⬅ Назад", callback_data="back:menu")])
    text=f"{EMOJI_CLUSTER} Кластеры:"
    if edit_message:
        try: await edit_message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
        except Exception: pass
    else:
        await send_safe_message(chat_id, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

# ==== Detail renderers for SKU / Warehouse / Cluster ====
def build_sku_detail_text(sku:int)->str:
    if not FACT_INDEX.get("sku"):
        return build_html(["Нет данных по индексу. Запустите /analyze."])
    entry=FACT_INDEX["sku"].get(sku)
    if not entry:
        return build_html([f"Нет данных для SKU {sku}."])
    name=entry["name"]; total_qty=entry["total_qty"]; worst=entry["worst_coverage"]
    lines=[f"{EMOJI_BOX} §§B§§{html.escape(name)}§§EB§§ (SKU {sku})",
           f"Итого остаток: {total_qty}",
           f"Худшее покрытие: {int(worst*100):02d}%", SEP_THIN]
    for w in entry.get("warehouses", []):
        cov=w["coverage"]; bar, sev = coverage_bar(cov)
        badge=need_pct_text(w["qty"], w["norm"], w["target"])
        lines.append(f"• §§B§§{html.escape(w['name'])}§§EB§§: Остаток {w['qty']} / Норма {w['norm']} / Цель {w['target']} → +{w['need']}")
        lines.append(f"  {bar} {sev} · {badge}")
        lines.append("")
    if lines and lines[-1]=="":
        lines.pop()
    return build_html(lines)

def build_warehouse_detail_text(wkey:str,wname:str)->str:
    if not FACT_INDEX.get("sku"):
        return build_html(["Нет данных по индексу. Запустите /analyze."])
    items=[]
    for sku, entry in FACT_INDEX["sku"].items():
        for w in entry.get("warehouses", []):
            if w["wkey"]==wkey:
                items.append({
                    "sku": sku,
                    "name": entry["name"],
                    "qty": w["qty"], "norm": w["norm"], "target": w["target"],
                    "need": w["need"], "coverage": w["coverage"]
                })
    if not items:
        return build_html([f"{EMOJI_WH} §§B§§{html.escape(wname)}§§EB§§","Нет позиций."])
    items.sort(key=lambda x:(x["coverage"], -x["need"]))
    lines=[f"{EMOJI_WH} §§B§§{html.escape(wname)}§§EB§§", SEP_THIN]
    for it in items[:60]:
        bar, sev = coverage_bar(it["coverage"])
        badge=need_pct_text(it["qty"], it["norm"], it["target"])
        lines.append(f"• §§B§§{html.escape(it['name'])}§§EB§§ (SKU {it['sku']})")
        lines.append(f"  Остаток {it['qty']} / Норма {it['norm']} / Цель {it['target']} → +{it['need']} · {badge}")
        lines.append(f"  {bar} {sev}")
        lines.append("")
    if lines and lines[-1]=="":
        lines.pop()
    return build_html(lines)

# ==== Commands ====
@dp.message(Command("version"))
async def cmd_version(m:Message):
    ensure_admin(m.from_user.id)
    await m.answer(version_info())

@dp.message(Command("help"))
@dp.message(Command("start"))
async def cmd_start(m:Message):
    ensure_admin(m.from_user.id)
    load_known_users()
    await ensure_sku_names(force=True)
    await ensure_fact_index()
    if sw and hasattr(sw, "set_global_crossdock") and DEFAULT_DROPOFF_ID:
        try:
            sw.set_global_crossdock(DEFAULT_DROPOFF_ID, DEFAULT_DROПОFF_NAME if 'DEFAULT_DРОПОFF_NAME' in globals() else DEFAULT_DROPOFF_NAME)  # type: ignore
            log.info("Global crossdock set from ENV at start: %s (%s)", DEFAULT_DROПОFF_NAME if 'DEFAULT_DРОПОFF_NAME' in globals() else DEFAULT_DROPOFF_NAME, DEFAULT_DROPOFF_ID)  # type: ignore
        except Exception as e:
            log.warning("set_global_crossdock at start failed: %s", e)
    try_mount_external_name_resolver()
    await m.answer(f"{EMOJI_OK} Бот активен. Версия {VERSION}.", reply_markup=main_menu_kb())
    kb=[[InlineKeyboardButton(text="Автобронирование",callback_data="menu_autobook")]]
    await send_long(m.chat.id, start_overview(), kb=InlineKeyboardMarkup(inline_keyboard=kb))

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

@dp.message(Command("chat_mode"))
async def cmd_chat_mode(m:Message):
    ensure_admin(m.from_user.id)
    mode=BOT_STATE.get("chat_mode","fact").upper()
    kb=[[InlineKeyboardButton(text="🔁 Переключить",callback_data="chatmode:toggle")]]
    await m.answer(build_html(["§§B§§Режим чата§§EB§§",f"Текущий: {mode}","/fact /general или кнопка."]), reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.callback_query(F.data=="chatmode:toggle")
async def cb_chatmode_toggle(c:CallbackQuery):
    cur=BOT_STATE.get("chat_mode","fact")
    BOT_STATE["chat_mode"]="general" if cur=="fact" else "fact"
    save_state()
    await c.message.edit_text(build_html(["§§B§§Режим чата§§EB§§",f"Текущий: {BOT_STATE['chat_mode'].upper()}","/fact /general или снова нажмите кнопку."]),
                              reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔁 Переключить",callback_data="chatmode:toggle")]]))
    await c.answer("Переключено.")

@dp.message(Command("fact"))
async def cmd_fact(m:Message):
    ensure_admin(m.from_user.id); BOT_STATE["chat_mode"]="fact"; save_state(); await m.answer("FACT")

@dp.message(Command("general"))
async def cmd_general(m:Message):
    ensure_admin(m.from_user.id); BOT_STATE["chat_mode"]="general"; save_state(); await m.answer("GENERAL")

@dp.message(Command("chat"))
async def cmd_chat(m:Message):
    ensure_admin(m.from_user.id); q=m.text.partition(" ")[2].strip()
    if not q: await m.answer("Формат: /chat <сообщение>"); return
    await m.answer(f"{EMOJI_CHAT} Общаюсь…")
    raw,mode=await llm_general_answer(m.chat.id,q); styled=style_ai_answer(q,raw,mode,False)
    await send_long(m.chat.id, styled)

@dp.message(Command("analyze"))
async def cmd_analyze(m:Message):
    ensure_admin(m.from_user.id)
    await handle_analyze(m.chat.id)

@dp.message(Command("force_notify"))
async def cmd_force_notify(m:Message):
    ensure_admin(m.from_user.id)
    await m.answer("Отчёт…")
    await daily_notify_job()
    await m.answer("Готово.")

@dp.message(Command("diag"))
async def cmd_diag(m:Message):
    ensure_admin(m.from_user.id)
    await ensure_fact_index()
    rep=build_diag_report()
    await send_long(m.chat.id, rep)

@dp.message(Command("stock"))
async def cmd_stock(m:Message):
    ensure_admin(m.from_user.id)
    await render_stock_list(m.chat.id)

@dp.message(Command("warehouses"))
async def cmd_warehouses(m:Message):
    ensure_admin(m.from_user.id)
    await render_warehouses_list(m.chat.id)

@dp.message(Command("clusters"))
async def cmd_clusters(m:Message):
    ensure_admin(m.from_user.id)
    await render_clusters_list(m.chat.id)

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

# ==== Text buttons ====
@dp.message(F.text == "🔍 Анализ")
@dp.message(F.text.regexp(r"(?i)^анализ$"))
async def btn_analyze(m:Message):
    ensure_admin(m.from_user.id)
    await handle_analyze(m.chat.id)

@dp.message(F.text == "📦 Товары")
@dp.message(F.text.regexp(r"(?i)^товар"))
async def btn_stock(m:Message):
    ensure_admin(m.from_user.id)
    await render_stock_list(m.chat.id)

@dp.message(F.text == "🏬 Склады")
@dp.message(F.text.regexp(r"(?i)^склад"))
async def btn_warehouses(m:Message):
    ensure_admin(m.from_user.id)
    await render_warehouses_list(m.chat.id)

@dp.message(F.text == "🗺 Кластеры")
@dp.message(F.text.regexp(r"(?i)^кластер"))
async def btn_clusters_btn(m:Message):
    ensure_admin(m.from_user.id)
    await render_clusters_list(m.chat.id)

@dp.message(F.text == "🔧 Автобронирование")
@dp.message(F.text.regexp(r"(?i)автоброн"))
async def btn_autobook(m:Message,state:FSMContext):
    ensure_admin(m.from_user.id)
    await state.set_state(AutobookStates.choose_crossdock)
    await m.answer("Выберите склад отправления (кроссдок):", reply_markup=crossdock_kb())

@dp.message(F.text == "🤖 AI чат")
@dp.message(F.text.regexp(r"(?i)^ai\s*чат"))
async def btn_ai_chat(m:Message,state:FSMContext):
    ensure_admin(m.from_user.id)
    await ensure_fact_index()
    await state.set_state(AIChatState.waiting)
    mode=BOT_STATE.get("chat_mode","fact")
    await m.answer(f"AI чат включён. Режим: {mode.upper()}.\nНапишите вопрос.\nКоманды: /fact /general /cancel", reply_markup=main_menu_kb())

@dp.message(F.text == "❌ Отмена")
@dp.message(F.text.regexp(r"(?i)^отмена$"))
async def btn_cancel(m:Message,state:FSMContext):
    ensure_admin(m.from_user.id)
    await state.clear()
    await m.answer("Состояния сброшены.", reply_markup=main_menu_kb())

# ==== AI Chat FSM ====
@dp.message(AIChatState.waiting)
async def ai_chat_waiting(m:Message,state:FSMContext):
    ensure_admin(m.from_user.id)
    q=(m.text or "").strip()
    if not q:
        await m.answer("Пришлите текст."); return
    lower=q.lower()
    if lower in ("/cancel","cancel","❌ отмена","отмена"):
        await state.clear(); await m.answer("AI чат закрыт.", reply_markup=main_menu_kb()); return
    if lower=="/fact":
        BOT_STATE["chat_mode"]="fact"; save_state(); await m.answer("FACT режим."); return
    if lower=="/general":
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
        log.exception("ai_chat error")
        await m.answer(f"Ошибка AI: {e}")

# ==== Active tasks list (extended) ====
async def render_creating_tasks_list(chat_id:int, edit_message:Message=None):
    """
    Рендерит АКТИВНЫЕ задачи: пред-заявочные стадии (DRAFT/WAIT/SLOT/...), исключая ошибки/отмены
    и исключая стадии заполнения/создания заявок.
    """
    try:
        _load_crossdocks_warehouses_env()
        _load_crossdocks_env()
    except Exception:
        pass
    await ensure_sku_names(force=True)
    tasks=await fetch_tasks_global()
    # Уведомим о созданных (если попались)
    await scan_and_notify_created(chat_id, tasks)
    active=[t for t in tasks if is_active_task(t)]
    TASKS_CACHE[chat_id]=active
    text=build_tasks_list_text(active, chat_id)
    kb=build_tasks_kb(len(active))
    if edit_message:
        try:
            await edit_message.edit_text(text, parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True)
            return
        except Exception:
            pass
    await send_safe_message(chat_id, text, parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True)

# Backward-compatible alias (некоторые версии кода вызывали именно это имя)
async def render_active_tasks_list(chat_id:int, edit_message:Optional[Message]=None):
    await render_creating_tasks_list(chat_id, edit_message)

# ==== Applications (Заявки) ====
def build_applications_list_text(apps:List[Dict[str,Any]], chat_id:int)->str:
    if not apps:
        return build_html(["§§B§§📄 Заявки (0)§§EB§§", SEP_THIN, "Заявок нет."])
    lines=[f"§§B§§📄 Заявки ({len(apps)})§§EB§§", SEP_THIN]
    for i,t in enumerate(apps,1):
        em,stage=classify_task_stage(t)
        qty=_sum_qty(t)
        date=t.get("date") or (t.get("desired_from_iso","")[:10] if t.get("desired_from_iso") else "-")
        slot=_human_window(t.get("timeslot") or "", t.get("desired_from_iso") or "", t.get("desired_to_iso") or "")
        tid=t.get("id") or "-"
        wh=_task_warehouse_name(t)
        cd=_resolve_crossdock_name_warehouses(t, chat_id)
        status_label=_application_status_text((t.get("status") or t.get("state") or ""))
        lines.append(f"{i}) {em} {stage} | §§B§§{qty} шт§§EB§§ | {date} — {slot} | §§B§§{tid}§§EB§§")
        lines.append(f"   Статус: {status_label}")
        lines.append(f"   Склад поставки: §§B§§{html.escape(wh)}§§EB§§ | Кроссдок: §§B§§{html.escape(cd)}§§EB§§")
        sl=t.get("sku_list")
        if isinstance(sl,list) and sl:
            lines.append("   Позиции:")
            for it in sl[:20]:
                sku=it.get("sku"); q=it.get("total_qty") or it.get("qty") or 0
                sname=get_sku_name_local(int(sku)) if sku else "-"
                lines.append(f"   • §§B§§{html.escape(sname)}§§EB§§ (SKU {sku}) — {q} шт")
        lines.append("")
    if lines and lines[-1]=="":
        lines.pop()
    lines.append(SEP_THIN)
    return build_html(lines)

async def render_applications_list(chat_id:int, edit_message:Optional[Message]=None):
    try:
        _load_crossdocks_warehouses_env()
        _load_crossdocks_env()
    except Exception:
        pass
    await ensure_sku_names(force=True)
    tasks=await fetch_tasks_global()
    # Уведомим о созданных
    await scan_and_notify_created(chat_id, tasks)
    apps=[t for t in tasks if is_application_task(t)]
    APPS_CACHE[chat_id]=apps
    # Если нет — показываем последние созданные по событиям
    if not apps:
        text=build_supplies_last_created(limit=5)
        kb=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"{EMOJI_REFRESH} Обновить",callback_data="apps:refresh")],
            [InlineKeyboardButton(text="⬅ Меню",callback_data="back:menu")],
            [InlineKeyboardButton(text="✖ Закрыть",callback_data="apps:close")]
        ])
    else:
        text=build_applications_list_text(apps, chat_id)
        kb=build_apps_kb(len(apps))
    if edit_message:
        try:
            await edit_message.edit_text(text, parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True)
            return
        except Exception:
            pass
    await send_safe_message(chat_id, text, parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True)

# ==== Buttons and commands for tasks/apps ====
@dp.message(F.text == "📋 Задачи")
async def btn_tasks(m:Message):
    ensure_admin(m.from_user.id)
    await render_creating_tasks_list(m.chat.id)

@dp.message(Command("tasks"))
async def cmd_tasks(m:Message):
    ensure_admin(m.from_user.id)
    await render_creating_tasks_list(m.chat.id)

@dp.message(Command("all_tasks"))
async def cmd_all_tasks(m:Message):
    ensure_admin(m.from_user.id)
    await render_creating_tasks_list(m.chat.id)

@dp.callback_query(F.data=="tasks:refresh")
async def cb_tasks_refresh(c:CallbackQuery):
    ensure_admin(c.from_user.id)
    await render_creating_tasks_list(c.message.chat.id, edit_message=c.message)
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

@dp.callback_query(F.data.startswith("tasks:detail:"))
async def cb_tasks_detail(c:CallbackQuery):
    ensure_admin(c.from_user.id)
    try: idx=int(c.data.rsplit(":",1)[1])-1
    except Exception: await c.answer(); return
    tasks=TASKS_CACHE.get(c.message.chat.id) or []
    if idx<0 or idx>=len(tasks):
        await c.answer("Нет задачи.")
        await render_creating_tasks_list(c.message.chat.id, edit_message=c.message)
        return
    t=tasks[idx]
    tid=str(t.get("id") or "-")
    text=build_task_detail_text(t, c.message.chat.id)
    kb=task_detail_kb(tid)
    try:
        await c.message.edit_text(text,parse_mode="HTML",reply_markup=kb,disable_web_page_preview=True)
    except Exception:
        await send_safe_message(c.message.chat.id,text,parse_mode="HTML",reply_markup=kb,disable_web_page_preview=True)
    await c.answer()

@dp.callback_query(F.data.startswith("tasks:delete_id:"))
async def cb_tasks_delete_id(c:CallbackQuery):
    ensure_admin(c.from_user.id)
    tid=c.data.split(":",2)[2]
    ok=False
    try:
        ok = await delete_task_by_id(tid)
    except Exception as e:
        log.warning("delete by id failed: %s", e)
    _remove_task_from_caches(c.message.chat.id, tid)
    await c.answer("Удалено." if ok else "Не удалось удалить (см. логи).")
    await render_creating_tasks_list(c.message.chat.id, edit_message=c.message)

@dp.callback_query(F.data=="tasks:delete_all")
async def cb_tasks_delete_all(c:CallbackQuery):
    ensure_admin(c.from_user.id)
    cnt = await delete_all_tasks_for_chat(c.message.chat.id)
    await c.answer(f"Удалено задач: {cnt}")
    await render_creating_tasks_list(c.message.chat.id, edit_message=c.message)

@dp.message(F.text == "📄 Заявки")
async def btn_zayavki(m:Message):
    ensure_admin(m.from_user.id)
    await render_applications_list(m.chat.id)

@dp.message(Command("supplies"))
async def cmd_supplies(m:Message):
    ensure_admin(m.from_user.id)
    await render_applications_list(m.chat.id)

@dp.callback_query(F.data=="apps:refresh")
async def cb_apps_refresh(c:CallbackQuery):
    ensure_admin(c.from_user.id)
    await render_applications_list(c.message.chat.id, edit_message=c.message)
    await c.answer("Обновлено")

@dp.callback_query(F.data=="apps:close")
async def cb_apps_close(c:CallbackQuery):
    ensure_admin(c.from_user.id)
    try:
        await c.message.edit_reply_markup(reply_markup=None)
        await c.message.edit_text(build_html(["Список заявок закрыт."]))
    except Exception:
        pass
    await c.answer()

@dp.callback_query(F.data.startswith("apps:detail:"))
async def cb_apps_detail(c:CallbackQuery):
    ensure_admin(c.from_user.id)
    try: idx=int(c.data.rsplit(":",1)[1])-1
    except Exception: await c.answer(); return
    apps=APPS_CACHE.get(c.message.chat.id) or []
    if idx<0 or idx>=len(apps):
        await c.answer("Нет заявки.")
        await render_applications_list(c.message.chat.id, edit_message=c.message)
        return
    t=apps[idx]
    text=build_task_detail_text(t, c.message.chat.id)
    kb=apps_detail_kb()
    try:
        await c.message.edit_text(text,parse_mode="HTML",reply_markup=kb,disable_web_page_preview=True)
    except Exception:
        await send_safe_message(c.message.chat.id,text,parse_mode="HTML",reply_markup=kb,disable_web_page_preview=True)
    await c.answer()

# ==== Stock/Warehouses/Clusters callbacks ====
@dp.callback_query(F.data=="open:stock")
async def cb_open_stock(c:CallbackQuery):
    ensure_admin(c.from_user.id)
    await render_stock_list(c.message.chat.id, edit_message=c.message)
    await c.answer()

@dp.callback_query(F.data=="open:warehouses")
async def cb_open_wh(c:CallbackQuery):
    ensure_admin(c.from_user.id)
    await render_warehouses_list(c.message.chat.id, edit_message=c.message)
    await c.answer()

@dp.callback_query(F.data=="open:clusters")
async def cb_open_clusters(c:CallbackQuery):
    ensure_admin(c.from_user.id)
    await render_clusters_list(c.message.chat.id, edit_message=c.message)
    await c.answer()

@dp.callback_query(F.data.startswith("sku:"))
async def cb_sku_detail(c:CallbackQuery):
    ensure_admin(c.from_user.id)
    try:
        sku=int(c.data.split(":",1)[1])
    except Exception:
        await c.answer("Неверный SKU")
        return
    await ensure_fact_index()
    text=build_sku_detail_text(sku)
    kb=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅ Назад к товарам", callback_data="open:stock")],
        [InlineKeyboardButton(text="✖ Закрыть", callback_data="noop")]
    ])
    try:
        await c.message.edit_text(text, parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True)
    except Exception:
        await send_safe_message(c.message.chat.id, text, parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True)
    await c.answer()

@dp.callback_query(F.data.startswith("whid:"))
async def cb_wh_detail(c:CallbackQuery):
    ensure_admin(c.from_user.id)
    hid=c.data.split(":",1)[1]
    pair=WAREHOUSE_CB_MAP.get(hid)
    if not pair:
        await c.answer("Склад не найден, обновляю список…")
        await render_warehouses_list(c.message.chat.id, edit_message=c.message)
        return
    wkey,wname=pair
    await ensure_fact_index()
    text=build_warehouse_detail_text(wkey,wname)
    kb=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅ Назад к складам", callback_data="open:warehouses")],
        [InlineKeyboardButton(text="✖ Закрыть", callback_data="noop")]
    ])
    try:
        await c.message.edit_text(text, parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True)
    except Exception:
        await send_safe_message(c.message.chat.id, text, parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True)
    await c.answer()

@dp.callback_query(F.data.startswith("cluster:"))
async def cb_cluster_detail(c:CallbackQuery):
    ensure_admin(c.from_user.id)
    cname=c.data.split(":",1)[1]
    await ensure_fact_index()
    text=build_cluster_detail(cname, FACT_INDEX.get("cluster",{}), FACT_INDEX.get("sku",{}), short=False)
    kb=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅ Назад к кластерам", callback_data="open:clusters")],
        [InlineKeyboardButton(text="✖ Закрыть", callback_data="noop")]
    ])
    try:
        await c.message.edit_text(text, parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True)
    except Exception:
        await send_safe_message(c.message.chat.id, text, parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True)
    await c.answer()

@dp.callback_query(F.data=="back:menu")
async def cb_back_menu(c:CallbackQuery):
    ensure_admin(c.from_user.id)
    await c.message.edit_text("Вы в главном меню.", reply_markup=None)
    await bot.send_message(c.message.chat.id, "Готово.", reply_markup=main_menu_kb())
    await c.answer()

# ==== Analyze filters and actions ====
def _build_filtered_deficit_text(flat:List[Dict[str,Any]], mode:str)->str:
    # mode: all | crit | mid
    if not flat:
        return build_html([f"{EMOJI_ANALYZE} Нет данных."])
    def pass_item(it):
        cov=it.get("coverage", 0.0)
        if mode=="crit":
            return cov<0.5
        if mode=="mid":
            return 0.5<=cov<0.8
        return True
    by_sku: Dict[int, List[Dict[str,Any]]] = {}
    for it in flat:
        if pass_item(it):
            by_sku.setdefault(int(it["sku"]), []).append(it)
    if not by_sku:
        return build_html([f"{EMOJI_ANALYZE} Нет позиций для выбранного фильтра."])
    lines=[f"{EMOJI_ANALYZE} §§B§§Дефицит ({'все' if mode=='all' else ('критично' if mode=='crit' else '50–80%')})§§EB§§", LEGEND_TEXT, SEP_THIN]
    order=sorted(by_sku.keys(), key=lambda s: min(x["coverage"] for x in by_sku[s]))
    for sku in order[:80]:
        items=sorted(by_sku[sku], key=lambda x:x["coverage"])
        name=items[0].get("name") or SKU_NAME_CACHE.get(sku, f"SKU {sku}")
        lines.append(f"• <b>{html.escape(name)}</b> (SKU {sku})")
        for it in items[:6]:
            bar, sev = coverage_bar(it["coverage"])
            badge=need_pct_text(it["qty"], it["norm"], it["target"])
            lines.append(f"  {html.escape(it['warehouse_name'])}: +{it['need']} · {badge}")
            lines.append(f"  {bar} {sev}")
        lines.append(SEP_THIN)
    return "\n".join(lines)

@dp.callback_query(F.data.startswith("filter:"))
async def cb_filter(c:CallbackQuery):
    """
    Filter handler for Analysis view. Uses cache or triggers recomputation if missing.
    """
    ensure_admin(c.from_user.id)
    mode=c.data.split(":",1)[1]
    # Ensure cache exists, recompute if necessary
    cache = await _ensure_deficit_cache_for_chat(c.message.chat.id)
    flat = cache.get("flat") or []
    text=_build_filtered_deficit_text(flat, mode)
    kb=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Все",callback_data="filter:all"),
         InlineKeyboardButton(text="Критично",callback_data="filter:crit"),
         InlineKeyboardButton(text="50–80%",callback_data="filter:mid")],
        [InlineKeyboardButton(text=f"{EMOJI_REFRESH} Обновить",callback_data="action:reanalyze")],
        [InlineKeyboardButton(text="Автобронирование",callback_data="menu_autobook")],
    ])
    try:
        await c.message.edit_text(text, parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True)
    except Exception:
        await send_safe_message(c.message.chat.id, text, parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True)
    await c.answer("Фильтр применён")

@dp.callback_query(F.data=="action:reanalyze")
async def cb_reanalyze(c:CallbackQuery):
    ensure_admin(c.from_user.id)
    await c.answer("Пересчёт…")
    await handle_analyze(c.message.chat.id, verbose=False)

@dp.callback_query(F.data=="noop")
async def cb_noop(c:CallbackQuery):
    # Просто закрываем всплывающее
    await c.answer()

# ==== Scheduler and startup ====
scheduler = AsyncIOScheduler(timezone=ZoneInfo(TZ_NAME))

def setup_scheduler():
    try:
        # Периодический снимок
        scheduler.add_job(snapshot_job, "interval", minutes=SNAPSHOT_INTERVAL_MINUTES, id="snapshot_job", replace_existing=True)
    except Exception as e:
        log.warning("Scheduler: snapshot_job add failed: %s", e)
    try:
        # Ежедневное уведомление
        scheduler.add_job(daily_notify_job, "cron", hour=DAILY_NOTIFY_HOUR, minute=DAILY_NOTIFY_MINUTE, id="daily_notify", replace_existing=True)
    except Exception as e:
        log.warning("Scheduler: daily_notify add failed: %s", e)
    try:
        # Обслуживание истории
        scheduler.add_job(maintenance_job, "interval", minutes=HISTORY_PRUNE_EVERY_MINUTES, id="maintenance", replace_existing=True)
    except Exception as e:
        log.warning("Scheduler: maintenance add failed: %s", e)
    try:
        scheduler.start()
    except Exception as e:
        log.warning("Scheduler start failed: %s", e)

async def on_startup():
    load_state()
    load_cache()
    load_known_users()
    try:
        await init_snapshot()
    except Exception as e:
        log.warning("init_snapshot failed: %s", e)
    setup_scheduler()
    # Роутер внешнего автобронирования (если есть)
    if AUTOBOOK_ENABLED and autobook_router is not None:
        try:
            dp.include_router(autobook_router)
            log.info("External autobook router included.")
        except Exception as e:
            log.warning("include external router failed: %s", e)
    # Supply-watch background scheduler (если есть)
    try:
        if register_supply_scheduler:
            sig=inspect.signature(register_supply_scheduler)
            if len(sig.parameters)>=3:
                register_supply_scheduler(bot, dp, scheduler)
            elif len(sig.parameters)==2:
                register_supply_scheduler(bot, dp)
            elif len(sig.parameters)==1:
                register_supply_scheduler(dp)
            else:
                register_supply_scheduler()
            log.info("supply_watch scheduler registered.")
    except Exception as e:
        log.warning("register_supply_scheduler error: %s", e)

    log.info("Bot started. Version %s", VERSION)

def run():
    loop=asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(bot.session.close()))
        except Exception:
            pass
    loop.run_until_complete(on_startup())
    try:
        loop.run_until_complete(dp.start_polling(bot))
    except KeyboardInterrupt:
        pass
    finally:
        try:
            loop.run_until_complete(bot.session.close())
        except Exception:
            pass

if __name__ == "__main__":
    run()
