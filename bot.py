#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ozon FBO Telegram Bot + GigaChat + Auto-supplies

VERSION: stable-grounded-1.3.3-cluster-pretty

Что изменено относительно вашей базовой версии:
- Добавлены notify-обёртки для авто-заявок (текст и PDF).
- Единичная регистрация фоновой джобы авто-заявок через supply_watch.register_supply_scheduler (без дублей).
- Мягкая очистка возможных дубликатов джобы из setup_supply_handlers (если модуль её создаёт сам).
- Опечатки с EMOJI_* устранены (используются только латинские символы в идентификаторах).
- В остальном старый функционал сохранён.

Примечание:
Чтобы починить залипание задач на тайм-слоте (404/429), у вас должен быть обновлённый supply_watch.py
с трактовкой «только 404/429» как not_supported_404. Если вы используете мой файл supply_watch.py
из предыдущего ответа — просто оставьте его рядом.
"""
import os
os.environ["AUTO_BOOK"] = "0"  # гарантия: timeslot-патч не будет сам бронировать
import asyncio
import logging
import json
import math
import time
import re
import base64
import uuid
import hashlib
import signal
import tempfile
import subprocess
import shlex
from typing import Dict, List, Tuple, Optional, Any
from pathlib import Path
from zoneinfo import ZoneInfo
import html

import httpx
from dotenv import load_dotenv
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

# ======= AUTO-SUPPLY INTEGRATION =======
import supply_integration as si
from supply_watch import register_supply_scheduler
# (опц.) Если в будущем захотите управлять задачами из бота:
try:
    from supply_watch import purge_tasks, purge_all_tasks, purge_stale_nonfinal  # noqa: F401
except Exception:
    purge_tasks = purge_all_tasks = purge_stale_nonfinal = None
# ======================================

# ================== Version ==================
VERSION = "stable-grounded-1.3.3-cluster-pretty"

# ================== ENV ==================
load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
OZON_CLIENT_ID = os.getenv("OZON_CLIENT_ID", "").strip()
OZON_API_KEY = os.getenv("OZON_API_KEY", "").strip()

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
GIGACHAT_CLIENT_ID = os.getenv("GIGACHAT_CLIENT_ID", "").strip()
GIGACHAT_CLIENT_SECRET = os.getenv("GIGACHAT_CLIENT_SECRET", "").strip()
GIGACHAT_SCOPE = os.getenv("GIGACHAT_SCOPE", "GIGACHAT_API_B2B").strip()
GIGACHAT_TOKEN_URL = os.getenv("GIGACHAT_TOKEN_URL", "https://ngw.devices.sberbank.ru:9443/api/v2/oauth").strip()
GIGACHAT_API_URL = os.getenv("GIGACHAT_API_URL", "https://gigachat.devices.sberbank.ru/api/v1/chat/completions").strip()
GIGACHAT_MODEL = os.getenv("GIGACHAT_MODEL", "GigaChat").strip()
GIGACHAT_TEMPERATURE = float(os.getenv("GIGACHAT_TEMPERATURE", "0.3"))
GIGACHAT_MAX_TOKENS = int(os.getenv("GIGACHAT_MAX_TOKENS", "800"))
GIGACHAT_TIMEOUT_SECONDS = int(os.getenv("GIGACHAT_TIMEOUT_SECONDS", "40"))
GIGACHAT_VERIFY_SSL = os.getenv("GIGACHAT_VERIFY_SSL", "1") != "0"
GIGACHAT_SSL_MODE = os.getenv("GIGACHAT_SSL_MODE", "auto").lower().strip()
GIGACHAT_CA_CERT = os.getenv("GIGACHAT_CA_CERT", "/app/ca/gigachat_ca.pem").strip()
GIGACHAT_TOKEN_CACHE_ENV = os.getenv("GIGACHAT_TOKEN_CACHE", "keys/gigachat_token_cache.json")

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
DEFAULT_CHAT_MODE = os.getenv("DEFAULT_CHAT_MODE", "fact").lower().strip()

DIAG_TOP_DEFICITS = int(os.getenv("DIAG_TOP_DEFICITS", "8"))
DIAG_TOP_WAREHOUSES = int(os.getenv("DIAG_TOP_WAREHOUSES", "6"))
DIAG_TOP_CLUSTERS = int(os.getenv("DIAG_TOP_CLUSTERS", "6"))

WAREHOUSE_CLUSTERS_ENV = os.getenv("WAREHOUSE_CLUSTERS", "").strip()

STOCK_PAGE_SIZE = min(25, max(5, int(os.getenv("STOCK_PAGE_SIZE", "40"))))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# Интервал фоновой джобы supply_watch (секунды)
SUPPLY_JOB_INTERVAL = int(os.getenv("SUPPLY_JOB_INTERVAL", "45"))

# ================== Logging ==================
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s:%(name)s: %(message)s"
)
log = logging.getLogger("ozon-bot")

# ================== Paths ==================
DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
STATE_FILE = DATA_DIR / "bot_state.json"
CACHE_FILE = DATA_DIR / "sku_cache.json"
HISTORY_FILE = DATA_DIR / "stock_history.json"
KEYS_DIR = DATA_DIR / "keys"
KEYS_DIR.mkdir(exist_ok=True)
GIGACHAT_TOKEN_CACHE_FILE = (DATA_DIR / GIGACHAT_TOKEN_CACHE_ENV).resolve()

# ================== Global State ==================
if not TELEGRAM_BOT_TOKEN:
    raise SystemExit("Missing TELEGRAM_BOT_TOKEN")

MOCK_MODE = not (OZON_CLIENT_ID and OZON_API_KEY)
GIGACHAT_ENABLED = (LLM_PROVIDER == "gigachat")

bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

ADMIN_ID: Optional[int] = None
SKU_NAME_CACHE: Dict[int, str] = {}
BOT_STATE: Dict[str, Any] = {}
LAST_DEFICIT_CACHE: Dict[int, Dict[str, Any]] = {}
HISTORY_CACHE: List[dict] = []
LAST_SNAPSHOT_TS = 0

ANALYZE_LOCK = asyncio.Lock()
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

ENV_SKU = os.getenv("SKU_LIST", "")
if ENV_SKU:
    try:
        SKU_LIST = [int(s.strip()) for s in ENV_SKU.replace(";", ",").split(",") if s.strip()]
    except Exception:
        SKU_LIST = []
else:
    SKU_LIST = []

# ================== Cluster Configuration ==================
CLUSTER_MAP: Dict[str, str] = {}

CLUSTER_NAME_CATALOG = [
    "Санкт-Петербург и СЗО",
    "Казань",
    "Самара",
    "Уфа",
    "Юг",
    "Воронеж",
    "Саратов",
    "Кавказ",
    "Красноярск",
    "Сибирь",
    "Урал",
    "Тюмень",
    "Дальний Восток",
    "Калининград",
    "Ярославль",
    "Беларусь",
    "Казахстан",
    "Армения",
]

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
    # Try JSON
    try:
        obj = json.loads(raw)
        if isinstance(obj, dict):
            CLUSTER_MAP = {str(k): str(v) for k, v in obj.items()}
            return
    except Exception:
        pass
    mapping={}
    for part in raw.split(";"):
        p=part.strip()
        if not p:
            continue
        if "=" in p:
            k,v=p.split("=",1)
            mapping[str(k.strip())]=v.strip().strip('"').strip("'")
    CLUSTER_MAP = mapping

parse_cluster_env()

# ================== UI Const ==================
GR_FILL = {"red": "🟥", "orange": "🟧", "yellow": "🟨", "green": "🟩"}
EMPTY_SEG = "▫"
BAR_LEN = 12
SEP_THIN = "─" * 60
SEP_BOLD = "═" * 60

EMOJI_OK = "✅"
EMOJI_WARN = "⚠"
EMOJI_ANALYZE = "🔍"
EMOJI_NOTIFY = "📣"
EMOJI_BOX = "📦"
EMOJI_WH = "🏬"
EMOJI_CLUSTER = "🗺"
EMOJI_REFRESH = "🔄"
EMOJI_SETTINGS = "⚙"
EMOJI_TARGET = "🎯"
EMOJI_INFO = "ℹ"
EMOJI_DIAG = "🧪"
EMOJI_AI = "🤖"
EMOJI_PERF = "⏱"
EMOJI_INV = "📊"
EMOJI_CLOUD = "☁"
EMOJI_CHAT = "💬"

LEGEND_TEXT = "Легенда: 🟥 <25%  🟧 <50%  🟨 <80%  🟩 ≥80%"
AI_MAX_RENDER_LINES = 420

# ================== FSM ==================
class AIChatState(StatesGroup):
    waiting = State()

# ================== Start Overview ==================
def start_overview() -> str:
    rows = [
        ("🔍 Анализ", "Пересчёт дефицитов / индекса"),
        ("📣 Отчёт сейчас", "Быстрый анализ"),
        ("📦 Товары", "Список SKU"),
        ("🏬 Склады", "Статистика по складам"),
        ("🗺 Кластеры", "Агрегация по группам складов"),
        ("⚙ Режим отображения", "FULL / COMPACT"),
        ("🧪 Диагностика", "Статусы и топы"),
        ("🔄 Сброс кэша", "Очистка имён SKU"),
        ("🤖 AI чат", "Интерактив (FACT/GENERAL)"),
        ("❌ Отмена", "Выход из AI чата"),
    ]
    lines=[]
    header=f"{'═'*25}  {EMOJI_INFO} ОБЗОР БОТА  {'═'*25}"
    lines.append(header)
    lines.append(f"Версия: {VERSION} | Режим: {BOT_STATE.get('chat_mode','?').upper()} | Стиль: {'ON' if BOT_STATE.get('style_enabled') else 'OFF'}")
    lines.append(SEP_THIN)
    lines.append("Команды: /analyze /stock /warehouses /clusters /diag /facts_info /ai /ask /chat_mode /style_toggle /cluster_map")
    lines.append(SEP_THIN)
    lines.append("Кнопки:")
    ml=max(len(k) for k,_ in rows)
    for k,d in rows:
        lines.append(f" {k}{' '*(ml-len(k))} │ {d}")
    lines.append(SEP_THIN)
    lines.append("AI режимы: FACT (точно по данным) / GENERAL (свободно).")
    lines.append("Кластеры: эвристика или ENV WAREHOUSE_CLUSTERS.")
    lines.append("Подсказки: /fact /general /ask_raw /force_notify /ai_scope /help")
    lines.append("═"*len(header))
    return build_html(lines)

# ================== Utilities ==================
def ensure_admin(uid: int):
    global ADMIN_ID
    if ADMIN_ID is None:
        ADMIN_ID = uid

def build_html(lines: List[str]) -> str:
    return html.escape("\n".join(lines)).replace("§§B§§","<b>").replace("§§EB§§","</b>")

def _atomic_write(path: Path, text: str):
    fd,tmp=tempfile.mkstemp(dir=str(path.parent), prefix=path.name, suffix=".tmp")
    try:
        with os.fdopen(fd,"w",encoding="utf-8") as f:
            f.write(text); f.flush(); os.fsync(f.fileno())
        os.replace(tmp,path)
    except Exception:
        try: os.unlink(tmp)
        except Exception: pass
        raise

def load_state():
    global BOT_STATE
    if STATE_FILE.exists():
        try:
            BOT_STATE=json.loads(STATE_FILE.read_text("utf-8"))
        except Exception:
            BOT_STATE={}
    BOT_STATE.setdefault("view_mode", DEFAULT_VIEW_MODE)
    BOT_STATE.setdefault("style_enabled", LLM_STYLE_ENABLED)
    BOT_STATE.setdefault("chat_mode", DEFAULT_CHAT_MODE if DEFAULT_CHAT_MODE in ("fact","general") else "fact")

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

def save_cache_if_needed(prev: int):
    if len(SKU_NAME_CACHE)>prev:
        try: _atomic_write(CACHE_FILE, json.dumps(SKU_NAME_CACHE, ensure_ascii=False, indent=2))
        except Exception as e: log.warning("cache save error: %s", e)

def load_history():
    global HISTORY_CACHE, LAST_SNAPSHOT_TS
    if HISTORY_FILE.exists():
        try:
            arr=json.loads(HISTORY_FILE.read_text("utf-8"))
            if isinstance(arr,list): HISTORY_CACHE[:]=arr
        except Exception as e: log.warning("history load error: %s", e)
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
        except Exception as e: log.warning("history flush error: %s", e)

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

def append_snapshot(rows: List[Dict]):
    global LAST_SNAPSHOT_TS
    ts=int(time.time()); nr=[]
    for r in rows:
        try:
            sku=int(r.get("sku") or 0)
            if not sku: continue
            wid_raw=r.get("warehouse_id")
            wname=(r.get("warehouse_name") or (r.get("warehouse") or {}).get("name")
                   or (str(wid_raw) if wid_raw else "Склад"))
            qty=int(r.get("free_to_sell_amount") or 0)
            if qty<0: qty=0
            wkey=str(wid_raw) if wid_raw not in (None,"") else f"name:{wname}"
            nr.append({"sku":sku,"warehouse_key":wkey,"warehouse_name":wname,"qty":qty})
        except Exception:
            continue
    HISTORY_CACHE.append({"ts":ts,"rows":nr})
    LAST_SNAPSHOT_TS=ts
    mark_history_dirty()

# ================== Ozon API ==================
async def ozon_stock_fbo(skus: List[int]) -> Tuple[List[Dict], Optional[str]]:
    if not skus: return [], "SKU_LIST пуст – задайте переменную окружения."
    if MOCK_MODE:
        demo_wh = [
            (1,"Санкт-Петербург ФБО"),
            (2,"Казань"),
            (3,"Самара"),
            (4,"Уфа"),
            (5,"Ростов-на-Дону"),
            (6,"Воронеж"),
            (7,"Саратов"),
            (8,"Махачкала"),
            (9,"Красноярск"),
            (10,"Новосибирск"),
            (11,"Екатеринбург"),
            (12,"Тюмень"),
            (13,"Владивосток"),
            (14,"Калининград"),
            (15,"Ярославль"),
            (16,"Минск"),
            (17,"Алматы"),
            (18,"Ереван"),
        ]
        rows=[]
        for sku in skus:
            base = (sku % 50)+20
            for wid,name in demo_wh:
                rows.append({
                    "sku": sku,
                    "warehouse_id": wid,
                    "warehouse_name": name,
                    "free_to_sell_amount": max(0, base - (wid*2) + (sku % 7))
                })
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
        try:
            data=resp.json()
            msg=data.get("message") or data.get("error") or resp.text
        except Exception:
            msg=resp.text
        return [], f"Ozon API {resp.status_code}: {msg}"
    try:
        data=resp.json()
    except Exception:
        return [], "Non-JSON response"
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

async def ozon_product_names_by_sku(skus: List[int]) -> Tuple[Dict[int,str], Optional[str]]:
    if not skus: return {}, None
    if MOCK_MODE:
        return {s: f"Demo SKU {s}" for s in skus}, None
    url="https://api-seller.ozon.ru/v3/product/info/list"
    payload={"sku":skus,"product_id":[]}
    headers={"Client-Id":OZON_CLIENT_ID,"Api-Key":OZON_API_KEY,"Content-Type":"application/json"}
    try:
        async with httpx.AsyncClient(timeout=API_TIMEOUT_SECONDS) as client:
            resp=await client.post(url,json=payload,headers=headers)
    except Exception as e:
        return {}, f"HTTP error: {e}"
    if resp.status_code!=200:
        return {}, f"Ozon product info {resp.status_code}: {resp.text}"
    try:
        data=resp.json()
    except Exception:
        return {}, "Non-JSON product info"
    mapping={}
    result=data.get("result")
    cand=[]
    if isinstance(result,list):
        cand=result
    elif isinstance(result,dict):
        for k in ("items","products"):
            if isinstance(result.get(k),list):
                cand=result[k]; break
        if not cand:
            for v in result.values():
                if isinstance(v,dict):
                    cand.append(v)
    else:
        for v in data.values():
            if isinstance(v,list):
                cand=v; break
    for item in cand:
        if not isinstance(item,dict): continue
        sku_v=item.get("sku") or item.get("offer_id") or item.get("id")
        name_v=(item.get("name") or item.get("title") or item.get("display_name") or item.get("product_name"))
        try:
            sku_i=int(sku_v)
            mapping[sku_i]=name_v or f"SKU {sku_i}"
        except Exception:
            continue
    return mapping, None

# ================== Norms / Aggregation ==================
def build_consumption_cache()->Dict[Tuple[int,str], Dict[str,Any]]:
    now=int(time.time())
    cutoff=now-HISTORY_LOOKBACK_DAYS*86400
    series={}
    for snap in HISTORY_CACHE:
        ts=snap.get("ts",0)
        if ts<cutoff: continue
        for r in snap.get("rows",[]):
            sku=r.get("sku"); wkey=r.get("warehouse_key"); qty=r.get("qty")
            if sku is None or wkey is None: continue
            try:
                sku_i=int(sku); qty_i=int(qty)
            except Exception:
                continue
            series.setdefault((sku_i,wkey),[]).append((ts,qty_i))
    cache={}
    for key, arr in series.items():
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
        cache[key]={"norm":norm,"target":target,"history_used":False}
    return cache

def evaluate_position_cached(sku:int,wkey:str,qty:int,
                             ccache:Dict[Tuple[int,str],Dict[str,Any]])->Dict[str,Any]:
    meta=ccache.get((sku,wkey))
    if not meta:
        norm=MIN_STOCK; target=int(MIN_STOCK*TARGET_MULTIPLIER)
        return {"norm":norm,"target":target,"is_low":qty<norm,
                "need":max(0,target-qty) if qty<norm else 0,"history_used":False}
    norm=meta["norm"]; target=meta["target"]; is_low=qty<norm
    return {"norm":norm,"target":target,"is_low":is_low,
            "need":max(0,target-qty) if is_low else 0,"history_used":meta["history_used"]}

def aggregate_rows(rows: List[Dict])->Dict[int, Dict[str, Dict[str,Any]]]:
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

def coverage_bar(r: float)->Tuple[str,str]:
    if r<0: r=0
    if r<0.25: c=GR_FILL["red"]; sev="КРИТ"
    elif r<0.5: c=GR_FILL["orange"]; sev="КРИТ"
    elif r<0.8: c=GR_FILL["yellow"]; sev="НИЗКО"
    else: c=GR_FILL["green"]; sev="OK"
    filled=min(BAR_LEN,max(0,round(r*BAR_LEN)))
    bar=c*filled+EMPTY_SEG*(BAR_LEN-filled)
    return f"{bar} {int(r*100):02d}%", sev

def generate_deficit_report(rows: List[Dict],
                            name_map: Dict[int,str],
                            ccache: Dict[Tuple[int,str],Dict[str,Any]])->Tuple[str,List[dict]]:
    agg=aggregate_rows(rows)
    deficits={}
    flat=[]
    for sku,wmap in agg.items():
        for wkey, info in wmap.items():
            qty=info["qty"]
            st=evaluate_position_cached(sku,wkey,qty,ccache)
            if st["is_low"]:
                cov=qty/st["norm"] if st["norm"] else 0
                d={"sku":sku,"name":name_map.get(sku,f"SKU {sku}"),
                   "warehouse_key":wkey,"warehouse_name":info["warehouse_name"],
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
    lines=[f"{EMOJI_ANALYZE} §§B§§ДЕФИЦИТНЫЕ ПОЗИЦИИ§§EB§§", LEGEND_TEXT, SEP_BOLD]
    for sku in sku_order:
        items=deficits[sku]; items.sort(key=lambda x:x["coverage"])
        pname=items[0]["name"]
        worst=min(i["coverage"] for i in items)
        head=EMOJI_WARN if worst<0.5 else "➤"
        lines.append(f"{head} <b>{pname} (SKU {sku})</b>")
        total_qty=sum(i["qty"] for i in items)
        total_need=sum(i["need"] for i in items)
        for i in items:
            bar, sev=coverage_bar(i["coverage"])
            if i["coverage"]<0.5: crit+=1
            elif i["coverage"]<0.8: mid+=1
            else: hi+=1
            hist="hist" if i["history_used"] else "min"
            if full:
                lines.append(f"• {i['warehouse_name']}: {i['qty']} | norm {i['norm']} | target {i['target']} → +{i['need']}\n  {bar} {sev} [{hist}]")
            else:
                lines.append(f"• {i['warehouse_name']}: {i['qty']} → +{i['need']} {bar}")
        lines.append(f"  Σ qty={total_qty} need={total_need}")
        lines.append(SEP_THIN)
    lines.append(f"{EMOJI_TARGET} Итог: SKU={len(deficits)} строк={len(flat)} <50%={crit} 50–80%={mid} ≥80%<norm={hi} режим={view_mode}")
    return build_html(lines), flat

def filter_deficit(flat: List[dict], mode: str)->List[dict]:
    if mode=="crit": return [d for d in flat if d["coverage"]<0.5]
    if mode=="mid": return [d for d in flat if 0.5<=d["coverage"]<0.8]
    return flat

def rebuild_filtered_report(flat: List[dict], mode: str, view_mode: str)->str:
    if not flat: return build_html([f"{EMOJI_OK} Нет дефицитов."])
    f2=filter_deficit(flat, mode)
    if not f2:
        label={"crit":"критичных(<50%)","mid":"50–80%"}[mode]
        return build_html([f"{EMOJI_OK} Нет позиций категории {label}."])
    per={}
    for d in f2: per.setdefault(d["sku"],[]).append(d)
    sku_order=sorted(per.keys(), key=lambda s: min(x["coverage"] for x in per[s]))
    full=(view_mode=="FULL")
    lines=[f"{EMOJI_ANALYZE} §§B§§Фильтр: {mode}§§EB§§", SEP_BOLD]
    for sku in sku_order:
        items=per[sku]; items.sort(key=lambda x:x["coverage"])
        pname=items[0]["name"]
        lines.append(f"➤ <b>{pname} (SKU {sku})</b>")
        total_need=sum(i["need"] for i in items)
        total_qty=sum(i["qty"] for i in items)
        for i in items:
            bar, sev=coverage_bar(i["coverage"])
            if full:
                lines.append(f"• {i['warehouse_name']}: {i['qty']} | norm {i['norm']} | target {i['target']} → +{i['need']}\n  {bar} {sev}")
            else:
                lines.append(f"• {i['warehouse_name']}: {i['qty']} → +{i['need']} {bar}")
        lines.append(f"  Σ qty={total_qty} need={total_need}")
        lines.append(SEP_THIN)
    lines.append(f"{EMOJI_TARGET} Показано SKU={len(per)} строк={len(f2)} режим={view_mode}")
    return build_html(lines)

def deficit_filters_kb()->InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Все", callback_data="filter:all"),
            InlineKeyboardButton(text="Критично", callback_data="filter:crit"),
            InlineKeyboardButton(text="50–80%", callback_data="filter:mid"),
        ],
        [InlineKeyboardButton(text=f"{EMOJI_REFRESH} Обновить", callback_data="action:reanalyze")]
    ])

# ================== Cluster Helpers ==================
def resolve_cluster_for_warehouse(wkey: str, wname: str) -> str:
    if CLUSTER_MAP:
        raw_id = None if wkey.startswith("name:") else wkey
        if raw_id and raw_id in CLUSTER_MAP:
            return CLUSTER_MAP[raw_id]
        if wname in CLUSTER_MAP:
            return CLUSTER_MAP[wname]
        return "Прочие"
    lname = (wname or "").lower()
    for cname, patterns in CLUSTER_PATTERN_MAP.items():
        for pat in patterns:
            if pat.search(lname):
                return cname
    return "Прочие"

def aggregate_clusters_from_fact(sku_section: Dict[int, Any]) -> Dict[str, Any]:
    clusters={}
    for sku, data in sku_section.items():
        for w in data.get("warehouses", []):
            cname = resolve_cluster_for_warehouse(w["wkey"], w["name"])
            c = clusters.setdefault(cname, {
                "name": cname,
                "total_qty": 0,
                "total_need": 0,
                "deficit_need": 0,
                "sku_set": set(),
                "critical_sku": 0,
                "mid_sku": 0,
                "ok_sku": 0,
                "warehouses": set()
            })
            qty = w["qty"]
            gap_target = max(0, w["target"] - w["qty"])
            c["total_qty"] += qty
            c["total_need"] += gap_target
            c["deficit_need"] += w["need"]
            c["warehouses"].add(w["name"])
            if sku not in c["sku_set"]:
                c["sku_set"].add(sku)
            cov = w["coverage"]
            if cov < 0.5:
                c["critical_sku"] += 1
            elif cov < 0.8:
                c["mid_sku"] += 1
            else:
                c["ok_sku"] += 1
    out={}
    for cname, meta in clusters.items():
        out[cname] = {
            "name": cname,
            "total_qty": meta["total_qty"],
            "total_need": meta["total_need"],
            "deficit_need": meta["deficit_need"],
            "total_sku": len(meta["sku_set"]),
            "critical_sku": meta["critical_sku"],
            "mid_sku": meta["mid_sku"],
            "ok_sku": meta["ok_sku"],
            "warehouses": sorted(meta["warehouses"])
        }
    return out

def build_cluster_list_kb(cluster_section: Dict[str, Any]) -> InlineKeyboardMarkup:
    if not cluster_section:
        return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Нет кластеров", callback_data="noop")]])
    sorted_names = sorted(cluster_section.keys(), key=lambda c: cluster_section[c]["total_need"], reverse=True)
    buttons=[]
    for cname in sorted_names:
        info=cluster_section[cname]
        cap=f"{cname[:38]} (need={info['total_need']})"
        buttons.append([InlineKeyboardButton(text=cap, callback_data=f"cluster:{cname}")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# ======== Fancy cluster report helpers ========
def fancy_ratio_bar(parts: List[Tuple[int,str]], total: int, length: int = 24) -> str:
    if total <= 0:
        return EMPTY_SEG * length
    raw = []
    for count, color in parts:
        frac = count / total if total > 0 else 0.0
        raw_blocks = frac * length
        raw.append((color, raw_blocks))
    allocated = []
    acc = 0
    for color, raw_blocks in raw:
        blk = int(raw_blocks)
        allocated.append([color, blk, raw_blocks - blk])
        acc += blk
    remain = length - acc
    if remain > 0:
        allocated.sort(key=lambda x: x[2], reverse=True)
        for i in range(remain):
            allocated[i][1] += 1
    bar = "".join(color * blocks for color, blocks, _ in allocated)
    if len(bar) < length:
        bar += EMPTY_SEG * (length - len(bar))
    return bar

def small_cov_bar(cov: float, length: int = 12) -> str:
    cov = max(0.0, min(1.0, cov))
    if cov < 0.25: color = GR_FILL["red"]
    elif cov < 0.5: color = GR_FILL["orange"]
    elif cov < 0.8: color = GR_FILL["yellow"]
    else: color = GR_FILL["green"]
    filled = max(1, round(cov * length))
    return color * filled + EMPTY_SEG * (length - filled)

def format_pct_s(part: int, total: int) -> str:
    if total <= 0: return "0%"
    return f"{(part/total*100):.0f}%"

def build_cluster_detail(name: str, cluster_section: Dict[str, Any], sku_section: Dict[int, Any]) -> str:
    cl=cluster_section.get(name)
    if not cl:
        return build_html([f"{EMOJI_CLUSTER} Кластер не найден."])
    sku_rows=[]
    all_cluster_worst_coverages = []
    cov_cat = {"crit":0, "mid":0, "ok":0}
    for sku, skud in sku_section.items():
        need_cluster=0
        worst_cov_in_cluster=1.0
        in_cluster = False
        for w in skud.get("warehouses", []):
            if resolve_cluster_for_warehouse(w["wkey"], w["name"]) == name:
                in_cluster = True
                need_cluster += w["need"]
                worst_cov_in_cluster = min(worst_cov_in_cluster, w["coverage"])
        if in_cluster:
            all_cluster_worst_coverages.append(worst_cov_in_cluster)
            if worst_cov_in_cluster < 0.5:
                cov_cat["crit"] += 1
            elif worst_cov_in_cluster < 0.8:
                cov_cat["mid"] += 1
            else:
                cov_cat["ok"] += 1
        if need_cluster > 0:
            sku_rows.append((sku, skud["name"], need_cluster, worst_cov_in_cluster))
    sku_rows.sort(key=lambda x: (-x[2], x[3]))

    total_sku_present = len(all_cluster_worst_coverages)
    cluster_worst = min(all_cluster_worst_coverages) if all_cluster_worst_coverages else 0.0
    cluster_avg_worst = (sum(all_cluster_worst_coverages)/total_sku_present) if total_sku_present else 0.0

    if cluster_worst < 0.25:
        sev_badge = "🔥 КРИТИЧНО"
    elif cluster_worst < 0.5:
        sev_badge = "⚠ НИЖЕ НОРМЫ"
    elif cluster_worst < 0.8:
        sev_badge = "🟨 РИСК"
    else:
        sev_badge = "🟩 OK"

    dist_bar = fancy_ratio_bar([
        (cov_cat["crit"], "🟥"),
        (cov_cat["mid"], "🟨"),
        (cov_cat["ok"], "🟩"),
    ], total_sku_present, length=24)

    worst_bar = small_cov_bar(cluster_worst, 20)
    avg_bar = small_cov_bar(cluster_avg_worst, 20)

    lines=[]
    lines.append(f"🗺✨ §§B§§КЛАСТЕР: {name}§§EB§§")
    lines.append(SEP_THIN)
    lines.append(f"Статус: {sev_badge}")
    lines.append(f"Склады: {len(cl['warehouses'])} | SKU в кластере: {cl['total_sku']} | SKU (учтено по покрытиям): {total_sku_present}")
    lines.append(f"Qty: {cl['total_qty']}  Need: {cl['total_need']}  DeficitNeed: {cl['deficit_need']}")
    lines.append("")
    lines.append("Покрытие (worst по SKU в кластере):")
    lines.append(f"  Худшее : {worst_bar} {int(cluster_worst*100):02d}%")
    lines.append(f"  Среднее: {avg_bar} {int(cluster_avg_worst*100):02d}%")
    lines.append("")
    lines.append("Распределение SKU по худшему покрытию в кластере:")
    lines.append(f"  🟥 <50%: {cov_cat['crit']} ({format_pct_s(cov_cat['crit'], total_sku_present)})  "
                 f"🟨 50–80%: {cov_cat['mid']} ({format_pct_s(cov_cat['mid'], total_sku_present)})  "
                 f"🟩 ≥80%: {cov_cat['ok']} ({format_pct_s(cov_cat['ok'], total_sku_present)})")
    lines.append(f"  BAR: {dist_bar}")
    lines.append(SEP_THIN)
    lines.append(f"Склады ({len(cl['warehouses'])}): {', '.join(cl['warehouses']) if cl['warehouses'] else '-'}")
    lines.append(SEP_THIN)

    if sku_rows:
        lines.append("§§B§§TOP дефицитных SKU§§EB§§ (sorted by need desc, then coverage asc)")
        header = f"{'#':>2} {'SKU':>8} {'Need':>8} {'Cov':>5}  Bar"
        lines.append(header)
        lines.append("-"*len(header))
        for idx, (sku, nm, need, cov) in enumerate(sku_rows[:30], 1):
            bar = small_cov_bar(cov, 12)
            lines.append(f"{idx:>2} {sku:>8} {need:>8} {int(cov*100):>3d}%  {bar}  {nm[:45]}")
    else:
        lines.append("Нет дефицитных SKU в этом кластере.")
    lines.append(SEP_THIN)
    lines.append("Легенда:")
    lines.append("  Need – недобор до целевых значений (target).")
    lines.append("  DeficitNeed – та же потребность только в зоне < norm.")
    lines.append("  Худшее покрытие (worst) SKU = qty / norm для самого проблемного склада.")
    lines.append("  Распределение считается по худшему покрытию каждого SKU.")
    lines.append(f"  {LEGEND_TEXT}")
    lines.append(SEP_THIN)
    lines.append("Совет: увеличьте поставку SKU с красным/оранжевым покрытием и большим Need.")

    return build_html(lines)

# ================== FACT INDEX ==================
def build_fact_index(rows: List[dict], flat: List[dict], ccache: Dict[Tuple[int,str],Dict[str,Any]]):
    global FACT_INDEX
    agg=aggregate_rows(rows)
    sku_section={}
    wh_agg={}
    for sku,wmap in agg.items():
        name=SKU_NAME_CACHE.get(sku,f"SKU {sku}")
        entry={"name":name,"total_qty":0,"total_need":0,"deficit_need":0,"worst_coverage":1.0,"warehouses":[]}
        for wkey, info in wmap.items():
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
                "wkey":wkey,"name":info["warehouse_name"],"qty":qty,
                "norm":st["norm"],"target":st["target"],"need":need_def,
                "coverage":round(coverage,4),"history_used":st["history_used"]
            })
            wm=wh_agg.setdefault(wkey,{
                "name":info["warehouse_name"],"total_qty":0,"total_need":0,
                "deficit_need":0,"sku_set":set(),"critical_sku":0,"mid_sku":0,"ok_sku":0
            })
            wm["total_qty"]+=qty
            wm["total_need"]+=gap_target
            wm["deficit_need"]+=need_def
            if sku not in wm["sku_set"]:
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
    for k, meta in wh_agg.items():
        wh_section[k]={
            "name":meta["name"],
            "total_qty":meta["total_qty"],
            "total_need":meta["total_need"],
            "deficit_need":meta["deficit_need"],
            "total_sku":len(meta["sku_set"]),
            "critical_sku":meta["critical_sku"],
            "mid_sku":meta["mid_sku"],
            "ok_sku":meta["ok_sku"]
        }
    cluster_section=aggregate_clusters_from_fact(sku_section)
    top_clusters=sorted(
        ({"cluster": c, "name": v["name"], "total_need": v["total_need"], "deficit_need": v["deficit_need"]}
         for c,v in cluster_section.items()),
        key=lambda x:x["total_need"], reverse=True
    )[:LLM_TOP_CLUSTERS]
    top_warehouses=sorted(
        ({"wkey":k,"name":v["name"],"total_need":v["total_need"],"deficit_need":v["deficit_need"]}
         for k,v in wh_section.items()),
        key=lambda x:x["total_need"], reverse=True
    )[:LLM_TOP_WAREHOUSES]
    sample=[]
    for sku in sorted(sku_section.keys())[:LLM_INVENTORY_SAMPLE_SKU]:
        sample.append(f"{sku}:{sku_section[sku]['name'][:50]}")
    FACT_INDEX={
        "updated_ts":int(time.time()),
        "snapshot_ts":LAST_SNAPSHOT_TS,
        "sku":sku_section,
        "warehouse":wh_section,
        "cluster":cluster_section,
        "top_deficits":top_deficits,
        "top_warehouses":top_warehouses,
        "top_clusters":top_clusters,
        "inventory_overview":{"total_sku":len(sku_section),"sample_skus":sample}
    }

# ================== Classification / FACTS ==================
FULL_DUMP_PATTERNS=[
    "весь объем","весь объём","все данные","полный список","полный перечень",
    "full dump","все sku","весь список","весь ассортимент","доступные товары","все товары"
]
PRODUCT_LIST_PATTERNS=[
    "какие товары","список товаров","перечень товаров","ассортимент","какие у нас товары","что за товары"
]

def extract_skus_from_question(q: str)->List[int]:
    return [int(m.group()) for m in re.finditer(r"\b\d{3,}\b", q)]

def is_full_dump_question(q:str)->bool:
    ql=q.lower()
    return any(p in ql for p in FULL_DUMP_PATTERNS)

def is_list_products_question(q:str)->bool:
    ql=q.lower()
    return any(p in ql for p in PRODUCT_LIST_PATTERNS)

def build_facts_block(question: str)->Tuple[str,str]:
    if not FACT_INDEX:
        return "NO_DATA_INDEX","empty"
    q=question.strip()
    skus_in=extract_skus_from_question(q)
    sku_data=FACT_INDEX.get("sku",{})
    inv=FACT_INDEX.get("inventory_overview",{})
    mode="general"
    if is_full_dump_question(q):
        mode="full_dump"
        lines=[f"updated_ts={FACT_INDEX['updated_ts']} snapshot_ts={FACT_INDEX['snapshot_ts']} MODE=FULL_DUMP",
               f"TOTAL_SKU={inv.get('total_sku')}"]
        for sku, entry in list(sku_data.items())[:LLM_FULL_DETAIL_SKU]:
            lines.append(f"SKU {sku} '{entry['name']}' worst_cov={round(entry['worst_coverage'],3)} total_qty={entry['total_qty']} deficit_need={entry['deficit_need']}")
            for w in entry["warehouses"][:LLM_FULL_DETAIL_WAREHOUSES]:
                lines.append(f"  WH '{w['name']}' qty={w['qty']} norm={w['norm']} target={w['target']} need={w['need']} cov={w['coverage']} hist={1 if w['history_used'] else 0}")
        lines.append("TOP_DEFICITS:")
        for td in FACT_INDEX["top_deficits"]:
            lines.append(f"  SKU {td['sku']} '{td['name']}' cov={td['coverage']} deficit_need={td['deficit_need']}")
        lines.append("TOP_WAREHOUSES:")
        for w in FACT_INDEX["top_warehouses"]:
            lines.append(f"  WH {w['wkey']} '{w['name']}' total_need={w['total_need']} deficit_need={w['deficit_need']}")
        if FACT_INDEX.get("top_clusters"):
            lines.append("TOP_CLUSTERS:")
            for c in FACT_INDEX["top_clusters"]:
                lines.append(f"  CL {c['cluster']} '{c['name']}' total_need={c['total_need']} deficit_need={c['deficit_need']}")
        return _trim_facts("\n".join(lines)), mode
    if skus_in:
        mode="specific"
        lines=[f"updated_ts={FACT_INDEX['updated_ts']} snapshot_ts={FACT_INDEX['snapshot_ts']} MODE=SPECIFIC_SKU"]
        for sku in skus_in[:LLM_MAX_CONTEXT_SKU]:
            entry=sku_data.get(sku)
            if not entry:
                lines.append(f"SKU {sku}: NO_DATA"); continue
            lines.append(f"SKU {sku} '{entry['name']}' worst_cov={round(entry['worst_coverage'],3)} total_qty={entry['total_qty']} deficit_need={entry['deficit_need']}")
            for w in entry["warehouses"][:LLM_MAX_CONTEXT_WAREHOUSE]:
                lines.append(f"  WH '{w['name']}' qty={w['qty']} norm={w['norm']} target={w['target']} need={w['need']} cov={w['coverage']}")
        return _trim_facts("\n".join(lines)), mode
    if is_list_products_question(q):
        mode="list"
        lines=[f"updated_ts={FACT_INDEX['updated_ts']} snapshot_ts={FACT_INDEX['snapshot_ts']} MODE=LIST_PRODUCTS",
               f"TOTAL_SKU={inv.get('total_sku')}",
               "SAMPLE_SKUS:"]
        for s in inv.get("sample_skus",[])[:LLM_INVENTORY_SAMPLE_SKU]:
            lines.append(f"  {s}")
        lines.append("TOP_DEFICITS:")
        for td in FACT_INDEX["top_deficits"][:LLM_MAX_CONTEXT_SKU]:
            lines.append(f"  SKU {td['sku']} '{td['name']}' cov={td['coverage']} deficit_need={td['deficit_need']}")
        if FACT_INDEX.get("top_clusters"):
            lines.append("TOP_CLUSTERS:")
            for c in FACT_INDEX["top_clusters"][:LLM_TOP_CLUSTERS]:
                lines.append(f"  CL {c['cluster']} '{c['name']}' total_need={c['total_need']}")
        return _trim_facts("\n".join(lines)), mode
    mode="general"
    lines=[f"updated_ts={FACT_INDEX['updated_ts']} snapshot_ts={FACT_INDEX['snapshot_ts']} MODE=GENERAL",
           f"TOTAL_SKU={inv.get('total_sku')}",
           "TOP_DEFICITS:"]
    for td in FACT_INDEX["top_deficits"][:LLM_MAX_CONTEXT_SKU]:
        lines.append(f"  SKU {td['sku']} '{td['name']}' cov={td['coverage']} deficit_need={td['deficit_need']}")
    lines.append("TOP_WAREHOUSES:")
    for w in FACT_INDEX["top_warehouses"][:LLM_MAX_CONTEXT_WAREHOUSE]:
        lines.append(f"  WH {w['wkey']} '{w['name']}' total_need={w['total_need']} deficit_need={w['deficit_need']}")
    if FACT_INDEX.get("top_clusters"):
        lines.append("TOP_CLUSTERS:")
        for c in FACT_INDEX["top_clusters"][:LLM_TOP_CLUSTERS]:
            lines.append(f"  CL {c['cluster']} '{c['name']}' total_need={c['total_need']} deficit_need={c['deficit_need']}")
    return _trim_facts("\n".join(lines)), mode

def _trim_facts(text: str)->str:
    if len(text)<=LLM_FACT_SOFT_LIMIT_CHARS: return text
    lines=text.splitlines()
    keep=[]; n=0
    for ln in lines:
        if n+len(ln)+1>int(LLM_FACT_SOFT_LIMIT_CHARS*0.95):
            keep.append("... (усечено)")
            break
        keep.append(ln); n+=len(ln)+1
    return "\n".join(keep)

def cache_answer(question: str, mode: str, answer: str):
    if not LLM_ENABLE_ANSWER_CACHE or not FACT_INDEX: return
    key=f"{mode}|{FACT_INDEX.get('snapshot_ts')}|{question.strip().lower()}"
    ANSWER_CACHE[hashlib.sha1(key.encode()).hexdigest()]=answer

def get_cached_answer(question: str, mode: str)->Optional[str]:
    if not LLM_ENABLE_ANSWER_CACHE or not FACT_INDEX: return None
    key=f"{mode}|{FACT_INDEX.get('snapshot_ts')}|{question.strip().lower()}"
    return ANSWER_CACHE.get(hashlib.sha1(key.encode()).hexdigest())

def build_messages_fact(question: str)->Tuple[List[Dict[str,str]], str]:
    facts, mode=build_facts_block(question)
    if facts=="NO_DATA_INDEX":
        return [
            {"role":"system","content":"Нет индекса FACTS. Выполните /analyze."},
            {"role":"user","content":question}
        ], mode
    system=("Ты аналитик остатков. Используй ТОЛЬКО данные из FACTS; не выдумывай. "
            "Если данных нет — 'Нет данных в FACTS'. Форматируй списками + итог.")
    return [
        {"role":"system","content":system},
        {"role":"user","content":f"Вопрос:\n{question}\n\nFACTS:\n{facts}"}
    ], mode

# ================== AI Styling / GigaChat ==================
HIGHLIGHT_PATTERNS=[
    (r"\b(\d{1,3}(?:[\s.,]\d{3})+|\d+)\b","num"),
    (r"\b(дефицит\w*)\b","kw"),
    (r"\b(склад\w*)\b","kw"),
    (r"\b(класт\w*)\b","kw"),
    (r"\b(норм[аиы]?)\b","kw"),
    (r"\b(цель|target)\b","kw"),
    (r"\b(покрыти[ея]|coverage)\b","kw"),
    (r"\b(SKU\s*\d+)\b","sku"),
    (r"\b\d{1,3}%\b","pct")
]

def style_ai_answer(question:str, raw:str, mode:str, fact_mode:bool)->str:
    if not BOT_STATE.get("style_enabled", True):
        return build_html([raw])
    lines=[l.rstrip() for l in raw.splitlines()]
    compact=[]; blank=False
    for l in lines:
        if not l.strip():
            if not blank: compact.append("")
            blank=True
        else:
            compact.append(l); blank=False
    def bulletize(s:str)->str:
        st=s.lstrip("-*•— ")
        if re.match(r"^\d+[\).]\s", st): return "№ "+st
        return "• "+st
    processed=[]
    for l in compact:
        lt=l.strip()
        if not lt:
            processed.append(""); continue
        if re.match(r"^[-*•—]\s", lt) or re.match(r"^\d+[\).]\s", lt):
            processed.append(bulletize(lt))
        else:
            processed.append(lt)
    if fact_mode and not any("итог" in x.lower() for x in processed[-6:]):
        tail=[x for x in processed[-10:] if re.search(r"\d", x)]
        if tail:
            processed.append("")
            processed.append("ИТОГ: " + "; ".join(tail[-3:]))
    styled="\n".join(processed)
    styled=html.escape(styled)
    def repl(m): return f"<b>{html.escape(m.group(1))}</b>"
    for pat,_ in HIGHLIGHT_PATTERNS:
        styled=re.sub(pat,repl,styled,flags=re.IGNORECASE)
    header=f"{EMOJI_AI} <b>Ответ</b> | режим={'FACT' if fact_mode else 'GENERAL'}:{mode} | snap={FACT_INDEX.get('snapshot_ts','-')} | {time.strftime('%H:%M:%S')}"
    qline=f"<b>Вопрос:</b> {html.escape(question)}"
    return f"{header}\n{SEP_THIN}\n{qline}\n{SEP_THIN}\n{styled}"

def _gigachat_verify_param():
    if GIGACHAT_SSL_MODE=="insecure": return False
    if GIGACHAT_SSL_MODE=="custom":
        if not os.path.isfile(GIGACHAT_CA_CERT):
            log.warning("CA не найден (%s)", GIGACHAT_CA_CERT)
            return GIGACHAT_VERIFY_SSL
        return GIGACHAT_CA_CERT
    return GIGACHAT_VERIFY_SSL

def _read_token_cache_file():
    if not GIGACHAT_TOKEN_CACHE_FILE.exists(): return None
    try: return json.loads(GIGACHAT_TOKEN_CACHE_FILE.read_text("utf-8"))
    except Exception: return None

def _write_token_cache_file(data: dict):
    try: _atomic_write(GIGACHAT_TOKEN_CACHE_FILE, json.dumps(data, ensure_ascii=False, indent=2))
    except Exception as e: log.warning("token cache write error: %s", e)

def _token_valid(tok: dict)->bool:
    if not tok: return False
    exp=tok.get("expires_epoch")
    if not exp and tok.get("obtained_at") and tok.get("expires_in"):
        exp=tok["obtained_at"]+int(tok["expires_in"])
    if not exp: return False
    return (exp-time.time())>120

async def get_gigachat_token(force=False)->str:
    if not GIGACHAT_ENABLED: raise RuntimeError("LLM_PROVIDER != gigachat")
    if not (GIGACHAT_CLIENT_ID and GIGACHAT_CLIENT_SECRET):
        raise RuntimeError("Нет GIGACHAT_CLIENT_ID / SECRET")
    global _GIGACHAT_TOKEN_MEM
    if not force and _token_valid(_GIGACHAT_TOKEN_MEM):
        return _GIGACHAT_TOKEN_MEM["access_token"]
    if not force:
        file_obj=_read_token_cache_file()
        if _token_valid(file_obj):
            _GIGACHAT_TOKEN_MEM=file_obj
            return file_obj["access_token"]
    basic=base64.b64encode(f"{GIGACHAT_CLIENT_ID}:{GIGACHAT_CLIENT_SECRET}".encode()).decode()
    headers={"Authorization":f"Basic {basic}","RqUID":str(uuid.uuid4()),
             "Content-Type":"application/x-www-form-urlencoded","Accept":"application/json"}
    data={"scope":GIGACHAT_SCOPE}
    try:
        async with httpx.AsyncClient(verify=_gigachat_verify_param(), timeout=20) as client:
            resp=await client.post(GIGACHAT_TOKEN_URL,data=data,headers=headers)
            if resp.status_code>=400: raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:300]}")
            js=resp.json()
    except Exception as e:
        raise RuntimeError(f"Ошибка получения токена: {e}") from e
    obtained=int(time.time())
    exp_epoch=None
    if isinstance(js.get("expires_at"),(int,float)): exp_epoch=int(js["expires_at"])
    if not exp_epoch and js.get("expires_in"):
        try: exp_epoch=obtained+int(js["expires_in"])
        except Exception: pass
    if not exp_epoch: exp_epoch=obtained+1800
    token_obj={"access_token":js.get("access_token"),
               "obtained_at":obtained,"expires_in":js.get("expires_in"),
               "expires_epoch":exp_epoch}
    if not token_obj["access_token"]:
        raise RuntimeError(f"Ответ без access_token: {js}")
    _GIGACHAT_TOKEN_MEM=token_obj
    _write_token_cache_file(token_obj)
    return token_obj["access_token"]

async def llm_fact_answer(question: str)->Tuple[str,str]:
    if not GIGACHAT_ENABLED: return "LLM отключён.","off"
    if not (GIGACHAT_CLIENT_ID and GIGACHAT_CLIENT_SECRET): return "Не заданы креды GigaChat.","off"
    q=question.strip()
    if not q: return "Пустой запрос.","empty"
    global _LAST_AI_CALL
    now=time.time()
    if AI_MIN_INTERVAL_SECONDS>0 and (now-_LAST_AI_CALL)<AI_MIN_INTERVAL_SECONDS:
        return f"Слишком часто. Подождите {AI_MIN_INTERVAL_SECONDS-int(now-_LAST_AI_CALL)} сек.","rate"
    messages, mode=build_messages_fact(q)
    cached=get_cached_answer(q,mode)
    if cached: return "(cached)\n"+cached, mode
    _LAST_AI_CALL=now
    try:
        token=await get_gigachat_token()
    except Exception as e:
        return str(e),"token"
    payload={"model":GIGACHAT_MODEL,"messages":messages,"temperature":min(0.2,GIGACHAT_TEMPERATURE),"max_tokens":GIGACHAT_MAX_TOKENS}
    try:
        async with httpx.AsyncClient(verify=_gigachat_verify_param(), timeout=GIGACHAT_TIMEOUT_SECONDS) as client:
            r=await client.post(GIGACHAT_API_URL,json=payload,headers={"Authorization":f"Bearer {token}","Content-Type":"application/json"})
            if r.status_code==401:
                token=await get_gigachat_token(force=True)
                r=await client.post(GIGACHAT_API_URL,json=payload,headers={"Authorization":f"Bearer {token}","Content-Type":"application/json"})
            if r.status_code>=400:
                return f"GigaChat HTTP {r.status_code}: {r.text[:300]}", "http"
            data=r.json()
    except Exception as e:
        return f"Ошибка запроса к GigaChat: {e}", "net"
    ch=data.get("choices")
    if not ch: return f"Пустой ответ: {data}","empty"
    text=(ch[0].get("message",{}).get("content") or "").strip()
    if not text: return f"Пустой контент: {data}","empty"
    cache_answer(q,mode,text)
    return text, mode

GENERAL_WORK_KEYWORDS=[
    "sku","склад","склады","дефицит","норм","target","покрыт","остат","товар","ozon","озон","кластер"
]

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
    sys=("Ты дружелюбный помощник. Общайся свободно. Если запрос про склады/остатки/кластеры/SKU — порекомендуй /ai.")
    msgs=[{"role":"system","content":sys}]
    for msg in history[-(GENERAL_HISTORY_MAX-1):]:
        msgs.append(msg)
    msgs.append({"role":"user","content":question})
    if looks_like_work_question(question):
        msgs.append({"role":"system","content":"Рабочий вопрос — упомяни /ai."})
    return msgs

async def llm_general_answer(chat_id:int, question:str)->Tuple[str,str]:
    if not GIGACHAT_ENABLED: return "LLM отключён.","off"
    if not (GIGACHAT_CLIENT_ID and GIGACHAT_CLIENT_SECRET): return "Не заданы креды GigaChat.","off"
    q=question.strip()
    if not q: return "Пустой запрос.","empty"
    global _LAST_AI_CALL
    now=time.time()
    if AI_MIN_INTERVAL_SECONDS>0 and (now-_LAST_AI_CALL)<AI_MIN_INTERVAL_SECONDS:
        return f"Слишком часто. Подождите {AI_MIN_INTERVAL_SECONDS-int(now-_LAST_AI_CALL)} сек.","rate"
    _LAST_AI_CALL=now
    messages=build_general_messages(chat_id,q)
    try:
        token=await get_gigachat_token()
    except Exception as e:
        return str(e),"token"
    payload={"model":GIGACHAT_MODEL,"messages":messages,"temperature":LLM_GENERAL_TEMPERATURE,"max_tokens":GIGACHAT_MAX_TOKENS}
    try:
        async with httpx.AsyncClient(verify=_gigachat_verify_param(), timeout=GIGACHAT_TIMEOUT_SECONDS) as client:
            r=await client.post(GIGACHAT_API_URL,json=payload,headers={"Authorization":f"Bearer {token}","Content-Type":"application/json"})
            if r.status_code==401:
                token=await get_gigachat_token(force=True)
                r=await client.post(GIGACHAT_API_URL,json=payload,headers={"Authorization":f"Bearer {token}","Content-Type":"application/json"})
            if r.status_code>=400:
                return f"GigaChat HTTP {r.status_code}: {r.text[:300]}", "http"
            data=r.json()
    except Exception as e:
        return f"Ошибка запроса к GigaChat: {e}","net"
    ch=data.get("choices")
    if not ch: return f"Пустой ответ: {data}","empty"
    text=(ch[0].get("message",{}).get("content") or "").strip()
    if not text: return f"Пустой контент: {data}","empty"
    add_general_history(chat_id,"user",q)
    add_general_history(chat_id,"assistant",text)
    return text,"general"

async def send_ai_answer(chat_id:int, question:str, raw:str, mode:str, fact_mode:bool):
    styled=style_ai_answer(question, raw, mode, fact_mode)
    if styled.count("\n")>AI_MAX_RENDER_LINES:
        parts=styled.splitlines()
        styled="\n".join(parts[:AI_MAX_RENDER_LINES])+"\n...(усечено)"
    await send_long(chat_id, styled)

# ================== Generic Messaging ==================
async def send_safe_message(chat_id:int, text:str, **kwargs):
    try:
        return await bot.send_message(chat_id, text, **kwargs)
    except TelegramRetryAfter as e:
        await asyncio.sleep(e.retry_after)
        return await bot.send_message(chat_id, text, **kwargs)
    except Exception as e:
        log.warning("send fail: %s", e)

async def send_long(chat_id:int, text:str, kb:Optional[InlineKeyboardMarkup]=None):
    max_len=3900
    parts=[]; buf=[]; ln=0
    for line in text.split("\n"):
        L=len(line)+1
        if buf and ln+L>max_len:
            parts.append("\n".join(buf)); buf=[line]; ln=L
        else:
            buf.append(line); ln+=L
    if buf: parts.append("\n".join(buf))
    for i,chunk in enumerate(parts):
        await send_safe_message(chat_id,
                                chunk.rstrip() or "\u200B",
                                parse_mode="HTML",
                                disable_web_page_preview=True,
                                reply_markup=kb if (kb and i==len(parts)-1) else None)
        await asyncio.sleep(0.02)

# ======= Supply-watch notify wrappers (для PDF/сообщений) =======
async def supply_notify_text(chat_id: int, text: str):
    await send_safe_message(chat_id, text, parse_mode="HTML", disable_web_page_preview=True)

async def supply_notify_file(chat_id: int, file_path: str, caption: str = ""):
    try:
        doc = FSInputFile(file_path)
        await bot.send_document(chat_id, document=doc, caption=caption)
    except Exception as e:
        log.warning("send_document fail: %s", e)
        await send_safe_message(chat_id, f"Не удалось отправить файл: {html.escape(str(e))}\n{caption}")

# ================== Analyze ==================
async def handle_analyze(chat_id:int, verbose:bool=True):
    global LAST_ANALYZE_MS, LAST_ANALYZE_ERROR
    async with ANALYZE_LOCK:
        start=time.time(); LAST_ANALYZE_ERROR=None
        temp_msg=None
        try:
            if verbose:
                temp_msg=await send_safe_message(chat_id,"⚙ Анализ...")
            need_snapshot=(time.time()-LAST_SNAPSHOT_TS>SNAPSHOT_STALE_MINUTES*60)
            rows, err=await ozon_stock_fbo(SKU_LIST)
            if err:
                LAST_ANALYZE_ERROR=err
                await send_safe_message(chat_id,f"Ошибка Ozon API: {html.escape(err)}")
                if temp_msg: await temp_msg.delete()
                return
            if need_snapshot and time.time()-LAST_SNAPSHOT_TS>SNAPSHOT_MIN_REUSE_SECONDS:
                append_snapshot(rows)
                await flush_history_if_needed(force=True)
            missing=[s for s in SKU_LIST if s not in SKU_NAME_CACHE]
            if missing:
                prev=len(SKU_NAME_CACHE)
                mp,_=await ozon_product_names_by_sku(missing)
                SKU_NAME_CACHE.update(mp); save_cache_if_needed(prev)
            ccache=build_consumption_cache()
            report, flat=generate_deficit_report(rows, SKU_NAME_CACHE, ccache)
            LAST_DEFICIT_CACHE[chat_id]={"flat":flat,"timestamp":int(time.time()),
                                         "report":report,"raw_rows":rows,"consumption_cache":ccache}
            try:
                build_fact_index(rows, flat, ccache)
            except Exception as e:
                log.warning("FACT_INDEX build error: %s", e)
            kb=deficit_filters_kb()
            if flat:
                await send_long(chat_id, report, kb=kb)
            else:
                await send_safe_message(chat_id, report)
            if temp_msg:
                try: await temp_msg.delete()
                except Exception: pass
        except Exception as e:
            LAST_ANALYZE_ERROR=str(e)
            log.exception("Analyze error")
            await send_safe_message(chat_id,f"❌ Ошибка анализа: {html.escape(str(e))}")
        finally:
            LAST_ANALYZE_MS=(time.time()-start)*1000
            await flush_history_if_needed()

# ================== Pagination (SKU list) ==================
def build_stock_page(page:int)->InlineKeyboardMarkup:
    total=len(SKU_LIST)
    if total==0:
        return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Нет SKU", callback_data="noop")]])
    pages=(total+STOCK_PAGE_SIZE-1)//STOCK_PAGE_SIZE
    page=max(0,min(page,pages-1))
    start=page*STOCK_PAGE_SIZE
    end=min(start+STOCK_PAGE_SIZE,total)
    buttons=[]
    for sku in SKU_LIST[start:end]:
        nm=SKU_NAME_CACHE.get(sku,f"SKU {sku}")
        buttons.append([InlineKeyboardButton(text=nm[:60], callback_data=f"sku:{sku}")])
    nav=[]
    if page>0: nav.append(InlineKeyboardButton(text="«", callback_data=f"stockpage:{page-1}"))
    nav.append(InlineKeyboardButton(text=f"{page+1}/{pages}", callback_data="noop"))
    if page<pages-1: nav.append(InlineKeyboardButton(text="»", callback_data=f"stockpage:{page+1}"))
    buttons.append(nav)
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# ================== Callback Handlers ==================
@dp.callback_query(F.data.startswith("stockpage:"))
async def cb_stock_page(c: CallbackQuery):
    ensure_admin(c.from_user.id)
    try: page=int(c.data.split(":")[1])
    except Exception:
        await c.answer(); return
    missing=[s for s in SKU_LIST if s not in SKU_NAME_CACHE]
    if missing:
        prev=len(SKU_NAME_CACHE)
        mp,_=await ozon_product_names_by_sku(missing)
        SKU_NAME_CACHE.update(mp); save_cache_if_needed(prev)
    kb=build_stock_page(page)
    try:
        await c.message.edit_reply_markup(reply_markup=kb)
    except Exception:
        await c.message.answer("Страница товаров:", reply_markup=kb)
    await c.answer()

@dp.callback_query(F.data=="noop")
async def cb_noop(c: CallbackQuery):
    await c.answer()

@dp.callback_query(F.data=="action:reanalyze")
async def cb_reanalyze(c: CallbackQuery):
    ensure_admin(c.from_user.id)
    await handle_analyze(c.message.chat.id, verbose=False)
    await c.answer()

@dp.callback_query(F.data.startswith("filter:"))
async def cb_filter(c: CallbackQuery):
    ensure_admin(c.from_user.id)
    mode=c.data.split(":")[1]
    cache=LAST_DEFICIT_CACHE.get(c.message.chat.id)
    if not cache:
        await c.answer("Нет кэша — анализ...")
        await handle_analyze(c.message.chat.id, verbose=False)
        return
    flat=cache["flat"]
    rep=rebuild_filtered_report(flat, mode, BOT_STATE.get("view_mode", DEFAULT_VIEW_MODE))
    kb=deficit_filters_kb()
    await send_long(c.message.chat.id, rep, kb=kb)
    await c.answer()

@dp.callback_query(F.data.startswith("sku:"))
async def cb_sku(c: CallbackQuery):
    ensure_admin(c.from_user.id)
    try: sku=int(c.data.split(":")[1])
    except Exception:
        await c.answer(); return
    rows, err=await ozon_stock_fbo(SKU_LIST)
    if err:
        await c.message.answer(f"Ошибка Ozon API: {html.escape(err)}")
        await c.answer(); return
    ccache=build_consumption_cache()
    agg=aggregate_rows(rows)
    if sku not in agg:
        await c.message.answer("Нет данных по SKU.")
        await c.answer(); return
    if sku not in SKU_NAME_CACHE:
        prev=len(SKU_NAME_CACHE)
        mp,_=await ozon_product_names_by_sku([sku])
        SKU_NAME_CACHE.update(mp); save_cache_if_needed(prev)
    name=SKU_NAME_CACHE.get(sku,f"SKU {sku}")
    lines=[f"{EMOJI_BOX} <b>{html.escape(name)} (SKU {sku})</b>", SEP_THIN]
    for wkey, info in sorted(agg[sku].items()):
        qty=info["qty"]
        st=evaluate_position_cached(sku,wkey,qty,ccache)
        cov=qty/st["norm"] if st["norm"] else 0
        bar, sev=coverage_bar(cov)
        status=EMOJI_WARN if st["is_low"] else EMOJI_OK
        hist="hist" if st["history_used"] else "min"
        lines.append(f"• {html.escape(info['warehouse_name'])}: {qty} | norm {st['norm']} | target {st['target']} {status}\n  {bar} {sev} [{hist}]")
    await send_long(c.message.chat.id, "\n".join(lines))
    await c.answer()

@dp.callback_query(F.data.startswith("wh:"))
async def cb_wh(c: CallbackQuery):
    ensure_admin(c.from_user.id)
    wkey=c.data.split(":",1)[1]
    rows, err=await ozon_stock_fbo(SKU_LIST)
    if err:
        await c.message.answer(f"Ошибка Ozon API: {html.escape(err)}")
        await c.answer(); return
    ccache=build_consumption_cache()
    agg=aggregate_rows(rows)
    lines=[f"{EMOJI_WH} <b>Склад {html.escape(wkey)}</b>", SEP_THIN]
    present=False
    for sku in sorted(agg.keys()):
        if wkey in agg[sku]:
            present=True
            info=agg[sku][wkey]
            if sku not in SKU_NAME_CACHE:
                prev=len(SKU_NAME_CACHE)
                mp,_=await ozon_product_names_by_sku([sku])
                SKU_NAME_CACHE.update(mp); save_cache_if_needed(prev)
            name=SKU_NAME_CACHE.get(sku,f"SKU {sku}")
            qty=info["qty"]
            st=evaluate_position_cached(sku,wkey,qty,ccache)
            cov=qty/st["norm"] if st["norm"] else 0
            bar, sev=coverage_bar(cov)
            status=EMOJI_WARN if st["is_low"] else EMOJI_OK
            lines.append(f"{html.escape(name)} (SKU {sku}): {qty} | norm {st['norm']} | target {st['target']} {status}\n  {bar} {sev}")
    if not present:
        lines.append("Нет данных по складу.")
    await send_long(c.message.chat.id, "\n".join(lines))
    await c.answer()

@dp.callback_query(F.data.startswith("cluster:"))
async def cb_cluster(c: CallbackQuery):
    ensure_admin(c.from_user.id)
    cname=c.data.split(":",1)[1]
    cl_sec=FACT_INDEX.get("cluster",{})
    sku_sec=FACT_INDEX.get("sku",{})
    if not cl_sec:
        await c.message.answer("Кластерный индекс пуст. Выполните /analyze.")
        await c.answer(); return
    rep=build_cluster_detail(cname, cl_sec, sku_sec)
    await send_long(c.message.chat.id, rep)
    await c.answer()

@dp.callback_query(F.data=="chatmode:toggle")
async def cb_chatmode_toggle(c: CallbackQuery):
    ensure_admin(c.from_user.id)
    cur=BOT_STATE.get("chat_mode","fact")
    new="general" if cur=="fact" else "fact"
    BOT_STATE["chat_mode"]=new
    save_state()
    await c.message.edit_text(build_html([
        "Режим чата переключён.",
        f"Теперь: <b>{new.upper()}</b>.",
        "Нажми ещё раз для смены."
    ]), reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔁 Переключить режим", callback_data="chatmode:toggle")]
    ]))
    await c.answer()

# ================== Menus ==================
def main_menu_kb()->ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🔍 Анализ"), KeyboardButton(text="📣 Отчёт сейчас")],
            [KeyboardButton(text="📦 Товары"), KeyboardButton(text="🏬 Склады")],
            [KeyboardButton(text="🗺 Кластеры"), KeyboardButton(text="⚙ Режим отображения")],
            [KeyboardButton(text="🧪 Диагностика"), KeyboardButton(text="🔄 Сброс кэша")],
            [KeyboardButton(text="🤖 AI чат"), KeyboardButton(text="❌ Отмена")],
        ],
        resize_keyboard=True
    )

def version_info()->str:
    import sys
    return (f"Версия: {VERSION}\nPython: {sys.version.split()[0]}\n"
            f"Snapshots: {len(HISTORY_CACHE)}\n"
            f"Index SKU: {len(FACT_INDEX.get('sku', {}))}\n"
            f"Clusters: {len(FACT_INDEX.get('cluster', {}))}\n"
            f"ChatMode: {BOT_STATE.get('chat_mode')} Style:{BOT_STATE.get('style_enabled')}")

# ================== Diagnostics ==================
def build_diag_report()->str:
    inv=FACT_INDEX.get("inventory_overview",{})
    sku_section=FACT_INDEX.get("sku",{})
    top_def=FACT_INDEX.get("top_deficits",[])
    top_wh=FACT_INDEX.get("top_warehouses",[])
    top_cl=FACT_INDEX.get("top_clusters",[])
    cov={"<25":0,"25-50":0,"50-80":0,"80-100":0,"100+":0}
    for _, info in sku_section.items():
        cv=info["worst_coverage"]
        if cv<0.25: cov["<25"]+=1
        elif cv<0.5: cov["25-50"]+=1
        elif cv<0.8: cov["50-80"]+=1
        elif cv<1: cov["80-100"]+=1
        else: cov["100+"]+=1
    lines=[]
    s=lambda t:f"§§B§§{t}§§EB§§"
    lines+=[
        f"{EMOJI_DIAG} {s('ДИАГНОСТИКА')} ({time.strftime('%H:%M:%S')})",
        SEP_BOLD,
        f"{EMOJI_INFO} Версия {VERSION} | ChatMode={BOT_STATE.get('chat_mode')} | Style={'ON' if BOT_STATE.get('style_enabled') else 'OFF'}",
        f"{EMOJI_CLOUD} Snapshot {FACT_INDEX.get('snapshot_ts','-')} | SKU_index={inv.get('total_sku','-')} | Clusters={len(FACT_INDEX.get('cluster',{}))}",
        ""
    ]
    lines+=[
        f"{EMOJI_INV} {s('Инвентарь')}",
        SEP_BOLD,
        f"Покрытие (worst): <25={cov['<25']} 25–50={cov['25-50']} 50–80={cov['50-80']} 80–100={cov['80-100']} ≥100={cov['100+']}",
        ""
    ]
    lines+=[f"{EMOJI_TARGET} {s('Top дефициты')}", SEP_BOLD]
    if top_def:
        for td in top_def[:8]:
            lines.append(f"SKU {td['sku']} {td['name'][:30]} cov={td['coverage']:.2f} need={td['deficit_need']}")
    else:
        lines.append("Нет дефицитов.")
    lines.append("")
    lines+=[f"{EMOJI_WH} {s('Top склады')}", SEP_BOLD]
    if top_wh:
        for w in top_wh[:6]:
            lines.append(f"{w['name'][:30]} need={w['total_need']} def_need={w['deficit_need']}")
    else:
        lines.append("Нет складов.")
    lines.append("")
    lines+=[f"{EMOJI_CLUSTER} {s('Top кластеры')}", SEP_BOLD]
    if top_cl:
        for c in top_cl[:6]:
            lines.append(f"{c['name'][:30]} need={c['total_need']} def_need={c['deficit_need']}")
    else:
        lines.append("Нет кластеров.")
    lines.append("")
    lines+=[
        f"{EMOJI_PERF} {s('Производительность')}",
        SEP_BOLD,
        f"API {LAST_API_LATENCY_MS:.0f}ms | Анализ {LAST_ANALYZE_MS:.0f}ms | Ошибка={LAST_ANALYZE_ERROR or '-'}",
        f"Snapshots={len(HISTORY_CACHE)} | Кэш ответов={len(ANSWER_CACHE)}"
    ]
    return build_html(lines)

# ================== Answer dispatch ==================
async def answer_and_send_fact(chat_id:int, question:str):
    raw, mode=await llm_fact_answer(question)
    await send_ai_answer(chat_id, question, raw, mode, True)

async def answer_and_send_general(chat_id:int, question:str):
    raw, mode=await llm_general_answer(chat_id, question)
    await send_ai_answer(chat_id, question, raw, mode, False)

# ================== Commands ==================
@dp.message(Command("version"))
async def cmd_version(m: Message):
    ensure_admin(m.from_user.id)
    await m.answer(version_info())

@dp.message(Command("help"))
@dp.message(Command("start"))
async def cmd_start(m: Message):
    ensure_admin(m.from_user.id)
    await m.answer(f"{EMOJI_OK} Бот активен. Версия {VERSION}.", reply_markup=main_menu_kb())
    await send_long(m.chat.id, start_overview())

@dp.message(Command("cluster_map"))
async def cmd_cluster_map(m: Message):
    ensure_admin(m.from_user.id)
    if CLUSTER_MAP:
        lines=["§§B§§Определённые (ENV) кластеры§§EB§§", SEP_THIN]
        for k,v in CLUSTER_MAP.items():
            lines.append(f"{k} => {v}")
        lines.append(SEP_THIN)
    else:
        lines=["§§B§§ENV не задан — работают эвристики§§EB§§",
               "Если склад не опознан — 'Прочие'."]
    await send_long(m.chat.id, build_html(lines))

@dp.message(Command("health"))
async def cmd_health(m: Message):
    ensure_admin(m.from_user.id)
    flags=[]
    if LAST_API_LATENCY_MS>HEALTH_WARN_LATENCY_MS: flags.append("API медленно")
    if LAST_ANALYZE_MS>HEALTH_WARN_LATENCY_MS: flags.append("Анализ медленно")
    status="OK" if not flags else " | ".join(flags)
    lines=[
        "§§B§§HEALTH§§EB§§",
        f"API {LAST_API_LATENCY_MS:.0f}ms  Анализ {LAST_ANALYZE_MS:.0f}ms",
        f"Snapshots={len(HISTORY_CACHE)} SKU_index={len(FACT_INDEX.get('sku', {}))} Clusters={len(FACT_INDEX.get('cluster', {}))}",
        f"ChatMode={BOT_STATE.get('chat_mode')} Style={'ON' if BOT_STATE.get('style_enabled') else 'OFF'}",
        f"Status={status}"
    ]
    await send_long(m.chat.id, build_html(lines))

@dp.message(Command("view_mode"))
async def cmd_view_mode(m: Message):
    ensure_admin(m.from_user.id)
    BOT_STATE["view_mode"]="COMPACT" if BOT_STATE.get("view_mode")=="FULL" else "FULL"
    save_state()
    await m.answer(f"Режим отображения: {BOT_STATE['view_mode']}")

@dp.message(Command("style_toggle"))
async def cmd_style_toggle(m: Message):
    ensure_admin(m.from_user.id)
    BOT_STATE["style_enabled"]=not BOT_STATE.get("style_enabled", True)
    save_state()
    await m.answer(f"Стилизация AI: {'ON' if BOT_STATE['style_enabled'] else 'OFF'}")

@dp.message(Command("chat_mode"))
async def cmd_chat_mode(m: Message):
    ensure_admin(m.from_user.id)
    mode=BOT_STATE.get("chat_mode","fact").upper()
    kb=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔁 Переключить режим", callback_data="chatmode:toggle")]
    ])
    await m.answer(build_html([
        "§§B§§Режим чата§§EB§§",
        f"Текущий: <b>{mode}</b>",
        "Нажмите кнопку или используйте /fact /general."
    ]), reply_markup=kb)

@dp.message(Command("fact"))
async def cmd_fact(m: Message):
    ensure_admin(m.from_user.id)
    BOT_STATE["chat_mode"]="fact"; save_state()
    await m.answer("Режим установлен: FACT")

@dp.message(Command("general"))
async def cmd_general(m: Message):
    ensure_admin(m.from_user.id)
    BOT_STATE["chat_mode"]="general"; save_state()
    await m.answer("Режим установлен: GENERAL")

@dp.message(Command("chat"))
async def cmd_chat(m: Message):
    ensure_admin(m.from_user.id)
    q=m.text.partition(" ")[2].strip()
    if not q:
        return await m.answer("Формат: /chat <сообщение>")
    await m.answer(f"{EMOJI_CHAT} Общаюсь…")
    await answer_and_send_general(m.chat.id, q)

@dp.message(Command("refresh"))
async def cmd_refresh(m: Message):
    ensure_admin(m.from_user.id)
    SKU_NAME_CACHE.clear(); save_cache_if_needed(0)
    await m.answer("Кэш имён SKU очищён.")

@dp.message(Command("analyze"))
async def cmd_analyze(m: Message):
    ensure_admin(m.from_user.id)
    await handle_analyze(m.chat.id)

@dp.message(Command("force_notify"))
async def cmd_force_notify(m: Message):
    ensure_admin(m.from_user.id)
    await m.answer("Формирую отчёт…")
    await daily_notify_job()
    await m.answer("Готово.")

@dp.message(Command("diag"))
async def cmd_diag(m: Message):
    ensure_admin(m.from_user.id)
    rep=build_diag_report()
    await send_long(m.chat.id, rep)

@dp.message(Command("diag_env"))
async def cmd_diag_env(m: Message):
    ensure_admin(m.from_user.id)
    def mask(v:str)->str:
        if not v: return "(empty)"
        if len(v)<8: return v[0]+"***"
        return v[:4]+"****"+v[-4:]
    lines=[
        "<b>ENV</b>",
        f"VERSION={VERSION}",
        f"OZON_CLIENT_ID={'yes' if OZON_CLIENT_ID else 'no'}",
        f"OZON_API_KEY={mask(OZON_API_KEY)}",
        f"LLM_PROVIDER={LLM_PROVIDER}",
        f"GIGACHAT_SCOPE={GIGACHAT_SCOPE}",
        f"GENERAL_TEMP={LLM_GENERAL_TEMPERATURE}",
        f"CHAT_MODE={BOT_STATE.get('chat_mode')}",
        f"STYLE={BOT_STATE.get('style_enabled')}",
        f"INVENTORY_SAMPLE={LLM_INVENTORY_SAMPLE_SKU}",
        f"FULL_DETAIL_SKU={LLM_FULL_DETAIL_SKU}",
        f"FULL_DETAIL_WH={LLM_FULL_DETAIL_WAREHOUSES}",
        f"FACT_SOFT_LIMIT={LLM_FACT_SOFT_LIMIT_CHARS}",
        f"STOCK_PAGE_SIZE={STOCK_PAGE_SIZE}",
        f"CLUSTER_MAP={'yes' if CLUSTER_MAP else 'heuristic'}",
        f"CLUSTER_COUNT={len(FACT_INDEX.get('cluster', {}))}"
    ]
    await send_long(m.chat.id, build_html(lines))

@dp.message(Command("stock"))
async def cmd_stock(m: Message):
    ensure_admin(m.from_user.id)
    missing=[s for s in SKU_LIST if s not in SKU_NAME_CACHE]
    if missing:
        prev=len(SKU_NAME_CACHE)
        mp,_=await ozon_product_names_by_sku(missing)
        SKU_NAME_CACHE.update(mp); save_cache_if_needed(prev)
    kb=build_stock_page(0)
    await m.answer(f"{EMOJI_BOX} Товары:", reply_markup=kb)

@dp.message(Command("warehouses"))
async def cmd_warehouses(m: Message):
    ensure_admin(m.from_user.id)
    rows, err=await ozon_stock_fbo(SKU_LIST)
    if err:
        await m.answer(f"Ошибка Ozon API: {html.escape(err)}"); return
    agg=aggregate_rows(rows)
    wh_map={}
    for wmap in agg.values():
        for wk,info in wmap.items():
            wh_map.setdefault(wk, info["warehouse_name"])
    if not wh_map:
        await m.answer("Нет данных."); return
    kb=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=(nm or wk)[:60], callback_data=f"wh:{wk}")]
        for wk,nm in sorted(wh_map.items(), key=lambda x: x[1].lower())
    ])
    await m.answer(f"{EMOJI_WH} Склады:", reply_markup=kb)

@dp.message(Command("clusters"))
async def cmd_clusters(m: Message):
    ensure_admin(m.from_user.id)
    if not FACT_INDEX.get("cluster"):
        await m.answer("Кластеры ещё не построены. Выполните /analyze.")
        return
    kb=build_cluster_list_kb(FACT_INDEX["cluster"])
    await m.answer(f"{EMOJI_CLUSTER} Кластеры:", reply_markup=kb)

@dp.message(Command("ai"))
async def cmd_ai(m: Message):
    ensure_admin(m.from_user.id)
    q=m.text.partition(" ")[2].strip() or "Весь объём данных"
    await m.answer(f"{EMOJI_AI} Обрабатываю…")
    await answer_and_send_fact(m.chat.id, q)

@dp.message(Command("ask"))
async def cmd_ask(m: Message):
    ensure_admin(m.from_user.id)
    q=m.text.partition(" ")[2].strip()
    if not q:
        return await m.answer("Формат: /ask <вопрос>")
    await m.answer(f"{EMOJI_AI} Анализ фактов…")
    await answer_and_send_fact(m.chat.id, q)

@dp.message(Command("ask_raw"))
async def cmd_ask_raw(m: Message):
    ensure_admin(m.from_user.id)
    q=m.text.partition(" ")[2].strip() or "Весь объем данных"
    facts, mode=build_facts_block(q)
    facts_colored=[]
    for ln in facts.splitlines():
        if "cov=" in ln:
            mc=re.search(r"cov=([0-9.]+)", ln)
            if mc:
                cv=float(mc.group(1))
                color="🟥" if cv<0.25 else "🟧" if cv<0.5 else "🟨" if cv<0.8 else "🟩"
                ln=f"{color} {ln}"
        facts_colored.append(ln)
    facts="\n".join(facts_colored)
    out=[f"MODE={mode}","----- FACTS -----", facts[:3800]]
    if len(facts)>3800: out.append("...(усечено)")
    await send_long(m.chat.id, build_html(out))

@dp.message(Command("facts_info"))
async def cmd_facts_info(m: Message):
    ensure_admin(m.from_user.id)
    if not FACT_INDEX:
        await m.answer("Индекс не построен. /analyze")
        return
    inv=FACT_INDEX.get("inventory_overview", {})
    lines=[
        "§§B§§FACTS INFO§§EB§§",
        f"updated_ts={FACT_INDEX.get('updated_ts')}",
        f"snapshot_ts={FACT_INDEX.get('snapshot_ts')}",
        f"SKU indexed={inv.get('total_sku','?')}",
        f"Warehouses={len(FACT_INDEX.get('warehouse', {}))}",
        f"Clusters={len(FACT_INDEX.get('cluster', {}))}",
        f"Top deficits={len(FACT_INDEX.get('top_deficits', []))}",
        f"Top warehouses={len(FACT_INDEX.get('top_warehouses', []))}",
        f"Top clusters={len(FACT_INDEX.get('top_clusters', []))}",
        f"Answer cache size={len(ANSWER_CACHE)}"
    ]
    await send_long(m.chat.id, build_html(lines))

@dp.message(Command("facts_dump"))
async def cmd_facts_dump(m: Message):
    ensure_admin(m.from_user.id)
    if not FACT_INDEX:
        await m.answer("Пусто.")
        return
    clone=dict(FACT_INDEX)
    for sku, entry in clone.get("sku", {}).items():
        entry["warehouses"]=entry.get("warehouses", [])[:LLM_FULL_DETAIL_WAREHOUSES]
    dump=json.dumps(clone, ensure_ascii=False)
    if len(dump)>3900:
        dump=dump[:3900]+"...(усечено)"
    await send_long(m.chat.id, build_html(["JSON:", dump]))

@dp.message(Command("ai_scope"))
async def cmd_ai_scope(m: Message):
    ensure_admin(m.from_user.id)
    tok=_GIGACHAT_TOKEN_MEM
    ttl=int(tok["expires_epoch"]-time.time()) if tok and tok.get("expires_epoch") else -1
    insecure=(_gigachat_verify_param() is False)
    lines=[
        "§§B§§GigaChat статус§§EB§§",
        f"Enabled={GIGACHAT_ENABLED} ChatMode={BOT_STATE.get('chat_mode')}",
        f"Token={'yes' if tok else 'no'} TTL={ttl if ttl>=0 else '-'}",
        f"SSL mode={GIGACHAT_SSL_MODE}{' (INSECURE!)' if insecure else ''}",
        f"Index SKU={len(FACT_INDEX.get('sku', {}))} Clusters={len(FACT_INDEX.get('cluster', {}))}",
        f"Answer cache={len(ANSWER_CACHE)}"
    ]
    await send_long(m.chat.id, build_html(lines))

@dp.message(Command("ai_reset_token"))
async def cmd_ai_reset_token(m: Message):
    ensure_admin(m.from_user.id)
    global _GIGACHAT_TOKEN_MEM
    _GIGACHAT_TOKEN_MEM={}
    if GIGACHAT_TOKEN_CACHE_FILE.exists():
        try: GIGACHAT_TOKEN_CACHE_FILE.unlink()
        except Exception: pass
    await m.answer("Токен сброшен.")

def _run_shell(cmd:str, timeout=20)->Tuple[int,str,str]:
    try:
        p=subprocess.run(shlex.split(cmd),capture_output=True,text=True,timeout=timeout)
        return p.returncode,p.stdout,p.stderr
    except Exception as e:
        return 1,"",str(e)

@dp.message(Command("ai_tls_diag"))
async def cmd_ai_tls_diag(m: Message):
    ensure_admin(m.from_user.id)
    cmd="openssl s_client -showcerts -servername ngw.devices.sberbank.ru -connect ngw.devices.sberbank.ru:9443 < /dev/null"
    rc,out,err=_run_shell(cmd,timeout=25)
    snippet="\n".join(out.splitlines()[:30])
    lines=[
        "§§B§§TLS Диагностика§§EB§§",
        f"rc={rc}",
        f"BEGIN_CERT count={out.count('BEGIN CERTIFICATE')}",
        f"SSL_MODE={GIGACHAT_SSL_MODE}",
        "----- SNIPPET -----",
        snippet or "(empty)"
    ]
    await send_long(m.chat.id, build_html(lines))

@dp.message(Command("ai_auto_ca"))
async def cmd_ai_auto_ca(m: Message):
    ensure_admin(m.from_user.id)
    if not GIGACHAT_CA_CERT:
        await m.answer("GIGACHAT_CA_CERT не задан.")
        return
    cmd="openssl s_client -showcerts -servername ngw.devices.sberbank.ru -connect ngw.devices.sberbank.ru:9443 < /dev/null"
    rc,out,err=_run_shell(cmd,timeout=25)
    if rc!=0 or "BEGIN CERTIFICATE" not in out:
        await m.answer(f"Не удалось получить цепочку rc={rc} err={err[:120]}")
        return
    blocks=out.split("-----BEGIN CERTIFICATE-----")
    certs=[]
    for blk in blocks:
        if "-----END CERTIFICATE-----" in blk:
            cert="-----BEGIN CERTIFICATE-----"+blk.split("-----END CERTIFICATE-----")[0]+"-----END CERTIFICATE-----\n"
            certs.append(cert)
    if not certs:
        await m.answer("Сертификаты не извлечены.")
        return
    bundle="".join(certs)
    try:
        Path(GIGACHAT_CA_CERT).parent.mkdir(parents=True, exist_ok=True)
        Path(GIGACHAT_CA_CERT).write_text(bundle, encoding="utf-8")
    except Exception as e:
        await m.answer(f"Ошибка записи CA: {e}")
        return
    await m.answer(f"CA bundle обновлён. Сертификатов: {bundle.count('BEGIN CERTIFICATE')}.")

# (опц.) Хелперы управления задачами (если supply_watch предоставляет функции)
@dp.message(Command("supply_purge_all"))
async def cmd_supply_purge_all(m: Message):
    ensure_admin(m.from_user.id)
    if not purge_all_tasks:
        await m.answer("Функция недоступна в этой сборке.")
        return
    cnt = purge_all_tasks()
    await m.answer(f"Удалено задач: {cnt}")

@dp.message(Command("supply_purge_stale"))
async def cmd_supply_purge_stale(m: Message):
    ensure_admin(m.from_user.id)
    if not purge_stale_nonfinal:
        await m.answer("Функция недоступна в этой сборке.")
        return
    try:
        hours = int(m.text.strip().split(maxsplit=1)[1])
    except Exception:
        hours = 48
    cnt = purge_stale_nonfinal(hours=hours)
    await m.answer(f"Удалено зависших не финальных задач за >{hours}ч: {cnt}")

# ================== FSM Chat ==================
@dp.message(F.text == "🤖 AI чат")
async def btn_ai_chat(m: Message, state: FSMContext):
    ensure_admin(m.from_user.id)
    if not GIGACHAT_ENABLED:
        await m.answer("LLM отключён.", reply_markup=main_menu_kb())
        return
    await state.set_state(AIChatState.waiting)
    mode=BOT_STATE.get("chat_mode","fact")
    await m.answer(
        f"AI чат активирован. Режим: {mode.upper()}.\nКоманды: /fact /general /chat_mode /cancel\nНапиши сообщение:",
        reply_markup=main_menu_kb()
    )

@dp.message(Command("cancel"))
@dp.message(F.text == "❌ Отмена")
async def cmd_cancel(m: Message, state: FSMContext):
    ensure_admin(m.from_user.id)
    await state.clear()
    await m.answer("AI чат завершён.", reply_markup=main_menu_kb())

@dp.message(AIChatState.waiting)
async def ai_chat_waiting(m: Message, state: FSMContext):
    ensure_admin(m.from_user.id)
    q=(m.text or "").strip()
    if not q:
        await m.answer("Пустой запрос. /cancel для выхода.")
        return
    mode=BOT_STATE.get("chat_mode","fact")
    await m.answer(f"{EMOJI_AI} Думаю… ({mode})")
    if mode=="general":
        raw, rmode=await llm_general_answer(m.chat.id, q)
        await send_ai_answer(m.chat.id, q, raw, rmode, False)
    else:
        raw, rmode=await llm_fact_answer(q)
        await send_ai_answer(m.chat.id, q, raw, rmode, True)
    await m.answer("Следующий вопрос или /cancel.", reply_markup=main_menu_kb())

# ================== Button Aliases ==================
BUTTON_ALIASES={
    "анализ": cmd_analyze,
    "отчёт сейчас": cmd_analyze,
    "товары": cmd_stock,
    "склады": cmd_warehouses,
    "кластеры": cmd_clusters,
    "режим отображения": cmd_view_mode,
    "диагностика": cmd_diag,
    "сброс кэша": cmd_refresh
}

# ВАЖНО: универсальный хендлер не ловит команды — благодаря regexp, исключающему строки, начинающиеся с "/"
@dp.message(F.text.regexp(r'^(?!\/)'))
async def text_buttons(m: Message, state: FSMContext):
    # Если активен AI-чат — не обрабатываем кнопки тут
    if await state.get_state()==AIChatState.waiting.state:
        return
    raw=(m.text or "").lower()
    for em in ["🔍","📣","📦","🏬","🗺","⚙","🧪","🔄","🤖","❌"]:
        raw=raw.replace(em.lower(),"")
    raw=raw.strip()
    if raw in BUTTON_ALIASES:
        ensure_admin(m.from_user.id)
        await BUTTON_ALIASES[raw](m)

# ================== Jobs ==================
async def snapshot_job():
    if time.time()-LAST_SNAPSHOT_TS<SNAPSHOT_MIN_REUSE_SECONDS: return
    rows, err=await ozon_stock_fbo(SKU_LIST)
    if err:
        log.warning("snapshot_job error: %s", err); return
    if not rows:
        log.warning("snapshot_job empty rows"); return
    append_snapshot(rows)
    missing=[s for s in SKU_LIST if s not in SKU_NAME_CACHE]
    if missing:
        prev=len(SKU_NAME_CACHE)
        mp,_=await ozon_product_names_by_sku(missing)
        SKU_NAME_CACHE.update(mp); save_cache_if_needed(prev)
    try:
        ccache=build_consumption_cache()
        build_fact_index(rows, [], ccache)
    except Exception as e:
        log.warning("snapshot index build fail: %s", e)
    await flush_history_if_needed(force=True)

async def daily_notify_job():
    if ADMIN_ID is None: return
    async with ANALYZE_LOCK:
        rows, err=await ozon_stock_fbo(SKU_LIST)
        if err:
            await send_safe_message(ADMIN_ID,f"Ошибка Ozon API: {html.escape(err)}"); return
        if time.time()-LAST_SNAPSHOT_TS>SNAPSHOT_MIN_REUSE_SECONDS:
            append_snapshot(rows); await flush_history_if_needed(force=True)
        ccache=build_consumption_cache()
        missing=[s for s in SKU_LIST if s not in SKU_NAME_CACHE]
        if missing:
            prev=len(SKU_NAME_CACHE)
            mp,_=await ozon_product_names_by_sku(missing)
            SKU_NAME_CACHE.update(mp); save_cache_if_needed(prev)
        report, flat=generate_deficit_report(rows, SKU_NAME_CACHE, ccache)
        LAST_DEFICIT_CACHE[ADMIN_ID]={"flat":flat,"timestamp":int(time.time()),
                                      "report":report,"raw_rows":rows,"consumption_cache":ccache}
        try:
            build_fact_index(rows, flat, ccache)
        except Exception as e:
            log.warning("FACT_INDEX daily build fail: %s", e)
        header=f"{EMOJI_NOTIFY} <b>Ежедневный отчёт {DAILY_NOTIFY_HOUR:02d}:{DAILY_NOTIFY_MINUTE:02d}</b>\n"
        kb=deficit_filters_kb()
        await send_long(ADMIN_ID, header+report, kb=kb)
        await flush_history_if_needed()

async def maintenance_job():
    prune_history()
    await flush_history_if_needed()

async def init_snapshot():
    rows, err=await ozon_stock_fbo(SKU_LIST)
    if err:
        log.warning("init snapshot error: %s", err); return
    append_snapshot(rows)
    try:
        ccache=build_consumption_cache()
        build_fact_index(rows, [], ccache)
    except Exception as e:
        log.warning("init index build fail: %s", e)
    await flush_history_if_needed(force=True)
    log.info("Initial snapshot rows=%d", len(rows))

def log_jobs(sched: AsyncIOScheduler):
    for job in sched.get_jobs():
        try:
            fn = getattr(job, "func", None)
            fn_name = getattr(fn, "__qualname__", str(fn))
        except Exception:
            fn_name = "<?>"
        log.info("Job id=%s next=%s func=%s", job.id, job.next_run_time, fn_name)

def _cleanup_duplicate_supply_jobs(scheduler: AsyncIOScheduler):
    """
    Если setup_supply_handlers зарегистрировал свою интервал-добу process_tasks,
    пытаемся убрать дубликат, оставив только register_supply_scheduler.
    """
    removed = 0
    for job in list(scheduler.get_jobs()):
        try:
            fn = getattr(job, "func", None)
            qn = getattr(fn, "__qualname__", "") or ""
            mod = getattr(fn, "__module__", "") or ""
            # эвристика: локальная функция из setup_supply_handlers
            if "setup_supply_handlers" in qn or "supply_integration" in mod and "process_tasks" in qn:
                scheduler.remove_job(job.id)
                removed += 1
                log.info("Removed duplicate supply process job id=%s qn=%s", job.id, qn)
        except Exception as e:
            log.debug("job scan error: %s", e)
    if removed:
        log.info("Duplicate supply jobs removed: %d", removed)

# ================== Signals ==================
def _install_signals(loop: asyncio.AbstractEventLoop):
    async def _graceful(sig:str):
        log.info("Signal %s -> graceful stop", sig)
        try: await flush_history_if_needed(force=True)
        except Exception: pass
        loop.stop()
    for s in ("SIGINT","SIGTERM"):
        if hasattr(signal,s):
            try:
                loop.add_signal_handler(getattr(signal,s), lambda ss=s: asyncio.create_task(_graceful(ss)))
            except NotImplementedError:
                pass

# ================== Main ==================
async def main():
    load_state(); load_cache(); load_history()
    if not HISTORY_CACHE:
        await init_snapshot()
    try:
        tz=ZoneInfo(TZ_NAME)
    except Exception:
        tz=ZoneInfo("UTC")
        log.warning("Fallback TZ=UTC")
    scheduler=AsyncIOScheduler(timezone=tz)
    scheduler.add_job(snapshot_job,"interval",minutes=SNAPSHOT_INTERVAL_MINUTES,id="snapshot_job",replace_existing=True)
    scheduler.add_job(daily_notify_job,"cron",hour=DAILY_NOTIFY_HOUR,minute=DAILY_NOTIFY_MINUTE,id="daily_notify",replace_existing=True)
    scheduler.add_job(maintenance_job,"interval",minutes=HISTORY_PRUNE_EVERY_MINUTES,id="maintenance_job",replace_existing=True)
    scheduler.start()

    # ======= AUTO-SUPPLY INTEGRATION =======
    # Регистрируем хендлеры и служебные команды автопоставок
    si.setup_supply_handlers(bot, dp, scheduler)
    log.info("Auto-supply handlers registered.")

    # Попробуем убрать возможные дубли планировщика, созданные внутри setup_supply_handlers
    _cleanup_duplicate_supply_jobs(scheduler)

    # Регистрируем фоновую джобу supply_watch РОВНО один раз (без дублей)
    register_supply_scheduler(
        scheduler,
        notify_text=supply_notify_text,
        notify_file=supply_notify_file,
        interval_seconds=SUPPLY_JOB_INTERVAL,
    )
    log.info("Supply-watch scheduler registered (interval=%ss).", SUPPLY_JOB_INTERVAL)
    # ======================================

    log_jobs(scheduler)
    loop=asyncio.get_running_loop()
    _install_signals(loop)
    log.info("Bot started version=%s mock_mode=%s", VERSION, MOCK_MODE)
    try:
        await dp.start_polling(bot)
    finally:
        try: scheduler.shutdown(wait=False)
        except Exception: pass
        await flush_history_if_needed(force=True)
        log.info("Shutdown complete.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("KeyboardInterrupt -> exit")
        try:
            asyncio.run(flush_history_if_needed(force=True))
        except Exception:
            pass