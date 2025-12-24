from __future__ import annotations

import json
import os
from typing import List, Dict, Any, Tuple
from datetime import datetime, timedelta, timezone

# Реиспользуем готовый оркестратор
from supply_watch import (
    create_tasks_from_template,
    OzonApi,
    api_draft_create,
    api_draft_create_info,
    api_timeslot_info,
    REVERSE_WAREHOUSE_MAP,
)

# ===== Даты (локальные удобные утилиты) =====

TZ_YEKAT = timezone(timedelta(hours=5))

def today_iso() -> str:
    return datetime.now(TZ_YEKAT).strftime("%Y-%m-%d")

def tomorrow_iso() -> str:
    return (datetime.now(TZ_YEKAT) + timedelta(days=1)).strftime("%Y-%m-%d")

def add_days_iso(date_iso: str, days: int) -> str:
    dt = datetime(int(date_iso[0:4]), int(date_iso[5:7]), int(date_iso[8:10]), tzinfo=TZ_YEKAT)
    return (dt + timedelta(days=int(days))).strftime("%Y-%m-%d")


# ===== Источники данных для товаров/складов =====

SKU_CACHE_FILE = os.getenv("SKU_CACHE_FILE", "sku_cache.json")

async def find_products_page(page: int = 1, page_size: int = 10) -> Tuple[List[Dict[str, Any]], bool, bool]:
    """
    Читает sku_cache.json и отдаёт страничку товаров.
    Ожидаемые поля на элемент: {"sku": "...", "name": "..."} — гибкая обработка.
    """
    try:
        with open(SKU_CACHE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        data = []

    items: List[Dict[str, Any]] = []
    for it in data if isinstance(data, list) else []:
        sku = str(it.get("sku") or it.get("offer_id") or it.get("id") or "")
        name = str(it.get("name") or it.get("title") or sku)
        if not sku:
            continue
        items.append({"sku": sku, "title": name})

    total = len(items)
    start = max(0, (page - 1) * page_size)
    end = min(total, start + page_size)
    page_items = items[start:end]
    has_prev = page > 1
    has_next = end < total
    return page_items, has_prev, has_next


async def list_warehouses_for_product(product_id: str) -> List[Dict[str, Any]]:
    """
    Отдаёт список складов на основе REVERSE_WAREHOUSE_MAP (из supply_watch).
    Если карта пуста — вернём пустой список, мастер всё равно отработает (умный выбор в оркестраторе).
    """
    out: List[Dict[str, Any]] = []
    if isinstance(REVERSE_WAREHOUSE_MAP, dict) and REVERSE_WAREHOUSE_MAP:
        for wid, nm in REVERSE_WAREHOUSE_MAP.items():
            out.append({"id": int(wid), "title": str(nm)})
    return out


# ===== Превью слотов (временный draft) =====

async def ensure_preview_draft_and_timeslots(form: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Создаёт временный draft под выбранные SKU/qty/склад и читает слоты на выбранную дату.
    Никаких задач не создаём — это только превью для кнопок.
    """
    client_id = os.getenv("OZON_CLIENT_ID", "")
    api_key = os.getenv("OZON_API_KEY", "")
    api = OzonApi(client_id, api_key)

    # Черновик в минимальной форме, понятной supply_watch.api_draft_create(...)
    task_like = {
        "id": "preview",
        "sku_list": [{
            "sku": form["product_id"],
            "total_qty": int(form["quantity"]),
            "boxes": 1,
            "per_box": int(form["quantity"]),
            "warehouse_name": form.get("warehouse_name") or "",
        }],
        "supply_type": "CREATE_TYPE_DIRECT",
        "chosen_warehouse_id": int(form["warehouse_id"]),
        "date": str(form["date_iso"]),
    }

    ok, op_id, err, status = await api_draft_create(api, task_like)
    if not ok or not op_id:
        return []

    ok2, draft_id, warehouses, err2, status2 = await api_draft_create_info(api, op_id)
    if not ok2 or not draft_id:
        return []

    wid = int(form["warehouse_id"])
    ok3, slots, err3, status3 = await api_timeslot_info(api, str(draft_id), [wid], str(form["date_iso"]))
    if not ok3:
        return []

    out: List[Dict[str, Any]] = []
    for s in slots or []:
        fr = (s.get("from_in_timezone") or s.get("from") or "")[-8:-3]
        to = (s.get("to_in_timezone") or s.get("to") or "")[-8:-3]
        sid = s.get("id") or s.get("timeslot_id") or s.get("slot_id") or ""
        out.append({"id": sid, "from_hhmm": fr, "to_hhmm": to})
    return out


# ===== Создание задачи из формы =====

async def create_task_from_form(form: Dict[str, Any], chat_id: int) -> Dict[str, Any]:
    """
    Конструируем текст под create_tasks_from_template.
    Если slot выбран — ставим точное окно; если нет (автопоиск) — задаём нейтральное окно 12:00–13:00.
    """
    date_iso = str(form["date_iso"])
    # Выбран слот? В превью мы не сохраняли его из API (только id). Окно выставит supply_watch при создании.
    hhmm_from = "12:00"
    hhmm_to = "13:00"

    qty = int(form.get("quantity") or 1)
    per_box = qty
    boxes = 1

    sku = str(form["product_id"])
    wh_name = str(form.get("warehouse_name") or "")  # supply_watch может сметчить умно и по ID/названию

    # “На DD.MM.YYYY, HH:MM-HH:MM”
    header = f"На {date_iso[8:10]}.{date_iso[5:7]}.{date_iso[0:4]}, {hhmm_from}-{hhmm_to}"
    # “SKU - кол-во X, Y коробка, в каждой коробке по Z <warehouse>”
    line = f"{sku} - кол-во {qty}, {boxes} коробка, в каждой коробке по {per_box} {wh_name}".strip()

    template = f"{header}\n{line}"
    tasks = create_tasks_from_template(template, mode="FBO", chat_id=int(chat_id))
    return tasks[0] if tasks else {}