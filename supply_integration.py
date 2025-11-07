import logging
import os
import re
import time
from typing import Tuple, Optional, List, Dict, Any

# Базовая настройка логгера
if not logging.getLogger().handlers:
    logging.basicConfig(
        level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
        format="%(levelname)s:%(name)s:%(message)s",
    )

# Патч нормализации Timestamp для timeslot/info (учёт TZ drop-off)
try:
    import httpx_timeslot_patch  # noqa: F401
    logging.info("supply_integration: httpx_timeslot_patch is active")
except Exception as _e_patch:
    logging.warning("supply_integration: httpx_timeslot_patch failed to load: %s", _e_patch)

# Опциональный лимитер httpx — снижает 429
try:
    import httpx_rate_limit_patch  # noqa: F401
    logging.info("supply_integration: httpx rate limit patch is active")
except Exception as _e_rl:
    logging.info("supply_integration: httpx rate limit patch not loaded: %s", _e_rl)

from aiogram import F
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.dispatcher.middlewares.base import BaseMiddleware
from aiogram.types.input_file import FSInputFile

import supply_watch as sw
import httpx  # <— для обращения к API Ozon

# Маппинги складов приёмки
try:
    from supply_mapping import resolve_warehouse_id, available_keys
except Exception as _e:
    resolve_warehouse_id = None  # type: ignore
    available_keys = None        # type: ignore
    logging.error("supply_integration: supply_mapping not available: %s", _e)

# Опционально: отдельный маппинг для drop-off, если есть
try:
    from dropoff_mapping import resolve_dropoff_id, available_dropoffs
except Exception:
    resolve_dropoff_id = None       # type: ignore
    available_dropoffs = None       # type: ignore

# =========================== Env (drop-off) ===========================

def _normalize_name(s: str) -> str:
    s = (s or "").strip()
    s = s.replace("_", " ").replace("-", " ")
    s = re.sub(r"\s+", " ", s, flags=re.IGNORECASE)
    return s.upper()

def _fetch_dropoff_map_from_ozon() -> Dict[str, int]:
    client_id = os.getenv("OZON_CLIENT_ID", "").strip()
    api_key = os.getenv("OZON_API_KEY", "").strip()
    if not client_id or not api_key:
        logging.warning("env drop-off resolve: OZON_CLIENT_ID/API_KEY not set; skip API lookup")
        return {}

    url = "https://api-seller.ozon.ru/v1/warehouse/fbo/list"
    headers = {"Client-Id": client_id, "Api-Key": api_key, "Content-Type": "application/json"}
    try:
        with httpx.Client(timeout=15.0) as s:
            r = s.post(url, headers=headers, json={})
            if r.status_code != 200:
                logging.warning("env drop-off resolve: %s -> HTTP %s %s", url, r.status_code, r.text[:300])
                return {}
            data = r.json()
    except Exception as e:
        logging.warning("env drop-off resolve: request failed: %s", e)
        return {}

    result: Dict[str, int] = {}
    def _dig(obj):
        if isinstance(obj, dict):
            if "name" in obj and any(k in obj for k in ("warehouse_id", "warehouseId", "id")):
                try:
                    wid = obj.get("warehouse_id", obj.get("warehouseId", obj.get("id")))
                    if isinstance(wid, (int, str)) and str(wid).isdigit():
                        nm = str(obj["name"]).strip()
                        result[nm] = int(wid)
                except Exception:
                    pass
            for v in obj.values():
                _dig(v)
        elif isinstance(obj, list):
            for v in obj:
                _dig(v)

    _dig(data)
    if not result:
        logging.warning("env drop-off resolve: empty drop-off list parsed from response")
    return result

def _env_dropoff() -> Tuple[Optional[str], Optional[int]]:
    env_name = (os.getenv("OZON_DROP_OFF_NAME") or "").strip() or None
    raw_id = (os.getenv("OZON_DROP_OFF_ID") or "").strip()
    env_id: Optional[int] = None
    if raw_id.isdigit():
        try:
            env_id = int(raw_id)
        except Exception:
            env_id = None

    mapped_local: Optional[int] = None
    if env_name:
        try:
            if callable(resolve_dropoff_id):
                v = resolve_dropoff_id(env_name)
                if v and str(v).isdigit():
                    mapped_local = int(v)
            elif callable(resolve_warehouse_id):
                v = resolve_warehouse_id(env_name)
                if v and str(v).isdigit():
                    mapped_local = int(v)
        except Exception:
            mapped_local = None

    remote = _fetch_dropoff_map_from_ozon()
    chosen_name = None
    chosen_id = None

    if env_id is not None:
        if env_id in remote.values() or not remote:
            chosen_id = env_id
            chosen_name = env_name
        else:
            logging.warning("ENV drop-off id=%s not found in Ozon drop-off list; will attempt resolve by name", env_id)

    if chosen_id is None and env_name:
        if env_name in remote:
            chosen_name = env_name
            chosen_id = remote[env_name]
        else:
            target = _normalize_name(env_name)
            candidates = [(nm, wid) for nm, wid in remote.items() if _normalize_name(nm) == target]
            if candidates:
                chosen_name, chosen_id = candidates[0]
            else:
                parts = [p for p in target.split() if len(p) >= 3]
                for nm, wid in remote.items():
                    norm_nm = _normalize_name(nm)
                    if all(p in norm_nm for p in parts):
                        chosen_name, chosen_id = nm, wid
                        break

    if chosen_id is None and mapped_local is not None:
        chosen_id = mapped_local
        chosen_name = env_name

    if chosen_id is not None:
        if chosen_name:
            logging.info("ENV drop-off resolved: %s -> id=%s", chosen_name, chosen_id)
        else:
            logging.info("ENV drop-off resolved: id=%s", chosen_id)
    else:
        if env_name or env_id:
            logging.error("ENV drop-off cannot be resolved. name=%r id=%r. Configure OZON_DROP_OFF_ID to an actual drop-off point id (from /v1/warehouse/fbo/list).", env_name, env_id)
        else:
            logging.warning("ENV drop-off is not set. Set OZON_DROP_OFF_ID or OZON_DROP_OFF_NAME.")

    return chosen_name or env_name, chosen_id


# =========================== Helpers ===========================

def _split_command_and_payload(text: str) -> Tuple[str, str]:
    text = text or ""
    lines = text.splitlines()
    cmd_line = lines[0] if lines else ""
    payload = "\n".join(lines[1:]) if len(lines) > 1 else ""
    return cmd_line.strip(), payload.strip()


def _detect_mode(cmd_line: str) -> str:
    m = re.search(r"\b(FBO|FBS|FBY)\b", (cmd_line or "").upper())
    return m.group(1) if m else "FBO"


def _extract_arg(message: Message) -> str:
    text = (message.text or "").strip()
    m = re.match(r"^/\w+(?:@\w+)?\s*(.*)$", text)
    arg = (m.group(1) if m else "").strip()
    if not arg and message.reply_to_message:
        arg = (message.reply_to_message.text or message.reply_to_message.caption or "").strip()
    return arg


def _find_task_by_id_or_sku(token: str):
    token = (token or "").strip()
    if not token:
        return None
    # По id либо short(id)
    for t in sw.list_all_tasks():
        tid = str(t.get("id") or "")
        if tid == token or sw.short(tid) == token:
            return t
    # По SKU (если число)
    if token.isdigit():
        for t in sw.list_all_tasks():
            try:
                sku = (t.get("sku_list") or [{}])[0].get("sku")
                if str(sku) == token:
                    return t
            except Exception:
                continue
    return None


# =========================== Warehouse extraction (only supply from template) ===========================

WAREHOUSE_LINE_RE    = re.compile(r"^\s*(?:Склад|СКЛАД|warehouse|WAREHOUSE)\s*(?::|=|\s+)\s*(.+?)\s*$", re.IGNORECASE)
WAREHOUSE_ID_LINE_RE = re.compile(r"^\s*(?:WAREHOUSE[_\- ]?ID|СКЛАД[_\- ]?ID)\s*(?::|=|\s+)\s*(\d{11,19})\s*$", re.IGNORECASE)
BARE_WID_LINE_RE     = re.compile(r"^\s*(\d{11,19})\s*$")

def _extract_supply_from_text_block(block: str) -> Tuple[str, Optional[str], Optional[int]]:
    lines = (block or "").splitlines()
    keep: List[str] = []
    supply_name: Optional[str] = None
    supply_id: Optional[int] = None

    for line in lines:
        ln = line.strip()

        if supply_id is None:
            m = WAREHOUSE_ID_LINE_RE.match(ln)
            if m:
                try:
                    supply_id = int(m.group(1)); continue
                except Exception:
                    pass

        if supply_id is None:
            m = BARE_WID_LINE_RE.match(ln)
            if m:
                try:
                    supply_id = int(m.group(1)); continue
                except Exception:
                    pass

        if supply_name is None:
            m = WAREHOUSE_LINE_RE.match(ln)
            if m:
                supply_name = m.group(1).strip(); continue

        keep.append(line)

    return "\n".join(keep).strip(), supply_name, supply_id


def _resolve_name_by_id_generic(wid: Optional[int], keys_fn, resolve_fn) -> Optional[str]:
    if not wid or not resolve_fn or not callable(resolve_fn):
        return None
    try:
        keys = list(keys_fn() or []) if callable(keys_fn) else []
        for name in keys:
            try:
                mid = resolve_fn(str(name))
                if mid and int(mid) == int(wid):
                    return str(name)
            except Exception:
                continue
    except Exception as e:
        logging.warning("resolve_name_by_id_generic failed: %s", e)
    return None


# =========================== Payload normalizer ===========================

# Все виды дефисов, которые встречаются у пользователей: - ‐ ‑ ‒ – — ― −
_DASH_CHARS = "\u002D\u2010\u2011\u2012\u2013\u2014\u2015\u2212"
PHONE_FINDER_RE = re.compile(r"(\+?\d[\d\-\s\(\)]{6,}\d)")
ITEM_MARKERS_RE = re.compile(r"(кол-?\s*во|количеств|короб|шт\b)", re.IGNORECASE)

def _canon_all_dashes(text: str) -> str:
    if not isinstance(text, str):
        return text
    # узкие/неразрывные пробелы -> обычный пробел
    text = text.replace("\u00A0", " ").replace("\u2009", " ").replace("\u202F", " ")
    for ch in _DASH_CHARS:
        text = text.replace(ch, "-")
    # выровнять пробелы вокруг дефиса и сжать пробелы
    text = re.sub(r"\s*-\s*", " - ", text)
    text = re.sub(r"\s{2,}", " ", text)
    return text.strip()

def _normalize_basic(text: str) -> str:
    text = _canon_all_dashes(text)
    text = re.sub(r"\bшт\.\b", "шт", text, flags=re.IGNORECASE)
    text = re.sub(r"\bштук(?:[а-я]*)\b", "шт", text, flags=re.IGNORECASE)
    text = re.sub(r"\bштуки\b", "шт", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*,\s*", ", ", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    return text.strip()

def _is_contact_line(s: str) -> bool:
    """
    Контактная строка — только если:
      - найден телефонный паттерн И
      - нет маркеров товарной позиции (кол-во/количество, коробка, шт) И
      - есть явные признаки телефона/контакта: '+', '(', ')', '-' или запятая в строке
    Это исключает SKU-линии вида '2625768907 — количество ...'
    """
    if not PHONE_FINDER_RE.search(s or ""):
        return False
    if ITEM_MARKERS_RE.search(s or ""):
        return False
    return ("+" in s) or ("(" in s) or (")" in s) or ("-" in s) or ("," in s)


SKU_RE = re.compile(r"(\d{6,})")

def _parse_item_line_freeform(line: str) -> Dict[str, Any]:
    """
    Извлекает sku/qty/boxes/per из произвольной строки. Отсутствующие числа оставляет None.
    Примеры:
      "2625768907 — количество 10, 1 коробки, по 10 шт"
      "2625768907 - 10 шт., 1 коробка по 10 шт."
    """
    raw = (line or "").strip()
    out: Dict[str, Any] = {"sku": None, "qty": None, "boxes": None, "per": None, "_raw": raw}
    if not raw:
        return out

    # SKU
    msku = SKU_RE.search(raw)
    if not msku:
        return out
    out["sku"] = msku.group(1)

    # Количество
    mqty = re.search(r"количеств[оаы]?\s*[:=]?\s*(\d+)\b", raw, flags=re.IGNORECASE)
    if mqty:
        out["qty"] = int(mqty.group(1))
    else:
        mqty2 = re.search(r"\b(\d+)\s*шт\.?\b", raw, flags=re.IGNORECASE)
        if mqty2:
            out["qty"] = int(mqty2.group(1))

    # Коробки
    mbox = re.search(r"\b(\d+)\s*короб", raw, flags=re.IGNORECASE)
    if mbox:
        out["boxes"] = int(mbox.group(1))

    # По N шт (в коробке)
    mper = re.search(r"\bпо\s*(\d+)\s*шт\.?\b", raw, flags=re.IGNORECASE)
    if mper:
        out["per"] = int(mper.group(1))
    else:
        mper2 = re.search(r"\bв\s*(?:каждой\s*)?коробк[еаи]\s*по\s*(\d+)\s*шт\.?\b", raw, flags=re.IGNORECASE)
        if mper2:
            out["per"] = int(mper2.group(1))

    # Доукомплектуем недостающие поля по простой логике
    q, b, p = out["qty"], out["boxes"], out["per"]
    try:
        if q is None and b is not None and p is not None:
            out["qty"] = b * p
        elif q is not None and b is None and p is not None and p > 0 and q % p == 0:
            out["boxes"] = q // p
        elif q is not None and b is not None and p is None and b > 0 and q % b == 0:
            out["per"] = q // b
    except Exception:
        pass

    return out


def _split_header_and_items(payload: str) -> Tuple[str, List[str]]:
    lines = [ln for ln in (payload or "").splitlines()]
    while lines and not lines[0].strip():
        lines.pop(0)
    if not lines:
        return "", []
    header = lines[0].strip()
    items = [ln for ln in lines[1:] if ln.strip()]
    return header, items


def _boxes_word_ru(n: int) -> str:
    n = int(n)
    last = n % 10
    last2 = n % 100
    if last == 1 and last2 != 11:
        return "коробка"
    if 2 <= last <= 4 and not (12 <= last2 <= 14):
        return "коробки"
    return "коробок"


def _build_canonical_line(item: Dict[str, Any], wh_tail: Optional[str]) -> Optional[str]:
    sku = item.get("sku")
    qty = item.get("qty")
    boxes = item.get("boxes")
    per = item.get("per")

    if qty is None and boxes is not None and per is not None:
        qty = boxes * per
    if boxes is None and qty is not None and per is not None and per > 0 and qty % per == 0:
        boxes = qty // per
    if per is None and qty is not None and boxes is not None and boxes > 0 and qty % boxes == 0:
        per = qty // boxes

    if not (sku and isinstance(qty, int) and isinstance(boxes, int) and isinstance(per, int)):
        return None

    # ВАЖНО: всегда используем обычный дефис '-' (не длинное тире)
    base = f"{sku} - количество {qty}, {boxes} {_boxes_word_ru(boxes)}, по {per} шт"
    if wh_tail:
        return f"{base}, {wh_tail}"
    return base


def _construct_payload(header: str, parsed_items: List[Dict[str, Any]], wh_tail: Optional[str]) -> Optional[str]:
    lines = [header]
    for it in parsed_items:
        canon = _build_canonical_line(it, wh_tail)
        if not canon:
            return None
        lines.append(canon)
    # унифицируем дефисы на всякий случай
    return _canon_all_dashes("\n".join(lines))


# =========================== Mapping helpers ===========================

def _map_supply_inplace(t: Dict[str, Any]) -> None:
    try:
        # Нормализация типов
        for key in ("warehouse_id", "chosen_warehouse_id"):
            wid = t.get(key)
            if isinstance(wid, str) and wid.isdigit():
                t[key] = int(wid)
        wid = t.get("warehouse_id") or t.get("chosen_warehouse_id")

        if isinstance(wid, int):
            t["warehouse_id"] = int(wid)
            t["chosen_warehouse_id"] = int(wid)
            return

        # Маппинг по названию приёмки
        name = t.get("warehouse") or t.get("warehouse_name") or t.get("warehouse_title")
        if not name:
            return

        if resolve_warehouse_id is None:
            t["last_error"] = "supply_mapping не подключен: нельзя определить warehouse_id по названию"
            return

        mapped = resolve_warehouse_id(str(name))
        if mapped is None:
            hint = ""
            try:
                if callable(available_keys):
                    keys = list(available_keys() or ())
                    if keys:
                        hint = f" Доступные: {', '.join(map(str, keys[:50]))}" + (" ..." if len(keys) > 50 else "")
            except Exception:
                pass
            t["last_error"] = f"Склад приёмки '{name}' не найден." + hint
            return

        t["warehouse_id"] = int(mapped)
        t["chosen_warehouse_id"] = int(mapped)
        for k in ("warehouse", "warehouse_name", "warehouse_title"):
            t.pop(k, None)

        logging.info("Mapped SUPPLY '%s' -> %s for task id=%s", name, mapped, t.get("id"))
    except Exception as e:
        logging.exception("map_supply_inplace error: %s", e)
        t["last_error"] = f"Ошибка маппинга склада приёмки: {e}"


def _apply_mapping_to_tasks(tasks: List[Dict[str, Any]]) -> None:
    for t in tasks or ():
        _map_supply_inplace(t)


async def _remap_all_tasks() -> None:
    try:
        tasks = sw.list_tasks() or []
        _apply_mapping_to_tasks(tasks)
    except Exception as e:
        logging.exception("remap_all_tasks failed: %s", e)


# =========================== Handlers ===========================

async def cmd_schedule_supply(message: Message):
    cmd_line, payload0 = _split_command_and_payload(message.text or "")
    if not payload0 and message.reply_to_message and (message.reply_to_message.text or message.reply_to_message.caption):
        payload0 = (message.reply_to_message.text or message.reply_to_message.caption or "").strip()

    mode = _detect_mode(cmd_line)
    chat_id = message.chat.id

    if not payload0:
        hint_supply = ""
        try:
            if callable(available_keys):
                keys = list(available_keys() or ())
                if keys:
                    hint_supply = "\nДоступные склады приёмки: " + ", ".join(map(str, keys[:20])) + (" ..." if len(keys) > 20 else "")
        except Exception:
            pass

        await message.answer(
            "Пример:\n"
            "/schedule_supply FBO\n"
            "На 22.09.2025, 10:00-11:00\n"
            "Склад: ХОРУГВИНО_РФЦ\n"
            "2625768907 - количество 10, 1 коробка, по 10 шт\n"
            "(Drop-off берём из окружения: OZON_DROP_OFF_ID или OZON_DROP_OFF_NAME)"
            + hint_supply
        )
        return

    # 1) Supply (из шаблона)
    payload, supply_name, supply_id = _extract_supply_from_text_block(payload0)

    # 2) Нормализация
    payload = _normalize_basic(payload)

    # 3) Заголовок и позиции
    header, item_lines_all = _split_header_and_items(payload)
    if not header:
        await message.answer("Не найден заголовок с датой/окном (строка 'На <дата>, <окно>').")
        return
    if not item_lines_all:
        await message.answer("Не найдены строки позиций (SKU).")
        return

    # 3.1) Отфильтруем служебные строки: глобальный склад и контакт (телефон)
    item_lines: List[str] = []
    for ln in item_lines_all:
        ln_stripped = ln.strip()
        if not ln_stripped:
            continue
        if WAREHOUSE_LINE_RE.match(ln_stripped):
            continue
        if _is_contact_line(ln_stripped):
            # не позиция, а строка контакта
            continue
        item_lines.append(ln_stripped)

    if not item_lines:
        await message.answer("Не найдены строки позиций (после удаления служебных строк).")
        return

    # 4) Разбор позиций
    parsed_items: List[Dict[str, Any]] = []
    for ln in item_lines:
        parsed_items.append(_parse_item_line_freeform(ln))

    # 5) Хвост для канонического payload — имя склада приёмки
    wh_tail: Optional[str] = supply_name or None
    if not supply_name and supply_id:
        wh_tail = _resolve_name_by_id_generic(supply_id, available_keys, resolve_warehouse_id)

    # 6) Соберём канонический payload под ваш парсер sw
    candidate_payload = _construct_payload(header, parsed_items, wh_tail)
    if not candidate_payload:
        await message.answer("Не хватает данных в строках позиций. Укажи SKU, количество, коробки и 'по N шт'.")
        return

    # 7) Пред‑парсинг supply_watch (диагностика и автопочинка)
    try:
        sw.parse_template(candidate_payload)
    except Exception:
        logging.error("Pre-parse failed on canonical payload:\n%s", candidate_payload)
        # Попробуем несколько безопасных вариантов автопочинки
        alts = []
        # корректная форма для 1 коробки
        alts.append(re.sub(r",\s*1\s+коробки,", ", 1 коробка,", candidate_payload, flags=re.IGNORECASE))
        # замена длинного тире на дефис (если где-то остался)
        alts.append(candidate_payload.replace(" — ", " - ").replace("—", "-"))
        # перестрахуемся: убрать хвост склада, если глобальный «Склад: ...» был и парсер его увидит
        if supply_name:
            alts.append(re.sub(r",\s*" + re.escape(supply_name) + r"\s*$", "", candidate_payload))

        fixed = None
        for gv in alts:
            try:
                sw.parse_template(gv)
                fixed = gv
                break
            except Exception:
                continue
        if not fixed:
            await message.answer("Не удалось распарсить строки позиций. Пришли 2–3 реальные строки — подгоню правила.")
            return
        candidate_payload = fixed

    # 8) drop-off из ENV c валидацией через API
    env_drop_name, env_drop_id = _env_dropoff()

    # 9) Создаём задачи
    try:
        created = sw.create_tasks_from_template(candidate_payload, mode, chat_id)

        # Проставим склады
        for t in created or []:
            # supply
            if supply_id:
                t["warehouse_id"] = int(supply_id)
                t["chosen_warehouse_id"] = int(supply_id)
            elif supply_name:
                t["warehouse"] = supply_name

            # drop-off — из ENV (проверенный)
            if env_drop_id:
                t["drop_off_warehouse_id"] = int(env_drop_id)
                t["drop_off_point_warehouse_id"] = int(env_drop_id)
            elif env_drop_name:
                t["drop_off"] = env_drop_name
                t["last_error"] = (t.get("last_error") or "") + " | drop-off id не определён по имени; проверь OZON_DROP_OFF_NAME/OZON_DROP_OFF_ID."

        # Маппинг supply-name -> id (если id не были заданы)
        _apply_mapping_to_tasks(created)

        # Логи (ВАЖНО: не подставляем supply_id — это warehouse_id, оставляем как есть)
        for t in created:
            logging.info(
                "Created task id=%s warehouse_id=%s drop_off_id=%s window=%s %s-%s",
                t.get("id"),
                t.get("warehouse_id") or t.get("chosen_warehouse_id"),
                t.get("drop_off_warehouse_id") or t.get("drop_off_point_warehouse_id"),
                t.get("date"),
                str(t.get("desired_from_iso"))[-14:-9],
                str(t.get("desired_to_iso"))[-14:-9],
            )

    except Exception as e:
        logging.exception("schedule_supply parsing/mapping error")
        await message.answer(f"Ошибка: {e}")
        return

    # Ответ пользователю
    lines = []
    for t in created:
        sku = (t.get("sku_list") or [{}])[0].get("sku", "-")
        wnd = f"{t.get('date')} {t.get('desired_from_iso','')[-14:-9]}-{t.get('desired_to_iso','')[-14:-9]}"
        sup_wid = t.get("warehouse_id") or t.get("chosen_warehouse_id") or "-"
        drop_wid = t.get("drop_off_warehouse_id") or t.get("drop_off_point_warehouse_id") or "-"
        err = t.get("last_error")
        suffix = f" [err: {err}]" if err else ""
        lines.append(f"- {sw.short(t['id'])}: {sku} -> {wnd} supply={sup_wid} drop_off={drop_wid}{suffix}")

    await message.answer(f"Создано задач: {len(created)}.\n" + "\n".join(lines))


async def cmd_list(message: Message):
    tasks = sw.list_tasks()
    if not tasks:
        await message.answer("Нет активных задач поставки.")
        return
    lines = []
    for t in tasks:
        sku = t.get("sku_list", [{}])[0].get("sku", "-")
        status = t.get("status", "-")
        sup_wid = t.get("warehouse_id") or t.get("chosen_warehouse_id")
        drop_wid = t.get("drop_off_warehouse_id") or t.get("drop_off_point_warehouse_id")
        wnd = f"{t.get('date')} {t.get('desired_from_iso','')[-14:-9]}-{t.get('desired_to_iso','')[-14:-9]}"
        err = t.get("last_error", "")
        lines.append(f"[{sw.short(t['id'])}] {status} {sku} supply={sup_wid} drop_off={drop_wid} окно={wnd}" + (f" err={err}" if err else ""))
    await message.answer("Задачи:\n" + "\n".join(lines[:100]))


async def cmd_info(message: Message):
    arg = _extract_arg(message)
    if not arg:
        await message.answer("Укажи SKU или id: /supply_info <sku|id>")
        return
    t = _find_task_by_id_or_sku(arg)
    if not t:
        await message.answer("Задача не найдена.")
        return
    await message.answer(
        "Инфо:\n"
        f"id={t.get('id')}\n"
        f"status={t.get('status')}\n"
        f"mode={t.get('mode')}\n"
        f"date={t.get('date')} from={t.get('desired_from_iso')} to={t.get('desired_to_iso')}\n"
        f"sku={(t.get('sku_list') or ['-'])[0]}\n"
        f"supply_id={t.get('warehouse_id')} chosen_supply_id={t.get('chosen_warehouse_id')}\n"
        f"drop_off_warehouse_id={t.get('drop_off_warehouse_id')} drop_off_point_warehouse_id={t.get('drop_off_point_warehouse_id')}\n"
        f"supply_id/ord/supply_id? supply_id={t.get('supply_id')} order_id={t.get('order_id')} draft_id={t.get('draft_id')}\n"
        f"cargo_ids={t.get('cargo_ids')} last_error={t.get('last_error','')}"
    )


async def cmd_cancel(message: Message):
    arg = _extract_arg(message)
    t = _find_task_by_id_or_sku(arg)
    if not t:
        await message.answer("Задача не найдена.")
        return
    ok = sw.cancel_task(t["id"])
    await message.answer("Отменена." if ok else "Не удалось отменить.")


async def cmd_retry(message: Message):
    arg = _extract_arg(message)
    t = _find_task_by_id_or_sku(arg)
    if not t:
        await message.answer("Задача не найдена.")
        return
    ok, msg = sw.retry_task(t["id"])
    await message.answer("OK" if ok else f"Ошибка: {msg}")


async def cmd_purge(message: Message):
    n = sw.purge_tasks()
    await message.answer(f"Удалено завершённых/старых задач: {n}")


async def _purge_all_impl() -> Dict[str, int]:
    stats = {"total_before": 0, "canceled": 0, "cancel_fail": 0, "purged": 0, "remaining": 0}
    tasks = sw.list_tasks() or []
    stats["total_before"] = len(tasks)
    for t in tasks:
        try:
            if sw.cancel_task(t["id"]):
                stats["canceled"] += 1
            else:
                stats["cancel_fail"] += 1
        except Exception:
            stats["cancel_fail"] += 1

    for _ in range(6):
        try:
            n = sw.purge_tasks() or 0
            stats["purged"] += n
            if n == 0:
                left = sw.list_tasks() or []
                stats["remaining"] = len(left)
                if not left:
                    break
        except Exception:
            break
    left = sw.list_tasks() or []
    stats["remaining"] = len(left)
    return stats


async def cmd_purge_all_supplies(message: Message):
    text = (message.text or "").strip().upper()
    confirm = (" YES" in text) or (" ДА" in text)
    if not confirm and message.reply_to_message:
        rt = (message.reply_to_message.text or message.reply_to_message.caption or "").strip().upper()
        confirm = rt in ("YES", "ДА")

    if not confirm:
        await message.answer("Это удалит ВСЕ задачи безвозвратно. Для подтверждения пришли: /purge_all_supplies YES\n"
                             "Или используй без подтверждения: /purge_all_supplies_now")
        return

    stats = await _purge_all_impl()
    await message.answer(
        "Полная очистка завершена.\n"
        f"Всего было: {stats['total_before']}\n"
        f"Отменено: {stats['canceled']} (ошибок отмены: {stats['cancel_fail']})\n"
        f"Удалено: {stats['purged']}\n"
        f"Осталось: {stats['remaining']}"
    )


async def cmd_purge_all_supplies_now(message: Message):
    stats = await _purge_all_impl()
    await message.answer(
        "Полная очистка (без подтверждения) завершена.\n"
        f"Всего было: {stats['total_before']}\n"
        f"Отменено: {stats['canceled']} (ошибок отмены: {stats['cancel_fail']})\n"
        f"Удалено: {stats['purged']}\n"
        f"Осталось: {stats['remaining']}"
    )


async def cmd_tick(message: Message):
    async def _notify_text(chat_id, text):
        await message.bot.send_message(chat_id, text)

    async def _notify_file(chat_id, path, caption):
        await message.bot.send_document(chat_id, document=FSInputFile(path), caption=caption)

    await _remap_all_tasks()
    await sw.process_tasks(_notify_text, _notify_file)
    await message.answer("Тик выполнен.")


# =========================== Diagnostics Middlewares ===========================

class _PingMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        try:
            if isinstance(event, Message):
                t = event.text or ""
                if t.startswith("/ping"):
                    logging.info("PING via outer middleware chat=%s text=%r", getattr(event.chat, "id", None), t)
                    await event.answer("pong")
                    return
        except Exception as e:
            logging.exception("Ping outer middleware error: %s", e)
        return await handler(event, data)


class _LogAllMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        try:
            if isinstance(event, Message):
                logging.info(
                    "MW INCOMING: chat=%s from=%s %s text=%r",
                    getattr(event.chat, "id", None),
                    getattr(event.from_user, "id", None),
                    getattr(event.from_user, "username", None),
                    getattr(event, "text", None),
                )
        except Exception as e:
            logging.exception("LogAll middleware error: %s", e)
        return await handler(event, data)


# =========================== Setup ===========================

def setup_supply_handlers(bot, dp, scheduler=None):
    dp.message.register(cmd_schedule_supply, Command("schedule_supply"))
    dp.message.register(cmd_list,            Command("list_supplies"))
    dp.message.register(cmd_info,            Command("supply_info"))
    dp.message.register(cmd_cancel,          Command("cancel_supply"))
    dp.message.register(cmd_retry,           Command("retry_supply"))
    dp.message.register(cmd_purge,           Command("purge_supplies"))
    dp.message.register(cmd_purge_all_supplies,     Command("purge_all_supplies"))
    dp.message.register(cmd_purge_all_supplies_now, Command("purge_all_supplies_now"))
    dp.message.register(cmd_tick,            Command("supply_tick_job"))

    async def _ping(m: Message):
        logging.info(
            "PING from %s %s chat=%s: %r",
            getattr(m.from_user, "id", None),
            getattr(m.from_user, "username", None),
            getattr(m.chat, "id", None),
            getattr(m, "text", None),
        )
        await m.answer("pong")
    dp.message.register(_ping, Command("ping"))

    async def _log_any(m: Message):
        logging.info(
            "INCOMING: chat=%s from=%s %s text=%r",
            getattr(m.chat, "id", None),
            getattr(m.from_user, "id", None),
            getattr(m.from_user, "username", None),
            getattr(m, "text", None),
        )
    dp.message.register(_log_any, F.text)

    dp.update.outer_middleware(_PingMiddleware())
    dp.update.outer_middleware(_LogAllMiddleware())

    if scheduler:
        async def _notify_text(chat_id, text):
            await bot.send_message(chat_id, text)

        async def _notify_file(chat_id, path, caption):
            await bot.send_document(chat_id, document=FSInputFile(path), caption=caption)

        async def _process_tasks_job():
            t0 = time.monotonic()
            logging.info("process_tasks: START")
            try:
                await _remap_all_tasks()
                await sw.process_tasks(_notify_text, _notify_file)
            except Exception:
                logging.exception("process_tasks: UNHANDLED ERROR")
            finally:
                dt = time.monotonic() - t0
                logging.info("process_tasks: END duration=%.2fs", dt)

        interval_seconds = int(os.getenv("SUPPLY_JOB_INTERVAL", str(getattr(sw, "OPERATION_POLL_INTERVAL_SECONDS", 45))))
        try:
            from apscheduler.triggers.interval import IntervalTrigger
            scheduler.add_job(
                _process_tasks_job,
                trigger=IntervalTrigger(seconds=interval_seconds),
                id="process_tasks",
                replace_existing=True,
                coalesce=True,
                max_instances=1,
                misfire_grace_time=120,
            )
        except Exception:
            scheduler.add_job(
                _process_tasks_job,
                "interval",
                seconds=interval_seconds,
                id="process_tasks",
                replace_existing=True,
                coalesce=True,
                max_instances=1,
                misfire_grace_time=120,
            )

    logging.info("supply_integration: handlers registered")