from __future__ import annotations
import re
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple, Optional
import zoneinfo

from supply_window import roll_window_forward

# 1) Линия даты / окна
DATE_LINE_RE = re.compile(
    r"^\s*(?:На|на)\s+(\d{2}\.\d{2}\.\d{4}),\s*(\d{2}:\d{2})\s*-\s*(\d{2}:\d{2})\s*$"
)

# 2) Линия склада отдельная
WAREHOUSE_LINE_RE = re.compile(
    r"^\s*(?:Склад|СКЛАД)\s*[:\-]\s*(.+?)\s*$"
)

# 3) SKU строка (первая группа — sku). После текста могут быть дополнительные куски, включая название склада
SKU_LINE_RE = re.compile(r"^\s*(\d{6,})\s*[—\-:]\s*(.+)$")

RE_TOTAL_QTY = re.compile(r"(?:кол-во|количество)\s*[:=]?\s*(\d+)", re.IGNORECASE)
RE_BOXES = re.compile(r"(\d+)\s*коробк", re.IGNORECASE)
RE_PER_BOX = re.compile(r"по\s*(\d+)\s*шт", re.IGNORECASE)

def parse_datetime_window(d: str, t_from: str, t_to: str, tz_name: str):
    day = datetime.strptime(d, "%d.%m.%Y")
    hf = datetime.strptime(t_from, "%H:%M")
    ht = datetime.strptime(t_to, "%H:%M")
    tz = zoneinfo.ZoneInfo(tz_name)
    start = day.replace(hour=hf.hour, minute=hf.minute, second=0, microsecond=0, tzinfo=tz)
    end = day.replace(hour=ht.hour, minute=ht.minute, second=0, microsecond=0, tzinfo=tz)
    if end <= start:
        # Если теоретически переходит через полночь — здесь можно добавить сутки (пока не делаем)
        pass
    return start, end

def to_utc_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def parse_item_line(line: str) -> Tuple[Optional[Dict[str, Any]], List[str]]:
    errs: List[str] = []
    m = SKU_LINE_RE.match(line)
    if not m:
        return None, ["not_sku_line"]
    sku = m.group(1)
    rest = m.group(2)

    total_qty = None
    boxes = None
    per_box = None

    mt = RE_TOTAL_QTY.search(rest)
    if mt:
        total_qty = int(mt.group(1))

    mb = RE_BOXES.search(rest)
    if mb:
        boxes = int(mb.group(1))

    mpb = RE_PER_BOX.search(rest)
    if mpb:
        per_box = int(mpb.group(1))

    if boxes and per_box:
        proposed_qty = boxes * per_box
    elif total_qty:
        proposed_qty = total_qty
    else:
        proposed_qty = 1

    item = {
        "sku": int(sku),
        "proposed_qty": proposed_qty,
        "parsed_total_qty": total_qty,
        "parsed_boxes": boxes,
        "parsed_per_box": per_box,
        "raw_tail": rest
    }
    return item, errs

def _extract_warehouse_inline(tail: str, warehouse_name_map: Dict[str, int]) -> Optional[str]:
    """
    Пытаемся понять, есть ли в хвосте SKU строки название склада через запятую
    Пример: "количество 10, 1 коробки, по 10 шт, УФА_РФЦ"
    Берём последнюю запятую -> токен -> upper -> смотрим в map
    """
    parts = [p.strip() for p in tail.split(",") if p.strip()]
    if not parts:
        return None
    candidate = parts[-1].upper()
    if candidate in warehouse_name_map:
        return parts[-1]  # возвращаем в оригинальном регистре
    return None

def parse_schedule_payload(
    text: str,
    tz_name: str,
    min_lead_minutes: int,
    max_roll_days: int,
    warehouse_name_map: Dict[str, int],
    allow_no_command: bool = True
) -> Tuple[bool, Dict[str, Any], List[str]]:
    """
    Универсальный парсер:
    - Опциональная первая строка /schedule_supply [TYPE]
    - Линия окна: "На dd.mm.yyyy, HH:MM-HH:MM"
    - Опциональная линия склада "Склад: ..."
    - SKU строки
    - Возможен склад в конце SKU строки после запятой
    """
    lines_raw = text.splitlines()
    lines = [l.rstrip() for l in lines_raw if l.strip()]
    errors: List[str] = []
    if not lines:
        return False, {}, ["empty_input"]

    first_line = lines[0].strip()
    supply_type = None
    has_command = first_line.startswith("/schedule_supply")
    if has_command:
        parts = first_line.split()
        if len(parts) >= 2:
            supply_type = parts[1].strip().upper()
        # Убираем командную строку из дальнейшего анализа
        lines = lines[1:]
    else:
        if not allow_no_command:
            errors.append("missing_command_line")

    date_line = None
    warehouse_line = None
    item_lines: List[str] = []

    # Сканируем оставшиеся:
    for ln in lines:
        if not date_line and DATE_LINE_RE.match(ln):
            date_line = ln
            continue
        if not warehouse_line and WAREHOUSE_LINE_RE.match(ln):
            warehouse_line = ln
            continue
        item_lines.append(ln)

    if not date_line:
        errors.append("missing_date_line")

    desired_from_iso = None
    desired_to_iso = None
    if date_line:
        mdt = DATE_LINE_RE.match(date_line)
        if mdt:
            dstr, t_from, t_to = mdt.groups()
            try:
                start_dt, end_dt = parse_datetime_window(dstr, t_from, t_to, tz_name)
                base_from = to_utc_iso(start_dt)
                base_to = to_utc_iso(end_dt)
                new_from, new_to, rolled, _why = roll_window_forward(
                    base_from, base_to, tz_name, min_lead_minutes, max_roll_days
                )
                desired_from_iso = new_from
                desired_to_iso = new_to
            except Exception:
                errors.append("failed_parse_datetime")
        else:
            errors.append("bad_date_line_format")

    # Отдельная строка склада
    explicit_warehouse_name = None
    explicit_warehouse_id = None
    if warehouse_line:
        mw = WAREHOUSE_LINE_RE.match(warehouse_line)
        if mw:
            explicit_warehouse_name = mw.group(1).strip()
            key = explicit_warehouse_name.upper()
            if key in warehouse_name_map:
                explicit_warehouse_id = warehouse_name_map[key]
        else:
            errors.append("bad_warehouse_line_format")

    sku_items: List[Dict[str, Any]] = []
    inline_warehouse_name: Optional[str] = None
    inline_warehouse_id: Optional[int] = None

    for raw in item_lines:
        it, errs_it = parse_item_line(raw)
        if it:
            # проверим склад “в хвосте”
            wh_inline = _extract_warehouse_inline(it["raw_tail"], warehouse_name_map)
            if wh_inline and not explicit_warehouse_name:  # приоритет явной строки склада
                inline_warehouse_name = wh_inline
                key = wh_inline.upper()
                if key in warehouse_name_map:
                    inline_warehouse_id = warehouse_name_map[key]
            sku_items.append(it)
        else:
            # Если строка не SKU — не критично, просто игнорируем
            pass

    if not sku_items:
        errors.append("no_sku_items_parsed")

    # Выберем фактический склад (приоритет: явный -> inline -> None)
    final_wh_name = explicit_warehouse_name or inline_warehouse_name
    final_wh_id = explicit_warehouse_id or inline_warehouse_id

    ok = not errors
    data = {
        "desired_from_iso": desired_from_iso,
        "desired_to_iso": desired_to_iso,
        "warehouse_name": final_wh_name,
        "warehouse_id_override": final_wh_id,
        "sku_items": sku_items,
        "supply_type": supply_type,
        "has_command": has_command
    }
    return ok, data, errors

def parse_autosupply_text_any(
    text: str,
    tz_name: str,
    min_lead_minutes: int,
    max_roll_days: int,
    warehouse_name_map: Dict[str, int]
) -> Tuple[bool, Dict[str, Any], List[str]]:
    """
    Универсальная точка входа для старых и новых форматов:
    - Пытается парсить как /schedule_supply ...
    - Если не команда — тоже парсим (allow_no_command=True).
    """
    ok, data, errs = parse_schedule_payload(
        text,
        tz_name=tz_name,
        min_lead_minutes=min_lead_minutes,
        max_roll_days=max_roll_days,
        warehouse_name_map=warehouse_name_map,
        allow_no_command=True
    )
    return ok, data, errs