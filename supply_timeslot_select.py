from __future__ import annotations
from typing import Dict, Any, Optional, Tuple, List
import logging

logger = logging.getLogger(__name__)

def pick_timeslot(data: Dict[str, Any],
                  desired_from: Optional[str],
                  desired_to: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Возвращает (slot_id, from_iso, to_iso).
    Если desired_* указаны и точно совпадают — берём их.
    Иначе — первый доступный слот.
    """
    drops = data.get("drop_off_warehouse_timeslots") or []
    slots: List[Dict[str, Any]] = []
    for drop in drops:
        for day in drop.get("days", []):
            for ts in day.get("timeslots", []):
                slots.append(ts)

    if not slots:
        return None, None, None

    def ts_from(ts): return ts.get("from_in_timezone") or ts.get("from") or ts.get("from_utc")
    def ts_to(ts): return ts.get("to_in_timezone") or ts.get("to") or ts.get("to_utc")

    # 1) точное совпадение желаемого окна
    if desired_from and desired_to:
        for ts in slots:
            if ts_from(ts) == desired_from and ts_to(ts) == desired_to:
                slot_id = ts.get("id") or ts.get("slot_id") or ts.get("value_id") or ts.get("timeslot_id")
                return slot_id, ts_from(ts), ts_to(ts)

    # 2) fallback — первый
    first = slots[0]
    slot_id = first.get("id") or first.get("slot_id") or first.get("value_id") or first.get("timeslot_id")
    return slot_id, ts_from(first), ts_to(first)