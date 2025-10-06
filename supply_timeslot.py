from __future__ import annotations
from typing import Dict, Any, Optional, Tuple, List
import logging
from supply_state import SupplyStatus, set_failed, record_event

logger = logging.getLogger(__name__)

MAX_TIME_SLOT_SET_ATTEMPTS = 8
MAX_404_BEFORE_FATAL = 2


def extract_slot_choice(timeslot_json: Dict[str, Any], desired_from: Optional[str], desired_to: Optional[str]) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    slots: List[Dict[str, Any]] = []
    drops = timeslot_json.get("drop_off_warehouse_timeslots") or []
    for drop in drops:
        for day in drop.get("days", []):
            for ts in day.get("timeslots", []):
                slots.append(ts)
    if not slots:
        return None, None

    if desired_from and desired_to:
        for ts in slots:
            fr = ts.get("from_in_timezone") or ts.get("from") or ts.get("from_utc")
            to = ts.get("to_in_timezone") or ts.get("to") or ts.get("to_utc")
            if fr == desired_from and to == desired_to:
                slot_id = ts.get("id") or ts.get("slot_id") or ts.get("value_id") or ts.get("timeslot_id")
                return slot_id, ts

    first = slots[0]
    slot_id = first.get("id") or first.get("slot_id") or first.get("value_id") or first.get("timeslot_id")
    return slot_id, first


async def api_set_timeslot_single(api, order_id: int, slot_from: str, slot_to: str, slot_id: Optional[str]) -> Tuple[bool, str | None, int]:
    endpoints = [
        "/v1/supply-order/appointment/set",
        "/v2/supply-order/appointment/set",
    ]
    body_variants = []
    def base(from_key: str, to_key: str):
        b = {"order_id": order_id, "timeslot": {from_key: slot_from, to_key: slot_to}}
        if slot_id:
            b["timeslot"]["id"] = slot_id
        return b
    body_variants.append(base("from", "to"))
    body_variants.append(base("from_in_timezone", "to_in_timezone"))

    last_err = None
    last_status = 0
    for ep in endpoints:
        for body in body_variants:
            ok, status, data, err = await api.json_request(
                "POST",
                ep,
                json=body,
                ignore_statuses=[404, 400, 422, 429],
            )
            if ok and status == 200:
                return True, None, status
            if status == 404:
                last_err = f"endpoint_404:{ep}"
                last_status = status
                continue
            if status in (400, 422):
                return False, f"validation:{ep}:{data}", status
            if status == 429:
                return False, f"rate_limit:{ep}", status
            last_err = f"{status}:{ep}:{data}"
            last_status = status
    return False, last_err, last_status or 500


async def ensure_timeslot(task: Dict[str, Any], api, notify_text) -> bool:
    if task.get("order_timeslot_set_ok"):
        return True

    attempts = int(task.get("order_timeslot_set_attempts") or 0)
    if attempts >= MAX_TIME_SLOT_SET_ATTEMPTS:
        set_failed(task, "timeslot_set_exceeded_attempts")
        return False

    slot_from = task.get("from_in_timezone") or task.get("desired_from_iso")
    slot_to = task.get("to_in_timezone") or task.get("desired_to_iso")
    if not (slot_from and slot_to):
        task["last_error"] = "timeslot_missing_from_to"
        return False

    ok, err, status = await api_set_timeslot_single(
        api,
        int(task["order_id"]),
        slot_from,
        slot_to,
        task.get("slot_id")
    )
    task["order_timeslot_set_attempts"] = attempts + 1

    if ok:
        task["order_timeslot_set_ok"] = True
        task["last_error"] = None
        record_event(task, "TIMESLOT_SET", {"order_id": task["order_id"]})
        try:
            await notify_text(task.get("chat_id") or 0, f"🟦 [{task['id'][:6]}] Тайм‑слот установлен.")
        except Exception:
            pass
        return True
    else:
        task["last_error"] = f"timeslot_set:{err or status}"
        if err and "endpoint_404" in err:
            c404 = int(task.get("timeslot_404_count") or 0) + 1
            task["timeslot_404_count"] = c404
            if c404 >= MAX_404_BEFORE_FATAL:
                set_failed(task, "timeslot_endpoint_unavailable")
                try:
                    await notify_text(task.get("chat_id") or 0, f"🟥 [{task['id'][:6]}] Эндпоинт тайм‑слота недоступен.")
                except Exception:
                    pass
                return False
        record_event(task, "TIMESLOT_SET_FAIL", {"error": task["last_error"]})
        return False