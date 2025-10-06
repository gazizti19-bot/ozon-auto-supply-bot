from __future__ import annotations
import logging
from typing import Dict, Any, Callable
from supply_state import SupplyStatus, record_event, set_failed
from supply_timeslot import ensure_timeslot, extract_slot_choice

logger = logging.getLogger(__name__)


class SupplyOrchestrator:
    """
    Оркестратор промежуточных статусов TIMESLOT_SEARCH / TIMESLOT_SETTING.
    Остальные шаги выполняются в supply_worker (черновик, supply, cargo, labels).
    """

    def __init__(
        self,
        api_factory: Callable[[], Any],
        notify_text: Callable[[int, str], Any],
        update_task: Callable[[Dict[str, Any]], None],
    ):
        self.api_factory = api_factory
        self.notify_text = notify_text
        self.update_task = update_task

    async def step(self, task: Dict[str, Any]) -> None:
        status = SupplyStatus(task.get("status"))
        if status == SupplyStatus.TIMESLOT_SEARCH:
            await self._handle_timeslot_search(task)
        elif status == SupplyStatus.TIMESLOT_SETTING:
            await self._handle_timeslot_setting(task)

    async def _handle_timeslot_search(self, task: Dict[str, Any]):
        ts_data = task.get("timeslot_info_raw")
        desired_from = task.get("desired_from_iso")
        desired_to = task.get("desired_to_iso")

        if not ts_data:
            task["last_error"] = "no_timeslot_info_data"
            record_event(task, "NO_TIMESLOT_INFO")
            return

        slot_id, slot_obj = extract_slot_choice(ts_data, desired_from, desired_to)
        if not slot_obj:
            task["last_error"] = "no_slots_found"
            record_event(task, "NO_SLOTS")
            return

        task["slot_id"] = slot_id
        fr = slot_obj.get("from_in_timezone") or slot_obj.get("from")
        to = slot_obj.get("to_in_timezone") or slot_obj.get("to")
        if fr:
            task["from_in_timezone"] = fr
        if to:
            task["to_in_timezone"] = to

        record_event(task, "TIMESLOT_SELECTED", {"slot_id": slot_id})
        task["status"] = SupplyStatus.TIMESLOT_SETTING.value
        self.update_task(task)

    async def _handle_timeslot_setting(self, task: Dict[str, Any]):
        api = self.api_factory()
        ok = await ensure_timeslot(task, api, self.notify_text)
        if ok:
            record_event(task, "TIMESLOT_CONFIRMED", {})
            task["status"] = SupplyStatus.SUPPLY_ORDER_FETCH.value
            self.update_task(task)
        else:
            if task.get("status") == SupplyStatus.FAILED.value:
                return