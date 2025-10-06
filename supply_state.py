from __future__ import annotations
from enum import Enum
from typing import Dict, Any, List, Optional
import time


class SupplyStatus(str, Enum):
    WAIT_WINDOW = "WAIT_WINDOW"
    DRAFT_CREATING = "DRAFT_CREATING"
    POLL_DRAFT = "POLL_DRAFT"
    TIMESLOT_SEARCH = "TIMESLOT_SEARCH"
    TIMESLOT_SETTING = "TIMESLOT_SETTING"
    SUPPLY_CREATING = "SUPPLY_CREATING"
    POLL_SUPPLY = "POLL_SUPPLY"
    SUPPLY_ORDER_FETCH = "SUPPLY_ORDER_FETCH"
    CARGO_PREP = "CARGO_PREP"
    CARGO_CREATING = "CARGO_CREATING"
    POLL_CARGO = "POLL_CARGO"
    LABELS_CREATING = "LABELS_CREATING"
    POLL_LABELS = "POLL_LABELS"
    LABELS_READY = "LABELS_READY"
    DONE = "DONE"
    FAILED = "FAILED"
    CANCELED = "CANCELED"


FINAL_STATUSES = {
    SupplyStatus.DONE,
    SupplyStatus.FAILED,
    SupplyStatus.CANCELED,
}

RETRY_CAPABLE = {
    SupplyStatus.DRAFT_CREATING,
    SupplyStatus.POLL_DRAFT,
    SupplyStatus.TIMESLOT_SEARCH,
    SupplyStatus.TIMESLOT_SETTING,
    SupplyStatus.SUPPLY_CREATING,
    SupplyStatus.POLL_SUPPLY,
    SupplyStatus.SUPPLY_ORDER_FETCH,
    SupplyStatus.CARGO_PREP,
    SupplyStatus.CARGO_CREATING,
    SupplyStatus.POLL_CARGO,
    SupplyStatus.LABELS_CREATING,
    SupplyStatus.POLL_LABELS,
}


def now_ts() -> int:
    return int(time.time())


def can_retry(status: SupplyStatus) -> bool:
    return status in RETRY_CAPABLE


def is_terminal(status: SupplyStatus) -> bool:
    return status in FINAL_STATUSES


def set_failed(task: Dict[str, Any], error_msg: str):
    task["status"] = SupplyStatus.FAILED.value
    task["last_error"] = error_msg
    task["failed_ts"] = now_ts()


def record_event(task: Dict[str, Any], event: str, extra: Optional[Dict[str, Any]] = None):
    hist: List[Dict[str, Any]] = task.setdefault("history", [])
    item = {"ts": now_ts(), "event": event}
    if extra:
        item.update(extra)
    hist.append(item)
    if len(hist) > 500:
        del hist[:-500]