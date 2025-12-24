from __future__ import annotations
import asyncio
import logging
import os
import json
import random
from typing import Dict, Any, Callable, List, Optional, Tuple

from supply_state import SupplyStatus, record_event, set_failed, now_ts
from supply_tasks_repo import SupplyTasksRepo
from ozon_api import OzonApi
from supply_orchestrator import SupplyOrchestrator

logger = logging.getLogger(__name__)

# ------------------- CONFIG / ENV -------------------
MAX_DRAFT_CREATE_ATTEMPTS = int(os.getenv("MAX_DRAFT_CREATE_ATTEMPTS", "14"))
MIN_CREATE_RETRY_SECONDS = int(os.getenv("MIN_CREATE_RETRY_SECONDS", "6"))
ALLOW_EMPTY_DROPOFF = os.getenv("ALLOW_EMPTY_DROPOFF", "1") == "1"
SUPPLY_DRAFT_DEBUG = os.getenv("SUPPLY_DRAFT_DEBUG", "0") == "1"

FAST_STRATEGY_DELAY = int(os.getenv("FAST_STRATEGY_DELAY", "2"))
NORMAL_STRATEGY_DELAY = int(os.getenv("NORMAL_STRATEGY_DELAY", "10"))
GENERIC_RETRY_DELAY = int(os.getenv("DRAFT_GENERIC_RETRY_DELAY", "25"))

RATE_LIMIT_BASE_WAIT = int(os.getenv("DRAFT_RATE_LIMIT_BASE_WAIT", "4"))
RATE_LIMIT_MAX_WAIT = int(os.getenv("DRAFT_RATE_LIMIT_MAX_WAIT", "40"))
GLOBAL_DRAFT_MIN_INTERVAL = float(os.getenv("GLOBAL_DRAFT_MIN_INTERVAL", "1.0"))

ST_DRAFT_CREATING = "DRAFT_CREATING"
ST_RATE_LIMIT = "RATE_LIMIT"
ST_POLL_DRAFT = SupplyStatus.POLL_DRAFT.value

SUPPLY_TYPE_ERROR_MARKER = "supply type is unknown"

# ------------------- HELPERS -------------------
def _short(i: str) -> str:
    return i[:8]

def _is_terminal(status: str) -> bool:
    return status in {
        SupplyStatus.DONE.value,
        SupplyStatus.FAILED.value,
        SupplyStatus.CANCELED.value,
        "DONE", "FAILED", "CANCELED",
    }

def _extract_raw(data: Any, err: Any, raw_text: str) -> str:
    if raw_text:
        return raw_text[:6000]
    if isinstance(data, dict):
        try:
            return json.dumps(data, ensure_ascii=False)[:6000]
        except Exception:
            return str(data)[:6000]
    if data:
        return str(data)[:6000]
    if err:
        return str(err)[:6000]
    return ""

def _extract_op(data: Any) -> Optional[str]:
    if isinstance(data, dict):
        return data.get("operation_id") or (data.get("result") or {}).get("operation_id")
    return None

def _analyze_hint(txt: str) -> str:
    low = txt.lower()
    hints = []
    if SUPPLY_TYPE_ERROR_MARKER in low:
        hints.append("Нужно корректно передать тип: supply_type|type = direct или xdock (или оба).")
    if "sku" in low and ("unknown" in low or "invalid" in low or "not" in low):
        hints.append("SKU не распознан — проверь, что это внутренний sku.")
    if "drop_off" in low:
        hints.append("Проверь drop_off_point_warehouse_id либо убери его.")
    if "permission" in low or "forbidden" in low or "denied" in low:
        hints.append("Права аккаунта не разрешают этот тип поставки.")
    if "quantity" in low or "items" in low:
        hints.append("Проверь items / quantity > 0.")
    if not hints:
        hints.append("Смотри raw текст.")
    return " | ".join(hints)[:360]

def _build_items(task: Dict[str, Any]) -> List[Dict[str, int]]:
    res = []
    for it in task.get("sku_items") or []:
        if not isinstance(it, dict):
            continue
        if "sku" not in it:
            continue
        try:
            sku = int(it["sku"])
        except Exception:
            continue
        qty = None
        for k in ("quantity", "qty", "proposed_qty"):
            if it.get(k) is not None:
                try:
                    qty = int(it[k])
                    break
                except Exception:
                    pass
        if qty is None and it.get("boxes") and it.get("per"):
            try:
                qty = int(it["boxes"]) * int(it["per"])
            except Exception:
                qty = None
        if not qty or qty <= 0:
            continue
        res.append({"sku": sku, "quantity": qty})
    return res

def _strategy_matrix(has_drop: bool) -> List[Dict[str, Any]]:
    base = [
        {"name": "st1_supply_type_direct", "supply_type": "direct"},
        {"name": "st2_supply_type_xdock", "supply_type": "xdock"},
        {"name": "st3_type_direct", "type": "direct"},
        {"name": "st4_type_xdock", "type": "xdock"},
        {"name": "st5_both_direct", "type": "direct", "supply_type": "direct"},
        {"name": "st6_both_xdock", "type": "xdock", "supply_type": "xdock"},
        {"name": "st7_old_enum_fbo", "type": "CREATE_TYPE_FBO"},
        {"name": "st8_old_enum_crossdock", "type": "CREATE_TYPE_CROSSDOCK"},
        {"name": "st9_old_both_fbo_direct", "type": "CREATE_TYPE_FBO", "supply_type": "direct"},
        {"name": "st10_old_both_cross_xdock", "type": "CREATE_TYPE_CROSSDOCK", "supply_type": "xdock"},
    ]
    # Для xdock стратегий добавим вариант с drop_off отдельно (если нужен):
    if has_drop:
        ext: List[Dict[str, Any]] = []
        for s in base:
            if "xdock" in json.dumps(s):
                sc = dict(s)
                sc["name"] += "_drop"
                sc["use_drop"] = True
                ext.append(sc)
        base.extend(ext)
    # remove duplicates by name
    seen = set()
    filtered = []
    for s in base:
        if s["name"] in seen:
            continue
        seen.add(s["name"])
        filtered.append(s)
    return filtered

def _ensure_strategies(task: Dict[str, Any]):
    if "draft_strategies" in task:
        return
    has_drop = bool(task.get("drop_off_warehouse_id") or task.get("drop_off_point_warehouse_id"))
    task["draft_strategies"] = _strategy_matrix(has_drop)
    task["draft_strategy_index"] = 0
    task["draft_strategies_tried"] = []

# ------------------- WORKER -------------------
class SupplyWorker:
    def __init__(
        self,
        client_id: str,
        api_key: str,
        repo: SupplyTasksRepo,
        notify_text: Callable[[int, str], Any] = lambda chat_id, text: None,
        poll_interval: int = 45,
        api_timeout: int = 25,
    ):
        self.repo = repo
        self.client_id = client_id
        self.api_key = api_key
        self.poll_interval = poll_interval
        self.api_timeout = api_timeout
        self._stop = False
        self._task_locks: Dict[str, asyncio.Lock] = {}
        self.notify_text = notify_text
        self.orchestrator = SupplyOrchestrator(
            api_factory=self._api_factory,
            notify_text=self._safe_notify,
            update_task=self._update_task,
        )
        self._global_draft_next = 0.0
        self._rate_fail_counter = 0

    # infra
    def _api_factory(self):
        return OzonApi(self.client_id, self.api_key, timeout=self.api_timeout)

    def _update_task(self, t: Dict[str, Any]):
        self.repo.upsert(t)

    async def _safe_notify(self, chat_id: int, text: str):
        try:
            r = self.notify_text(chat_id, text)
            if asyncio.iscoroutine(r):
                await r
        except Exception:
            logger.exception("notify failed")

    # loop
    async def start(self):
        logger.info("SupplyWorker started interval=%s", self.poll_interval)
        while not self._stop:
            try:
                await self._tick()
            except Exception:
                logger.exception("tick failed")
            await asyncio.sleep(self.poll_interval)

    def stop(self):
        self._stop = True

    async def _tick(self):
        tasks = self.repo.list()
        tasks.sort(key=lambda t: int(t.get("next_attempt_ts") or 0))
        for t in tasks:
            if _is_terminal(t.get("status", "")):
                continue
            lock = self._task_locks.setdefault(t["id"], asyncio.Lock())
            if lock.locked():
                continue
            async with lock:
                try:
                    await self._process_task(t)
                except Exception:
                    logger.exception("process_task failed id=%s", t["id"])

    async def _process_task(self, task: Dict[str, Any]):
        now = now_ts()
        if (nxt := int(task.get("next_attempt_ts") or 0)) and now < nxt:
            return

        rat = int(task.get("retry_after_ts") or 0)
        if rat and now < rat:
            if task.get("status") != ST_RATE_LIMIT:
                task["status"] = ST_RATE_LIMIT
                self._update_task(task)
            return
        elif rat and now >= rat and task.get("status") == ST_RATE_LIMIT:
            task["status"] = ST_DRAFT_CREATING
            task.pop("retry_after_ts", None)
            task["next_attempt_ts"] = now
            self._update_task(task)

        st = task.get("status") or ""
        if not st:
            task["status"] = ST_DRAFT_CREATING
            self._update_task(task)
            st = ST_DRAFT_CREATING

        if st == ST_DRAFT_CREATING or st == SupplyStatus.DRAFT_CREATING.value:
            await self._draft_stage(task)
            return
        if st in ("POLL_DRAFT", "DRAFT_INFO_POLL", SupplyStatus.POLL_DRAFT.value):
            await self._poll_draft(task)
            return

        try:
            est = SupplyStatus(st)
        except Exception:
            est = None

        if est in (SupplyStatus.TIMESLOT_SEARCH, SupplyStatus.TIMESLOT_SETTING):
            await self.orchestrator.step(task)
            self._update_task(task)
            return
        if est == SupplyStatus.SUPPLY_CREATING:
            await self._supply_create(task); return
        if est == SupplyStatus.POLL_SUPPLY:
            await self._poll_supply_create(task); return
        if est == SupplyStatus.SUPPLY_ORDER_FETCH:
            await self._order_fetch(task); return
        if est == SupplyStatus.CARGO_PREP:
            await self._cargo_prep(task); return
        if est == SupplyStatus.CARGO_CREATING:
            await self._cargo_create(task); return
        if est == SupplyStatus.POLL_CARGO:
            await self._poll_cargo(task); return
        if est == SupplyStatus.LABELS_CREATING:
            await self._labels_create(task); return
        if est == SupplyStatus.POLL_LABELS:
            await self._poll_labels(task); return
        if est == SupplyStatus.LABELS_READY:
            task["status"] = SupplyStatus.DONE.value
            record_event(task, "DONE")
            self._update_task(task)

    # ----- Draft Stage -----
    async def _draft_stage(self, task: Dict[str, Any]):
        _ensure_strategies(task)
        attempts = int(task.get("create_attempts") or 0)
        if attempts >= MAX_DRAFT_CREATE_ATTEMPTS:
            return

        now = now_ts()
        if now < self._global_draft_next:
            task["next_attempt_ts"] = int(self._global_draft_next)
            self._update_task(task)
            return

        last_upd = int(task.get("updated_ts") or 0)
        if attempts > 0 and now - last_upd < MIN_CREATE_RETRY_SECONDS:
            return

        items = _build_items(task)
        if not items:
            set_failed(task, "draft_items_empty")
            self._update_task(task)
            return

        strategies: List[Dict[str, Any]] = task["draft_strategies"]
        idx = int(task.get("draft_strategy_index") or 0)
        if idx >= len(strategies):
            set_failed(task, "draft_strategies_exhausted")
            self._update_task(task)
            return
        strat = strategies[idx]
        tried = task.get("draft_strategies_tried") or []
        if not tried or tried[-1] != strat["name"]:
            tried.append(strat["name"])
            task["draft_strategies_tried"] = tried

        drop_id = task.get("drop_off_warehouse_id") or task.get("drop_off_point_warehouse_id")

        # Сбор payload
        payload = {"items": items}
        for k in ("type", "supply_type"):
            if k in strat:
                payload[k] = strat[k]
        if strat.get("use_drop") and drop_id:
            try:
                payload["drop_off_point_warehouse_id"] = int(drop_id)
            except Exception:
                pass

        attempts += 1
        task["create_attempts"] = attempts
        task["updated_ts"] = now
        task["draft_payload_snapshot"] = payload
        task["status"] = ST_DRAFT_CREATING
        self._update_task(task)

        if attempts == 1 or SUPPLY_DRAFT_DEBUG:
            logger.info(
                "draft_create attempt=%s strategy=%s task=%s payload=%s",
                attempts, strat["name"], _short(task["id"]),
                json.dumps(payload, ensure_ascii=False),
            )

        self._global_draft_next = now + GLOBAL_DRAFT_MIN_INTERVAL

        api = self._api_factory()
        ok, status_code, data, err, raw_text = await api.json_request(
            "POST", "/v1/draft/create", json=payload
        )
        raw = _extract_raw(data, err, raw_text)

        # Запишем историю каждой попытки
        record_event(task, "DRAFT_VAR_TRY", {
            "strategy": strat["name"],
            "status_code": status_code,
            "len_raw": len(raw),
        })

        # ----- 429 -----
        if status_code == 429:
            self._rate_fail_counter += 1
            wait = min(RATE_LIMIT_BASE_WAIT * (2 ** (self._rate_fail_counter - 1)), RATE_LIMIT_MAX_WAIT)
            wait += random.uniform(0, 1.5)
            task["status"] = ST_RATE_LIMIT
            task["last_error"] = f"429_retry_in_{int(wait)}s"
            task["retry_after_ts"] = now_ts() + int(wait)
            task["next_attempt_ts"] = task["retry_after_ts"]
            task["last_error_raw_http"] = raw
            record_event(task, "DRAFT_429", {"wait": int(wait), "strategy": strat["name"]})
            self._update_task(task)
            return
        else:
            self._rate_fail_counter = 0

        # ----- 400 -----
        if status_code == 400:
            task["last_error_raw_http"] = raw
            task["last_error"] = f"HTTP400:{raw[:140]}..."
            task["last_error_hint"] = _analyze_hint(raw)

            # Если это "supply type is unknown" — сразу следующая стратегия с FAST delay
            if SUPPLY_TYPE_ERROR_MARKER in raw.lower():
                self._advance_strategy(task, idx, attempts, fast=True, tag="supply_type_unknown")
            else:
                # другая ошибка -> возможно мы прошли supply type, дальше появится ошибка SKU
                # тогда логично остановиться и не крутить стратегии, если уже получили ИНУЮ ошибку.
                # Но пока даём попробовать ещё 1 стратегию (может нужен другой формат).
                self._advance_strategy(task, idx, attempts, fast=False, tag="draft_400")
            return

        # ----- 409 -----
        if status_code == 409:
            task["last_error_raw_http"] = raw
            task["last_error"] = f"HTTP409:{raw[:140]}..."
            task["last_error_hint"] = _analyze_hint(raw)
            op_id = _extract_op(data)
            if op_id:
                task["draft_create_operation_id"] = op_id
                task["status"] = ST_POLL_DRAFT
                task["last_error"] = ""
                task["winning_strategy"] = strat["name"]
                record_event(task, "DRAFT_CREATE_OK_FROM_409", {"operation_id": op_id, "strategy": strat["name"]})
                self._update_task(task)
                return
            self._advance_strategy(task, idx, attempts, fast=False, tag="draft_409")
            return

        # ----- 401 / 403 -----
        if status_code in (401, 403):
            task["last_error_raw_http"] = raw
            task["last_error"] = f"http_auth:{status_code}"
            task["last_error_hint"] = _analyze_hint(raw)
            record_event(task, "DRAFT_HTTP_AUTH", {"status": status_code})
            set_failed(task, f"auth_or_forbidden:{status_code}")
            self._update_task(task)
            return

        # ----- generic fail -----
        if not ok:
            task["last_error_raw_http"] = raw
            task["last_error"] = f"draft_create_error:{status_code}:{raw[:140]}"
            record_event(task, "DRAFT_CREATE_ERROR", {"strategy": strat["name"], "status": status_code})
            if attempts >= MAX_DRAFT_CREATE_ATTEMPTS:
                set_failed(task, "draft_create_generic_fail")
            else:
                task["next_attempt_ts"] = now_ts() + GENERIC_RETRY_DELAY
            self._update_task(task)
            return

        # ----- success (2xx) -----
        op_id = _extract_op(data)
        if not op_id:
            task["last_error_raw_http"] = raw
            task["last_error"] = "draft_create_no_operation_id"
            record_event(task, "DRAFT_NO_OPERATION_ID", {"strategy": strat["name"]})
            self._advance_strategy(task, idx, attempts, fast=False, tag="draft_no_opid")
            return

        task["draft_create_operation_id"] = op_id
        task["status"] = ST_POLL_DRAFT
        task["last_error"] = ""
        task["winning_strategy"] = strat["name"]
        record_event(task, "DRAFT_CREATE_OK", {"operation_id": op_id, "strategy": strat["name"]})
        self._update_task(task)

    def _advance_strategy(self, task: Dict[str, Any], idx: int, attempts: int, fast: bool, tag: str):
        strategies = task.get("draft_strategies") or []
        next_idx = idx + 1
        if next_idx >= len(strategies) and attempts >= MAX_DRAFT_CREATE_ATTEMPTS:
            set_failed(task, f"{tag}_exhausted")
        else:
            task["draft_strategy_index"] = min(next_idx, len(strategies) - 1)
            delay = FAST_STRATEGY_DELAY if fast else NORMAL_STRATEGY_DELAY
            task["next_attempt_ts"] = now_ts() + delay
            task["status"] = ST_DRAFT_CREATING
        self._update_task(task)

    # ----- POLL DRAFT -----
    async def _poll_draft(self, task: Dict[str, Any]):
        api = self._api_factory()
        op_id = task.get("draft_create_operation_id") or task.get("operation_id")
        if not op_id:
            set_failed(task, "missing_draft_operation_id")
            self._update_task(task)
            return
        body = {"operation_id": op_id}
        ok, status, data, err, raw = await api.json_request("POST", "/v1/draft/create/info", json=body)
        if ok and isinstance(data, dict):
            st = data.get("status")
            if st in ("CALCULATION_STATUS_SUCCESS", "success", "SUCCESS"):
                draft_id = data.get("draft_id") or data.get("id") or (data.get("result") or {}).get("draft_id")
                task["draft_id"] = draft_id
                clusters = data.get("clusters") or (data.get("result") or {}).get("clusters") or []
                chosen = None
                if isinstance(clusters, list):
                    for c in clusters:
                        for wh in (c.get("warehouses") or []):
                            wid = ((wh.get("supply_warehouse") or {}).get("warehouse_id")
                                   or wh.get("warehouse_id"))
                            if wid:
                                chosen = wid
                                break
                        if chosen:
                            break
                if chosen:
                    task["chosen_warehouse_id"] = chosen
                task["status"] = SupplyStatus.TIMESLOT_SEARCH.value
                record_event(task, "DRAFT_READY", {"draft_id": draft_id, "chosen_warehouse_id": chosen})
            elif st in ("CALCULATION_STATUS_IN_PROGRESS", "IN_PROGRESS", "progress"):
                record_event(task, "DRAFT_PROGRESS", {"op": op_id})
            elif st in ("CALCULATION_STATUS_ERROR", "error", "ERROR"):
                set_failed(task, f"draft_calc_error:{st}")
            else:
                set_failed(task, f"draft_poll_unexpected:{st}")
        else:
            set_failed(task, f"draft_poll_error:{err or status}")
        self._update_task(task)

    # ----- Остальные стадии (без изменений концептуально) -----
    async def _supply_create(self, task: Dict[str, Any]):
        api = self._api_factory()
        draft_id = task.get("draft_id")
        if not draft_id:
            set_failed(task, "missing_draft_id_before_supply_create")
            self._update_task(task)
            return
        wid = task.get("chosen_warehouse_id") or task.get("warehouse_id")
        body = {
            "draft_id": draft_id,
            "warehouse_id": wid,
            "from_in_timezone": task.get("from_in_timezone") or task.get("desired_from_iso"),
            "to_in_timezone": task.get("to_in_timezone") or task.get("desired_to_iso"),
        }
        ok, status, data, err, raw = await api.json_request("POST", "/v1/draft/supply/create", json=body)
        if ok and isinstance(data, dict) and data.get("operation_id"):
            task["supply_create_operation_id"] = data["operation_id"]
            task["status"] = SupplyStatus.POLL_SUPPLY.value
            record_event(task, "SUPPLY_CREATE_SENT", {"operation_id": data["operation_id"]})
        else:
            set_failed(task, f"supply_create_error:{err or status}")
        self._update_task(task)

    async def _poll_supply_create(self, task: Dict[str, Any]):
        api = self._api_factory()
        op_id = task.get("supply_create_operation_id")
        if not op_id:
            set_failed(task, "missing_supply_create_op_id")
            self._update_task(task)
            return
        body = {"operation_id": op_id}
        ok, status, data, err, raw = await api.json_request("POST", "/v1/draft/supply/create/status", json=body)
        
        # Handle 429 rate limit with exponential backoff
        if status == 429:
            attempts = task.get("poll_supply_429_attempts", 0) + 1
            task["poll_supply_429_attempts"] = attempts
            wait = min(60, 2 * (2 ** (attempts - 1)))
            task["last_error"] = "rate_limit:429"
            task["retry_after_ts"] = now_ts() + int(wait)
            task["next_attempt_ts"] = task["retry_after_ts"]
            record_event(task, "POLL_SUPPLY_429", {"wait": int(wait), "attempts": attempts})
            self._update_task(task)
            return
        
        if ok and isinstance(data, dict):
            st = data.get("status")
            if st in ("DraftSupplyCreateStatusSuccess", "SUCCESS"):
                res = data.get("result") or {}
                oids = res.get("order_ids") or []
                if oids:
                    task["order_id"] = oids[0]
                    task["status"] = SupplyStatus.SUPPLY_ORDER_FETCH.value
                    task.pop("poll_supply_429_attempts", None)  # Reset counter on success
                    record_event(task, "SUPPLY_ORDER_ID", {"order_id": task["order_id"]})
                else:
                    set_failed(task, "supply_create_no_order_ids")
            elif st in ("IN_PROGRESS", "DraftSupplyCreateStatusInProgress"):
                record_event(task, "SUPPLY_CREATE_PROGRESS", {"op": op_id})
            else:
                # Treat unexpected statuses as non-fatal - keep in ORDER_DATA_FILLING
                task["status"] = "ORDER_DATA_FILLING"
                task["last_error"] = f"unexpected_status:{st}"
                record_event(task, "SUPPLY_CREATE_STATUS_UNEXPECTED", {"status": st})
        else:
            # Treat HTTP errors as non-fatal - keep in ORDER_DATA_FILLING
            task["status"] = "ORDER_DATA_FILLING"
            task["last_error"] = f"http_error:{err or status}"
            record_event(task, "SUPPLY_CREATE_HTTP_ERROR", {"err": str(err), "status": status})
        self._update_task(task)

    async def _order_fetch(self, task: Dict[str, Any]):
        # Changed: Use information already available from /v1/draft/supply/create/status
        # instead of calling non-public /v2/supply-order/get
        # Timeslot info should come from task data or prompt user to complete in UI
        if not task.get("order_id"):
            set_failed(task, "missing_order_id")
            self._update_task(task)
            return
        
        # Check if we already have timeslot info from previous stages
        fr = task.get("from_in_timezone") or task.get("desired_from_iso")
        to = task.get("to_in_timezone") or task.get("desired_to_iso")
        
        if fr and to:
            task["order_timeslot_set_ok"] = True
            task["status"] = SupplyStatus.CARGO_PREP.value
            record_event(task, "ORDER_FETCH_WITH_SLOT", {})
        else:
            # If timeslot info missing, mark as ORDER_DATA_FILLING for user to complete in UI
            # Do NOT fail the task - this is a non-fatal state
            task["status"] = "ORDER_DATA_FILLING"
            task["last_error"] = "timeslot_info_needed_complete_in_ui"
            record_event(task, "ORDER_NEEDS_COMPLETION_IN_UI", {})
            logger.info("Task %s needs timeslot completion in UI", task.get("id"))
        
        self._update_task(task)

    async def _cargo_prep(self, task: Dict[str, Any]):
        items = task.get("sku_items") or []
        boxes = []
        for idx, it in enumerate(items, start=1):
            boxes.append({
                "index": idx,
                "sku": it.get("sku"),
                "qty": it.get("proposed_qty") or it.get("quantity") or it.get("qty") or 1,
            })
        task["prepared_boxes"] = boxes
        task["status"] = SupplyStatus.CARGO_CREATING.value
        record_event(task, "CARGO_PREP_DONE", {"boxes": len(boxes)})
        self._update_task(task)

    async def _cargo_create(self, task: Dict[str, Any]):
        task["cargo_create_operation_id"] = "SIMULATED"
        task["status"] = SupplyStatus.POLL_CARGO.value
        record_event(task, "CARGO_CREATE_SENT", {})
        self._update_task(task)

    async def _poll_cargo(self, task: Dict[str, Any]):
        task["cargo_ids"] = [f"CG{b['index']}" for b in task.get("prepared_boxes", [])]
        task["status"] = SupplyStatus.LABELS_CREATING.value
        record_event(task, "CARGO_READY", {"cargo_ids": task["cargo_ids"]})
        self._update_task(task)

    async def _labels_create(self, task: Dict[str, Any]):
        task["labels_operation_id"] = "SIMULATED_LABELS"
        task["status"] = SupplyStatus.POLL_LABELS.value
        record_event(task, "LABELS_CREATE_SENT", {})
        self._update_task(task)

    async def _poll_labels(self, task: Dict[str, Any]):
        task["labels_pdf_path"] = f"labels_{task['id']}.pdf"
        task["status"] = SupplyStatus.LABELS_READY.value
        record_event(task, "LABELS_READY", {"pdf": task["labels_pdf_path"]})
        self._update_task(task)