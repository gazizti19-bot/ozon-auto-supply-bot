import os
import time
import uuid
import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import httpx
from datetime import datetime, timedelta, timezone

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # Python<3.9 fallback


# -----------------------
# Logging
# -----------------------
logger = logging.getLogger("ozon_supply_flow")
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s:%(name)s:%(message)s"
    )


# -----------------------
# Helpers
# -----------------------
def as_utc_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def compute_window_next_local_midnight(days: int, tzname: str) -> Tuple[str, str]:
    tz = ZoneInfo(tzname) if ZoneInfo else None
    now_utc = datetime.now(timezone.utc)
    if tz:
        now_local = now_utc.astimezone(tz)
        start_local = (now_local + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        end_local = start_local + timedelta(days=days) - timedelta(seconds=1)
        return as_utc_iso(start_local), as_utc_iso(end_local)
    # Fallback if zoneinfo is missing
    return as_utc_iso(now_utc), as_utc_iso(now_utc + timedelta(days=days))


def short(txt: str, limit: int = 300) -> str:
    if not txt:
        return ""
    return txt if len(txt) <= limit else txt[:limit] + "...(truncated)"


@dataclass
class OzonClient:
    client_id: str
    api_key: str
    base_url: str = "https://api-seller.ozon.ru"
    max_retries: int = 6
    backoff_base: float = 1.5
    backoff_cap: float = 180.0
    timeout: float = 30.0

    def _headers(self) -> Dict[str, str]:
        return {
            "Client-Id": self.client_id,
            "Api-Key": self.api_key,
            "Content-Type": "application/json",
        }

    def post(self, path: str, json_body: Dict[str, Any]) -> httpx.Response:
        url = path if path.startswith("http") else f"{self.base_url}{path}"
        attempt = 0
        with httpx.Client(timeout=self.timeout) as s:
            while True:
                attempt += 1
                try:
                    resp = s.post(url, json=json_body, headers=self._headers())
                    if resp.status_code == 429 and attempt <= self.max_retries:
                        # Respect server backoff if provided
                        delay = min(self.backoff_base * attempt, self.backoff_cap)
                        logger.warning("429 for %s -> wait %.2fs (attempt %d)", url, delay, attempt)
                        time.sleep(delay)
                        continue
                    logger.info("HTTP POST %s -> %s, resp=%s", url, resp.status_code, short(resp.text))
                    return resp
                except Exception as e:
                    if attempt >= self.max_retries:
                        raise
                    delay = min(self.backoff_base * attempt, self.backoff_cap)
                    logger.warning("HTTP error on %s: %s -> retry in %.2fs", url, e, delay)
                    time.sleep(delay)


# -----------------------
# Stage 3: Draft → Info → Timeslots → Supply Create
# -----------------------
def draft_create_crossdock(oz: OzonClient, items: List[Dict[str, Any]], drop_off_point_warehouse_id: int) -> str:
    """
    Returns: operation_id
    """
    payload = {
        "items": items,
        "type": "CREATE_TYPE_CROSSDOCK",
        "drop_off_point_warehouse_id": int(drop_off_point_warehouse_id),
    }
    resp = oz.post("/v1/draft/create", payload)
    resp.raise_for_status()
    op = resp.json().get("operation_id")
    if not op:
        raise RuntimeError("No operation_id returned from /v1/draft/create")
    return op


def draft_create_info_wait(oz: OzonClient, operation_id: str, timeout_s: int = 120, poll_s: float = 1.5) -> Dict[str, Any]:
    """
    Polls /v1/draft/create/info by operation_id until calculation is complete.
    Returns: dict with at least draft_id and clusters.
    """
    started = time.time()
    final: Optional[Dict[str, Any]] = None
    while time.time() - started < timeout_s:
        resp = oz.post("/v1/draft/create/info", {"operation_id": operation_id})
        resp.raise_for_status()
        js = resp.json()
        status = js.get("status")
        if status in ("CALCULATION_STATUS_SUCCESS", "success", "SUCCESS"):
            final = js
            break
        elif status in ("CALCULATION_STATUS_ERROR", "error", "ERROR"):
            raise RuntimeError(f"Draft calculation failed: {short(resp.text)}")
        time.sleep(poll_s)

    if final is None:
        raise TimeoutError("Timeout waiting for /v1/draft/create/info success")

    draft_id = final.get("draft_id")
    if not draft_id:
        # some variants return draft_id inside payload; attempt to find
        draft_id = final.get("result", {}).get("draft_id")
    if not draft_id:
        raise RuntimeError("No draft_id in /v1/draft/create/info result")

    logger.info("Draft ready: draft_id=%s", draft_id)
    return final


def draft_timeslot_info(oz: OzonClient, draft_id: int, warehouse_ids: List[int], days: int, tzname: str,
                        drop_off_point_warehouse_id: Optional[int] = None) -> Dict[str, Any]:
    df, dt = compute_window_next_local_midnight(days, tzname)
    payload: Dict[str, Any] = {
        "draft_id": int(draft_id),
        "date_from": df,
        "date_to": dt,
        "warehouse_ids": [int(x) for x in warehouse_ids],
    }
    # optional; API for crossdock returns slots by drop-off; harmless to pass explicitly
    if drop_off_point_warehouse_id:
        payload["drop_off_warehouse_id"] = int(drop_off_point_warehouse_id)

    resp = oz.post("/v1/draft/timeslot/info", payload)
    resp.raise_for_status()
    return resp.json()


def pick_first_slot_for_drop(resp_json: Dict[str, Any], drop_off_id: int) -> Optional[Tuple[str, str]]:
    arr = resp_json.get("drop_off_warehouse_timeslots")
    if not isinstance(arr, list):
        return None
    for it in arr:
        try:
            if int(it.get("drop_off_warehouse_id")) != int(drop_off_id):
                continue
            days = it.get("days") or []
            for d in days:
                slots = d.get("timeslots") or []
                for s in slots:
                    f = s.get("from_in_timezone") or s.get("fromInTimezone") or s.get("from")
                    t = s.get("to_in_timezone") or s.get("toInTimezone") or s.get("to")
                    if f and t:
                        return str(f), str(t)
        except Exception:
            continue
    return None


def supply_create(oz: OzonClient, draft_id: int, warehouse_ids: List[int], f_tz: str, t_tz: str) -> str:
    """
    According to your doc, booking is done by /v1/draft/supply/create
    Body requires: draft_id, warehouse_ids, from_in_timezone, to_in_timezone
    Returns: operation_id for status polling
    """
    payload = {
        "draft_id": int(draft_id),
        "warehouse_ids": [int(x) for x in warehouse_ids],
        "from_in_timezone": f_tz,
        "to_in_timezone": t_tz,
    }
    resp = oz.post("/v1/draft/supply/create", payload)
    resp.raise_for_status()
    op = resp.json().get("operation_id")
    if not op:
        raise RuntimeError("No operation_id from /v1/draft/supply/create")
    return op


# -----------------------
# Stage 4: Supply create status → Supply order get
# -----------------------
def supply_create_status_wait(oz: OzonClient, operation_id: str, timeout_s: int = 180, poll_s: float = 2.0) -> Dict[str, Any]:
    started = time.time()
    last: Optional[Dict[str, Any]] = None
    while time.time() - started < timeout_s:
        resp = oz.post("/v1/draft/supply/create/status", {"operation_id": operation_id})
        resp.raise_for_status()
        js = resp.json()
        status = (js.get("status") or "").lower()
        last = js
        if status in ("success", "ok", "done", "completed"):
            break
        if status in ("error", "failed", "fail"):
            raise RuntimeError(f"Supply create failed: {short(resp.text)}")
        time.sleep(poll_s)
    if not last:
        raise TimeoutError("No response from /v1/draft/supply/create/status")
    return last


def supply_order_get(oz: OzonClient, order_id: int) -> Dict[str, Any]:
    resp = oz.post("/v2/supply-order/get", {"order_id": int(order_id)})
    resp.raise_for_status()
    return resp.json()


# -----------------------
# Stage 5: Cargoes create → info
# -----------------------
def cargoes_create(oz: OzonClient, supply_id: int, cargoes: List[Dict[str, Any]], delete_current_version: bool = False) -> str:
    """
    Sends cargoes for the given supply.
    Body shape may differ per account; here we follow your doc:
    key, type, supply_id (+ optional items). We wrap into 'cargoes' and pass delete_current_version.
    Returns: operation_id to poll in /v1/cargoes/create/info
    """
    body = {
        "delete_current_version": bool(delete_current_version),
        "cargoes": cargoes,
    }
    # Ensure each cargo has supply_id; if not, inject it
    for c in body["cargoes"]:
        c.setdefault("supply_id", int(supply_id))

    resp = oz.post("/v1/cargoes/create", body)
    resp.raise_for_status()
    op = resp.json().get("operation_id")
    if not op:
        raise RuntimeError("No operation_id from /v1/cargoes/create")
    return op


def cargoes_create_info_wait(oz: OzonClient, operation_id: str, timeout_s: int = 180, poll_s: float = 2.0) -> Dict[str, Any]:
    started = time.time()
    last: Optional[Dict[str, Any]] = None
    while time.time() - started < timeout_s:
        resp = oz.post("/v1/cargoes/create/info", {"operation_id": operation_id})
        resp.raise_for_status()
        js = resp.json()
        last = js
        status = (js.get("status") or "").lower()
        if status in ("success", "ok", "done", "completed"):
            break
        if status in ("error", "failed", "fail"):
            raise RuntimeError(f"Cargoes create failed: {short(resp.text)}")
        time.sleep(poll_s)
    if not last:
        raise TimeoutError("No response from /v1/cargoes/create/info")
    return last


# -----------------------
# Stage 6: Labels create → get → file
# -----------------------
def cargoes_label_create(oz: OzonClient, supply_id: Optional[int] = None, cargo_ids: Optional[List[int]] = None) -> str:
    """
    Create labels by supply_id OR cargo_ids.
    Returns: operation_id to poll in /v1/cargoes-label/get
    """
    if not supply_id and not cargo_ids:
        raise ValueError("Provide supply_id or cargo_ids")
    body: Dict[str, Any] = {}
    if supply_id:
        body["supply_id"] = int(supply_id)
    if cargo_ids:
        body["cargo_ids"] = [int(x) for x in cargo_ids]
    resp = oz.post("/v1/cargoes-label/create", body)
    resp.raise_for_status()
    op = resp.json().get("operation_id")
    if not op:
        raise RuntimeError("No operation_id from /v1/cargoes-label/create")
    return op


def cargoes_label_get_wait(oz: OzonClient, operation_id: str, timeout_s: int = 120, poll_s: float = 2.0) -> Tuple[str, Dict[str, Any]]:
    """
    Waits until labels are generated.
    Returns: (file_guid, raw_response)
    """
    started = time.time()
    last: Optional[Dict[str, Any]] = None
    while time.time() - started < timeout_s:
        resp = oz.post("/v1/cargoes-label/get", {"operation_id": operation_id})
        resp.raise_for_status()
        js = resp.json()
        last = js
        status = (js.get("status") or "").lower()
        file_guid = js.get("file_guid") or js.get("fileGuid")
        if status in ("success", "ok", "done", "completed") and file_guid:
            return str(file_guid), js
        if status in ("error", "failed", "fail"):
            raise RuntimeError(f"Labels create failed: {short(resp.text)}")
        time.sleep(poll_s)
    raise TimeoutError("Timeout waiting for /v1/cargoes-label/get")


def cargoes_label_download(oz: OzonClient, file_guid: str, out_path: str) -> None:
    url = f"{oz.base_url}/v1/cargoes-label/file/{file_guid}"
    with httpx.Client(timeout=60) as s:
        resp = s.get(url, headers=oz._headers())
        resp.raise_for_status()
        with open(out_path, "wb") as f:
            f.write(resp.content)
    logger.info("Labels PDF saved to %s", out_path)


# -----------------------
# Stage 7: Timeslot update, Pass create, Pass status
# -----------------------
def supply_order_timeslot_update(oz: OzonClient, order_id: int, f_tz: str, t_tz: str) -> Dict[str, Any]:
    resp = oz.post("/v1/supply-order/timeslot/update", {
        "order_id": int(order_id),
        "from_in_timezone": f_tz,
        "to_in_timezone": t_tz,
    })
    resp.raise_for_status()
    return resp.json()


def supply_order_pass_create(oz: OzonClient, order_id: int, vehicle: Dict[str, Any]) -> Dict[str, Any]:
    """
    vehicle example:
    {
      "car_number": "A123BC116",
      "driver_name": "Ivan Ivanov",
      "driver_phone": "+79990000000",
      "car_model": "GAZelle",
    }
    Real schema may vary per account; adjust keys as required by your tenant.
    """
    body = {"order_id": int(order_id), "vehicle": vehicle}
    resp = oz.post("/v1/supply-order/pass/create", body)
    resp.raise_for_status()
    return resp.json()


def supply_order_pass_status(oz: OzonClient, order_id: int) -> Dict[str, Any]:
    resp = oz.post("/v1/supply-order/pass/status", {"order_id": int(order_id)})
    resp.raise_for_status()
    return resp.json()


# -----------------------
# End-to-end example
# -----------------------
def run_end_to_end() -> Dict[str, Any]:
    """
    A complete example of the flow:
    1) Create draft (crossdock) -> wait calc -> get draft_id + cluster
    2) Get timeslots -> choose first for drop-off
    3) Create supply (booking) -> wait status -> get supply order
    4) OPTIONAL: create cargoes, wait, labels
    """
    client_id = os.environ["OZON_CLIENT_ID"]
    api_key = os.environ["OZON_API_KEY"]
    base_url = os.getenv("OZON_BASE_URL", "https://api-seller.ozon.ru")
    drop_off_id = int(os.getenv("OZON_DROP_OFF_ID", "0") or "0")
    tzname = os.getenv("OZON_TZ", "Asia/Yekaterinburg")
    days = int(os.getenv("OZON_SLOT_DAYS", "7") or "7")

    oz = OzonClient(client_id=client_id, api_key=api_key, base_url=base_url)

    # 1) Create draft (crossdock)
    # Provide real SKUs and quantities here
    items = [
        {"sku": 2625768907, "quantity": 10},
        # add more if needed
    ]
    operation_id = draft_create_crossdock(oz, items, drop_off_point_warehouse_id=drop_off_id)
    info = draft_create_info_wait(oz, operation_id)
    draft_id = info.get("draft_id")
    if not draft_id:
        raise RuntimeError("draft_id not found in create/info result")

    # Choose a supply warehouse from clusters (first is fine)
    clusters = info.get("clusters") or []
    if not clusters:
        raise RuntimeError("No clusters returned by /v1/draft/create/info")
    # Take first cluster's first warehouse supply_warehouse_id
    first_wids: List[int] = []
    for c in clusters:
        for wh in (c.get("warehouses") or []):
            sw = (wh.get("supply_warehouse") or {})
            wid = sw.get("warehouse_id")
            if isinstance(wid, int):
                first_wids.append(wid)
    if not first_wids:
        raise RuntimeError("No supply warehouse ids found in clusters")
    supply_wid = first_wids[0]

    # 2) Get timeslots
    ts_info = draft_timeslot_info(
        oz,
        draft_id=int(draft_id),
        warehouse_ids=[supply_wid],
        days=days,
        tzname=tzname,
        drop_off_point_warehouse_id=drop_off_id or None,
    )
    slot = pick_first_slot_for_drop(ts_info, drop_off_id=drop_off_id) if drop_off_id else None
    if not slot:
        # Fallback: pick the very first slot in the entire response if drop_off filter isn't applicable
        slot = pick_first_slot_for_drop({"drop_off_warehouse_timeslots": [{"drop_off_warehouse_id": drop_off_id, "days": ts_info.get("days") or []}]}, drop_off_id)  # defensive
    if not slot:
        raise RuntimeError("No available timeslots found for the selected drop-off")
    f_tz, t_tz = slot
    logger.info("Chosen timeslot: %s -> %s (tz)", f_tz, t_tz)

    # 3) Create supply (this is the booking step per your spec)
    op_supply = supply_create(oz, draft_id=int(draft_id), warehouse_ids=[supply_wid], f_tz=f_tz, t_tz=t_tz)
    st = supply_create_status_wait(oz, op_supply)
    order_id = st.get("order_id") or st.get("result", {}).get("order_id")
    if not order_id:
        raise RuntimeError("No order_id returned by /v1/draft/supply/create/status")

    # 4) Get supply order info to obtain supply_id, supply_order_number, deadlines, etc.
    order = supply_order_get(oz, int(order_id))
    supply_id = order.get("supply_id") or order.get("result", {}).get("supply_id")
    supply_order_number = order.get("supply_order_number") or order.get("result", {}).get("supply_order_number")
    data_filling_deadline_utc = order.get("data_filling_deadline_utc") or order.get("result", {}).get("data_filling_deadline_utc")
    logger.info("Supply order created: number=%s, order_id=%s, supply_id=%s, deadline=%s",
                supply_order_number, order_id, supply_id, data_filling_deadline_utc)

    # 5) OPTIONAL: create cargoes → info
    # Fill items for each cargo according to your catalogue (SKU/qty set). Below is a minimal example.
    cargo_key = str(uuid.uuid4())
    cargoes = [
        {
            "key": cargo_key,
            "type": "BOX",  # or "PALLET"
            # "items": [{"sku": 2625768907, "quantity": 10}],  # Add composition if required by your tenant
        }
    ]
    op_cargo = cargoes_create(oz, int(supply_id), cargoes, delete_current_version=False)
    cargo_info = cargoes_create_info_wait(oz, op_cargo)
    # The exact shape may include cargo_ids per cargo; leave as-is for your parsing

    # 6) Labels: create → get → download
    op_lbl = cargoes_label_create(oz, supply_id=int(supply_id))
    file_guid, lbl_get = cargoes_label_get_wait(oz, op_lbl)
    out_pdf = f"labels_{supply_order_number or supply_id}.pdf"
    cargoes_label_download(oz, file_guid, out_pdf)

    # 7) OPTIONAL: update timeslot later, create pass, check pass status
    # supply_order_timeslot_update(oz, int(order_id), f_tz, t_tz)
    # supply_order_pass_create(oz, int(order_id), {"car_number": "A123BC116", "driver_name": "Ivan", "driver_phone": "+79990000000"})
    # pass_status = supply_order_pass_status(oz, int(order_id))

    return {
        "draft_id": draft_id,
        "supply_warehouse_id": supply_wid,
        "chosen_timeslot": {"from_in_timezone": f_tz, "to_in_timezone": t_tz},
        "order_id": order_id,
        "supply_id": supply_id,
        "supply_order_number": supply_order_number,
        "data_filling_deadline_utc": data_filling_deadline_utc,
        "labels_pdf": out_pdf,
        # "pass_status": pass_status if you call it
    }


if __name__ == "__main__":
    # Environment variables required:
    #  - OZON_CLIENT_ID
    #  - OZON_API_KEY
    #  - OZON_DROP_OFF_ID
    # Optional:
    #  - OZON_TZ (default Asia/Yekaterinburg)
    #  - OZON_BASE_URL (default https://api-seller.ozon.ru)
    result = run_end_to_end()
    print(json.dumps(result, ensure_ascii=False, indent=2))