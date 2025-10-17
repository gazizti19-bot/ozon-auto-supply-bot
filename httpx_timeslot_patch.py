import json
import logging
import os
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple, Set

import httpx

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # type: ignore

# Опциональный модуль верхнего слоя
try:
    import supply_watch as sw  # noqa: F401
except Exception:
    sw = None  # type: ignore

logger = logging.getLogger("httpx_timeslot_patch")
logger.setLevel(logging.INFO)

# === helpers for ENV ===
def _get_bool_env(*names: str, default: bool = False) -> bool:
    """
    Возвращает bool по первому найденному имени переменной окружения из списка.
    Значения "1,true,yes,y,on" (без учета регистра) считаются True.
    Если ни одна переменная не задана — вернёт default.
    """
    for name in names:
        v = os.getenv(name)
        if v is not None and v != "":
            s = str(v).strip().lower()
            return s in ("1", "true", "yes", "y", "on")
    return bool(default)

# === ENV ===
DROP_ID = int(os.getenv("OZON_DROP_OFF_ID", "0") or "0")
DROP_TZ = os.getenv("OZON_DROP_OFF_TZ", os.getenv("OZON_TZ", "Asia/Yekaterinburg"))
SLOT_SEARCH_DAYS = int(os.getenv("SLOT_SEARCH_DAYS", "7") or "7")
STRICT_DROP_ONLY = os.getenv("SLOT_STRICT_DROP_ONLY", "true").lower() in ("1", "true", "yes", "y")

DISCOVER_DROPOFFS = os.getenv("OZON_DISCOVER_DROPOFFS", "1").lower() in ("1", "true", "yes", "y")
FBO_LIST_SUPPLY_TYPES: List[str] = [
    s.strip()
    for s in (os.getenv("OZON_FBO_LIST_SUPPLY_TYPES", "WAREHOUSE_SUPPLY_TYPE_FBO") or "").split(",")
    if s.strip()
] or ["WAREHOUSE_SUPPLY_TYPE_FBO"]
STUB_FBO_LIST = os.getenv("OZON_STUB_FBO_LIST", "1").lower() in ("1", "true", "yes", "y")
USE_CLUSTER_WIDS = os.getenv("OZON_USE_CLUSTER_WIDS", "1").lower() in ("1", "true", "yes", "y")

# Управление fallback-логикой timeslot: ретраи на другие WID и discover-запросы без drop_off
DISABLE_TS_FALLBACK = os.getenv("OZON_TIMESLOT_DISABLE_FALLBACK", "1").lower() in ("1", "true", "yes", "y")

# ВАЖНО: читаем оба ключа, приоритет у AUTO_BOOK. По умолчанию ВЫКЛЮЧЕНО.
AUTO_BOOK = _get_bool_env("AUTO_BOOK", "OZON_AUTO_BOOK", default=False)

REQUIRE_SUPPLY_ID = os.getenv("REQUIRE_SUPPLY_ID", "0").lower() in ("1", "true", "yes", "y")
PROGRESS_TASK_ON_BOOK = os.getenv("PROGRESS_TASK_ON_BOOK", "1").lower() in ("1", "true", "yes", "y")

BOOKED_STATUS_NAME = os.getenv("BOOKED_STATUS_NAME", "booked").strip() or "booked"
BOOKED_ALT_STATUSES = [s.strip() for s in (os.getenv("BOOKED_ALT_STATUSES", "timeslot_booked,slot_booked,booked_ok") or "").split(",") if s.strip()]

# Поля статуса, которые пытаемся обновить и по которым проверяем состояние
BOOKED_STATUS_FIELDS = [s.strip() for s in (os.getenv(
    "BOOKED_STATUS_FIELDS",
    "status,state,phase,stage,step,pipeline_status,current_status,progress,phase_name"
) or "").split(",") if s.strip()]

# Возможные поля идентификатора
TASK_ID_FIELDS = [s.strip() for s in (os.getenv(
    "TASK_ID_FIELDS", "id,task_id,uuid,pk,external_id"
) or "").split(",") if s.strip()]

# Кэши
_DRAFT_CLUSTER_CACHE: Dict[int, List[int]] = {}
_DRAFT_PRIMARY_WID: Dict[int, int] = {}
_LAST_CLUSTER: List[int] = []
_LAST_PRIMARY_WID: Optional[int] = None
_BOOKED_DRAFTS: Set[int] = set()
_BOOKED_RESULTS: Dict[int, Dict[str, Any]] = {}

# Оригинальные методы httpx
_ORIG_SYNC_POST = httpx.Client.post
_ORIG_ASYNC_POST = httpx.AsyncClient.post


def _as_utc_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _compute_window_now_local(days: int, tzname: str) -> Tuple[str, str]:
    tz = ZoneInfo(tzname) if ZoneInfo else None
    now_utc = datetime.now(timezone.utc)
    if tz:
        now_local = now_utc.astimezone(tz)
        start_local = (now_local + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        end_local = start_local + timedelta(days=days) - timedelta(seconds=1)
        return _as_utc_iso(start_local), _as_utc_iso(end_local)
    return _as_utc_iso(now_utc), _as_utc_iso(now_utc + timedelta(days=days))


def _extract_cluster_and_primary(resp_json: Dict[str, Any]) -> Tuple[List[int], Optional[int]]:
    out: List[int] = []
    primary: Optional[int] = None
    try:
        clusters = resp_json.get("clusters") or []
        for c in clusters:
            warehouses = c.get("warehouses") or []
            ids = []
            for w in warehouses:
                sws = (w.get("supply_warehouse") or {})
                wid = sws.get("warehouse_id")
                if isinstance(wid, int):
                    ids.append(wid)
            if ids:
                logger.info("create/info cluster cache: %s", ids)
                out.extend(ids)
                if primary is None:
                    primary = ids[0]
    except Exception as e:
        logger.warning("create/info parse error: %s", e)
    seen = set()
    uniq: List[int] = []
    for wid in out:
        if wid not in seen:
            uniq.append(wid)
            seen.add(wid)
    return uniq, primary


def _sanitize_headers_for_raw_json(orig_headers: httpx.Headers) -> httpx.Headers:
    new_headers = httpx.Headers(orig_headers)
    for h in ("Content-Encoding", "Transfer-Encoding", "Content-Length"):
        if h in new_headers:
            del new_headers[h]
    new_headers["Content-Type"] = "application/json; charset=utf-8"
    return new_headers


def _apply_mod_and_rebuild_response(orig_resp: httpx.Response, data: Dict[str, Any]) -> httpx.Response:
    content = json.dumps(data, ensure_ascii=False).encode("utf-8")
    safe_headers = _sanitize_headers_for_raw_json(orig_resp.headers)
    return httpx.Response(
        status_code=orig_resp.status_code,
        headers=safe_headers,
        content=content,
        request=orig_resp.request,
    )


def _synthetic_json_response(url: str, status_code: int, data: Dict[str, Any]) -> httpx.Response:
    content = json.dumps(data, ensure_ascii=False).encode("utf-8")
    req = httpx.Request("POST", url)
    return httpx.Response(
        status_code=status_code,
        headers={"Content-Type": "application/json; charset=utf-8"},
        content=content,
        request=req,
    )


def _find_drop_item(resp_json: Dict[str, Any], drop_id: int) -> Tuple[Optional[Dict[str, Any]], int]:
    arr = resp_json.get("drop_off_warehouse_timeslots")
    if not isinstance(arr, list):
        return None, 0
    for it in arr:
        try:
            if int(it.get("drop_off_warehouse_id")) == int(drop_id):
                days = (it.get("days") or [])
                return it, (len(days) if isinstance(days, list) else 0)
        except Exception:
            continue
    return None, 0


def _summarize_available_dropoffs(resp_json: Dict[str, Any]) -> List[Tuple[int, int]]:
    out: List[Tuple[int, int]] = []
    arr = resp_json.get("drop_off_warehouse_timeslots") or []
    for it in arr:
        did = it.get("drop_off_warehouse_id")
        try:
            did = int(did)
        except Exception:
            continue
        days = it.get("days") or []
        out.append((did, len(days) if isinstance(days, list) else 0))
    return out


def _reorder_or_filter_to_drop_only(resp_json: Dict[str, Any]) -> Dict[str, Any]:
    arr = resp_json.get("drop_off_warehouse_timeslots")
    if not isinstance(arr, list) or not arr:
        return resp_json
    drop_item = None
    others: List[Dict[str, Any]] = []
    for it in arr:
        try:
            if int(it.get("drop_off_warehouse_id")) == int(DROP_ID):
                drop_item = it
            else:
                others.append(it)
        except Exception:
            others.append(it)
    if drop_item is None:
        logger.warning("no timeslots entry for drop-off %s in response", DROP_ID)
        return resp_json
    new_arr = [drop_item] if STRICT_DROP_ONLY else [drop_item] + others
    resp_json_mod = deepcopy(resp_json)
    resp_json_mod["drop_off_warehouse_timeslots"] = new_arr

    # короткая подсказка в лог
    days = (drop_item.get("days") or [])
    logger.info("drop-off %s days_count=%s", DROP_ID, len(days) if isinstance(days, list) else 0)
    try:
        for d in days:
            slots = d.get("timeslots") or []
            if slots:
                f = slots[0].get("from_in_timezone") or slots[0].get("from")
                t = slots[0].get("to_in_timezone") or slots[0].get("to")
                logger.info("drop-off %s first_slot: %s -> %s", DROP_ID, f, t)
                break
    except Exception:
        pass
    return resp_json_mod


def _build_timeslot_payload(base_js: Dict[str, Any]) -> Dict[str, Any]:
    js = deepcopy(base_js) if base_js else {}
    if DROP_ID:
        js["drop_off_warehouse_id"] = int(DROP_ID)
    df, dt = _compute_window_now_local(SLOT_SEARCH_DAYS, DROP_TZ)
    js["date_from"] = df
    js["date_to"] = dt
    return js


def _normalize_warehouse_ids_with_cluster(js: Dict[str, Any]) -> Tuple[Dict[str, Any], bool]:
    if not USE_CLUSTER_WIDS:
        return js, False
    draft_id = None
    try:
        draft_id = int(js.get("draft_id")) if js.get("draft_id") is not None else None
    except Exception:
        draft_id = None

    if draft_id and draft_id in _DRAFT_CLUSTER_CACHE:
        cluster = list(_DRAFT_CLUSTER_CACHE[draft_id])
        primary = _DRAFT_PRIMARY_WID.get(draft_id)
    else:
        cluster = list(_LAST_CLUSTER)
        primary = _LAST_PRIMARY_WID
        if draft_id and cluster:
            _DRAFT_CLUSTER_CACHE[draft_id] = list(cluster)
            if primary is not None:
                _DRAFT_PRIMARY_WID[draft_id] = int(primary)
            logger.info("bind draft_id=%s to LAST_CLUSTER (size=%s) and PRIMARY=%s", draft_id, len(cluster), primary)

    if not cluster:
        return js, False

    js2 = deepcopy(js)
    req_ids = js2.get("warehouse_ids") or []
    norm_req: List[int] = []
    for x in req_ids:
        try:
            norm_req.append(int(x))
        except Exception:
            continue

    cluster_set = set(int(x) for x in cluster)

    if not norm_req:
        js2["warehouse_ids"] = list(cluster_set)
        logger.warning("timeslot/info: warehouse_ids empty -> use cluster %s", js2["warehouse_ids"])
        return js2, True

    inter = [wid for wid in norm_req if wid in cluster_set]
    if inter:
        if inter != norm_req:
            js2["warehouse_ids"] = inter
            logger.warning("timeslot/info: warehouse_ids intersect cluster -> %s", inter)
            return js2, True
        return js2, False

    js2["warehouse_ids"] = list(cluster_set)
    logger.warning(
        "timeslot/info: warehouse_ids %s not in cluster %s -> replace with cluster",
        norm_req, list(cluster_set)
    )
    return js2, True


def _patch_fbo_list_payload(base_js: Dict[str, Any]) -> Dict[str, Any]:
    js = deepcopy(base_js) if base_js else {}
    f = js.get("filter_by_supply_type")
    if not isinstance(f, list) or len(f) == 0:
        js["filter_by_supply_type"] = list(FBO_LIST_SUPPLY_TYPES)
        logger.warning("patched /v1/warehouse/fbo/list: filter_by_supply_type=%s", js["filter_by_supply_type"])
    return js


def _make_fbo_stub_payload() -> Dict[str, Any]:
    wid = int(DROP_ID) if DROP_ID else 0
    return {
        "warehouses": ([] if wid == 0 else [{
            "warehouse_id": wid,
            "name": "ENV_DROP_OFF",
            "supply_type": "WAREHOUSE_SUPPLY_TYPE_FBO",
        }])
    }


def _choose_supply_wid(req_js: Dict[str, Any], draft_id: Optional[int], override: Optional[int] = None) -> Optional[int]:
    if override:
        return int(override)
    ids: List[int] = []
    for x in (req_js.get("warehouse_ids") or []):
        try:
            ids.append(int(x))
        except Exception:
            continue
    if len(ids) == 1:
        return ids[0]
    if draft_id and draft_id in _DRAFT_PRIMARY_WID:
        return int(_DRAFT_PRIMARY_WID[draft_id])
    if _LAST_PRIMARY_WID is not None:
        return int(_LAST_PRIMARY_WID)
    if ids:
        return ids[0]
    return None


# ---------- Slots helpers ----------

def _parse_dt_maybe_utc(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _collect_slots(drop_item: Dict[str, Any]) -> List[Tuple[str, str, datetime]]:
    slots: List[Tuple[str, str, datetime]] = []
    try:
        days = drop_item.get("days") or []
        for d in days:
            for s in (d.get("timeslots") or []):
                f = s.get("from_in_timezone") or s.get("fromInTimezone") or s.get("from")
                t = s.get("to_in_timezone") or s.get("toInTimezone") or s.get("to")
                if not (f and t):
                    continue
                dt = _parse_dt_maybe_utc(f)
                if dt is None:
                    continue
                slots.append((str(f), str(t), dt.astimezone(timezone.utc).replace(microsecond=0)))
    except Exception:
        pass
    slots.sort(key=lambda x: x[2])
    return slots


def _target_from_sw_by_draft(draft_id: Optional[int]) -> Tuple[Optional[str], Optional[str]]:
    if draft_id is None or sw is None:
        return None, None
    try:
        for t in sw.list_tasks() or []:
            try:
                if int(t.get("draft_id")) == int(draft_id):
                    f = t.get("desired_from_iso") or t.get("desired_from_in_timezone") or t.get("from_in_timezone")
                    g = t.get("desired_to_iso") or t.get("desired_to_in_timezone") or t.get("to_in_timezone")
                    if f and g:
                        return str(f), str(g)
            except Exception:
                continue
    except Exception:
        pass
    return None, None


def _extract_target_from_req(req_js: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    draft_id = None
    try:
        draft_id = int(req_js.get("draft_id")) if req_js.get("draft_id") is not None else None
    except Exception:
        draft_id = None
    f_sw, t_sw = _target_from_sw_by_draft(draft_id)
    if f_sw and t_sw:
        return f_sw, t_sw

    keys = [
        ("desired_from_iso", "desired_to_iso"),
        ("desired_from_in_timezone", "desired_to_in_timezone"),
        ("target_from", "target_to"),
        ("from_in_timezone", "to_in_timezone"),
        ("desiredFromIso", "desiredToIso"),
        ("desiredFromInTimezone", "desiredToInTimezone"),
        ("targetFrom", "targetTo"),
        ("fromInTimezone", "toInTimezone"),
    ]
    for kf, kt in keys:
        f = req_js.get(kf)
        t = req_js.get(kt)
        if f and t:
            return str(f), str(t)
    return None, None


def _select_slot(drop_item: Dict[str, Any], target_from: Optional[str], target_to: Optional[str]) -> Optional[Tuple[str, str, str]]:
    slots = _collect_slots(drop_item)
    if not slots:
        return None
    if target_from and target_to:
        tf_dt = _parse_dt_maybe_utc(target_from)
        tt_dt = _parse_dt_maybe_utc(target_to)
        if tf_dt and tt_dt:
            tfu = tf_dt.astimezone(timezone.utc).replace(microsecond=0)
            ttu = tt_dt.astimezone(timezone.utc).replace(microsecond=0)
            for f, t, sd in slots:
                td = _parse_dt_maybe_utc(t)
                if td and sd == tfu and td.astimezone(timezone.utc).replace(microsecond=0) == ttu:
                    logger.info("slot selector: exact match by full window")
                    return f, t, "exact"
            for f, t, sd in slots:
                if sd == tfu:
                    logger.info("slot selector: exact match by start")
                    return f, t, "exact_start"
            best = None
            best_diff = None
            for f, t, sd in slots:
                diff = abs((sd - tfu).total_seconds())
                if best is None or diff < best_diff or (diff == best_diff and sd < best[2]):
                    best = (f, t, sd)
                    best_diff = diff
            if best:
                logger.info("slot selector: nearest match diff=%.0fs", best_diff or -1)
                return best[0], best[1], "nearest"
    f, t, _ = slots[0]
    logger.info("slot selector: fallback to first slot")
    return f, t, "first"


# ---------- supply/create payload ----------

def _supply_create_payload_variants(draft_id: int, supply_wid: int, f: str, t: str) -> List[Dict[str, Any]]:
    v: List[Dict[str, Any]] = []
    v.append({
        "draft_id": int(draft_id),
        "warehouse_id": int(supply_wid),
        "from_in_timezone": f,
        "to_in_timezone": t,
        "drop_off_point_warehouse_id": int(DROP_ID) if DROP_ID else None,
    })
    v.append({
        "draft_id": int(draft_id),
        "warehouse_id": int(supply_wid),
        "from_in_timezone": f,
        "to_in_timezone": t,
    })
    v.append({
        "draft_id": int(draft_id),
        "warehouse_ids": [int(supply_wid)],
        "from_in_timezone": f,
        "to_in_timezone": t,
        "drop_off_point_warehouse_id": int(DROP_ID) if DROP_ID else None,
    })
    v.append({
        "draft_id": int(draft_id),
        "warehouse_ids": [int(supply_wid)],
        "from_in_timezone": f,
        "to_in_timezone": t,
    })
    v.append({
        "draftId": int(draft_id),
        "warehouseId": int(supply_wid),
        "fromInTimezone": f,
        "toInTimezone": t,
    })
    v.append({
        "draftId": int(draft_id),
        "warehouseIds": [int(supply_wid)],
        "fromInTimezone": f,
        "toInTimezone": t,
    })
    out: List[Dict[str, Any]] = []
    for payload in v:
        payload2 = {k: v for k, v in payload.items() if v is not None}
        out.append(payload2)
    return out


# ---------- status parsing ----------

def _deep_find_first_int(obj: Any, key_names: List[str]) -> Optional[int]:
    keys_lc = [k.lower() for k in key_names]
    def _walk(o: Any) -> Optional[int]:
        if isinstance(o, dict):
            for k, v in o.items():
                kl = str(k).lower()
                if kl in keys_lc:
                    try:
                        if isinstance(v, bool):
                            pass
                        elif isinstance(v, int) and v > 0:
                            return int(v)
                        elif isinstance(v, str) and v.isdigit():
                            return int(v)
                        elif isinstance(v, list):
                            for it in v:
                                if isinstance(it, int) and it > 0:
                                    return int(it)
                                if isinstance(it, str) and it.isdigit():
                                    return int(it)
                    except Exception:
                        pass
                r = _walk(v)
                if r is not None:
                    return r
        elif isinstance(o, list):
            for it in o:
                r = _walk(it)
                if r is not None:
                    return r
        return None
    return _walk(obj)


def _extract_status_and_order(js: Dict[str, Any]) -> Tuple[str, Optional[int]]:
    result = js.get("result") or {}
    status = (js.get("status") or js.get("state") or result.get("status") or result.get("state") or "").strip()

    order_id = js.get("order_id") or result.get("order_id")
    try:
        order_id = int(order_id) if order_id is not None else None
    except Exception:
        order_id = None

    if order_id is None:
        for key in ("order_ids", "orderIds"):
            arr = js.get(key) or result.get(key)
            if isinstance(arr, list):
                for it in arr:
                    if isinstance(it, int) and it > 0:
                        order_id = int(it)
                        break
                    if isinstance(it, str) and it.isdigit():
                        order_id = int(it)
                        break
            if order_id:
                try:
                    logger.info("supply/status: found %s=%s, use first=%s", key, arr, order_id)
                except Exception:
                    pass
                break

    if order_id is None:
        order_id = _deep_find_first_int(js, [
            "order_id", "orderId", "supply_order_id", "supplyOrderId", "order_ids", "orderIds", "id",
        ])

    return status, order_id


def _is_success_status(status: str) -> bool:
    s = (status or "").lower()
    return ("success" in s) or s in {
        "ok", "done", "completed", "created", "ready", "finished", "completed_success", "succeeded"
    }


# ---------- timeslot retry helpers ----------

def _candidate_wids_for_retry(draft_id: Optional[int], current_wid: Optional[int]) -> List[int]:
    if draft_id and draft_id in _DRAFT_CLUSTER_CACHE:
        cluster = list(_DRAFT_CLUSTER_CACHE[draft_id])
        primary = _DRAFT_PRIMARY_WID.get(draft_id)
    else:
        cluster = list(_LAST_CLUSTER)
        primary = _LAST_PRIMARY_WID
    out: List[int] = []
    if primary and (current_wid is None or int(primary) != int(current_wid)):
        out.append(int(primary))
    for wid in cluster:
        try:
            wid = int(wid)
        except Exception:
            continue
        if current_wid is not None and wid == int(current_wid):
            continue
        if wid not in out:
            out.append(wid)
    return out[:8]


def _has_slots_for_drop(resp_json: Dict[str, Any], drop_id: int) -> bool:
    di, days_cnt = _find_drop_item(resp_json, drop_id)
    return (di is not None) and (days_cnt > 0)


def _timeslot_retry_sync(self: httpx.Client, url: str, base_js: Dict[str, Any], headers: Optional[Dict[str, str]], draft_id: Optional[int]) -> Tuple[Optional[httpx.Response], Optional[Dict[str, Any]], Optional[int]]:
    cur_ids = base_js.get("warehouse_ids") or []
    cur_wid = None
    for x in cur_ids:
        try:
            cur_wid = int(x); break
        except Exception:
            continue
    candidates = _candidate_wids_for_retry(draft_id, cur_wid)
    for wid in candidates:
        js2 = deepcopy(base_js)
        js2["warehouse_ids"] = [int(wid)]
        logger.warning("timeslot retry: try warehouse_id=%s", wid)
        resp2 = _ORIG_SYNC_POST(self, url, json=js2, headers=headers)
        if resp2.status_code != 200:
            logger.warning("timeslot retry wid=%s HTTP %s: %s", wid, resp2.status_code, (resp2.text or "")[:200])
            continue
        data2 = resp2.json()
        if _has_slots_for_drop(data2, DROP_ID):
            logger.info("timeslot retry: SUCCESS on wid=%s for drop_off=%s", wid, DROP_ID)
            return resp2, data2, wid
        else:
            logger.warning("timeslot retry: no slots on wid=%s", wid)
    return None, None, None


async def _timeslot_retry_async(self: httpx.AsyncClient, url: str, base_js: Dict[str, Any], headers: Optional[Dict[str, str]], draft_id: Optional[int]) -> Tuple[Optional[httpx.Response], Optional[Dict[str, Any]], Optional[int]]:
    cur_ids = base_js.get("warehouse_ids") or []
    cur_wid = None
    for x in cur_ids:
        try:
            cur_wid = int(x); break
        except Exception:
            continue
    candidates = _candidate_wids_for_retry(draft_id, cur_wid)
    for wid in candidates:
        js2 = deepcopy(base_js)
        js2["warehouse_ids"] = [int(wid)]
        logger.warning("timeslot retry(async): try warehouse_id=%s", wid)
        resp2 = await _ORIG_ASYNC_POST(self, url, json=js2, headers=headers)
        if resp2.status_code != 200:
            logger.warning("timeslot retry(async) wid=%s HTTP %s: %s", wid, resp2.status_code, (resp2.text or "")[:200])
            continue
        data2 = resp2.json()
        if _has_slots_for_drop(data2, DROP_ID):
            logger.info("timeslot retry(async): SUCCESS on wid=%s for drop_off=%s", wid, DROP_ID)
            return resp2, data2, wid
        else:
            logger.warning("timeslot retry(async): no slots on wid=%s", wid)
    return None, None, None


# ---------- supply-order details ----------

def _deep_find_supply_like_id(obj: Any) -> Optional[int]:
    def _walk(o: Any) -> Optional[int]:
        if isinstance(o, dict):
            for k, v in o.items():
                kl = str(k).lower()
                try:
                    if ("supply" in kl) and ("id" in kl):
                        if isinstance(v, bool):
                            pass
                        elif isinstance(v, int) and v > 0:
                            return int(v)
                        elif isinstance(v, str) and v.isdigit():
                            return int(v)
                    r = _walk(v)
                    if r is not None:
                        return r
                except Exception:
                    continue
        elif isinstance(o, list):
            for it in o:
                r = _walk(it)
                if r is not None:
                    return r
        return None
    return _walk(obj)


def _extract_supply_order_info(js: Dict[str, Any]) -> Tuple[Optional[str], Optional[int]]:
    def _first_str(o: Any, keys: List[str]) -> Optional[str]:
        if isinstance(o, dict):
            for k in keys:
                v = o.get(k)
                if isinstance(v, str) and v:
                    return v
        return None

    def _first_int(o: Any, keys: List[str]) -> Optional[int]:
        if isinstance(o, dict):
            for k in keys:
                v = o.get(k)
                try:
                    if isinstance(v, bool):
                        continue
                    if isinstance(v, int) and v > 0:
                        return int(v)
                    if isinstance(v, str) and v.isdigit():
                        return int(v)
                    if isinstance(v, dict):
                        iv = _first_int(v, ["id", "supply_id", "supplyId"])
                        if iv:
                            return iv
                except Exception:
                    continue
        return None

    for scope in [js, js.get("result") or {}]:
        if not isinstance(scope, dict):
            continue
        num = _first_str(scope, ["supply_order_number", "supplyOrderNumber", "order_number", "orderNumber", "number"])
        sid = _first_int(scope, ["supply_id", "supplyId", "supply", "supply_order", "supplyOrder"])
        if num or sid:
            try:
                sid = int(sid) if sid is not None else None
            except Exception:
                sid = None
            return (num, sid)

    list_keys = ["orders", "supply_orders", "supplyOrders", "items", "data", "order_list", "supply_orders_list"]
    for scope in [js, js.get("result") or {}]:
        if not isinstance(scope, dict):
            continue
        for lk in list_keys:
            arr = scope.get(lk)
            if isinstance(arr, list) and arr:
                it = arr[0]
                if isinstance(it, dict):
                    num = _first_str(it, ["supply_order_number", "supplyOrderNumber", "order_number", "orderNumber", "number"])
                    sid = _first_int(it, ["supply_id", "supplyId", "supply", "supply_order", "supplyOrder"])
                    try:
                        sid = int(sid) if sid is not None else None
                    except Exception:
                        sid = None
                    if num or sid:
                        return (num, sid)

    sid = _deep_find_supply_like_id(js)
    return (None, sid)


def _supply_order_get_sync(self: httpx.Client, base_url: str, headers: Optional[Dict[str, str]], order_id: int) -> Optional[Dict[str, Any]]:
    endpoints = ["/v2/supply-order/get", "/v1/supply-order/get"]
    payloads = [
        {"order_ids": [int(order_id)]},
        {"order_id": int(order_id)},
        {"supply_order_ids": [int(order_id)]},
        {"supply_order_id": int(order_id)},
    ]
    for ep in endpoints:
        for pl in payloads:
            try:
                resp = _ORIG_SYNC_POST(self, base_url + ep, json=pl, headers=headers)
                if resp.status_code == 200:
                    logger.info("supply-order/get OK via %s payload=%s", ep, json.dumps(pl, ensure_ascii=False))
                    return resp.json()
                else:
                    logger.warning("supply-order/get HTTP %s via %s payload=%s: %s",
                                   resp.status_code, ep, pl, (resp.text or "")[:200])
            except Exception as e:
                logger.warning("supply-order/get error via %s payload=%s: %s", ep, pl, e)
    return None


async def _supply_order_get_async(self: httpx.AsyncClient, base_url: str, headers: Optional[Dict[str, str]], order_id: int) -> Optional[Dict[str, Any]]:
    endpoints = ["/v2/supply-order/get", "/v1/supply-order/get"]
    payloads = [
        {"order_ids": [int(order_id)]},
        {"order_id": int(order_id)},
        {"supply_order_ids": [int(order_id)]},
        {"supply_order_id": int(order_id)},
    ]
    for ep in endpoints:
        for pl in payloads:
            try:
                resp = await _ORIG_ASYNC_POST(self, base_url + ep, json=pl, headers=headers)
                if resp.status_code == 200:
                    logger.info("supply-order/get(async) OK via %s payload=%s", ep, json.dumps(pl, ensure_ascii=False))
                    return resp.json()
                else:
                    logger.warning("supply-order/get(async) HTTP %s via %s payload=%s: %s",
                                   resp.status_code, ep, pl, (resp.text or "")[:200])
            except Exception as e:
                logger.warning("supply-order/get(async) error via %s payload=%s: %s", ep, pl, e)
    return None


# ---------- supply_watch integration ----------

_TIMESLOT_SEARCH_VALUES = {
    "timeslot search", "timeslot_search", "search_timeslot", "slot_search",
    "slotsearch", "timeslot", "ts_search"
}

def _task_status_snapshot(task: Dict[str, Any]) -> Dict[str, Any]:
    snap: Dict[str, Any] = {}
    for k in BOOKED_STATUS_FIELDS:
        v = task.get(k)
        snap[k] = (str(v).lower() if isinstance(v, str) else v)
    return snap

def _is_timeslot_search(task: Dict[str, Any]) -> bool:
    snap = _task_status_snapshot(task)
    for v in snap.values():
        if isinstance(v, str) and v in _TIMESLOT_SEARCH_VALUES:
            return True
    return False

def _find_task_by_draft(draft_id: int) -> Optional[Dict[str, Any]]:
    if sw is None:
        return None
    try:
        tasks = sw.list_tasks() or []
    except Exception:
        return None
    for t in tasks:
        try:
            td = t.get("draft_id")
            if td is None:
                continue
            if str(td).isdigit() and int(td) == int(draft_id):
                return t
        except Exception:
            continue
    return None

def _extract_any_task_id(task: Dict[str, Any]) -> Optional[Any]:
    for key in TASK_ID_FIELDS:
        if key in task and task[key] not in (None, ""):
            return task[key]
    return None

def _call_sw(fn_name: str, *args, **kwargs) -> bool:
    try:
        fn = getattr(sw, fn_name, None) if sw else None
        if not callable(fn):
            return False
        fn(*args, **kwargs)
        logger.info("booking-result persisted via sw.%s%r", fn_name, (args or kwargs))
        return True
    except TypeError:
        return False
    except Exception:
        return False

def _try_progress_status(task_id_value: Any, status_field: str, status_value: str) -> bool:
    # Пробуем разные сигнатуры и методы
    # 1) update_task(**kwargs)
    if _call_sw("update_task", **{status_field: status_value, "id": task_id_value}):
        return True
    if _call_sw("update_task", **{status_field: status_value, "task_id": task_id_value}):
        return True
    # 2) update_task(payload)
    if _call_sw("update_task", {status_field: status_value, "id": task_id_value}):
        return True
    if _call_sw("update_task", {status_field: status_value, "task_id": task_id_value}):
        return True
    # 3) update_task(task_id, **kwargs)
    if _call_sw("update_task", task_id_value, **{status_field: status_value}):
        return True
    # 4) update_task(task_id, payload_dict)
    if _call_sw("update_task", task_id_value, {status_field: status_value}):
        return True
    # 5) Специализированные методы
    for method in ("set_status", "set_stage", "mark_booked", "advance", "complete", "notify_booked"):
        # kwargs с id
        if _call_sw(method, **{status_field: status_value, "id": task_id_value}):
            return True
        if _call_sw(method, **{status_field: status_value, "task_id": task_id_value}):
            return True
        # dict payload
        if _call_sw(method, {status_field: status_value, "id": task_id_value}):
            return True
        if _call_sw(method, {status_field: status_value, "task_id": task_id_value}):
            return True
        # сигнатура (task_id, value) — для set_status/set_stage/advance/complete
        if method in ("set_status", "set_stage", "advance", "complete"):
            if _call_sw(method, task_id_value, status_value):
                return True
    return False


def _persist_booking_result(
    draft_id: int,
    order_id: int,
    number: Optional[str],
    supply_id: Optional[int],
    slot_from: Optional[str],
    slot_to: Optional[str],
    supply_wid: Optional[int],
) -> None:
    try:
        _BOOKED_RESULTS[draft_id] = {
            "order_id": order_id,
            "number": number,
            "supply_id": supply_id,
            "slot_from": slot_from,
            "slot_to": slot_to,
            "warehouse_id": supply_wid,
            "drop_off_id": DROP_ID,
            "saved_at": datetime.now(timezone.utc).isoformat(),
        }
    except Exception:
        pass

    logger.info(
        "booking-result: draft_id=%s order_id=%s number=%s supply_id=%s from=%s to=%s wid=%s",
        draft_id, order_id, number, supply_id, slot_from, slot_to, supply_wid
    )

    if sw is None or not PROGRESS_TASK_ON_BOOK:
        return

    task = _find_task_by_draft(draft_id)
    task_id_value = _extract_any_task_id(task) if isinstance(task, dict) else None

    # Сначала пробуем “богатое” обновление, как раньше, вдруг ваш sw его принимает
    rich_payload = {
        "id": task_id_value,
        "task_id": task_id_value,
        "draft_id": draft_id,
        "order_id": order_id,
        "order_number": number,
        "number": number,
        "supply_order_number": number,
        "supply_id": supply_id,
        "from_in_timezone": slot_from,
        "to_in_timezone": slot_to,
        "warehouse_id": supply_wid,
        "drop_off_id": DROP_ID,
        "timeslot": {
            "from_in_timezone": slot_from,
            "to_in_timezone": slot_to,
            "warehouse_id": supply_wid,
            "drop_off_id": DROP_ID,
        },
        "result": {
            "order_id": order_id,
            "order_number": number,
            "supply_id": supply_id,
        },
    }
    # добавим статус во все поля
    for f in BOOKED_STATUS_FIELDS:
        rich_payload[f] = BOOKED_STATUS_NAME

    # богатыми способами
    _call_sw("update_task", **rich_payload) or _call_sw("update_task", rich_payload)

    def _read_snapshot(tag: str) -> Tuple[Dict[str, Any], bool]:
        try:
            t2 = _find_task_by_draft(draft_id)
            if isinstance(t2, dict):
                snap = _task_status_snapshot(t2)
                logger.info("booking-result: task snapshot %s: %s", tag, snap)
                return snap, _is_timeslot_search(t2)
            return {}, False
        except Exception:
            return {}, False

    snap1, still_search = _read_snapshot("after rich")
    if not still_search:
        return

    # Если не сработало — пробуем “минимальные” обновления: одно поле статуса за раз, разными сигнатурами
    if task_id_value is None:
        logger.warning("booking-result: cannot progress status (no task id fields among %s)", TASK_ID_FIELDS)
        return

    # 1) Пробуем основной статус
    for status_field in BOOKED_STATUS_FIELDS:
        if _try_progress_status(task_id_value, status_field, BOOKED_STATUS_NAME):
            snap2, still_search = _read_snapshot(f"after {status_field}={BOOKED_STATUS_NAME}")
            if not still_search:
                return

    # 2) Пробуем альтернативные значения статуса
    for alt in BOOKED_ALT_STATUSES:
        for status_field in BOOKED_STATUS_FIELDS:
            if _try_progress_status(task_id_value, status_field, alt):
                snap3, still_search = _read_snapshot(f"after alt {status_field}={alt}")
                if not still_search:
                    return

    # Если всё равно не получилось — фиксируем подсказку
    logger.warning(
        "booking-result: task still looks like 'timeslot search'. "
        "Tried id fields=%s, status fields=%s, values=[%r + %r]. "
        "Adjust supply_watch to accept these fields/signatures or share its update API.",
        TASK_ID_FIELDS, BOOKED_STATUS_FIELDS, BOOKED_STATUS_NAME, BOOKED_ALT_STATUSES
    )


# ---------- poll supply/create ----------

def _poll_supply_create_status_sync(self: httpx.Client, base_url: str, op_id: str, headers: Optional[Dict[str, str]]) -> Tuple[Optional[Dict[str, Any]], Optional[int]]:
    url = base_url + "/v1/draft/supply/create/status"
    for attempt in range(1, 61):
        resp = _ORIG_SYNC_POST(self, url, json={"operation_id": op_id}, headers=headers)
        if resp.status_code != 200:
            logger.warning("supply/status HTTP %s: %s", resp.status_code, resp.text[:200])
        else:
            js = resp.json()
            status, order_id = _extract_status_and_order(js)
            logger.info("supply/status attempt=%d status=%r order_id=%s", attempt, status, order_id)
            if order_id is not None or _is_success_status(status):
                return js, order_id
        try:
            import time as _t; _t.sleep(2.0)
        except Exception:
            pass
    return None, None


async def _poll_supply_create_status_async(self: httpx.AsyncClient, base_url: str, op_id: str, headers: Optional[Dict[str, str]]) -> Tuple[Optional[Dict[str, Any]], Optional[int]]:
    url = base_url + "/v1/draft/supply/create/status"
    for attempt in range(1, 61):
        resp = await _ORIG_ASYNC_POST(self, url, json={"operation_id": op_id}, headers=headers)
        if resp.status_code != 200:
            logger.warning("supply/status(async) HTTP %s: %s", resp.status_code, resp.text[:200])
        else:
            js = resp.json()
            status, order_id = _extract_status_and_order(js)
            logger.info("supply/status(async) attempt=%d status=%r order_id=%s", attempt, status, order_id)
            if order_id is not None or _is_success_status(status):
                return js, order_id
        try:
            import asyncio; await asyncio.sleep(2.0)
        except Exception:
            pass
    return None, None


# ---------- auto-book ----------

def _maybe_autobook_supply_sync(self: httpx.Client, timeslot_url: str, req_js: Dict[str, Any], resp_data: Dict[str, Any], headers: Optional[Dict[str, str]], override_wid: Optional[int] = None) -> None:
    if not AUTO_BOOK:
        return
    try:
        draft_id = int(req_js.get("draft_id")) if req_js.get("draft_id") is not None else None
    except Exception:
        draft_id = None
    if not draft_id or draft_id in _BOOKED_DRAFTS:
        return

    drop_item, days_cnt = _find_drop_item(resp_data, DROP_ID)
    if not drop_item or days_cnt == 0:
        logger.info("auto-book: no slots for drop-off %s, skip", DROP_ID)
        return

    target_from, target_to = _extract_target_from_req(req_js)
    sel = _select_slot(drop_item, target_from, target_to)
    if not sel:
        logger.info("auto-book: cannot find any timeslot entries, skip")
        return
    f, t, how = sel

    supply_wid = _choose_supply_wid(req_js, draft_id, override=override_wid)
    if not supply_wid:
        logger.info("auto-book: cannot determine supply warehouse_id, skip")
        return
    if how != "first":
        logger.info("auto-book: using slot via selector (%s) %s..%s", how, f, t)

    base, _, _ = timeslot_url.partition("/v1/")
    create_url = base + "/v1/draft/supply/create"
    variants = _supply_create_payload_variants(draft_id, supply_wid, f, t)

    op_id = None
    for idx, payload in enumerate(variants, 1):
        logger.info("auto-book: POST %s variant #%s %s", create_url, idx, json.dumps(payload, ensure_ascii=False))
        resp = _ORIG_SYNC_POST(self, create_url, json=payload, headers=headers)
        if resp.status_code == 200:
            op_id = resp.json().get("operation_id")
            if op_id:
                break
        logger.warning("auto-book: supply/create HTTP %s: %s", resp.status_code, resp.text[:200])

    if not op_id:
        logger.warning("auto-book: supply/create failed for all payload variants")
        return

    st, order_id = _poll_supply_create_status_sync(self, base, op_id, headers)
    if st is None:
        logger.warning("auto-book: supply/create/status timeout")
        return

    status, _ = _extract_status_and_order(st)
    if order_id:
        _BOOKED_DRAFTS.add(draft_id)
        logger.info("auto-book: BOOKED draft_id=%s drop_off=%s supply_wid=%s order_id=%s slot=%s..%s (status=%r)",
                    draft_id, DROP_ID, supply_wid, order_id, f, t, status)

        details = _supply_order_get_sync(self, base, headers, order_id) or {}
        num, sid = _extract_supply_order_info(details or {})
        if REQUIRE_SUPPLY_ID and sid is None:
            logger.warning("supply_id is missing in supply-order/get response, but REQUIRE_SUPPLY_ID=true")
        _persist_booking_result(draft_id, order_id, num, sid, f, t, supply_wid)
    else:
        _BOOKED_DRAFTS.add(draft_id)
        logger.warning("auto-book: SUCCESS status without single order_id. Raw response: %s",
                       json.dumps(st, ensure_ascii=False)[:800])


async def _maybe_autobook_supply_async(self: httpx.AsyncClient, timeslot_url: str, req_js: Dict[str, Any], resp_data: Dict[str, Any], headers: Optional[Dict[str, str]], override_wid: Optional[int] = None) -> None:
    if not AUTO_BOOK:
        return
    try:
        draft_id = int(req_js.get("draft_id")) if req_js.get("draft_id") is not None else None
    except Exception:
        draft_id = None
    if not draft_id or draft_id in _BOOKED_DRAFTS:
        return

    drop_item, days_cnt = _find_drop_item(resp_data, DROP_ID)
    if not drop_item or days_cnt == 0:
        logger.info("auto-book(async): no slots for drop-off %s, skip", DROP_ID)
        return

    target_from, target_to = _extract_target_from_req(req_js)
    sel = _select_slot(drop_item, target_from, target_to)
    if not sel:
        logger.info("auto-book(async): cannot find any timeslot entries, skip")
        return
    f, t, how = sel

    supply_wid = _choose_supply_wid(req_js, draft_id, override=override_wid)
    if not supply_wid:
        logger.info("auto-book(async): cannot determine supply warehouse_id, skip")
        return
    if how != "first":
        logger.info("auto-book(async): using slot via selector (%s) %s..%s", how, f, t)

    base, _, _ = timeslot_url.partition("/v1/")
    create_url = base + "/v1/draft/supply/create"
    variants = _supply_create_payload_variants(draft_id, supply_wid, f, t)

    op_id = None
    for idx, payload in enumerate(variants, 1):
        logger.info("auto-book(async): POST %s variant #%s %s", create_url, idx, json.dumps(payload, ensure_ascii=False))
        resp = await _ORIG_ASYNC_POST(self, create_url, json=payload, headers=headers)
        if resp.status_code == 200:
            op_id = resp.json().get("operation_id")
            if op_id:
                break
        logger.warning("auto-book(async): supply/create HTTP %s: %s", resp.status_code, resp.text[:200])

    if not op_id:
        logger.warning("auto-book(async): supply/create failed for all payload variants")
        return

    st, order_id = await _poll_supply_create_status_async(self, base, op_id, headers)
    if st is None:
        logger.warning("auto-book(async): supply/create/status timeout")
        return

    status, _ = _extract_status_and_order(st)
    if order_id:
        _BOOKED_DRAFTS.add(draft_id)
        logger.info("auto-book(async): BOOKED draft_id=%s drop_off=%s supply_wid=%s order_id=%s slot=%s..%s (status=%r)",
                    draft_id, DROP_ID, supply_wid, order_id, f, t, status)

        details = await _supply_order_get_async(self, base, headers, order_id) or {}
        num, sid = _extract_supply_order_info(details or {})
        if REQUIRE_SUPPLY_ID and sid is None:
            logger.warning("supply_id is missing in supply-order/get response, but REQUIRE_SUPPLY_ID=true")
        _persist_booking_result(draft_id, order_id, num, sid, f, t, supply_wid)
    else:
        _BOOKED_DRAFTS.add(draft_id)
        logger.warning("auto-book(async): SUCCESS status without single order_id. Raw response: %s",
                       json.dumps(st, ensure_ascii=False)[:800])


# ---------- httpx patches ----------

def _install_sync_post_patch():
    def _patched_post(self: httpx.Client, url: str, *args, **kwargs) -> httpx.Response:
        try:
            if url.endswith("/v1/warehouse/fbo/list"):
                if STUB_FBO_LIST:
                    data = _make_fbo_stub_payload()
                    logger.info(
                        "stub /v1/warehouse/fbo/list -> synthetic 200 with warehouses=%s",
                        len(data.get("warehouses", []))
                    )
                    return _synthetic_json_response(url, 200, data)
                js = kwargs.get("json") or {}
                js = _patch_fbo_list_payload(js)
                kwargs["json"] = js
                return _ORIG_SYNC_POST(self, url, *args, **kwargs)

            if url.endswith("/v1/draft/create"):
                js = kwargs.get("json") or {}
                if DROP_ID and js.get("drop_off_point_warehouse_id") != int(DROP_ID):
                    js = deepcopy(js)
                    js["drop_off_point_warehouse_id"] = int(DROP_ID)
                    kwargs["json"] = js
                    logger.info("inject drop_off_point_warehouse_id=%s into draft/create", DROP_ID)
                    logger.info("final json(draft/create) payload: %s", json.dumps(js, ensure_ascii=False))
                return _ORIG_SYNC_POST(self, url, *args, **kwargs)

            if url.endswith("/v1/draft/create/info"):
                resp = _ORIG_SYNC_POST(self, url, *args, **kwargs)
                try:
                    j = resp.json()
                    cluster, primary = _extract_cluster_and_primary(j)
                    dr_id = None
                    try:
                        dr_id = int((kwargs.get("json") or {}).get("operation_id") or (kwargs.get("json") or {}).get("draft_id"))
                    except Exception:
                        dr_id = None
                    if cluster:
                        global _LAST_CLUSTER, _LAST_PRIMARY_WID
                        _LAST_CLUSTER = cluster
                        if primary is not None:
                            _LAST_PRIMARY_WID = int(primary)
                        logger.info("create/info: LAST_CLUSTER=%s, PRIMARY=%s", _LAST_CLUSTER, _LAST_PRIMARY_WID)
                    if dr_id:
                        if cluster:
                            _DRAFT_CLUSTER_CACHE[dr_id] = cluster
                        if primary is not None:
                            _DRAFT_PRIMARY_WID[dr_id] = int(primary)
                except Exception as e:
                    logger.warning("create/info cache update error: %s", e)
                return resp

            if url.endswith("/v1/draft/timeslot/info"):
                js = kwargs.get("json") or {}
                js = _build_timeslot_payload(js)
                js, changed = _normalize_warehouse_ids_with_cluster(js)
                if changed:
                    logger.warning("final json(timeslot/info) payload adjusted by cluster: %s", json.dumps(js, ensure_ascii=False))
                else:
                    logger.info("final json(timeslot/info) payload: %s", json.dumps(js, ensure_ascii=False))
                kwargs["json"] = js

                resp = _ORIG_SYNC_POST(self, url, *args, **kwargs)
                if resp.status_code != 200:
                    return resp

                try:
                    data = resp.json()
                    override_wid: Optional[int] = None

                    di, days_cnt = _find_drop_item(data, DROP_ID)
                    if (not DISABLE_TS_FALLBACK) and (di is None or days_cnt == 0):
                        draft_id = None
                        try:
                            draft_id = int(js.get("draft_id")) if js.get("draft_id") is not None else None
                        except Exception:
                            draft_id = None
                        r2, d2, wid2 = _timeslot_retry_sync(self, url, js, kwargs.get("headers"), draft_id)
                        if r2 is not None and d2 is not None and wid2 is not None:
                            resp = r2
                            data = d2
                            js["warehouse_ids"] = [int(wid2)]
                            override_wid = int(wid2)
                            logger.info("timeslot: using fallback warehouse_id=%s", wid2)

                    mod = _reorder_or_filter_to_drop_only(data)
                    if mod is not data:
                        resp = _apply_mod_and_rebuild_response(resp, mod)

                    try:
                        if not DISABLE_TS_FALLBACK:
                            _maybe_autobook_supply_sync(self, url, js, mod, headers=kwargs.get("headers"), override_wid=override_wid)
                    except Exception as e:
                        logger.warning("auto-book(sync) error: %s", e)

                    drop_item, _ = _find_drop_item(mod, DROP_ID)
                    if (not DISABLE_TS_FALLBACK) and drop_item is None and DISCOVER_DROPOFFS:
                        discover_js = deepcopy(js)
                        discover_js.pop("drop_off_warehouse_id", None)
                        logger.warning("timeslot discover: retry without drop_off to list available drop-offs")
                        fb_resp = _ORIG_SYNC_POST(self, url, json=discover_js, headers=kwargs.get("headers"))
                        if fb_resp.status_code == 200:
                            fb_data = fb_resp.json()
                            summary = _summarize_available_dropoffs(fb_data)
                            logger.warning("available drop-offs for warehouse_ids=%s: %s", js.get("warehouse_ids"), summary)
                        else:
                            logger.warning("timeslot discover HTTP %s: %s", fb_resp.status_code, fb_resp.text[:200])
                except Exception as e:
                    logger.warning("timeslot/info response adjust error: %s", e)

                return resp

        except Exception as e:
            logger.warning("httpx_timeslot_patch(sync) error: %s", e)
        return _ORIG_SYNC_POST(self, url, *args, **kwargs)

    httpx.Client.post = _patched_post  # type: ignore[method-assign]


def _install_async_post_patch():
    async def _patched_post(self: httpx.AsyncClient, url: str, *args, **kwargs) -> httpx.Response:
        try:
            if url.endswith("/v1/warehouse/fbo/list"):
                if STUB_FBO_LIST:
                    data = _make_fbo_stub_payload()
                    logger.info(
                        "stub /v1/warehouse/fbo/list(async) -> synthetic 200 with warehouses=%s",
                        len(data.get("warehouses", []))
                    )
                    return _synthetic_json_response(url, 200, data)
                js = kwargs.get("json") or {}
                js = _patch_fbo_list_payload(js)
                kwargs["json"] = js
                return await _ORIG_ASYNC_POST(self, url, *args, **kwargs)

            if url.endswith("/v1/draft/create"):
                js = kwargs.get("json") or {}
                if DROP_ID and js.get("drop_off_point_warehouse_id") != int(DROP_ID):
                    js = deepcopy(js)
                    js["drop_off_point_warehouse_id"] = int(DROP_ID)
                    kwargs["json"] = js
                    logger.info("inject drop_off_point_warehouse_id=%s into draft/create", DROP_ID)
                    logger.info("final json(draft/create) payload: %s", json.dumps(js, ensure_ascii=False))
                return await _ORIG_ASYNC_POST(self, url, *args, **kwargs)

            if url.endswith("/v1/draft/create/info"):
                resp = await _ORIG_ASYNC_POST(self, url, *args, **kwargs)
                try:
                    j = resp.json()
                    cluster, primary = _extract_cluster_and_primary(j)
                    dr_id = None
                    try:
                        dr_id = int((kwargs.get("json") or {}).get("operation_id") or (kwargs.get("json") or {}).get("draft_id"))
                    except Exception:
                        dr_id = None
                    if cluster:
                        global _LAST_CLUSTER, _LAST_PRIMARY_WID
                        _LAST_CLUSTER = cluster
                        if primary is not None:
                            _LAST_PRIMARY_WID = int(primary)
                        logger.info("create/info: LAST_CLUSTER=%s, PRIMARY=%s", _LAST_CLUSTER, _LAST_PRIMARY_WID)
                    if dr_id:
                        if cluster:
                            _DRAFT_CLUSTER_CACHE[dr_id] = cluster
                        if primary is not None:
                            _DRAFT_PRIMARY_WID[dr_id] = int(primary)
                except Exception as e:
                    logger.warning("create/info cache update error: %s", e)
                return resp

            if url.endswith("/v1/draft/timeslot/info"):
                js = kwargs.get("json") or {}
                js = _build_timeslot_payload(js)
                js, changed = _normalize_warehouse_ids_with_cluster(js)
                if changed:
                    logger.warning("final json(timeslot/info) payload adjusted by cluster: %s", json.dumps(js, ensure_ascii=False))
                else:
                    logger.info("final json(timeslot/info) payload: %s", json.dumps(js, ensure_ascii=False))
                kwargs["json"] = js

                resp = await _ORIG_ASYNC_POST(self, url, *args, **kwargs)
                if resp.status_code != 200:
                    return resp

                try:
                    data = resp.json()
                    override_wid: Optional[int] = None

                    di, days_cnt = _find_drop_item(data, DROP_ID)
                    if (not DISABLE_TS_FALLBACK) and (di is None or days_cnt == 0):
                        draft_id = None
                        try:
                            draft_id = int(js.get("draft_id")) if js.get("draft_id") is not None else None
                        except Exception:
                            draft_id = None
                        r2, d2, wid2 = await _timeslot_retry_async(self, url, js, kwargs.get("headers"), draft_id)
                        if r2 is not None and d2 is not None and wid2 is not None:
                            resp = r2
                            data = d2
                            js["warehouse_ids"] = [int(wid2)]
                            override_wid = int(wid2)
                            logger.info("timeslot(async): using fallback warehouse_id=%s", wid2)

                    mod = _reorder_or_filter_to_drop_only(data)
                    if mod is not data:
                        resp = _apply_mod_and_rebuild_response(resp, mod)

                    try:
                        if not DISABLE_TS_FALLBACK:
                            await _maybe_autobook_supply_async(self, url, js, mod, headers=kwargs.get("headers"), override_wid=override_wid)
                    except Exception as e:
                        logger.warning("auto-book(async) error: %s", e)

                    drop_item, _ = _find_drop_item(mod, DROP_ID)
                    if (not DISABLE_TS_FALLBACK) and drop_item is None and DISCOVER_DROPOFFS:
                        discover_js = deepcopy(js)
                        discover_js.pop("drop_off_warehouse_id", None)
                        logger.warning("timeslot discover(async): retry without drop_off to list available drop-offs")
                        new_kwargs = {k: v for k, v in kwargs.items() if k != "json"}
                        fb_resp = await _ORIG_ASYNC_POST(self, url, *args, json=discover_js, **new_kwargs)
                        if fb_resp.status_code == 200:
                            fb_data = fb_resp.json()
                            summary = _summarize_available_dropoffs(fb_data)
                            logger.warning("available drop-offs for warehouse_ids=%s: %s", js.get("warehouse_ids"), summary)
                        else:
                            logger.warning("timeslot discover(async) HTTP %s: %s", fb_resp.status_code, fb_resp.text[:200])
                except Exception as e:
                    logger.warning("timeslot/info response adjust error(async): %s", e)

                return resp

        except Exception as e:
            logger.warning("httpx_timeslot_patch(async) error: %s", e)
        return await _ORIG_ASYNC_POST(self, url, *args, **kwargs)

    httpx.AsyncClient.post = _patched_post  # type: ignore[method-assign]


def install():
    _install_sync_post_patch()
    _install_async_post_patch()
    logger.info(
        "httpx_timeslot_patch: installed (DROP_TZ=%s, DAYS=%s, DROP_ID=%s, STRICT_DROP_ONLY=%s, FBO_FILTER=%s, STUB_FBO_LIST=%s, USE_CLUSTER_WIDS=%s, DISCOVER_DROPOFFS=%s, DISABLE_TS_FALLBACK=%s, AUTO_BOOK=%s)",
        DROP_TZ,
        SLOT_SEARCH_DAYS,
        DROP_ID,
        STRICT_DROP_ONLY,
        FBO_LIST_SUPPLY_TYPES,
        STUB_FBO_LIST,
        USE_CLUSTER_WIDS,
        DISCOVER_DROPOFFS,
        DISABLE_TS_FALLBACK,
        AUTO_BOOK,
    )


try:
    install()
except Exception as e:
    logger.warning("httpx_timeslot_patch install failed: %s", e)