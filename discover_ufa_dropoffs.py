import os
import sys
import json
import time
import argparse
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Tuple, Optional

import httpx

UFA_CROSSDOCK_ID = 1020001836368000  # УФА_РФЦ_КРОССДОКИНГ

def as_utc_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def compute_window_utc(days: int) -> Tuple[str, str]:
    now = datetime.now(timezone.utc)
    start = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=days) - timedelta(seconds=1)
    return as_utc_iso(start), as_utc_iso(end)

def summarize_dropoffs(resp: Dict[str, Any]) -> List[Tuple[int, int]]:
    arr = resp.get("drop_off_warehouse_timeslots") or []
    out: List[Tuple[int, int]] = []
    for it in arr:
        did = it.get("drop_off_warehouse_id")
        try:
            did = int(did)
        except Exception:
            continue
        days = it.get("days") or []
        out.append((did, len(days) if isinstance(days, list) else 0))
    return out

def has_ufa_drop(resp: Dict[str, Any], require_positive_days: bool) -> bool:
    for did, cnt in summarize_dropoffs(resp):
        if did == UFA_CROSSDOCK_ID:
            return (cnt > 0) if require_positive_days else True
    return False

def parse_ids_csv(s: str) -> List[int]:
    out: List[int] = []
    for p in (s or "").split(","):
        p = p.strip()
        if not p:
            continue
        try:
            out.append(int(p))
        except Exception:
            pass
    return out

def load_dest_ids_from_env_map() -> List[int]:
    raw = os.getenv("SUPPLY_WAREHOUSE_MAP", "")
    ids: List[int] = []
    for pair in raw.split(","):
        if "=" not in pair:
            continue
        _, val = pair.split("=", 1)
        val = val.strip()
        try:
            ids.append(int(val))
        except Exception:
            pass
    return ids

def backoff_sleep(attempt: int, base: float = 1.5, cap: float = 10.0) -> None:
    delay = min(cap, base * (2 ** (attempt - 1)))
    time.sleep(delay)

def main():
    ap = argparse.ArgumentParser(description="Discover destinations that offer drop-off UFA_CROSSDOCK in OZON.")
    ap.add_argument("--draft-id", type=int, required=True, help="Existing draft_id from /v1/draft/create")
    ap.add_argument("--dest-ids", type=str, default="", help="CSV of destination warehouse_ids to probe")
    ap.add_argument("--days", type=int, default=7, help="Horizon in days from tomorrow 00:00 UTC")
    ap.add_argument("--require-positive-days", action="store_true", help="If set, require days>0 (not just presence)")
    args = ap.parse_args()

    client_id = os.getenv("OZON_CLIENT_ID", "").strip()
    api_key = os.getenv("OZON_API_KEY", "").strip()
    if not client_id or not api_key:
        print("Set OZON_CLIENT_ID and OZON_API_KEY in env", file=sys.stderr)
        sys.exit(2)

    dest_ids = parse_ids_csv(args.dest_ids)
    if not dest_ids:
        dest_ids = load_dest_ids_from_env_map()
    if not dest_ids:
        print("No destination ids provided (use --dest-ids or SUPPLY_WAREHOUSE_MAP).", file=sys.stderr)
        sys.exit(2)

    df, dt = compute_window_utc(args.days)

    headers = {
        "Client-Id": client_id,
        "Api-Key": api_key,
        "Content-Type": "application/json",
    }

    url = "https://api-seller.ozon.ru/v1/draft/timeslot/info"
    offered: List[Dict[str, Any]] = []
    not_offered: List[int] = []

    with httpx.Client(headers=headers, timeout=20) as http:
        for wid in dest_ids:
            body = {
                "draft_id": args.draft_id,
                "date_from": df,
                "date_to": dt,
                "warehouse_ids": [int(wid)],
            }

            attempts = 0
            while True:
                attempts += 1
                r = http.post(url, json=body)
                if r.status_code in (429, 500, 502, 503, 504) and attempts <= 5:
                    backoff_sleep(attempts)
                    continue
                break

            if r.status_code != 200:
                not_offered.append(wid)
                continue

            data = r.json()
            if has_ufa_drop(data, args.require_positive_days):
                days_map = {did: cnt for did, cnt in summarize_dropoffs(data)}
                offered.append({
                    "destination_id": wid,
                    "ufa_offered": True,
                    "ufa_days": days_map.get(UFA_CROSSDOCK_ID, 0),
                    "all_dropoffs": days_map,
                })
            else:
                not_offered.append(wid)

    result = {
        "draft_id": args.draft_id,
        "drop_off": UFA_CROSSDOCK_ID,
        "require_positive_days": bool(args.require_positive_days),
        "horizon_days": args.days,
        "offered_destinations": offered,
        "not_offered_destinations": not_offered,
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
