from __future__ import annotations
from typing import Tuple, Optional
from datetime import datetime, timedelta, timezone
import zoneinfo

def _to_dt(s: str) -> datetime:
    # Допускаем 'Z' или чистый ISO
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)

def _normalize_iso(dt: datetime) -> str:
    # всегда в UTC ISO с 'Z'
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def now_in_tz(tz_name: str) -> datetime:
    tz = zoneinfo.ZoneInfo(tz_name)
    return datetime.now(tz)

def roll_window_forward(
    from_iso: str,
    to_iso: str,
    tz_name: str,
    min_lead_minutes: int,
    max_days_ahead: int
) -> Tuple[str, str, bool, str]:
    """
    Если окно прошло или слишком близко (< min_lead), переносим на ближайший день вперёд.
    Возвращает (new_from_iso, new_to_iso, rolled, reason)
    """
    try:
        dt_from = _to_dt(from_iso)
        dt_to = _to_dt(to_iso)
    except Exception:
        return from_iso, to_iso, False, "parse_failed"

    local_now = now_in_tz(tz_name)
    # приводим dt_from/dt_to в TZ склада (если они без TZ — считаем UTC)
    if dt_from.tzinfo is None:
        dt_from = dt_from.replace(tzinfo=timezone.utc)
    if dt_to.tzinfo is None:
        dt_to = dt_to.replace(tzinfo=timezone.utc)

    # Разница до начала окна
    lead = (dt_from.astimezone(local_now.tzinfo) - local_now).total_seconds() / 60.0
    rolled = False
    reason = "ok"

    if lead < min_lead_minutes or dt_to <= local_now:
        # Перекатываем на следующий день, сохранив длительность
        duration = dt_to - dt_from
        new_start = local_now + timedelta(minutes=min_lead_minutes)
        # округлить до целого часа (опционально)
        new_start = new_start.replace(minute=0, second=0, microsecond=0)
        new_end = new_start + duration

        # если слишком далеко — ограничить
        for _ in range(max_days_ahead):
            if (new_start - local_now).total_seconds() / 60.0 >= min_lead_minutes:
                break
            new_start += timedelta(days=1)
            new_end += timedelta(days=1)

        dt_from = new_start
        dt_to = new_end
        rolled = True
        reason = "rolled_forward"

    return _normalize_iso(dt_from), _normalize_iso(dt_to), rolled, reason