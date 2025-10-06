# Выполнить внутри контейнера: python -u heal_tasks.py
import json, os, time, sys

data_dir = os.getenv("DATA_DIR", "/app/data")
path = os.path.join(data_dir, "supply_tasks.json")
now = int(time.time())
changed = 0

try:
    with open(path, "r", encoding="utf-8") as f:
        tasks = json.load(f)
except Exception as e:
    print("Cannot read", path, e)
    sys.exit(1)

for t in tasks:
    st = str(t.get("status") or "").upper()
    if st in ("SUPPLY_CREATING", "CARGO_CREATING", "LABELS_CREATING"):
        if t.get("creating") is True:
            # Сбрасываем застрявший флаг и таймеры, чтобы тик сразу взял задачу
            t["creating"] = False
            t.pop("creating_since_ts", None)
            t["next_attempt_ts"] = 0
            t["retry_after_ts"] = 0
            t["updated_ts"] = now
            changed += 1

if changed:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(tasks, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

print(f"healed creating flags: {changed}")