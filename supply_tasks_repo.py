from __future__ import annotations
import json
import os
import uuid
import threading
from typing import Dict, Any, List, Optional
from supply_state import SupplyStatus, now_ts

LOCK = threading.Lock()


class SupplyTasksRepo:
    def __init__(self, path: str = "supply_tasks.json"):
        self.path = path
        if not os.path.exists(self.path):
            self._save({"tasks": []})

    def _load(self) -> Dict[str, Any]:
        if not os.path.exists(self.path):
            return {"tasks": []}
        with open(self.path, "r", encoding="utf-8") as f:
            try:
                return json.load(f)
            except Exception:
                return {"tasks": []}

    def _save(self, data: Dict[str, Any]):
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, self.path)

    def list(self) -> List[Dict[str, Any]]:
        with LOCK:
            return list(self._load().get("tasks", []))

    def get(self, task_id: str) -> Optional[Dict[str, Any]]:
        with LOCK:
            for t in self._load().get("tasks", []):
                if t.get("id") == task_id:
                    return t
        return None

    def upsert(self, task: Dict[str, Any]):
        with LOCK:
            data = self._load()
            tasks = data.get("tasks", [])
            for i, t in enumerate(tasks):
                if t.get("id") == task["id"]:
                    tasks[i] = task
                    break
            else:
                tasks.append(task)
            data["tasks"] = tasks
            self._save(data)

    def new_task(self,
                 warehouse_id: int,
                 dropoff_warehouse_id: int,
                 desired_from_iso: str,
                 desired_to_iso: str,
                 sku_items: list,
                 chat_id: int = 0) -> Dict[str, Any]:
        t = {
            "id": str(uuid.uuid4()),
            "created_at": now_ts(),
            "status": SupplyStatus.DRAFT_CREATING.value,
            "warehouse_id": warehouse_id,
            "dropoff_warehouse_id": dropoff_warehouse_id,
            "desired_from_iso": desired_from_iso,
            "desired_to_iso": desired_to_iso,
            "sku_items": sku_items,
            "chat_id": chat_id,
            "history": [],
        }
        self.upsert(t)
        return t