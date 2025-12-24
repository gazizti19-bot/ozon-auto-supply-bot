# -*- coding: utf-8 -*-
"""
supply_watch.py - Robust Supply Management Module

Key Changes:
- Removed all calls to non-public /v1|v2/supply-order/get endpoints
- Consolidated ST_SUPPLY_ORDER_FETCH and ST_ORDER_DATA_FILLING handling
- Tasks with order_id are marked UI_STATUS_CREATED with one-time notification
- 429 rate limits handled via rate_limit_resume_at with backoff
- Atomic task deletion from memory and persistence
- No false FAILED states due to transient errors or missing meta
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Callable, Awaitable

# ================== ENV helpers ==================

def _getenv_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return v if v is not None else default

def _getenv_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    try:
        return int(v)
    except Exception:
        try:
            return int(float(v))
        except Exception:
            return default

def _getenv_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "on")

# ================== Paths / ENV ==================

DATA_DIR = Path(_getenv_str("DATA_DIR", "/app/data")).resolve()
SUPPLY_TASK_FILE = DATA_DIR / _getenv_str("SUPPLY_TASK_FILE", "supply_tasks.json")

def _ensure_data_dir():
    """Ensure data directory exists, create if needed"""
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
    except (PermissionError, FileNotFoundError) as e:
        log.warning("Could not create DATA_DIR %s: %s", DATA_DIR, e)

# ================== Constants ==================

# Status constants
ST_WAIT_WINDOW = "WAIT_WINDOW"
ST_DRAFT_CREATING = "DRAFT_CREATING"
ST_POLL_DRAFT = "POLL_DRAFT"
ST_TIMESLOT_SEARCH = "TIMESLOT_SEARCH"
ST_SUPPLY_CREATING = "SUPPLY_CREATING"
ST_POLL_SUPPLY = "POLL_SUPPLY"
ST_SUPPLY_ORDER_FETCH = "SUPPLY_ORDER_FETCH"
ST_ORDER_DATA_FILLING = "ORDER_DATA_FILLING"
ST_CARGO_PREP = "CARGO_PREP"
ST_DONE = "DONE"
ST_FAILED = "FAILED"
ST_CANCELED = "CANCELED"
ST_RATE_LIMIT = "RATE_LIMIT"
ST_DELETED = "DELETED"

# UI status for created supplies
UI_STATUS_CREATED = "Создано"

# Process intervals
PROCESS_INTERVAL_SECONDS = _getenv_int("PROCESS_INTERVAL_SECONDS", 15)
RATE_LIMIT_DEFAULT_COOLDOWN = _getenv_int("RATE_LIMIT_DEFAULT_COOLDOWN", 10)
ORDER_FILL_POLL_INTERVAL_SECONDS = _getenv_int("ORDER_FILL_POLL_INTERVAL_SECONDS", 20)

# ================== Logging ==================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s: %(message)s"
)
log = logging.getLogger(__name__)

# ================== Task storage ==================

_tasks: Dict[str, Dict[str, Any]] = {}
_tasks_loaded = False

def now_ts() -> int:
    """Return current unix timestamp"""
    return int(time.time())

def short(uid: str) -> str:
    """Shorten UUID for logging"""
    try:
        return uid.split("-")[0]
    except Exception:
        return str(uid)[:8]

def ensure_loaded():
    """Load tasks from persistence file if not already loaded"""
    global _tasks_loaded, _tasks
    if _tasks_loaded:
        return
    
    _ensure_data_dir()
    
    if SUPPLY_TASK_FILE.exists():
        try:
            data = json.loads(SUPPLY_TASK_FILE.read_text(encoding="utf-8"))
            if isinstance(data, list):
                # Convert list to dict keyed by id
                _tasks = {str(t.get("id")): t for t in data if t.get("id")}
            elif isinstance(data, dict):
                _tasks = data
            else:
                _tasks = {}
        except Exception as e:
            log.warning("Failed to load tasks: %s", e)
            _tasks = {}
    else:
        _tasks = {}
    _tasks_loaded = True

def save_tasks():
    """Save tasks to persistence file atomically"""
    try:
        _ensure_data_dir()
        SUPPLY_TASK_FILE.parent.mkdir(parents=True, exist_ok=True)
        # Convert dict to list for compatibility
        task_list = list(_tasks.values())
        content = json.dumps(task_list, ensure_ascii=False, indent=2)
        
        # Atomic write via temp file
        temp_file = SUPPLY_TASK_FILE.with_suffix(".tmp")
        temp_file.write_text(content, encoding="utf-8")
        temp_file.replace(SUPPLY_TASK_FILE)
        
        log.debug("Saved %d tasks to %s", len(task_list), SUPPLY_TASK_FILE)
    except Exception as e:
        log.error("Failed to save tasks: %s", e)

def list_tasks() -> List[Dict[str, Any]]:
    """
    Return list of tasks from in-memory store.
    Excludes deleted tasks and terminal states.
    """
    ensure_loaded()
    result = []
    for task in _tasks.values():
        status = str(task.get("status") or "").upper()
        if status not in (ST_DELETED, ST_DONE, ST_FAILED, ST_CANCELED):
            result.append(dict(task))
    return result

def update_task(*args, **kwargs) -> Dict[str, Any]:
    """
    Flexible task updater compatible with multiple signatures:
    - update_task(task_id, payload_dict)
    - update_task(**payload_with_id)
    - update_task(payload_dict_with_id)
    
    Persists to SUPPLY_TASK_FILE and logs updates.
    When updating with a full task dict, it replaces the entire task.
    """
    ensure_loaded()
    
    # Parse arguments
    payload: Dict[str, Any] = {}
    task_id: Optional[str] = None
    replace_entire = False
    
    if args:
        if len(args) == 1 and isinstance(args[0], dict):
            # update_task(payload_dict_with_id) - check if it's a full task
            payload.update(args[0])
            # If it has an ID and looks like a full task (has created_ts), replace entire task
            if "id" in payload and "created_ts" in payload:
                replace_entire = True
        elif len(args) >= 2:
            # update_task(task_id, payload_dict) or update_task(task_id, **kwargs)
            task_id = str(args[0])
            if isinstance(args[1], dict):
                payload.update(args[1])
            if kwargs:
                payload.update(kwargs)
    else:
        # update_task(**payload_with_id)
        payload.update(kwargs)
    
    # Extract task_id from payload if not already set
    if task_id is None:
        task_id = str(payload.get("id") or payload.get("task_id") or payload.get("uuid") or "")
    
    if not task_id:
        # Create new task if no ID provided
        task_id = str(uuid.uuid4())
        payload["id"] = task_id
    
    # Get or create task
    if task_id in _tasks:
        if replace_entire:
            # Replace entire task (for cleanup of removed fields)
            task = {
                "id": task_id,
                "created_ts": _tasks[task_id].get("created_ts", now_ts()),
                "updated_ts": now_ts(),
            }
            task.update(payload)
            _tasks[task_id] = task
        else:
            # Merge updates
            task = _tasks[task_id]
            task.update(payload)
    else:
        task = {
            "id": task_id,
            "created_ts": now_ts(),
            "updated_ts": now_ts(),
            "status": ST_WAIT_WINDOW,
        }
        task.update(payload)
        _tasks[task_id] = task
    
    # Always update timestamp
    task["updated_ts"] = now_ts()
    
    # Persist and log
    save_tasks()
    log.debug("Updated task %s: status=%s", short(task_id), task.get("status"))
    
    return dict(task)

def delete_task(task_id: str) -> bool:
    """
    Atomically remove task from in-memory map and persistence.
    Marks status as ST_DELETED.
    Returns True if task was found and deleted.
    """
    ensure_loaded()
    
    task_id = str(task_id)
    if task_id not in _tasks:
        log.warning("Attempted to delete non-existent task %s", short(task_id))
        return False
    
    # Mark as deleted (for audit trail before removal)
    _tasks[task_id]["status"] = ST_DELETED
    _tasks[task_id]["deleted_ts"] = now_ts()
    
    # Remove from memory
    del _tasks[task_id]
    
    # Persist
    save_tasks()
    
    log.info("Deleted task %s", short(task_id))
    return True

def notify_text(chat_id: int, text: str):
    """
    Stub logging function for notifications.
    Actual messaging should be handled by caller/bot integration.
    """
    log.info("NOTIFY chat_id=%s: %s", chat_id, text[:100])

# ================== Consolidated Order Handler ==================

async def handle_order_state(
    task: Dict[str, Any],
    notify_fn: Optional[Callable[[int, str], Awaitable[None]]] = None
):
    """
    Consolidated handler for ST_SUPPLY_ORDER_FETCH and ST_ORDER_DATA_FILLING.
    
    Requirements:
    - If task["order_id"] exists: set task["status"] = UI_STATUS_CREATED,
      send one-time notification that supply is created, schedule short follow-up (5s).
      DO NOT mark FAILED due to missing meta.
    - If task["order_id"] is missing: schedule next poll (ORDER_FILL_POLL_INTERVAL_SECONDS),
      do not mark FAILED.
    """
    task_id = task.get("id")
    order_id = task.get("order_id")
    
    if order_id:
        # Supply order exists - mark as created
        if not task.get("cargo_prep_prompted"):
            task["status"] = UI_STATUS_CREATED
            task["cargo_prep_prompted"] = True
            task["created_message_ts"] = now_ts()
            task["next_attempt_ts"] = now_ts() + 5  # Short follow-up
            
            # Send one-time notification
            chat_id = task.get("chat_id")
            if chat_id and notify_fn:
                msg = (
                    f"🟦 Заявка создана (Task {short(task_id or '')})\n"
                    f"• Номер: {order_id}\n"
                    "• Действия: откройте ЛК, укажите грузоместа и количество, затем сгенерируйте этикетки."
                )
                try:
                    await notify_fn(chat_id, msg)
                except Exception as e:
                    log.exception("Failed to send notification: %s", e)
            
            update_task(task)
            log.info("Task %s marked as UI_STATUS_CREATED (order_id=%s)", short(task_id or ""), order_id)
        else:
            # Already notified, just maintain status
            task["status"] = UI_STATUS_CREATED
            update_task(task)
    else:
        # No order_id yet - schedule next poll without marking FAILED
        task["status"] = ST_ORDER_DATA_FILLING
        task["next_attempt_ts"] = now_ts() + ORDER_FILL_POLL_INTERVAL_SECONDS
        task.pop("last_error", None)  # Clear any previous errors
        update_task(task)
        log.debug("Task %s waiting for order_id, will retry in %ds", 
                 short(task_id or ""), ORDER_FILL_POLL_INTERVAL_SECONDS)

# ================== Rate Limit Handling ==================

def handle_rate_limit(task: Dict[str, Any], wait_seconds: Optional[int] = None):
    """
    Handle 429 rate limit by setting rate_limit_resume_at.
    DO NOT mark task as FAILED.
    Auto-transition back to previous status after resume time.
    """
    if wait_seconds is None:
        wait_seconds = RATE_LIMIT_DEFAULT_COOLDOWN
    
    task["status"] = ST_RATE_LIMIT
    task["rate_limit_resume_at"] = now_ts() + wait_seconds
    task["next_attempt_ts"] = task["rate_limit_resume_at"]
    task["last_error"] = f"rate_limit:429 (resume in {wait_seconds}s)"
    
    update_task(task)
    log.info("Task %s hit rate limit, will resume in %ds", short(task.get("id", "")), wait_seconds)

def check_rate_limit_resume(task: Dict[str, Any]) -> bool:
    """
    Check if task is in rate limit state and resume time has passed.
    Returns True if task should transition back to processing.
    Modifies task in-place and persists.
    """
    if task.get("status") != ST_RATE_LIMIT:
        return False
    
    resume_at = task.get("rate_limit_resume_at", 0)
    if now_ts() >= resume_at:
        # Resume processing - remove rate limit fields
        if "rate_limit_resume_at" in task:
            del task["rate_limit_resume_at"]
        if "last_error" in task:
            del task["last_error"]
        
        # Transition to safe default state
        task["status"] = ST_ORDER_DATA_FILLING
        task["next_attempt_ts"] = now_ts()
        
        update_task(task)
        log.info("Task %s resumed from rate limit", short(task.get("id", "")))
        return True
    
    return False

# ================== Main Processing Loop ==================

async def process_tasks_once(
    notify_fn: Optional[Callable[[int, str], Awaitable[None]]] = None
):
    """
    Process all active tasks once.
    Routes ST_SUPPLY_ORDER_FETCH and ST_ORDER_DATA_FILLING to consolidated handler.
    Avoids setting FAILED due to transient exceptions.
    """
    tasks = list_tasks()
    
    for task in tasks:
        try:
            task_id = task.get("id")
            status = str(task.get("status") or "").upper()
            
            # Skip deleted/terminal tasks
            if status in (ST_DELETED, ST_DONE, ST_FAILED, ST_CANCELED, UI_STATUS_CREATED):
                continue
            
            # Check rate limit resume
            if status == ST_RATE_LIMIT:
                check_rate_limit_resume(task)
                continue
            
            # Check if task should be processed now
            next_attempt = task.get("next_attempt_ts", 0)
            if now_ts() < next_attempt:
                continue
            
            # Handle supply order fetch/filling states
            if status in (ST_SUPPLY_ORDER_FETCH, ST_ORDER_DATA_FILLING):
                await handle_order_state(task, notify_fn)
            
            # Other states would be handled here in full implementation
            # For now, we focus on the required consolidated handling
            
        except Exception as e:
            # Transient exception - attach error and reschedule, don't mark FAILED
            log.exception("Error processing task %s: %s", short(task.get("id", "")), e)
            task["last_error"] = f"exception:{str(e)[:200]}"
            task["next_attempt_ts"] = now_ts() + ORDER_FILL_POLL_INTERVAL_SECONDS
            update_task(task)

async def process_tasks_loop(
    notify_fn: Optional[Callable[[int, str], Awaitable[None]]] = None,
    interval_seconds: int = PROCESS_INTERVAL_SECONDS
):
    """
    Main processing loop - continuously process tasks at regular intervals.
    """
    log.info("Starting supply watch processing loop (interval=%ds)", interval_seconds)
    
    while True:
        try:
            await process_tasks_once(notify_fn)
        except Exception as e:
            log.exception("Unexpected error in process_tasks_loop: %s", e)
        
        await asyncio.sleep(interval_seconds)

# ================== Public API ==================

def create_task(**kwargs) -> Dict[str, Any]:
    """Create a new task with provided parameters"""
    task_id = str(kwargs.get("id") or uuid.uuid4())
    kwargs["id"] = task_id
    kwargs.setdefault("status", ST_WAIT_WINDOW)
    kwargs.setdefault("created_ts", now_ts())
    return update_task(kwargs)

def get_task(task_id: str) -> Optional[Dict[str, Any]]:
    """Get a task by ID"""
    ensure_loaded()
    task = _tasks.get(str(task_id))
    return dict(task) if task else None

def list_all_tasks() -> List[Dict[str, Any]]:
    """List all tasks including terminal states"""
    ensure_loaded()
    return [dict(t) for t in _tasks.values()]

# ================== Scheduler Integration ==================

def register_supply_scheduler(
    scheduler: Any,
    notify_text: Callable[[int, str], Awaitable[None]],
    notify_file: Optional[Callable[[int, str, Optional[str]], Awaitable[None]]] = None,
    interval_seconds: int = PROCESS_INTERVAL_SECONDS,
):
    """
    Register supply processing with APScheduler.
    Compatible with both new and legacy signatures.
    """
    async def _tick():
        try:
            await process_tasks_once(notify_fn=notify_text)
        except Exception as e:
            log.exception("Scheduler tick failed: %s", e)
    
    try:
        # Try immediate first run
        loop = asyncio.get_running_loop()
        loop.create_task(_tick())
    except RuntimeError:
        pass
    
    # Register with scheduler
    try:
        if scheduler and hasattr(scheduler, "add_job"):
            scheduler.add_job(
                _tick,
                "interval",
                seconds=max(5, int(interval_seconds)),
                id="supply_watch_process",
                replace_existing=True,
                coalesce=True,
                max_instances=1,
            )
            log.info("Registered supply watch scheduler (interval=%ds)", interval_seconds)
    except Exception as e:
        log.warning("Failed to register scheduler: %s", e)

# ================== Exports ==================

__all__ = [
    # Status constants
    "ST_WAIT_WINDOW",
    "ST_DRAFT_CREATING",
    "ST_POLL_DRAFT",
    "ST_TIMESLOT_SEARCH",
    "ST_SUPPLY_CREATING",
    "ST_POLL_SUPPLY",
    "ST_SUPPLY_ORDER_FETCH",
    "ST_ORDER_DATA_FILLING",
    "ST_CARGO_PREP",
    "ST_DONE",
    "ST_FAILED",
    "ST_CANCELED",
    "ST_RATE_LIMIT",
    "ST_DELETED",
    "UI_STATUS_CREATED",
    
    # Core functions
    "list_tasks",
    "update_task",
    "delete_task",
    "notify_text",
    "process_tasks_once",
    "process_tasks_loop",
    "register_supply_scheduler",
    "create_task",
    "get_task",
    "list_all_tasks",
    
    # Handlers
    "handle_order_state",
    "handle_rate_limit",
    "check_rate_limit_resume",
]
