# -*- coding: utf-8 -*-
"""
bot_order_fill.py
Набор хендлеров aiogram для "ручного" заполнения недостающих данных заявки Ozon:
- Проставление тайм-слота (по команде, если нужно).
- Ввод транспорта: "ТРАНСПОРТ <short_task_id|task_id> <текст>"
- Ввод контакта:   "КОНТАКТ <short_task_id|task_id> <телефон> [Имя]"

Интеграция:
from bot_order_fill import register_order_fill_handlers
register_order_fill_handlers(dp, notify_text, supply_watch_module)

Где:
- dp — Dispatcher aiogram
- notify_text(chat_id: int, text: str) — async функция отправки текста
- supply_watch_module — импортированный модуль supply_watch (чтобы обратиться к async set_order_vehicle_from_chat/ set_order_contact_from_chat / update_task / list_tasks)
"""

import re
from typing import Callable, Awaitable, Any, Optional

from aiogram import types, Dispatcher

TASK_ID_RE = re.compile(r"^[0-9a-fA-F\-]{8,}$")

def _resolve_task_id(sw, token: str) -> Optional[str]:
    """
    Разрешает короткий или полный task_id.
    token может быть short(id) (первые 8 до '-') или полный uuid.
    """
    token = token.strip()
    if TASK_ID_RE.match(token) and "-" in token and len(token) > 12:
        # полный uuid
        return token
    # иначе ищем по short
    for t in sw.list_tasks():
        tid = t.get("id", "")
        if tid and tid.split("-")[0] == token:
            return tid
    return None

def register_order_fill_handlers(dp: Dispatcher, notify_text: Callable[[int, str], Awaitable[Any]], sw) -> None:
    """
    Регистрирует хендлеры:
    - /order_timeslot <task_id|short> — принудительно проставить тайм-слот в заявке (если доступно)
    - Сообщение вида "ТРАНСПОРТ <task_id|short> <текст>"
    - Сообщение вида "КОНТАКТ <task_id|short> <телефон> [Имя]"
    """

    @dp.message_handler(commands=["order_timeslot"])
    async def cmd_order_timeslot(message: types.Message):
        args = (message.get_args() or "").strip().split()
        if len(args) < 1:
            await message.reply("Использование: /order_timeslot <task_id|short>")
            return
        token = args[0]
        tid = _resolve_task_id(sw, token)
        if not tid:
            await message.reply("Задача не найдена по указанному идентификатору.")
            return
        t = sw.get_task(tid)
        if not t:
            await message.reply("Задача не найдена.")
            return
        if not t.get("order_id"):
            await message.reply("order_id ещё неизвестен для этой задачи.")
            return

        # используем уже выбранное окно
        fr = t.get("from_in_timezone") or t.get("desired_from_iso")
        to = t.get("to_in_timezone") or t.get("desired_to_iso")
        if not (fr and to):
            await message.reply("Не найдено окно для проставления.")
            return

        api = sw.OzonApi(sw.OZON_CLIENT_ID, sw.OZON_API_KEY, timeout=sw.API_TIMEOUT_SECONDS)
        ok, err, status = await sw.api_supply_order_timeslot_set(api, t["order_id"], fr, to)
        if ok:
            t["order_timeslot_set_ok"] = True
            sw.update_task(t)
            await message.reply(f"Тайм-слот установлен. Продолжаю опрос заявки…")
        else:
            await message.reply(f"Не удалось установить тайм-слот: {err or status}")

    @dp.message_handler(lambda m: m.text and m.text.strip().upper().startswith("ТРАНСПОРТ "))
    async def msg_vehicle(message: types.Message):
        text = message.text.strip()
        # Формат: ТРАНСПОРТ <task_id|short> <любой текст после>
        m = re.match(r"^(?i:ТРАНСПОРТ)\s+(\S+)\s+(.+)$", text)
        if not m:
            await message.reply("Формат: ТРАНСПОРТ <task_id|short> <госномер и описание>")
            return
        token = m.group(1)
        veh_text = m.group(2).strip()
        tid = _resolve_task_id(sw, token)
        if not tid:
            await message.reply("Задача не найдена по указанному идентификатору.")
            return
        ok, msg = await sw.set_order_vehicle_from_chat(tid, veh_text, notify_text)
        if ok:
            await message.reply("Транспорт принят. Ждём подтверждение от Ozon.")
        else:
            await message.reply(f"Не удалось сохранить транспорт: {msg}")

    @dp.message_handler(lambda m: m.text and m.text.strip().upper().startswith("КОНТАКТ "))
    async def msg_contact(message: types.Message):
        text = message.text.strip()
        # Формат: КОНТАКТ <task_id|short> <телефон> [Имя ...]
        m = re.match(r"^(?i:КОНТАКТ)\s+(\S+)\s+(\+?[0-9\-\s\(\)]{6,})\s*(.*)$", text)
        if not m:
            await message.reply("Формат: КОНТАКТ <task_id|short> <телефон> [Имя]")
            return
        token = m.group(1)
        phone = m.group(2).strip()
        name = (m.group(3) or "").strip()
        tid = _resolve_task_id(sw, token)
        if not tid:
            await message.reply("Задача не найдена по указанному идентификатору.")
            return
        ok, msg = await sw.set_order_contact_from_chat(tid, phone, name, notify_text)
        if ok:
            await message.reply("Контакт принят. Ждём подтверждение от Ozon.")
        else:
            await message.reply(f"Не удалось сохранить контакт: {msg}")