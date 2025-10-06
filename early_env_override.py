# -*- coding: utf-8 -*-
"""
Раннее принудительное окружение.
ДОЛЖЕН импортироваться ПЕРВЫМ в entrypoint (main.py), до любых патчей/логики.
"""
import os

# Полностью гасим автобукинг у timeslot-патча, даже если .env/compose где-то переопределяют
os.environ["AUTO_BOOK"] = "0"

# Базовые дефолты (можете переопределить в .env)
os.environ.setdefault("DATA_DIR", "/app/data")
os.environ.setdefault("TIMEZONE", "Asia/Yekaterinburg")

# Не фиксируем секунду слота, чтобы работала авто-ротация (можно разкомментировать временно)
# os.environ.setdefault("OZON_CREATE_SLOT_SEC", "11")