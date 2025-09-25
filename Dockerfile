FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Базовые пакеты (tzdata/ca-certificates для таймзоны и SSL)
RUN apt-get update && apt-get install -y --no-install-recommends \
    tzdata ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Устанавливаем зависимости
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Копируем весь код приложения (в т.ч. supply_integration.py и supply_watch.py)
COPY . /app

# Создаём каталоги данных
RUN mkdir -p /app/data/keys /app/ca

# Переменные окружения по умолчанию
ENV DATA_DIR=/app/data

# Точка входа
CMD ["python", "-u", "bot.py"]