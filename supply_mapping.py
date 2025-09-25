# -*- coding: utf-8 -*-
"""
supply_mapping.py
Модуль для:
- Нормализации названий складов в единый ключ.
- Разбора SUPPLY_WAREHOUSE_MAP из ENV.
- Резолва warehouse_id по введённому названию склада.

Конфигурация:
- SUPPLY_WAREHOUSE_MAP: строка вида "Имя1=ID1;Имя2=ID2;..." или "Имя1=ID1,Имя2=ID2,..."
  (поддерживаются оба разделителя: ';' и ',')
- SUPPLY_NORMALIZE_STRATEGY: AS_IS | UPPER_UNDERSCORE (по умолчанию UPPER_UNDERSCORE)
"""

from __future__ import annotations

import os
import re
from typing import Dict, Optional, Tuple


def _to_upper_and_underscore(s: str) -> str:
    """
    Нормализация:
    - trim
    - заменить 'ё' -> 'е'
    - все пробелы/дефисы/длинные тире/точки/слэши -> '_'
    - сжать подряд идущие '_' до одного
    - убрать лидирующие/замыкающие '_'
    - привести к UPPERCASE
    """
    s = s.strip()
    if not s:
        return s
    s = s.replace("ё", "е").replace("Ё", "Е")
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[ \-\—\.\/]+", "_", s)
    s = re.sub(r"[^\w\u0400-\u04FF]+", "", s, flags=re.UNICODE)
    s = re.sub(r"_+", "_", s).strip("_")
    return s.upper()


def normalize_name(name: str, strategy: Optional[str] = None) -> str:
    """
    Возвращает нормализованный ключ склада.
    strategy:
      - "AS_IS": только trim
      - "UPPER_UNDERSCORE": см. _to_upper_and_underscore
    """
    if name is None:
        return ""
    st = (strategy or os.getenv("SUPPLY_NORMALIZE_STRATEGY", "UPPER_UNDERSCORE")).upper()
    if st == "AS_IS":
        return name.strip()
    return _to_upper_and_underscore(name)


def parse_supply_map(env_value: str, strategy: Optional[str] = None) -> Dict[str, int]:
    """
    Разбирает SUPPLY_WAREHOUSE_MAP в dict: normalized_name -> warehouse_id (int).
    Поддерживает разделители ';' и ','.
    Пропускает пары с нечисловым ID.
    """
    mapping: Dict[str, int] = {}
    if not env_value:
        return mapping

    # Разделяем по ';' или ',' (одно или несколько подряд)
    parts = [p for p in re.split(r"[;,]+", env_value) if p.strip()]
    for pair in parts:
        if "=" not in pair:
            continue
        raw_name, raw_id = pair.split("=", 1)
        key = normalize_name(raw_name, strategy=strategy)
        wid_str = raw_id.strip()
        try:
            wid = int(wid_str)
        except ValueError:
            continue
        if key:
            mapping[key] = wid
    return mapping


# Глобальный кэш карты из ENV
_SUPPLY_MAP_RAW = os.getenv("SUPPLY_WAREHOUSE_MAP", "")
_SUPPLY_MAP: Dict[str, int] = parse_supply_map(_SUPPLY_MAP_RAW)


def reload_from_env() -> None:
    """Перечитать карту из ENV (если .env изменили и контейнер перезапускать не хотите)."""
    global _SUPPLY_MAP, _SUPPLY_MAP_RAW
    _SUPPLY_MAP_RAW = os.getenv("SUPPLY_WAREHOUSE_MAP", "")
    _SUPPLY_MAP = parse_supply_map(_SUPPLY_MAP_RAW)


def resolve_warehouse_id(name: str, strategy: Optional[str] = None) -> Optional[int]:
    """
    Возвращает warehouse_id по названию склада.
    Нормализует и сравнивает с ключами из SUPPLY_WAREHOUSE_MAP.
    """
    if not name:
        return None
    key = normalize_name(name, strategy=strategy)
    return _SUPPLY_MAP.get(key)


def available_keys(original: bool = False, strategy: Optional[str] = None) -> Tuple[str, ...]:
    """
    Возвращает кортеж доступных ключей для подсказки пользователю.
    Если original=False — вернёт нормализованные ключи (как в карте).
    Если original=True — попытается восстановить «как есть» из исходной строки ENV.
    """
    if not original:
        return tuple(sorted(_SUPPLY_MAP.keys()))
    raw = _SUPPLY_MAP_RAW
    out = []
    for pair in [p for p in re.split(r"[;,]+", raw) if p.strip()]:
        if "=" not in pair:
            continue
        nm, _ = pair.split("=", 1)
        out.append(nm.strip())
    return tuple(sorted(out))


if __name__ == "__main__":
    import sys
    args = sys.argv[1:]
    if not args:
        print("Usage: python -m supply_mapping <название_склада> [ещё_название...]")
        print("Strategy:", os.getenv("SUPPLY_NORMALIZE_STRATEGY", "UPPER_UNDERSCORE"))
        print("Доступные ключи (нормализованные):", ", ".join(available_keys()))
        sys.exit(0)
    for raw in args:
        wid = resolve_warehouse_id(raw)
        print(f"{raw} -> {wid}")