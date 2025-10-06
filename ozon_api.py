import httpx
import json
import logging
import time
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)

class OzonApi:
    def __init__(self, client_id: str, api_key: str, timeout: int = 25, max_retries: int = 3, backoff: float = 1.7):
        self.client_id = client_id
        self.api_key = api_key
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff = backoff
        self._session = httpx.AsyncClient(timeout=timeout, headers={
            "Client-Id": client_id,
            "Api-Key": api_key,
            "Content-Type": "application/json",
        })

    async def json_request(
        self,
        method: str,
        path: str,
        json: Any = None,
    ) -> Tuple[bool, int, Optional[Dict[str, Any]], Optional[str], str]:
        """
        Возвращает: ok, status_code, parsed_json (или None), err_message, raw_text
        Никогда НЕ прячет сырое тело при ошибках.
        Для /v1/draft/create отключаем внутренние ретраи 429 (пусть зовущий код сам решает).
        """
        url = f"https://api-seller.ozon.ru{path}"
        attempt = 0
        disable_retry = (path == "/v1/draft/create")

        while True:
            attempt += 1
            try:
                resp = await self._session.request(method, url, json=json)
            except Exception as e:
                if disable_retry:
                    return False, 0, None, f"http_error:{e}", ""
                if attempt >= self.max_retries:
                    return False, 0, None, f"http_error:{e}", ""
                delay = min(self.backoff * attempt, 8.0)
                logger.warning("HTTP network error %s -> retry in %.2fs (attempt=%s)", e, delay, attempt)
                await asyncio.sleep(delay)
                continue

            status = resp.status_code
            raw_text = resp.text
            parsed = None
            if raw_text:
                try:
                    parsed = resp.json()
                except Exception:
                    parsed = None

            # Лог (только при ошибках или debug)
            if status >= 400:
                logger.debug("Ozon %s %s -> %s body=%s", method, path, status, raw_text[:400])

            # Специально для draft_create: не ретраим внутри
            if disable_retry:
                return (200 <= status < 300), status, parsed, None, raw_text

            # Общая логика retries (кроме черновика)
            if status == 429:
                if attempt >= self.max_retries:
                    return False, status, parsed, "rate_limit", raw_text
                delay = min(self.backoff * attempt, 10.0)
                logger.warning("Ozon 429 %s %s attempt=%s sleep=%.2fs", method, path, attempt, delay)
                time.sleep(delay)
                continue

            if 500 <= status < 600:
                if attempt >= self.max_retries:
                    return False, status, parsed, f"http_{status}", raw_text
                delay = min(self.backoff * attempt, 8.0)
                logger.warning("Ozon %s server %s -> retry in %.2fs (attempt=%s)", path, status, delay, attempt)
                time.sleep(delay)
                continue

            # обычный возврат
            ok = 200 <= status < 300
            return ok, status, parsed, None, raw_text