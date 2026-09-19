import asyncio
import logging
import random
import time
from collections.abc import Awaitable, Callable
from typing import Any

import httpx

from .config import DatasetConfig, Settings


class AcquisitionError(RuntimeError):
    def __init__(self, message: str, retryable: bool = True) -> None:
        super().__init__(message)
        self.retryable = retryable


class NSEClient:
    def __init__(self, settings: Settings, transport: httpx.AsyncBaseTransport | None = None, logger: logging.Logger | None = None) -> None:
        self.settings = settings
        self.logger = logger
        self.client = httpx.AsyncClient(timeout=settings.request_timeout_seconds, headers={"User-Agent": settings.user_agent, "Accept": "application/json,text/plain,*/*"}, transport=transport)
        self._request_lock = asyncio.Lock()
        self._last_request = 0.0
        self._refresh_lock = asyncio.Lock()
        self._bootstrap_lock = asyncio.Lock()
        self._bootstrapped_landings: set[str] = set()
        self.retry_counts: dict[str, int] = {}
        self.durations: dict[str, float] = {}

    async def close(self) -> None:
        await self.client.aclose()

    async def _rate_limit(self) -> None:
        async with self._request_lock:
            delay = self.settings.rate_limit_seconds - (asyncio.get_running_loop().time() - self._last_request)
            if delay > 0:
                await asyncio.sleep(delay)
            self._last_request = asyncio.get_running_loop().time()

    async def bootstrap(self, landing_url: str, force: bool = False) -> None:
        async with self._bootstrap_lock:
            if not force and landing_url in self._bootstrapped_landings:
                return
            await self._rate_limit()
            response = await self.client.get(landing_url)
            response.raise_for_status()
            self._bootstrapped_landings.add(landing_url)

    async def _refresh(self, landing_url: str) -> None:
        async with self._refresh_lock:
            if self.logger:
                self.logger.info("refreshing NSE session", extra={"context": {"landing_url": landing_url}})
            await self.bootstrap(landing_url, force=True)

    async def fetch(self, dataset: DatasetConfig) -> Any:
        started = time.perf_counter()
        for request in dataset.requests:
            self.retry_counts[request.api_url] = 0
        last_error: Exception | None = None
        payloads: list[tuple[Any, str | None]] = []
        try:
            for request_index, request in enumerate(dataset.requests):
                request_succeeded = False
                for attempt in range(self.settings.max_retries + 1):
                    attempt_number = attempt + 1
                    try:
                        if self.logger:
                            self.logger.info("starting dataset request", extra={"context": {"dataset": request.api_url, "attempt": attempt_number}})
                        await self.bootstrap(dataset.landing_url)
                        await self._rate_limit()
                        response = await self.client.get(request.api_url, headers={"Referer": dataset.landing_url})
                        if response.status_code in (401, 403):
                            await self._refresh(dataset.landing_url)
                            raise AcquisitionError(f"HTTP {response.status_code}; session refreshed")
                        if response.status_code == 429 or response.status_code == 408 or response.status_code >= 500:
                            raise AcquisitionError(f"transient HTTP {response.status_code}")
                        if response.status_code >= 400:
                            raise AcquisitionError(f"permanent HTTP {response.status_code}", retryable=False)
                        response.raise_for_status()
                        try:
                            payloads.append((response.json(), request.category))
                            request_succeeded = True
                            break
                        except ValueError as exc:
                            raise AcquisitionError("malformed JSON response", retryable=False) from exc
                    except AcquisitionError as exc:
                        last_error = exc
                        if not exc.retryable or attempt >= self.settings.max_retries:
                            break
                        self.retry_counts[request.api_url] += 1
                        delay = min(8.0, 0.5 * (2**attempt)) + random.uniform(0, 0.25)
                        if self.logger:
                            self.logger.warning("retrying dataset request", extra={"context": {"dataset": request.api_url, "attempt": attempt_number, "next_attempt": attempt_number + 1, "delay_seconds": round(delay, 3), "error": str(exc)}})
                        await asyncio.sleep(delay)
                    except httpx.HTTPError as exc:
                        last_error = exc
                        if attempt >= self.settings.max_retries:
                            break
                        self.retry_counts[request.api_url] += 1
                        delay = min(8.0, 0.5 * (2**attempt)) + random.uniform(0, 0.25)
                        if self.logger:
                            self.logger.warning("retrying dataset request", extra={"context": {"dataset": request.api_url, "attempt": attempt_number, "next_attempt": attempt_number + 1, "delay_seconds": round(delay, 3), "error": str(exc)}})
                        await asyncio.sleep(delay)
                if not request_succeeded:
                    raise AcquisitionError(str(last_error) if last_error else "request failed")
            return payloads[0][0] if len(payloads) == 1 and payloads[0][1] is None else payloads
        finally:
            self.durations[dataset.api_urls[0]] = round(time.perf_counter() - started, 3)


async def fetch_all(client: NSEClient, datasets: dict[str, DatasetConfig], on_result: Callable[[str, Any, Exception | None], Awaitable[None]]) -> None:
    async def one(name: str, config: DatasetConfig) -> None:
        try:
            await on_result(name, await client.fetch(config), None)
        except Exception as exc:
            await on_result(name, None, exc)

    await asyncio.gather(*(one(name, config) for name, config in datasets.items()))
