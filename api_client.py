import asyncio
from typing import Optional
import aiohttp
from aiohttp import ClientTimeout
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)
from loguru import logger

from contracts import FactCheckApiResponse


class FactCheckApiClient:
    BASE_URL = "https://factchecktools.googleapis.com/v1alpha1/claims:search"

    DEFAULT_REQUESTS_PER_MINUTE = 300
    DEFAULT_MAX_CONCURRENT = 10
    RATE_LIMIT_WINDOW_SECONDS = 60
    REQUEST_TIMEOUT_SECONDS = 30
    RETRY_AFTER_DEFAULT_SECONDS = 60
    RETRY_MAX_ATTEMPTS = 5
    RETRY_BACKOFF_MIN_SECONDS = 4
    RETRY_BACKOFF_MAX_SECONDS = 60
    RETRY_BACKOFF_MULTIPLIER = 1
    LOG_PROGRESS_EVERY_N_PAGES = 10

    def __init__(
        self,
        api_key: str,
        max_concurrent: int = DEFAULT_MAX_CONCURRENT,
        requests_per_minute: int = DEFAULT_REQUESTS_PER_MINUTE,
    ):
        self.api_key = api_key
        self.max_concurrent = max_concurrent
        self.requests_per_minute = requests_per_minute

        self.session: Optional[aiohttp.ClientSession] = None
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.rate_limiter = asyncio.Semaphore(requests_per_minute)
        self.rate_limit_reset_task: Optional[asyncio.Task] = None

        self.total_requests = 0

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        self.rate_limit_reset_task = asyncio.create_task(
            self._reset_rate_limit())
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):  # noqa: ANN001
        if self.rate_limit_reset_task:
            self.rate_limit_reset_task.cancel()
            try:
                await self.rate_limit_reset_task
            except asyncio.CancelledError:
                pass
        if self.session:
            await self.session.close()

    async def _reset_rate_limit(self):
        while True:
            await asyncio.sleep(self.RATE_LIMIT_WINDOW_SECONDS)
            for _ in range(self.requests_per_minute):
                try:
                    self.rate_limiter.release()
                except ValueError:
                    break

    @retry(
        retry=retry_if_exception_type(
            (aiohttp.ClientError, asyncio.TimeoutError)),
        wait=wait_exponential(
            multiplier=RETRY_BACKOFF_MULTIPLIER,
            min=RETRY_BACKOFF_MIN_SECONDS,
            max=RETRY_BACKOFF_MAX_SECONDS,
        ),
        stop=stop_after_attempt(RETRY_MAX_ATTEMPTS),
        reraise=True,
    )
    async def fetch_page(
        self,
        query: Optional[str] = None,
        publisher_filter: Optional[str] = None,
        page_token: Optional[str] = None,
        page_size: int = 100,
        language_code: Optional[str] = None,
        max_age_days: Optional[int] = None,
    ) -> FactCheckApiResponse:
        params: dict[str, str | int] = {"key": self.api_key}

        if query:
            params["query"] = query
        if publisher_filter:
            params["reviewPublisherSiteFilter"] = publisher_filter
        if page_token:
            params["pageToken"] = page_token
        if page_size:
            params["pageSize"] = page_size
        if language_code:
            params["languageCode"] = language_code
        if max_age_days:
            params["maxAgeDays"] = max_age_days

        assert self.session is not None, "Session not initialized"

        async with self.rate_limiter:
            async with self.semaphore:
                self.total_requests += 1
                timeout = ClientTimeout(total=self.REQUEST_TIMEOUT_SECONDS)
                async with self.session.get(
                    self.BASE_URL, params=params, timeout=timeout
                ) as response:
                    if response.status == 429:
                        retry_after = int(
                            response.headers.get(
                                "Retry-After", self.RETRY_AFTER_DEFAULT_SECONDS
                            )
                        )
                        logger.warning(f"Rate limited. Waiting {retry_after}s")
                        await asyncio.sleep(retry_after)
                        raise aiohttp.ClientError("Rate limited")

                    response.raise_for_status()
                    data = await response.json()
                    return FactCheckApiResponse.model_validate(data)

    async def fetch_all_pages(
        self,
        query: Optional[str] = None,
        publisher_filter: Optional[str] = None,
        page_size: int = 100,
        language_code: Optional[str] = None,
        max_age_days: Optional[int] = None,
    ) -> list[FactCheckApiResponse]:
        responses = []
        page_token = None
        page_count = 0

        while True:
            try:
                response = await self.fetch_page(
                    query=query,
                    publisher_filter=publisher_filter,
                    page_token=page_token,
                    page_size=page_size,
                    language_code=language_code,
                    max_age_days=max_age_days,
                )

                responses.append(response)
                page_count += 1

                if page_count % self.LOG_PROGRESS_EVERY_N_PAGES == 0:
                    logger.debug(f"Fetched {page_count} pages")

                page_token = response.next_page_token
                if not page_token:
                    break

            except Exception as e:
                logger.error(f"Error fetching page {page_count + 1}: {e}")
                break

        return responses
