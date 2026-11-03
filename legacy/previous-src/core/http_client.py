import asyncio
from typing import Any
import httpx
import structlog
from src.config.settings import settings

logger = structlog.get_logger(__name__)


class AsyncHttpClient:
    """Resilient Async HTTP Client with rate limiting and exponential retry."""

    def __init__(
        self,
        base_url: str = "",
        timeout: float | None = None,
        rate_limit_rps: float | None = None,
        custom_headers: dict[str, str] | None = None,
    ):
        self.base_url = base_url
        self.timeout = timeout or settings.scraper_timeout_seconds
        self.rate_limit_rps = rate_limit_rps or settings.scraper_rate_limit_rps
        self._delay = 1.0 / self.rate_limit_rps if self.rate_limit_rps > 0 else 0.0

        headers = {
            "User-Agent": settings.scraper_user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
            "Connection": "keep-alive",
        }
        if custom_headers:
            headers.update(custom_headers)

        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            headers=headers,
            timeout=httpx.Timeout(self.timeout),
            follow_redirects=True,
            http2=True,
        )

    async def get(self, url: str, params: dict[str, Any] | None = None, max_retries: int | None = None) -> httpx.Response:
        retries = max_retries or settings.scraper_max_retries
        last_error = None

        for attempt in range(1, retries + 1):
            try:
                if self._delay > 0:
                    await asyncio.sleep(self._delay)

                response = await self._client.get(url, params=params)
                response.raise_for_status()
                return response
            except (httpx.RequestError, httpx.HTTPStatusError) as exc:
                last_error = exc
                wait_time = 2**attempt
                logger.warning(
                    "HTTP request failed",
                    url=url,
                    attempt=attempt,
                    retries=retries,
                    error=str(exc),
                    retry_in=wait_time,
                )
                if attempt < retries:
                    await asyncio.sleep(wait_time)

        logger.error("HTTP request completely failed after all retries", url=url, error=str(last_error))
        raise last_error

    async def close(self) -> None:
        await self._client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
