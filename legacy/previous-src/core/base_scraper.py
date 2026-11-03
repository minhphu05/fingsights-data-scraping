from abc import ABC, abstractmethod
from typing import Any
import structlog
from src.core.deduplicator import UrlDeduplicator
from src.core.http_client import AsyncHttpClient
from src.core.mq_publisher import MessagePublisher

logger = structlog.get_logger(__name__)


class BaseScraper(ABC):
    """Abstract Base Class for all News and Stock Scrapers."""

    def __init__(
        self,
        source_code: str,
        name: str,
        category: str,
        base_url: str,
        rate_limit_rps: float = 2.0,
    ):
        self.source_code = source_code
        self.name = name
        self.category = category
        self.base_url = base_url
        self.rate_limit_rps = rate_limit_rps

        self.http_client = AsyncHttpClient(base_url=self.base_url, rate_limit_rps=self.rate_limit_rps)
        self.deduplicator = UrlDeduplicator()
        self.publisher = MessagePublisher()

    @abstractmethod
    async def discover_urls(self) -> list[str]:
        """Discover latest item URLs (from RSS, listing pages, or API feeds)."""
        pass

    @abstractmethod
    async def scrape_item(self, url: str) -> dict[str, Any] | None:
        """Fetch and parse detailed data from a specific item URL."""
        pass

    async def run(self, max_items: int = 50) -> dict[str, Any]:
        """Execute standard scraper workflow: discover -> filter unseen -> scrape -> publish."""
        logger.info("Starting scrape run", source=self.source_code, name=self.name)
        discovered = await self.discover_urls()
        unseen_urls = self.deduplicator.filter_unseen_urls(self.source_code, discovered)

        target_urls = unseen_urls[:max_items]
        logger.info(
            "URLs discovered",
            source=self.source_code,
            total_discovered=len(discovered),
            unseen=len(unseen_urls),
            processing=len(target_urls),
        )

        scraped_items = []
        failed_count = 0

        for url in target_urls:
            try:
                item = await self.scrape_item(url)
                if item:
                    scraped_items.append(item)
                    self.deduplicator.mark_seen(self.source_code, url)
                    routing_key = f"{self.category}.{self.source_code}.scraped"
                    self.publisher.publish(routing_key, item)
            except Exception as exc:
                failed_count += 1
                logger.error("Failed scraping item", source=self.source_code, url=url, error=str(exc))

        await self.http_client.close()

        summary = {
            "source_code": self.source_code,
            "total_discovered": len(discovered),
            "unseen_count": len(unseen_urls),
            "scraped_count": len(scraped_items),
            "failed_count": failed_count,
            "items": scraped_items,
        }
        logger.info("Finished scrape run", **summary)
        return summary
