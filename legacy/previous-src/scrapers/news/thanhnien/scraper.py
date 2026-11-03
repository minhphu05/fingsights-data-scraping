from datetime import datetime
import re
from typing import Any
from bs4 import BeautifulSoup
import structlog
from src.core.base_scraper import BaseScraper

logger = structlog.get_logger(__name__)


class ThanhNienNewsScraper(BaseScraper):
    """Production scraper for Bao Thanh Nien Financial & Economy section."""

    def __init__(self):
        super().__init__(
            source_code="thanhnien_economy",
            name="Báo Thanh Niên Kinh Tế",
            category="news",
            base_url="https://thanhnien.vn",
            rate_limit_rps=2.0,
        )
        self.feed_urls = [
            "https://thanhnien.vn/rss/kinh-te.rss",
            "https://thanhnien.vn/rss/tai-chinh-kinh-doanh.rss",
        ]

    async def discover_urls(self) -> list[str]:
        urls = []
        for feed in self.feed_urls:
            try:
                resp = await self.http_client.get(feed)
                soup = BeautifulSoup(resp.content, "xml")
                for item in soup.find_all("item"):
                    link = item.find("link")
                    if link and link.text:
                        urls.append(link.text.strip())
            except Exception as exc:
                logger.warning("Error discovering Thanh Nien feed", feed=feed, error=str(exc))
        return list(dict.fromkeys(urls))

    async def scrape_item(self, url: str) -> dict[str, Any] | None:
        try:
            resp = await self.http_client.get(url)
            soup = BeautifulSoup(resp.content, "html.parser")

            title_elem = soup.find("h1", class_="detail-title") or soup.find("h1")
            title = title_elem.get_text(strip=True) if title_elem else None
            if not title:
                return None

            sapo_elem = soup.find("h2", class_="detail-sapo") or soup.find("div", class_="detail-sapo")
            summary = sapo_elem.get_text(strip=True) if sapo_elem else None

            content_elem = soup.find("div", class_="detail-content") or soup.find("div", id="contentDetail")
            paragraphs = []
            if content_elem:
                for p in content_elem.find_all("p"):
                    text = p.get_text(strip=True)
                    if text:
                        paragraphs.append(text)
            content_raw = "\n\n".join(paragraphs) if paragraphs else summary

            author_elem = soup.find("div", class_="author-name") or soup.find("span", class_="name")
            author = author_elem.get_text(strip=True) if author_elem else "Báo Thanh Niên"

            ticker_candidates = set(re.findall(r"\b([A-Z]{3})\b", f"{title} {summary}"))
            ignored = {"VND", "USD", "GDP", "CPI", "FED", "SSI", "CEO", "CTO", "BOD"}
            valid_tickers = [t for t in ticker_candidates if t not in ignored]

            return {
                "source_code": self.source_code,
                "url": url,
                "title": title,
                "summary": summary,
                "content_raw": content_raw,
                "author": author,
                "published_at": datetime.utcnow(),
                "tickers": valid_tickers,
                "metadata": {
                    "source": "Báo Thanh Niên",
                    "crawled_at": datetime.utcnow().isoformat(),
                },
            }
        except Exception as exc:
            logger.error("Thanh Nien parse error", url=url, error=str(exc))
            return None
