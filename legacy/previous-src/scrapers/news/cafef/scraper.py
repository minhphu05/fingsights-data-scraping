from datetime import datetime
import re
from typing import Any
from bs4 import BeautifulSoup
import structlog
from src.core.base_scraper import BaseScraper

logger = structlog.get_logger(__name__)


class CafeFNewsScraper(BaseScraper):
    """Production scraper for CafeF Financial News (Chung khoan & Doanh nghiep)."""

    def __init__(self):
        super().__init__(
            source_code="cafef_news",
            name="CafeF Thị trường & Doanh nghiệp",
            category="news",
            base_url="https://cafef.vn",
            rate_limit_rps=2.0,
        )
        self.feed_urls = [
            "https://cafef.vn/thi-truong-chung-khoan.rss",
            "https://cafef.vn/doanh-nghiep.rss",
            "https://cafef.vn/tai-chinh-ngan-hang.rss",
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
                logger.warning("Error discovering CafeF feed", feed=feed, error=str(exc))
        return list(dict.fromkeys(urls))

    async def scrape_item(self, url: str) -> dict[str, Any] | None:
        try:
            resp = await self.http_client.get(url)
            soup = BeautifulSoup(resp.content, "html.parser")

            title_elem = soup.find("h1", class_="title") or soup.find("h1")
            title = title_elem.get_text(strip=True) if title_elem else None
            if not title:
                return None

            sapo_elem = soup.find("h2", class_="sapo")
            summary = sapo_elem.get_text(strip=True) if sapo_elem else None

            content_elem = soup.find("div", class_="contentdetail") or soup.find("div", id="mainContent")
            paragraphs = []
            if content_elem:
                for p in content_elem.find_all("p"):
                    text = p.get_text(strip=True)
                    if text:
                        paragraphs.append(text)
            content_raw = "\n\n".join(paragraphs) if paragraphs else summary

            # Author / Source
            author_elem = soup.find("span", class_="author") or soup.find("p", class_="author")
            author = author_elem.get_text(strip=True) if author_elem else "CafeF"

            # Published at
            date_elem = soup.find("span", class_="pdate")
            published_at = datetime.utcnow()
            if date_elem:
                raw_date = date_elem.get_text(strip=True)
                # Parse format: 02-11-2026 - 15:30 PM
                match = re.search(r"(\d{2})[-/](\d{2})[-/](\d{4})", raw_date)
                if match:
                    d, m, y = match.groups()
                    try:
                        published_at = datetime(int(y), int(m), int(d))
                    except ValueError:
                        pass

            # Extract potential VN stock tickers (3 uppercase letters)
            ticker_candidates = set(re.findall(r"\b([A-Z]{3})\b", f"{title} {summary}"))
            # Filter non-tickers
            ignored = {"VND", "USD", "GDP", "CPI", "FED", "SSI", "CEO", "CTO", "BOD", "AGM", "EGM"}
            valid_tickers = [t for t in ticker_candidates if t not in ignored]

            return {
                "source_code": self.source_code,
                "url": url,
                "title": title,
                "summary": summary,
                "content_raw": content_raw,
                "author": author,
                "published_at": published_at,
                "tickers": valid_tickers,
                "metadata": {
                    "source": "CafeF",
                    "crawled_at": datetime.utcnow().isoformat(),
                },
            }
        except Exception as exc:
            logger.error("CafeF parse error", url=url, error=str(exc))
            return None
