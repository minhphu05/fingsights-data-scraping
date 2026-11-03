from __future__ import annotations

import asyncio
from datetime import date, datetime
import json
import logging
import os
import re
from typing import Any
from urllib.parse import urljoin, urlparse
from zoneinfo import ZoneInfo

from bs4 import BeautifulSoup
import httpx

from ...domain.models import Article, ContentBlock
from ...domain.windows import DateWindow
from .cafef import CafeFClient, IncompleteArchiveError, UnusableArticleError

logger = logging.getLogger("finsights.sources.news.baomoi")


class BaoMoiClient(CafeFClient):
    """High-throughput client for Baomoi finance news with SSR Next.js hydration extraction."""

    def __init__(self, config: dict[str, Any], security_matcher=None, timeout_seconds: float = 30.0):
        super().__init__(config, security_matcher=security_matcher, timeout_seconds=timeout_seconds)
        self.rate = float(os.getenv("BAOMOI_REQUEST_RATE_PER_SECOND", config.get("request_rate_per_second", 0.0)))
        self.max_pages = int(os.getenv(
            "BAOMOI_MAX_PAGES_PER_CATEGORY",
            config.get("max_pages_per_category", config.get("discovery", {}).get("max_pages", 1000))
        ))
        self.base_url = str(config.get("base_url", "https://baomoi.com")).rstrip("/")

    async def _find_start_page(self, category: dict[str, Any], end_date: date) -> int:
        """Binary search page index when target date is significantly in the past."""
        template = category.get("page_template")
        if not template:
            return 1
        low, high = 1, min(self.max_pages, 500)
        best_page = 1
        while low <= high:
            mid = (low + high) // 2
            page_url = template.format(page=mid)
            try:
                response = await self._get(page_url)
                soup = BeautifulSoup(response.content, "lxml")
                items = self._extract_listing_items(soup, page_url)
                if not items:
                    high = mid - 1
                    continue
                page_dates = [it[1].date() for it in items]
                if max(page_dates) <= end_date:
                    best_page = mid
                    high = mid - 1
                else:
                    low = mid + 1
            except Exception:
                high = mid - 1
        return max(1, best_page - 2)

    def _extract_listing_items(self, soup: BeautifulSoup, page_url: str) -> list[tuple[str, datetime, dict[str, Any]]]:
        """Extract article URL, publication datetime, and metadata from __NEXT_DATA__ or HTML cards."""
        items: list[tuple[str, datetime, dict[str, Any]]] = []
        script = soup.find("script", id="__NEXT_DATA__")
        if script and script.string:
            try:
                data = json.loads(script.string)
                sections = data.get("props", {}).get("pageProps", {}).get("resp", {}).get("data", {}).get("content", {}).get("sections", [])
                for sec in sections:
                    for it in sec.get("items", []):
                        raw_url = str(it.get("url") or "")
                        if not raw_url:
                            continue
                        clean_url = urljoin(page_url, raw_url.split("#")[0])
                        raw_date = it.get("date")
                        if raw_date:
                            dt = datetime.fromtimestamp(int(raw_date), tz=ZoneInfo(self.timezone_name))
                            items.append((clean_url, dt, it))
                if items:
                    return items
            except Exception:
                pass

        # Fallback to HTML cards if __NEXT_DATA__ is unavailable
        regex = re.compile(str(self.config.get("article_id_regex", r"-(?P<article_id>c\d+)\.epi$")), re.IGNORECASE)
        for a in soup.find_all("a", href=True):
            href = str(a["href"])
            if not regex.search(href):
                continue
            clean_url = urljoin(page_url, href.split("#")[0])
            card = a.find_parent("div", class_=lambda c: c and ("group/card" in c or "bm-card" in c))
            pub_dt = None
            if card:
                t = card.find("time")
                if t and t.get("datetime"):
                    try:
                        pub_dt = datetime.fromisoformat(str(t["datetime"]))
                    except Exception:
                        pass
            if pub_dt is None:
                # Fallback to now if no date present on card
                pub_dt = datetime.now(ZoneInfo(self.timezone_name))
            items.append((clean_url, pub_dt, {"title": a.get_text(strip=True), "url": clean_url}))
        return items

    async def collect(
        self,
        window: DateWindow,
        on_batch: Any | None = None,
    ) -> list[Article]:
        """Stream news day-by-day directly into persistence to maintain zero memory footprint."""
        discovered: dict[str, tuple[str, str]] = {}
        seen_article_urls: set[str] = set()
        results: dict[str, Article] = {}
        day_buffer: dict[date, list[Article]] = {}
        processed_uids: set[str] = set()

        def _flush_days(min_date_cutoff: date | None = None, category_code: str | None = None):
            for pub_date in sorted(list(day_buffer.keys()), reverse=True):
                if min_date_cutoff is None or pub_date > min_date_cutoff:
                    batch = day_buffer.pop(pub_date, [])
                    if batch:
                        if on_batch:
                            try:
                                on_batch(pub_date, batch, category_code)
                            except TypeError:
                                on_batch(pub_date, batch)
                        else:
                            for a in batch:
                                results[a.source_article_id] = a

        today = datetime.now(ZoneInfo(self.timezone_name)).date()
        categories = self.config["discovery"]["archive_categories"]
        for category in categories:
            cat_code = category.get("code", "archive")
            category_complete = False
            saw_any_articles = False
            template = category.get("page_template")

            start_page = 1
            if (today - window.end_date).days > 2:
                start_page = await self._find_start_page(category, window.end_date)

            for page_number in range(start_page, self.max_pages + 1):
                page_url = category.get("first_page") if page_number == 1 else template.format(page=page_number)
                try:
                    response = await self._get(page_url)
                except Exception:
                    break

                soup = BeautifulSoup(response.content, "lxml")
                items = self._extract_listing_items(soup, page_url)
                if not items:
                    if page_number == 1:
                        raise IncompleteArchiveError(f"No article cards found for Baomoi archive {cat_code}")
                    break

                saw_any_articles = True
                page_dates = [it[1] for it in items]

                for article_url, article_dt, raw_item in items:
                    discovered.setdefault(article_url, (cat_code, "archive"))
                    article_date = article_dt.date()

                    # Pre-filter outside requested window
                    if article_date > window.end_date or article_date < window.start_date:
                        continue

                    if article_url in seen_article_urls:
                        continue
                    seen_article_urls.add(article_url)

                    try:
                        article = await self.fetch_article(article_url, category=cat_code, listing_item=raw_item)
                        if window.contains(article.published_at) and self.is_financial(article):
                            if article.source_article_id not in processed_uids:
                                processed_uids.add(article.source_article_id)
                                day_buffer.setdefault(article_date, []).append(article)
                    except (UnusableArticleError, httpx.HTTPError):
                        continue

                # Flush days older than newest if page processed
                if page_dates:
                    _flush_days(min_date_cutoff=min(page_dates).date(), category_code=cat_code)

                # Stop when every item on the page is older than start_date
                if page_dates and max(page_dates).date() < window.start_date:
                    category_complete = True
                    break

            _flush_days(min_date_cutoff=None, category_code=cat_code)

            if not saw_any_articles or not category_complete:
                if self.config.get("discovery", {}).get("fail_if_archive_boundary_not_proven", False):
                    raise IncompleteArchiveError(
                        f"Baomoi archive {cat_code} did not prove coverage before {window.start_date}"
                    )

        _flush_days(min_date_cutoff=None)
        return list(results.values())

    async def fetch_article(
        self,
        url: str,
        category: str | None = None,
        listing_item: dict[str, Any] | None = None,
    ) -> Article:
        clean_url = url.split("#")[0]
        match = re.search(str(self.config["article_id_regex"]), urlparse(clean_url).path, re.IGNORECASE)
        if not match:
            raise UnusableArticleError(f"No Baomoi article ID in {clean_url}")
        source_article_id = match.group("article_id")

        response = await self._get(clean_url)
        soup = BeautifulSoup(response.content, "lxml")

        title = None
        summary = None
        published_at = None
        modified_at = None
        author = None
        original_source_url = None
        cover_image_url = None
        blocks: list[ContentBlock] = []

        script = soup.find("script", id="__NEXT_DATA__")
        if script and script.string:
            try:
                data = json.loads(script.string)
                c = data.get("props", {}).get("pageProps", {}).get("resp", {}).get("data", {}).get("content", {})
                if c:
                    title = c.get("title")
                    summary = c.get("description")
                    if c.get("date"):
                        published_at = datetime.fromtimestamp(int(c["date"]), tz=ZoneInfo(self.timezone_name))
                    if c.get("publishedDate"):
                        modified_at = datetime.fromtimestamp(int(c["publishedDate"]), tz=ZoneInfo(self.timezone_name))
                    author = c.get("publisher", {}).get("name")
                    original_source_url = c.get("originalUrl")
                    cover_image_url = c.get("thumb")

                    for b in c.get("bodys", []):
                        b_type = b.get("type")
                        if b_type == "text":
                            txt = BeautifulSoup(str(b.get("content", "")), "lxml").get_text(strip=True)
                            if txt:
                                blocks.append(ContentBlock(order=len(blocks), block_type="paragraph", text=txt))
                        elif b_type == "image":
                            img_src = b.get("contentOrigin") or b.get("content")
                            caption = b.get("caption") or b.get("title")
                            if img_src:
                                blocks.append(ContentBlock(
                                    order=len(blocks),
                                    block_type="image",
                                    media={"original_url": img_src, "caption": caption},
                                ))
            except Exception:
                pass

        # Fallback to listing_item or HTML meta
        if not title:
            title = (listing_item.get("title") if listing_item else None) or self._meta(soup, "property", "og:title") or self._first_text(soup, ["h1"])
        if not title:
            raise UnusableArticleError(f"Baomoi article title is missing: {clean_url}")

        if not summary:
            summary = (listing_item.get("description") if listing_item else None) or self._meta(soup, "name", "description") or self._meta(soup, "property", "og:description")

        if published_at is None:
            if listing_item and listing_item.get("date"):
                published_at = datetime.fromtimestamp(int(listing_item["date"]), tz=ZoneInfo(self.timezone_name))
            else:
                published_at, _ = self._published_time(soup)
        if published_at is None:
            raise UnusableArticleError(f"Baomoi article has no reliable publication timestamp: {clean_url}")

        if not cover_image_url:
            cover_image_url = self._meta(soup, "property", "og:image")
        if not author:
            author = self._meta(soup, "name", "author") or self._first_text(soup, [".article__source", ".source"])

        if not blocks and summary:
            blocks = [ContentBlock(order=0, block_type="paragraph", text=summary, metadata={"fallback": "summary_only"})]
        if not blocks:
            root = None
            for selector in self.config.get("article", {}).get("content_selectors", [".article__body", ".article-body", ".content-body"]):
                root = soup.select_one(selector)
                if root:
                    break
            if root:
                blocks = self._content_blocks(root)
        if not blocks:
            raise UnusableArticleError(f"Baomoi article body is missing: {clean_url}")

        canonical_node = soup.find("link", attrs={"rel": re.compile("canonical", re.I)})
        canonical = urljoin(clean_url, str(canonical_node.get("href"))) if canonical_node and canonical_node.get("href") else clean_url

        metadata = {
            "source_id_regex": self.config.get("article_id_regex"),
            "publisher_name": self.config.get("name", "Báo Mới"),
            "homepage_url": self.base_url,
            "discovered_from": category,
            "rights_status": self.config.get("rights", {}).get("status", "unknown"),
            "original_source_url": original_source_url,
            "content_fingerprint_sha256": self._fingerprint(blocks),
        }

        if self.security_matcher is not None:
            local_date = published_at.astimezone(ZoneInfo(self.timezone_name)).date()
            all_text = " ".join([title, summary or "", *(b.text or "" for b in blocks)])
            matches = self.security_matcher.match(all_text, local_date)
            metadata["matched_securities"] = [
                {
                    "security_uid": m.security_uid,
                    "symbol": m.symbol,
                    "alias_text": m.alias_text,
                    "alias_type": m.alias_type,
                    "confidence": m.confidence,
                    "method": m.method,
                    "evidence": m.evidence,
                }
                for m in matches
            ]

        return Article(
            source_code=self.source_code,
            publisher_code=self.publisher_code,
            source_article_id=source_article_id,
            canonical_url=canonical,
            title=title,
            published_at=published_at,
            summary=summary,
            author=author,
            original_source_url=original_source_url,
            section=category,
            cover_image_url=cover_image_url,
            cover_image_alt=title,
            blocks=blocks,
            metadata=metadata,
        )

    @staticmethod
    def _first_text(soup: BeautifulSoup, selectors: list[str]) -> str | None:
        for selector in selectors:
            node = soup.select_one(selector)
            if node:
                text = node.get_text(" ", strip=True)
                if text:
                    return text
        return None

