from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
from datetime import date, datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Iterable
from urllib.parse import urljoin, urlparse, urlunparse
from urllib.robotparser import RobotFileParser
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup, NavigableString, Tag

from ...domain.models import Article, ContentBlock
from ...domain.windows import DateWindow


class IncompleteArchiveError(RuntimeError):
    """Raised when CafeF cannot prove it traversed the requested date range."""


class UnusableArticleError(RuntimeError):
    pass


FINANCIAL_TERMS = (
    "cổ phiếu", "chứng khoán", "vn-index", "vn30", "hose", "hnx", "upcom",
    "cổ tức", "niêm yết", "trái phiếu", "báo cáo tài chính", "lợi nhuận",
    "doanh thu", "ngân hàng", "lãi suất", "tỷ giá", "thị trường tài chính",
    "đầu tư", "margin", "thanh khoản", "khối ngoại", "ipo", "giá cổ phiếu",
    "mã ck", "công bố thông tin", "chứng quyền", "phái sinh",
)

SKIP_CLASSES = {
    "ads", "advertisement", "related-news", "relatednews", "box-related",
    "social-share", "comment", "toolbox", "hidden", "recommendation",
}


def _clean_url(url: str) -> str:
    parts = urlparse(url)
    return urlunparse((parts.scheme or "https", parts.netloc.lower(), parts.path, "", "", ""))


def _extract_date_from_url(url: str) -> date | None:
    match = re.search(r"188(\d{2})(\d{2})(\d{2})", url)
    if match:
        year = int("20" + match.group(1))
        month = int(match.group(2))
        day = int(match.group(3))
        try:
            return date(year, month, day)
        except ValueError:
            pass
    return None


def _text(node: Tag | None) -> str | None:
    if node is None:
        return None
    value = node.get_text(" ", strip=True)
    return re.sub(r"\s+", " ", value).strip() or None


def _parse_datetime(value: str | None, timezone_name: str) -> datetime | None:
    if not value:
        return None
    raw = value.strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return parsed.replace(tzinfo=ZoneInfo(timezone_name)) if parsed.tzinfo is None else parsed
    except ValueError:
        pass
    try:
        parsed = parsedate_to_datetime(raw)
        return parsed.replace(tzinfo=ZoneInfo(timezone_name)) if parsed.tzinfo is None else parsed
    except (TypeError, ValueError, OverflowError):
        pass

    match = re.search(
        r"(?P<day>\d{1,2})[-/](?P<month>\d{1,2})[-/](?P<year>\d{4})"
        r"(?:\s*(?:-|,)?\s*(?P<hour>\d{1,2}):(?P<minute>\d{2})(?::(?P<second>\d{2}))?\s*(?P<ampm>AM|PM)?)?",
        raw,
        re.IGNORECASE,
    )
    if not match:
        return None
    hour = int(match.group("hour") or 0)
    ampm = (match.group("ampm") or "").upper()
    # CafeF sometimes prints 16:51 PM. Preserve a valid 24-hour hour as-is.
    if ampm == "PM" and 1 <= hour < 12:
        hour += 12
    elif ampm == "AM" and hour == 12:
        hour = 0
    try:
        return datetime(
            int(match.group("year")), int(match.group("month")), int(match.group("day")),
            hour, int(match.group("minute") or 0), int(match.group("second") or 0),
            tzinfo=ZoneInfo(timezone_name),
        )
    except ValueError:
        return None


def _jsonld_values(value: Any) -> Iterable[dict[str, Any]]:
    if isinstance(value, dict):
        yield value
        graph = value.get("@graph")
        if isinstance(graph, list):
            for item in graph:
                yield from _jsonld_values(item)
    elif isinstance(value, list):
        for item in value:
            yield from _jsonld_values(item)


class CafeFClient:
    def __init__(self, config: dict[str, Any], security_matcher=None, timeout_seconds: float = 30.0):
        self.config = config
        self.security_matcher = security_matcher
        self.source_code = str(config["source_code"])
        self.publisher_code = str(config["publisher_code"])
        self.timezone_name = str(config.get("timezone", "Asia/Ho_Chi_Minh"))
        self.rate = float(os.getenv("CAFEF_REQUEST_RATE_PER_SECOND", config.get("request_rate_per_second", 0.0)))
        backfill_config = config.get("discovery", {}).get("html_backfill", {})
        self.max_pages = int(os.getenv(
            "CAFEF_MAX_PAGES_PER_CATEGORY",
            backfill_config.get("max_pages", config.get("max_pages_per_category", 5000)),
        ))
        self._last_request = 0.0
        self._request_rate_lock = asyncio.Lock()
        self.fetch_attempts: list[dict[str, Any]] = []
        self._robots_parser: RobotFileParser | None = None
        self._robots_loaded = False
        self._client = httpx.AsyncClient(
            timeout=timeout_seconds,
            follow_redirects=True,
            headers={
                "User-Agent": "FinsightsDataResearch/1.0 (+personal academic research; attribution retained)",
                "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.7",
            },
        )

    async def close(self) -> None:
        if not self._client.is_closed:
            try:
                await self._client.aclose()
            except Exception:
                pass

    async def _get(self, url: str) -> httpx.Response:
        if self.config.get("access", {}).get("respect_robots_txt", True):
            await self._load_robots(url)
            if self._robots_parser and not self._robots_parser.can_fetch("FinsightsDataResearch", url):
                raise RuntimeError(f"robots.txt disallows this URL for {self.source_code}: {url}")
        interval = (1.0 / self.rate) if self.rate > 0 else 0.0
        max_retries = 2
        for attempt in range(max_retries + 1):
            async with self._request_rate_lock:
                loop = asyncio.get_running_loop()
                if interval > 0:
                    elapsed = loop.time() - self._last_request
                    if elapsed < interval:
                        await asyncio.sleep(interval - elapsed)
                requested_at = datetime.now(timezone.utc).replace(tzinfo=None)
                started = loop.time()
                # Space request starts even when several article responses are
                # concurrently in flight.
                if interval > 0:
                    self._last_request = started
            try:
                response = await self._client.get(url)
                response.raise_for_status()
                self.fetch_attempts.append({
                    "url": url, "requested_at": requested_at,
                    "completed_at": datetime.now(timezone.utc).replace(tzinfo=None),
                    "http_status": response.status_code,
                    "content_type": response.headers.get("content-type"),
                    "size_bytes": len(response.content),
                    "response_sha256": hashlib.sha256(response.content).hexdigest(),
                    "duration_ms": int((loop.time() - started) * 1000),
                    "outcome": "SUCCESS",
                })
                return response
            except (httpx.RemoteProtocolError, httpx.TransportError) as exc:
                if attempt < max_retries:
                    await asyncio.sleep(1.0)
                    continue
                self.fetch_attempts.append({
                    "url": url, "requested_at": requested_at,
                    "completed_at": datetime.now(timezone.utc).replace(tzinfo=None),
                    "http_status": getattr(getattr(exc, "response", None), "status_code", None),
                    "duration_ms": int((loop.time() - started) * 1000),
                    "outcome": "FAILED", "error_type": type(exc).__name__, "error_message": str(exc)[:2000],
                })
                raise
            except Exception as exc:
                self.fetch_attempts.append({
                    "url": url, "requested_at": requested_at,
                    "completed_at": datetime.now(timezone.utc).replace(tzinfo=None),
                    "http_status": getattr(getattr(exc, "response", None), "status_code", None),
                    "duration_ms": int((loop.time() - started) * 1000),
                    "outcome": "FAILED", "error_type": type(exc).__name__, "error_message": str(exc)[:2000],
                })
                raise

    async def _load_robots(self, url: str) -> None:
        if self._robots_loaded:
            return
        parsed = urlparse(url)
        robots_url = urlunparse((parsed.scheme, parsed.netloc, "/robots.txt", "", "", ""))
        requested_at = datetime.now(timezone.utc).replace(tzinfo=None)
        response = await self._client.get(robots_url)
        self.fetch_attempts.append({
            "url": robots_url,
            "resource_kind": "robots_txt",
            "requested_at": requested_at,
            "completed_at": datetime.now(timezone.utc).replace(tzinfo=None),
            "http_status": response.status_code,
            "content_type": response.headers.get("content-type"),
            "size_bytes": len(response.content),
            "outcome": "SUCCESS" if response.status_code in {200, 404} else "FAILED",
        })
        if response.status_code == 404:
            self._robots_loaded = True
            return
        if response.status_code != 200:
            raise RuntimeError(f"Cannot read robots.txt for {self.source_code}; refusing archive collection.")
        parser = RobotFileParser(robots_url)
        parser.parse(response.text.splitlines())
        self._robots_parser = parser
        self._robots_loaded = True
        crawl_delay = parser.crawl_delay("FinsightsDataResearch") or parser.crawl_delay("*")
        if crawl_delay and crawl_delay > 0:
            self.rate = min(self.rate, 1.0 / float(crawl_delay)) if self.rate > 0 else 1.0 / float(crawl_delay)

    async def _find_start_page(self, category: dict[str, Any], end_date: date) -> int:
        """Binary search page index when target date is significantly in the past."""
        template = category["page_template"]
        low, high = 1, min(self.max_pages, 1000)
        best_page = 1
        while low <= high:
            mid = (low + high) // 2
            page_url = template.format(page=mid)
            try:
                response = await self._get(page_url)
                urls = re.findall(r"/[^\"'\s]+-188\d+\.chn", response.text)
                dates = [_extract_date_from_url(u) for u in urls]
                valid_dates = [d for d in dates if d is not None]
                if not valid_dates:
                    high = mid - 1
                    continue
                if max(valid_dates) <= end_date:
                    best_page = mid
                    high = mid - 1
                else:
                    low = mid + 1
            except Exception:
                high = mid - 1
        return max(1, best_page - 2)

    async def collect(
        self,
        window: DateWindow,
        on_batch: Any | None = None,
        archive_only: bool = False,
    ) -> list[Article]:
        """Fetch archive pages and detail pages; flushes day-batches incrementally to prevent OOM."""
        discovered: dict[str, tuple[str, str]] = {}
        seen_article_urls: set[str] = set()
        results: dict[str, Article] = {}
        day_buffer: dict[date, list[Article]] = {}
        processed_uids: set[str] = set()
        archive_pages = 0
        coverage_proven = True

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
        rss_urls = (
            await self._rss_urls()
            if not archive_only and (today - window.end_date).days <= 2
            else []
        )
        for url in rss_urls:
            discovered[url] = ("rss", "recent_feed")

        discovery = self.config.get("discovery", {})
        backfill_config = discovery.get("html_backfill", {})
        categories = backfill_config.get("archive_categories", discovery.get("archive_categories", []))
        for category in categories:
            cat_code = category.get("code", "archive")
            category_complete = False
            saw_any_articles = False
            template = category["page_template"]

            # If target end_date is older than 2 days ago, jump directly using binary search
            start_page = 1
            if (today - window.end_date).days > 2:
                start_page = await self._find_start_page(category, window.end_date)

            for page_number in range(start_page, self.max_pages + 1):
                page_url = category.get("first_page") if page_number == 1 else template.format(page=page_number)
                response = await self._get(page_url)
                archive_pages += 1
                soup = BeautifulSoup(response.content, "lxml")
                candidates = self._archive_urls(soup, page_url)
                if not candidates:
                    if page_number == 1:
                        raise IncompleteArchiveError(f"No article cards found for archive {category['code']}")
                    break
                saw_any_articles = True
                page_dates: list[datetime] = []
                for article_url in candidates:
                    discovered.setdefault(article_url, (category["code"], "archive"))

                    # Pre-filter using timestamp encoded directly in the CafeF URL
                    estimated_date = _extract_date_from_url(article_url)
                    if estimated_date is not None:
                        est_dt = datetime.combine(estimated_date, datetime.min.time(), tzinfo=ZoneInfo(self.timezone_name))
                        page_dates.append(est_dt)
                        if estimated_date > window.end_date or estimated_date < window.start_date:
                            # Skip full detail download if outside requested date window!
                            continue

                    if article_url in seen_article_urls:
                        continue
                    seen_article_urls.add(article_url)
                    try:
                        article = await self.fetch_article(article_url, category=category["code"])
                        article_date = article.published_at.astimezone(ZoneInfo(self.timezone_name)).date()
                        page_dates.append(article.published_at.astimezone(ZoneInfo(self.timezone_name)))
                        if window.contains(article.published_at) and self.is_financial(article):
                            if article.source_article_id not in processed_uids:
                                processed_uids.add(article.source_article_id)
                                day_buffer.setdefault(article_date, []).append(article)
                    except (UnusableArticleError, httpx.HTTPError):
                        continue

                # Once a page finishes, flush any days strictly newer than the oldest article on this page
                if page_dates:
                    _flush_days(min_date_cutoff=min(page_dates).date(), category_code=cat_code)

                # Stop only after fully parsing a page whose every item predates the start.
                if page_dates and max(page_dates).date() < window.start_date:
                    category_complete = True
                    break

            _flush_days(min_date_cutoff=None, category_code=cat_code)

            if not saw_any_articles or not category_complete:
                coverage_proven = False
                if backfill_config.get(
                    "fail_if_archive_boundary_not_proven",
                    discovery.get("fail_if_archive_boundary_not_proven", False),
                ):
                    raise IncompleteArchiveError(
                        f"CafeF archive {category['code']} did not prove coverage before {window.start_date}; "
                        "increase max_pages_per_category or verify the archive pagination format"
                    )

        # Reuse detail fetches made during archive coverage; only RSS-only URLs need another fetch.
        for url, (_, discovery_method) in discovered.items():
            if url in seen_article_urls:
                continue
            seen_article_urls.add(url)
            estimated_date = _extract_date_from_url(url)
            if estimated_date is not None and not (window.start_date <= estimated_date <= window.end_date):
                continue
            try:
                article = await self.fetch_article(url)
            except (UnusableArticleError, httpx.HTTPError):
                continue
            if window.contains(article.published_at) and self.is_financial(article):
                if article.source_article_id not in processed_uids:
                    processed_uids.add(article.source_article_id)
                    article_date = article.published_at.astimezone(ZoneInfo(self.timezone_name)).date()
                    day_buffer.setdefault(article_date, []).append(article)

        _flush_days(min_date_cutoff=None, category_code="rss")
        self.collection_stats = {
            "method": "html_archive_backfill" if archive_only else "html_archive_with_rss_overlap",
            "archive_categories": len(categories),
            "archive_pages": archive_pages,
            "article_urls_discovered": len(discovered),
            "article_pages_fetched": len(seen_article_urls),
            "financial_articles": len(processed_uids),
            "coverage_proven": coverage_proven,
        }
        return list(results.values())

    async def _rss_urls(self) -> list[str]:
        regex = re.compile(self.config["article_id_regex"], re.IGNORECASE)
        urls: list[str] = []
        for feed_url in self.config["discovery"].get("rss_feeds", []):
            try:
                response = await self._get(feed_url)
                soup = BeautifulSoup(response.content, "xml")
                for item in soup.find_all("item"):
                    link = item.find("link")
                    if link and link.get_text(strip=True):
                        clean = _clean_url(urljoin(feed_url, link.get_text(strip=True)))
                        if regex.search(clean):
                            urls.append(clean)
            except Exception:
                continue
        return list(dict.fromkeys(urls))

    def _archive_urls(self, soup: BeautifulSoup, page_url: str) -> list[str]:
        regex = re.compile(self.config["article_id_regex"], re.IGNORECASE)
        urls = []
        for anchor in soup.select("a[href]"):
            href = urljoin(page_url, str(anchor.get("href", "")))
            parsed = urlparse(href)
            if parsed.hostname not in {"cafef.vn", "www.cafef.vn"}:
                continue
            if regex.search(parsed.path):
                urls.append(_clean_url(href))
        return list(dict.fromkeys(urls))

    async def fetch_article(self, url: str, category: str | None = None) -> Article:
        clean_url = _clean_url(url)
        response = await self._get(clean_url)
        soup = BeautifulSoup(response.content, "lxml")
        article_id_regex = re.compile(self.config["article_id_regex"], re.IGNORECASE)
        id_match = article_id_regex.search(urlparse(clean_url).path)
        if not id_match:
            raise UnusableArticleError(f"CafeF URL has no recognized publisher article ID: {clean_url}")
        source_article_id = id_match.group("article_id")

        title_node = soup.select_one("h1.title, h1.detail-title, article h1, h1")
        title = _text(title_node)
        if not title:
            raise UnusableArticleError(f"CafeF article title is missing: {clean_url}")
        summary = (
            _text(soup.select_one("h2.sapo, .sapo, .detail-sapo"))
            or self._meta(soup, "name", "description")
            or self._meta(soup, "property", "og:description")
        )

        published_at, schema_org = self._published_time(soup)
        if published_at is None:
            raise UnusableArticleError(f"CafeF article has no reliable publication timestamp: {clean_url}")
        modified_at = self._modified_time(soup)

        root = None
        for selector in self.config.get("article", {}).get(
            "content_selectors", [".contentdetail", "#mainContent", ".detail-content", "article"]
        ):
            root = soup.select_one(selector)
            if root:
                break
        blocks = self._content_blocks(root) if root else []
        full_text_required = self.config.get("article", {}).get("full_text_required", False)
        if not blocks and summary and not full_text_required:
            blocks = [ContentBlock(order=0, block_type="paragraph", text=summary, metadata={"fallback": "summary_only"})]
        has_article_text = any((block.text or "").strip() for block in blocks)
        has_table_text = any(
            str(cell).strip()
            for block in blocks
            if block.table
            for row in block.table.get("rows", [])
            for cell in row
        )
        if full_text_required and not (has_article_text or has_table_text):
            raise UnusableArticleError(f"CafeF article body has no text: {clean_url}")
        if not blocks:
            raise UnusableArticleError(f"CafeF article body is missing: {clean_url}")

        author = (
            self._meta(soup, "name", "author")
            or _text(soup.select_one(".author, .p-author, [rel='author']"))
        )
        image_url = self._meta(soup, "property", "og:image")
        image_alt = self._meta(soup, "property", "og:image:alt")
        canonical_node = soup.find("link", attrs={"rel": re.compile("canonical", re.I)})
        canonical = _clean_url(urljoin(clean_url, str(canonical_node.get("href")))) if canonical_node and canonical_node.get("href") else clean_url
        original_source_url = self._original_source_url(soup)
        rights = self.config.get("rights", {})
        metadata = {
            "collection_method": "html_archive",
            "source_id_regex": self.config["article_id_regex"],
            "publisher_name": self.config.get("name", "CafeF"),
            "homepage_url": self.config.get("base_url", "https://cafef.vn"),
            "schema_org": schema_org,
            "modified_at": modified_at,
            "discovered_from": category,
            "page_date_timezone": self.timezone_name,
            "rights_status": rights.get("status", "unknown"),
            "download_binary_assets_only_if_rights_status_in": rights.get("binary_asset_statuses", []),
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
            cover_image_url=image_url,
            cover_image_alt=image_alt,
            blocks=blocks,
            metadata=metadata,
        )

    @staticmethod
    def _meta(soup: BeautifulSoup, attr: str, name: str) -> str | None:
        node = soup.find("meta", attrs={attr: name})
        return str(node.get("content")).strip() if node and node.get("content") else None

    def _published_time(self, soup: BeautifulSoup) -> tuple[datetime | None, dict[str, Any] | None]:
        candidates = [
            self._meta(soup, "property", "article:published_time"),
            self._meta(soup, "name", "pubdate"),
            self._meta(soup, "name", "datePublished"),
        ]
        schema_org = None
        for script in soup.select('script[type="application/ld+json"]'):
            try:
                value = json.loads(script.string or script.get_text())
            except (json.JSONDecodeError, TypeError):
                continue
            for item in _jsonld_values(value):
                if item.get("datePublished"):
                    candidates.append(str(item["datePublished"]))
                    schema_org = item
                    break
        for selector in ("time[datetime]", ".pdate", ".time-update", "time"):
            node = soup.select_one(selector)
            if node:
                candidates.append(str(node.get("datetime") or node.get_text(" ", strip=True)))
        for candidate in candidates:
            parsed = _parse_datetime(candidate, self.timezone_name)
            if parsed:
                return parsed, schema_org
        return None, schema_org

    def _modified_time(self, soup: BeautifulSoup) -> datetime | None:
        candidates = [
            self._meta(soup, "property", "article:modified_time"),
            self._meta(soup, "name", "dateModified"),
        ]
        for script in soup.select('script[type="application/ld+json"]'):
            try:
                value = json.loads(script.string or script.get_text())
            except (json.JSONDecodeError, TypeError):
                continue
            for item in _jsonld_values(value):
                if item.get("dateModified"):
                    candidates.append(str(item["dateModified"]))
        for candidate in candidates:
            parsed = _parse_datetime(candidate, self.timezone_name)
            if parsed:
                return parsed
        return None

    @staticmethod
    def _original_source_url(soup: BeautifulSoup) -> str | None:
        for anchor in soup.select("a[href]"):
            label = (anchor.get_text(" ", strip=True) or "").casefold()
            if any(word in label for word in ("nguồn", "source", "theo")):
                href = anchor.get("href")
                if href and urlparse(str(href)).scheme in {"http", "https"}:
                    return str(href)
        return None

    def _content_blocks(self, root: Tag) -> list[ContentBlock]:
        output: list[ContentBlock] = []

        def append(kind: str, text: str | None = None, media: dict[str, Any] | None = None,
                   table: dict[str, Any] | None = None, metadata: dict[str, Any] | None = None) -> None:
            output.append(ContentBlock(len(output), kind, text, media, table, metadata or {}))

        def emit_inline_paragraph(node: Tag) -> None:
            text_parts: list[str] = []

            def flush() -> None:
                text = re.sub(r"\s+", " ", "".join(text_parts)).strip()
                if text:
                    append("paragraph", text)
                text_parts.clear()

            def walk_inline(child):
                if isinstance(child, NavigableString):
                    text_parts.append(str(child))
                    return
                if not isinstance(child, Tag):
                    return
                classes = {str(cls).casefold() for cls in child.get("class", [])}
                if child.name in {"script", "style", "noscript", "svg"} or classes & SKIP_CLASSES:
                    return
                if child.name == "br":
                    flush()
                    append("line_break")
                elif child.name in {"img", "audio", "video", "iframe"}:
                    flush()
                    emit_media(child)
                else:
                    for descendant in child.children:
                        walk_inline(descendant)

            for child in node.children:
                walk_inline(child)
            flush()

        def emit_media(node: Tag) -> None:
            tag = node
            if node.name == "figure":
                tag = node.find(["img", "audio", "video", "iframe"]) or node
            kind = {"img": "image", "audio": "audio", "video": "video", "iframe": "video"}.get(tag.name)
            if not kind:
                return
            src = tag.get("src") or tag.get("data-src") or tag.get("data-original")
            if not src and tag.name == "video":
                child_source = tag.find("source")
                src = child_source.get("src") if child_source else None
            if not src:
                return
            url = urljoin(str(self.config.get("base_url", "https://cafef.vn")), str(src))
            caption_node = node.find("figcaption") if node.name == "figure" else None
            append(kind, media={
                "original_url": url,
                "alt": tag.get("alt"),
                "caption": _text(caption_node),
                "credit": tag.get("data-credit"),
            })

        def walk(node: Tag) -> None:
            for child in node.children:
                if not isinstance(child, Tag) or child.name in {"script", "style", "noscript", "svg"}:
                    continue
                classes = {str(cls).casefold() for cls in child.get("class", [])}
                if classes & SKIP_CLASSES:
                    continue
                if child.name == "p":
                    emit_inline_paragraph(child)
                elif child.name in {"h2", "h3", "h4"}:
                    text = _text(child)
                    if text:
                        append("heading", text)
                elif child.name in {"blockquote", "q"}:
                    text = _text(child)
                    if text:
                        append("quote", text)
                elif child.name == "figure":
                    emit_media(child)
                elif child.name in {"img", "audio", "video", "iframe"}:
                    emit_media(child)
                elif child.name == "table":
                    rows = []
                    for row in child.select("tr"):
                        cells = [_text(cell) or "" for cell in row.find_all(["th", "td"], recursive=False)]
                        if cells:
                            rows.append(cells)
                    if rows:
                        append("table", table={"rows": rows})
                elif child.name in {"ul", "ol"}:
                    items = [_text(item) for item in child.find_all("li", recursive=False)]
                    items = [item for item in items if item]
                    if items:
                        append("list", "\n".join(items), metadata={"ordered": child.name == "ol"})
                else:
                    walk(child)

        walk(root)
        for index, block in enumerate(output):
            block.order = index
        return output

    @staticmethod
    def _fingerprint(blocks: list[ContentBlock]) -> str:
        payload = []
        for block in blocks:
            value: dict[str, Any] = {"type": block.block_type}
            if block.text:
                value["text"] = re.sub(r"\s+", " ", block.text).strip().casefold()
            if block.table:
                value["table"] = [
                    [re.sub(r"\s+", " ", str(cell)).strip().casefold() for cell in row]
                    for row in block.table.get("rows", [])
                ]
            if block.media:
                value["media"] = {
                    "url": block.media.get("original_url"),
                    "caption": re.sub(r"\s+", " ", str(block.media.get("caption") or "")).strip().casefold(),
                }
            payload.append(value)
        canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    def is_financial(self, article: Article) -> bool:
        text = " ".join([article.title, article.summary or "", *(b.text or "" for b in article.blocks)]).casefold()
        if any(term in text for term in FINANCIAL_TERMS):
            return True
        if self.security_matcher is not None:
            local_date = article.published_at.astimezone(ZoneInfo(self.timezone_name)).date()
            return bool(self.security_matcher.match(text, local_date))
        return False

