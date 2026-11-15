from __future__ import annotations

import json
import hashlib
import re
from datetime import date, datetime, timezone
from typing import Any
from urllib.parse import urljoin, urlparse, urlunparse
from urllib.robotparser import RobotFileParser
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup

from ...domain.models import Article, ContentBlock
from ...domain.windows import DateWindow
from .cafef import (
    CafeFClient,
    IncompleteArchiveError,
    UnusableArticleError,
    _clean_url,
    _text,
)


class SourceAuthorizationError(RuntimeError):
    """Raised when collection is disallowed by source policy or robots.txt."""


def _date_from_article_url(url: str) -> date | None:
    """Return a publication date when the publisher embeds one in its URL."""
    match_ymd = re.search(r"/(?P<y>20\d{2})/(?P<m>\d{1,2})(?:/(?P<d>\d{1,2}))?", url)
    if match_ymd:
        y = int(match_ymd.group("y"))
        m = int(match_ymd.group("m"))
        d = int(match_ymd.group("d")) if match_ymd.group("d") else None
        if d is not None:
            try:
                return date(y, m, d)
            except ValueError:
                pass
    match_prefix = re.search(r"-(?:185|188|100)(?P<yy>\d{2})(?P<mm>\d{2})(?P<dd>\d{2})\d*\.(?:htm|chn)$", url)
    if match_prefix:
        try:
            return date(
                2000 + int(match_prefix.group("yy")),
                int(match_prefix.group("mm")),
                int(match_prefix.group("dd")),
            )
        except ValueError:
            return None
    return None


def _url_outside_window(url: str, window: DateWindow) -> bool:
    item_date = _date_from_article_url(url)
    return item_date is not None and not (window.start_date <= item_date <= window.end_date)


def source_collection_enabled(config: dict[str, Any]) -> bool:
    access = config.get("access", {})
    return bool(
        config.get("collection_enabled") is True
        and config.get("use_scope") == "personal_academic_research"
        and access.get("use_scope") == "personal_academic_research"
        and access.get("internal_only") is True
        and access.get("attribution_required") is True
        and access.get("redistribution_allowed") is False
        and access.get("respect_robots_txt") is True
    )


class ConfiguredHtmlNewsClient(CafeFClient):
    """Config-driven RSS/archive/detail collector for publisher news sites.

    The client requires an explicit internal research scope in source config.
    It also fails a date-window run unless every configured archive reaches an
    article older than the inclusive start date.
    """

    def __init__(self, config: dict[str, Any], security_matcher=None, timeout_seconds: float = 30.0):
        super().__init__(config, security_matcher=security_matcher, timeout_seconds=timeout_seconds)
        self.rate = float(config.get("request_rate_per_second", 0.25))
        self.max_pages = int(config.get("discovery", {}).get("max_pages", 5000))
        self.publisher_domain = str(config["publisher_domain"]).lower().removeprefix("www.")
        self._robots_parser: RobotFileParser | None = None
        self._robots_loaded = False

    async def _get(self, url: str):
        if self.config.get("access", {}).get("respect_robots_txt", True):
            await self._load_robots(url)
            if self._robots_parser and not self._robots_parser.can_fetch("FinsightsDataResearch", url):
                raise SourceAuthorizationError(f"robots.txt disallows this URL for {self.source_code}: {url}")
        return await super()._get(url)

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
            raise SourceAuthorizationError(
                f"Cannot read robots.txt for {self.source_code}; refusing to collect pages."
            )
        parser = RobotFileParser(robots_url)
        parser.parse(response.text.splitlines())
        self._robots_parser = parser
        self._robots_loaded = True
        crawl_delay = parser.crawl_delay("FinsightsDataResearch") or parser.crawl_delay("*")
        if crawl_delay and crawl_delay > 0:
            self.rate = min(self.rate, 1.0 / float(crawl_delay))

    def collection_ready(self) -> bool:
        return source_collection_enabled(self.config)

    async def collect(
        self,
        window: DateWindow,
        on_batch: Any | None = None,
        archive_only: bool = False,
    ) -> list[Article]:
        if not self.collection_ready():
            raise SourceAuthorizationError(
                f"{self.source_code} is disabled: set collection_enabled and the "
                "personal_academic_research internal-only scope in its source YAML."
            )

        articles_by_id: dict[str, Article] = {}
        seen_article_ids: set[str] = set()
        day_buffer: dict[date, list[Article]] = {}
        stats = {
            "method": "html_archive_backfill" if archive_only else "html_archive_with_rss_overlap",
            "archive_categories": len(self.config.get("discovery", {}).get("archive_categories", [])),
            "archive_pages": 0,
            "article_urls_discovered": 0,
            "article_pages_fetched": 0,
            "article_pages_failed": 0,
            "financial_articles": 0,
            "coverage_proven": True,
        }

        def flush_days(category_code: str | None = None, newer_than: date | None = None) -> None:
            ready_dates = [
                pub_date for pub_date in day_buffer
                if newer_than is None or pub_date > newer_than
            ]
            for pub_date in sorted(ready_dates):
                batch = day_buffer.pop(pub_date)
                if not batch:
                    continue
                if on_batch:
                    on_batch(pub_date, batch, category_code)
                else:
                    for article in batch:
                        articles_by_id[article.source_article_id] = article

        today_local = datetime.now(ZoneInfo(self.timezone_name)).date()
        recent_days = int(self.config.get("discovery", {}).get("rss_recent_days", 7))
        if not archive_only and (today_local - window.end_date).days <= recent_days:
            for url, category in await self._rss_urls():
                if _url_outside_window(url, window):
                    continue
                try:
                    article = await self.fetch_article(url, category=category)
                    articles_by_id[article.source_article_id] = article
                except Exception:
                    continue

        for category in self.config.get("discovery", {}).get("archive_categories", []):
            category_complete = False
            saw_articles = False
            first_page = str(category["first_page"])
            template = category.get("page_template")
            next_page_url: str | None = first_page

            for page_number in range(1, self.max_pages + 1):
                if not next_page_url:
                    break
                page_url = next_page_url
                try:
                    response = await self._get(page_url)
                except Exception:
                    stats["coverage_proven"] = False
                    break
                soup = BeautifulSoup(response.content, "lxml")
                urls = self._archive_urls(soup, page_url)
                if not urls:
                    if page_number == 1:
                        raise IncompleteArchiveError(
                            f"No article links matched {self.source_code} archive {category['code']}"
                        )
                    break

                saw_articles = True
                stats["archive_pages"] += 1
                stats["article_urls_discovered"] += len(urls)
                page_articles: list[Article] = []
                page_dates: list[date] = []
                for url in urls:
                    embedded_date = _date_from_article_url(url)
                    if embedded_date is not None:
                        page_dates.append(embedded_date)
                    if _url_outside_window(url, window):
                        continue
                    match = re.search(str(self.config["article_id_regex"]), urlparse(url).path, re.IGNORECASE)
                    article_id = match.groupdict().get("article_id") if match else None
                    if article_id and article_id in seen_article_ids:
                        continue
                    try:
                        article = await self.fetch_article(url, category=category["code"])
                        stats["article_pages_fetched"] += 1
                        page_articles.append(article)
                        article_date = article.published_at.astimezone(ZoneInfo(self.timezone_name)).date()
                        page_dates.append(article_date)
                        if (
                            window.contains(article.published_at)
                            and self.is_financial(article)
                            and article.source_article_id not in seen_article_ids
                        ):
                            seen_article_ids.add(article.source_article_id)
                            stats["financial_articles"] += 1
                            if on_batch:
                                day_buffer.setdefault(article_date, []).append(article)
                            else:
                                articles_by_id[article.source_article_id] = article
                    except SourceAuthorizationError:
                        raise
                    except Exception:
                        stats["article_pages_failed"] += 1
                        continue

                if on_batch and page_dates:
                    # Archives are newest-first; later pages cannot add items newer than this page's oldest item.
                    flush_days(str(category.get("code", "archive")), newer_than=min(page_dates))

                if page_dates and max(page_dates) < window.start_date:
                    category_complete = True
                    break

                if template:
                    next_page_url = str(template).format(page=page_number + 1)
                else:
                    next_page_url = self._next_page_url(soup, page_url, category)
                if next_page_url == page_url:
                    break

            if on_batch:
                flush_days(str(category.get("code", "archive")))

            if not saw_articles or not category_complete:
                stats["coverage_proven"] = False
                if self.config.get("discovery", {}).get("fail_if_archive_boundary_not_proven", True):
                    raise IncompleteArchiveError(
                        f"{self.source_code} archive {category['code']} did not prove coverage before "
                        f"{window.start_date.isoformat()}; pagination may be incomplete or max_pages too low"
                    )

        results = [
            article for article in articles_by_id.values()
            if window.contains(article.published_at) and self.is_financial(article)
        ]
        self.collection_stats = stats
        return sorted(results, key=lambda item: (item.published_at, item.source_article_id))

    def _archive_urls(self, soup: BeautifulSoup, page_url: str) -> list[str]:
        regex = re.compile(str(self.config["article_id_regex"]), re.IGNORECASE)
        selectors = self.config.get("discovery", {}).get("link_selectors", ["a[href]"])
        urls: list[str] = []
        for selector in selectors:
            for anchor in soup.select(selector):
                href = anchor.get("href")
                if not href:
                    continue
                absolute = _clean_url(urljoin(page_url, str(href)))
                host = (urlparse(absolute).hostname or "").lower().removeprefix("www.")
                if host != self.publisher_domain and not host.endswith("." + self.publisher_domain):
                    continue
                if regex.search(urlparse(absolute).path):
                    urls.append(absolute)
        return list(dict.fromkeys(urls))

    def _next_page_url(self, soup: BeautifulSoup, page_url: str, category: dict[str, Any]) -> str | None:
        selectors = category.get("next_link_selectors", self.config["discovery"].get(
            "next_link_selectors", ["a[rel='next']", "a.next", "a[aria-label*='Tiếp']"]
        ))
        for selector in selectors:
            anchor = soup.select_one(selector)
            if anchor and anchor.get("href"):
                candidate = _clean_url(urljoin(page_url, str(anchor["href"])))
                host = (urlparse(candidate).hostname or "").lower().removeprefix("www.")
                if host == self.publisher_domain or host.endswith("." + self.publisher_domain):
                    return candidate
        return None

    async def _rss_urls(self) -> list[tuple[str, str]]:
        discovered: list[tuple[str, str]] = []
        for feed in self.config.get("discovery", {}).get("rss_feeds", []):
            try:
                feed_url = str(feed["url"] if isinstance(feed, dict) else feed)
                category = str(feed.get("category", "rss") if isinstance(feed, dict) else "rss")
                response = await self._get(feed_url)
                soup = BeautifulSoup(response.content, "xml")
                for item in soup.find_all("item"):
                    node = item.find("link")
                    if node and node.get_text(strip=True):
                        url = _clean_url(urljoin(feed_url, node.get_text(strip=True)))
                        host = (urlparse(url).hostname or "").lower().removeprefix("www.")
                        if host == self.publisher_domain or host.endswith("." + self.publisher_domain):
                            discovered.append((url, category))
            except Exception:
                continue
        return list(dict.fromkeys(discovered))

    async def fetch_article(self, url: str, category: str | None = None) -> Article:
        clean_url = _clean_url(url)
        response = await self._get(clean_url)
        soup = BeautifulSoup(response.content, "lxml")
        match = re.search(str(self.config["article_id_regex"]), urlparse(clean_url).path, re.IGNORECASE)
        if not match:
            raise UnusableArticleError(f"No publisher article ID in {clean_url}")
        raw_source_article_id = match.group("article_id")
        source_article_id = raw_source_article_id
        if len(source_article_id) > 180:
            source_article_id = hashlib.sha256(source_article_id.encode("utf-8")).hexdigest()

        article_config = self.config.get("article", {})
        title = self._first_text(soup, article_config.get("title_selectors", ["h1"]))
        title = title or self._meta(soup, "property", "og:title")
        if not title:
            raise UnusableArticleError(f"Article title is missing: {clean_url}")
        summary = self._first_text(soup, article_config.get("summary_selectors", []))
        summary = summary or self._meta(soup, "name", "description") or self._meta(soup, "property", "og:description")

        published_at, schema_org = self._published_time(soup)
        if published_at is None:
            raise UnusableArticleError(f"Article has no reliable publication timestamp: {clean_url}")
        modified_at = self._modified_time(soup)

        root = None
        for selector in article_config.get("content_selectors", []):
            root = soup.select_one(selector)
            if root:
                break
        blocks = self._content_blocks(root) if root else []
        full_text_required = article_config.get("full_text_required", True)
        if not blocks and summary and not full_text_required:
            blocks = [ContentBlock(0, "paragraph", text=summary, metadata={"fallback": "summary_only"})]
        if full_text_required and not blocks:
            raise UnusableArticleError(f"Article body is missing: {clean_url}")

        author = self._first_text(soup, article_config.get("author_selectors", []))
        author = author or self._meta(soup, "name", "author")
        image_url = self._meta(soup, "property", "og:image")
        image_alt = self._meta(soup, "property", "og:image:alt")
        canonical_node = soup.find("link", attrs={"rel": re.compile("canonical", re.I)})
        canonical = (
            _clean_url(urljoin(clean_url, str(canonical_node.get("href"))))
            if canonical_node and canonical_node.get("href") else clean_url
        )
        original_source_url = self._original_source_url(soup)
        rights = self.config.get("rights", {})
        all_text = " ".join([title, summary or "", *(b.text or "" for b in blocks)])
        terms = [str(term) for term in self.config.get("financial_relevance", {}).get("terms", [])]
        matched_terms = [t for t in terms if t.casefold() in all_text.casefold()]

        meta = {
            "collection_method": "html_archive",
            "publisher_article_id_sha256": (
                hashlib.sha256(raw_source_article_id.encode("utf-8")).hexdigest()
                if raw_source_article_id != source_article_id else None
            ),
            "publisher_name": self.config.get("name"),
            "publisher_domain": self.config.get("publisher_domain"),
            "homepage_url": self.config.get("base_url"),
            "attribution_name": self.config.get("attribution_name", self.publisher_code),
            "schema_org": schema_org,
            "modified_at": modified_at,
            "discovered_from": category,
            "page_date_timezone": self.timezone_name,
            "collection_scope": self.config.get("use_scope"),
            "internal_only": self.config.get("access", {}).get("internal_only") is True,
            "redistribution_allowed": self.config.get("access", {}).get("redistribution_allowed") is True,
            "binary_asset_collection_enabled": self.config.get("binary_asset_collection_enabled") is True,
            "rights_status": rights.get("status", "unknown"),
            "download_binary_assets_only_if_rights_status_in": rights.get("binary_asset_statuses", []),
            "content_fingerprint_sha256": self._fingerprint(blocks),
            "financial_terms": matched_terms,
        }

        if self.security_matcher is not None:
            local_date = published_at.astimezone(ZoneInfo(self.timezone_name)).date()
            matches = self.security_matcher.match(all_text, local_date)
            meta["matched_securities"] = [
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
            subtitle=self._first_text(soup, article_config.get("subtitle_selectors", [])),
            author=author,
            original_source_url=original_source_url,
            section=category,
            cover_image_url=image_url,
            cover_image_alt=image_alt,
            blocks=blocks,
            metadata=meta,
        )

    @staticmethod
    def _first_text(soup: BeautifulSoup, selectors: list[str]) -> str | None:
        for selector in selectors:
            value = _text(soup.select_one(selector))
            if value:
                return value
        return None

    def is_financial(self, article: Article) -> bool:
        configured = self.config.get("financial_relevance", {})
        terms = [str(term).casefold() for term in configured.get("terms", [])]
        text = " ".join([article.title, article.summary or "", *(block.text or "" for block in article.blocks)]).casefold()
        if any(term in text for term in terms):
            return True
        if self.security_matcher is not None:
            local_date = article.published_at.astimezone(ZoneInfo(self.timezone_name)).date()
            return bool(self.security_matcher.match(text, local_date))
        return False

