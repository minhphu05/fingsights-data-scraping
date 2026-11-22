from __future__ import annotations

import asyncio
import copy
from datetime import date, datetime, timedelta
import hashlib
import ipaddress
import json
import logging
import os
import re
import unicodedata
import xml.etree.ElementTree as ET
from typing import Any
from urllib.parse import urljoin, urlparse, urlunparse
from urllib.robotparser import RobotFileParser
from zoneinfo import ZoneInfo

from bs4 import BeautifulSoup
import httpx

from ...domain.models import Article, ContentBlock
from ...domain.windows import DateWindow
from .cafef import CafeFClient, IncompleteArchiveError, UnusableArticleError
from .configured_html import SourceAuthorizationError, source_collection_enabled

logger = logging.getLogger("finsights.sources.news.baomoi")


class SummaryOnlyArticleError(UnusableArticleError):
    """The aggregator and its permitted original URL both lacked a full body."""


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
        self._robots_by_origin: dict[str, RobotFileParser | None] = {}
        self._origin_request_locks: dict[str, asyncio.Lock] = {}
        self._origin_last_request: dict[str, float] = {}
        self._origin_intervals: dict[str, float] = {}

    async def _get(self, url: str) -> httpx.Response:
        parsed = urlparse(url)
        if parsed.scheme.lower() != "https" or not parsed.hostname or parsed.username or parsed.password:
            raise SourceAuthorizationError(f"Refusing unsafe URL for {self.source_code}")
        host = parsed.hostname.lower()
        port = parsed.port
        origin = f"https://{host}" + (f":{port}" if port and port != 443 else "")
        origin_lock = self._origin_request_locks.setdefault(origin, asyncio.Lock())

        async with origin_lock:
            base_interval = (1.0 / self.rate) if self.rate > 0 else 0.0
            if self.config.get("access", {}).get("respect_robots_txt", True):
                if origin not in self._robots_by_origin:
                    await self._load_origin_robots(parsed, origin)
                parser = self._robots_by_origin[origin]
                if parser and not parser.can_fetch("FinsightsDataResearch", url):
                    raise SourceAuthorizationError(
                        f"robots.txt disallows this URL for {self.source_code}: {url}"
                    )

            interval = max(base_interval, self._origin_intervals.get(origin, 0.0))
            max_retries = 2
            for attempt in range(max_retries + 1):
                loop = asyncio.get_running_loop()
                elapsed = loop.time() - self._origin_last_request.get(origin, 0.0)
                if elapsed < interval:
                    await asyncio.sleep(interval - elapsed)
                requested_at = datetime.utcnow()
                started = loop.time()
                self._origin_last_request[origin] = started
                response: httpx.Response | None = None
                try:
                    response = await self._client.get(url)
                    response.raise_for_status()
                    self.fetch_attempts.append({
                        "url": url,
                        "requested_at": requested_at,
                        "completed_at": datetime.utcnow(),
                        "http_status": response.status_code,
                        "content_type": response.headers.get("content-type"),
                        "size_bytes": len(response.content),
                        "response_sha256": hashlib.sha256(response.content).hexdigest(),
                        "duration_ms": int((loop.time() - started) * 1000),
                        "outcome": "SUCCESS",
                    })
                    return response
                except Exception as exc:
                    error_response = getattr(exc, "response", None) or response
                    self.fetch_attempts.append({
                        "url": url,
                        "requested_at": requested_at,
                        "completed_at": datetime.utcnow(),
                        "http_status": error_response.status_code if error_response else None,
                        "content_type": error_response.headers.get("content-type") if error_response else None,
                        "size_bytes": len(error_response.content) if error_response else None,
                        "duration_ms": int((loop.time() - started) * 1000),
                        "outcome": "FAILED",
                        "error": f"{type(exc).__name__}: {str(exc)[:500]}",
                    })
                    if attempt >= max_retries:
                        raise

    async def _load_origin_robots(self, parsed_url, origin: str) -> None:
        """Cache and enforce robots policy and delay independently for each publisher host."""
        robots_url = urlunparse((parsed_url.scheme, parsed_url.netloc, "/robots.txt", "", "", ""))
        requested_at = datetime.utcnow()
        response = await self._client.get(robots_url)
        self._origin_last_request[origin] = asyncio.get_running_loop().time()
        self.fetch_attempts.append({
            "url": robots_url,
            "resource_kind": "robots_txt",
            "requested_at": requested_at,
            "completed_at": datetime.utcnow(),
            "http_status": response.status_code,
            "content_type": response.headers.get("content-type"),
            "size_bytes": len(response.content),
            "outcome": "SUCCESS" if response.status_code in {200, 404} else "FAILED",
        })
        if response.status_code == 404:
            self._robots_by_origin[origin] = None
            self._origin_intervals[origin] = (1.0 / self.rate) if self.rate > 0 else 0.0
            return
        if response.status_code != 200:
            raise SourceAuthorizationError(
                f"Cannot read robots.txt for {self.source_code}; refusing to collect pages."
            )

        parser = RobotFileParser(robots_url)
        parser.parse(response.text.splitlines())
        self._robots_by_origin[origin] = parser
        crawl_delay = parser.crawl_delay("FinsightsDataResearch") or parser.crawl_delay("*")
        base_interval = (1.0 / self.rate) if self.rate > 0 else 0.0
        self._origin_intervals[origin] = max(
            base_interval,
            float(crawl_delay) if crawl_delay and crawl_delay > 0 else 0.0,
        )

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
                # Do not invent a publication date; undated cards cannot prove archive coverage.
                continue
            items.append((clean_url, pub_dt, {"title": a.get_text(strip=True), "url": clean_url}))
        return items

    async def collect(
        self,
        window: DateWindow,
        on_batch: Any | None = None,
        archive_only: bool = False,
    ) -> list[Article]:
        """Collect the Baomoi HTML archive and stream publication-day batches."""
        if not source_collection_enabled(self.config):
            raise SourceAuthorizationError(
                "Baomoi collection requires enabled, attributed, internal-only access and robots.txt checks"
            )

        backfill = self.config.get("discovery", {}).get("html_backfill", {})
        if archive_only and backfill.get("strategy") == "daily_sitemap":
            return await self._collect_daily_sitemap_backfill(window, on_batch, backfill)

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
        stats = {
            "method": "html_archive_backfill" if archive_only else "html_archive_latest",
            "archive_categories": len(categories),
            "archive_pages": 0,
            "article_urls_discovered": 0,
            "article_pages_fetched": 0,
            "article_pages_failed": 0,
            "article_pages_skipped_summary_only": 0,
            "financial_articles": 0,
            "coverage_proven": True,
            "requested_start_date": window.start_date.isoformat(),
            "requested_end_date": window.end_date.isoformat(),
        }
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
                stats["archive_pages"] += 1
                stats["article_urls_discovered"] += len(items)
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
                        stats["article_pages_fetched"] += 1
                        if window.contains(article.published_at) and self.is_financial(article):
                            if article.source_article_id not in processed_uids:
                                processed_uids.add(article.source_article_id)
                                article_day = article.published_at.astimezone(
                                    ZoneInfo(self.timezone_name)
                                ).date()
                                article.metadata["collection_method"] = stats["method"]
                                article.metadata.setdefault(
                                    "content_scope",
                                    "aggregate_excerpt"
                                    if any(block.metadata.get("fallback") == "summary_only" for block in article.blocks)
                                    else "baomoi_aggregator_full_text",
                                )
                                day_buffer.setdefault(article_day, []).append(article)
                                stats["financial_articles"] += 1
                    except SummaryOnlyArticleError:
                        stats["article_pages_skipped_summary_only"] += 1
                        continue
                    except SourceAuthorizationError:
                        raise
                    except Exception:
                        stats["article_pages_failed"] += 1
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
                stats["coverage_proven"] = False
                if self.config.get("discovery", {}).get("fail_if_archive_boundary_not_proven", True):
                    raise IncompleteArchiveError(
                        f"Baomoi archive {cat_code} did not prove coverage before {window.start_date}"
                    )

        _flush_days(min_date_cutoff=None)
        stats["article_failure_ratio"] = round(
            stats["article_pages_failed"]
            / max(1, stats["article_pages_failed"] + stats["article_pages_fetched"] + stats["article_pages_skipped_summary_only"]),
            4,
        )
        max_failure_ratio = float(self.config.get("discovery", {}).get("max_article_failure_ratio", 0.25))
        if stats["article_failure_ratio"] > max_failure_ratio:
            raise RuntimeError(
                f"Baomoi archive article failure ratio {stats['article_failure_ratio']:.1%} "
                f"exceeds configured limit {max_failure_ratio:.1%}"
            )
        self.collection_stats = stats
        return list(results.values())

    async def _collect_daily_sitemap_backfill(
        self,
        window: DateWindow,
        on_batch: Any | None,
        backfill: dict[str, Any],
    ) -> list[Article]:
        """Backfill Baomoi's dated article sitemaps rather than its short rolling archive."""
        sitemap_index_url = str(backfill.get("sitemap_index_url", "")).strip()
        if not sitemap_index_url:
            raise IncompleteArchiveError("Baomoi daily sitemap index URL is not configured")

        index_response = await self._get(sitemap_index_url)
        try:
            index_root = ET.fromstring(index_response.content)
        except ET.ParseError as exc:
            raise IncompleteArchiveError("Baomoi sitemap index is not valid XML") from exc

        partition_pattern = re.compile(str(backfill.get(
            "partition_pattern", r"/article-(?P<date>\d{8})\.xml$"
        )))
        partitions: dict[date, str] = {}
        for element in index_root.iter():
            if element.tag.rsplit("}", 1)[-1] != "loc" or not element.text:
                continue
            match = partition_pattern.search(element.text.strip())
            if not match:
                continue
            raw_date = match.groupdict().get("date")
            if not raw_date:
                continue
            try:
                partition_date = date.fromisoformat(
                    f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}"
                )
            except ValueError:
                continue
            partitions[partition_date] = element.text.strip()

        expected_dates = {
            window.start_date + timedelta(days=offset)
            for offset in range((window.end_date - window.start_date).days + 1)
        }
        missing_dates = sorted(expected_dates - set(partitions))
        if missing_dates:
            raise IncompleteArchiveError(
                "Baomoi sitemap index is missing dated partitions for "
                f"{len(missing_dates)} requested day(s), starting {missing_dates[0]}"
            )

        raw_terms = [
            *backfill.get("financial_title_terms", []),
            *backfill.get("ticker_title_terms", []),
        ]
        terms = sorted({self._slug_term(str(term)) for term in raw_terms if str(term).strip()})
        terms = [term for term in terms if term]
        if not terms:
            raise IncompleteArchiveError("Baomoi sitemap title candidate terms are not configured")

        day_buffer: dict[date, list[Article]] = {}
        result_articles: dict[str, Article] = {}
        seen_urls: set[str] = set()
        seen_ids: set[str] = set()
        stats = {
            "method": "html_daily_sitemap_backfill",
            "requested_start_date": window.start_date.isoformat(),
            "requested_end_date": window.end_date.isoformat(),
            "sitemap_partitions_expected": len(expected_dates),
            "sitemap_partitions_fetched": 0,
            "candidate_article_urls": 0,
            "article_pages_fetched": 0,
            "article_pages_failed": 0,
            "article_pages_skipped_summary_only": 0,
            "financial_articles": 0,
            "coverage_proven": True,
        }

        def flush_days() -> None:
            for publication_day in sorted(day_buffer):
                batch = day_buffer.pop(publication_day, [])
                if not batch:
                    continue
                if on_batch:
                    try:
                        on_batch(publication_day, batch, "sitemap")
                    except TypeError:
                        on_batch(publication_day, batch)
                else:
                    for article in batch:
                        result_articles[article.source_article_id] = article

        selected = [
            (partition_date, partitions[partition_date])
            # Backfill newest partitions first so current news is available while
            # the full historical window is still being processed.
            for partition_date in sorted(expected_dates, reverse=True)
        ]
        max_concurrent_fetches = max(
            1, int(backfill.get("max_concurrent_article_fetches", 4))
        )
        fetch_semaphore = asyncio.Semaphore(max_concurrent_fetches)

        for partition_date, sitemap_url in selected:
            response = await self._get(sitemap_url)
            try:
                sitemap_root = ET.fromstring(response.content)
            except ET.ParseError as exc:
                stats["coverage_proven"] = False
                self.collection_stats = stats
                raise IncompleteArchiveError(
                    f"Baomoi daily sitemap for {partition_date} is not valid XML"
                ) from exc

            stats["sitemap_partitions_fetched"] += 1
            candidates: list[tuple[str, str | None]] = []
            for entry in sitemap_root.iter():
                if entry.tag.rsplit("}", 1)[-1] != "url":
                    continue
                article_url = None
                last_modified = None
                for child in entry:
                    local_name = child.tag.rsplit("}", 1)[-1]
                    if local_name == "loc" and child.text:
                        article_url = child.text.strip()
                    elif local_name == "lastmod" and child.text:
                        last_modified = child.text.strip()
                if not article_url or not article_url.casefold().endswith(".epi"):
                    continue
                if article_url in seen_urls:
                    continue
                seen_urls.add(article_url)
                if self._url_has_candidate_term(article_url, terms):
                    candidates.append((article_url, last_modified))

            stats["candidate_article_urls"] += len(candidates)
            async def _fetch_candidate(
                article_url: str,
                last_modified: str | None,
            ) -> tuple[Article | None, Exception | None]:
                async with fetch_semaphore:
                    try:
                        return (
                            await self.fetch_article(
                                article_url,
                                category="sitemap",
                                listing_item={"sitemap_lastmod": last_modified},
                            ),
                            None,
                        )
                    except SourceAuthorizationError:
                        raise
                    except Exception as exc:
                        return None, exc

            fetched_candidates = await asyncio.gather(
                *(_fetch_candidate(article_url, last_modified)
                  for article_url, last_modified in candidates)
            )
            for (article_url, _last_modified), (article, error) in zip(
                candidates, fetched_candidates
            ):
                if error is not None:
                    if isinstance(error, SummaryOnlyArticleError):
                        stats["article_pages_skipped_summary_only"] += 1
                        continue
                    stats["article_pages_failed"] += 1
                    continue
                if article is None:
                    stats["article_pages_failed"] += 1
                    continue
                stats["article_pages_fetched"] += 1
                if window.contains(article.published_at) and self.is_financial(article):
                    if article.source_article_id not in seen_ids:
                        seen_ids.add(article.source_article_id)
                        article_day = article.published_at.astimezone(
                            ZoneInfo(self.timezone_name)
                        ).date()
                        article.metadata["collection_method"] = stats["method"]
                        article.metadata.setdefault(
                            "content_scope",
                            "aggregate_excerpt"
                            if any(block.metadata.get("fallback") == "summary_only" for block in article.blocks)
                            else "baomoi_aggregator_full_text",
                        )
                        day_buffer.setdefault(article_day, []).append(article)
                        stats["financial_articles"] += 1

            flush_days()

        flush_days()
        stats["article_failure_ratio"] = round(
            stats["article_pages_failed"]
            / max(1, stats["article_pages_failed"] + stats["article_pages_fetched"] + stats["article_pages_skipped_summary_only"]),
            4,
        )
        max_failure_ratio = float(backfill.get(
            "max_article_failure_ratio",
            self.config.get("discovery", {}).get("max_article_failure_ratio", 0.25),
        ))
        self.collection_stats = stats
        if stats["article_failure_ratio"] > max_failure_ratio:
            raise RuntimeError(
                f"Baomoi sitemap article failure ratio {stats['article_failure_ratio']:.1%} "
                f"exceeds configured limit {max_failure_ratio:.1%}"
            )
        return list(result_articles.values())

    @staticmethod
    def _slug_term(value: str) -> str:
        value = unicodedata.normalize("NFKD", value.casefold().replace("đ", "d"))
        value = "".join(character for character in value if not unicodedata.combining(character))
        return re.sub(r"[^a-z0-9]+", "-", value).strip("-")

    @staticmethod
    def _url_has_candidate_term(url: str, terms: list[str]) -> bool:
        path = urlparse(url).path.casefold().strip("/")
        slug = path.rsplit("/", 1)[-1]
        bounded_slug = f"-{slug}-"
        return any(f"-{term}-" in bounded_slug for term in terms)

    @staticmethod
    def _body_characters(blocks: list[ContentBlock]) -> int:
        text_parts = [block.text or "" for block in blocks]
        for block in blocks:
            if block.table:
                text_parts.extend(
                    str(cell)
                    for row in block.table.get("rows", [])
                    for cell in row
                )
        return len(" ".join(text_parts).strip())

    def _has_full_text(self, blocks: list[ContentBlock], summary: str | None) -> bool:
        minimum = int(self.config.get("article", {}).get("full_text_min_characters", 600))
        summary_length = len((summary or "").strip())
        required = max(minimum, int(summary_length * 1.3))
        return self._body_characters(blocks) >= required

    def _safe_original_source_url(self, value: str | None) -> str | None:
        if not value:
            return None
        url = urljoin(self.base_url + "/", value.strip())
        parsed = urlparse(url)
        host = (parsed.hostname or "").casefold().rstrip(".")
        if parsed.scheme != "https" or not host or parsed.username or parsed.password:
            return None
        try:
            port = parsed.port
        except ValueError:
            return None
        if port not in (None, 443) or host == "baomoi.com" or host.endswith(".baomoi.com"):
            return None
        if host in {"localhost", "localhost.localdomain"} or host.endswith((".local", ".internal")):
            return None
        try:
            if not ipaddress.ip_address(host).is_global:
                return None
        except ValueError:
            pass
        return url

    def _original_html_blocks(self, soup: BeautifulSoup) -> list[ContentBlock]:
        def article_bodies(value: Any):
            if isinstance(value, dict):
                body = value.get("articleBody")
                if isinstance(body, str) and body.strip():
                    yield body
                for child in value.values():
                    yield from article_bodies(child)
            elif isinstance(value, list):
                for child in value:
                    yield from article_bodies(child)

        min_chars = int(self.config.get("article", {}).get("full_text_min_characters", 600))
        jsonld_candidates: list[str] = []
        for script in soup.select('script[type="application/ld+json"]'):
            try:
                parsed = json.loads(script.string or script.get_text())
            except (json.JSONDecodeError, TypeError):
                continue
            jsonld_candidates.extend(article_bodies(parsed))
        for raw_body in sorted(jsonld_candidates, key=len, reverse=True):
            body_soup = BeautifulSoup(raw_body, "lxml")
            paragraphs = [
                re.sub(r"\s+", " ", node.get_text(" ", strip=True)).strip()
                for node in body_soup.find_all("p")
            ]
            paragraphs = [paragraph for paragraph in paragraphs if paragraph]
            if not paragraphs:
                text = re.sub(r"\s+", " ", body_soup.get_text(" ", strip=True)).strip()
                paragraphs = [text] if text else []
            if sum(map(len, paragraphs)) >= min_chars:
                return [
                    ContentBlock(order=index, block_type="paragraph", text=paragraph)
                    for index, paragraph in enumerate(paragraphs)
                ]

        selectors = (
            '[itemprop="articleBody"]', ".article-content", ".article__body",
            ".article-body", ".content-detail", ".detail-content", ".entry-content",
            ".post-content", ".article-detail", ".news-content", ".article-main",
            "main", "article",
        )
        candidates: dict[int, tuple[int, Any]] = {}
        for selector in selectors:
            try:
                nodes = soup.select(selector)
            except Exception:
                continue
            for node in nodes:
                candidate = copy.deepcopy(node)
                for junk in candidate.select(
                    "script,style,noscript,nav,aside,footer,header,form,button,iframe,"
                    ".related,.related-news,.recommend,.recommend-news,.social,.share,"
                    ".comment,.breadcrumb,.advertisement,.ads,[aria-hidden='true']"
                ):
                    junk.decompose()
                visible_length = len(candidate.get_text(" ", strip=True))
                if visible_length >= min_chars:
                    candidates[id(node)] = (visible_length, candidate)
        if not candidates:
            return []
        root = max(candidates.values(), key=lambda item: item[0])[1]
        return self._content_blocks(root)

    async def _fetch_original_source_blocks(self, original_source_url: str | None) -> list[ContentBlock]:
        safe_url = self._safe_original_source_url(original_source_url)
        if not safe_url:
            return []
        try:
            response = await self._get(safe_url)
        except SourceAuthorizationError:
            logger.info("Original source blocked by robots.txt; retaining no excerpt-only copy")
            return []
        except Exception as exc:
            logger.info("Original source fetch failed (%s)", type(exc).__name__)
            return []
        return self._original_html_blocks(BeautifulSoup(response.content, "lxml"))

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

        full_text_required = self.config.get("article", {}).get("full_text_required", False)
        if not blocks and summary and not full_text_required:
            blocks = [ContentBlock(order=0, block_type="paragraph", text=summary, metadata={"fallback": "summary_only"})]
        if not blocks:
            root = None
            for selector in self.config.get("article", {}).get("content_selectors", [".article__body", ".article-body", ".content-body"]):
                root = soup.select_one(selector)
                if root:
                    break
            if root:
                blocks = self._content_blocks(root)
        content_scope = "baomoi_aggregator_full_text"
        if full_text_required and not self._has_full_text(blocks, summary):
            original_blocks = await self._fetch_original_source_blocks(original_source_url)
            if self._has_full_text(original_blocks, summary):
                blocks = original_blocks
                content_scope = "baomoi_original_source_full_text"
            else:
                raise SummaryOnlyArticleError(
                    f"No full article body in Baomoi or its original source: {clean_url}"
                )
        if not blocks:
            raise UnusableArticleError(f"Baomoi article body is missing: {clean_url}")

        canonical_node = soup.find("link", attrs={"rel": re.compile("canonical", re.I)})
        canonical = urljoin(clean_url, str(canonical_node.get("href"))) if canonical_node and canonical_node.get("href") else clean_url

        metadata = {
            "source_id_regex": self.config.get("article_id_regex"),
            "publisher_name": self.config.get("name", "Báo Mới"),
            "publisher_domain": self.config.get("publisher_domain"),
            "homepage_url": self.base_url,
            "attribution_name": self.config.get("attribution_name", "Báo Mới"),
            "attribution_required": self.config.get("access", {}).get("attribution_required") is True,
            "discovered_from": category,
            "collection_scope": self.config.get("use_scope"),
            "rights_status": self.config.get("rights", {}).get("status", "unknown"),
            "internal_only": self.config.get("access", {}).get("internal_only") is True,
            "redistribution_allowed": self.config.get("access", {}).get("redistribution_allowed") is True,
            "download_binary_assets_only_if_rights_status_in": self.config.get("rights", {}).get("binary_asset_statuses", []),
            "binary_asset_collection_enabled": bool(self.config.get("binary_asset_collection_enabled", True)),
            "original_source_url": original_source_url,
            "content_scope": content_scope,
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

