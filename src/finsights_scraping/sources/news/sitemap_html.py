from __future__ import annotations

import calendar
import re
import unicodedata
from datetime import date, timedelta
from typing import Any, Callable
from urllib.parse import unquote, urlparse
from zoneinfo import ZoneInfo

from bs4 import BeautifulSoup, Tag

from ...domain.models import Article
from ...domain.windows import DateWindow
from .cafef import IncompleteArchiveError
from .configured_html import (
    ConfiguredHtmlNewsClient,
    SourceAuthorizationError,
    _date_from_article_url,
)


def _local_name(node: Tag) -> str:
    return str(node.name or "").split(":")[-1].lower()


def _text_without_cdata(node: Tag) -> str:
    value = node.get_text(" ", strip=True)
    value = re.sub(r"^\s*<!\[CDATA\[|\]\]>\s*$", "", value)
    return re.sub(r"\s+", " ", value).strip()


def _fold_text(value: str) -> str:
    decomposed = unicodedata.normalize("NFKD", value.casefold().replace("đ", "d"))
    return "".join(char for char in decomposed if not unicodedata.combining(char))


class SitemapHtmlNewsClient(ConfiguredHtmlNewsClient):
    """Discover dated archive URLs from a publisher sitemap, then parse full HTML pages."""

    async def collect(
        self,
        window: DateWindow,
        on_batch: Callable[[date, list[Article], str | None], None] | None = None,
        archive_only: bool = True,
    ) -> list[Article]:
        if not self.collection_ready():
            raise SourceAuthorizationError(
                f"{self.source_code} is disabled: set collection_enabled and the "
                "licensed_personal_project redistribution policy in its source YAML."
            )

        discovery = self.config.get("discovery", {})
        backfill = discovery.get("html_backfill", {})
        index_url = str(backfill.get("sitemap_index_url", "")).strip()
        if not index_url:
            raise ValueError(f"No HTML backfill sitemap index configured for {self.source_code}")

        index_response = await self._get(index_url)
        index_soup = BeautifulSoup(index_response.content, "xml")
        sitemap_urls = self._sitemap_urls(index_soup)
        partitioned = [
            (url, self._partition_dates(url, backfill))
            for url in sitemap_urls
        ]
        selected = [
            (url, dates)
            for url, dates in partitioned
            if dates and dates[0] <= window.end_date and dates[1] >= window.start_date
        ]
        if not selected:
            raise IncompleteArchiveError(
                f"No dated sitemap partitions for {self.source_code} cover "
                f"{window.start_date}..{window.end_date}"
            )

        self._ensure_partition_coverage(selected, window, str(backfill.get("partition_type", "")))
        self.collection_stats = {
            "method": "html_sitemap_backfill",
            "sitemap_index_url": index_url,
            "sitemaps_selected": len(selected),
            "sitemap_urls_seen": len(sitemap_urls),
            "article_urls_seen": 0,
            "article_urls_filtered_by_title": 0,
            "article_pages_fetched": 0,
            "article_pages_failed": 0,
            "financial_articles": 0,
            "coverage_proven": True,
            "requested_start_date": window.start_date.isoformat(),
            "requested_end_date": window.end_date.isoformat(),
        }

        financial_terms = [
            _fold_text(str(term))
            for term in backfill.get("headline_filter_terms", self.config.get("financial_relevance", {}).get("terms", []))
            if str(term).strip()
        ]
        articles_by_id: dict[str, Article] = {}
        seen_article_ids: set[str] = set()
        publisher_domain = self.publisher_domain

        for sitemap_url, _partition in selected:
            response = await self._get(sitemap_url)
            sitemap_soup = BeautifulSoup(response.content, "xml")
            entries = [node for node in sitemap_soup.find_all(True) if _local_name(node) == "url"]
            if not entries:
                raise IncompleteArchiveError(
                    f"Sitemap {sitemap_url} returned no article entries for {self.source_code}"
                )

            day_batches: dict[date, list[Article]] = {}
            for entry in entries:
                loc_node = next((node for node in entry.find_all(True, recursive=False) if _local_name(node) == "loc"), None)
                if loc_node is None:
                    continue
                article_url = _text_without_cdata(loc_node)
                if not article_url:
                    continue
                article_url = unquote(article_url)
                parsed = urlparse(article_url)
                host = (parsed.hostname or "").lower().removeprefix("www.")
                if parsed.scheme not in {"http", "https"} or not (
                    host == publisher_domain or host.endswith("." + publisher_domain)
                ):
                    continue
                if not re.search(str(self.config["article_id_regex"]), parsed.path, re.IGNORECASE):
                    continue

                self.collection_stats["article_urls_seen"] += 1
                embedded_date = _date_from_article_url(article_url)
                if embedded_date and not (window.start_date <= embedded_date <= window.end_date):
                    continue
                title = self._entry_title(entry)
                headline_text = _fold_text(f"{title} {unquote(parsed.path).replace('-', ' ')}")
                if financial_terms and not any(term in headline_text for term in financial_terms):
                    self.collection_stats["article_urls_filtered_by_title"] += 1
                    continue

                pad_days = backfill.get("lastmod_window_pad_days")
                if pad_days is not None:
                    lastmod = self._entry_lastmod(entry)
                    if lastmod and not (
                        window.start_date - timedelta(days=int(pad_days))
                        <= lastmod
                        <= window.end_date + timedelta(days=int(pad_days))
                    ):
                        continue

                article_id_match = re.search(str(self.config["article_id_regex"]), parsed.path, re.IGNORECASE)
                article_id = article_id_match.groupdict().get("article_id") if article_id_match else None
                if article_id and article_id in seen_article_ids:
                    continue

                try:
                    article = await self.fetch_article(article_url, category="sitemap_archive")
                except SourceAuthorizationError:
                    raise
                except Exception:
                    self.collection_stats["article_pages_failed"] += 1
                    continue

                self.collection_stats["article_pages_fetched"] += 1
                if not window.contains(article.published_at) or not self.is_financial(article):
                    continue
                if article.source_article_id in seen_article_ids:
                    continue
                seen_article_ids.add(article.source_article_id)
                article.metadata["collection_method"] = "html_sitemap_backfill"
                article.metadata["sitemap_url"] = sitemap_url
                self.collection_stats["financial_articles"] += 1
                day = article.published_at.astimezone(ZoneInfo(self.timezone_name)).date()
                day_batches.setdefault(day, []).append(article)

            for batch_date, batch in sorted(day_batches.items()):
                if on_batch:
                    on_batch(batch_date, batch, "sitemap_archive")
                else:
                    for article in batch:
                        articles_by_id[article.source_article_id] = article

        failed = int(self.collection_stats["article_pages_failed"])
        fetched = int(self.collection_stats["article_pages_fetched"])
        max_failure_ratio = float(backfill.get("max_article_failure_ratio", 0.25))
        failure_ratio = failed / max(1, failed + fetched)
        self.collection_stats["article_failure_ratio"] = round(failure_ratio, 4)
        if failed + fetched and failure_ratio > max_failure_ratio:
            raise RuntimeError(
                f"{self.source_code} HTML backfill had {failed}/{failed + fetched} failed article pages "
                f"({failure_ratio:.1%}), above the configured limit {max_failure_ratio:.1%}"
            )

        return sorted(
            articles_by_id.values(),
            key=lambda article: (article.published_at, article.source_article_id),
        )

    @staticmethod
    def _sitemap_urls(soup: BeautifulSoup) -> list[str]:
        return list(dict.fromkeys(
            _text_without_cdata(node)
            for node in soup.find_all(True)
            if _local_name(node) == "loc" and _text_without_cdata(node)
        ))

    @staticmethod
    def _entry_title(entry: Tag) -> str:
        titles = [
            _text_without_cdata(node)
            for node in entry.find_all(True)
            if _local_name(node) in {"title", "news:title"}
        ]
        return next((value for value in titles if value), "")

    @staticmethod
    def _entry_lastmod(entry: Tag) -> date | None:
        for node in entry.find_all(True, recursive=False):
            if _local_name(node) != "lastmod":
                continue
            value = _text_without_cdata(node)
            try:
                return date.fromisoformat(value[:10])
            except ValueError:
                return None
        return None

    @staticmethod
    def _partition_dates(url: str, config: dict[str, Any]) -> tuple[date, date] | None:
        partition_type = str(config.get("partition_type", ""))
        path = urlparse(url).path
        if partition_type == "five_day":
            match = re.search(
                r"sitemaps-(?P<year>20\d{2})-(?P<month>\d{1,2})-"
                r"(?P<start>\d{1,2})-(?P<end>\d{1,2})\.xml$",
                path,
                re.IGNORECASE,
            )
            if not match:
                return None
            try:
                start_day = date(
                    int(match.group("year")), int(match.group("month")), int(match.group("start"))
                )
                end_day = date(
                    int(match.group("year")), int(match.group("month")), int(match.group("end"))
                )
                return start_day, end_day
            except ValueError:
                return None
        if partition_type == "monthly":
            match = re.search(r"news-(?P<year>20\d{2})-(?P<month>\d{2})\.xml$", path, re.IGNORECASE)
            if not match:
                return None
            year, month = int(match.group("year")), int(match.group("month"))
            try:
                return date(year, month, 1), date(year, month, calendar.monthrange(year, month)[1])
            except ValueError:
                return None
        raise ValueError(f"Unsupported sitemap partition type: {partition_type!r}")

    @staticmethod
    def _ensure_partition_coverage(
        selected: list[tuple[str, tuple[date, date]]],
        window: DateWindow,
        partition_type: str,
    ) -> None:
        if partition_type == "monthly":
            covered = {
                (day.year, day.month)
                for _url, (start, end) in selected
                for day in (start, end)
            }
            cursor = window.start_date.replace(day=1)
            end_month = window.end_date.replace(day=1)
            expected: set[tuple[int, int]] = set()
            while cursor <= end_month:
                expected.add((cursor.year, cursor.month))
                if cursor.month == 12:
                    cursor = date(cursor.year + 1, 1, 1)
                else:
                    cursor = date(cursor.year, cursor.month + 1, 1)
            if not expected.issubset(covered):
                raise IncompleteArchiveError(
                    f"Sitemap index misses monthly partitions for {sorted(expected - covered)}"
                )
            return

        spans = sorted((start, end) for _url, (start, end) in selected)
        cursor = window.start_date
        for span_start, span_end in spans:
            if span_end < cursor:
                continue
            if span_start > cursor:
                raise IncompleteArchiveError(
                    f"Sitemap index has a partition gap at {cursor.isoformat()} for {window.start_date}..{window.end_date}"
                )
            cursor = max(cursor, span_end)
            if cursor >= window.end_date:
                return
            cursor += timedelta(days=1)
        if cursor < window.end_date:
            raise IncompleteArchiveError(
                f"Sitemap index ends at {cursor.isoformat()}, before requested end {window.end_date.isoformat()}"
            )
