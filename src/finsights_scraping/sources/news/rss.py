from __future__ import annotations

import hashlib
import re
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Any, Callable
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from bs4 import BeautifulSoup, Tag

from ...domain.models import Article
from ...domain.windows import DateWindow
from .cafef import _clean_url
from .configured_html import ConfiguredHtmlNewsClient, SourceAuthorizationError


def _local_name(tag: Tag) -> str:
    return str(tag.name or "").split(":")[-1].lower()


def _child_text(element: Tag, *names: str) -> str | None:
    wanted = {name.lower() for name in names}
    for child in element.find_all(recursive=False):
        if _local_name(child) not in wanted:
            continue
        value = child.get("href") or child.get("url") or child.get_text(" ", strip=True)
        value = re.sub(r"\s+", " ", str(value)).strip()
        if value:
            return value
    return None


def _parse_feed_date(value: str | None, timezone_name: str) -> datetime | None:
    if not value:
        return None
    raw = value.strip().replace("\u202f", " ").replace("\xa0", " ")
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        parsed = None
    if parsed is None:
        try:
            parsed = parsedate_to_datetime(raw)
        except (TypeError, ValueError, OverflowError):
            parsed = None
    if parsed is None:
        for pattern in (
            "%m/%d/%Y %I:%M:%S %p",
            "%m/%d/%Y %H:%M:%S",
            "%d/%m/%Y %H:%M:%S",
            "%d-%m-%Y %H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
        ):
            try:
                parsed = datetime.strptime(raw, pattern)
                break
            except ValueError:
                continue
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo(timezone_name))
    return parsed


def _plain_text(value: str | None, limit: int = 5000) -> str | None:
    if not value:
        return None
    text = BeautifulSoup(value, "html.parser").get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:limit] or None


class RssNewsClient(ConfiguredHtmlNewsClient):
    """Collect RSS metadata only; never fetch publisher article pages."""

    def __init__(self, config: dict[str, Any], security_matcher=None, timeout_seconds: float = 30.0):
        super().__init__(config, security_matcher=security_matcher, timeout_seconds=timeout_seconds)
        self.feed_stats: list[dict[str, Any]] = []
        self.collection_stats: dict[str, Any] = {}

    async def collect(
        self,
        window: DateWindow,
        on_batch: Callable[[Any, list[Article], str | None], None] | None = None,
    ) -> list[Article]:
        if not self.collection_ready():
            raise SourceAuthorizationError(
                f"{self.source_code} is disabled; check its RSS access policy in the source YAML."
            )

        feeds = self.config.get("discovery", {}).get("rss_feeds", [])
        if not feeds:
            raise ValueError(f"No RSS feeds configured for {self.source_code}")

        publisher_domain = self.publisher_domain
        timezone_name = self.timezone_name
        rights = self.config.get("rights", {})
        access = self.config.get("access", {})
        articles_by_id: dict[str, Article] = {}
        successful_feeds = 0
        all_feed_dates: list[datetime] = []
        total_items = 0

        for feed in feeds:
            feed_url = str(feed["url"] if isinstance(feed, dict) else feed)
            feed_category = str(feed.get("category", "rss") if isinstance(feed, dict) else "rss")
            stats: dict[str, Any] = {
                "url": feed_url,
                "category": feed_category,
                "http_status": None,
                "items_seen": 0,
                "items_in_window": 0,
                "items_financial": 0,
                "items_without_date": 0,
                "oldest_item_at": None,
                "newest_item_at": None,
                "outcome": "FAILED",
            }
            attempt_start = len(self.fetch_attempts)
            try:
                response = await self._get(feed_url)
                for attempt in self.fetch_attempts[attempt_start:]:
                    attempt["resource_kind"] = "rss_feed"
                stats["http_status"] = response.status_code
                stats["content_type"] = response.headers.get("content-type")
                soup = BeautifulSoup(response.content, "xml")
                entries = [
                    tag for tag in soup.find_all(True)
                    if _local_name(tag) in {"item", "entry"}
                ]
                successful_feeds += 1
                stats["outcome"] = "SUCCESS"
                dates_in_feed: list[datetime] = []

                for entry in entries:
                    stats["items_seen"] += 1
                    total_items += 1
                    title = _plain_text(_child_text(entry, "title"), limit=1000)
                    link = _child_text(entry, "link")
                    if not link:
                        link = _child_text(entry, "guid", "id")
                    if not title or not link:
                        continue
                    canonical_url = _clean_url(link)
                    parsed_url = urlparse(canonical_url)
                    host = (parsed_url.hostname or "").lower().removeprefix("www.")
                    if parsed_url.scheme not in {"http", "https"} or not (
                        host == publisher_domain or host.endswith("." + publisher_domain)
                    ):
                        continue

                    published_raw = _child_text(
                        entry, "pubdate", "published", "updated", "date", "datepublished"
                    )
                    published_at = _parse_feed_date(published_raw, timezone_name)
                    if published_at is None:
                        stats["items_without_date"] += 1
                        continue
                    dates_in_feed.append(published_at)
                    all_feed_dates.append(published_at)
                    if not window.contains(published_at):
                        continue
                    stats["items_in_window"] += 1

                    # Never copy content:encoded or article-body fields; retain only the RSS summary/excerpt.
                    summary = _plain_text(_child_text(entry, "description", "summary"))
                    author = _plain_text(_child_text(entry, "author", "creator", "managingeditor"), limit=220)
                    item_categories = [
                        _plain_text(child.get_text(" ", strip=True), limit=120)
                        for child in entry.find_all(recursive=False)
                        if _local_name(child) == "category"
                    ]
                    item_categories = [value for value in item_categories if value]
                    section = item_categories[0] if item_categories else feed_category
                    guid = _child_text(entry, "guid", "id")
                    id_match = re.search(
                        str(self.config.get("article_id_regex", r"$^")),
                        parsed_url.path,
                        re.IGNORECASE,
                    )
                    if id_match:
                        source_article_id = id_match.groupdict().get("article_id") or id_match.group(0)
                    elif guid and not guid.lower().startswith(("http://", "https://")):
                        source_article_id = guid
                    else:
                        source_article_id = hashlib.sha256(canonical_url.encode("utf-8")).hexdigest()
                    source_article_id = str(source_article_id).strip()
                    if len(source_article_id) > 180:
                        source_article_id = hashlib.sha256(source_article_id.encode("utf-8")).hexdigest()

                    feed_image = None
                    for child in entry.find_all(True):
                        if _local_name(child) in {"thumbnail", "content", "enclosure"} and child.get("url"):
                            feed_image = str(child.get("url"))
                            break

                    fingerprint_payload = "\n".join(
                        (title.casefold(), published_at.isoformat(), (summary or "").casefold())
                    )
                    fingerprint = hashlib.sha256(fingerprint_payload.encode("utf-8")).hexdigest()
                    metadata = {
                        "collection_method": "rss_only",
                        "rss_feed_url": feed_url,
                        "rss_guid": guid,
                        "rss_categories": item_categories or [feed_category],
                        "publisher_name": self.config.get("name", self.publisher_code),
                        "publisher_domain": publisher_domain,
                        "homepage_url": self.config.get("base_url"),
                        "attribution_name": self.config.get("attribution_name", self.publisher_code),
                        "terms_url": self.config.get("terms_url"),
                        "rights_status": rights.get("status", "rss_excerpt_only"),
                        "collection_scope": self.config.get("use_scope"),
                        "internal_only": access.get("internal_only") is True,
                        "redistribution_allowed": access.get("redistribution_allowed") is True,
                        "binary_asset_collection_enabled": False,
                        "content_fingerprint_sha256": fingerprint,
                        "financial_terms": [
                            str(term) for term in self.config.get("financial_relevance", {}).get("terms", [])
                            if str(term).casefold() in f"{title} {summary or ''}".casefold()
                        ],
                    }
                    article = Article(
                        source_code=self.source_code,
                        publisher_code=self.publisher_code,
                        source_article_id=source_article_id,
                        canonical_url=canonical_url,
                        title=title,
                        published_at=published_at,
                        summary=summary,
                        author=author,
                        section=section,
                        cover_image_url=feed_image,
                        blocks=[],
                        metadata=metadata,
                    )
                    if not self.is_financial(article):
                        continue
                    stats["items_financial"] += 1
                    if self.security_matcher is not None:
                        local_date = published_at.astimezone(ZoneInfo(timezone_name)).date()
                        matches = self.security_matcher.match(
                            f"{title} {summary or ''}", local_date
                        )
                        metadata["matched_securities"] = [
                            {
                                "security_uid": match.security_uid,
                                "symbol": match.symbol,
                                "alias_text": match.alias_text,
                                "alias_type": match.alias_type,
                                "confidence": match.confidence,
                                "method": match.method,
                                "evidence": match.evidence,
                            }
                            for match in matches
                        ]
                    previous = articles_by_id.get(source_article_id)
                    if previous:
                        categories = set(previous.metadata.get("rss_categories", []))
                        categories.update(metadata["rss_categories"])
                        previous.metadata["rss_categories"] = sorted(categories)
                        if len(summary or "") > len(previous.summary or ""):
                            previous.summary = summary
                    else:
                        articles_by_id[source_article_id] = article

                if dates_in_feed:
                    stats["oldest_item_at"] = min(dates_in_feed).isoformat()
                    stats["newest_item_at"] = max(dates_in_feed).isoformat()
                self.feed_stats.append(stats)
            except Exception as exc:
                for attempt in self.fetch_attempts[attempt_start:]:
                    attempt["resource_kind"] = "rss_feed"
                stats["error"] = f"{type(exc).__name__}: {exc}"[:500]
                self.feed_stats.append(stats)

        if successful_feeds == 0:
            errors = "; ".join(str(item.get("error", "feed returned no response")) for item in self.feed_stats)
            raise RuntimeError(f"Every RSS feed failed for {self.source_code}: {errors}")

        articles = sorted(
            articles_by_id.values(),
            key=lambda item: (item.published_at, item.source_article_id),
        )
        if on_batch:
            articles_by_day: dict[Any, list[Article]] = {}
            for article in articles:
                day = article.published_at.astimezone(ZoneInfo(timezone_name)).date()
                articles_by_day.setdefault(day, []).append(article)
            for day, day_articles in sorted(articles_by_day.items()):
                on_batch(day, day_articles, None)

        oldest_feed_date = min(all_feed_dates).astimezone(ZoneInfo(timezone_name)).date() if all_feed_dates else None
        coverage_limited = oldest_feed_date is None or oldest_feed_date > window.start_date
        self.collection_stats = {
            "method": "rss_only",
            "requested_start_date": window.start_date.isoformat(),
            "requested_end_date": window.end_date.isoformat(),
            "configured_feeds": len(feeds),
            "successful_feeds": successful_feeds,
            "items_seen": total_items,
            "articles_in_window": len(articles),
            "oldest_feed_publication_date": oldest_feed_date.isoformat() if oldest_feed_date else None,
            "newest_feed_publication_date": max(all_feed_dates).astimezone(ZoneInfo(timezone_name)).date().isoformat() if all_feed_dates else None,
            "coverage_status": "RSS_RETENTION_LIMITED" if coverage_limited else "RSS_WINDOW_NOT_PROVEN",
            "window_coverage_proven": False,
            "historical_limit_note": (
                "RSS endpoints expose a rolling feed; the feed does not prove coverage from the requested start date."
            ),
            "feeds": self.feed_stats,
        }
        return articles
