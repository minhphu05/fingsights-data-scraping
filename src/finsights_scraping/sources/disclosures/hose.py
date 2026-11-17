from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from ...domain.disclosures import DisclosureRecord
from ...domain.windows import DateWindow
from .base import OfficialSourceClient, clean_text, infer_disclosure_kind, in_window, parse_local_datetime

SYMBOL_PREFIX = re.compile(r"^\s*([A-Z][A-Z0-9]{1,11})\s*[:：-]")
logger = logging.getLogger("finsights.sources.hose")


class HoseDisclosureClient(OfficialSourceClient):
    """HOSE's public news API is the source of issuer notices and attachments metadata."""

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        self.api_url = str(config.get("api_url", "https://api.hsx.vn/n/api/v1/1/news")).rstrip("/")
        self.media_api_url = str(
            config.get("media_api_url", "https://api.hsx.vn/m/api/v1/1/mediafiles")
        ).rstrip("/")
        self.static_files_base_url = str(
            config.get("static_files_base_url", "https://staticfile.hsx.vn")
        ).rstrip("/")
        self._media_pages: dict[tuple[str, str, int], dict[int, dict[str, Any]]] = {}

    def _detail(self, item_id: str) -> dict[str, Any]:
        payload = self.get_json(f"{self.api_url}/{item_id}")
        data = payload.get("data", {})
        return data if isinstance(data, dict) else {}

    def _media_page(self, related_type: str, related_id: str, year: int, page: int) -> dict[str, Any]:
        key = (related_type, related_id, year)
        cached = self._media_pages.setdefault(key, {})
        if page not in cached:
            payload = self.get_json(
                f"{self.media_api_url}/{related_type}/{related_id}",
                params={
                    "pageIndex": page,
                    "pageSize": int(self.config.get("media_page_size", 100)),
                    "year": year,
                },
            )
            data = payload.get("data", {})
            cached[page] = data if isinstance(data, dict) else {}
        return cached[page]

    def _attachments_for_item(
        self, item_id: str, related_type: Any, related_id: Any, published_at: datetime | None,
    ) -> list[dict[str, str]]:
        if related_type in (None, "") or related_id in (None, ""):
            return []
        related_type = str(related_type)
        related_id = str(related_id)
        year = published_at.year if published_at else datetime.now().year
        max_pages = int(self.config.get("media_max_pages", 100))
        for page_number in range(1, max_pages + 1):
            data = self._media_page(related_type, related_id, year, page_number)
            media_items = data.get("list", [])
            matches: list[dict[str, str]] = []
            for media in media_items:
                file_path = str(media.get("filePath") or "").strip()
                if not file_path or not re.search(rf"/{re.escape(item_id)}/", file_path):
                    continue
                if file_path.startswith(("http://", "https://")):
                    file_url = file_path
                else:
                    normalized_path = file_path.removeprefix("~/").lstrip("/")
                    file_url = urljoin(self.static_files_base_url + "/", normalized_path)
                matches.append({
                    "url": file_url,
                    "label": str(media.get("fileName") or file_path)[:300],
                    "mime_type": str(media.get("fileType") or "")[:100],
                    "source_file_path": file_path[:1000],
                })
            if matches:
                return matches

            paging = data.get("paging", {}) if isinstance(data.get("paging"), dict) else {}
            total_pages = int(paging.get("totalPages") or 0)
            if not media_items or (total_pages and page_number >= total_pages):
                break

            if published_at:
                media_dates = [
                    parse_local_datetime(media.get("publishDate") or media.get("createdDate"))
                    for media in media_items
                ]
                media_dates = [value for value in media_dates if value]
                # The official endpoint is newest-first. Once its last item is
                # older than this news item, later pages cannot contain a match.
                if media_dates and media_dates[-1].date() < published_at.date():
                    break
        else:
            raise RuntimeError(
                f"HOSE attachment page limit reached for item {item_id} ({related_type}/{related_id})"
            )
        return []

    @staticmethod
    def _attachment_urls(value: Any) -> list[dict[str, str]]:
        out: list[dict[str, str]] = []
        seen: set[str] = set()

        def visit(node: Any, label: str = "") -> None:
            if isinstance(node, dict):
                for key, child in node.items():
                    visit(child, str(key))
            elif isinstance(node, list):
                for child in node:
                    visit(child, label)
            elif isinstance(node, str) and node.startswith(("http://", "https://")):
                lower = node.lower().split("?", 1)[0]
                if node not in seen and (lower.endswith((".pdf", ".xls", ".xlsx", ".doc", ".docx", ".zip")) or "download" in lower):
                    seen.add(node)
                    out.append({"url": node, "label": label[:300]})

        visit(value)
        return out

    def collect(self, window: DateWindow) -> list[DisclosureRecord]:
        page_size = min(100, int(self.config.get("page_size", 100)))
        max_pages = int(self.config.get("max_pages", 5000))
        output: list[DisclosureRecord] = []
        target_symbols = {str(value).strip().upper() for value in self.config.get("target_symbols", [])}
        page = 1
        while page <= max_pages:
            payload = self.get_json(self.api_url, params={
                "pageIndex": page,
                "pageSize": page_size,
                "startDate": window.start_date.isoformat(),
                "endDate": window.end_date.isoformat(),
            })
            envelope = payload.get("data", {})
            items = envelope.get("list", []) if isinstance(envelope, dict) else []
            if not items:
                break
            for listing in items:
                item_id = str(listing.get("id") or "").strip()
                if not item_id:
                    continue
                listing_title = clean_text(str(listing.get("title") or "")) or ""
                listing_symbol = str(listing.get("code") or "").strip().upper() or None
                if not listing_symbol:
                    match = SYMBOL_PREFIX.search(listing_title)
                    listing_symbol = match.group(1) if match else None
                # The public list API returns warrants and general exchange notices
                # without an issuer code. The final record filter cannot keep those
                # rows without a target equity symbol, so avoid a detail request for
                # candidates that would be discarded anyway.
                if target_symbols and (not listing_symbol or listing_symbol not in target_symbols):
                    continue
                detail = listing
                if self.config.get("fetch_details", True):
                    try:
                        detail = self._detail(item_id)
                    except Exception:
                        # Keep the public list row if its optional detail endpoint is unavailable.
                        detail = listing
                combined = {**listing, **detail}
                published_at = parse_local_datetime(
                    combined.get("postedDate") or combined.get("approvedDate")
                    or combined.get("createdDate") or combined.get("publishFrom")
                )
                if not in_window(published_at, window):
                    continue
                title = clean_text(str(combined.get("title") or ""))
                if not title:
                    continue
                symbol = str(combined.get("code") or "").strip().upper() or None
                if not symbol:
                    match = SYMBOL_PREFIX.search(title)
                    symbol = match.group(1) if match else None
                if target_symbols and (not symbol or symbol not in target_symbols):
                    continue
                summary = clean_text(str(combined.get("summary") or ""))
                category = str(combined.get("catName") or combined.get("categoryGroupName") or "")
                api_detail_url = f"{self.api_url}/{item_id}"
                link = str(combined.get("link") or "").strip()
                original_url = urljoin(str(self.config.get("website_url", "https://www.hsx.vn/")), link) if link else api_detail_url
                attachments = self._attachments_for_item(
                    item_id,
                    combined.get("relatedType"),
                    combined.get("relatedId"),
                    published_at,
                ) if self.config.get("fetch_attachments", True) else []
                output.append(DisclosureRecord(
                    source_code=str(self.config["source_code"]),
                    source_item_id=item_id[:200],
                    title=title[:600],
                    original_url=original_url,
                    published_at=published_at,
                    summary=summary,
                    symbol=symbol,
                    item_type="issuer_disclosure" if symbol else "regulatory_notice",
                    disclosure_kind=infer_disclosure_kind(f"{title} {summary or ''}"),
                    notice_kind=infer_disclosure_kind(title),
                    authority_name="Sở Giao dịch Chứng khoán Thành phố Hồ Chí Minh (HOSE)",
                    attachments=attachments or self._attachment_urls(combined),
                    metadata={
                        "category": category,
                        "category_id": combined.get("catId"),
                        "related_id": combined.get("relatedId"),
                        "related_type": combined.get("relatedType"),
                        "api_detail_url": api_detail_url,
                        "source_fields": {
                            key: combined.get(key) for key in
                            ("postedDate", "publishFrom", "approvedDate", "createdDate", "newsType")
                        },
                    },
                ))
            paging = envelope.get("paging", {}) if isinstance(envelope, dict) else {}
            total_pages = int(paging.get("totalPages") or 0)
            if page == 1 or page % 20 == 0 or len(items) < page_size:
                logger.info(
                    "HOSE listing progress: page=%d/%s rows=%d target_records=%d",
                    page,
                    total_pages or "unknown",
                    len(items),
                    len(output),
                )
            if len(items) < page_size or (total_pages and page >= total_pages):
                break
            page += 1
        else:
            raise RuntimeError(f"HOSE page limit reached ({max_pages}); requested window may be incomplete")
        return output
