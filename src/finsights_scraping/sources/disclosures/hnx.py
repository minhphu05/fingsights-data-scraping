from __future__ import annotations

import re
from typing import Any
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup

from ...domain.disclosures import DisclosureRecord
from ...domain.windows import DateWindow
from .base import OfficialSourceClient, clean_text, deduplicate_records, extract_attachments, html_tables_as_records


class HnxDisclosureClient(OfficialSourceClient):
    """Reads the listed-company and UPCoM disclosure boards from HNX's public portal."""

    def _enrich_detail(self, record: DisclosureRecord) -> None:
        if not record.original_url or record.original_url.lower().split("?", 1)[0].endswith(
            (".pdf", ".xls", ".xlsx", ".doc", ".docx", ".zip")
        ):
            return
        html, final_url = self.get_html(record.original_url)
        soup = BeautifulSoup(html, "lxml")
        for selector in ("main", ".detail-content", ".news-detail", ".content-detail", "article"):
            content = soup.select_one(selector)
            if content:
                break
        else:
            content = soup
        title_node = soup.select_one("h1, h2.title, .news-title, .detail-title")
        detail_title = clean_text(title_node.get_text(" ", strip=True)) if title_node else None
        detail_text = clean_text(content.get_text(" ", strip=True))
        if detail_title:
            record.title = detail_title[:600]
        if detail_text:
            record.summary = detail_text[:12000]
        record.original_url = final_url
        found = extract_attachments(content, final_url)
        known_urls = {item.get("url") for item in record.attachments}
        record.attachments.extend(item for item in found if item.get("url") not in known_urls)
        record.metadata["detail_enriched"] = True

    def collect(self, window: DateWindow) -> list[DisclosureRecord]:
        endpoints = self.config.get("listing_pages", [])
        max_pages = int(self.config.get("max_pages", 1000))
        page_param = str(self.config.get("page_param", "page"))
        output: list[DisclosureRecord] = []
        target_symbols = {str(value).strip().upper() for value in self.config.get("target_symbols", [])}
        visited_first_items: set[tuple[str, str]] = set()

        for endpoint in endpoints:
            listing_url = urljoin(self.base_url + "/", str(endpoint["url"]).lstrip("/"))
            board_code = str(endpoint.get("code", "hnx_listed"))
            page = 1
            while page <= max_pages:
                parsed = urlparse(listing_url)
                query = parse_qs(parsed.query, keep_blank_values=True)
                query[page_param] = [str(page)]
                if self.config.get("send_date_filters", False):
                    query[str(self.config.get("start_date_param", "fromDate"))] = [window.start_date.strftime("%d/%m/%Y")]
                    query[str(self.config.get("end_date_param", "toDate"))] = [window.end_date.strftime("%d/%m/%Y")]
                page_url = urlunparse(parsed._replace(query=urlencode(query, doseq=True)))
                html, final_url = self.get_html(page_url)
                all_page_records = html_tables_as_records(
                    html,
                    source_code=str(self.config["source_code"]),
                    authority_name="Sở Giao dịch Chứng khoán Hà Nội (HNX)",
                    base_url=final_url,
                    window=window,
                    page_number=page,
                    include_outside_window=True,
                )
                if not all_page_records:
                    # If the portal changes its HTML contract, do not mark an empty run as success.
                    if page == 1:
                        raise RuntimeError(f"HNX disclosure table could not be parsed: {final_url}")
                    break
                first = (all_page_records[0].source_item_id, all_page_records[0].title)
                if first in visited_first_items:
                    break
                visited_first_items.add(first)
                dates = [record.published_at.date() for record in all_page_records if record.published_at]
                below_start = bool(dates) and max(dates) < window.start_date
                page_records = [
                    record for record in all_page_records
                    if not target_symbols or record.symbol and record.symbol.upper() in target_symbols
                ]
                current_page_records = [
                    record for record in page_records
                    if record.published_at and window.contains(record.published_at)
                ]
                output.extend(current_page_records)
                if self.config.get("fetch_details", True):
                    for record in current_page_records:
                        try:
                            self._enrich_detail(record)
                        except Exception as exc:
                            record.metadata["detail_fetch_error"] = f"{type(exc).__name__}: {exc}"[:500]
                # HNX pages are reverse chronological. Once every row is older than the range,
                # later pages cannot contribute data to the requested window.
                if below_start:
                    break
                if len(all_page_records) < int(self.config.get("page_size", 15)):
                    break
                page += 1
            else:
                raise RuntimeError(f"HNX page limit reached ({max_pages}) for {board_code}")
        return deduplicate_records(output)
