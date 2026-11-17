from __future__ import annotations

import re
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from ...domain.disclosures import DisclosureRecord
from ...domain.windows import DateWindow
from .base import OfficialSourceClient, clean_text, deduplicate_records, extract_attachments, html_tables_as_records


class HnxDisclosureClient(OfficialSourceClient):
    """Reads the listed-company and UPCoM disclosure boards from HNX's public portal."""

    def _enrich_detail(self, record: DisclosureRecord) -> None:
        article_id = record.metadata.get("hnx_article_id")
        if not article_id:
            return
        detail_url = urljoin(self.base_url + "/", str(self.config["detail_path"]).lstrip("/"))
        html, final_url = self.post_html(
            detail_url,
            form_data={
                "pArticlesID": article_id,
                "pFileAttach": record.metadata.get("hnx_file_attach", "1"),
            },
            headers={"X-Requested-With": "XMLHttpRequest", "Referer": record.metadata["hnx_listing_url"]},
        )
        soup = BeautifulSoup(html, "lxml")
        title_node = soup.select_one("h1, h2.title, .news-title, .detail-title")
        title_node = title_node or soup.select_one(".Box-TieuDe label, .Box-TieuDe")
        detail_title = clean_text(title_node.get_text(" ", strip=True)) if title_node else None
        content = soup.select_one(".Box-Noidung") or soup
        detail_text = clean_text(content.get_text(" ", strip=True))
        if detail_title:
            record.title = detail_title[:600]
        if detail_text:
            record.summary = detail_text[:12000]
        found = extract_attachments(soup, final_url)
        known_urls = {item.get("url") for item in record.attachments}
        record.attachments.extend(item for item in found if item.get("url") not in known_urls)
        record.metadata["detail_enriched"] = True

    def collect(self, window: DateWindow) -> list[DisclosureRecord]:
        endpoints = self.config.get("listing_pages", [])
        max_pages = int(self.config.get("max_pages", 1000))
        page_size = int(self.config.get("page_size", 50))
        output: list[DisclosureRecord] = []
        target_symbols = {str(value).strip().upper() for value in self.config.get("target_symbols", [])}

        for endpoint in endpoints:
            listing_url = urljoin(self.base_url + "/", str(endpoint["url"]).lstrip("/"))
            board_code = str(endpoint.get("code", "hnx_listed"))
            search_path = str(endpoint.get("search_path", "")).strip()
            if not search_path:
                raise RuntimeError(f"HNX search_path is required for {board_code}")

            # The public board renders its first page in HTML and loads later/search pages
            # through the same form POST endpoint used by its own search UI. The initial GET
            # establishes the session cookie required by that endpoint.
            self.get_html(listing_url)
            search_url = urljoin(self.base_url + "/", search_path.lstrip("/"))
            symbols: list[str | None] = sorted(target_symbols) if target_symbols else [None]
            for search_symbol in symbols:
                page = 1
                while page <= max_pages:
                    html, final_url = self.post_html(
                        search_url,
                        form_data={
                            "pNumPage": page,
                            "pAction": 1,
                            "pNhomTin": "",
                            "pTieuDeTin": "",
                            "pMaChungKhoan": search_symbol or "",
                            "pFromDate": window.start_date.strftime("%d/%m/%Y"),
                            "pToDate": window.end_date.strftime("%d/%m/%Y"),
                            "pOrderBy": "",
                            "pNumRecord": page_size,
                        },
                        headers={"X-Requested-With": "XMLHttpRequest", "Referer": listing_url},
                    )
                    raw_rows = BeautifulSoup(html, "lxml").select("table tbody tr")
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
                        page_text = " ".join(BeautifulSoup(html, "lxml").get_text(" ", strip=True).split())
                        empty_response = any(
                            phrase in page_text.casefold()
                            for phrase in ("không tìm thấy dữ liệu", "không có dữ liệu", "không có bản ghi")
                        )
                        if not empty_response and page == 1:
                            raise RuntimeError(
                                f"HNX disclosure response could not be parsed for "
                                f"{board_code}/{search_symbol or 'all issuers'} page {page}: "
                                f"{page_text[:300]}"
                            )
                        break

                    for record, row in zip(all_page_records, raw_rows):
                        title_link = row.select_one("a.hrefViewDetail[onclick]")
                        onclick = title_link.get("onclick", "") if title_link else ""
                        match = re.search(r"funcViewDetailArticlesByID\((\d+)\s*,\s*(\d+)\)", onclick)
                        if match:
                            article_id, file_attach = match.groups()
                            record.metadata["hnx_article_id"] = article_id
                            record.metadata["hnx_file_attach"] = file_attach
                            record.metadata["hnx_listing_url"] = listing_url
                            record.source_item_id = f"{board_code}:{article_id}"
                        # The title's href is JavaScript-only. Point the stored record at the
                        # official board; details and file URLs are extracted through its AJAX API.
                        record.original_url = listing_url

                    page_records = [
                        record for record in all_page_records
                        if record.published_at and window.contains(record.published_at)
                    ]
                    if search_symbol:
                        for record in page_records:
                            # HNX often displays a bond code (for example MBB125008) while
                            # searching by the issuer's equity symbol. Retain that source code
                            # in metadata and link the disclosure to the requested issuer.
                            record.metadata["source_security_code"] = record.symbol
                            record.metadata["search_symbol"] = search_symbol
                            record.symbol = search_symbol
                            record.item_type = "issuer_disclosure"
                    output.extend(page_records)
                    if len(raw_rows) < page_size:
                        break
                    page += 1
                else:
                    raise RuntimeError(
                        f"HNX page limit reached ({max_pages}) for {board_code} / {search_symbol or 'all issuers'}"
                    )

        unique_records = deduplicate_records(output)
        if self.config.get("fetch_details", True):
            for record in unique_records:
                try:
                    self._enrich_detail(record)
                except Exception as exc:
                    record.metadata["detail_fetch_error"] = f"{type(exc).__name__}: {exc}"[:500]
        return unique_records
