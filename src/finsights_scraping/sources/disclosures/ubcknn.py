from __future__ import annotations

import re
from typing import Any
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup

from ...domain.disclosures import DisclosureRecord
from ...domain.windows import DateWindow
from .base import OfficialSourceClient, clean_text, deduplicate_records, extract_attachments, html_tables_as_records, infer_disclosure_kind, parse_local_datetime, stable_id
from .browser import HeadlessOfficialBrowser

DOC_NAME = re.compile(r"dDocName=([A-Za-z0-9_-]+)")


def _notice_kind(text: str) -> str:
    value = text.casefold()
    if any(word in value for word in ("xử phạt", "xử phạt vi phạm", "phạt tiền", "đình chỉ")):
        return "penalty"
    if any(word in value for word in ("luật", "nghị định", "thông tư", "quyết định", "quy chế", "văn bản")):
        return "regulation"
    if any(word in value for word in ("thông báo", "chỉ đạo", "cảnh báo", "công bố", "nhắc nhở")):
        return "management_notice"
    return "other"


def _table_and_card_rows(html: str, base_url: str, source_code: str, page_number: int) -> list[DisclosureRecord]:
    # First use the source's tabular disclosure layout when available.
    windowless_records = html_tables_as_records(
        html,
        source_code=source_code,
        authority_name="Ủy ban Chứng khoán Nhà nước (UBCKNN)",
        base_url=base_url,
        window=None,
        page_number=page_number,
        include_outside_window=True,
    )
    if windowless_records:
        for record in windowless_records:
            record.item_type = "regulatory_notice"
            record.notice_kind = _notice_kind(record.title)
            record.disclosure_kind = infer_disclosure_kind(record.title)
            record.authority_name = "Ủy ban Chứng khoán Nhà nước (UBCKNN)"
        return windowless_records

    soup = BeautifulSoup(html, "lxml")
    output: list[DisclosureRecord] = []
    seen: set[str] = set()
    for anchor in soup.select("a[href*='dDocName='], a[href*='chitit']"):
        title = clean_text(anchor.get_text(" ", strip=True))
        if not title:
            continue
        href = urljoin(base_url, anchor.get("href", ""))
        if href in seen:
            continue
        parent = anchor
        for _ in range(5):
            if not parent.parent:
                break
            parent = parent.parent
            if re.search(r"\d{2}/\d{2}/\d{4}", parent.get_text(" ", strip=True)):
                break
        row_text = clean_text(parent.get_text(" ", strip=True)) or title
        # Some SSC titles contain dates themselves (for example an anniversary
        # range). Prefer the date in the card metadata after removing the title.
        listing_context = row_text.replace(title, "", 1).strip()
        date_match = re.search(r"\b(\d{2}/\d{2}/\d{4})(?:\s+(\d{1,2}:\d{2}))?\b", listing_context)
        published_at = parse_local_datetime(
            f"{date_match.group(1)} {date_match.group(2) or '00:00'}" if date_match else None
        )
        doc_match = DOC_NAME.search(href)
        source_item_id = doc_match.group(1) if doc_match else stable_id(source_code, href, title, published_at)
        seen.add(href)
        output.append(DisclosureRecord(
            source_code=source_code,
            source_item_id=source_item_id[:200],
            title=title[:600],
            original_url=href,
            published_at=published_at,
            summary=row_text[:2000],
            item_type="regulatory_notice",
            disclosure_kind=infer_disclosure_kind(title),
            notice_kind=_notice_kind(title),
            authority_name="Ủy ban Chứng khoán Nhà nước (UBCKNN)",
            attachments=extract_attachments(parent, base_url),
            metadata={"page": page_number, "listing_text": row_text},
        ))
    return output


class UbcknnNoticeClient(OfficialSourceClient):
    """Reads publicly posted SSC management notices and legal/penalty notices."""

    def collect(self, window: DateWindow) -> list[DisclosureRecord]:
        list_urls = self.config.get("list_urls", [])
        if not list_urls:
            raise ValueError("UBCKNN config must define at least one official public list URL")
        max_pages = int(self.config.get("max_pages", 100))
        page_param = str(self.config.get("page_param", "selectedPage"))
        output: list[DisclosureRecord] = []
        seen_page_fingerprints: set[tuple[str, str]] = set()
        with HeadlessOfficialBrowser(user_agent=self.client.headers.get("User-Agent")) as browser:
            for list_url in list_urls:
                for page_number in range(1, max_pages + 1):
                    parsed = urlparse(str(list_url))
                    query = parse_qs(parsed.query, keep_blank_values=True)
                    query[page_param] = [str(page_number)]
                    requested_url = urlunparse(parsed._replace(query=urlencode(query, doseq=True)))
                    html, final_url, status, duration = browser.goto(requested_url)
                    self.fetch_attempts.append(HeadlessOfficialBrowser.attempt(final_url, html, status=status, duration_ms=duration))
                    records = _table_and_card_rows(
                        html, final_url, str(self.config["source_code"]), page_number
                    )
                    if not records:
                        if page_number == 1:
                            body = BeautifulSoup(html, "lxml").get_text(" ", strip=True)
                            raise RuntimeError(
                                "UBCKNN page rendered without parsable notice rows; "
                                f"URL={final_url}, title={BeautifulSoup(html, 'lxml').title}, body={body[:250]}"
                            )
                        break
                    fingerprint = (records[0].source_item_id, records[0].title)
                    if fingerprint in seen_page_fingerprints:
                        break
                    seen_page_fingerprints.add(fingerprint)
                    old_page = bool(records) and all(
                        record.published_at and record.published_at.date() < window.start_date for record in records
                    )
                    for record in records:
                        if not record.published_at or not window.contains(record.published_at):
                            continue
                        try:
                            detail_html, detail_url, detail_status, detail_duration = browser.goto(record.original_url)
                            self.fetch_attempts.append(HeadlessOfficialBrowser.attempt(
                                detail_url, detail_html, status=detail_status, duration_ms=detail_duration
                            ))
                            detail_soup = BeautifulSoup(detail_html, "lxml")
                            detail_text = clean_text(detail_soup.get_text(" ", strip=True)) or record.summary or ""
                            record.summary = detail_text[:12000]
                            record.original_url = detail_url
                            record.attachments = extract_attachments(detail_soup, detail_url)
                            record.metadata["detail_text"] = detail_text[:2000]
                            record.notice_kind = _notice_kind(f"{record.title} {detail_text}")
                        except Exception as exc:
                            # The official list remains useful if a specific linked detail is temporarily unavailable.
                            record.metadata["detail_fetch_error"] = f"{type(exc).__name__}: {exc}"[:500]
                        output.append(record)
                    if old_page:
                        break
                else:
                    raise RuntimeError(f"UBCKNN page limit reached ({max_pages}) for {list_url}")
        return deduplicate_records(output)
