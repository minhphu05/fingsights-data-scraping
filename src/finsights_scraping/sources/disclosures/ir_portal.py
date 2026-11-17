from __future__ import annotations

import hashlib
import re
from collections import deque
from datetime import datetime
from types import SimpleNamespace
from typing import Any
from unicodedata import normalize
from urllib.parse import urldefrag, urljoin, urlparse
from zoneinfo import ZoneInfo

from bs4 import BeautifulSoup

from ...domain.disclosures import DisclosureRecord
from ...domain.windows import DateWindow
from .base import OfficialSourceClient, clean_text, infer_disclosure_kind
from .browser import HeadlessOfficialBrowser

LOCAL_TZ = ZoneInfo("Asia/Ho_Chi_Minh")
DOCUMENT_EXTENSIONS = {".pdf", ".xls", ".xlsx", ".doc", ".docx", ".ppt", ".pptx", ".zip", ".csv"}
IR_KEYWORDS = (
    "report", "financial", "annual", "earnings", "investor", "shareholder", "disclosure",
    "document", "meeting", "quarter", "presentation", "announcement", "result", "ir",
    "bao-cao", "bao cao", "tai-chinh", "tai chinh", "cong-bo", "cong bo", "co-dong",
    "co dong", "nha-dau-tu", "nha dau tu", "tai-lieu", "tai lieu", "ket-qua", "ket qua",
)
MONTHS = {
    "jan": 1, "january": 1, "thang mot": 1, "feb": 2, "february": 2, "thang hai": 2,
    "mar": 3, "march": 3, "thang ba": 3, "apr": 4, "april": 4, "thang tu": 4,
    "may": 5, "thang nam": 5, "jun": 6, "june": 6, "thang sau": 6,
    "jul": 7, "july": 7, "thang bay": 7, "aug": 8, "august": 8, "thang tam": 8,
    "sep": 9, "sept": 9, "september": 9, "thang chin": 9,
    "oct": 10, "october": 10, "thang muoi": 10, "nov": 11, "november": 11,
    "thang muoi mot": 11, "dec": 12, "december": 12, "thang muoi hai": 12,
}


def _ascii(value: str) -> str:
    return " ".join(normalize("NFKD", value).encode("ascii", "ignore").decode("ascii").casefold().split())


def _date_in(value: str) -> datetime | None:
    text = _ascii(value)
    # ISO and numeric dates, including compact dates embedded in official filenames.
    patterns = (
        (r"(?<!\d)(20\d{2})[-/.](\d{1,2})[-/.](\d{1,2})(?!\d)", "ymd"),
        (r"(?<!\d)(\d{1,2})[-/.](\d{1,2})[-/.](20\d{2})(?!\d)", "dmy"),
        (r"(?<!\d)(20\d{2})(\d{2})(\d{2})(?!\d)", "compact"),
    )
    for pattern, kind in patterns:
        match = re.search(pattern, text)
        if match:
            values = [int(v) for v in match.groups()]
            try:
                year, month, day = (values if kind in {"ymd", "compact"} else (values[2], values[1], values[0]))
                return datetime(year, month, day, tzinfo=LOCAL_TZ)
            except ValueError:
                pass

    # Vietnamese and English month-name dates, e.g. "29 Tháng Ba 2026".
    month_names = "|".join(sorted((re.escape(k) for k in MONTHS), key=len, reverse=True))
    for pattern in (
        rf"\b(\d{{1,2}})\s+(?:thang\s+)?({month_names})\s+(20\d{{2}})\b",
        rf"\b({month_names})\s+(\d{{1,2}})(?:st|nd|rd|th)?[,]?\s+(20\d{{2}})\b",
    ):
        match = re.search(pattern, text)
        if not match:
            continue
        if match.group(1).isdigit():
            day, month_name, year = int(match.group(1)), match.group(2), int(match.group(3))
        else:
            month_name, day, year = match.group(1), int(match.group(2)), int(match.group(3))
        # Remove the generic "thang" prefix to find the Vietnamese month name.
        month_name = month_name.strip()
        month = MONTHS.get(month_name) or MONTHS.get(f"thang {month_name}")
        if month:
            try:
                return datetime(year, month, day, tzinfo=LOCAL_TZ)
            except ValueError:
                pass
    return None


def _date_for(text: str, url: str) -> datetime | None:
    # Prefer the visible publication date. URL dates are a fallback for dated files/archives.
    return _date_in(text) or _date_in(url)


def _is_document(url: str, label: str) -> bool:
    parsed = urlparse(url)
    path = parsed.path.casefold()
    if any(path.endswith(ext) for ext in DOCUMENT_EXTENSIONS):
        return True
    lowered = f"{path} {label.casefold()}"
    if parsed.hostname and parsed.hostname.casefold() == "drive.google.com" and path.startswith("/file/d/"):
        return any(token in lowered for token in IR_KEYWORDS + ("download", "tải về", "file"))
    return any(token in path for token in ("/download", "/attachment", "/filedownload", "documentdownload", "/media/")) and any(
        token in lowered for token in ("report", "financial", "annual", "investor", "document", "statement", "tai-lieu", "bao-cao", "bctc", "download", "tai ve")
    )


def _direct_document_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.hostname and parsed.hostname.casefold() == "drive.google.com" and parsed.path.startswith("/file/d/"):
        file_id = parsed.path.split("/file/d/", 1)[1].split("/", 1)[0]
        if file_id:
            return f"https://drive.google.com/uc?export=download&id={file_id}"
    return url


class InvestorRelationsClient(OfficialSourceClient):
    """Crawl public company IR listings and normalize linked reports into disclosures."""

    def __init__(self, group_config: dict[str, Any], company_config: dict[str, Any]):
        listing_urls = [str(url) for url in company_config.get("listing_urls", [])]
        if not listing_urls:
            raise ValueError(f"No IR listing_urls configured for {company_config.get('symbol')}")
        config = {**group_config, **company_config}
        config["base_url"] = str(config.get("base_url") or listing_urls[0])
        config["source_code"] = str(config.get("source_code") or f"ir_{str(company_config['symbol']).lower()}")
        super().__init__(config)
        self.listing_urls = listing_urls
        self.symbol = str(company_config["symbol"]).upper()
        self.company_name = str(company_config["company_name"])
        self.allowed_hosts = {str(host).casefold().lstrip(".") for host in company_config.get("allowed_hosts", [])}
        if not self.allowed_hosts:
            host = urlparse(listing_urls[0]).hostname
            if host:
                self.allowed_hosts = {host.casefold()}
        self.max_pages = max(1, int(group_config.get("max_pages_per_company", 18)))
        self.max_documents = max(1, int(group_config.get("max_documents_per_company", 300)))
        self.render_with_browser = bool(company_config.get("render_with_browser", group_config.get("render_with_browser", False)))
        self._browser: HeadlessOfficialBrowser | None = None
        self.collection_stats: dict[str, Any] = {"pages_visited": 0, "items_discovered": 0, "undated_items": 0, "page_errors": 0}

    def close(self) -> None:
        if self._browser is not None:
            self._browser.__exit__(None, None, None)
            self._browser = None
        super().close()

    def _get_page(self, url: str):
        if self.render_with_browser:
            try:
                if self._browser is None:
                    self._browser = HeadlessOfficialBrowser(
                        timeout_ms=int(float(self.config.get("timeout_seconds", 35)) * 1000),
                        user_agent=str(self.config.get("user_agent") or "FinsightsDataCollector/1.0 (+https://finsights.vn)"),
                    )
                    self._browser.__enter__()
                html, final_url, status, duration_ms = self._browser.goto(url)
                attempt = HeadlessOfficialBrowser.attempt(final_url, html, status=status, duration_ms=duration_ms)
                self.fetch_attempts.append(attempt)
                if status is not None and status >= 400:
                    raise ValueError(f"Browser received HTTP {status} for {url}")
                return SimpleNamespace(
                    url=final_url,
                    content=html.encode("utf-8"),
                    text=html,
                    headers={"content-type": "text/html; browser-rendered"},
                )
            except Exception:
                # Fall back to the same public page over HTTP if Chromium is unavailable.
                if self._browser is not None:
                    self._browser.__exit__(None, None, None)
                    self._browser = None
        response = self.request("GET", url)
        return response

    def _same_company_site(self, url: str) -> bool:
        host = (urlparse(url).hostname or "").casefold()
        return any(host == allowed or host.endswith("." + allowed) for allowed in self.allowed_hosts)

    @staticmethod
    def _anchor_urls(anchor, base_url: str) -> list[str]:
        urls: list[str] = []
        for attr in ("href", "data-href", "data-url", "data-file", "data-download-url"):
            raw = anchor.get(attr)
            if raw and not str(raw).strip().startswith(("#", "javascript:", "mailto:", "tel:")):
                absolute, _fragment = urldefrag(urljoin(base_url, str(raw).strip()))
                if absolute.startswith(("http://", "https://")) and absolute not in urls:
                    urls.append(absolute)
        return urls

    @staticmethod
    def _anchor_context(anchor) -> str:
        chunks = []
        node = anchor
        for _ in range(4):
            if node is None:
                break
            text = clean_text(str(node))
            if text and text not in chunks:
                chunks.append(text[:1200])
            node = getattr(node, "parent", None)
        return " | ".join(chunks)

    @staticmethod
    def _title_from_anchor(anchor, context: str) -> str:
        title = clean_text(anchor.get_text(" ", strip=True))
        if title and title.casefold() not in {"download", "read more", "view here", "tải về", "xem chi tiết"}:
            return title[:600]
        for part in context.split(" | "):
            if len(part) > 8:
                return part[:600]
        return title or "Investor relations document"

    def _record(
        self, *, item_url: str, source_page: str, title: str, date_text: str,
        description: str | None = None, attachment_url: str | None = None,
        attachment_label: str | None = None,
    ) -> DisclosureRecord | None:
        published_at = _date_for(date_text, item_url)
        if published_at and not self._window.contains(published_at):
            return None
        source_year = None
        if published_at is None:
            year_match = re.search(r"\b(20\d{2})\b", _ascii(f"{title} {date_text} {item_url}"))
            source_year = int(year_match.group(1)) if year_match else None
            if source_year is not None and not self._window.start_date.year <= source_year <= self._window.end_date.year:
                return None
            if source_year is None and not self.config.get("include_undated_items", False):
                return None
        identity = hashlib.sha256(f"{self.symbol}|{item_url}".encode("utf-8")).hexdigest()[:40]
        title = clean_text(title) or f"{self.symbol} investor relations publication"
        attachments = []
        if attachment_url:
            attachments.append({"url": attachment_url, "label": (attachment_label or title)[:300]})
        date_state = "published_date_found" if published_at else (
            "source_year_only" if source_year is not None else "published_date_missing"
        )
        return DisclosureRecord(
            source_code=str(self.config["source_code"]),
            source_item_id=identity,
            title=title[:600],
            original_url=item_url,
            published_at=published_at,
            summary=(description or "")[:2000] or None,
            symbol=self.symbol,
            company_name=self.company_name,
            item_type="issuer_disclosure",
            disclosure_kind=infer_disclosure_kind(title),
            notice_kind=infer_disclosure_kind(title),
            authority_name=self.company_name,
            attachments=attachments,
            metadata={
                "collection_method": "official_ir_html",
                "source_page_url": source_page,
                "source_group": self.config.get("source_group"),
                "date_state": date_state,
                "source_year": source_year,
                "rights_status": "link_only",
            },
        )

    def collect(self, window: DateWindow) -> list[DisclosureRecord]:
        self._window = window
        queue = deque((url, 0) for url in self.listing_urls)
        visited: set[str] = set()
        records: dict[str, DisclosureRecord] = {}

        while queue and len(visited) < self.max_pages and len(records) < self.max_documents:
            url, depth = queue.popleft()
            if url in visited or not self._same_company_site(url):
                continue
            visited.add(url)
            try:
                response = self._get_page(url)
            except Exception:
                self.collection_stats["page_errors"] += 1
                continue
            self.collection_stats["pages_visited"] += 1
            self.fetch_attempts[-1]["page_number"] = len(visited)
            content_type = response.headers.get("content-type", "").casefold()
            if "html" not in content_type and "json" not in content_type:
                title = urlparse(url).path.rsplit("/", 1)[-1] or f"{self.symbol} IR document"
                record = self._record(
                    item_url=url, source_page=url, title=title, date_text=url,
                    attachment_url=url, attachment_label=title,
                )
                if record:
                    records[record.source_item_id] = record
                continue

            soup = BeautifulSoup(response.content, "lxml")
            page_title = clean_text(soup.title.get_text(" ", strip=True)) if soup.title else ""
            page_text = clean_text(soup.get_text(" ", strip=True)) or ""
            page_date = _date_for(page_text[:12000], url)
            page_attachments: list[tuple[str, str, str]] = []
            document_urls: set[str] = set()

            # Several official portals use span/div elements as clickable cards instead of anchors.
            for anchor in soup.select("[href], [data-href], [data-url], [data-file], [data-download-url]"):
                if anchor.name in {"link", "script", "img", "source", "meta"}:
                    continue
                label = clean_text(anchor.get_text(" ", strip=True)) or ""
                context = self._anchor_context(anchor)
                for target in self._anchor_urls(anchor, url):
                    if _is_document(target, f"{label} {context}"):
                        target = _direct_document_url(target)
                        document_urls.add(target)
                        doc_title = self._title_from_anchor(anchor, context)
                        record = self._record(
                            item_url=target,
                            source_page=url,
                            title=doc_title,
                            date_text=f"{context} {label} {target}",
                            description=context,
                            attachment_url=target,
                            attachment_label=label or doc_title,
                        )
                        if record:
                            records.setdefault(record.source_item_id, record)
                            page_attachments.append((target, doc_title, context))
                        continue

                    if depth + 1 >= 3 or not self._same_company_site(target):
                        continue
                    candidate = f"{target} {label}".casefold()
                    if any(part in candidate for part in ("/login", "/sign-in", "/signin", "returnurl=")):
                        continue
                    if any(keyword in _ascii(candidate) for keyword in IR_KEYWORDS) and target not in visited:
                        queue.append((target, depth + 1))

            # Next.js and other server-rendered apps serialize report URLs inside script
            # payloads rather than adding a conventional <a href="..."> to the HTML.
            raw_html = response.text.replace("\\/", "/").replace("\\u002F", "/").replace("&amp;", "&")
            embedded_pattern = re.compile(
                r"(?P<url>(?:https?:)?//[^\s\"'<>\\]+?\.(?:pdf|xlsx?|docx?|pptx?|zip|csv)(?:\?[^\s\"'<>\\]*)?|/[^\s\"'<>\\]+?\.(?:pdf|xlsx?|docx?|pptx?|zip|csv)(?:\?[^\s\"'<>\\]*)?)",
                re.IGNORECASE,
            )
            for match in embedded_pattern.finditer(raw_html):
                target = urljoin(url, match.group("url").rstrip(".,;)}]"))
                target = _direct_document_url(target)
                if target in document_urls:
                    continue
                document_urls.add(target)
                before = raw_html[max(0, match.start() - 1000):match.start()]
                around = raw_html[max(0, match.start() - 600):min(len(raw_html), match.end() + 600)]
                context = clean_text(BeautifulSoup(around, "lxml").get_text(" ", strip=True)) or ""
                filename = urlparse(target).path.rsplit("/", 1)[-1]
                title = re.sub(r"[_-]+", " ", re.sub(r"\.[^.]+$", "", filename)).strip()
                title = title.replace("%20", " ") or f"{self.symbol} investor relations document"
                record = self._record(
                    item_url=target,
                    source_page=url,
                    title=title,
                    date_text=f"{context} {before[-250:]} {page_text[:6000]} {target}",
                    description=context or None,
                    attachment_url=target,
                    attachment_label=filename,
                )
                if record:
                    records.setdefault(record.source_item_id, record)
                    page_attachments.append((target, title, context))

            # Some IR portals publish a short HTML disclosure page without a file attachment.
            if depth > 0 and len(page_text) >= 160 and any(k in _ascii(f"{page_title} {url}") for k in IR_KEYWORDS):
                item_title = page_title or f"{self.symbol} investor relations update"
                record = self._record(
                    item_url=url, source_page=url, title=item_title,
                    date_text=f"{page_text[:4000]} {url}",
                    description=page_text[:2000],
                )
                if record:
                    records.setdefault(record.source_item_id, record)

            # A date listed next to a linked attachment is preferred; the listing page date is only fallback.
            if page_date:
                for doc_url, doc_title, context in page_attachments:
                    identity = hashlib.sha256(f"{self.symbol}|{doc_url}".encode("utf-8")).hexdigest()[:40]
                    existing = records.get(identity)
                    if existing and existing.published_at is None:
                        existing.published_at = page_date
                        existing.metadata["date_state"] = "listing_page_date_fallback"

        self.collection_stats["items_discovered"] = len(records)
        self.collection_stats["undated_items"] = sum(1 for item in records.values() if item.published_at is None)
        return list(records.values())
