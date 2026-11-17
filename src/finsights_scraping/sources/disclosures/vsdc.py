from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from datetime import date
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from ...domain.disclosures import DisclosureRecord
from ...domain.windows import DateWindow
from .base import OfficialSourceClient, clean_text, extract_attachments, infer_disclosure_kind, parse_local_date, parse_local_datetime
from .browser import HeadlessOfficialBrowser

EVENT_HREF = re.compile(r"^/vi/ad1?/\d+/?$")
UPDATE_DATE = re.compile(r"Cập nhật ngày\s+(\d{2}/\d{2}/\d{4})(?:\s*[-–]\s*(\d{2}:\d{2}:\d{2}))?", re.I)
SYMBOL = re.compile(r"(?:^|\s)([A-Z][A-Z0-9]{1,11})(?=\s*[:：])")


def _number(value: str) -> Decimal | None:
    raw = value.strip().replace(" ", "")
    if not raw:
        return None
    # Vietnamese financial pages use dot as thousands separator and comma as decimal separator.
    if "," in raw:
        raw = raw.replace(".", "").replace(",", ".")
    elif raw.count(".") > 1 or ("." in raw and len(raw.rsplit(".", 1)[-1]) == 3):
        raw = raw.replace(".", "")
    try:
        return Decimal(raw)
    except InvalidOperation:
        return None


def _field_map(content: BeautifulSoup) -> dict[str, str]:
    fields: dict[str, str] = {}
    for row in content.select(".row"):
        label = row.select_one(".item-info:not(.item-info-main)")
        value = row.select_one(".item-info-main")
        if label and value:
            key = clean_text(label.get_text(" ", strip=True)) or ""
            fields[key.rstrip(":").casefold()] = clean_text(value.get_text(" ", strip=True)) or ""
    return fields


def _field(fields: dict[str, str], *prefixes: str) -> str | None:
    for key, value in fields.items():
        if any(key.startswith(prefix.casefold()) for prefix in prefixes) and value:
            return value
    return None


def _dates(text: str, label: str) -> date | None:
    pattern = re.compile(re.escape(label) + r"\s*:?\s*(\d{2}/\d{2}/\d{4})", re.I)
    match = pattern.search(text)
    return parse_local_date(match.group(1)) if match else None


def _action_type(text: str) -> str:
    value = text.casefold()
    if "cổ tức" in value and "bằng tiền" in value:
        return "cash_dividend"
    if "cổ tức" in value and ("cổ phiếu" in value or "bằng cổ phiếu" in value):
        return "stock_dividend"
    if any(k in value for k in ("quyền mua", "chào bán cho cổ đông", "phát hành thêm")):
        return "rights_issue"
    if any(k in value for k in ("trái phiếu", "thanh toán lãi", "trả lãi")):
        return "bond_interest"
    if any(k in value for k in ("đại hội đồng cổ đông", "lấy ý kiến cổ đông")):
        return "shareholder_meeting"
    if any(k in value for k in ("ngày đăng ký cuối cùng", "thực hiện quyền")):
        return "corporate_action"
    return "other"


def _cash_per_share(text: str) -> Decimal | None:
    patterns = (
        r"(?:được nhận|nhận)\s*([\d.,]+)\s*đồng",
        r"mỗi cổ phiếu[^\d]{0,30}([\d.,]+)\s*đồng",
    )
    for pattern in patterns:
        match = re.search(pattern, text, re.I)
        if match:
            return _number(match.group(1))
    return None


def _ratio(text: str) -> tuple[Decimal | None, Decimal | None]:
    for pattern in (
        r"1\s*cổ phiếu\s*(?:được\s*)?nhận\s*([\d.,]+)\s*cổ phiếu",
        r"1\s*cổ phiếu\s*được\s*hưởng\s*([\d.,]+)\s*cổ phiếu",
    ):
        match = re.search(pattern, text, re.I)
        if match:
            return _number(match.group(1)), Decimal("1")
    match = re.search(r"tỷ lệ\s*(?:thực hiện)?\s*:?\s*([\d.,]+)\s*[:/]\s*([\d.,]+)", text, re.I)
    if match:
        return _number(match.group(1)), _number(match.group(2))
    return None, None


class VsdcCorporateActionClient(OfficialSourceClient):
    """Collects VSDC's issuer-rights notices and normalizes event dates/ratios."""

    def _event_links(self, html: str, page_url: str) -> list[tuple[str, str, Any]]:
        soup = BeautifulSoup(html, "lxml")
        output: list[tuple[str, str, Any]] = []
        seen: set[str] = set()
        for anchor in soup.select("#d_list_news a[href], .list-news a[href], a[href]"):
            href = anchor.get("href", "").strip()
            if not EVENT_HREF.fullmatch(href) or href in seen:
                continue
            title = clean_text(anchor.get_text(" ", strip=True))
            if not title:
                continue
            seen.add(href)
            parent = anchor
            for _ in range(5):
                if not parent.parent:
                    break
                parent = parent.parent
                if UPDATE_DATE.search(parent.get_text(" ", strip=True)):
                    break
            page_text = clean_text(parent.get_text(" ", strip=True)) or ""
            updated = UPDATE_DATE.search(page_text)
            pub_at = parse_local_datetime(
                f"{updated.group(1)} {updated.group(2) or '00:00:00'}" if updated else None
            )
            output.append((urljoin(page_url, href), title, pub_at))
        return output

    def _read_detail(self, url: str, listing_title: str, listing_time) -> DisclosureRecord:
        html, final_url = self.get_html(url)
        soup = BeautifulSoup(html, "lxml")
        title_node = soup.select_one(".container-small h3.title-category, h3.title-category, .title-category")
        title = clean_text(title_node.get_text(" ", strip=True)) if title_node else listing_title
        updated_node = soup.select_one(".time-newstcph")
        updated_match = UPDATE_DATE.search(updated_node.get_text(" ", strip=True)) if updated_node else None
        published_at = parse_local_datetime(
            f"{updated_match.group(1)} {updated_match.group(2) or '00:00:00'}" if updated_match else None
        ) or listing_time
        content = soup.select_one(".content-category")
        content = content or soup
        fields = _field_map(content)
        content_text = clean_text(content.get_text(" ", strip=True)) or ""
        symbol = _field(fields, "mã chứng khoán", "mã ck")
        if symbol:
            symbol = symbol.split()[0].upper()
        else:
            match = SYMBOL.search(title or "")
            symbol = match.group(1) if match else None
        company_name = _field(fields, "tên tổ chức đăng ký chứng khoán", "tên tổ chức phát hành")
        record_date = parse_local_date(_field(fields, "ngày đăng ký cuối cùng")) or _dates(content_text, "Ngày đăng ký cuối cùng")
        ex_date = parse_local_date(_field(fields, "ngày giao dịch không hưởng quyền")) or _dates(content_text, "Ngày giao dịch không hưởng quyền")
        payment_date = parse_local_date(_field(fields, "ngày thanh toán")) or _dates(content_text, "Ngày thanh toán")
        purpose = _field(fields, "lý do mục đích")
        action_text = " ".join(filter(None, [title, purpose, content_text]))
        action_type = _action_type(action_text)
        numerator, denominator = _ratio(content_text)
        id_match = re.search(r"/vi/ad1?/(\d+)", final_url)
        source_item_id = id_match.group(1) if id_match else final_url.rstrip("/").rsplit("/", 1)[-1]
        return DisclosureRecord(
            source_code=str(self.config["source_code"]),
            source_item_id=source_item_id[:200],
            title=(title or "VSDC corporate action")[:600],
            original_url=final_url,
            published_at=published_at,
            summary=content_text[:12000] or None,
            symbol=symbol,
            company_name=company_name,
            item_type="corporate_action" if symbol else "regulatory_notice",
            disclosure_kind="corporate_action",
            notice_kind=action_type,
            authority_name="Tổng công ty Lưu ký và Bù trừ chứng khoán Việt Nam (VSDC)",
            effective_date=record_date,
            ex_date=ex_date,
            record_date=record_date,
            payment_date=payment_date,
            cash_per_share_vnd=_cash_per_share(content_text),
            ratio_numerator=numerator,
            ratio_denominator=denominator,
            attachments=extract_attachments(content, final_url),
            metadata={
                "exchange": _field(fields, "nơi giao dịch", "sàn giao dịch"),
                "isin": _field(fields, "mã isin"),
                "security_type": _field(fields, "loại chứng khoán"),
                "purpose": purpose,
                "action_type": action_type,
                "page_fields": fields,
            },
        )

    def collect(self, window: DateWindow) -> list[DisclosureRecord]:
        listing_url = str(self.config.get("listing_url", f"{self.base_url}/vi/alc/94"))
        max_pages = int(self.config.get("max_pages", 1000))
        page_size = int(self.config.get("page_size", 15))
        target_symbols = {str(value).strip().upper() for value in self.config.get("target_symbols", [])}
        output: list[DisclosureRecord] = []
        with HeadlessOfficialBrowser(user_agent=self.client.headers.get("User-Agent")) as browser:
            first_html, page_url, status, duration = browser.goto(listing_url)
            self.fetch_attempts.append(HeadlessOfficialBrowser.attempt(page_url, first_html, status=status, duration_ms=duration))
            first_soup = BeautifulSoup(first_html, "lxml")
            total_text = first_soup.select_one("#d_total_rec")
            total_count_match = re.search(r"/\s*([\d.,]+)\s*bản ghi", total_text.get_text(" ", strip=True)) if total_text else None
            total_count = _number(total_count_match.group(1)) if total_count_match else None
            total_pages = min(max_pages, int((total_count or 0) / page_size) + 1) if total_count else max_pages
            stop = False
            for page_number in range(1, total_pages + 1):
                if page_number == 1:
                    html = first_html
                    current_url = page_url
                else:
                    html, current_url, duration = browser.change_page(page_number)
                    self.fetch_attempts.append(HeadlessOfficialBrowser.attempt(current_url, html, status=200, duration_ms=duration))
                links = self._event_links(html, current_url)
                if not links:
                    if page_number == 1:
                        raise RuntimeError(f"VSDC rights-events list is not available at {listing_url}")
                    break
                for article_url, title, published_at in links:
                    if published_at and published_at.date() < window.start_date:
                        stop = True
                        continue
                    if not published_at or not window.contains(published_at):
                        continue
                    symbol_hint = SYMBOL.search(title or "")
                    if target_symbols and symbol_hint and symbol_hint.group(1).upper() not in target_symbols:
                        continue
                    try:
                        record = self._read_detail(article_url, title, published_at)
                    except Exception as exc:
                        # A list result without its detail cannot populate company/event columns safely.
                        self.fetch_attempts.append({
                            "resource_kind": "detail", "url": article_url,
                            "requested_at": None, "completed_at": None,
                            "outcome": "FAILED", "error_type": type(exc).__name__,
                            "error_message": str(exc)[:500],
                        })
                        continue
                    if target_symbols and (not record.symbol or record.symbol.upper() not in target_symbols):
                        continue
                    if record.published_at and window.contains(record.published_at):
                        output.append(record)
                if stop:
                    break
                if len(links) < page_size:
                    break
            else:
                raise RuntimeError(f"VSDC page limit reached ({max_pages}); requested window may be incomplete")
        return output
