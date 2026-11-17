from __future__ import annotations

import hashlib
import json
import re
import ssl
import time
from datetime import date, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup

from ...domain.disclosures import DisclosureRecord
from ...domain.windows import DateWindow

LOCAL_TZ = ZoneInfo("Asia/Ho_Chi_Minh")


def clean_text(value: str | None) -> str | None:
    if not value:
        return None
    text = BeautifulSoup(value, "lxml").get_text(" ", strip=True)
    return re.sub(r"\s+", " ", text).strip() or None


def parse_local_datetime(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=LOCAL_TZ) if value.tzinfo is None else value
    raw_numeric = str(value).strip()
    if isinstance(value, (int, float)) or (raw_numeric.isdigit() and len(raw_numeric) >= 10):
        stamp = float(value)
        if stamp > 10_000_000_000:
            stamp /= 1000
        try:
            return datetime.fromtimestamp(stamp, tz=LOCAL_TZ)
        except (OverflowError, OSError, ValueError):
            return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return parsed.replace(tzinfo=LOCAL_TZ) if parsed.tzinfo is None else parsed
    except ValueError:
        pass
    for fmt in (
        "%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M", "%d/%m/%Y",
        "%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d-%m-%Y %H:%M:%S", "%d-%m-%Y",
    ):
        try:
            return datetime.strptime(raw, fmt).replace(tzinfo=LOCAL_TZ)
        except ValueError:
            continue
    try:
        parsed = parsedate_to_datetime(raw)
        return parsed.replace(tzinfo=LOCAL_TZ) if parsed.tzinfo is None else parsed
    except (TypeError, ValueError, OverflowError):
        return None


def parse_local_date(value: Any) -> date | None:
    dt = parse_local_datetime(value)
    return dt.astimezone(LOCAL_TZ).date() if dt else None


def stable_id(source_code: str, url: str, title: str, published_at: datetime | None) -> str:
    raw = f"{source_code}|{url}|{title}|{published_at.isoformat() if published_at else ''}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:40]


def extract_attachments(root: BeautifulSoup, base_url: str) -> list[dict[str, str]]:
    results: list[dict[str, str]] = []
    seen: set[str] = set()
    suffixes = (".pdf", ".xls", ".xlsx", ".doc", ".docx", ".zip", ".rar")
    for anchor in root.select("a[href]"):
        href = urljoin(base_url, anchor.get("href", "").strip())
        label = anchor.get_text(" ", strip=True)
        lower = href.lower().split("?", 1)[0]
        if not href.startswith(("http://", "https://")) or href in seen:
            continue
        if not (lower.endswith(suffixes) or any(k in lower for k in ("download", "attachment", "filedownload", "contentattachfile"))):
            continue
        seen.add(href)
        results.append({"url": href, "label": label[:300]})
    return results


def infer_disclosure_kind(text: str) -> str:
    value = text.casefold()
    rules = (
        (("báo cáo tài chính", "bctc", "báo cáo thường niên"), "financial_report"),
        (("giải trình", "giải thích"), "explanation"),
        (("nghị quyết", "đại hội đồng cổ đông", "đhđcđ"), "shareholder_resolution"),
        (("giao dịch cổ phiếu", "người nội bộ", "cổ đông lớn"), "insider_transaction"),
        (("thay đổi đăng ký niêm yết", "niêm yết", "upcom"), "listing"),
        (("cổ tức", "ngày đăng ký cuối cùng", "ngày đkcc", "thực hiện quyền"), "corporate_action"),
        (("công bố thông tin", "thông tin công bố"), "general_disclosure"),
    )
    for phrases, kind in rules:
        if any(phrase in value for phrase in phrases):
            return kind
    return "other"


def in_window(value: datetime | None, window: DateWindow) -> bool:
    return value is not None and window.contains(value)


class OfficialSourceClient:
    """Small HTTP client with TLS verification, throttling, and auditable fetch metadata."""

    def __init__(self, config: dict[str, Any]):
        self.config = config
        self.base_url = str(config["base_url"]).rstrip("/")
        self.rate = float(config.get("request_rate_per_second", 2.0))
        self._last_request = 0.0
        self.fetch_attempts: list[dict[str, Any]] = []
        extra_ca_file = config.get("tls_extra_ca_file")
        verify: bool | ssl.SSLContext = True
        if extra_ca_file:
            ca_path = Path(str(extra_ca_file))
            if not ca_path.is_absolute():
                project_root = Path(__file__).resolve().parents[4]
                ca_path = (project_root / ca_path).resolve()
            if not ca_path.is_file():
                raise FileNotFoundError(f"Configured TLS CA certificate does not exist: {ca_path}")
            # Some official portals omit an intermediate CA from their TLS handshake.
            # Add the verified issuer to the normal platform trust store; hostname and
            # certificate verification remain enabled.
            verify = ssl.create_default_context()
            verify.load_verify_locations(cafile=str(ca_path))
        self.client = httpx.Client(
            timeout=httpx.Timeout(float(config.get("timeout_seconds", 30))),
            follow_redirects=True,
            verify=verify,
            headers={
                "User-Agent": config.get(
                    "user_agent",
                    "FinsightsDataCollector/1.0 (+https://finsights.vn; public market information)",
                ),
                "Accept": "text/html,application/json,application/xhtml+xml,*/*;q=0.8",
                "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.6",
            },
        )

    def close(self) -> None:
        self.client.close()

    def _throttle(self) -> None:
        if self.rate <= 0:
            return
        interval = 1 / self.rate
        wait = interval - (time.monotonic() - self._last_request)
        if wait > 0:
            time.sleep(wait)

    def request(self, method: str, url: str, *, params=None, json_body=None, form_data=None, headers=None) -> httpx.Response:
        self._throttle()
        started = datetime.now().astimezone()
        before = time.monotonic()
        attempt: dict[str, Any] = {
            "resource_kind": "page",
            "url": str(httpx.URL(url, params=params) if params else url),
            "requested_at": started,
            "attempt_number": 1,
        }
        try:
            response = self.client.request(
                method, url, params=params, json=json_body, data=form_data, headers=headers
            )
            self._last_request = time.monotonic()
            attempt.update({
                "completed_at": datetime.now().astimezone(),
                "http_status": response.status_code,
                "content_type": response.headers.get("content-type"),
                "size_bytes": len(response.content),
                "response_sha256": hashlib.sha256(response.content).hexdigest(),
                "duration_ms": round((time.monotonic() - before) * 1000),
                "outcome": "SUCCESS" if response.is_success else "HTTP_ERROR",
            })
            self.fetch_attempts.append(attempt)
            response.raise_for_status()
            return response
        except Exception as exc:
            self._last_request = time.monotonic()
            attempt.update({
                "completed_at": datetime.now().astimezone(),
                "duration_ms": round((time.monotonic() - before) * 1000),
                "outcome": "FAILED",
                "error_type": type(exc).__name__,
                "error_message": str(exc)[:1000],
            })
            if not self.fetch_attempts or self.fetch_attempts[-1] is not attempt:
                self.fetch_attempts.append(attempt)
            raise

    def get_json(self, url: str, *, params=None) -> dict[str, Any]:
        response = self.request("GET", url, params=params)
        data = response.json()
        if not isinstance(data, dict):
            raise ValueError(f"Expected a JSON object from {url}")
        return data

    def get_html(self, url: str, *, params=None) -> tuple[str, str]:
        response = self.request("GET", url, params=params)
        return response.text, str(response.url)

    def post_html(self, url: str, *, json_body=None, form_data=None, headers=None) -> tuple[str, str]:
        response = self.request("POST", url, json_body=json_body, form_data=form_data, headers=headers)
        return response.text, str(response.url)

    def collect(self, window: DateWindow) -> list[DisclosureRecord]:
        raise NotImplementedError


def record_from_html_row(
    *, source_code: str, authority_name: str, row: Any, base_url: str,
    title: str, published_at: datetime | None, source_item_id: str | None = None,
    symbol: str | None = None, canonical_url: str | None = None,
    summary: str | None = None, metadata: dict[str, Any] | None = None,
) -> DisclosureRecord:
    soup = row if isinstance(row, BeautifulSoup) else BeautifulSoup(str(row), "lxml")
    link = soup.select_one("a[href]")
    url = canonical_url or (urljoin(base_url, link.get("href")) if link else base_url)
    clean_title = clean_text(title) or "Untitled official disclosure"
    item_id = str(source_item_id or stable_id(source_code, url, clean_title, published_at))
    text = clean_text(summary) or clean_text(soup.get_text(" ", strip=True))
    return DisclosureRecord(
        source_code=source_code,
        source_item_id=item_id[:200],
        title=clean_title[:600],
        original_url=url,
        published_at=published_at,
        summary=text,
        symbol=symbol.upper() if symbol else None,
        item_type="issuer_disclosure" if symbol else "regulatory_notice",
        disclosure_kind=infer_disclosure_kind(f"{clean_title} {text or ''}"),
        notice_kind=infer_disclosure_kind(clean_title),
        authority_name=authority_name,
        attachments=extract_attachments(soup, base_url),
        metadata=metadata or {},
    )


def html_tables_as_records(
    html: str, *, source_code: str, authority_name: str, base_url: str,
    window: DateWindow | None, page_number: int, include_outside_window: bool = False,
) -> list[DisclosureRecord]:
    soup = BeautifulSoup(html, "lxml")
    output: list[DisclosureRecord] = []
    for table in soup.select("table"):
        header_cells = [clean_text(cell.get_text(" ", strip=True)) or "" for cell in table.select("thead th, thead td")]
        if not header_cells:
            first_row = table.select_one("tr")
            if first_row:
                header_cells = [clean_text(cell.get_text(" ", strip=True)) or "" for cell in first_row.select("th,td")]
        header_text = " ".join(header_cells).casefold()
        if not any(term in header_text for term in ("ngày đăng", "ngày công bố", "thời gian", "ngày tin")):
            continue
        if not any(term in header_text for term in ("tiêu đề", "nội dung", "thông tin")):
            continue
        rows = table.select("tbody tr") or table.select("tr")[1:]
        for row in rows:
            cells = row.select("td")
            if len(cells) < 2:
                continue
            texts = [clean_text(cell.get_text(" ", strip=True)) or "" for cell in cells]
            title_index = next((
                index for index, header in enumerate(header_cells)
                if any(term in header.casefold() for term in ("tiêu đề", "nội dung", "thông tin"))
            ), None)
            title_cell = (
                cells[title_index]
                if title_index is not None and title_index < len(cells)
                else next((cell for cell in cells if cell.select_one("a[href]")), cells[-1])
            )
            title = clean_text(title_cell.get_text(" ", strip=True))
            if not title:
                continue
            link = title_cell.select_one("a[href]")
            canonical_url = urljoin(base_url, link.get("href")) if link else None
            published_at = next((parse_local_datetime(text) for text in texts if parse_local_datetime(text)), None)
            if window is not None and not include_outside_window and not in_window(published_at, window):
                continue
            symbol = None
            for text in texts:
                match = re.fullmatch(r"[A-Z0-9]{3,12}", text.strip())
                if match:
                    symbol = match.group(0)
                    break
            output.append(record_from_html_row(
                source_code=source_code,
                authority_name=authority_name,
                row=row,
                base_url=base_url,
                title=title,
                published_at=published_at,
                source_item_id=None,
                symbol=symbol,
                canonical_url=canonical_url,
                metadata={"page": page_number, "table_headers": header_cells, "row_values": texts},
            ))
    return output


def deduplicate_records(items: list[DisclosureRecord]) -> list[DisclosureRecord]:
    result: dict[tuple[str, str], DisclosureRecord] = {}
    for item in items:
        result[(item.source_code, item.source_item_id)] = item
    return list(result.values())


def encode_metadata(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)
