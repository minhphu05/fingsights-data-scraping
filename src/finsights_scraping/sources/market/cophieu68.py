from __future__ import annotations

from datetime import date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
import hashlib
import re
import time as time_module
import unicodedata
from typing import Any
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup

from ...domain.financials import FinancialPeriodRecord, FinancialValueRecord
from ...domain.models import MarketRecord
from ...domain.windows import DateWindow


class CoPhieu68ParseError(RuntimeError):
    pass


class CoPhieu68Client:
    """Public HTML adapter for CoPhieu68 price history and financial statements."""

    BASE_URL = "https://cophieu68.vn/"
    HISTORY_URL = BASE_URL + "quote/history.php"
    FINANCIAL_URL = BASE_URL + "quote/financial.php"
    FINANCIAL_DETAIL_URL = BASE_URL + "quote/financial_detail.php"
    TIMEZONE = ZoneInfo("Asia/Ho_Chi_Minh")

    def __init__(self, config: dict[str, Any]):
        self.config = config
        self.min_interval_seconds = float(config.get("min_interval_seconds", config.get("request_interval_seconds", 0.5)))
        self.max_history_pages = int(config.get("max_history_pages", 250))
        self.max_attempts = int(config.get("max_attempts", config.get("retry_attempts", 3)))
        self.fetch_attempts: list[dict[str, Any]] = []
        self.symbol_errors: dict[str, str] = {}
        self._last_request_at = 0.0
        self._client: httpx.Client | None = None

    def collect(
        self, window: DateWindow, securities: list[dict[str, Any]]
    ) -> tuple[list[MarketRecord], list[FinancialPeriodRecord]]:
        if not securities:
            raise ValueError("The Node 2 market database returned no active securities")

        market_records: list[MarketRecord] = []
        period_records: dict[tuple[str, str], FinancialPeriodRecord] = {}
        headers = {
            "User-Agent": "FinsightsDataCollector/1.0",
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "vi,en;q=0.8",
        }
        with httpx.Client(timeout=httpx.Timeout(35.0, connect=12.0), follow_redirects=True, headers=headers) as client:
            self._client = client
            for security in securities:
                symbol = str(security["symbol"]).strip().upper()
                try:
                    market_records.extend(self._collect_history(window, security))
                    self._collect_financials(symbol, period_records)
                except Exception as exc:
                    self.symbol_errors[symbol] = f"{type(exc).__name__}: {str(exc)[:1200]}"
            self._client = None

        if not market_records and not period_records:
            raise RuntimeError("CoPhieu68 returned no valid market or financial records")
        return market_records, [period for period in period_records.values() if period.values]

    def _request(self, url: str, *, params: dict[str, Any], resource_kind: str, page_number: int | None = None) -> httpx.Response:
        if self._client is None:
            raise RuntimeError("CoPhieu68 HTTP client is not initialized")
        last_error: Exception | None = None
        for attempt_number in range(1, self.max_attempts + 1):
            elapsed = time_module.monotonic() - self._last_request_at
            if self._last_request_at and elapsed < self.min_interval_seconds:
                time_module.sleep(self.min_interval_seconds - elapsed)
            requested_at = datetime.now(self.TIMEZONE).replace(tzinfo=None)
            started = time_module.monotonic()
            response: httpx.Response | None = None
            try:
                response = self._client.get(url, params=params)
                response.raise_for_status()
                completed_at = datetime.now(self.TIMEZONE).replace(tzinfo=None)
                self.fetch_attempts.append({
                    "url": str(response.url), "resource_kind": resource_kind, "page_number": page_number,
                    "attempt_number": attempt_number, "requested_at": requested_at, "completed_at": completed_at,
                    "http_status": response.status_code, "content_type": response.headers.get("content-type"),
                    "size_bytes": len(response.content),
                    "response_sha256": hashlib.sha256(response.content).hexdigest(),
                    "duration_ms": round((time_module.monotonic() - started) * 1000), "outcome": "SUCCESS",
                })
                self._last_request_at = time_module.monotonic()
                return response
            except Exception as exc:
                last_error = exc
                completed_at = datetime.now(self.TIMEZONE).replace(tzinfo=None)
                self.fetch_attempts.append({
                    "url": str(response.url) if response is not None else url,
                    "resource_kind": resource_kind, "page_number": page_number,
                    "attempt_number": attempt_number, "requested_at": requested_at, "completed_at": completed_at,
                    "http_status": response.status_code if response is not None else getattr(getattr(exc, "response", None), "status_code", None),
                    "content_type": response.headers.get("content-type") if response is not None else None,
                    "size_bytes": len(response.content) if response is not None else None,
                    "duration_ms": round((time_module.monotonic() - started) * 1000), "outcome": "FAILED",
                    "error_type": type(exc).__name__, "error_message": str(exc)[:2000],
                })
                self._last_request_at = time_module.monotonic()
                if attempt_number < self.max_attempts:
                    time_module.sleep(min(2 ** (attempt_number - 1), 4))
        assert last_error is not None
        raise last_error

    def _collect_history(self, window: DateWindow, security: dict[str, Any]) -> list[MarketRecord]:
        symbol = str(security["symbol"]).strip().upper()
        records: list[MarketRecord] = []
        seen_dates: set[date] = set()
        crossed_start = False
        for page in range(1, self.max_history_pages + 1):
            params = {"id": symbol.lower()}
            if page > 1:
                params["cP"] = page
            response = self._request(
                self.HISTORY_URL, params=params, resource_kind="price_history", page_number=page,
            )
            soup = BeautifulSoup(response.text, "lxml")
            table = soup.find("table", id="history")
            if table is None:
                raise CoPhieu68ParseError(f"{symbol} page {page}: missing table#history")
            data_rows: list[tuple[date, list[str]]] = []
            for row in table.select("tr"):
                cells = [cell.get_text(" ", strip=True) for cell in row.select("th,td")]
                if len(cells) < 8:
                    continue
                try:
                    trade_date = datetime.strptime(cells[0].strip(), "%d/%m/%Y").date()
                except ValueError:
                    continue
                data_rows.append((trade_date, cells))
            if not data_rows:
                raise CoPhieu68ParseError(f"{symbol} page {page}: no dated rows in table#history")

            page_dates = [trade_date for trade_date, _ in data_rows]
            for trade_date, cells in data_rows:
                if trade_date in seen_dates or not (window.start_date <= trade_date <= window.end_date):
                    continue
                close = _parse_decimal(cells[1])
                volume = _parse_integer(cells[2])
                opened = _parse_decimal(cells[3])
                high = _parse_decimal(cells[4])
                low = _parse_decimal(cells[5])
                foreign_buy = _parse_integer(cells[6])
                foreign_sell = _parse_integer(cells[7])
                if close is None or volume is None:
                    continue
                source_url = str(response.url)
                fields = {
                    "exchange_code": str(security.get("exchange_code") or "HOSE").upper(),
                    "security_name": security.get("name"),
                    # CoPhieu68 quotes are displayed in thousands of VND; the Node 2 schema stores VND.
                    "last_price_vnd": close * Decimal(1000),
                    "open_price_vnd": opened * Decimal(1000) if opened is not None else None,
                    "high_price_vnd": high * Decimal(1000) if high is not None else None,
                    "low_price_vnd": low * Decimal(1000) if low is not None else None,
                    "matched_volume": volume,
                    "foreign_buy_volume": foreign_buy,
                    "foreign_sell_volume": foreign_sell,
                    "is_final": True,
                }
                observed_at = datetime.combine(trade_date, time(15, 0), self.TIMEZONE)
                records.append(MarketRecord(
                    kind="quote", provider_code="cophieu68", trade_date=trade_date,
                    symbol_or_index=symbol, observed_at=observed_at, fields=fields,
                    raw_payload={
                        "source_url": source_url,
                        "source_headers": ["date", "close", "volume", "open", "high", "low", "foreign_buy", "foreign_sell", "foreign_value_billion_vnd"],
                        "source_row": cells,
                        "displayed_price_unit": "thousand VND",
                    },
                ))
                seen_dates.add(trade_date)
            if min(page_dates) < window.start_date:
                crossed_start = True
                break
            if len(data_rows) < 2:
                break
        else:
            raise RuntimeError(f"{symbol}: reached max_history_pages={self.max_history_pages} before the requested start date")

        if not crossed_start and records and min(record.trade_date for record in records) > window.start_date:
            # Some symbols may have had no trading sessions exactly on start_date;
            # exhaustion of their history still has to be distinguished from a truncated scan.
            if len(records) >= self.max_history_pages * 50:
                raise RuntimeError(f"{symbol}: price history may be truncated before {window.start_date}")
        if not records:
            if window.start_date == window.end_date:
                return []
            raise CoPhieu68ParseError(f"{symbol}: no price records found in {window.start_date}..{window.end_date}")
        return records

    def _collect_financials(
        self, symbol: str, periods: dict[tuple[str, str], FinancialPeriodRecord]
    ) -> None:
        for period_type in ("quarter", "year"):
            response = self._request(
                self.FINANCIAL_DETAIL_URL,
                params={"id": symbol.lower(), "type": period_type},
                resource_kind=f"financial_{period_type}",
            )
            soup = BeautifulSoup(response.text, "lxml")
            url = str(response.url)
            table_ids = {
                "table_content_income": "income_statement",
                "table_content_balance": "balance_sheet",
            }
            found = 0
            for table_id, family in table_ids.items():
                table = soup.find("table", id=table_id)
                if table is None:
                    continue
                found += 1
                self._read_financial_table(table, symbol, family, url, periods)
            if found < 2:
                raise CoPhieu68ParseError(f"{symbol} {period_type}: income/balance statement tables were not both present")

        response = self._request(
            self.FINANCIAL_URL, params={"id": symbol.lower()}, resource_kind="financial_ratios",
        )
        soup = BeautifulSoup(response.text, "lxml")
        table = soup.find("table", id="financial_indexes")
        if table is None:
            raise CoPhieu68ParseError(f"{symbol}: missing table#financial_indexes")
        self._read_financial_table(table, symbol, "financial_ratio", str(response.url), periods)

        if not any(key[0] == symbol and period.values for key, period in periods.items()):
            raise CoPhieu68ParseError(f"{symbol}: financial pages contained no parsed period values")

    def _read_financial_table(
        self,
        table,
        symbol: str,
        family: str,
        source_url: str,
        periods: dict[tuple[str, str], FinancialPeriodRecord],
    ) -> None:
        rows = table.select("tr")
        if not rows:
            return
        headers = [cell.get_text(" ", strip=True) for cell in rows[0].select("th,td")]
        period_columns: dict[int, tuple[int, int | None, str, date, date]] = {}
        for index, header in enumerate(headers[1:], start=1):
            parsed = _parse_period(header)
            if parsed:
                fiscal_year, fiscal_quarter, period_type, period_code, period_start, period_end = parsed
                period_columns[index] = (fiscal_year, fiscal_quarter, period_type, period_start, period_end)
                periods.setdefault((symbol, period_code), FinancialPeriodRecord(
                    symbol=symbol, period_code=period_code, fiscal_year=fiscal_year,
                    fiscal_quarter=fiscal_quarter, period_type=period_type,
                    period_start=period_start, period_end=period_end, source_url=source_url,
                ))

        for row_order, row in enumerate(rows[1:], start=1):
            cells = row.select("th,td")
            if len(cells) < 2:
                continue
            label = " ".join(cells[0].get_text(" ", strip=True).split())
            if not label or _normalize_text(label) in {"chi tieu", "chi so tai chinh"}:
                continue
            for column_index, period_info in period_columns.items():
                if column_index >= len(cells):
                    continue
                raw_value = " ".join(cells[column_index].get_text(" ", strip=True).split())
                parsed_value = _parse_financial_number(raw_value)
                if parsed_value is None:
                    continue
                value, original, reported_unit, multiplier, unit_code = parsed_value
                fiscal_year, fiscal_quarter, period_type, _period_start, _period_end = period_info
                period_code = f"{fiscal_year}Q{fiscal_quarter}" if fiscal_quarter else f"{fiscal_year}Y"
                record = periods[(symbol, period_code)]
                normalized_label = _slug(label) or f"row_{row_order}"
                line_key = f"{family}:{normalized_label}"[:120]
                existing_keys = {value.line_key for value in record.values}
                if line_key in existing_keys:
                    line_key = f"{family}:{normalized_label}:{row_order}"[:120]
                    suffix = 2
                    while line_key in existing_keys:
                        line_key = f"{family}:{normalized_label}:{row_order}_{suffix}"[:120]
                        suffix += 1
                record.values.append(FinancialValueRecord(
                    statement_family=family,
                    source_label=label[:320],
                    line_key=line_key,
                    value_decimal=value,
                    original_value_decimal=original,
                    reported_unit=reported_unit,
                    unit_multiplier=multiplier,
                    unit_code=unit_code,
                    value_kind="source_reported",
                    source_currency=(
                        "VND" if family != "financial_ratio" or reported_unit in {"K", "M", "B"} else None
                    ),
                    row_order=row_order,
                    raw_fields={
                        "source_url": source_url,
                        "period_code": period_code,
                        "period_type": period_type,
                        "raw_value": raw_value,
                        "unit_not_stated_for_statement_tables": family != "financial_ratio",
                    },
                ))


def _normalize_text(value: str) -> str:
    value = unicodedata.normalize("NFKD", value.strip().casefold())
    return "".join(ch for ch in value if not unicodedata.combining(ch))


def _slug(value: str) -> str:
    ascii_value = _normalize_text(value)
    return re.sub(r"[^a-z0-9]+", "_", ascii_value).strip("_")


def _parse_period(value: str) -> tuple[int, int | None, str, str, date, date] | None:
    text = re.sub(r"\s+", " ", _normalize_text(value))
    quarter = re.search(r"(?:quy|q)\s*([1-4])\s*[-/]?\s*(20\d{2})", text)
    if quarter:
        q, year = int(quarter.group(1)), int(quarter.group(2))
        month = q * 3
        start = date(year, month - 2, 1)
        if month == 12:
            end = date(year, 12, 31)
        else:
            end = date(year, month + 1, 1) - timedelta(days=1)
        return year, q, "quarter", f"{year}Q{q}", start, end
    annual = re.search(r"(?:nam|year)?\s*(20\d{2})\s*$", text)
    if annual:
        year = int(annual.group(1))
        return year, None, "year", f"{year}Y", date(year, 1, 1), date(year, 12, 31)
    return None


def _parse_decimal(value: str) -> Decimal | None:
    cleaned = value.strip().replace(",", "").replace(" ", "")
    if not cleaned or cleaned in {"-", "—", "–", "N/A", "n/a"}:
        return None
    try:
        return Decimal(cleaned)
    except InvalidOperation:
        return None


def _parse_integer(value: str) -> int | None:
    number = _parse_decimal(value)
    return int(number) if number is not None else None


def _parse_financial_number(value: str) -> tuple[Decimal, Decimal, str, Decimal, str] | None:
    raw = value.strip().replace("\xa0", " ")
    if not raw or raw in {"-", "—", "–", "N/A", "n/a"}:
        return None
    negative = raw.startswith("(") and raw.endswith(")")
    if negative:
        raw = raw[1:-1].strip()
    raw = re.sub(r"\s+", " ", raw)
    suffix = ""
    unit_code = "source_reported"
    if raw.endswith("%"):
        suffix = "%"
        raw = raw[:-1].strip()
        unit_code = "percent"
    else:
        unit = re.search(r"\((?:l\u1ea7n|lan)\)\s*$", _normalize_text(raw))
        if unit:
            raw = raw[:unit.start()].strip()
            suffix = "lần"
            unit_code = "multiple"
        else:
            scale = re.search(r"\s*([KMB])\s*$", raw, flags=re.IGNORECASE)
            if scale:
                suffix = scale.group(1).upper()
                raw = raw[:scale.start()].strip()
                unit_code = "source_reported"
    raw = raw.replace(",", "").replace(" ", "")
    try:
        original = Decimal(raw)
    except InvalidOperation:
        return None
    if negative:
        original = -original
    multiplier = {"K": Decimal(1000), "M": Decimal(1000000), "B": Decimal(1000000000)}.get(suffix, Decimal(1))
    return original * multiplier, original, suffix or "not_stated", multiplier, unit_code
