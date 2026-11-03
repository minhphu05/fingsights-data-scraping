from __future__ import annotations

import os
from urllib.parse import urljoin, urlparse
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from ...domain.models import MarketRecord
from ...domain.windows import DateWindow


class ProviderContractError(RuntimeError):
    pass


def _json_path(value: Any, path: str) -> Any:
    current = value
    for token in path.split(".") if path else []:
        if isinstance(current, list):
            current = current[int(token)]
        else:
            current = current[token]
    return current


def _field_value(row: dict[str, Any], path: str) -> Any:
    return _json_path(row, path)


class VietstockHistoryClient:
    """Client for a documented/licensed DataFeed endpoint; never invents history."""

    def __init__(self, config: dict[str, Any], mapping: dict[str, Any]):
        self.config = config
        self.mapping = mapping
        self.timezone_name = str(config.get("timezone", "Asia/Ho_Chi_Minh"))
        self.fetch_attempts: list[dict[str, Any]] = []

    async def collect(self, window: DateWindow) -> list[MarketRecord]:
        provider = self.config.get("providers", {}).get("licensed_history", {})
        if not provider.get("enabled", False):
            raise ProviderContractError("Vietstock licensed history is disabled in configs/sources/market/vietstock.yaml")
        endpoint_base = os.getenv(provider.get("endpoint_env", "VIETSTOCK_HISTORY_ENDPOINT"), "").strip()
        token = os.getenv(provider.get("token_env", "VIETSTOCK_API_TOKEN"), "").strip()
        request = self.mapping.get("request", {})
        response_mapping = self.mapping.get("response", {})
        request_endpoint = str(request.get("endpoint") or "").strip()
        endpoint = request_endpoint if urlparse(request_endpoint).scheme in {"http", "https"} else (
            urljoin(endpoint_base.rstrip("/") + "/", request_endpoint.lstrip("/")) if endpoint_base and request_endpoint else ""
        )
        if not endpoint or urlparse(endpoint).scheme != "https" or not token:
            raise ProviderContractError(
                "Vietstock history requires an HTTPS licensed DataFeed endpoint and credentials, "
                "and exact request/response mapping are supplied. The public board is current-state "
                "only and cannot satisfy arbitrary start_date/end_date backfills."
            )
        has_auth_placement = bool(request.get("api_key_header") or request.get("api_key_parameter"))
        if not request.get("start_date_parameter") or not request.get("end_date_parameter") or not has_auth_placement:
            raise ProviderContractError("Vietstock request mapping is incomplete")
        if not response_mapping.get("records_json_path") or not response_mapping.get("record_trade_date_field"):
            raise ProviderContractError("Vietstock response mapping must identify records and each record's trade date")

        params = {
            request["start_date_parameter"]: window.start_date.isoformat(),
            request["end_date_parameter"]: window.end_date.isoformat(),
        }
        headers = {request["api_key_header"]: token} if request.get("api_key_header") else {}
        if request.get("api_key_parameter"):
            params[request["api_key_parameter"]] = token
        requested_at = datetime.now(ZoneInfo(self.timezone_name)).replace(tzinfo=None)
        async with httpx.AsyncClient(timeout=45.0, follow_redirects=True) as client:
            try:
                response = await client.get(endpoint, params=params, headers=headers)
                response.raise_for_status()
                payload = response.json()
                self.fetch_attempts.append({"url": str(response.url), "resource_kind": "history_api",
                    "requested_at": requested_at, "completed_at": datetime.now(ZoneInfo(self.timezone_name)).replace(tzinfo=None),
                    "http_status": response.status_code, "content_type": response.headers.get("content-type"),
                    "size_bytes": len(response.content), "outcome": "SUCCESS"})
            except Exception as exc:
                self.fetch_attempts.append({"url": endpoint, "resource_kind": "history_api",
                    "requested_at": requested_at, "completed_at": datetime.now(ZoneInfo(self.timezone_name)).replace(tzinfo=None),
                    "http_status": getattr(getattr(exc, "response", None), "status_code", None),
                    "outcome": "FAILED", "error_type": type(exc).__name__, "error_message": str(exc)[:2000]})
                raise

        try:
            rows = _json_path(payload, response_mapping["records_json_path"])
        except (KeyError, IndexError, TypeError, ValueError) as exc:
            raise ProviderContractError("Vietstock response did not match configured JSON path") from exc
        if not isinstance(rows, list):
            raise ProviderContractError("Configured Vietstock records path must resolve to a list")

        field_map = response_mapping.get("stock_quote_fields", {})
        index_map = response_mapping.get("market_index_fields", {})
        coverage_mapping = self.mapping.get("coverage", {})
        index_aliases = {
            str(key).strip().upper(): str(value).strip().upper()
            for key, value in coverage_mapping.get("market_index_aliases", {}).items()
        }
        exchange_aliases = {
            str(key).strip().upper(): str(value).strip().upper()
            for key, value in coverage_mapping.get("exchange_aliases", {}).items()
        }
        if field_map.get("symbol") and (not field_map.get("exchange_code") or not field_map.get("observed_at")):
            raise ProviderContractError("Quote mapping must include exchange_code and observed_at")
        if index_map.get("code") and not index_map.get("observed_at"):
            raise ProviderContractError("Index mapping must include observed_at")
        results: list[MarketRecord] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            trade_value = _field_value(row, response_mapping["record_trade_date_field"])
            trade_date = self._parse_date(trade_value)
            if trade_date is None:
                raise ProviderContractError("Vietstock record is missing a parseable provider trade date")
            if not (window.start_date <= trade_date <= window.end_date):
                continue
            for kind, mapping in (("quote", field_map), ("index", index_map)):
                identifier_field = mapping.get("symbol") if kind == "quote" else mapping.get("code")
                identifier = _field_value(row, identifier_field) if identifier_field else None
                if not identifier:
                    continue
                identifier = str(identifier).strip().upper()
                if kind == "index":
                    identifier = index_aliases.get(identifier, identifier)
                observed_field = mapping.get("observed_at")
                observed = self._parse_datetime(_field_value(row, observed_field), trade_date)
                if observed.date() != trade_date:
                    raise ProviderContractError("Vietstock observed_at does not belong to its declared trade_date")
                normalized = {_target: _field_value(row, source) for _target, source in mapping.items()
                              if _target not in {"symbol", "code", "observed_at"}}
                if kind == "quote" and normalized.get("exchange_code"):
                    provider_exchange = str(normalized["exchange_code"]).strip().upper()
                    normalized["exchange_code"] = exchange_aliases.get(provider_exchange, provider_exchange)
                results.append(MarketRecord(
                    kind=kind,
                    provider_code="vietstock_datafeed",
                    trade_date=trade_date,
                    symbol_or_index=identifier,
                    observed_at=observed,
                    fields=normalized,
                    raw_payload=row,
                ))
        if results and self.config.get("coverage", {}).get("validate_each_returned_trade_date", False):
            expected_exchanges = set(self.config["coverage"]["securities"]["exchanges"])
            expected_indices = set(self.config["coverage"]["indices"]["required_codes"])
            for trade_date in {record.trade_date for record in results}:
                dated_records = [record for record in results if record.trade_date == trade_date]
                actual_exchanges = {
                    str(record.fields.get("exchange_code", "")).strip().upper()
                    for record in dated_records if record.kind == "quote"
                }
                actual_indices = {
                    record.symbol_or_index for record in dated_records if record.kind == "index"
                }
                missing_exchanges = expected_exchanges - actual_exchanges
                missing_indices = expected_indices - actual_indices
                if missing_exchanges or missing_indices:
                    raise ProviderContractError(
                        f"Vietstock returned partial coverage for {trade_date}: "
                        f"missing quote exchanges={sorted(missing_exchanges)}, "
                        f"missing indices={sorted(missing_indices)}"
                    )
        return results

    @staticmethod
    def _parse_date(value: Any) -> date | None:
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, datetime):
            return value.date()
        if not value:
            return None
        text = str(value).strip()
        try:
            return date.fromisoformat(text[:10])
        except ValueError:
            for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y%m%d"):
                try:
                    return datetime.strptime(text, fmt).date()
                except ValueError:
                    continue
        return None

    def _parse_datetime(self, value: Any, trade_date: date) -> datetime:
        zone = ZoneInfo(self.timezone_name)
        if isinstance(value, datetime):
            parsed = value
        elif value:
            raw = str(value).strip()
            if ":" not in raw and "T" not in raw:
                raise ProviderContractError("Provider observation timestamp must include a time, not date alone")
            try:
                parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            except ValueError:
                parsed = None
                for fmt in ("%H:%M:%S", "%H:%M"):
                    try:
                        parsed = datetime.combine(trade_date, datetime.strptime(raw, fmt).time())
                        break
                    except ValueError:
                        continue
                if parsed is None:
                    raise ProviderContractError(f"Cannot parse provider observation time: {raw}")
        else:
            raise ProviderContractError("Provider must supply observed_at; do not substitute current time")
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=zone)
        return parsed.astimezone(zone)


class VietstockPublicBoardClient:
    """Scrapes market index snapshots directly from the public Vietstock board."""

    def __init__(self, config: dict[str, Any] | None = None):
        self.config = config or {}
        self.board_url = self.config.get("providers", {}).get("live_board", {}).get(
            "url", "https://banggia.vietstock.vn"
        )
        self.timezone_name = str(self.config.get("timezone", "Asia/Ho_Chi_Minh"))
        self.fetch_attempts: list[dict[str, Any]] = []

    async def collect(self, window: DateWindow) -> list[MarketRecord]:
        from bs4 import BeautifulSoup

        zone = ZoneInfo(self.timezone_name)
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.7",
        }
        requested_at = datetime.now(zone).replace(tzinfo=None)
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=headers) as client:
            try:
                response = await client.get(self.board_url)
                response.raise_for_status()
                self.fetch_attempts.append({
                    "url": self.board_url,
                    "resource_kind": "public_price_board",
                    "requested_at": requested_at,
                    "completed_at": datetime.now(zone).replace(tzinfo=None),
                    "http_status": response.status_code,
                    "content_type": response.headers.get("content-type"),
                    "size_bytes": len(response.content),
                    "outcome": "SUCCESS",
                })
            except Exception as exc:
                self.fetch_attempts.append({
                    "url": self.board_url,
                    "resource_kind": "public_price_board",
                    "requested_at": requested_at,
                    "completed_at": datetime.now(zone).replace(tzinfo=None),
                    "http_status": getattr(getattr(exc, "response", None), "status_code", None),
                    "outcome": "FAILED",
                    "error_type": type(exc).__name__,
                    "error_message": str(exc)[:2000],
                })
                raise

        soup = BeautifulSoup(response.content, "lxml")
        records: list[MarketRecord] = []
        observed_at = datetime.combine(window.end_date, datetime.min.time()).replace(
            hour=15, minute=0, second=0, tzinfo=zone
        )

        for i in range(1, 15):
            m_node = soup.select_one(f"#index-market-{i}")
            c_node = soup.select_one(f"#index-close-{i}")
            vol_node = soup.select_one(f"#index-totalVol-{i}")
            val_node = soup.select_one(f"#index-totalVal-{i}")
            if not m_node:
                continue

            name = m_node.text.strip()
            try:
                ref = float(m_node.get("data-value") or 0)
                close = float(c_node.get("data-value") or 0) if c_node else ref
                vol = int(float(vol_node.get("data-value") or 0)) if vol_node else 0
                val = float(val_node.get("data-value") or 0) if val_node else 0.0
            except (ValueError, TypeError):
                continue

            change = round(close - ref, 2)
            pct_change = round((change / ref) * 100, 2) if ref else 0.0

            fields = {
                "reference_price": ref,
                "close_price": close,
                "index_value": close,
                "price_change": change,
                "change_points": change,
                "pct_change": pct_change,
                "change_percent": pct_change,
                "total_volume": vol,
                "total_value_bil_vnd": val,
                "turnover_vnd": int(val * 1_000_000_000),
            }
            raw_payload = {
                "index_name": name,
                "ref_val": m_node.get("data-value"),
                "close_val": c_node.get("data-value") if c_node else None,
                "vol_val": vol_node.get("data-value") if vol_node else None,
                "val_val": val_node.get("data-value") if val_node else None,
                "source": "banggia.vietstock.vn",
            }
            records.append(MarketRecord(
                kind="index",
                provider_code="vietstock",
                trade_date=window.end_date,
                symbol_or_index=name,
                observed_at=observed_at,
                fields=fields,
                raw_payload=raw_payload,
            ))

        return records

