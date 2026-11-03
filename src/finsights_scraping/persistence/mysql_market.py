from __future__ import annotations

import json
import os
import uuid
from decimal import Decimal, InvalidOperation
from datetime import timezone
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.engine import Engine

from ..domain.models import MarketRecord
from .mysql_engine import create_tls_mysql_engine


QUOTE_COLUMNS = {
    "reference_price_vnd", "ceiling_price_vnd", "floor_price_vnd",
    "bid_price_1_vnd", "bid_volume_1", "bid_price_2_vnd", "bid_volume_2",
    "bid_price_3_vnd", "bid_volume_3", "last_price_vnd", "matched_volume",
    "change_vnd", "change_percent", "ask_price_1_vnd", "ask_volume_1",
    "ask_price_2_vnd", "ask_volume_2", "ask_price_3_vnd", "ask_volume_3",
    "total_volume", "high_price_vnd", "low_price_vnd", "average_price_vnd",
    "foreign_buy_volume", "foreign_sell_volume", "total_turnover_vnd", "market_state", "is_delayed",
}
INDEX_COLUMNS = {"index_value", "change_points", "change_percent", "total_volume", "turnover_vnd"}
NUMERIC_COLUMNS = QUOTE_COLUMNS - {"market_state", "is_delayed"}
DAILY_QUOTE_NUMERIC_FIELDS = {
    "open_price_vnd", "high_price_vnd", "low_price_vnd", "adjusted_close_vnd",
    "reference_price_vnd", "ceiling_price_vnd", "floor_price_vnd", "change_percent",
    "matched_volume", "total_turnover_vnd", "foreign_buy_volume", "foreign_sell_volume",
}
DAILY_INDEX_NUMERIC_FIELDS = {
    "open_points", "high_points", "low_points", "change_points", "change_percent",
    "total_volume", "turnover_vnd",
}


def _decimal_or_raw(value):
    if value is None or isinstance(value, (int, float, Decimal)):
        return value
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return Decimal(raw)
    except InvalidOperation as exc:
        raise ValueError(f"Vietstock numeric value is not an unambiguous decimal: {raw!r}") from exc


class Node1MarketWriter:
    def __init__(self, engine: Engine | None = None):
        url = os.getenv("MARKET_DATABASE_URL", "").strip()
        if engine is None:
            if not url:
                raise RuntimeError("MARKET_DATABASE_URL is required for Node 1 market persistence")
            engine = create_tls_mysql_engine(url, os.getenv("MYSQL_NODE_1_TLS_CA_FILE", ""))
        self.engine = engine

    def persist_records(self, records: list[MarketRecord]) -> int:
        if not records:
            return 0
        latest_quotes: dict[tuple[int, object], tuple[MarketRecord, int]] = {}
        latest_indices: dict[tuple[int, object], tuple[MarketRecord, int]] = {}
        with self.engine.begin() as connection:
            for record in sorted(records, key=lambda item: (item.trade_date, item.observed_at)):
                if record.kind == "quote":
                    security_id, provider_id = self._persist_quote(connection, record)
                    latest_quotes[(security_id, record.trade_date)] = (record, provider_id)
                elif record.kind == "index":
                    index_id, provider_id = self._persist_index(connection, record)
                    latest_indices[(index_id, record.trade_date)] = (record, provider_id)
                else:
                    raise ValueError(f"Unsupported Vietstock record kind: {record.kind}")
            for (security_id, _trade_date), (record, provider_id) in latest_quotes.items():
                self._persist_daily_quote(connection, record, security_id, provider_id)
            for (index_id, _trade_date), (record, provider_id) in latest_indices.items():
                self._persist_daily_index(connection, record, index_id, provider_id)
        return len(records)

    @staticmethod
    def _provider_id(connection, provider_code: str) -> int:
        if provider_code == "vietstock_datafeed":
            connection.execute(text("""
                INSERT INTO market_providers (code, name, homepage_url, data_license, attribution_text, is_active)
                VALUES (:code, 'Vietstock DataFeed', 'https://api.vietstock.vn/', 'licensed',
                    'Vietstock DataFeed', 1)
                ON DUPLICATE KEY UPDATE name=VALUES(name), homepage_url=VALUES(homepage_url),
                    data_license=VALUES(data_license), attribution_text=VALUES(attribution_text), is_active=1
            """), {"code": provider_code})
        value = connection.execute(
            text("SELECT id FROM market_providers WHERE code=:code AND is_active=1"), {"code": provider_code}
        ).scalar_one_or_none()
        if value is None:
            raise RuntimeError(f"market_providers must be provisioned for provider {provider_code!r}")
        return int(value)

    def _persist_quote(self, connection, record: MarketRecord) -> tuple[int, int]:
        provider_id = self._provider_id(connection, record.provider_code)
        exchange_code = str(record.fields.get("exchange_code") or "").strip().upper()
        if exchange_code:
            exchange_id = self._ensure_exchange(connection, exchange_code)
            security_id = connection.execute(text(
                "SELECT id FROM market_securities WHERE exchange_id=:exchange_id AND symbol=:symbol"
            ), {"exchange_id": exchange_id, "symbol": record.symbol_or_index}).scalar_one_or_none()
            if security_id is None:
                result = connection.execute(text("""
                    INSERT INTO market_securities
                        (security_uid, exchange_id, symbol, name, security_type, currency, trading_status, metadata_json)
                    VALUES (:uid, :exchange_id, :symbol, :name, :security_type, 'VND', 'listed', :metadata)
                    ON DUPLICATE KEY UPDATE name=COALESCE(VALUES(name), name), metadata_json=VALUES(metadata_json)
                """), {"uid": str(uuid.uuid4()), "exchange_id": exchange_id,
                      "symbol": record.symbol_or_index,
                      "name": record.fields.get("security_name") or record.fields.get("name"),
                      "security_type": record.fields.get("security_type") or "stock",
                      "metadata": json.dumps({"catalog_source": record.provider_code}, ensure_ascii=False)})
                security_id = result.lastrowid
                if not security_id:
                    security_id = connection.execute(text(
                        "SELECT id FROM market_securities WHERE exchange_id=:exchange_id AND symbol=:symbol"
                    ), {"exchange_id": exchange_id, "symbol": record.symbol_or_index}).scalar_one()
        else:
            candidates = connection.execute(text(
                "SELECT id FROM market_securities WHERE symbol=:symbol AND trading_status <> 'delisted'"
            ), {"symbol": record.symbol_or_index}).scalars().all()
            if len(candidates) != 1:
                raise RuntimeError(f"Vietstock row {record.symbol_or_index!r} needs exchange_code to resolve its canonical security")
            security_id = candidates[0]
        fields = {}
        for key, value in record.fields.items():
            if key not in QUOTE_COLUMNS:
                continue
            if key in NUMERIC_COLUMNS:
                value = _decimal_or_raw(value)
            elif key == "is_delayed" and value is not None and not isinstance(value, bool):
                value = str(value).strip().casefold() in {"1", "true", "yes", "delayed"}
            fields[key] = value
        params = {"security_id": int(security_id), "provider_id": provider_id,
                  "quoted_at": record.observed_at.astimezone(timezone.utc).replace(tzinfo=None),
                  "raw_fields_json": json.dumps(record.raw_payload, ensure_ascii=False, default=str)}
        params.update(fields)
        columns = ["security_id", "provider_id", "quoted_at", *fields, "raw_fields_json"]
        values = [f":{column}" for column in columns]
        updates = ", ".join(f"{column}=VALUES({column})" for column in [*fields, "raw_fields_json"])
        connection.execute(text(
            f"INSERT INTO market_quote_snapshots ({', '.join(columns)}) VALUES ({', '.join(values)}) "
            f"ON DUPLICATE KEY UPDATE {updates}"
        ), params)
        self._persist_latest_quote(connection, int(security_id), provider_id, record, fields)
        return int(security_id), provider_id

    @staticmethod
    def _persist_latest_quote(connection, security_id: int, provider_id: int,
                              record: MarketRecord, fields: dict) -> None:
        quote_at = record.observed_at.astimezone(timezone.utc).replace(tzinfo=None)
        values = {
            "last_price_vnd": fields.get("last_price_vnd"),
            "reference_price_vnd": fields.get("reference_price_vnd"),
            "ceiling_price_vnd": fields.get("ceiling_price_vnd"),
            "floor_price_vnd": fields.get("floor_price_vnd"),
            "change_vnd": fields.get("change_vnd"),
            "change_percent": fields.get("change_percent"),
            "total_volume": fields.get("total_volume"),
            "total_turnover_vnd": fields.get("total_turnover_vnd"),
            "best_bid_price_vnd": fields.get("bid_price_1_vnd"),
            "best_bid_volume": fields.get("bid_volume_1"),
            "best_ask_price_vnd": fields.get("ask_price_1_vnd"),
            "best_ask_volume": fields.get("ask_volume_1"),
            "market_state": fields.get("market_state"),
            "is_delayed": fields.get("is_delayed") if fields.get("is_delayed") is not None else False,
        }
        columns = ["security_id", "quote_at", "provider_id", *values]
        params = {"security_id": security_id, "quote_at": quote_at, "provider_id": provider_id, **values}
        assignments = [
            f"{column}=IF(VALUES(quote_at)>=market_quotes_latest.quote_at, "
            f"VALUES({column}), market_quotes_latest.{column})"
            for column in values
        ]
        # Set quote_at last so each conditional compares against the existing timestamp.
        assignments.append("quote_at=GREATEST(market_quotes_latest.quote_at, VALUES(quote_at))")
        assignments.append("provider_id=IF(VALUES(quote_at)>=market_quotes_latest.quote_at, "
                           "VALUES(provider_id), market_quotes_latest.provider_id)")
        connection.execute(text(
            f"INSERT INTO market_quotes_latest ({', '.join(columns)}) "
            f"VALUES ({', '.join(':' + column for column in columns)}) "
            f"ON DUPLICATE KEY UPDATE {', '.join(assignments)}, updated_at=CURRENT_TIMESTAMP"
        ), params)

    def _persist_index(self, connection, record: MarketRecord) -> tuple[int, int]:
        provider_id = self._provider_id(connection, record.provider_code)
        index_id = connection.execute(text(
            "SELECT id FROM market_indices WHERE code=:code AND is_active=1"
        ), {"code": record.symbol_or_index}).scalar_one_or_none()
        if index_id is None:
            index_names = {
                "VN-INDEX": ("VN-Index", "HOSE"),
                "VNINDEX": ("VN-Index", "HOSE"),
                "VN30": ("VN30", "HOSE"),
                "VN30-INDEX": ("VN30", "HOSE"),
                "VNALLSHARE": ("VNAllshare", "HOSE"),
                "VNMIDCAP": ("VNMidcap", "HOSE"),
                "VN100": ("VN100", "HOSE"),
                "VNSMALLCAP": ("VNSmallcap", "HOSE"),
                "HNX-INDEX": ("HNX-Index", "HNX"),
                "HNX30": ("HNX30", "HNX"),
                "HNX30-INDEX": ("HNX30", "HNX"),
                "HNX30TRI": ("HNX30TRI", "HNX"),
                "UPCOM-INDEX": ("UPCoM-Index", "UPCOM"),
                "UPCOM-LARGE-INDEX": ("UPCoM Large Index", "UPCOM"),
                "UPCOM-MEDIUM-INDEX": ("UPCoM Medium Index", "UPCOM"),
                "UPCOM-SMALL-INDEX": ("UPCoM Small Index", "UPCOM"),
            }
            name, exchange_code = index_names.get(record.symbol_or_index, (record.symbol_or_index, ""))
            exchange_id = self._ensure_exchange(connection, exchange_code) if exchange_code else None
            result = connection.execute(text("""
                INSERT INTO market_indices (code, name, exchange_id, currency, is_active)
                VALUES (:code, :name, :exchange_id, 'VND', 1)
                ON DUPLICATE KEY UPDATE name=VALUES(name), exchange_id=VALUES(exchange_id), is_active=1
            """), {"code": record.symbol_or_index, "name": name, "exchange_id": exchange_id})
            index_id = result.lastrowid
            if not index_id:
                index_id = connection.execute(text(
                    "SELECT id FROM market_indices WHERE code=:code"
                ), {"code": record.symbol_or_index}).scalar_one()
        fields = {key: _decimal_or_raw(value) for key, value in record.fields.items() if key in INDEX_COLUMNS}
        params = {"index_id": int(index_id), "provider_id": provider_id,
                  "observed_at": record.observed_at.astimezone(timezone.utc).replace(tzinfo=None),
                  "raw_fields_json": json.dumps(record.raw_payload, ensure_ascii=False, default=str)}
        params.update(fields)
        columns = ["index_id", "provider_id", "observed_at", *fields, "raw_fields_json"]
        values = [f":{column}" for column in columns]
        updates = ", ".join(f"{column}=VALUES({column})" for column in [*fields, "raw_fields_json"])
        connection.execute(text(
            f"INSERT INTO market_index_snapshots ({', '.join(columns)}) VALUES ({', '.join(values)}) "
            f"ON DUPLICATE KEY UPDATE {updates}"
        ), params)
        return int(index_id), provider_id

    @staticmethod
    def _is_final(record: MarketRecord) -> bool:
        value = record.fields.get("is_final")
        if value is not None:
            return value is True or str(value).strip().casefold() in {"1", "true", "yes", "final"}
        local_time = record.observed_at.astimezone(ZoneInfo("Asia/Ho_Chi_Minh")).time()
        # The configured job is EOD-only; switch this only when a separate
        # intraday job and its provider finality field are introduced.
        return True

    def _persist_daily_quote(self, connection, record: MarketRecord, security_id: int, provider_id: int) -> None:
        close = record.fields.get("last_price_vnd")
        if close is None:
            return
        values = {key: _decimal_or_raw(value) for key, value in record.fields.items()
                  if key in DAILY_QUOTE_NUMERIC_FIELDS}
        values.update({
            "security_id": security_id, "trading_date": record.trade_date,
            "close_vnd": _decimal_or_raw(close), "is_final": self._is_final(record), "provider_id": provider_id,
        })
        mapping = {
            "open_vnd": "open_price_vnd", "high_vnd": "high_price_vnd", "low_vnd": "low_price_vnd",
            "adjusted_close_vnd": "adjusted_close_vnd", "reference_vnd": "reference_price_vnd",
            "ceiling_vnd": "ceiling_price_vnd", "floor_vnd": "floor_price_vnd",
            "change_percent": "change_percent", "matched_volume": "matched_volume",
            "turnover_vnd": "total_turnover_vnd", "foreign_buy_volume": "foreign_buy_volume",
            "foreign_sell_volume": "foreign_sell_volume",
        }
        columns = ["security_id", "trading_date", "close_vnd", "is_final", "provider_id"]
        params = {key: values[key] for key in columns}
        for column, source in mapping.items():
            columns.append(column)
            params[column] = values.get(source)
        update_columns = [column for column in columns if column not in {"security_id", "trading_date"}]
        updates = ", ".join(
            f"{column}=IF(market_prices_daily.is_final=1, market_prices_daily.{column}, VALUES({column}))"
            for column in update_columns if column != "is_final"
        )
        updates += ", is_final=GREATEST(market_prices_daily.is_final, VALUES(is_final))"
        connection.execute(text(
            f"INSERT INTO market_prices_daily ({', '.join(columns)}) "
            f"VALUES ({', '.join(':' + column for column in columns)}) "
            f"ON DUPLICATE KEY UPDATE {updates}, updated_at=CURRENT_TIMESTAMP"
        ), params)

    def _persist_daily_index(self, connection, record: MarketRecord, index_id: int, provider_id: int) -> None:
        close = record.fields.get("index_value")
        if close is None:
            return
        values = {key: _decimal_or_raw(value) for key, value in record.fields.items()
                  if key in DAILY_INDEX_NUMERIC_FIELDS}
        columns = ["index_id", "trading_date", "close_points", "provider_id"]
        params = {"index_id": index_id, "trading_date": record.trade_date,
                  "close_points": _decimal_or_raw(close), "provider_id": provider_id}
        for column in DAILY_INDEX_NUMERIC_FIELDS:
            if column in values:
                columns.append(column)
                params[column] = values[column]
        updates = ", ".join(
            f"{column}=COALESCE(VALUES({column}), market_index_prices_daily.{column})"
            for column in columns if column not in {"index_id", "trading_date"}
        )
        connection.execute(text(
            f"INSERT INTO market_index_prices_daily ({', '.join(columns)}) "
            f"VALUES ({', '.join(':' + column for column in columns)}) "
            f"ON DUPLICATE KEY UPDATE {updates}, updated_at=CURRENT_TIMESTAMP"
        ), params)

    @staticmethod
    def _ensure_exchange(connection, code: str) -> int:
        names = {
            "HOSE": ("Ho Chi Minh Stock Exchange", "https://www.hsx.vn/"),
            "HNX": ("Hanoi Stock Exchange", "https://www.hnx.vn/"),
            "UPCOM": ("Unlisted Public Company Market", "https://www.hnx.vn/"),
        }
        if code not in names:
            raise RuntimeError(f"Unknown Vietstock exchange code: {code!r}")
        value = connection.execute(text("SELECT id FROM market_exchanges WHERE code=:code"), {"code": code}).scalar_one_or_none()
        if value is None:
            name, website = names[code]
            result = connection.execute(text("""
                INSERT INTO market_exchanges (code, name, timezone, currency, website_url, is_active)
                VALUES (:code, :name, 'Asia/Ho_Chi_Minh', 'VND', :website, 1)
                ON DUPLICATE KEY UPDATE name=VALUES(name), website_url=VALUES(website_url), is_active=1
            """), {"code": code, "name": name, "website": website})
            value = result.lastrowid or connection.execute(
                text("SELECT id FROM market_exchanges WHERE code=:code"), {"code": code}
            ).scalar_one()
        return int(value)

