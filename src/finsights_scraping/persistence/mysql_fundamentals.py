from __future__ import annotations

import hashlib
import json
import os
import re
import uuid
from decimal import Decimal
from typing import Iterable
import unicodedata

from sqlalchemy import text
from sqlalchemy.engine import Engine, make_url

from ..domain.financials import FinancialPeriodRecord, FinancialValueRecord
from .mysql_engine import create_tls_mysql_engine


class Node2FundamentalsWriter:
    """Persists source-reported financial periods and values to Node 2."""

    SOURCE_CODE = "cophieu68"

    def __init__(self, engine: Engine | None = None, market_engine: Engine | None = None):
        market_url = os.getenv("MARKET_DATABASE_URL", "").strip()
        ca_file = os.getenv("MYSQL_NODE_2_TLS_CA_FILE", "").strip()
        if not market_url:
            raise RuntimeError("MARKET_DATABASE_URL is required to resolve Node 2 company identifiers")
        if not ca_file:
            raise RuntimeError("MYSQL_NODE_2_TLS_CA_FILE is required for encrypted Node 2 connections")
        if market_engine is None:
            market_engine = create_tls_mysql_engine(
                market_url, ca_file, password_env="MYSQL_SCRAPING_APP_PASSWORD",
            )
        if engine is None:
            fundamentals_url = make_url(market_url).set(
                database="finsights_fundamentals", username="finsights_fundamentals_app", password=None,
            ).render_as_string(hide_password=False)
            engine = create_tls_mysql_engine(
                fundamentals_url, ca_file, password_env="MYSQL_SCRAPING_APP_PASSWORD",
            )
        self.engine = engine
        self.market_engine = market_engine

    def persist(self, periods: list[FinancialPeriodRecord]) -> tuple[int, int]:
        periods = [period for period in periods if period.values]
        if not periods:
            return 0, 0
        company_uids = self._company_uids({period.symbol for period in periods})
        missing = sorted({period.symbol for period in periods} - set(company_uids))
        if missing:
            raise RuntimeError(f"Node 2 market catalog has no company_uid for: {', '.join(missing)}")

        persisted_periods = 0
        persisted_values = 0
        metric_cache: dict[tuple[str, str, str, str], int] = {}
        mapping_cache: set[tuple[str, str, str]] = set()
        with self.engine.begin() as connection:
            for period in periods:
                values_by_key = {value.line_key: value for value in period.values}
                if len(values_by_key) != len(period.values):
                    raise ValueError(f"{period.symbol} {period.period_code} has duplicate line_key values")
                current = connection.execute(text("""
                    SELECT id, revision_no FROM financial_periods
                    WHERE source_code=:source_code AND source_company_id=:symbol
                      AND period_code=:period_code AND statement_scope='unspecified' AND is_current=1
                    ORDER BY revision_no DESC LIMIT 1
                """), {
                    "source_code": self.SOURCE_CODE, "symbol": period.symbol,
                    "period_code": period.period_code,
                }).mappings().first()
                revision = int(current["revision_no"]) if current else 1
                period_id = int(current["id"]) if current else None
                if current:
                    existing = connection.execute(text("""
                        SELECT line_key, source_label, value_decimal, original_value_decimal,
                               reported_unit, unit_multiplier, source_currency
                        FROM financial_values WHERE period_id=:period_id
                    """), {"period_id": period_id}).mappings().all()
                    if _values_equal(existing, values_by_key):
                        period.revision_no = revision
                    else:
                        max_revision = connection.execute(text("""
                            SELECT COALESCE(MAX(revision_no),0) FROM financial_periods
                            WHERE source_code=:source_code AND source_company_id=:symbol
                              AND period_code=:period_code AND statement_scope='unspecified'
                        """), {
                            "source_code": self.SOURCE_CODE, "symbol": period.symbol,
                            "period_code": period.period_code,
                        }).scalar_one()
                        revision = int(max_revision) + 1
                        connection.execute(text(
                            "UPDATE financial_periods SET is_current=0 WHERE id=:id"
                        ), {"id": period_id})
                        period_id = None
                        period.revision_no = revision
                else:
                    period.revision_no = revision

                if period_id is None:
                    period_uid = period.period_uid
                    result = connection.execute(text("""
                        INSERT INTO financial_periods
                            (period_uid, company_uid, source_code, source_company_id, period_code,
                             fiscal_year, fiscal_quarter, period_type, statement_scope, period_start,
                             period_end, audit_status, currency, revision_no, is_current)
                        VALUES
                            (:period_uid, :company_uid, :source_code, :symbol, :period_code,
                             :fiscal_year, :fiscal_quarter, :period_type, 'unspecified', :period_start,
                             :period_end, 'unknown', 'VND', :revision_no, 1)
                    """), {
                        "period_uid": period_uid, "company_uid": company_uids[period.symbol],
                        "source_code": self.SOURCE_CODE, "symbol": period.symbol,
                        "period_code": period.period_code, "fiscal_year": period.fiscal_year,
                        "fiscal_quarter": period.fiscal_quarter, "period_type": period.period_type,
                        "period_start": period.period_start, "period_end": period.period_end,
                        "revision_no": revision,
                    })
                    period_id = int(result.lastrowid)

                value_params = []
                for value in period.values:
                    cache_key = (value.statement_family, value.source_label, value.unit_code, value.value_kind)
                    metric_id = metric_cache.get(cache_key)
                    if metric_id is None:
                        metric_id = self._ensure_metric(connection, value)
                        metric_cache[cache_key] = metric_id
                    mapping_key = (value.statement_family, value.source_label, str(value.unit_multiplier))
                    if mapping_key not in mapping_cache:
                        self._ensure_mapping(connection, value, metric_id)
                        mapping_cache.add(mapping_key)
                    value_params.append({
                        "period_id": period_id, "metric_id": metric_id, "line_key": value.line_key,
                        "source_label": value.source_label, "value_decimal": value.value_decimal,
                        "original_value_decimal": value.original_value_decimal,
                        "reported_unit": value.reported_unit, "unit_multiplier": value.unit_multiplier,
                        "source_currency": value.source_currency,
                        "row_order": value.row_order,
                        "raw_fields_json": json.dumps(value.raw_fields, ensure_ascii=False, default=str),
                    })
                connection.execute(text("""
                    INSERT INTO financial_values
                        (period_id, metric_id, line_key, source_label, value_decimal,
                         original_value_decimal, reported_unit, unit_multiplier, source_currency,
                         row_order, raw_fields_json)
                    VALUES
                        (:period_id, :metric_id, :line_key, :source_label, :value_decimal,
                         :original_value_decimal, :reported_unit, :unit_multiplier, :source_currency,
                         :row_order, :raw_fields_json)
                    ON DUPLICATE KEY UPDATE metric_id=VALUES(metric_id),
                        source_label=VALUES(source_label), value_decimal=VALUES(value_decimal),
                        original_value_decimal=VALUES(original_value_decimal),
                        reported_unit=VALUES(reported_unit), unit_multiplier=VALUES(unit_multiplier),
                        source_currency=VALUES(source_currency), row_order=VALUES(row_order),
                        raw_fields_json=VALUES(raw_fields_json), updated_at=CURRENT_TIMESTAMP(3)
                """), value_params)
                persisted_values += len(value_params)
                persisted_periods += 1
        return persisted_periods, persisted_values

    def _company_uids(self, symbols: set[str]) -> dict[str, str]:
        if not symbols:
            return {}
        params = {f"symbol_{index}": symbol for index, symbol in enumerate(sorted(symbols))}
        placeholders = ", ".join(f":{key}" for key in params)
        with self.market_engine.connect() as connection:
            rows = connection.execute(text(f"""
                SELECT s.symbol, c.company_uid
                FROM market_securities s
                JOIN market_companies c ON c.id=s.company_id
                WHERE s.symbol IN ({placeholders})
            """), params).all()
        return {str(symbol): str(company_uid) for symbol, company_uid in rows if company_uid}

    def _ensure_metric(self, connection, value: FinancialValueRecord) -> int:
        label_slug = _slug(value.source_label)
        metric_code = f"cophieu68_{value.statement_family}_{label_slug}"
        if len(metric_code) > 90:
            digest = hashlib.sha1(metric_code.encode("utf-8")).hexdigest()[:8]
            metric_code = metric_code[:81] + "_" + digest
        connection.execute(text("""
            INSERT INTO financial_metrics
                (metric_code, canonical_name_vi, statement_family, metric_group, unit_code,
                 value_kind, description, is_active)
            VALUES (:code, :label, :family, :group_name, :unit_code, :value_kind, :description, 1)
            ON DUPLICATE KEY UPDATE canonical_name_vi=VALUES(canonical_name_vi),
                statement_family=VALUES(statement_family), metric_group=VALUES(metric_group),
                unit_code=VALUES(unit_code), value_kind=VALUES(value_kind),
                description=VALUES(description), is_active=1
        """), {
            "code": metric_code, "label": value.source_label[:220],
            "family": value.statement_family, "group_name": value.statement_family,
            "unit_code": value.unit_code, "value_kind": value.value_kind,
            "description": "CoPhieu68 source label; statement unit is not stated on the page; see raw_fields_json.",
        })
        return int(connection.execute(text(
            "SELECT id FROM financial_metrics WHERE metric_code=:code"
        ), {"code": metric_code}).scalar_one())

    def _ensure_mapping(self, connection, value: FinancialValueRecord, metric_id: int) -> None:
        mapping_label = f"[{value.statement_family}] {value.source_label}"[:320]
        connection.execute(text("""
            INSERT INTO financial_metric_mappings
                (source_code, source_metric_code, source_label, metric_id, unit_multiplier, is_active)
            VALUES ('cophieu68', :metric_code, :source_label, :metric_id, :multiplier, 1)
            ON DUPLICATE KEY UPDATE source_metric_code=VALUES(source_metric_code),
                metric_id=VALUES(metric_id), unit_multiplier=VALUES(unit_multiplier), is_active=1
        """), {
            "metric_code": value.line_key[:120], "source_label": mapping_label,
            "metric_id": metric_id, "multiplier": value.unit_multiplier,
        })


def _values_equal(existing, incoming: dict[str, FinancialValueRecord]) -> bool:
    if len(existing) != len(incoming):
        return False
    for row in existing:
        value = incoming.get(row["line_key"])
        if value is None:
            return False
        if (
            row["source_label"] != value.source_label
            or _decimal_text(row["value_decimal"]) != _decimal_text(value.value_decimal)
            or _decimal_text(row["original_value_decimal"]) != _decimal_text(value.original_value_decimal)
            or str(row["reported_unit"] or "") != value.reported_unit
            or _decimal_text(row["unit_multiplier"]) != _decimal_text(value.unit_multiplier)
            or str(row["source_currency"] or "") != str(value.source_currency or "")
        ):
            return False
    return True


def _decimal_text(value: Decimal | int | str | None) -> str | None:
    if value is None:
        return None
    return format(Decimal(str(value)).normalize(), "f")


def _slug(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value.strip().casefold())
    ascii_value = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    slug = re.sub(r"[^a-z0-9]+", "_", ascii_value).strip("_") or "metric"
    if len(slug) > 65:
        slug = slug[:56].rstrip("_") + "_" + hashlib.sha1(value.encode("utf-8")).hexdigest()[:8]
    return slug
