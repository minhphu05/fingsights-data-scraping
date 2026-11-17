#!/usr/bin/env python3
"""Idempotently seed the 15 sheet-listed securities and aliases into Node 2."""

from __future__ import annotations

import argparse
import os
import uuid
from pathlib import Path

import yaml
from sqlalchemy import text

from src.finsights_scraping.persistence.mysql_engine import create_tls_mysql_engine
from src.finsights_scraping.persistence.security_matching import normalize_text


ROOT = Path(__file__).resolve().parents[1]
UUID_NAMESPACE = uuid.UUID("718e1ef3-76d3-4c79-ae97-c84ce6781265")


def _stable_uid(kind: str, exchange_code: str, symbol: str) -> str:
    return str(uuid.uuid5(UUID_NAMESPACE, f"finsights:{kind}:{exchange_code}:{symbol}"))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        default=str(ROOT / "configs/reference/financial-news-universe.yaml"),
    )
    args = parser.parse_args()

    config = yaml.safe_load(Path(args.config).read_text(encoding="utf-8"))
    if not isinstance(config, dict) or config.get("version") != 1:
        raise ValueError("Universe file must be a version: 1 YAML mapping")
    database_url = os.getenv("MARKET_DATABASE_URL", "").strip()
    ca_file = os.getenv("MYSQL_NODE_2_TLS_CA_FILE", "").strip()
    engine = create_tls_mysql_engine(
        database_url,
        ca_file,
        password_env="MYSQL_SCRAPING_APP_PASSWORD",
    )
    exchange = config["exchange"]
    exchange_code = str(exchange["code"]).strip().upper()
    securities = config.get("securities", [])
    expected_symbols = {str(row["symbol"]).strip().upper() for row in securities}
    if len(expected_symbols) != len(securities):
        raise ValueError("Universe contains duplicate ticker symbols")

    aliases_written = 0
    with engine.begin() as connection:
        connection.execute(text("""
            INSERT INTO market_exchanges (code, name, timezone, currency, website_url, is_active)
            VALUES (:code, :name, 'Asia/Ho_Chi_Minh', 'VND', :website_url, 1)
            ON DUPLICATE KEY UPDATE name=VALUES(name), timezone=VALUES(timezone),
                currency=VALUES(currency), website_url=VALUES(website_url), is_active=1
        """), {
            "code": exchange_code,
            "name": exchange["name"],
            "website_url": exchange.get("website_url"),
        })
        exchange_id = int(connection.execute(text(
            "SELECT id FROM market_exchanges WHERE code=:code"
        ), {"code": exchange_code}).scalar_one())

        for security in securities:
            symbol = str(security["symbol"]).strip().upper()
            company_uid = _stable_uid("company", exchange_code, symbol)
            security_uid = _stable_uid("security", exchange_code, symbol)
            company_id = connection.execute(text(
                "SELECT id FROM market_companies WHERE company_uid=:uid"
            ), {"uid": company_uid}).scalar_one_or_none()
            if company_id is None:
                company_id = connection.execute(text("""
                    INSERT INTO market_companies (company_uid, legal_name, short_name, status, metadata_json)
                    VALUES (:uid, :legal_name, :short_name, 'active', :metadata)
                """), {
                    "uid": company_uid,
                    "legal_name": security["company_name"],
                    "short_name": security.get("short_name"),
                    "metadata": '{"catalog_source":"financial_news_universe"}',
                }).lastrowid
            else:
                connection.execute(text("""
                    UPDATE market_companies SET legal_name=:legal_name, short_name=:short_name,
                        status='active', metadata_json=:metadata WHERE id=:id
                """), {
                    "id": company_id,
                    "legal_name": security["company_name"],
                    "short_name": security.get("short_name"),
                    "metadata": '{"catalog_source":"financial_news_universe"}',
                })

            security_row = connection.execute(text("""
                SELECT id, security_uid FROM market_securities
                WHERE exchange_id=:exchange_id AND symbol=:symbol LIMIT 1
            """), {"exchange_id": exchange_id, "symbol": symbol}).mappings().first()
            if security_row is None:
                security_id = connection.execute(text("""
                    INSERT INTO market_securities
                        (security_uid, company_id, exchange_id, symbol, name, security_type, currency,
                         trading_status, metadata_json)
                    VALUES (:uid, :company_id, :exchange_id, :symbol, :name, 'stock', 'VND',
                            'listed', :metadata)
                """), {
                    "uid": security_uid,
                    "company_id": company_id,
                    "exchange_id": exchange_id,
                    "symbol": symbol,
                    "name": security.get("short_name") or security["company_name"],
                    "metadata": '{"catalog_source":"financial_news_universe"}',
                }).lastrowid
            else:
                security_id = int(security_row["id"])
                connection.execute(text("""
                    UPDATE market_securities SET company_id=:company_id, name=:name,
                        trading_status='listed', metadata_json=:metadata WHERE id=:id
                """), {
                    "id": security_id,
                    "company_id": company_id,
                    "name": security.get("short_name") or security["company_name"],
                    "metadata": '{"catalog_source":"financial_news_universe"}',
                })

            alias_rows = [{
                "alias_text": alias,
                "alias_normalized": normalize_text(alias),
                "alias_type": "ticker" if alias.strip().upper() == symbol else (
                    "legal_name" if normalize_text(alias) == normalize_text(security["company_name"])
                    else "brand_name"
                ),
                "match_mode": "exact_token" if alias.strip().upper() == symbol else "phrase",
                "needs_context": alias.strip().upper() == symbol,
                "confidence": 0.98 if alias.strip().upper() == symbol else 0.97,
            } for alias in security.get("aliases", [])]
            if not any(row["alias_type"] == "ticker" for row in alias_rows):
                raise ValueError(f"{symbol} must have an exact ticker alias")
            for alias in alias_rows:
                existing_id = connection.execute(text("""
                    SELECT id FROM market_security_aliases
                    WHERE security_id=:security_id AND alias_normalized=:normalized AND alias_type=:alias_type
                    LIMIT 1
                """), {
                    "security_id": security_id,
                    "normalized": alias["alias_normalized"],
                    "alias_type": alias["alias_type"],
                }).scalar_one_or_none()
                values = {
                    "security_id": security_id,
                    "alias_text": alias["alias_text"],
                    "alias_normalized": alias["alias_normalized"],
                    "alias_type": alias["alias_type"],
                    "match_mode": alias["match_mode"],
                    "needs_context": alias["needs_context"],
                    "confidence": alias["confidence"],
                }
                if existing_id is None:
                    connection.execute(text("""
                        INSERT INTO market_security_aliases
                            (security_id, alias_text, alias_normalized, alias_type, language_code,
                             match_mode, requires_financial_context, confidence, is_active)
                        VALUES (:security_id, :alias_text, :alias_normalized, :alias_type, 'vi',
                                :match_mode, :needs_context, :confidence, 1)
                    """), values)
                else:
                    connection.execute(text("""
                        UPDATE market_security_aliases SET alias_text=:alias_text,
                            match_mode=:match_mode, requires_financial_context=:needs_context,
                            confidence=:confidence, is_active=1 WHERE id=:id
                    """), {**values, "id": existing_id})
                aliases_written += 1

    print(f"Node 2 securities synchronized: {len(expected_symbols)} symbols; {aliases_written} aliases")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
