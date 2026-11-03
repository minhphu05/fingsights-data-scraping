from __future__ import annotations

import argparse
from pathlib import Path

import yaml
from sqlalchemy import text

from src.finsights_scraping.persistence.mysql_engine import create_tls_mysql_engine
from src.finsights_scraping.persistence.security_matching import normalize_text


ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    parser = argparse.ArgumentParser(description="Idempotently load configured stock/company aliases into Node 1 market catalog")
    parser.add_argument("--config", default=str(ROOT / "configs/reference/security-aliases.yaml"))
    args = parser.parse_args()
    config = yaml.safe_load(Path(args.config).read_text(encoding="utf-8"))
    if not isinstance(config, dict) or config.get("version") != 1:
        raise ValueError("Alias file must be a version: 1 YAML mapping")

    import os
    database_url = os.getenv("MARKET_DATABASE_URL", "").strip()
    ca_file = os.getenv("MYSQL_NODE_1_TLS_CA_FILE", "").strip()
    engine = create_tls_mysql_engine(database_url, ca_file)
    rows_written = 0
    with engine.begin() as connection:
        for security in config.get("securities", []):
            result = connection.execute(text("""
                SELECT s.id FROM market_securities s
                JOIN market_exchanges e ON e.id=s.exchange_id
                WHERE s.symbol=:symbol AND e.code=:exchange_code AND s.trading_status <> 'delisted'
                LIMIT 1
            """), {"symbol": security["symbol"], "exchange_code": security["exchange_code"]})
            security_id = result.scalar_one_or_none()
            if security_id is None:
                raise RuntimeError(
                    f"Security {security['symbol']} ({security['exchange_code']}) is not in market_securities yet"
                )
            for alias in security.get("aliases", []):
                normalized = normalize_text(alias["text"])
                existing_id = connection.execute(text("""
                    SELECT id FROM market_security_aliases
                    WHERE security_id=:security_id AND alias_normalized=:normalized AND alias_type=:alias_type
                    LIMIT 1
                """), {"security_id": security_id, "normalized": normalized,
                      "alias_type": alias["alias_type"]}).scalar_one_or_none()
                values = {
                    "security_id": security_id, "alias_text": alias["text"],
                    "alias_normalized": normalized, "alias_type": alias["alias_type"],
                    "language_code": alias.get("language_code", "vi"),
                    "match_mode": alias.get("match_mode", "phrase"),
                    "requires_financial_context": bool(alias.get("requires_financial_context", True)),
                    "confidence": float(alias.get("confidence", 0.9)),
                    "effective_from": alias.get("effective_from"), "effective_to": alias.get("effective_to"),
                }
                if existing_id is None:
                    connection.execute(text("""
                        INSERT INTO market_security_aliases
                            (security_id, alias_text, alias_normalized, alias_type, language_code,
                             match_mode, requires_financial_context, confidence, effective_from, effective_to, is_active)
                        VALUES (:security_id, :alias_text, :alias_normalized, :alias_type, :language_code,
                             :match_mode, :requires_financial_context, :confidence, :effective_from, :effective_to, 1)
                    """), values)
                else:
                    connection.execute(text("""
                        UPDATE market_security_aliases SET alias_text=:alias_text, language_code=:language_code,
                            match_mode=:match_mode, requires_financial_context=:requires_financial_context,
                            confidence=:confidence, effective_from=:effective_from, effective_to=:effective_to,
                            is_active=1
                        WHERE id=:id
                    """), {**values, "id": existing_id})
                rows_written += 1
    print(f"Configured aliases synchronized: {rows_written}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
