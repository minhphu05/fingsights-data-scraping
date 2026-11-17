#!/usr/bin/env python3
"""Apply current Node 2 ticker/company aliases to already-stored news articles."""

from __future__ import annotations

import argparse
import os
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import yaml
from sqlalchemy import text

from src.finsights_scraping.persistence.mysql_engine import create_tls_mysql_engine
from src.finsights_scraping.persistence.security_matching import SecurityAliasMatcher


ROOT = Path(__file__).resolve().parents[1]
LOCAL_TZ = ZoneInfo("Asia/Ho_Chi_Minh")
INSERT_MATCH_SQL = text("""
    INSERT INTO news_article_securities
        (article_id, security_uid, symbol_at_publication, relation_type, is_primary,
         match_method, match_confidence, matched_alias, evidence_text, review_status)
    VALUES (:article_id, :security_uid, :symbol, 'mentioned', 0, :method,
            :confidence, :alias, :evidence, :review_status)
    ON DUPLICATE KEY UPDATE match_method=VALUES(match_method),
        match_confidence=VALUES(match_confidence), matched_alias=VALUES(matched_alias),
        evidence_text=VALUES(evidence_text), review_status=VALUES(review_status)
""")


def _utc_boundary(local_day: date) -> datetime:
    return datetime.combine(local_day, time.min, LOCAL_TZ).astimezone(timezone.utc).replace(tzinfo=None)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", default="2026-01-01")
    parser.add_argument("--end-date", default="2026-11-17")
    parser.add_argument("--batch-size", type=int, default=500)
    args = parser.parse_args()
    start_date = date.fromisoformat(args.start_date)
    end_date = date.fromisoformat(args.end_date)
    if end_date < start_date:
        raise ValueError("end-date must be on or after start-date")
    if args.batch_size < 1:
        raise ValueError("batch-size must be positive")

    config = yaml.safe_load((ROOT / "configs/reference/financial-news-universe.yaml").read_text(encoding="utf-8"))
    allowed_symbols = {str(row["symbol"]).upper() for row in config["securities"]}
    ca_file = os.getenv("MYSQL_NODE_2_TLS_CA_FILE", "").strip()
    news_engine = create_tls_mysql_engine(
        os.getenv("NEWS_DATABASE_URL", "").strip(),
        ca_file,
        password_env="MYSQL_NEWS_APP_PASSWORD",
    )
    market_engine = create_tls_mysql_engine(
        os.getenv("MARKET_DATABASE_URL", "").strip(),
        ca_file,
        password_env="MYSQL_SCRAPING_APP_PASSWORD",
    )
    matcher = SecurityAliasMatcher(market_engine=market_engine, fallback_to_file=False)
    if not matcher.aliases:
        raise RuntimeError("Node 2 market catalog has no security aliases; run sync_security_catalog.py first")

    start_utc = _utc_boundary(start_date)
    end_utc = _utc_boundary(end_date + timedelta(days=1))
    cursor_id = 0
    article_count = 0
    match_count = 0
    statement = text("""
        SELECT a.id, a.title, a.summary, a.excerpt, a.published_at, c.body_plaintext
        FROM news_articles a
        LEFT JOIN news_article_contents c ON c.article_id=a.id
        WHERE a.id > :cursor_id AND a.published_at >= :start_utc AND a.published_at < :end_utc
        ORDER BY a.id LIMIT :batch_size
    """)
    while True:
        with news_engine.connect() as connection:
            rows = connection.execute(statement, {
                "cursor_id": cursor_id,
                "start_utc": start_utc,
                "end_utc": end_utc,
                "batch_size": args.batch_size,
            }).mappings().all()
        if not rows:
            break

        match_rows = []
        for row in rows:
            cursor_id = int(row["id"])
            article_count += 1
            published_at = row["published_at"]
            if published_at is None:
                continue
            local_day = published_at.replace(tzinfo=timezone.utc).astimezone(LOCAL_TZ).date()
            text_blob = "\n".join(
                str(row[field] or "") for field in ("title", "summary", "excerpt", "body_plaintext")
            )
            for match in matcher.match(text_blob, local_day):
                if match.symbol.upper() not in allowed_symbols:
                    continue
                match_rows.append({
                    "article_id": int(row["id"]),
                    "security_uid": match.security_uid,
                    "symbol": match.symbol,
                    "method": match.method,
                    "confidence": match.confidence,
                    "alias": match.alias_text,
                    "evidence": match.evidence,
                    "review_status": (
                        "auto" if match.confidence >= 0.95 and match.method == "ticker_boundary" else "pending"
                    ),
                })

        if match_rows:
            with news_engine.begin() as connection:
                connection.execute(INSERT_MATCH_SQL, match_rows)
            match_count += len(match_rows)
        print(f"Processed through news_articles.id={cursor_id}; rows={len(rows)}; matches={match_count}", flush=True)

    print(f"Reconciled articles={article_count}; ticker matches upserted={match_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
