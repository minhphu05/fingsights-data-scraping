from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.engine import Engine

from ..domain.models import Article, MarketRecord
from .mysql_engine import create_tls_mysql_engine


class ScrapeAuditWriter:
    """Writes run and source-item audit rows to Node 2 finsights_scraping only."""

    def __init__(self, engine: Engine | None = None):
        url = os.getenv("SCRAPING_DATABASE_URL", "").strip()
        if engine is None:
            if not url:
                raise RuntimeError("SCRAPING_DATABASE_URL is required; crawl history belongs in Node 2")
            engine = create_tls_mysql_engine(url, os.getenv("MYSQL_NODE_2_TLS_CA_FILE", ""))
        self.engine = engine

    def start_run(self, *, source_code: str, source_kind: str, display_name: str, base_url: str,
                  dagster_run_id: str, job_name: str, start_date, end_date, config: dict[str, Any] | None = None) -> int:
        with self.engine.begin() as connection:
            connection.execute(text("""
                INSERT INTO scrape_sources (source_code, source_kind, display_name, base_url, timezone, config_json)
                VALUES (:code, :kind, :name, :url, 'Asia/Ho_Chi_Minh', :config)
                ON DUPLICATE KEY UPDATE source_kind=VALUES(source_kind), display_name=VALUES(display_name),
                    base_url=VALUES(base_url), config_json=VALUES(config_json), is_active=1
            """), {"code": source_code, "kind": source_kind, "name": display_name, "url": base_url,
                  "config": json.dumps(config or {}, ensure_ascii=False)})
            source_id = int(connection.execute(text(
                "SELECT id FROM scrape_sources WHERE source_code=:code"
            ), {"code": source_code}).scalar_one())
            result = connection.execute(text("""
                INSERT INTO scrape_runs (dagster_run_id, job_name, source_id, start_date, end_date, status)
                VALUES (:run_id, :job_name, :source_id, :start_date, :end_date, 'STARTED')
            """), {"run_id": dagster_run_id, "job_name": job_name, "source_id": source_id,
                  "start_date": start_date, "end_date": end_date})
            return int(result.lastrowid)

    def save_items(self, run_id: int, source_code: str, items: list[Article | MarketRecord],
                   destination_uids: dict[str, str] | None = None) -> None:
        destination_uids = destination_uids or {}
        with self.engine.begin() as connection:
            source_id = int(connection.execute(text("""
                SELECT source_id FROM scrape_runs WHERE id=:id
            """), {"id": run_id}).scalar_one())
            for item in items:
                if isinstance(item, Article):
                    source_item_id = item.source_article_id
                    canonical_url = item.canonical_url
                    kind = "article"
                    published_at = item.published_at.astimezone(timezone.utc).replace(tzinfo=None)
                    business_date = item.published_at.astimezone(ZoneInfo("Asia/Ho_Chi_Minh")).date()
                    metadata = {"title": item.title, "publisher_code": item.publisher_code}
                    content_fingerprint = item.metadata.get("content_fingerprint_sha256")
                    destination_uid = destination_uids.get(source_item_id)
                    destination_domain, destination_db = "news", "finsights_news"
                else:
                    source_item_id = f"{item.kind}:{item.symbol_or_index}:{item.observed_at.isoformat()}"
                    canonical_url = f"vietstock://{item.kind}/{item.symbol_or_index}/{item.observed_at.isoformat()}"
                    kind = item.kind
                    published_at = item.observed_at.astimezone(timezone.utc).replace(tzinfo=None)
                    business_date = item.trade_date
                    metadata = {"symbol_or_index": item.symbol_or_index}
                    content_fingerprint = hashlib.sha256(
                        json.dumps(item.raw_payload, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
                    ).hexdigest()
                    destination_uid = None
                    destination_domain, destination_db = "market", "finsights_market"
                identity = hashlib.sha256(f"{source_code}:{source_item_id}".encode()).hexdigest()
                url_hash = hashlib.sha256(canonical_url.encode()).hexdigest()
                connection.execute(text("""
                    INSERT INTO scrape_items
                        (source_id, item_kind, identity_sha256, source_item_id, canonical_url,
                         canonical_url_sha256, published_at, business_date, content_fingerprint_sha256, destination_domain,
                         destination_database, destination_uid, last_run_id, last_outcome, metadata_json)
                    VALUES (:source_id, :kind, :identity, :source_item_id, :url, :url_hash,
                        :published_at, :business_date, :content_fingerprint, :domain, :database, :destination_uid,
                        :run_id, 'PERSISTED', :metadata)
                    ON DUPLICATE KEY UPDATE canonical_url=VALUES(canonical_url),
                        canonical_url_sha256=VALUES(canonical_url_sha256), published_at=VALUES(published_at),
                        business_date=VALUES(business_date), content_fingerprint_sha256=VALUES(content_fingerprint_sha256),
                        destination_domain=VALUES(destination_domain),
                        destination_database=VALUES(destination_database), destination_uid=VALUES(destination_uid),
                        last_seen_at=CURRENT_TIMESTAMP(3), last_run_id=VALUES(last_run_id),
                        last_outcome='PERSISTED', metadata_json=VALUES(metadata_json)
                """), {"source_id": source_id, "kind": kind, "identity": identity,
                      "source_item_id": source_item_id, "url": canonical_url, "url_hash": url_hash,
                      "published_at": published_at, "business_date": business_date,
                      "content_fingerprint": content_fingerprint, "domain": destination_domain,
                      "database": destination_db, "destination_uid": destination_uid, "run_id": run_id,
                      "metadata": json.dumps(metadata, ensure_ascii=False, default=str)})
                item_id = int(connection.execute(text("""
                    SELECT id FROM scrape_items WHERE source_id=:source_id AND identity_sha256=:identity
                """), {"source_id": source_id, "identity": identity}).scalar_one())
                connection.execute(text("""
                    INSERT INTO scrape_run_items (run_id, item_id, outcome, processed_at)
                    VALUES (:run_id, :item_id, 'PERSISTED', CURRENT_TIMESTAMP(3))
                    ON DUPLICATE KEY UPDATE outcome='PERSISTED', processed_at=CURRENT_TIMESTAMP(3)
                """), {"run_id": run_id, "item_id": item_id})

    def save_attempts(self, run_id: int, source_code: str, attempts: list[dict[str, Any]]) -> None:
        if not attempts:
            return
        with self.engine.begin() as connection:
            source_id = int(connection.execute(text(
                "SELECT source_id FROM scrape_runs WHERE id=:id"
            ), {"id": run_id}).scalar_one())
            for attempt in attempts:
                connection.execute(text("""
                    INSERT INTO scrape_fetch_attempts
                        (run_id, source_id, resource_kind, resource_url, page_number, attempt_number,
                         requested_at, completed_at, http_status, response_content_type,
                         response_size_bytes, response_sha256, duration_ms, outcome, error_type, error_message)
                    VALUES (:run_id, :source_id, :resource_kind, :url, :page, :attempt,
                         :requested_at, :completed_at, :status, :content_type, :size_bytes,
                         :response_hash, :duration_ms, :outcome, :error_type, :error_message)
                """), {"run_id": run_id, "source_id": source_id,
                      "resource_kind": attempt.get("resource_kind", "page"),
                      "url": attempt["url"], "page": attempt.get("page_number"),
                      "attempt": attempt.get("attempt_number", 1),
                      "requested_at": attempt.get("requested_at"),
                      "completed_at": attempt.get("completed_at"),
                      "status": attempt.get("http_status"),
                      "content_type": attempt.get("content_type"),
                      "size_bytes": attempt.get("size_bytes"),
                      "response_hash": attempt.get("response_sha256"),
                      "duration_ms": attempt.get("duration_ms"),
                      "outcome": attempt.get("outcome", "SUCCESS"),
                      "error_type": attempt.get("error_type"),
                      "error_message": attempt.get("error_message")})

    def finish_run(self, run_id: int, status: str, *, discovered: int, persisted: int,
                   failed: int = 0, error: str | None = None) -> None:
        finished = datetime.now(timezone.utc).replace(tzinfo=None)
        with self.engine.begin() as connection:
            connection.execute(text("""
                UPDATE scrape_runs SET status=:status, finished_at=:finished,
                    items_discovered=:discovered, items_fetched=:discovered,
                    items_persisted=:persisted, items_failed=:failed, error_summary=:error,
                    duration_ms=TIMESTAMPDIFF(MICROSECOND, started_at, :finished) / 1000
                WHERE id=:run_id
            """), {"status": status, "finished": finished, "discovered": discovered,
                  "persisted": persisted, "failed": failed, "error": error, "run_id": run_id})

    def save_checkpoint(self, source_code: str, checkpoint_key: str, checkpoint_value: str | dict | Any) -> None:
        """Persist or update a scraping progress checkpoint in Node 2 scrape_checkpoints."""
        val_str = json.dumps(checkpoint_value, ensure_ascii=False) if isinstance(checkpoint_value, (dict, list)) else str(checkpoint_value)
        with self.engine.begin() as connection:
            source_id = connection.execute(text(
                "SELECT id FROM scrape_sources WHERE source_code=:code"
            ), {"code": source_code}).scalar_one_or_none()
            if source_id is None:
                return
            connection.execute(text("""
                INSERT INTO scrape_checkpoints (source_id, checkpoint_key, checkpoint_value, updated_at)
                VALUES (:source_id, :key, :val, CURRENT_TIMESTAMP(3))
                ON DUPLICATE KEY UPDATE checkpoint_value=VALUES(checkpoint_value), updated_at=CURRENT_TIMESTAMP(3)
            """), {"source_id": int(source_id), "key": checkpoint_key, "val": val_str[:1000]})

    def get_checkpoint(self, source_code: str, checkpoint_key: str) -> str | None:
        """Retrieve the last recorded checkpoint for a source and key."""
        with self.engine.begin() as connection:
            row = connection.execute(text("""
                SELECT c.checkpoint_value
                FROM scrape_checkpoints c
                JOIN scrape_sources s ON s.id = c.source_id
                WHERE s.source_code = :code AND c.checkpoint_key = :key
            """), {"code": source_code, "key": checkpoint_key}).scalar_one_or_none()
            return str(row) if row is not None else None


