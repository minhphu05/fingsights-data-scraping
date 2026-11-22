from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import bindparam, text
from sqlalchemy.engine import Engine

from .mysql_engine import create_tls_mysql_engine
from .news_quality import FinancialNewsQualityGate


LOCAL_TZ = ZoneInfo("Asia/Ho_Chi_Minh")
DEFAULT_PUBLISHERS = ("cafef", "thanhnien", "tuoitre", "vnexpress", "baomoi")


def _utc_boundary(day: date) -> datetime:
    return datetime.combine(day, time.min, LOCAL_TZ).astimezone(timezone.utc).replace(tzinfo=None)


def _status_for(decision: str) -> str:
    return {
        "publish": "published",
        "exclude": "quality_excluded",
        "review": "quality_review",
    }[decision]


class NewsQualityBackfill:
    """Reclassify already-persisted articles using the live ingestion ruleset."""

    def __init__(
        self,
        publisher_domains: dict[str, str],
        engine: Engine | None = None,
        *,
        batch_size: int = 500,
    ) -> None:
        if batch_size < 1 or batch_size > 5000:
            raise ValueError("batch_size must be between 1 and 5000")
        if engine is None:
            import os

            engine = create_tls_mysql_engine(
                os.getenv("NEWS_DATABASE_URL", "").strip(),
                os.getenv("MYSQL_NODE_2_TLS_CA_FILE", "").strip(),
                password_env="MYSQL_NEWS_APP_PASSWORD",
            )
        self.engine = engine
        self.publisher_domains = publisher_domains
        self.batch_size = batch_size
        self.gate = FinancialNewsQualityGate()

    def run(
        self,
        start_date: date,
        end_date: date,
        publisher_codes: tuple[str, ...] = DEFAULT_PUBLISHERS,
        run_id: str | None = None,
    ) -> dict[str, Any]:
        if end_date < start_date:
            raise ValueError("end_date must be on or after start_date")
        if not publisher_codes:
            raise ValueError("At least one publisher code is required")

        start_utc = _utc_boundary(start_date)
        end_utc = _utc_boundary(end_date + timedelta(days=1))
        counters: dict[str, Counter[str]] = defaultdict(Counter)
        total = 0
        cursor_id = 0
        select_batch = text("""
            SELECT a.id, pub.code AS publisher_code, a.title, a.summary, a.excerpt,
                   a.original_section, a.canonical_url, a.published_at,
                   c.body_plaintext,
                   (SELECT p.original_source_url
                    FROM news_article_publications p
                    WHERE p.article_id=a.id
                    ORDER BY p.is_primary DESC, p.id ASC LIMIT 1) AS original_source_url
            FROM news_articles a
            JOIN news_publishers pub ON pub.id=a.publisher_id
            LEFT JOIN news_article_contents c ON c.article_id=a.id
            WHERE pub.code IN :publisher_codes
              AND a.published_at >= :start_utc AND a.published_at < :end_utc
              AND a.id > :cursor_id
            ORDER BY a.id ASC LIMIT :batch_size
        """).bindparams(bindparam("publisher_codes", expanding=True))

        with self.engine.begin() as connection:
            for publisher_code, domain in self.publisher_domains.items():
                if publisher_code in publisher_codes:
                    connection.execute(text("""
                        UPDATE news_publishers SET domain=:domain, updated_at=CURRENT_TIMESTAMP
                        WHERE code=:publisher_code
                    """), {"domain": domain, "publisher_code": publisher_code})

        while True:
            with self.engine.connect() as connection:
                rows = connection.execute(select_batch, {
                    "publisher_codes": list(publisher_codes),
                    "start_utc": start_utc,
                    "end_utc": end_utc,
                    "cursor_id": cursor_id,
                    "batch_size": self.batch_size,
                }).mappings().all()
            if not rows:
                break

            article_ids = [int(row["id"]) for row in rows]
            matched_by_article: dict[int, list[dict[str, Any]]] = defaultdict(list)
            match_statement = text("""
                SELECT article_id, symbol_at_publication, matched_alias, match_method,
                       match_confidence, evidence_text, review_status
                FROM news_article_securities
                WHERE article_id IN :article_ids
            """).bindparams(bindparam("article_ids", expanding=True))
            with self.engine.connect() as connection:
                match_rows = connection.execute(match_statement, {"article_ids": article_ids}).mappings().all()
            for match_row in match_rows:
                matched_by_article[int(match_row["article_id"])].append({
                    "symbol": match_row["symbol_at_publication"],
                    "alias": match_row["matched_alias"],
                    "method": match_row["match_method"],
                    "confidence": float(match_row["match_confidence"] or 0),
                    "evidence": match_row["evidence_text"],
                    "previous_review_status": match_row["review_status"],
                })

            quality_rows: list[dict[str, Any]] = []
            status_rows: list[dict[str, Any]] = []
            rejected_ids: list[dict[str, int]] = []
            for row in rows:
                article_id = int(row["id"])
                cursor_id = article_id
                publisher_code = str(row["publisher_code"])
                assessment = self.gate.assess(
                    title=str(row["title"] or ""),
                    summary=row["summary"] or row["excerpt"],
                    body=str(row["body_plaintext"] or ""),
                    section=row["original_section"],
                    canonical_url=str(row["canonical_url"] or ""),
                    original_source_url=row["original_source_url"],
                    publisher_domain=self.publisher_domains.get(publisher_code),
                    matched_securities=matched_by_article.get(article_id, []),
                )
                quality_rows.append({
                    "article_id": article_id,
                    "relevance": assessment.financial_relevance,
                    "topic": assessment.topic_label,
                    "decision": assessment.publication_decision,
                    "confidence": assessment.confidence,
                    "reason": assessment.reason_code,
                    "evidence": json.dumps(assessment.evidence, ensure_ascii=False),
                    "ruleset": self.gate.ruleset_version,
                    "run_id": run_id,
                    "review_status": "pending" if assessment.publication_decision == "review" else "automatic",
                })
                status_rows.append({
                    "article_id": article_id,
                    "article_status": _status_for(assessment.publication_decision),
                })
                if assessment.publication_decision != "publish":
                    rejected_ids.append({"article_id": article_id})
                counters[publisher_code][assessment.financial_relevance] += 1
                counters[publisher_code][assessment.publication_decision] += 1
                counters[publisher_code][f"topic:{assessment.topic_label}"] += 1
                total += 1

            with self.engine.begin() as connection:
                connection.execute(text("""
                    INSERT INTO news_article_quality
                        (article_id, financial_relevance, topic_label, publication_decision,
                         confidence, reason_code, evidence_json, ruleset_version, assessed_by,
                         last_run_id, review_status, assessed_at, updated_at)
                    VALUES (:article_id, :relevance, :topic, :decision, :confidence, :reason,
                            :evidence, :ruleset, 'news_quality_backfill', :run_id, :review_status,
                            CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3))
                    ON DUPLICATE KEY UPDATE financial_relevance=VALUES(financial_relevance),
                        topic_label=VALUES(topic_label), publication_decision=VALUES(publication_decision),
                        confidence=VALUES(confidence), reason_code=VALUES(reason_code),
                        evidence_json=VALUES(evidence_json), ruleset_version=VALUES(ruleset_version),
                        assessed_by=VALUES(assessed_by), last_run_id=VALUES(last_run_id),
                        review_status=VALUES(review_status), assessed_at=CURRENT_TIMESTAMP(3),
                        updated_at=CURRENT_TIMESTAMP(3)
                """), quality_rows)
                connection.execute(text("""
                    UPDATE news_articles SET article_status=:article_status,
                        updated_at=CURRENT_TIMESTAMP WHERE id=:article_id
                """), status_rows)
                if rejected_ids:
                    connection.execute(text("""
                        UPDATE news_article_securities SET review_status='rejected'
                        WHERE article_id=:article_id
                    """), rejected_ids)

            print(
                f"Classified through news_articles.id={cursor_id}; "
                f"batch={len(rows)}; total={total}",
                flush=True,
            )

        return {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "publishers": {
                publisher: dict(counts)
                for publisher, counts in sorted(counters.items())
            },
            "total_articles": total,
            "ruleset_version": self.gate.ruleset_version,
        }
