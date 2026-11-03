from datetime import datetime, date
from typing import Any
import uuid
from sqlalchemy.orm import Session
from sqlalchemy import select
from src.database.models import (
    CrawlSource,
    CrawlJob,
    CrawlRun,
    RawArticle,
    RawStockQuote,
    CrawlError,
    DailyCrawlMetric,
)
from src.core.deduplicator import UrlDeduplicator


class CrawlRepository:
    """Repository handling all database transactions for crawling operations."""

    def __init__(self, session: Session):
        self.session = session

    def get_or_create_source(
        self,
        source_code: str,
        name: str,
        category: str,
        base_url: str,
        rate_limit_rps: float = 2.0,
    ) -> CrawlSource:
        source = self.session.execute(
            select(CrawlSource).where(CrawlSource.source_code == source_code)
        ).scalar_one_or_none()

        if not source:
            source = CrawlSource(
                source_code=source_code,
                name=name,
                category=category,
                base_url=base_url,
                rate_limit_rps=rate_limit_rps,
            )
            self.session.add(source)
            self.session.flush()
        return source

    def start_run(
        self,
        source_id: int,
        dagster_run_id: str,
        job_id: int | None = None,
        pod_name: str | None = None,
        node_name: str | None = None,
    ) -> CrawlRun:
        run = CrawlRun(
            source_id=source_id,
            job_id=job_id,
            dagster_run_id=dagster_run_id,
            pod_name=pod_name,
            node_name=node_name,
            status="RUNNING",
            started_at=datetime.utcnow(),
        )
        self.session.add(run)
        self.session.flush()
        return run

    def finish_run(
        self,
        run_id: int,
        status: str,
        discovered: int,
        scraped: int,
        skipped: int,
        failed: int,
        error_message: str | None = None,
    ) -> None:
        run = self.session.get(CrawlRun, run_id)
        if run:
            run.status = status
            run.items_discovered = discovered
            run.items_scraped = scraped
            run.items_skipped = skipped
            run.items_failed = failed
            run.error_message = error_message
            run.finished_at = datetime.utcnow()
            run.duration_seconds = int((run.finished_at - run.started_at).total_seconds())
            self.session.flush()

    def save_article(
        self,
        source_id: int,
        url: str,
        title: str,
        run_id: int | None = None,
        summary: str | None = None,
        content_raw: str | None = None,
        author: str | None = None,
        published_at: datetime | None = None,
        tickers: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> RawArticle:
        url_hash = UrlDeduplicator.compute_hash(url)
        article = RawArticle(
            source_id=source_id,
            run_id=run_id,
            article_uid=str(uuid.uuid4()),
            url_hash=url_hash,
            url=url,
            title=title,
            summary=summary,
            content_raw=content_raw,
            author=author,
            published_at=published_at,
            scraped_at=datetime.utcnow(),
            pipeline_status="RAW",
            tickers=tickers or [],
            metadata_json=metadata or {},
        )
        self.session.add(article)
        self.session.flush()
        return article

    def save_stock_quote(
        self,
        source_id: int,
        ticker: str,
        trade_date: date,
        run_id: int | None = None,
        price_open: float | None = None,
        price_high: float | None = None,
        price_low: float | None = None,
        price_close: float | None = None,
        price_change: float | None = None,
        pct_change: float | None = None,
        total_volume: int | None = None,
        total_value: float | None = None,
        raw_payload: dict[str, Any] | None = None,
    ) -> RawStockQuote:
        quote = RawStockQuote(
            source_id=source_id,
            run_id=run_id,
            ticker=ticker.upper().strip(),
            trade_date=trade_date,
            price_open=price_open,
            price_high=price_high,
            price_low=price_low,
            price_close=price_close,
            price_change=price_change,
            pct_change=pct_change,
            total_volume=total_volume,
            total_value=total_value,
            raw_payload=raw_payload or {},
            scraped_at=datetime.utcnow(),
        )
        self.session.add(quote)
        self.session.flush()
        return quote

    def record_error(
        self,
        source_id: int,
        target_url: str,
        error_type: str,
        error_message: str,
        run_id: int | None = None,
        http_status: int | None = None,
    ) -> CrawlError:
        err = CrawlError(
            source_id=source_id,
            run_id=run_id,
            target_url=target_url,
            error_type=error_type,
            error_message=error_message,
            http_status=http_status,
            created_at=datetime.utcnow(),
        )
        self.session.add(err)
        self.session.flush()
        return err
