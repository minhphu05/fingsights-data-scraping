from datetime import datetime
from sqlalchemy import (
    BigInteger,
    Column,
    Date,
    DateTime,
    Decimal,
    ForeignKey,
    Index,
    Integer,
    JSON,
    SmallInteger,
    String,
    Text,
)
from sqlalchemy.dialects.mysql import LONGTEXT, TINYINT
from src.database.connection import Base


class CrawlSource(Base):
    __tablename__ = "crawl_sources"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    source_code = Column(String(64), unique=True, nullable=False)
    name = Column(String(128), nullable=False)
    category = Column(String(32), nullable=False)  # news, stocks, financial_report
    base_url = Column(String(512), nullable=False)
    is_active = Column(TINYINT(1), default=1, nullable=False)
    crawl_interval_minutes = Column(Integer, default=60, nullable=False)
    rate_limit_rps = Column(Decimal(4, 2), default=2.0, nullable=False)
    config = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class CrawlJob(Base):
    __tablename__ = "crawl_jobs"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    source_id = Column(BigInteger, ForeignKey("crawl_sources.id", ondelete="CASCADE"), nullable=False)
    job_code = Column(String(96), unique=True, nullable=False)
    dagster_job_name = Column(String(128), nullable=False)
    cron_schedule = Column(String(64), nullable=False)
    is_enabled = Column(TINYINT(1), default=1, nullable=False)
    last_run_at = Column(DateTime, nullable=True)
    last_run_status = Column(String(32), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class CrawlRun(Base):
    __tablename__ = "crawl_runs"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    job_id = Column(BigInteger, ForeignKey("crawl_jobs.id", ondelete="SET NULL"), nullable=True)
    source_id = Column(BigInteger, ForeignKey("crawl_sources.id", ondelete="CASCADE"), nullable=False)
    dagster_run_id = Column(String(64), unique=True, nullable=False)
    pod_name = Column(String(128), nullable=True)
    node_name = Column(String(64), nullable=True)
    status = Column(String(32), default="STARTING", nullable=False)
    items_discovered = Column(Integer, default=0, nullable=False)
    items_scraped = Column(Integer, default=0, nullable=False)
    items_skipped = Column(Integer, default=0, nullable=False)
    items_failed = Column(Integer, default=0, nullable=False)
    error_message = Column(Text, nullable=True)
    started_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    finished_at = Column(DateTime, nullable=True)
    duration_seconds = Column(Integer, nullable=True)


class RawArticle(Base):
    __tablename__ = "raw_articles"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    source_id = Column(BigInteger, ForeignKey("crawl_sources.id", ondelete="CASCADE"), nullable=False)
    run_id = Column(BigInteger, ForeignKey("crawl_runs.id", ondelete="SET NULL"), nullable=True)
    article_uid = Column(String(36), unique=True, nullable=False)
    url_hash = Column(String(64), unique=True, nullable=False)
    url = Column(String(1024), nullable=False)
    title = Column(String(512), nullable=False)
    summary = Column(Text, nullable=True)
    content_raw = Column(LONGTEXT, nullable=True)
    author = Column(String(128), nullable=True)
    published_at = Column(DateTime, nullable=True)
    scraped_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    pipeline_status = Column(String(32), default="RAW", nullable=False)
    tickers = Column(JSON, nullable=True)
    metadata_json = Column("metadata", JSON, nullable=True)


class RawStockQuote(Base):
    __tablename__ = "raw_stock_quotes"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    source_id = Column(BigInteger, ForeignKey("crawl_sources.id", ondelete="CASCADE"), nullable=False)
    run_id = Column(BigInteger, ForeignKey("crawl_runs.id", ondelete="SET NULL"), nullable=True)
    ticker = Column(String(16), nullable=False)
    trade_date = Column(Date, nullable=False)
    price_open = Column(Decimal(12, 2), nullable=True)
    price_high = Column(Decimal(12, 2), nullable=True)
    price_low = Column(Decimal(12, 2), nullable=True)
    price_close = Column(Decimal(12, 2), nullable=True)
    price_change = Column(Decimal(12, 2), nullable=True)
    pct_change = Column(Decimal(6, 2), nullable=True)
    total_volume = Column(BigInteger, nullable=True)
    total_value = Column(Decimal(18, 2), nullable=True)
    raw_payload = Column(JSON, nullable=True)
    scraped_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class CrawlError(Base):
    __tablename__ = "crawl_errors"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    run_id = Column(BigInteger, ForeignKey("crawl_runs.id", ondelete="SET NULL"), nullable=True)
    source_id = Column(BigInteger, ForeignKey("crawl_sources.id", ondelete="CASCADE"), nullable=False)
    target_url = Column(String(1024), nullable=False)
    http_status = Column(Integer, nullable=True)
    error_type = Column(String(64), nullable=False)
    error_message = Column(Text, nullable=True)
    retry_count = Column(Integer, default=0, nullable=False)
    resolved = Column(TINYINT(1), default=0, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class DailyCrawlMetric(Base):
    __tablename__ = "daily_crawl_metrics"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    metric_date = Column(Date, nullable=False)
    source_id = Column(BigInteger, ForeignKey("crawl_sources.id", ondelete="CASCADE"), nullable=False)
    total_runs = Column(Integer, default=0, nullable=False)
    successful_runs = Column(Integer, default=0, nullable=False)
    failed_runs = Column(Integer, default=0, nullable=False)
    total_items_scraped = Column(Integer, default=0, nullable=False)
    avg_duration_seconds = Column(Decimal(8, 2), default=0.0, nullable=False)
    success_rate_pct = Column(Decimal(5, 2), default=0.0, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
