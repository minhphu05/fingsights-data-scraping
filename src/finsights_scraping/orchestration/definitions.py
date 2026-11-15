from __future__ import annotations

import asyncio
import json
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import yaml
from dagster import Definitions, Field, RunRequest, SkipReason, String, job, op, schedule

from ..domain.models import Article
from ..domain.windows import DateWindow
from ..persistence import Node2MarketWriter, Node2NewsWriter, ScrapeAuditWriter
from ..sources.market import VietstockHistoryClient, VietstockPublicBoardClient
from ..sources.news import (
    BaoMoiClient,
    CafeFClient,
    ConfiguredHtmlNewsClient,
    RssNewsClient,
    SitemapHtmlNewsClient,
    SourceAuthorizationError,
    source_collection_enabled,
)


ROOT = Path(__file__).resolve().parents[3]
CONFIG_DIR = ROOT / "configs"
LOCAL_TZ = ZoneInfo("Asia/Ho_Chi_Minh")
CONFIGURED_NEWS_SOURCES = {
    "thanhnien": ("thanhnien_news", "30 2 * * *"),
    "tuoitre": ("tuoitre_news", "45 2 * * *"),
    "vnexpress": ("vnexpress_news", "15 3 * * *"),
    "vneconomy": ("vneconomy_news", "0 4 * * *"),
}
FINANCIAL_NEWS_SOURCES = ("cafef", "thanhnien", "tuoitre", "vnexpress", "baomoi")
RSS_FINANCIAL_NEWS_SOURCES = ("cafef", "thanhnien", "tuoitre", "vnexpress")
HTML_BACKFILL_SOURCES = FINANCIAL_NEWS_SOURCES


def _yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as stream:
        value = yaml.safe_load(stream)
    if not isinstance(value, dict):
        raise ValueError(f"Expected a YAML mapping in {path}")
    return value


def _vietstock_mapping_path() -> Path:
    path = Path(os.getenv(
        "VIETSTOCK_MAPPING_FILE",
        str(CONFIG_DIR / "sources/market/vietstock-datafeed-mapping.yaml"),
    ))
    if not path.is_absolute():
        path = ROOT / path
    return path if path.exists() else CONFIG_DIR / "sources/market/vietstock-datafeed-mapping.example.yaml"


def _vietstock_schedule_ready() -> bool:
    source_config = _yaml(CONFIG_DIR / "sources/market/vietstock.yaml")
    if not source_config.get("providers", {}).get("licensed_history", {}).get("enabled", False):
        return False
    mapping = _yaml(_vietstock_mapping_path())
    request = mapping.get("request", {})
    response = mapping.get("response", {})
    coverage = mapping.get("coverage", {})
    endpoint = str(request.get("endpoint") or "").strip()
    endpoint_scheme = urlparse(endpoint).scheme
    base_scheme = urlparse(os.getenv("VIETSTOCK_HISTORY_ENDPOINT", "").strip()).scheme
    has_endpoint = endpoint_scheme == "https" or (not endpoint_scheme and bool(endpoint) and base_scheme == "https")
    has_auth = bool((request.get("api_key_header") or request.get("api_key_parameter")) and os.getenv("VIETSTOCK_API_TOKEN"))
    stock_fields = response.get("stock_quote_fields", {})
    index_fields = response.get("market_index_fields", {})
    has_stock_mapping = bool(
        stock_fields.get("symbol") and stock_fields.get("exchange_code")
        and stock_fields.get("observed_at")
    )
    has_index_mapping = bool(index_fields.get("code") and index_fields.get("observed_at"))
    required_indices = set(source_config.get("coverage", {}).get("indices", {}).get("required_codes", []))
    required_exchanges = set(source_config.get("coverage", {}).get("securities", {}).get("exchanges", []))
    has_coverage_aliases = (
        required_indices.issubset(set(coverage.get("market_index_aliases", {}).values()))
        and required_exchanges.issubset(set(coverage.get("exchange_aliases", {}).values()))
    )
    has_response_mapping = bool(
        response.get("records_json_path") and response.get("record_trade_date_field")
        and has_stock_mapping and has_index_mapping and has_coverage_aliases
    )
    return bool(has_endpoint and has_auth and request.get("start_date_parameter")
                and request.get("end_date_parameter") and has_response_mapping)


def _window(config: dict | None = None, timezone_name: str = "Asia/Ho_Chi_Minh") -> DateWindow:
    config = config or {}
    today_str = datetime.now(ZoneInfo(timezone_name)).strftime("%Y-%m-%d")
    start = config.get("start_date") or today_str
    end = config.get("end_date") or today_str
    return DateWindow.parse(start, end, timezone_name)


def _collect_rss_source(
    context,
    source_key: str,
    source_config: dict,
    window: DateWindow,
    storage_mode: str = "link_only",
    job_name: str | None = None,
) -> dict:
    if not source_collection_enabled(source_config):
        raise SourceAuthorizationError(
            f"{source_key} is disabled; check its internal-only RSS access policy."
        )

    writer = Node2NewsWriter(storage_mode=storage_mode)
    client = RssNewsClient(source_config, security_matcher=writer.matcher)
    audit = ScrapeAuditWriter()
    source_code = source_config["source_code"]
    audit_run_id = f"{context.run_id}:{source_code}"[:64]
    db_run_id = audit.start_run(
        source_code=source_code,
        source_kind="news",
        display_name=source_config["name"],
        base_url=source_config["base_url"],
        dagster_run_id=audit_run_id,
        job_name=job_name or context.job_name,
        start_date=window.start_date,
        end_date=window.end_date,
        config={
            "collection_method": "rss_only",
            "storage_mode": storage_mode,
            "terms_url": source_config.get("terms_url"),
            "redistribution_allowed": source_config.get("access", {}).get("redistribution_allowed", False),
        },
    )
    total_discovered = 0
    total_persisted = 0
    days_completed = 0

    def persist_day(batch_date: date, articles: list[Article], category_code: str | None = None) -> None:
        nonlocal total_discovered, total_persisted, days_completed
        total_discovered += len(articles)
        article_uids = writer.persist_articles(articles)
        total_persisted += len(article_uids)
        days_completed += 1
        audit.save_items(db_run_id, source_code, articles, article_uids)
        audit.save_checkpoint(source_code, "last_completed_day", {
            "completed_day": batch_date.isoformat(),
            "articles_persisted": len(article_uids),
            "total_persisted_so_far": total_persisted,
        })
        context.log.info(
            f"{source_key} RSS persisted {len(article_uids)} article records for {batch_date} "
            f"(cumulative: {total_persisted})"
        )

    async def execute_collection():
        try:
            return await client.collect(window, on_batch=persist_day)
        finally:
            await client.close()

    try:
        articles = asyncio.run(execute_collection())
        stats = client.collection_stats
        audit.save_attempts(db_run_id, source_code, client.fetch_attempts)
        coverage = str(stats.get("coverage_status", "RSS_WINDOW_NOT_PROVEN"))
        run_status = "PARTIAL" if coverage == "RSS_RETENTION_LIMITED" else "SUCCESS"
        audit.finish_run(
            db_run_id,
            run_status,
            discovered=total_discovered,
            persisted=total_persisted,
            failed=0,
        )
        audit.save_checkpoint(source_code, "last_completed_window", {
            "start_date": window.start_date.isoformat(),
            "end_date": window.end_date.isoformat(),
            "persisted_articles": total_persisted,
            "days_completed": days_completed,
            "coverage_status": coverage,
            "status": run_status,
        })
        if run_status == "PARTIAL":
            context.log.warning(
                f"{source_key} RSS only retains items from {stats.get('oldest_feed_publication_date')}; "
                f"requested start {window.start_date.isoformat()} is not covered."
            )
        context.log.info(
            f"{source_key} RSS collection finished: {total_persisted} stored, "
            f"coverage={coverage}, window={window.start_date}..{window.end_date}"
        )
        return {
            "source": source_key,
            "run_status": run_status,
            "articles_discovered": total_discovered,
            "articles_persisted": total_persisted,
            "days_completed": days_completed,
            "collection_stats": stats,
        }
    except Exception as exc:
        audit.save_attempts(db_run_id, source_code, client.fetch_attempts)
        audit.finish_run(
            db_run_id,
            "FAILED",
            discovered=total_discovered,
            persisted=total_persisted,
            failed=1,
            error=str(exc)[:4000],
        )
        audit.save_checkpoint(source_code, "last_interrupted_run", {
            "start_date": window.start_date.isoformat(),
            "end_date": window.end_date.isoformat(),
            "error": str(exc)[:500],
            "fetch_attempts": len(client.fetch_attempts),
            "persisted_before_failure": total_persisted,
        })
        raise


def _news_source_config(source_key: str) -> dict:
    if source_key not in {*FINANCIAL_NEWS_SOURCES, *CONFIGURED_NEWS_SOURCES, "cafef"}:
        raise ValueError(f"Unknown configured news source: {source_key!r}")
    return _yaml(CONFIG_DIR / f"sources/news/{source_key}.yaml")


def _collect_html_backfill(
    context,
    source_key: str,
    source_config: dict,
    window: DateWindow,
    storage_mode: str = "link_only",
) -> dict:
    if not source_collection_enabled(source_config):
        raise SourceAuthorizationError(
            f"{source_key} is disabled; check its internal-only HTML backfill access policy."
        )

    writer = Node2NewsWriter(storage_mode=storage_mode)
    if source_key == "cafef":
        client = CafeFClient(source_config, security_matcher=writer.matcher)
    elif source_key == "baomoi":
        client = BaoMoiClient(source_config, security_matcher=writer.matcher)
    elif source_config.get("discovery", {}).get("html_backfill", {}).get("strategy") == "sitemap":
        client = SitemapHtmlNewsClient(source_config, security_matcher=writer.matcher)
    else:
        client = ConfiguredHtmlNewsClient(source_config, security_matcher=writer.matcher)

    audit = ScrapeAuditWriter()
    source_code = source_config["source_code"]
    job_name = str(context.job_name)
    run_row_id = audit.start_run(
        source_code=source_code,
        source_kind="news",
        display_name=source_config["name"],
        base_url=source_config["base_url"],
        dagster_run_id=f"{context.run_id}:{source_code}"[:64],
        job_name=job_name,
        start_date=window.start_date,
        end_date=window.end_date,
        config={
            "collection_method": "html_archive_backfill",
            "archive_strategy": source_config.get("discovery", {}).get("html_backfill", {}).get("strategy", "category_archive"),
            "internal_only": source_config.get("access", {}).get("internal_only") is True,
            "redistribution_allowed": source_config.get("access", {}).get("redistribution_allowed") is True,
            "storage_mode": storage_mode,
        },
    )
    total_discovered = 0
    total_persisted = 0
    days_completed = 0

    def persist_day(batch_date: date, articles: list[Article], category_code: str | None = None) -> None:
        nonlocal total_discovered, total_persisted, days_completed
        if not articles:
            return
        total_discovered += len(articles)
        uids = writer.persist_articles(articles)
        total_persisted += len(uids)
        days_completed += 1
        audit.save_items(run_row_id, source_code, articles, uids)
        audit.save_checkpoint(source_code, "last_completed_day", {
            "completed_day": batch_date.isoformat(),
            "category": category_code,
            "articles_persisted": len(uids),
            "total_persisted_so_far": total_persisted,
        })
        context.log.info(
            f"{source_key} HTML backfill persisted {len(uids)} articles for {batch_date} "
            f"(cumulative: {total_persisted})"
        )

    async def execute_collection():
        try:
            return await client.collect(window, on_batch=persist_day, archive_only=True)
        finally:
            await client.close()

    try:
        asyncio.run(execute_collection())
        stats = getattr(client, "collection_stats", {})
        if stats.get("coverage_proven") is False:
            raise RuntimeError(f"{source_key} HTML archive did not prove requested date-window coverage")
        audit.save_attempts(run_row_id, source_code, client.fetch_attempts)
        audit.finish_run(
            run_row_id,
            "SUCCESS",
            discovered=total_discovered,
            persisted=total_persisted,
        )
        audit.save_checkpoint(source_code, "last_completed_window", {
            "start_date": window.start_date.isoformat(),
            "end_date": window.end_date.isoformat(),
            "articles_persisted": total_persisted,
            "days_completed": days_completed,
            "collection_stats": stats,
            "status": "SUCCESS",
        })
        context.log.info(
            f"{source_key} HTML backfill finished: {total_persisted} articles persisted; "
            f"coverage_proven={stats.get('coverage_proven', True)}"
        )
        return {
            "source": source_key,
            "run_status": "SUCCESS",
            "articles_discovered": total_discovered,
            "articles_persisted": total_persisted,
            "days_completed": days_completed,
            "collection_stats": stats,
        }
    except Exception as exc:
        audit.save_attempts(run_row_id, source_code, client.fetch_attempts)
        audit.finish_run(
            run_row_id,
            "FAILED",
            discovered=total_discovered,
            persisted=total_persisted,
            failed=1,
            error=str(exc)[:4000],
        )
        audit.save_checkpoint(source_code, "last_interrupted_run", {
            "start_date": window.start_date.isoformat(),
            "end_date": window.end_date.isoformat(),
            "error": str(exc)[:500],
            "fetch_attempts": len(client.fetch_attempts),
            "persisted_before_failure": total_persisted,
        })
        raise


def _pod_tags(source: str, memory_limit: str = "768Mi") -> dict:
    return {
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "100m", "memory": "256Mi"},
                    "limits": {"cpu": "500m", "memory": memory_limit},
                },
            },
            "pod_spec_config": {"restart_policy": "Never"},
            "pod_template_spec_metadata": {
                "labels": {
                    "app.kubernetes.io/part-of": "finsights-data-scraping",
                    "app.kubernetes.io/component": "source-job",
                    "finsights.io/source": source,
                }
            },
        }
    }


@op(config_schema={
    "start_date": Field(String, is_required=False, description="Inclusive YYYY-MM-DD in Asia/Ho_Chi_Minh (defaults to today)"),
    "end_date": Field(String, is_required=False, description="Inclusive YYYY-MM-DD in Asia/Ho_Chi_Minh (defaults to today)"),
    "storage_mode": Field(String, is_required=False, default_value="link_only", description="link_only (no OCI binary upload) or full_media"),
})
def collect_cafef_news(context):
    source_config = _yaml(CONFIG_DIR / "sources/news/cafef.yaml")
    window = _window(context.op_config, source_config.get("timezone", "Asia/Ho_Chi_Minh"))
    storage_mode = context.op_config.get("storage_mode", "link_only")
    return _collect_rss_source(
        context, "cafef", source_config, window, storage_mode, job_name="cafef_news_job"
    )


@op(config_schema={
    "start_date": Field(String, is_required=False, description="Inclusive YYYY-MM-DD in Asia/Ho_Chi_Minh (defaults to today)"),
    "end_date": Field(String, is_required=False, description="Inclusive YYYY-MM-DD in Asia/Ho_Chi_Minh (defaults to today)"),
    "storage_mode": Field(String, is_required=False, default_value="link_only", description="link_only (no OCI binary upload) or full_media"),
})
def collect_baomoi_news(context):
    source_config = _yaml(CONFIG_DIR / "sources/news/baomoi.yaml")
    window = _window(context.op_config, source_config.get("timezone", "Asia/Ho_Chi_Minh"))
    storage_mode = context.op_config.get("storage_mode", "link_only")
    writer = Node2NewsWriter(storage_mode=storage_mode)
    client = BaoMoiClient(source_config, security_matcher=writer.matcher)
    audit = ScrapeAuditWriter()
    run_row_id = audit.start_run(
        source_code=source_config["source_code"], source_kind="news",
        display_name=source_config["name"], base_url=source_config["base_url"],
        dagster_run_id=context.run_id, job_name="baomoi_news_job",
        start_date=window.start_date, end_date=window.end_date,
        config={"archive_mode": source_config["discovery"]["mode"], "storage_mode": storage_mode},
    )
    total_discovered = 0
    total_persisted = 0
    days_completed = 0

    def _persist_day_batch(batch_date: date, day_articles: list[Article], category_code: str | None = None) -> None:
        nonlocal total_discovered, total_persisted, days_completed
        if not day_articles:
            return
        total_discovered += len(day_articles)
        uids = writer.persist_articles(day_articles)
        total_persisted += len(uids)
        days_completed += 1
        audit.save_items(run_row_id, source_config["source_code"], day_articles, uids)
        audit.save_checkpoint(source_config["source_code"], "last_completed_day", {
            "completed_day": batch_date.isoformat(),
            "category": category_code,
            "articles_persisted": len(uids),
            "total_persisted_so_far": total_persisted,
        })
        cat_prefix = f"[{category_code}] " if category_code else ""
        context.log.info(
            f"Baomoi {cat_prefix}persisted day batch for {batch_date}: {len(uids)} articles (cumulative: {total_persisted})"
        )

    async def _execute_collection():
        try:
            await client.collect(window, on_batch=_persist_day_batch)
        finally:
            await client.close()

    try:
        asyncio.run(_execute_collection())
        audit.save_attempts(run_row_id, source_config["source_code"], client.fetch_attempts)
        audit.finish_run(run_row_id, "SUCCESS", discovered=total_discovered, persisted=total_persisted)
        audit.save_checkpoint(source_config["source_code"], "last_completed_window", {
            "start_date": window.start_date.isoformat(),
            "end_date": window.end_date.isoformat(),
            "persisted_articles": total_persisted,
            "days_completed": days_completed,
            "status": "SUCCESS",
        })
        context.log.info(
            f"Baomoi completed collection: {total_persisted} articles persisted across {days_completed} days",
            extra={
                "start_date": window.start_date.isoformat(),
                "end_date": window.end_date.isoformat(),
                "articles_persisted": total_persisted,
                "days_completed": days_completed,
            },
        )
        return {"articles_persisted": total_persisted, "days_completed": days_completed}
    except Exception as exc:
        audit.save_attempts(run_row_id, source_config["source_code"], client.fetch_attempts)
        audit.finish_run(run_row_id, "FAILED", discovered=total_discovered, persisted=total_persisted, failed=1, error=str(exc)[:4000])
        audit.save_checkpoint(source_config["source_code"], "last_interrupted_run", {
            "start_date": window.start_date.isoformat(),
            "end_date": window.end_date.isoformat(),
            "error": str(exc)[:500],
            "fetch_attempts": len(client.fetch_attempts),
            "persisted_before_failure": total_persisted,
        })
        raise


@op(config_schema={
    "source_key": Field(String, is_required=True, description="Configured source key: thanhnien, tuoitre, vnexpress, or vneconomy"),
    "start_date": Field(String, is_required=False, description="Inclusive YYYY-MM-DD in Asia/Ho_Chi_Minh (defaults to today)"),
    "end_date": Field(String, is_required=False, description="Inclusive YYYY-MM-DD in Asia/Ho_Chi_Minh (defaults to today)"),
    "storage_mode": Field(String, is_required=False, default_value="link_only", description="link_only (no OCI binary upload) or full_media"),
})
def collect_configured_news(context):
    source_key = str(context.op_config["source_key"])
    source_config = _news_source_config(source_key)
    if not source_collection_enabled(source_config):
        raise SourceAuthorizationError(
            f"{source_key} is disabled; enable its personal academic research scope in the source YAML before running."
        )

    window = _window(context.op_config, source_config.get("timezone", "Asia/Ho_Chi_Minh"))
    storage_mode = context.op_config.get("storage_mode", "link_only")
    if source_config.get("discovery", {}).get("mode") == "rss_only":
        return _collect_rss_source(
            context, source_key, source_config, window, storage_mode,
            job_name=f"{source_key}_news_job",
        )

    writer = Node2NewsWriter(storage_mode=storage_mode)
    if source_key == "baomoi":
        client = BaoMoiClient(source_config, security_matcher=writer.matcher)
    else:
        client = ConfiguredHtmlNewsClient(source_config, security_matcher=writer.matcher)
    audit = ScrapeAuditWriter()
    job_name = f"{source_key}_news_job"
    run_row_id = audit.start_run(
        source_code=source_config["source_code"], source_kind="news",
        display_name=source_config["name"], base_url=source_config["base_url"],
        dagster_run_id=context.run_id, job_name=job_name,
        start_date=window.start_date, end_date=window.end_date,
        config={
            "archive_mode": source_config["discovery"]["mode"],
            "collection_scope": source_config.get("use_scope"),
            "binary_asset_collection_enabled": source_config.get("binary_asset_collection_enabled") is True,
            "storage_mode": storage_mode,
        },
    )
    total_discovered = 0
    total_persisted = 0
    days_completed = 0

    def _persist_day_batch(batch_date: date, day_articles: list[Article], category_code: str | None = None) -> None:
        nonlocal total_discovered, total_persisted, days_completed
        if not day_articles:
            return
        total_discovered += len(day_articles)
        uids = writer.persist_articles(day_articles)
        total_persisted += len(uids)
        days_completed += 1
        audit.save_items(run_row_id, source_config["source_code"], day_articles, uids)
        audit.save_checkpoint(source_config["source_code"], "last_completed_day", {
            "completed_day": batch_date.isoformat(),
            "category": category_code,
            "articles_persisted": len(uids),
            "total_persisted_so_far": total_persisted,
        })
        cat_prefix = f"[{category_code}] " if category_code else ""
        context.log.info(
            f"{source_key} {cat_prefix}persisted day batch for {batch_date}: {len(uids)} articles (cumulative: {total_persisted})"
        )

    async def _execute_collection():
        try:
            await client.collect(window, on_batch=_persist_day_batch)
        finally:
            await client.close()

    try:
        asyncio.run(_execute_collection())
        audit.save_attempts(run_row_id, source_config["source_code"], client.fetch_attempts)
        audit.finish_run(run_row_id, "SUCCESS", discovered=total_discovered, persisted=total_persisted)
        audit.save_checkpoint(source_config["source_code"], "last_completed_window", {
            "start_date": window.start_date.isoformat(),
            "end_date": window.end_date.isoformat(),
            "persisted_articles": total_persisted,
            "days_completed": days_completed,
            "status": "SUCCESS",
        })
        context.log.info(f"{source_key} completed exact-range collection", extra={
            "start_date": window.start_date.isoformat(),
            "end_date": window.end_date.isoformat(),
            "articles_persisted": total_persisted,
            "days_completed": days_completed,
        })
        return {"articles_persisted": total_persisted, "days_completed": days_completed}
    except Exception as exc:
        audit.save_attempts(run_row_id, source_config["source_code"], client.fetch_attempts)
        audit.finish_run(run_row_id, "FAILED", discovered=total_discovered, persisted=total_persisted, failed=1, error=str(exc)[:4000])
        audit.save_checkpoint(source_config["source_code"], "last_interrupted_run", {
            "start_date": window.start_date.isoformat(),
            "end_date": window.end_date.isoformat(),
            "error": str(exc)[:500],
            "fetch_attempts": len(client.fetch_attempts),
            "persisted_before_failure": total_persisted,
        })
        raise


@op(config_schema={
    "start_date": Field(String, is_required=False, description="Inclusive YYYY-MM-DD in Asia/Ho_Chi_Minh (defaults to today)"),
    "end_date": Field(String, is_required=False, description="Inclusive YYYY-MM-DD in Asia/Ho_Chi_Minh (defaults to today)"),
    "storage_mode": Field(String, is_required=False, default_value="link_only", description="RSS sources store summaries/links; Baomoi is collected from HTML"),
})
def collect_financial_news_batch(context):
    window = _window(context.op_config)
    storage_mode = context.op_config.get("storage_mode", "link_only")
    results: dict[str, dict] = {}
    errors: dict[str, str] = {}
    for source_key in RSS_FINANCIAL_NEWS_SOURCES:
        source_config = _yaml(CONFIG_DIR / f"sources/news/{source_key}.yaml")
        try:
            results[source_key] = _collect_rss_source(
                context,
                source_key,
                source_config,
                window,
                storage_mode,
                job_name="financial_news_job",
            )
        except Exception as exc:
            errors[source_key] = f"{type(exc).__name__}: {exc}"[:1000]
            context.log.error(f"{source_key} batch failed: {errors[source_key]}")

    summary = {
        "start_date": window.start_date.isoformat(),
        "end_date": window.end_date.isoformat(),
        "sources": results,
        "errors": errors,
    }
    if errors:
        raise RuntimeError(f"Financial News batch had source failures: {errors}")
    return summary


@op(config_schema={
    "source_key": Field(String, is_required=True, description="One of the five Financial News publisher keys"),
    "start_date": Field(String, is_required=True, description="Inclusive YYYY-MM-DD (Asia/Ho_Chi_Minh)"),
    "end_date": Field(String, is_required=True, description="Inclusive YYYY-MM-DD (Asia/Ho_Chi_Minh)"),
    "storage_mode": Field(String, is_required=False, default_value="link_only", description="No binary media download by default"),
})
def collect_news_backfill(context):
    source_key = str(context.op_config["source_key"])
    if source_key not in HTML_BACKFILL_SOURCES:
        raise ValueError(f"Unsupported HTML backfill source: {source_key!r}")
    source_config = _news_source_config(source_key)
    window = _window(context.op_config, source_config.get("timezone", "Asia/Ho_Chi_Minh"))
    return _collect_html_backfill(
        context,
        source_key,
        source_config,
        window,
        context.op_config.get("storage_mode", "link_only"),
    )


@op(config_schema={
    "start_date": Field(String, is_required=False, description="Inclusive provider trading date YYYY-MM-DD (defaults to today)"),
    "end_date": Field(String, is_required=False, description="Inclusive provider trading date YYYY-MM-DD (defaults to today)"),
})
def collect_vietstock_market(context):
    source_config = _yaml(CONFIG_DIR / "sources/market/vietstock.yaml")
    window = _window(context.op_config, source_config.get("timezone", "Asia/Ho_Chi_Minh"))
    licensed_enabled = source_config.get("providers", {}).get("licensed_history", {}).get("enabled", False)
    if licensed_enabled:
        mapping_path = _vietstock_mapping_path()
        mapping = _yaml(mapping_path)
        client = VietstockHistoryClient(source_config, mapping)
        provider_kind = "licensed_history"
        run_config = {"provider": "licensed_history", "mapping_file": str(mapping_path)}
    else:
        client = VietstockPublicBoardClient(source_config)
        provider_kind = "public_board"
        run_config = {"provider": "public_board", "url": client.board_url}

    audit = ScrapeAuditWriter()
    run_row_id = audit.start_run(
        source_code=source_config["source_code"], source_kind="market",
        display_name=source_config["name"], base_url=source_config["website"],
        dagster_run_id=context.run_id, job_name="vietstock_market_job",
        start_date=window.start_date, end_date=window.end_date,
        config=run_config,
    )
    try:
        records = asyncio.run(client.collect(window))
        persisted = Node2MarketWriter().persist_records(records)
        audit.save_items(run_row_id, source_config["source_code"], records)
        audit.save_attempts(run_row_id, source_config["source_code"], client.fetch_attempts)
        audit.finish_run(run_row_id, "SUCCESS", discovered=len(records), persisted=persisted)
        context.log.info(
            f"Vietstock ({provider_kind}) collection completed: {persisted} records persisted",
            extra={"provider": provider_kind, "records_persisted": persisted},
        )
        return {"market_records_persisted": persisted, "provider": provider_kind}
    except Exception as exc:
        audit.save_attempts(run_row_id, source_config["source_code"], client.fetch_attempts)
        audit.finish_run(run_row_id, "FAILED", discovered=0, persisted=0, failed=1, error=str(exc)[:4000])
        raise


@job(name="cafef_news_job", tags=_pod_tags("cafef"), description="CafeF financial-news collection; one isolated K3s run pod.")
def cafef_news_job():
    collect_cafef_news()


@job(name="baomoi_news_job", tags=_pod_tags("baomoi", "512Mi"), description="Baomoi financial-news collection; one isolated K3s run pod.")
def baomoi_news_job():
    collect_baomoi_news()


@job(name="vietstock_market_job", tags=_pod_tags("vietstock", "512Mi"), description="Licensed Vietstock data collection; one isolated K3s run pod.")
def vietstock_market_job():
    collect_vietstock_market()


@job(name="financial_news_job", tags=_pod_tags("financial-news", "1Gi"), description="Batch collect RSS summaries from the four sheet publishers with RSS feeds.")
def financial_news_job():
    collect_financial_news_batch()


def _build_configured_news_job(source_key: str):
    @job(
        name=f"{source_key}_news_job",
        tags=_pod_tags(source_key, "512Mi"),
        description=f"{source_key} financial-news collection; one isolated K3s run pod.",
    )
    def configured_source_job():
        collect_configured_news()

    return configured_source_job


def _build_news_backfill_job(source_key: str):
    @job(
        name=f"{source_key}_news_backfill_job",
        tags=_pod_tags(f"{source_key}-backfill", "1Gi"),
        description=(
            f"Historical HTML financial-news backfill for {source_key}; "
            "date-batched writes to Node 2 in an isolated K3s run pod."
        ),
    )
    def source_backfill_job():
        collect_news_backfill()

    return source_backfill_job


def _build_configured_news_schedule(source_key: str, job_definition, cron_schedule: str):
    source_code = CONFIGURED_NEWS_SOURCES[source_key][0]

    @schedule(
        job=job_definition,
        cron_schedule=cron_schedule,
        execution_timezone="Asia/Ho_Chi_Minh",
        name=f"{source_key}_daily_news_schedule",
    )
    def configured_source_schedule(context):
        source_config = _news_source_config(source_key)
        if not source_collection_enabled(source_config):
            return SkipReason(
                f"{source_key} is disabled; enable its personal academic research scope in configs/sources/news/{source_key}.yaml."
            )
        publication_day = (
            context.scheduled_execution_time.astimezone(LOCAL_TZ).date() - timedelta(days=1)
        ).isoformat()
        return RunRequest(
            run_key=f"{source_code}-{publication_day}",
            run_config={"ops": {"collect_configured_news": {"config": {
                "source_key": source_key,
                "start_date": publication_day,
                "end_date": publication_day,
            }}}},
        )

    return configured_source_schedule


configured_news_jobs = {
    source_key: _build_configured_news_job(source_key)
    for source_key in CONFIGURED_NEWS_SOURCES
}
news_backfill_jobs = {
    source_key: _build_news_backfill_job(source_key)
    for source_key in HTML_BACKFILL_SOURCES
}
configured_news_schedules = {
    source_key: _build_configured_news_schedule(
        source_key, configured_news_jobs[source_key], cron_schedule
    )
    for source_key, (_source_code, cron_schedule) in CONFIGURED_NEWS_SOURCES.items()
}


@schedule(
    job=cafef_news_job,
    cron_schedule="15 2 * * *",
    execution_timezone="Asia/Ho_Chi_Minh",
    name="cafef_daily_news_schedule",
)
def cafef_daily_news_schedule(context):
    publication_day = (
        context.scheduled_execution_time.astimezone(LOCAL_TZ).date() - timedelta(days=1)
    ).isoformat()
    return RunRequest(run_key=f"cafef-news-{publication_day}", run_config={
        "ops": {"collect_cafef_news": {"config": {"start_date": publication_day, "end_date": publication_day}}}
    })


@schedule(
    job=baomoi_news_job,
    cron_schedule="0 3 * * *",
    execution_timezone="Asia/Ho_Chi_Minh",
    name="baomoi_daily_news_schedule",
)
def baomoi_daily_news_schedule(context):
    publication_day = (
        context.scheduled_execution_time.astimezone(LOCAL_TZ).date() - timedelta(days=1)
    ).isoformat()
    return RunRequest(run_key=f"baomoi-news-{publication_day}", run_config={
        "ops": {"collect_baomoi_news": {"config": {"start_date": publication_day, "end_date": publication_day}}}
    })


@schedule(
    job=vietstock_market_job,
    cron_schedule="30 16 * * 1-5",
    execution_timezone="Asia/Ho_Chi_Minh",
    name="vietstock_eod_schedule",
)
def vietstock_eod_schedule(context):
    if not _vietstock_schedule_ready():
        return SkipReason("Vietstock EOD is paused until licensed DataFeed credentials and field mapping are configured.")
    run_day = context.scheduled_execution_time.astimezone(LOCAL_TZ).date().isoformat()
    return RunRequest(run_key=f"vietstock-eod-{run_day}", run_config={
        "ops": {"collect_vietstock_market": {"config": {"start_date": run_day, "end_date": run_day}}}
    })


defs = Definitions(
    jobs=[
        cafef_news_job,
        baomoi_news_job,
        vietstock_market_job,
        financial_news_job,
        *configured_news_jobs.values(),
        *news_backfill_jobs.values(),
    ],
    schedules=[
        cafef_daily_news_schedule,
        baomoi_daily_news_schedule,
        vietstock_eod_schedule,
        *configured_news_schedules.values(),
    ],
)

