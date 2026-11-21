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
from ..persistence import Node2DisclosureWriter, Node2FundamentalsWriter, Node2MarketWriter, Node2NewsWriter, ScrapeAuditWriter
from ..sources.disclosures import (
    InvestorRelationsClient,
    build_client as build_disclosure_client,
)
from ..sources.market import CoPhieu68Client, VietstockHistoryClient, VietstockPublicBoardClient
from ..sources.news import (
    BaoMoiClient,
    CafeFClient,
    ConfiguredHtmlNewsClient,
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
HTML_BACKFILL_SOURCES = FINANCIAL_NEWS_SOURCES
OFFICIAL_DISCLOSURE_SOURCES = {
    "hose": ("hose_disclosures", "0 7,15 * * 1-5", "hose_disclosures_job", "512Mi"),
    "hnx": ("hnx_disclosures", "30 2 * * 1-5", "hnx_disclosures_job", "768Mi"),
    "ubcknn": ("ubcknn_notices", "0 3 * * 1-5", "ubcknn_notices_job", "1Gi"),
    "vsdc": ("vsdc_corporate_actions", "30 3 * * 1-5", "vsdc_corporate_actions_job", "1Gi"),
}
INVESTOR_RELATIONS_SOURCES = {
    "banks": ("ir_banks", "ir_banks_job", "0 5 * * *", "2Gi"),
    "steel_materials": ("ir_steel_materials", "ir_steel_materials_job", "30 5 * * *", "2Gi"),
    "other_sectors": ("ir_other_sectors", "ir_other_sectors_job", "0 6 * * *", "2Gi"),
}


def _disclosure_config(source_key: str) -> dict:
    if source_key not in OFFICIAL_DISCLOSURE_SOURCES:
        raise ValueError(f"Unknown official disclosure source: {source_key!r}")
    return _yaml(CONFIG_DIR / f"sources/disclosures/{source_key}.yaml")


def _investor_relations_config(source_key: str) -> dict:
    if source_key not in INVESTOR_RELATIONS_SOURCES:
        raise ValueError(f"Unknown investor relations source group: {source_key!r}")
    key = INVESTOR_RELATIONS_SOURCES[source_key][0]
    config = _yaml(CONFIG_DIR / f"sources/investor_relations/{key.removeprefix('ir_')}.yaml")
    if not config.get("companies"):
        raise ValueError(f"No companies configured for investor relations source group {source_key!r}")
    return config


def _disclosure_client(source_key: str, source_config: dict):
    return build_disclosure_client(source_key, source_config)


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
            f"{source_key} is disabled; check its licensed HTML redistribution policy."
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
    # Node 2 currently has intermittent pod DNS failures when resolving Dagster's
    # PostgreSQL service. Keep scraper workers on the healthy control-plane node
    # until the cluster networking issue is corrected; operators may override it.
    run_node = os.getenv("DAGSTER_SCRAPER_RUN_NODE", "finsights-prod-node-01").strip()
    pod_spec_config = {"restart_policy": "Never"}
    if run_node:
        pod_spec_config["node_selector"] = {"kubernetes.io/hostname": run_node}
    return {
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "100m", "memory": "256Mi"},
                    "limits": {"cpu": "500m", "memory": memory_limit},
                },
            },
            "pod_spec_config": pod_spec_config,
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
    return _collect_html_backfill(
        context, "cafef", source_config, window, storage_mode
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
    "source_key": Field(String, is_required=True, description="Configured source key: thanhnien, tuoitre, vnexpress, vneconomy, or baomoi"),
    "start_date": Field(String, is_required=False, description="Inclusive YYYY-MM-DD in Asia/Ho_Chi_Minh (defaults to today)"),
    "end_date": Field(String, is_required=False, description="Inclusive YYYY-MM-DD in Asia/Ho_Chi_Minh (defaults to today)"),
    "storage_mode": Field(String, is_required=False, default_value="link_only", description="link_only (no OCI binary upload) or full_media"),
})
def collect_configured_news(context):
    source_key = str(context.op_config["source_key"])
    source_config = _news_source_config(source_key)
    if not source_collection_enabled(source_config):
        raise SourceAuthorizationError(
            f"{source_key} is disabled; enable its licensed redistribution policy in the source YAML before running."
        )

    window = _window(context.op_config, source_config.get("timezone", "Asia/Ho_Chi_Minh"))
    storage_mode = context.op_config.get("storage_mode", "link_only")

    writer = Node2NewsWriter(storage_mode=storage_mode)
    if source_key == "baomoi":
        client = BaoMoiClient(source_config, security_matcher=writer.matcher)
    elif (
        source_key in {"thanhnien", "tuoitre"}
        and source_config.get("discovery", {}).get("html_backfill", {}).get("strategy") == "sitemap"
    ):
        client = SitemapHtmlNewsClient(source_config, security_matcher=writer.matcher)
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
    "storage_mode": Field(String, is_required=False, default_value="link_only", description="Store licensed HTML article bodies; binary media stays link-only"),
})
def collect_financial_news_batch(context):
    window = _window(context.op_config)
    storage_mode = context.op_config.get("storage_mode", "link_only")
    results: dict[str, dict] = {}
    errors: dict[str, str] = {}
    for source_key in FINANCIAL_NEWS_SOURCES:
        source_config = _yaml(CONFIG_DIR / f"sources/news/{source_key}.yaml")
        try:
            results[source_key] = _collect_html_backfill(
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


@op(config_schema={
    "start_date": Field(String, is_required=False, description="Inclusive price history start date YYYY-MM-DD (defaults to today)"),
    "end_date": Field(String, is_required=False, description="Inclusive price history end date YYYY-MM-DD (defaults to today)"),
})
def collect_cophieu68_market(context):
    source_config = _yaml(CONFIG_DIR / "sources/market/cophieu68.yaml")
    window = _window(context.op_config, source_config.get("timezone", "Asia/Ho_Chi_Minh"))
    market_writer = Node2MarketWriter()
    securities = market_writer.list_active_securities()
    audit = ScrapeAuditWriter()
    source_code = str(source_config["source_code"])
    run_row_id = audit.start_run(
        source_code=source_code,
        source_kind="market",
        display_name=source_config["name"],
        base_url=source_config["base_url"],
        dagster_run_id=context.run_id,
        job_name="cophieu68_market_job",
        start_date=window.start_date,
        end_date=window.end_date,
        config={"price_date_window": [window.start_date.isoformat(), window.end_date.isoformat()],
                "financial_history": "all periods published by source",
                "symbols": [item["symbol"] for item in securities]},
    )
    client = CoPhieu68Client(source_config)
    market_records = []
    period_records = []
    persisted_market = 0
    persisted_periods = 0
    persisted_values = 0
    finished = False
    try:
        market_records, period_records = client.collect(window, securities)
        persisted_market = market_writer.persist_daily_history(market_records)
        latest_by_symbol = {}
        for record in market_records:
            current = latest_by_symbol.get(record.symbol_or_index)
            if current is None or record.trade_date > current.trade_date:
                latest_by_symbol[record.symbol_or_index] = record
        if latest_by_symbol:
            market_writer.persist_records(list(latest_by_symbol.values()))
        persisted_periods, persisted_values = Node2FundamentalsWriter(
            market_engine=market_writer.engine,
        ).persist(period_records)
        audit.save_items(run_row_id, source_code, [*market_records, *period_records])
        audit.save_attempts(run_row_id, source_code, client.fetch_attempts)
        discovered = len(market_records) + len(period_records)
        persisted = persisted_market + persisted_periods
        status = "PARTIAL" if client.symbol_errors else "SUCCESS"
        errors = "; ".join(f"{symbol}: {error}" for symbol, error in sorted(client.symbol_errors.items()))
        audit.finish_run(
            run_row_id, status, discovered=discovered, persisted=persisted,
            failed=len(client.symbol_errors), error=errors or None,
        )
        finished = True
        summary = {
            "symbols_configured": len(securities),
            "symbols_failed": client.symbol_errors,
            "price_rows": persisted_market,
            "financial_periods": persisted_periods,
            "financial_values": persisted_values,
            "requested_price_window": [window.start_date.isoformat(), window.end_date.isoformat()],
        }
        context.log.info(f"CoPhieu68 collection summary: {json.dumps(summary, ensure_ascii=False)}")
        if client.symbol_errors:
            raise RuntimeError(f"CoPhieu68 completed with symbol failures: {errors[:3000]}")
        return summary
    except Exception as exc:
        if not finished:
            try:
                audit.save_attempts(run_row_id, source_code, client.fetch_attempts)
                discovered = len(market_records) + len(period_records)
                persisted = persisted_market + persisted_periods
                status = "PARTIAL" if persisted else "FAILED"
                audit.finish_run(
                    run_row_id, status, discovered=discovered, persisted=persisted,
                    failed=max(len(client.symbol_errors), 1), error=str(exc)[:4000],
                )
            except Exception as audit_exc:
                context.log.error(f"Could not finalize CoPhieu68 audit run: {audit_exc}")
        raise


@op(config_schema={
    "source_key": Field(String, is_required=True, description="hose, hnx, ubcknn, or vsdc"),
    "start_date": Field(String, is_required=False, description="Inclusive YYYY-MM-DD in Asia/Ho_Chi_Minh (defaults to today)"),
    "end_date": Field(String, is_required=False, description="Inclusive YYYY-MM-DD in Asia/Ho_Chi_Minh (defaults to today)"),
})
def collect_official_disclosures(context):
    source_key = str(context.op_config["source_key"])
    source_config = _disclosure_config(source_key)
    window = _window(context.op_config, source_config.get("timezone", "Asia/Ho_Chi_Minh"))
    source_code = str(source_config["source_code"])
    job_name = f"{source_key}_disclosures_job" if source_key in {"hose", "hnx"} else (
        "ubcknn_notices_job" if source_key == "ubcknn" else "vsdc_corporate_actions_job"
    )
    audit = ScrapeAuditWriter()
    run_id = audit.start_run(
        source_code=source_code,
        source_kind="disclosure",
        display_name=str(source_config["name"]),
        base_url=str(source_config["base_url"]),
        dagster_run_id=f"{context.run_id}:{source_code}"[:64],
        job_name=job_name,
        start_date=window.start_date,
        end_date=window.end_date,
        config={
            "source_key": source_key,
            "window_timezone": window.timezone_name,
            "rights_status": source_config.get("rights_status", "link_only"),
            "attachment_policy": "record official URLs; do not mirror binaries until storage/rights are configured",
        },
    )
    client = None
    writer = None
    discovered = 0
    persisted = 0
    try:
        client = _disclosure_client(source_key, source_config)
        records = client.collect(window)
        discovered = len(records)
        writer = Node2DisclosureWriter(
            attachment_storage_mode=str(source_config.get("attachment_storage_mode", "link_only")),
            tls_extra_ca_file=source_config.get("tls_extra_ca_file"),
        )
        destination_uids = writer.persist_records(records)
        persisted = len(destination_uids)
        audit.save_items(run_id, source_code, records, destination_uids)
        client.fetch_attempts.extend(writer.attachment_attempts)
        audit.save_attempts(run_id, source_code, client.fetch_attempts)
        audit.finish_run(run_id, "SUCCESS", discovered=discovered, persisted=persisted)
        audit.save_checkpoint(source_code, "last_completed_window", {
            "start_date": window.start_date.isoformat(),
            "end_date": window.end_date.isoformat(),
            "items_discovered": discovered,
            "items_persisted": persisted,
            "status": "SUCCESS",
        })
        context.log.info(
            f"{source_key} disclosures: {persisted} rows persisted to Node 2 finsights_disclosures "
            f"for {window.start_date}..{window.end_date}; attachments uploaded="
            f"{writer.attachments_uploaded}, pending={writer.attachments_pending}, "
            f"failed={writer.attachments_failed}"
        )
        return {
            "source": source_key,
            "start_date": window.start_date.isoformat(),
            "end_date": window.end_date.isoformat(),
            "items_discovered": discovered,
            "items_persisted": persisted,
            "attachments_uploaded": writer.attachments_uploaded,
            "attachments_pending": writer.attachments_pending,
            "attachments_failed": writer.attachments_failed,
        }
    except Exception as exc:
        if client is not None:
            try:
                audit.save_attempts(run_id, source_code, client.fetch_attempts)
            except Exception:
                pass
        try:
            audit.finish_run(
                run_id, "FAILED", discovered=discovered, persisted=persisted,
                failed=max(1, discovered - persisted), error=str(exc)[:4000],
            )
            audit.save_checkpoint(source_code, "last_interrupted_run", {
                "start_date": window.start_date.isoformat(),
                "end_date": window.end_date.isoformat(),
                "error": str(exc)[:500],
            })
        except Exception as audit_exc:
            context.log.error(f"Could not finish Node 2 scrape audit after source failure: {audit_exc}")
        raise
    finally:
        if client is not None:
            client.close()
        if writer is not None:
            writer.close()


@op(config_schema={
    "source_key": Field(String, is_required=True, description="banks, steel_materials, or other_sectors"),
    "start_date": Field(String, is_required=False, default_value="2026-01-01", description="Inclusive YYYY-MM-DD (defaults to 2026-01-01 for the initial IR backfill)"),
    "end_date": Field(String, is_required=False, description="Inclusive YYYY-MM-DD in Asia/Ho_Chi_Minh (defaults to today)"),
    "attachment_storage_mode": Field(String, is_required=False, default_value="oci", description="oci uploads official IR documents to private OCI Object Storage; link_only stores links and metadata"),
})
def collect_investor_relations(context):
    source_key = str(context.op_config["source_key"])
    group_config = _investor_relations_config(source_key)
    window = _window(context.op_config, group_config.get("timezone", "Asia/Ho_Chi_Minh"))
    attachment_storage_mode = context.op_config.get("attachment_storage_mode", "oci")
    audit = ScrapeAuditWriter()
    writer = Node2DisclosureWriter(attachment_storage_mode=attachment_storage_mode)
    source_results: dict[str, dict] = {}
    source_errors: dict[str, str] = {}

    try:
        for company in group_config["companies"]:
            symbol = str(company["symbol"]).upper()
            source_code = str(company.get("source_code") or f"ir_{symbol.casefold()}")
            listing_urls = [str(url) for url in company["listing_urls"]]
            run_row_id = audit.start_run(
                source_code=source_code,
                source_kind="disclosure",
                display_name=f"{company['company_name']} Investor Relations",
                base_url=listing_urls[0],
                dagster_run_id=f"{context.run_id}:{source_code}"[:64],
                job_name=context.job_name,
                start_date=window.start_date,
                end_date=window.end_date,
                config={
                    "source_group": group_config["source_group"],
                    "collection_method": "official_ir_html",
                    "attachment_storage_mode": attachment_storage_mode,
                    "rights_status": "link_only",
                },
            )
            client = InvestorRelationsClient(group_config, company)
            uploaded_before = writer.attachments_uploaded
            pending_before = writer.attachments_pending
            failed_before = writer.attachments_failed
            try:
                records = client.collect(window)
                destination_uids = writer.persist_records(records)
                audit.save_items(run_row_id, source_code, records, destination_uids)
                audit.save_attempts(run_row_id, source_code, client.fetch_attempts)
                stats = dict(client.collection_stats)
                attachments_uploaded = writer.attachments_uploaded - uploaded_before
                attachments_pending = writer.attachments_pending - pending_before
                attachments_failed = writer.attachments_failed - failed_before
                status = "PARTIAL" if (
                    stats.get("page_errors") or attachments_pending or attachments_failed
                ) else "SUCCESS"
                audit.finish_run(
                    run_row_id,
                    status,
                    discovered=len(records),
                    persisted=len(destination_uids),
                    failed=int(stats.get("page_errors", 0)) + attachments_pending + attachments_failed,
                )
                audit.save_checkpoint(source_code, "last_completed_window", {
                    "start_date": window.start_date.isoformat(),
                    "end_date": window.end_date.isoformat(),
                    "items_discovered": len(records),
                    "items_persisted": len(destination_uids),
                    "pages_visited": stats.get("pages_visited", 0),
                    "undated_items": stats.get("undated_items", 0),
                    "attachments_uploaded": attachments_uploaded,
                    "attachments_pending": attachments_pending,
                    "attachments_failed": attachments_failed,
                    "status": status,
                })
                source_results[source_code] = {
                    "symbol": symbol,
                    "items_discovered": len(records),
                    "items_persisted": len(destination_uids),
                    "pages_visited": stats.get("pages_visited", 0),
                    "page_errors": stats.get("page_errors", 0),
                    "undated_items": stats.get("undated_items", 0),
                    "attachments_uploaded": attachments_uploaded,
                    "attachments_pending": attachments_pending,
                    "attachments_failed": attachments_failed,
                    "status": status,
                }
                context.log.info(
                    f"{source_code}: {len(destination_uids)} disclosure records; "
                    f"pages={stats.get('pages_visited', 0)}, OCI uploaded={attachments_uploaded}, "
                    f"pending={attachments_pending}, failed={attachments_failed}, status={status}"
                )
            except Exception as exc:
                source_errors[source_code] = f"{type(exc).__name__}: {exc}"[:1000]
                try:
                    audit.save_attempts(run_row_id, source_code, client.fetch_attempts)
                    audit.finish_run(
                        run_row_id,
                        "FAILED",
                        discovered=0,
                        persisted=0,
                        failed=1,
                        error=source_errors[source_code],
                    )
                except Exception as audit_exc:
                    context.log.error(f"Could not finalize {source_code} audit: {type(audit_exc).__name__}")
                context.log.error(f"{source_code} failed: {source_errors[source_code]}")
            finally:
                client.close()
    finally:
        writer.close()

    summary = {
        "source_group": group_config["source_group"],
        "start_date": window.start_date.isoformat(),
        "end_date": window.end_date.isoformat(),
        "attachment_storage_mode": attachment_storage_mode,
        "sources": source_results,
        "errors": source_errors,
    }
    context.log.info(f"Investor relations batch summary: {json.dumps(summary, ensure_ascii=False)}")
    if source_errors:
        raise RuntimeError(f"Investor relations source failures: {source_errors}")
    return summary


@job(name="cafef_news_job", tags=_pod_tags("cafef"), description="CafeF financial-news collection; one isolated K3s run pod.")
def cafef_news_job():
    collect_cafef_news()


@job(name="baomoi_news_job", tags=_pod_tags("baomoi", "512Mi"), description="Baomoi financial-news collection; one isolated K3s run pod.")
def baomoi_news_job():
    collect_baomoi_news()


@job(name="vietstock_market_job", tags=_pod_tags("vietstock", "512Mi"), description="Licensed Vietstock data collection; one isolated K3s run pod.")
def vietstock_market_job():
    collect_vietstock_market()


@job(name="cophieu68_market_job", tags=_pod_tags("cophieu68-market", "1Gi"), description="CoPhieu68 daily prices, foreign volumes, statements and ratios into Node 2.")
def cophieu68_market_job():
    collect_cophieu68_market()


@job(name="financial_news_job", tags=_pod_tags("financial-news", "1Gi"), description="Batch collect full licensed HTML articles from the five Financial News publishers.")
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


def _build_official_disclosure_job(source_key: str):
    _source_code, _cron, job_name, memory_limit = OFFICIAL_DISCLOSURE_SOURCES[source_key]

    @job(
        name=job_name,
        tags=_pod_tags(f"{source_key}-disclosures", memory_limit),
        description=f"Collect official {source_key.upper()} disclosures into Node 2 finsights_disclosures.",
    )
    def official_disclosure_job():
        collect_official_disclosures()

    return official_disclosure_job


def _build_official_disclosure_schedule(source_key: str, job_definition):
    source_code, cron_schedule, _job_name, _memory_limit = OFFICIAL_DISCLOSURE_SOURCES[source_key]

    @schedule(
        job=job_definition,
        cron_schedule=cron_schedule,
        execution_timezone="Asia/Ho_Chi_Minh",
        name=f"{source_key}_official_disclosures_schedule",
    )
    def official_disclosure_schedule(context):
        source_config = _disclosure_config(source_key)
        if source_config.get("collection_enabled", True) is False:
            return SkipReason(f"{source_key} disclosure collection is disabled in source config")
        publication_day = (
            context.scheduled_execution_time.astimezone(LOCAL_TZ).date() - timedelta(days=1)
        ).isoformat()
        execution_slot = context.scheduled_execution_time.astimezone(LOCAL_TZ).strftime("%H%M")
        op_name = "collect_official_disclosures"
        return RunRequest(
            run_key=f"{source_code}-{publication_day}-{execution_slot}",
            run_config={"ops": {op_name: {"config": {
                "source_key": source_key,
                "start_date": publication_day,
                "end_date": publication_day,
            }}}},
        )

    return official_disclosure_schedule


def _build_investor_relations_job(source_key: str):
    source_group, job_name, _cron, memory_limit = INVESTOR_RELATIONS_SOURCES[source_key]

    @job(
        name=job_name,
        tags=_pod_tags(source_group, memory_limit),
        description=f"Collect public investor-relations disclosures and archive linked documents for {source_key} issuers.",
    )
    def investor_relations_job():
        collect_investor_relations()

    return investor_relations_job


def _build_investor_relations_schedule(source_key: str, job_definition):
    source_group, _job_name, cron_schedule, _memory_limit = INVESTOR_RELATIONS_SOURCES[source_key]

    @schedule(
        job=job_definition,
        cron_schedule=cron_schedule,
        execution_timezone="Asia/Ho_Chi_Minh",
        name=f"{source_key}_investor_relations_schedule",
    )
    def investor_relations_schedule(context):
        publication_day = context.scheduled_execution_time.astimezone(LOCAL_TZ).date() - timedelta(days=1)
        return RunRequest(
            run_key=f"{source_group}-{publication_day.isoformat()}",
            run_config={"ops": {"collect_investor_relations": {"config": {
                "source_key": source_key,
                "start_date": publication_day.isoformat(),
                "end_date": publication_day.isoformat(),
                "attachment_storage_mode": "oci",
            }}}},
        )

    return investor_relations_schedule


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
                f"{source_key} is disabled; enable its licensed HTML redistribution policy in configs/sources/news/{source_key}.yaml."
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
official_disclosure_jobs = {
    source_key: _build_official_disclosure_job(source_key)
    for source_key in OFFICIAL_DISCLOSURE_SOURCES
}
official_disclosure_schedules = {
    source_key: _build_official_disclosure_schedule(
        source_key, official_disclosure_jobs[source_key]
    )
    for source_key in OFFICIAL_DISCLOSURE_SOURCES
}
investor_relations_jobs = {
    source_key: _build_investor_relations_job(source_key)
    for source_key in INVESTOR_RELATIONS_SOURCES
}
investor_relations_schedules = {
    source_key: _build_investor_relations_schedule(
        source_key, investor_relations_jobs[source_key]
    )
    for source_key in INVESTOR_RELATIONS_SOURCES
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


@schedule(
    job=cophieu68_market_job,
    cron_schedule="30 17 * * 1-5",
    execution_timezone="Asia/Ho_Chi_Minh",
    name="cophieu68_daily_schedule",
)
def cophieu68_daily_schedule(context):
    run_day = context.scheduled_execution_time.astimezone(LOCAL_TZ).date().isoformat()
    return RunRequest(run_key=f"cophieu68-eod-{run_day}", run_config={
        "ops": {"collect_cophieu68_market": {"config": {"start_date": run_day, "end_date": run_day}}}
    })


defs = Definitions(
    jobs=[
        cafef_news_job,
        baomoi_news_job,
        vietstock_market_job,
        cophieu68_market_job,
        financial_news_job,
        *configured_news_jobs.values(),
        *news_backfill_jobs.values(),
        *official_disclosure_jobs.values(),
        *investor_relations_jobs.values(),
    ],
    schedules=[
        cafef_daily_news_schedule,
        baomoi_daily_news_schedule,
        vietstock_eod_schedule,
        cophieu68_daily_schedule,
        *configured_news_schedules.values(),
        *official_disclosure_schedules.values(),
        *investor_relations_schedules.values(),
    ],
)

