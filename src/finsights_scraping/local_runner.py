from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
from pathlib import Path
from typing import Any

import yaml

from .domain.windows import DateWindow
from .persistence.local_writer import LocalDataWriter
from .persistence.security_matching import SecurityAliasMatcher
from .sources.market.cophieu68 import CoPhieu68Client
from .sources.market.vietstock import VietstockPublicBoardClient
from .sources.news.baomoi import BaoMoiClient
from .sources.news.cafef import CafeFClient
from .sources.news.configured_html import ConfiguredHtmlNewsClient
from .sources.news.sitemap_html import SitemapHtmlNewsClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("finsights.local_runner")

ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = ROOT / "configs"

FINANCIAL_NEWS_SOURCES = ["cafef", "tuoitre", "thanhnien", "vnexpress", "baomoi"]
OPTIONAL_NEWS_SOURCES: list[str] = []
LEGACY_SOURCES = ["vietstock", "cophieu68"]
ALL_SOURCES = FINANCIAL_NEWS_SOURCES
SOURCE_CHOICES = [*FINANCIAL_NEWS_SOURCES, *OPTIONAL_NEWS_SOURCES, *LEGACY_SOURCES]


def load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as stream:
        data = yaml.safe_load(stream)
    if not isinstance(data, dict):
        raise ValueError(f"Expected a YAML mapping in {path}")
    return data


class LocalRunner:
    def __init__(self, data_dir: Path | str = ROOT / "data"):
        self.data_dir = Path(data_dir)
        self.writer = LocalDataWriter(self.data_dir)
        alias_file = CONFIG_DIR / "reference/financial-news-universe.yaml"
        self.matcher = SecurityAliasMatcher(
            market_engine=None,
            alias_file=alias_file if alias_file.exists() else None,
        )

    async def run_news_source(
        self,
        source_key: str,
        start_date: str,
        end_date: str,
        collection_method: str = "latest",
    ) -> dict[str, Any]:
        logger.info(f"==> Starting local collection for news source: {source_key} [{start_date}..{end_date}]")
        start_ts = time.time()
        cfg_path = CONFIG_DIR / f"sources/news/{source_key}.yaml"
        if not cfg_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {cfg_path}")
        config = load_yaml(cfg_path)

        window = DateWindow.parse(start_date, end_date, config.get("timezone", "Asia/Ho_Chi_Minh"))

        if collection_method == "html_backfill":
            if source_key not in FINANCIAL_NEWS_SOURCES:
                raise ValueError(f"HTML backfill is not configured for {source_key}")
            strategy = config.get("discovery", {}).get("html_backfill", {}).get("strategy")
            if source_key == "baomoi":
                client = BaoMoiClient(config, security_matcher=self.matcher)
            elif source_key == "cafef":
                client = CafeFClient(config, security_matcher=self.matcher)
            elif strategy == "sitemap":
                client = SitemapHtmlNewsClient(config, security_matcher=self.matcher)
            else:
                client = ConfiguredHtmlNewsClient(config, security_matcher=self.matcher)
        elif source_key == "cafef":
            client = CafeFClient(config, security_matcher=self.matcher)
        elif source_key == "baomoi":
            client = BaoMoiClient(config, security_matcher=self.matcher)
        else:
            client = ConfiguredHtmlNewsClient(config, security_matcher=self.matcher)

        try:
            if collection_method == "html_backfill":
                articles = await client.collect(window, archive_only=True)
            else:
                articles = await client.collect(window)
            elapsed = time.time() - start_ts
            window_key = start_date if start_date == end_date else f"{start_date}_to_{end_date}"
            collection_stats = getattr(client, "collection_stats", {})
            if collection_method == "html_backfill" and collection_stats.get("coverage_proven") is False:
                raise RuntimeError(f"{source_key} archive did not prove the requested date coverage")
            out_file = self.writer.save_articles(
                source_key,
                window_key,
                articles,
                client.fetch_attempts,
                collection_stats=collection_stats,
                start_date=start_date,
                end_date=end_date,
            )
            partial = collection_stats.get("coverage_status") == "RSS_RETENTION_LIMITED"
            logger.info(
                f"[{ 'PARTIAL' if partial else 'SUCCESS' }] {source_key}: collected {len(articles)} "
                f"{collection_method} items in {elapsed:.1f}s -> {out_file}"
            )
            return {
                "source": source_key,
                "status": "PARTIAL" if partial else "SUCCESS",
                "kind": "news",
                "collection_method": collection_method,
                "start_date": start_date,
                "end_date": end_date,
                "articles_count": len(articles),
                "collection_stats": collection_stats,
                "elapsed_seconds": round(elapsed, 2),
                "output_path": str(out_file),
            }
        except Exception as exc:
            elapsed = time.time() - start_ts
            logger.error(f"[FAILED] {source_key} scraping failed after {elapsed:.1f}s: {exc}", exc_info=True)
            return {
                "source": source_key,
                "status": "FAILED",
                "kind": "news",
                "start_date": start_date,
                "end_date": end_date,
                "error": str(exc),
                "elapsed_seconds": round(elapsed, 2),
            }
        finally:
            await client.close()

    async def run_vietstock(self, date_str: str) -> dict[str, Any]:
        logger.info(f"==> Starting local scraping for: vietstock on {date_str}")
        start_ts = time.time()
        results: dict[str, Any] = {
            "source": "vietstock",
            "date": date_str,
            "status": "SUCCESS",
            "market_records_count": 0,
            "articles_count": 0,
        }

        # 1. Market Indices Snapshot
        try:
            mkt_cfg_path = CONFIG_DIR / "sources/market/vietstock.yaml"
            mkt_cfg = load_yaml(mkt_cfg_path) if mkt_cfg_path.exists() else {}
            window = DateWindow.parse(date_str, date_str, mkt_cfg.get("timezone", "Asia/Ho_Chi_Minh"))
            mkt_client = VietstockPublicBoardClient(mkt_cfg)
            records = await mkt_client.collect(window)
            out_mkt = self.writer.save_market_records("vietstock", date_str, records, mkt_client.fetch_attempts)
            logger.info(f"[SUCCESS] vietstock market: collected {len(records)} index snapshots -> {out_mkt}")
            results["market_records_count"] = len(records)
            results["market_output_path"] = str(out_mkt)
        except Exception as exc:
            logger.warning(f"[WARNING] vietstock market index extraction failed: {exc}")
            results["market_error"] = str(exc)

        # 2. Vietstock News Articles
        news_cfg_path = CONFIG_DIR / "sources/news/vietstock.yaml"
        if news_cfg_path.exists():
            news_cfg = load_yaml(news_cfg_path)
            window = DateWindow.parse(date_str, date_str, news_cfg.get("timezone", "Asia/Ho_Chi_Minh"))
            news_client = ConfiguredHtmlNewsClient(news_cfg, security_matcher=self.matcher)
            try:
                articles = await news_client.collect(window)
                out_news = self.writer.save_articles("vietstock", date_str, articles, news_client.fetch_attempts)
                logger.info(f"[SUCCESS] vietstock news: collected {len(articles)} articles -> {out_news}")
                results["articles_count"] = len(articles)
                results["news_output_path"] = str(out_news)
            except Exception as exc:
                logger.warning(f"[WARNING] vietstock news scraping failed: {exc}")
                results["news_error"] = str(exc)
            finally:
                await news_client.close()

        elapsed = time.time() - start_ts
        results["elapsed_seconds"] = round(elapsed, 2)
        return results

    async def run_cophieu68_market(
        self, start_date: str, end_date: str, symbols: list[str] | None = None,
    ) -> dict[str, Any]:
        logger.info(f"==> Starting local CoPhieu68 market collection [{start_date}..{end_date}]")
        started = time.time()
        config = load_yaml(CONFIG_DIR / "sources/market/cophieu68.yaml")
        universe = load_yaml(CONFIG_DIR / "reference/financial-news-universe.yaml")
        exchange_code = str(universe.get("exchange", {}).get("code") or "HOSE")
        known_symbols = {str(item.get("symbol", "")).upper() for item in universe.get("securities", [])}
        requested_symbols = [symbol.upper() for symbol in symbols or []]
        if len(requested_symbols) != len(set(requested_symbols)) or set(requested_symbols) - known_symbols:
            raise ValueError(f"Unknown or duplicate CoPhieu68 symbols: {symbols}")
        securities = [
            {"symbol": str(item["symbol"]).upper(), "name": item.get("company_name"), "exchange_code": exchange_code}
            for item in universe.get("securities", [])
            if not requested_symbols or str(item.get("symbol", "")).upper() in set(requested_symbols)
        ]
        window = DateWindow.parse(start_date, end_date, config.get("timezone", "Asia/Ho_Chi_Minh"))
        client = CoPhieu68Client(config)
        try:
            market_records, periods = await asyncio.to_thread(client.collect, window, securities)
            window_key = start_date if start_date == end_date else f"{start_date}_to_{end_date}"
            output = self.writer.save_cophieu68_records(
                window_key, market_records, periods, client.fetch_attempts,
                start_date=start_date, end_date=end_date,
            )
            summary = {
                "source": "cophieu68",
                "status": "PARTIAL" if client.symbol_errors else "SUCCESS",
                "price_rows": len(market_records),
                "financial_periods": len(periods),
                "financial_values": sum(len(period.values) for period in periods),
                "symbol_errors": client.symbol_errors,
                "fetch_attempts": len(client.fetch_attempts),
                "date_window": [start_date, end_date],
                "output_path": str(output),
                "elapsed_seconds": round(time.time() - started, 2),
            }
            logger.info(f"CoPhieu68 local collection summary: {summary}")
            return summary
        except Exception as exc:
            logger.exception("CoPhieu68 local collection failed")
            return {
                "source": "cophieu68", "status": "FAILED", "date_window": [start_date, end_date],
                "error": str(exc), "elapsed_seconds": round(time.time() - started, 2),
            }

    async def run(
        self,
        source: str,
        start_date: str,
        end_date: str,
        collection_method: str = "latest",
        symbols: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        sources_to_run = ALL_SOURCES if source in {"all", "*"} else [source]
        summaries: list[dict[str, Any]] = []

        for src in sources_to_run:
            if src == "vietstock":
                if start_date != end_date:
                    summaries.append({
                        "source": src,
                        "status": "FAILED",
                        "error": "The local Vietstock public-board adapter accepts one current snapshot date only.",
                    })
                    continue
                res = await self.run_vietstock(end_date)
            elif src == "cophieu68":
                res = await self.run_cophieu68_market(start_date, end_date, symbols)
            else:
                res = await self.run_news_source(src, start_date, end_date, collection_method)
            summaries.append(res)

        return summaries

