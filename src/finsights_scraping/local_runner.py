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
from .sources.market.vietstock import VietstockPublicBoardClient
from .sources.news.cafef import CafeFClient
from .sources.news.configured_html import ConfiguredHtmlNewsClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("finsights.local_runner")

ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = ROOT / "configs"

ALL_SOURCES = ["cafef", "baomoi", "thanhnien", "tuoitre", "vnexpress", "vietstock"]


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
        alias_file = CONFIG_DIR / "reference/security-aliases.yaml"
        self.matcher = SecurityAliasMatcher(
            market_engine=None,
            alias_file=alias_file if alias_file.exists() else None,
        )

    async def run_news_source(self, source_key: str, date_str: str) -> dict[str, Any]:
        logger.info(f"==> Starting local scraping for news source: {source_key} on {date_str}")
        start_ts = time.time()
        cfg_path = CONFIG_DIR / f"sources/news/{source_key}.yaml"
        if not cfg_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {cfg_path}")
        config = load_yaml(cfg_path)

        window = DateWindow.parse(date_str, date_str, config.get("timezone", "Asia/Ho_Chi_Minh"))

        client_cls = CafeFClient if source_key == "cafef" else ConfiguredHtmlNewsClient
        client = client_cls(config, security_matcher=self.matcher)

        try:
            articles = await client.collect(window)
            elapsed = time.time() - start_ts
            out_file = self.writer.save_articles(source_key, date_str, articles, client.fetch_attempts)
            logger.info(
                f"[SUCCESS] {source_key}: collected {len(articles)} articles in {elapsed:.1f}s -> {out_file}"
            )
            return {
                "source": source_key,
                "status": "SUCCESS",
                "kind": "news",
                "date": date_str,
                "articles_count": len(articles),
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
                "date": date_str,
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

    async def run(self, source: str, date_str: str) -> list[dict[str, Any]]:
        sources_to_run = ALL_SOURCES if source in {"all", "*"} else [source]
        summaries: list[dict[str, Any]] = []

        for src in sources_to_run:
            if src == "vietstock":
                res = await self.run_vietstock(date_str)
            else:
                res = await self.run_news_source(src, date_str)
            summaries.append(res)

        return summaries

