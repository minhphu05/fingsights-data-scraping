#!/usr/bin/env python3
"""Fetch one HTML article per Financial News publisher and verify body extraction.

This is a bounded, read-only smoke test: it never writes to the local data folder,
MySQL, object storage, or Dagster. Four fixture URLs come from the existing local
sample batch; Báo Mới is discovered from its dated HTML sitemap.
"""
from __future__ import annotations

import asyncio
import json
import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.finsights_scraping.sources.news.configured_html import (  # noqa: E402
    ConfiguredHtmlNewsClient,
    source_collection_enabled,
)
from src.finsights_scraping.sources.news.baomoi import BaoMoiClient  # noqa: E402

SAMPLE_DATE = "2026-11-02"
FIXTURE_SOURCES = ("cafef", "tuoitre", "thanhnien", "vnexpress")
ALL_SOURCES = (*FIXTURE_SOURCES, "baomoi")
MIN_BODY_WORDS = 120
BAOMOI_TEST_DATE = "2026-11-20"


def _load_config(source: str) -> dict[str, Any]:
    path = ROOT / "configs" / "sources" / "news" / f"{source}.yaml"
    with path.open("r", encoding="utf-8") as stream:
        config = yaml.safe_load(stream)
    if not isinstance(config, dict):
        raise ValueError(f"Expected a YAML mapping in {path}")
    return config


def _fixture_url(source: str) -> str:
    path = ROOT / "data" / source / SAMPLE_DATE / "articles.json"
    with path.open("r", encoding="utf-8") as stream:
        rows = json.load(stream)
    if isinstance(rows, dict):
        rows = rows.get("articles", [])
    for article in rows:
        url = article.get("canonical_url")
        if url:
            return str(url)
    raise ValueError(f"No sample article URL in {path}")


async def _baomoi_url(client: BaoMoiClient, config: dict[str, Any]) -> str:
    sitemap_index_url = str(config["discovery"]["html_backfill"]["sitemap_index_url"])
    index_response = await client._get(sitemap_index_url)
    index_root = ET.fromstring(index_response.content)
    date_key = BAOMOI_TEST_DATE.replace("-", "")
    daily_sitemap = next(
        (
            node.text.strip()
            for node in index_root.iter()
            if node.tag.rsplit("}", 1)[-1] == "loc"
            and node.text
            and date_key in node.text
        ),
        None,
    )
    if not daily_sitemap:
        raise ValueError(f"No Báo Mới sitemap partition for {BAOMOI_TEST_DATE}")

    daily_response = await client._get(daily_sitemap)
    daily_root = ET.fromstring(daily_response.content)
    finance_slug_terms = (
        "chung-khoan",
        "co-phieu",
        "vn-index",
        "ngan-hang",
        "lai-suat",
        "loi-nhuan",
        "co-tuc",
        "bao-cao-tai-chinh",
        "thi-truong-chung-khoan",
    )
    for entry in daily_root.iter():
        if entry.tag.rsplit("}", 1)[-1] != "url":
            continue
        article_url = next(
            (
                child.text.strip()
                for child in entry
                if child.tag.rsplit("}", 1)[-1] == "loc" and child.text
            ),
            None,
        )
        if article_url and article_url.endswith(".epi") and any(
            term in article_url.casefold() for term in finance_slug_terms
        ):
            return article_url
    raise ValueError(
        f"No clearly financial Báo Mới article URL in {BAOMOI_TEST_DATE} sitemap"
    )


async def _test_source(source: str) -> dict[str, Any]:
    config = _load_config(source)
    if not source_collection_enabled(config):
        raise RuntimeError(f"{source} is not enabled by the configured access policy")
    if config.get("article", {}).get("full_text_required") is not True:
        raise RuntimeError(f"{source} does not require full article text")
    client = BaoMoiClient(config) if source == "baomoi" else ConfiguredHtmlNewsClient(config)
    try:
        url = await _baomoi_url(client, config) if source == "baomoi" else _fixture_url(source)
        article = await client.fetch_article(url, category="one_article_full_text_test")
        body = "\n".join(block.text or "" for block in article.blocks).strip()
        body_words = len(re.findall(r"\S+", body))
        summary_words = len(re.findall(r"\S+", article.summary or ""))
        fallback = any(
            bool(getattr(block, "metadata", {}).get("fallback"))
            for block in article.blocks
        )
        full_text_ok = not fallback and body_words >= max(
            MIN_BODY_WORDS, summary_words + 25
        )
        return {
            "source": source,
            "status": "PASS" if full_text_ok else "FAIL",
            "title": article.title,
            "url": article.canonical_url,
            "body_words": body_words,
            "summary_words": summary_words,
            "blocks": len(article.blocks),
            "summary_fallback": fallback,
        }
    finally:
        await client.close()


async def main() -> int:
    results = []
    for source in ALL_SOURCES:
        try:
            result = await _test_source(source)
        except Exception as exc:
            result = {"source": source, "status": "FAIL", "error": f"{type(exc).__name__}: {exc}"}
        results.append(result)
        print(json.dumps(result, ensure_ascii=False))
    passed = sum(result.get("status") == "PASS" for result in results)
    print(json.dumps({"passed": passed, "total": len(results)}, ensure_ascii=False))
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
