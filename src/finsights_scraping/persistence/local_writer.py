from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

from ..domain.models import Article, ContentBlock, MarketRecord


class LocalDataWriter:
    """Writes scraped news articles and market data locally into structured JSON files."""

    def __init__(self, base_dir: Path | str = "data"):
        self.base_dir = Path(base_dir)

    def _json_serialize(self, obj: Any) -> Any:
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if is_dataclass(obj):
            return self._json_serialize(asdict(obj))
        if isinstance(obj, dict):
            return {k: self._json_serialize(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple, set)):
            return [self._json_serialize(item) for item in obj]
        return str(obj) if not isinstance(obj, (int, float, bool, type(None), str)) else obj

    def save_articles(
        self,
        source: str,
        date_str: str,
        articles: list[Article],
        fetch_attempts: list[dict[str, Any]] | None = None,
    ) -> Path:
        out_dir = self.base_dir / source / date_str
        out_dir.mkdir(parents=True, exist_ok=True)

        serialized_articles = []
        unique_tickers = set()

        for art in articles:
            tickers = art.metadata.get("matched_securities", [])
            ticker_symbols = [t.get("symbol") for t in tickers if isinstance(t, dict) and t.get("symbol")]
            unique_tickers.update(ticker_symbols)

            item = {
                "source_code": art.source_code,
                "publisher_code": art.publisher_code,
                "source_article_id": art.source_article_id,
                "canonical_url": art.canonical_url,
                "title": art.title,
                "published_at": art.published_at.isoformat() if art.published_at else None,
                "summary": art.summary,
                "subtitle": art.subtitle,
                "author": art.author,
                "section": art.section,
                "cover_image_url": art.cover_image_url,
                "cover_image_alt": art.cover_image_alt,
                "matched_tickers": ticker_symbols,
                "matched_securities_detail": tickers,
                "financial_terms": art.metadata.get("financial_terms", []),
                "blocks_count": len(art.blocks),
                "blocks": [self._json_serialize(b) for b in art.blocks],
                "metadata": {
                    k: self._json_serialize(v)
                    for k, v in art.metadata.items()
                    if k not in {"matched_securities", "financial_terms"}
                },
            }
            serialized_articles.append(item)

        articles_file = out_dir / "articles.json"
        articles_file.write_text(
            json.dumps(serialized_articles, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        summary = {
            "source": source,
            "target_date": date_str,
            "collected_at": datetime.now().isoformat(),
            "total_articles": len(articles),
            "unique_tickers_matched": sorted(unique_tickers),
            "total_fetch_attempts": len(fetch_attempts or []),
            "articles": [
                {
                    "source_article_id": a["source_article_id"],
                    "title": a["title"],
                    "canonical_url": a["canonical_url"],
                    "published_at": a["published_at"],
                    "tickers": a["matched_tickers"],
                }
                for a in serialized_articles
            ],
        }
        summary_file = out_dir / "summary.json"
        summary_file.write_text(
            json.dumps(summary, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return articles_file

    def save_market_records(
        self,
        source: str,
        date_str: str,
        records: list[MarketRecord | dict[str, Any]],
        fetch_attempts: list[dict[str, Any]] | None = None,
    ) -> Path:
        out_dir = self.base_dir / source / date_str
        out_dir.mkdir(parents=True, exist_ok=True)

        serialized_records = [self._json_serialize(r) for r in records]
        indices_file = out_dir / "market_indices.json"
        indices_file.write_text(
            json.dumps(serialized_records, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        summary = {
            "source": source,
            "target_date": date_str,
            "collected_at": datetime.now().isoformat(),
            "total_records": len(records),
            "total_fetch_attempts": len(fetch_attempts or []),
            "records": serialized_records,
        }
        summary_file = out_dir / "summary.json"
        summary_file.write_text(
            json.dumps(summary, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return indices_file
