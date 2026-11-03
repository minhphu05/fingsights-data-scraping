from __future__ import annotations

import logging
import os
import re
import unicodedata
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import yaml
from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger("finsights.persistence.security_matching")


FINANCIAL_CONTEXT = (
    "cổ phiếu", "chứng khoán", "thị trường", "ngân hàng", "tài chính", "hose", "hnx",
    "upcom", "vn-index", "vn30", "cổ tức", "trái phiếu", "lãi suất", "lợi nhuận",
    "báo cáo", "niêm yết", "đầu tư", "mã ck", "công bố thông tin", "khối ngoại",
)


def normalize_text(value: str) -> str:
    value = unicodedata.normalize("NFKD", value).casefold()
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", value).strip()


@dataclass
class SecurityMatch:
    security_uid: str
    symbol: str
    alias_text: str
    alias_type: str
    confidence: float
    method: str
    evidence: str


class SecurityAliasMatcher:
    def __init__(self, market_engine: Engine | None = None, alias_file: str | Path | None = None):
        self.market_engine = market_engine
        self.aliases: list[dict[str, Any]] = []
        if self.market_engine is not None:
            try:
                statement = text(
                    "SELECT s.security_uid, s.symbol, a.alias_text, a.alias_type, a.match_mode, "
                    "a.requires_financial_context, a.confidence, a.effective_from, a.effective_to "
                    "FROM market_security_aliases a JOIN market_securities s ON s.id = a.security_id "
                    "WHERE a.is_active = 1"
                )
                with self.market_engine.connect() as connection:
                    self.aliases = [dict(row) for row in connection.execute(statement).mappings()]
            except Exception as exc:
                logger.warning(f"Could not load aliases from market_engine: {exc}. Falling back to alias_file.")

        if not self.aliases:
            self._load_from_file(alias_file)

    def _load_from_file(self, alias_file: str | Path | None = None) -> None:
        if alias_file is None:
            root = Path(__file__).resolve().parents[3]
            alias_file = root / "configs/reference/security-aliases.yaml"
        path = Path(alias_file)
        if not path.exists():
            logger.warning(f"Alias reference file does not exist: {path}")
            return
        try:
            content = yaml.safe_load(path.read_text(encoding="utf-8"))
            if not isinstance(content, dict):
                return
            for sec in content.get("securities", []):
                symbol = str(sec.get("symbol", "")).strip()
                for alias in sec.get("aliases", []):
                    alias_text = str(alias.get("text", "")).strip()
                    alias_type = str(alias.get("alias_type", "ticker"))
                    self.aliases.append({
                        "security_uid": f"local-{symbol}",
                        "symbol": symbol,
                        "alias_text": alias_text,
                        "alias_type": alias_type,
                        "match_mode": alias.get("match_mode", "exact_token" if alias_type in {"ticker", "symbol"} else "phrase"),
                        "requires_financial_context": bool(alias.get("requires_financial_context", True)),
                        "confidence": float(alias.get("confidence", 0.95)),
                        "effective_from": alias.get("effective_from"),
                        "effective_to": alias.get("effective_to"),
                    })
        except Exception as exc:
            logger.warning(f"Failed to load aliases from {path}: {exc}")

    def match(self, text_blob: str, published_date: date) -> list[SecurityMatch]:
        normalized = normalize_text(text_blob)
        financial_context = any(normalize_text(term) in normalized for term in FINANCIAL_CONTEXT)
        matches: dict[str, SecurityMatch] = {}
        for row in self.aliases:
            if row["effective_from"] and row["effective_from"] > published_date:
                continue
            if row["effective_to"] and row["effective_to"] < published_date:
                continue
            alias = str(row["alias_text"])
            if row["requires_financial_context"] and not financial_context:
                continue
            candidate = normalize_text(alias)
            if not candidate:
                continue
            if row["match_mode"] == "exact_token" or row["alias_type"] in {"ticker", "symbol"}:
                raw_candidate = alias.upper()
                pattern = rf"(?<![\w]){re.escape(raw_candidate)}(?![\w])"
                if not re.search(pattern, text_blob):
                    continue
                if not financial_context:
                    continue
            else:
                pattern = rf"(?<![\w]){re.escape(candidate)}(?![\w])"
                if not re.search(pattern, normalized):
                    continue
            key = str(row["security_uid"])
            match = SecurityMatch(
                security_uid=key,
                symbol=str(row["symbol"]),
                alias_text=alias,
                alias_type=str(row["alias_type"]),
                confidence=float(row["confidence"]),
                method="ticker_boundary" if row["alias_type"] in {"ticker", "symbol"} else "company_phrase",
                evidence=self._evidence(text_blob, alias),
            )
            previous = matches.get(key)
            if previous is None or match.confidence > previous.confidence:
                matches[key] = match
        return list(matches.values())

    @staticmethod
    def _evidence(blob: str, alias: str, radius: int = 100) -> str:
        normalized_alias = normalize_text(alias)
        normalized_blob = normalize_text(blob)
        position = normalized_blob.find(normalized_alias)
        if position < 0:
            return blob[: radius * 2]
        # Evidence is a review aid; retain readable source spelling from the start.
        start = max(position - radius, 0)
        return blob[start : position + len(alias) + radius]

