from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Any


@dataclass
class ContentBlock:
    order: int
    block_type: str
    text: str | None = None
    media: dict[str, Any] | None = None
    table: dict[str, Any] | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class Article:
    source_code: str
    publisher_code: str
    source_article_id: str
    canonical_url: str
    title: str
    published_at: datetime
    summary: str | None = None
    subtitle: str | None = None
    author: str | None = None
    original_source_url: str | None = None
    section: str | None = None
    cover_image_url: str | None = None
    cover_image_alt: str | None = None
    blocks: list[ContentBlock] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class MarketRecord:
    kind: str
    provider_code: str
    trade_date: date
    symbol_or_index: str
    observed_at: datetime
    fields: dict[str, Any]
    raw_payload: dict[str, Any]

