from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any


@dataclass
class DisclosureRecord:
    """Normalized official exchange, regulator, and depository publication."""

    source_code: str
    source_item_id: str
    title: str
    original_url: str
    published_at: datetime | None
    summary: str | None = None
    symbol: str | None = None
    company_name: str | None = None
    item_type: str = "issuer_disclosure"
    disclosure_kind: str = "other"
    notice_kind: str = "other"
    authority_name: str | None = None
    effective_date: date | None = None
    ex_date: date | None = None
    record_date: date | None = None
    payment_date: date | None = None
    cash_per_share_vnd: Decimal | None = None
    ratio_numerator: Decimal | None = None
    ratio_denominator: Decimal | None = None
    subscription_price_vnd: Decimal | None = None
    attachments: list[dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    document_asset_uid: str | None = None
