from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Any
import uuid


@dataclass
class FinancialValueRecord:
    statement_family: str
    source_label: str
    line_key: str
    value_decimal: Decimal
    original_value_decimal: Decimal
    reported_unit: str
    unit_multiplier: Decimal
    unit_code: str
    value_kind: str
    source_currency: str | None
    row_order: int
    raw_fields: dict[str, Any] = field(default_factory=dict)


@dataclass
class FinancialPeriodRecord:
    symbol: str
    period_code: str
    fiscal_year: int
    fiscal_quarter: int | None
    period_type: str
    period_start: date
    period_end: date
    source_url: str
    revision_no: int = 1
    values: list[FinancialValueRecord] = field(default_factory=list)

    @property
    def period_uid(self) -> str:
        return str(uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"cophieu68:{self.symbol}:{self.period_code}:unspecified:{self.revision_no}",
        ))
