from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
import re
from zoneinfo import ZoneInfo


class DateWindowError(ValueError):
    pass


@dataclass(frozen=True)
class DateWindow:
    """Inclusive local-calendar-day window converted to UTC for storage."""

    start_date: date
    end_date: date
    timezone_name: str = "Asia/Ho_Chi_Minh"

    def __post_init__(self) -> None:
        if self.end_date < self.start_date:
            raise DateWindowError("end_date must be on or after start_date")
        try:
            ZoneInfo(self.timezone_name)
        except Exception as exc:
            raise DateWindowError(f"Unknown timezone: {self.timezone_name}") from exc

    @classmethod
    def parse(cls, start_date: str, end_date: str, timezone_name: str = "Asia/Ho_Chi_Minh") -> "DateWindow":
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(start_date)) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(end_date)):
            raise DateWindowError("start_date and end_date must use YYYY-MM-DD")
        try:
            start = date.fromisoformat(start_date)
            end = date.fromisoformat(end_date)
        except (TypeError, ValueError) as exc:
            raise DateWindowError("start_date and end_date must use YYYY-MM-DD") from exc
        return cls(start, end, timezone_name)

    @property
    def local_start(self) -> datetime:
        return datetime.combine(self.start_date, time.min, ZoneInfo(self.timezone_name))

    @property
    def local_end_exclusive(self) -> datetime:
        return datetime.combine(self.end_date + timedelta(days=1), time.min, ZoneInfo(self.timezone_name))

    def contains(self, value: datetime) -> bool:
        if value.tzinfo is None:
            raise DateWindowError("Publication/observation timestamps must include a timezone")
        local_value = value.astimezone(ZoneInfo(self.timezone_name))
        return self.start_date <= local_value.date() <= self.end_date

