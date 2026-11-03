from src.orchestration.schedules.news_schedules import news_schedules
from src.orchestration.schedules.stock_schedules import stock_schedules

all_schedules = news_schedules + stock_schedules

__all__ = [
    "news_schedules",
    "stock_schedules",
    "all_schedules",
]
