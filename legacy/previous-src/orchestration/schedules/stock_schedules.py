from dagster import ScheduleDefinition
from src.orchestration.jobs.stock_jobs import (
    job_vietstock_indices,
    job_fireant_stocks,
    job_all_stocks_parallel,
)

# Run market index scraper every 15 minutes during VN stock trading hours (09:00 - 15:00, Mon-Fri)
trading_hours_indices_schedule = ScheduleDefinition(
    job=job_vietstock_indices,
    cron_schedule="*/15 9-15 * * 1-5",
    description="Crawls Vietnam market indices every 15 minutes during trading sessions (Mon-Fri)",
)

# Run real-time stock quote scraper every 5 minutes during VN stock trading hours (Mon-Fri)
trading_hours_quotes_schedule = ScheduleDefinition(
    job=job_fireant_stocks,
    cron_schedule="*/5 9-15 * * 1-5",
    description="Crawls top VN30 quotes every 5 minutes during trading sessions (Mon-Fri)",
)

# End of day market settlement crawl (15:15 Mon-Fri)
eod_market_settlement_schedule = ScheduleDefinition(
    job=job_all_stocks_parallel,
    cron_schedule="15 15 * * 1-5",
    description="Triggers comprehensive end-of-day parallel market settlement data collection",
)

stock_schedules = [
    trading_hours_indices_schedule,
    trading_hours_quotes_schedule,
    eod_market_settlement_schedule,
]
