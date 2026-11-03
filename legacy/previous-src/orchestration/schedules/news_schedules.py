from dagster import ScheduleDefinition
from src.orchestration.jobs.news_jobs import (
    job_cafef_news,
    job_thanhnien_news,
    job_all_news_parallel,
)

# Run CafeF crawl at minute 0 of every hour
cafef_hourly_schedule = ScheduleDefinition(
    job=job_cafef_news,
    cron_schedule="0 * * * *",
    description="Crawls CafeF financial news at the top of every hour",
)

# Run Thanh Nien crawl at minute 30 of every hour
thanhnien_hourly_schedule = ScheduleDefinition(
    job=job_thanhnien_news,
    cron_schedule="30 * * * *",
    description="Crawls Thanh Nien financial news at minute 30 of every hour",
)

# Run all news scrapers in parallel every 2 hours
all_news_parallel_schedule = ScheduleDefinition(
    job=job_all_news_parallel,
    cron_schedule="0 */2 * * *",
    description="Triggers parallel multi-pod news scraping across all media outlets every 2 hours",
)

news_schedules = [
    cafef_hourly_schedule,
    thanhnien_hourly_schedule,
    all_news_parallel_schedule,
]
