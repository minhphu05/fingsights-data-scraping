from src.orchestration.jobs.news_jobs import (
    job_cafef_news,
    job_thanhnien_news,
    job_all_news_parallel,
)
from src.orchestration.jobs.stock_jobs import (
    job_vietstock_indices,
    job_fireant_stocks,
    job_all_stocks_parallel,
)

all_jobs = [
    job_cafef_news,
    job_thanhnien_news,
    job_all_news_parallel,
    job_vietstock_indices,
    job_fireant_stocks,
    job_all_stocks_parallel,
]

__all__ = [
    "job_cafef_news",
    "job_thanhnien_news",
    "job_all_news_parallel",
    "job_vietstock_indices",
    "job_fireant_stocks",
    "job_all_stocks_parallel",
    "all_jobs",
]
