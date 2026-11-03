from dagster import define_asset_job, AssetSelection
from src.orchestration.k8s_config import get_k8s_job_tags

# Independent job for Vietstock market indices - dedicated K3s pod
job_vietstock_indices = define_asset_job(
    name="job_crawl_vietstock_indices",
    selection=AssetSelection.assets("vietstock_indices_asset"),
    description="Dedicated job executing Vietstock indices scraping in an isolated Kubernetes Pod",
    tags=get_k8s_job_tags(source_name="vietstock_indices", cpu_limit="300m", memory_limit="384Mi"),
)

# Independent job for FireAnt stock quotes - dedicated K3s pod
job_fireant_stocks = define_asset_job(
    name="job_crawl_fireant_stocks",
    selection=AssetSelection.assets("fireant_stocks_asset"),
    description="Dedicated job executing FireAnt stock quotes scraping in an isolated Kubernetes Pod",
    tags=get_k8s_job_tags(source_name="fireant_stocks", cpu_limit="500m", memory_limit="512Mi"),
)

# Parallel stock job across all financial data feeds
job_all_stocks_parallel = define_asset_job(
    name="job_crawl_all_stocks_parallel",
    selection=AssetSelection.groups("stock_scrapers"),
    description="Multi-pod parallel financial index & stock quotes collection job",
    tags=get_k8s_job_tags(source_name="all_stocks", cpu_limit="1", memory_limit="1Gi"),
)
