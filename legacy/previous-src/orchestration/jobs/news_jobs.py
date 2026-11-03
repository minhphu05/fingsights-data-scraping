from dagster import define_asset_job, AssetSelection
from src.orchestration.k8s_config import get_k8s_job_tags

# Independent job for CafeF - executes in dedicated K3s pod
job_cafef_news = define_asset_job(
    name="job_crawl_cafef_news",
    selection=AssetSelection.assets("cafef_articles_asset"),
    description="Dedicated job executing CafeF news scraping in an isolated Kubernetes Pod",
    tags=get_k8s_job_tags(source_name="cafef_news", cpu_limit="500m", memory_limit="512Mi"),
)

# Independent job for Thanh Nien - executes in dedicated K3s pod
job_thanhnien_news = define_asset_job(
    name="job_crawl_thanhnien_news",
    selection=AssetSelection.assets("thanhnien_articles_asset"),
    description="Dedicated job executing Thanh Nien news scraping in an isolated Kubernetes Pod",
    tags=get_k8s_job_tags(source_name="thanhnien_economy", cpu_limit="500m", memory_limit="512Mi"),
)

# Parallel multi-pod news job - triggers both scrapers in parallel
job_all_news_parallel = define_asset_job(
    name="job_crawl_all_news_parallel",
    selection=AssetSelection.groups("news_scrapers"),
    description="Multi-pod parallel news collection job across all media sources",
    tags=get_k8s_job_tags(source_name="all_news", cpu_limit="1", memory_limit="1Gi"),
)
