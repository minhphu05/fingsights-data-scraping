from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Global configuration settings for data scraping engine."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --------------------------------------------------------------------------
    # MySQL Node 2 (Oracle Cloud Singapore)
    # --------------------------------------------------------------------------
    mysql_host: str = Field(default="129.221.137.201", alias="MYSQL_HOST")
    mysql_port: int = Field(default=3306, alias="MYSQL_PORT")
    mysql_database: str = Field(default="finsights_collection", alias="MYSQL_DATABASE")
    mysql_user: str = Field(default="finsights_collection_app", alias="MYSQL_USER")
    mysql_password: str = Field(default="", alias="MYSQL_PASSWORD")
    mysql_tls_ca: str | None = Field(default=None, alias="MYSQL_TLS_CA")

    # --------------------------------------------------------------------------
    # Redis Cache (Deduplication & Rate Limiting)
    # --------------------------------------------------------------------------
    redis_host: str = Field(default="redis-master.finsights-platform.svc.cluster.local", alias="REDIS_HOST")
    redis_port: int = Field(default=6379, alias="REDIS_PORT")
    redis_password: str = Field(default="FinsightsRedis2026!", alias="REDIS_PASSWORD")
    redis_db: int = Field(default=0, alias="REDIS_DB")

    # --------------------------------------------------------------------------
    # RabbitMQ Message Broker (Raw Events Publishing)
    # --------------------------------------------------------------------------
    rabbitmq_host: str = Field(default="rabbitmq.finsights-platform.svc.cluster.local", alias="RABBITMQ_HOST")
    rabbitmq_port: int = Field(default=5672, alias="RABBITMQ_PORT")
    rabbitmq_user: str = Field(default="admin", alias="RABBITMQ_USER")
    rabbitmq_password: str = Field(default="FinsightsRabbitMQ2026!", alias="RABBITMQ_PASSWORD")
    rabbitmq_exchange: str = Field(default="finsights.scraping.events", alias="RABBITMQ_EXCHANGE")
    rabbitmq_queue_news: str = Field(default="news.raw.events", alias="RABBITMQ_QUEUE_NEWS")
    rabbitmq_queue_stocks: str = Field(default="stocks.raw.events", alias="RABBITMQ_QUEUE_STOCKS")

    # --------------------------------------------------------------------------
    # HTTP Client & Scraper Defaults
    # --------------------------------------------------------------------------
    scraper_user_agent: str = Field(
        default="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        alias="SCRAPER_USER_AGENT",
    )
    scraper_timeout_seconds: float = Field(default=25.0, alias="SCRAPER_TIMEOUT_SECONDS")
    scraper_max_retries: int = Field(default=3, alias="SCRAPER_MAX_RETRIES")
    scraper_rate_limit_rps: float = Field(default=2.0, alias="SCRAPER_RATE_LIMIT_RPS")

    # --------------------------------------------------------------------------
    # Kubernetes Pod Launcher Parameters
    # --------------------------------------------------------------------------
    k8s_namespace: str = Field(default="orchestration", alias="K8S_NAMESPACE")
    k8s_node_selector_key: str = Field(default="database-node", alias="K8S_NODE_SELECTOR_KEY")
    k8s_node_selector_value: str = Field(default="true", alias="K8S_NODE_SELECTOR_VALUE")
    dagster_current_image: str = Field(default="finsights-data-scraping:latest", alias="DAGSTER_CURRENT_IMAGE")


settings = Settings()
