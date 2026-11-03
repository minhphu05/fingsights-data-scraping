import hashlib
import redis
import structlog
from src.config.settings import settings

logger = structlog.get_logger(__name__)


class UrlDeduplicator:
    """Redis-backed URL and content deduplicator."""

    def __init__(self, key_prefix: str = "crawldedup"):
        self.key_prefix = key_prefix
        self._client = redis.Redis(
            host=settings.redis_host,
            port=settings.redis_port,
            password=settings.redis_password,
            db=settings.redis_db,
            decode_responses=True,
            socket_connect_timeout=5,
        )

    @staticmethod
    def compute_hash(value: str) -> str:
        """Compute SHA-256 hash of a normalized URL or string."""
        normalized = value.strip().lower()
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()

    def is_seen(self, source_code: str, url: str) -> bool:
        """Check if a URL has already been scraped for this source."""
        url_hash = self.compute_hash(url)
        key = f"{self.key_prefix}:{source_code}:hashes"
        try:
            return bool(self._client.sismember(key, url_hash))
        except redis.RedisError as exc:
            logger.warning("Redis dedup check error, defaulting to unseen", error=str(exc))
            return False

    def mark_seen(self, source_code: str, url: str, ttl_days: int = 60) -> None:
        """Mark a URL hash as scraped, with an optional expiration window."""
        url_hash = self.compute_hash(url)
        key = f"{self.key_prefix}:{source_code}:hashes"
        try:
            self._client.sadd(key, url_hash)
            if ttl_days > 0:
                self._client.expire(key, ttl_days * 86400)
        except redis.RedisError as exc:
            logger.warning("Redis mark_seen failed", error=str(exc))

    def filter_unseen_urls(self, source_code: str, urls: list[str]) -> list[str]:
        """Filter a list of URLs and return only new, unseen URLs."""
        if not urls:
            return []
        return [url for url in urls if not self.is_seen(source_code, url)]
