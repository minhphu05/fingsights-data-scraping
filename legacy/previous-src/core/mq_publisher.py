import json
from typing import Any
import pika
import structlog
from src.config.settings import settings

logger = structlog.get_logger(__name__)


class MessagePublisher:
    """RabbitMQ Publisher for streaming raw scraped events to async pipelines."""

    def __init__(self):
        self.host = settings.rabbitmq_host
        self.port = settings.rabbitmq_port
        self.user = settings.rabbitmq_user
        self.password = settings.rabbitmq_password
        self.exchange = settings.rabbitmq_exchange

    def _get_connection(self) -> pika.BlockingConnection:
        credentials = pika.PlainCredentials(self.user, self.password)
        parameters = pika.ConnectionParameters(
            host=self.host,
            port=self.port,
            credentials=credentials,
            connection_attempts=3,
            retry_delay=2,
            socket_timeout=10,
        )
        return pika.BlockingConnection(parameters)

    def publish(self, routing_key: str, message: dict[str, Any]) -> bool:
        """Publish an event to the RabbitMQ exchange."""
        try:
            with self._get_connection() as connection:
                channel = connection.channel()
                channel.exchange_declare(
                    exchange=self.exchange,
                    exchange_type="topic",
                    durable=True,
                )
                payload = json.dumps(message, default=str)
                channel.basic_publish(
                    exchange=self.exchange,
                    routing_key=routing_key,
                    body=payload.encode("utf-8"),
                    properties=pika.BasicProperties(
                        delivery_mode=pika.DeliveryMode.Persistent,
                        content_type="application/json",
                    ),
                )
                logger.info("Published event to RabbitMQ", exchange=self.exchange, routing_key=routing_key)
                return True
        except Exception as exc:
            logger.error("Failed to publish message to RabbitMQ", error=str(exc), routing_key=routing_key)
            return False
