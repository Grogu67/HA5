"""Публикация событий в Kafka: Avro + Schema Registry, acks=all, retry с exponential backoff."""
import logging
import os
import threading
import time

from confluent_kafka import KafkaException, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from models import MovieEvent

log = logging.getLogger("producer")

TOPIC = os.getenv("KAFKA_TOPIC", "movie-events")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka-1:19092,kafka-2:19092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
SCHEMA_PATH = os.getenv("SCHEMA_PATH", "/app/schema/movie_event.avsc")
MAX_ATTEMPTS = int(os.getenv("PUBLISH_MAX_ATTEMPTS", "5"))
BACKOFF_BASE = float(os.getenv("PUBLISH_BACKOFF_BASE_SECONDS", "0.2"))


class PublishError(Exception):
    """Не удалось опубликовать событие после всех попыток."""


class SchemaValidationError(Exception):
    """Событие не соответствует Avro-схеме."""


class EventPublisher:
    def __init__(self):
        with open(SCHEMA_PATH, encoding="utf-8") as f:
            schema_str = f.read()
        registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
        # Схема уже зарегистрирована init-шагом; берём последнюю версию, сами не регистрируем
        self._serializer = AvroSerializer(
            registry, schema_str,
            conf={"auto.register.schemas": False, "use_latest_version": True},
        )
        self._producer = Producer({
            "bootstrap.servers": BOOTSTRAP,
            "client.id": "movie-service",
            "acks": "all",                 # подтверждение от всех in-sync реплик
            "enable.idempotence": True,    # повторы внутри клиента не создают дублей
            "linger.ms": 5,
            "message.timeout.ms": 15000,
        })
        self._stop = False
        threading.Thread(target=self._poll_loop, daemon=True).start()

    def _poll_loop(self):
        while not self._stop:
            self._producer.poll(0.1)

    def close(self):
        self._stop = True
        self._producer.flush(5)

    def publish(self, event: MovieEvent) -> dict:
        """Валидирует по схеме, отправляет, ждёт подтверждения. Возвращает partition/offset."""
        try:
            # Сериализация проверяет данные по Avro-схеме (enum, типы, обязательные поля)
            payload = self._serializer(event.to_avro(), SerializationContext(TOPIC, MessageField.VALUE))
        except Exception as e:  # SerializationError и ошибки схемы
            raise SchemaValidationError(str(e)) from e

        key = event.user_id.encode("utf-8")  # ключ партиционирования = user_id
        last_error = None
        for attempt in range(1, MAX_ATTEMPTS + 1):
            done = threading.Event()
            result = {}

            def on_delivery(err, msg):
                result["err"], result["msg"] = err, msg
                done.set()

            try:
                self._producer.produce(TOPIC, key=key, value=payload, on_delivery=on_delivery)
                if not done.wait(20):
                    result["err"] = "delivery timeout"
            except (BufferError, KafkaException) as e:
                result["err"] = e

            if result.get("err") is None:
                msg = result["msg"]
                log.info("published event_id=%s event_type=%s timestamp=%s partition=%s offset=%s",
                         event.event_id, event.event_type.value, event.timestamp.isoformat(),
                         msg.partition(), msg.offset())
                return {"partition": msg.partition(), "offset": msg.offset()}

            last_error = result["err"]
            delay = BACKOFF_BASE * 2 ** (attempt - 1)  # 0.2, 0.4, 0.8, 1.6 ...
            log.warning("publish failed event_id=%s attempt=%d/%d error=%s retry_in=%.1fs",
                        event.event_id, attempt, MAX_ATTEMPTS, last_error, delay)
            if attempt < MAX_ATTEMPTS:
                time.sleep(delay)
        raise PublishError(f"event {event.event_id} not published: {last_error}")
