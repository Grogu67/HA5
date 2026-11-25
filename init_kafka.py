"""Одноразовый init: создаёт topic и регистрирует Avro-схему в Schema Registry."""
import json
import logging
import os
import sys
import time

import requests
from confluent_kafka.admin import AdminClient, NewTopic

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("init")

TOPIC = os.getenv("KAFKA_TOPIC", "movie-events")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka-1:19092,kafka-2:19092")
SR_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
SCHEMA_PATH = os.getenv("SCHEMA_PATH", "/app/schema/movie_event.avsc")
PARTITIONS = int(os.getenv("TOPIC_PARTITIONS", "3"))
REPLICATION = int(os.getenv("TOPIC_REPLICATION_FACTOR", "2"))
MIN_ISR = os.getenv("TOPIC_MIN_INSYNC_REPLICAS", "1")
SUBJECT = f"{TOPIC}-value"
HEADERS = {"Content-Type": "application/vnd.schemaregistry.v1+json"}


def retry(fn, what, attempts=15, delay=3):
    for i in range(1, attempts + 1):
        try:
            return fn()
        except Exception as e:
            log.warning("%s failed (%d/%d): %s", what, i, attempts, e)
            time.sleep(delay)
    sys.exit(f"{what}: giving up")


def create_topic():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    topic = NewTopic(TOPIC, num_partitions=PARTITIONS, replication_factor=REPLICATION,
                     config={"min.insync.replicas": MIN_ISR})
    for name, fut in admin.create_topics([topic]).items():
        try:
            fut.result()
            log.info("topic %s created: partitions=%d replication.factor=%d min.insync.replicas=%s",
                     name, PARTITIONS, REPLICATION, MIN_ISR)
        except Exception as e:
            if "TOPIC_ALREADY_EXISTS" in str(e):
                log.info("topic %s already exists", name)
            else:
                raise


def register_schema():
    with open(SCHEMA_PATH, encoding="utf-8") as f:
        schema_str = json.dumps(json.load(f))
    r = requests.put(f"{SR_URL}/config/{SUBJECT}", headers=HEADERS,
                     json={"compatibility": "BACKWARD"}, timeout=10)
    r.raise_for_status()
    r = requests.post(f"{SR_URL}/subjects/{SUBJECT}/versions", headers=HEADERS,
                      json={"schema": schema_str, "schemaType": "AVRO"}, timeout=10)
    r.raise_for_status()
    versions = requests.get(f"{SR_URL}/subjects/{SUBJECT}/versions", timeout=10).json()
    log.info("schema registered: subject=%s id=%s versions=%s compatibility=BACKWARD",
             SUBJECT, r.json()["id"], versions)


if __name__ == "__main__":
    retry(create_topic, "create topic")
    retry(register_schema, "register schema")
    log.info("init done")
