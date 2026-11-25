"""Интеграционный тест: событие проходит путь HTTP -> Kafka -> ClickHouse."""
import os
import time
import uuid
from datetime import datetime, timezone

import clickhouse_connect
import pytest
import requests

MOVIE_SERVICE = os.getenv("MOVIE_SERVICE_URL", "http://movie-service:8000")
CH_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CH_USER = os.getenv("CLICKHOUSE_USER", "cinema")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "cinema")


@pytest.fixture(scope="module")
def ch():
    return clickhouse_connect.get_client(host=CH_HOST, port=8123, username=CH_USER, password=CH_PASSWORD)


def test_event_reaches_clickhouse(ch):
    event_id = str(uuid.uuid4())
    event = {
        "event_id": event_id,
        "user_id": f"test-user-{event_id[:8]}",
        "movie_id": "movie-test",
        "event_type": "VIEW_STARTED",
        "timestamp": "2026-01-15T12:00:00.123Z",
        "device_type": "TV",
        "session_id": f"test-session-{event_id[:8]}",
        "progress_seconds": 42,
    }
    try:
        r = requests.post(f"{MOVIE_SERVICE}/events", json=event, timeout=30)
        assert r.status_code == 201, r.text
        assert r.json()["event_id"] == event_id

        rows = []
        for _ in range(60):  # ждём до 60 секунд, пока событие доедет до ClickHouse
            rows = ch.query(
                "SELECT user_id, movie_id, event_type, timestamp, device_type, session_id, progress_seconds "
                "FROM cinema.movie_events WHERE event_id = {id:UUID}",
                parameters={"id": event_id},
            ).result_rows
            if rows:
                break
            time.sleep(1)
        assert len(rows) == 1, "событие не появилось в ClickHouse"

        user_id, movie_id, event_type, ts, device, session_id, progress = rows[0]
        assert user_id == event["user_id"]
        assert movie_id == "movie-test"
        assert event_type == "VIEW_STARTED"
        ts = ts.astimezone(timezone.utc) if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
        assert ts == datetime(2026, 1, 15, 12, 0, 0, 123000, tzinfo=timezone.utc)
        assert device == "TV"
        assert session_id == event["session_id"]
        assert progress == 42
    finally:
        # Чистим за собой, чтобы тест можно было запускать многократно
        ch.command("ALTER TABLE cinema.movie_events DELETE WHERE event_id = {id:UUID} SETTINGS mutations_sync = 1",
                   parameters={"id": event_id})


def test_invalid_event_is_rejected():
    bad = {"user_id": "u1", "movie_id": "m1", "event_type": "NOT_AN_EVENT",
           "device_type": "TV", "session_id": "s1"}
    r = requests.post(f"{MOVIE_SERVICE}/events", json=bad, timeout=10)
    assert r.status_code == 422
