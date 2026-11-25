"""Хранилище готовых метрик в PostgreSQL. Запись идемпотентна: UPSERT по (metric_date, metric, dimension)."""
import os
from datetime import date

import psycopg2
from psycopg2.extras import execute_values

TOP_N = int(os.getenv("TOP_MOVIES_N", "10"))


def connect():
    return psycopg2.connect(os.getenv("POSTGRES_DSN", "host=postgres dbname=metrics user=metrics password=metrics"),
                            connect_timeout=5)


def save_day(d: date, result: dict) -> int:
    """Сохраняет метрики дня одной транзакцией. Возвращает число записанных строк."""
    computed_at = result["computed_at"]
    top = sorted(result["movie_metrics"], key=lambda r: -r[2])[:TOP_N]  # в PG только топ-N фильмов
    rows = [(d, m, dim, v, computed_at) for m, dim, v in result["metrics"] + top]
    with connect() as conn, conn.cursor() as cur:  # with conn: commit при успехе, rollback при ошибке
        execute_values(cur, """
            INSERT INTO daily_metrics (metric_date, metric, dimension, value, computed_at)
            VALUES %s
            ON CONFLICT (metric_date, metric, dimension)
            DO UPDATE SET value = EXCLUDED.value, computed_at = EXCLUDED.computed_at
        """, rows)
        # фильм мог выпасть из топа после пересчёта: убираем устаревшие строки
        cur.execute("DELETE FROM daily_metrics WHERE metric_date = %s AND metric = 'movie_views' "
                    "AND dimension <> ALL(%s)", (d, [dim for _, dim, _ in top] or [""]))
    conn.close()
    return len(rows)


def fetch_day(d: date) -> list:
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT metric_date, metric, dimension, value, computed_at FROM daily_metrics "
                    "WHERE metric_date = %s ORDER BY metric, dimension", (d,))
        rows = cur.fetchall()
    conn.close()
    return rows
