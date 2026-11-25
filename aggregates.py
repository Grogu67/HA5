"""Расчёт бизнес-метрик за один день по raw-событиям ClickHouse (таблица cinema.movie_events)."""
import logging
import math
import os
from datetime import date, datetime, timedelta, timezone

import clickhouse_connect

log = logging.getLogger("aggregates")

RETENTION_DAYS = 7


def ch_client():
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "clickhouse"), port=8123,
        username=os.getenv("CLICKHOUSE_USER", "cinema"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "cinema"),
    )


# DAU, просмотры, конверсия (по уникальным сессиям), среднее время просмотра завершённых
Q_DAY = """
SELECT
    count()                                                    AS raw_events,
    uniqExact(user_id)                                         AS dau,
    uniqExactIf(session_id, event_type = 'VIEW_STARTED')       AS started,
    uniqExactIf(session_id, event_type = 'VIEW_FINISHED')      AS finished,
    avgIf(progress_seconds, event_type = 'VIEW_FINISHED')      AS avg_watch_seconds
FROM cinema.movie_events
WHERE event_date = {d:Date}
"""

# Рейтинг фильмов по количеству просмотров (VIEW_STARTED)
Q_MOVIES = """
SELECT movie_id, count() AS views
FROM cinema.movie_events
WHERE event_date = {d:Date} AND event_type = 'VIEW_STARTED' AND movie_id != ''
GROUP BY movie_id
ORDER BY views DESC
"""

# Когорта дня {d}: пользователи, чей ПЕРВЫЙ VIEW_STARTED пришёлся на {d}.
# Для каждого дня жизни 0..7 считаем, сколько из них снова смотрело в этот день.
Q_RETENTION = """
SELECT
    dateDiff('day', {d:Date}, e.event_date) AS day_n,
    uniqExact(e.user_id)                    AS users
FROM cinema.movie_events AS e
INNER JOIN
(
    SELECT user_id
    FROM cinema.movie_events
    WHERE event_type = 'VIEW_STARTED' AND event_date <= {d:Date}
    GROUP BY user_id
    HAVING min(event_date) = {d:Date}
) AS c ON e.user_id = c.user_id
WHERE e.event_type = 'VIEW_STARTED'
  AND e.event_date BETWEEN {d:Date} AND {d:Date} + 7
GROUP BY day_n
ORDER BY day_n
"""


def _num(x) -> float:
    x = float(x)
    return 0.0 if math.isnan(x) or math.isinf(x) else x


def aggregate_day(d: date) -> dict:
    """Считает метрики за день d, пишет их в ClickHouse. Возвращает данные для выгрузки в PostgreSQL."""
    client = ch_client()
    p = {"d": d}
    computed_at = datetime.now(timezone.utc).replace(tzinfo=None)  # UTC; без tzinfo для вставки в DateTime64

    raw_events, dau, started, finished, avg_watch = client.query(Q_DAY, parameters=p).result_rows[0]
    metrics = [
        ("dau", "", _num(dau)),
        ("views_started", "", _num(started)),
        ("views_finished", "", _num(finished)),
        ("view_conversion", "", _num(finished) / started if started else 0.0),
        ("avg_watch_seconds", "", _num(avg_watch)),
    ]
    movies = client.query(Q_MOVIES, parameters=p).result_rows
    movie_metrics = [("movie_views", mid, float(v)) for mid, v in movies]

    # Retention: показываем только те дни жизни, которые уже наступили
    today = datetime.now(timezone.utc).date()
    by_day = dict(client.query(Q_RETENTION, parameters=p).result_rows)
    cohort_size = by_day.get(0, 0)
    cohort_rows = []
    if cohort_size > 0:
        for n in range(RETENTION_DAYS + 1):
            if d + timedelta(days=n) > today:
                break
            returned = by_day.get(n, 0)
            cohort_rows.append([d, n, cohort_size, returned, 100.0 * returned / cohort_size, computed_at])
        by_n = {r[1]: r[4] for r in cohort_rows}
        metrics.append(("cohort_size", "", float(cohort_size)))
        if 1 in by_n:
            metrics.append(("retention_d1", "", by_n[1] / 100.0))
        if 7 in by_n:
            metrics.append(("retention_d7", "", by_n[7] / 100.0))

    # Материализация в ClickHouse (ReplacingMergeTree: повторный пересчёт заменяет прежние строки)
    all_metrics = metrics + movie_metrics
    client.insert("cinema.agg_daily_metrics",
                  [[d, m, dim, v, computed_at] for m, dim, v in all_metrics],
                  column_names=["metric_date", "metric", "dimension", "value", "computed_at"])
    if cohort_rows:
        client.insert("cinema.agg_retention_cohorts", cohort_rows,
                      column_names=["cohort_date", "day_n", "cohort_size", "returned_users",
                                    "retention_pct", "computed_at"])
    return {
        "raw_events": int(raw_events),
        "metrics": metrics,
        "movie_metrics": movie_metrics,
        "computed_at": computed_at.replace(tzinfo=timezone.utc),
    }
