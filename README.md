# Online Cinema: Event Streaming + Analytics Pipeline

Pipeline аналитики онлайн-кинотеатра: **Movie Service → Kafka (Avro + Schema Registry) → ClickHouse → Aggregation Service → PostgreSQL → Grafana / S3 (MinIO)**. Язык: Python.

```
Movie Service ──Avro──> Kafka (2 брокера) ──Kafka Engine + MV──> ClickHouse (raw)
 (HTTP + генератор)      Schema Registry                              │
                                                                      ▼
Grafana <── агрегаты в ClickHouse <── Aggregation Service ──> PostgreSQL ──> S3 (MinIO)
```

## Запуск

```
docker compose up --build
```

Одна команда поднимает всё: 3 узла Kafka, Schema Registry, ClickHouse, PostgreSQL, MinIO, оба сервиса и Grafana. Топик, схема и миграции создаются автоматически. Через 1–2 минуты (идёт генерация истории за 9 дней) данные видны везде.

| Что | Адрес |
|---|---|
| Movie Service (Swagger) | http://localhost:8000/docs |
| Aggregation Service (Swagger) | http://localhost:8001/docs |
| Grafana (admin / admin) | http://localhost:3000 |
| Schema Registry | http://localhost:8081/subjects |
| ClickHouse HTTP | http://localhost:8123 (user `cinema`, пароль `cinema`) |
| MinIO консоль (minioadmin / minioadmin) | http://localhost:9001 |

Интеграционный тест (одна команда, без ручных шагов, можно запускать многократно):

```
docker compose run --rm tests
```

## Где что реализовано

| Пункт | Баллы | Где |
|---|---|---|
| 1. Схема, topic, ключ | 1 | `schema/movie_event.avsc`, `movie_service/init_kafka.py` |
| 2. Продюсер | 1 | `movie_service/main.py` (HTTP), `generator.py`, `kafka_io.py` |
| 3. ClickHouse | 1 | `clickhouse/migrations/01_kafka_ingest.sql` |
| 4. Интеграционный тест | 1 | `tests/test_pipeline.py` |
| 5. Агрегация + PostgreSQL | 3 | `aggregation_service/`, `clickhouse/migrations/02_aggregates.sql`, `db/migration/V1__daily_metrics.sql` |
| 6. Grafana | 1 | `grafana/` |
| 7. Экспорт в S3 | 1 | `aggregation_service/exporter.py` |
| 8. Kafka-кластер, health checks | 1 | `docker-compose.yml` |

## Блок 1–4

**Схема (п. 1).** Avro, все поля из задания; `event_id` — `uuid`, `timestamp` — `timestamp-millis` (UTC), `event_type` и `device_type` — enum. Init-шаг `init` регистрирует схему в Schema Registry (subject `movie-events-value`, совместимость BACKWARD) и создаёт topic `movie-events`: 3 партиции, `replication.factor=2`, `min.insync.replicas=1`. Версии схемы: http://localhost:8081/subjects/movie-events-value/versions.

**Ключ партиционирования: `user_id`.** Все события одного пользователя попадают в одну партицию, поэтому порядок внутри сессии сохраняется (VIEW_STARTED всегда раньше VIEW_FINISHED). Ключ `movie_id` дал бы перекос: популярные фильмы создавали бы «горячие» партиции, а порядок событий одного пользователя терялся бы. Все наши метрики (DAU, retention, сессии) считаются по пользователям.

**Продюсер (п. 2).**
- `POST /events` — принимает JSON. Невалидное событие (неизвестный enum, пустой `user_id`, лишние поля) → 422. Успех → 201 и `event_id`.
- Валидация: Pydantic-модель, зеркало схемы, затем сериализация по Avro-схеме из Schema Registry (несоответствие → ошибка).
- `acks=all` + идемпотентный продюсер; при ошибке публикации retry с exponential backoff (0,2 → 0,4 → 0,8 → 1,6 с, 5 попыток), затем 503.
- Лог на каждое событие: `published event_id=... event_type=... timestamp=... partition=... offset=...`.
- Генератор (`GENERATOR_ENABLED=true`): сессии `[SEARCHED] → VIEW_STARTED → (PAUSED → RESUMED)* → VIEW_FINISHED → [LIKED]`, `progress_seconds` растёт, около 30% зрителей бросают просмотр. При первом запуске один раз создаёт историю за 9 дней (иначе retention пустой), потом идёт поток в реальном времени (2 события/с). Для `SEARCHED` `movie_id` пустой.

**ClickHouse (п. 3).** `movie_events_queue` (Kafka Engine, формат `AvroConfluent`) → материализованное представление `movie_events_mv` → `movie_events` (MergeTree, партиции по месяцам, сортировка `(event_date, event_type, user_id, timestamp)` под запросы по дню и типу события). Типы соответствуют схеме: uuid → UUID, enum → Enum8, timestamp-millis → DateTime64(3), int → Int32.

**Тест (п. 4).** Публикует событие через HTTP, ждёт до 60 с, пока оно появится в `cinema.movie_events`, сверяет все поля, затем удаляет свою запись. Второй тест проверяет, что невалидное событие отклоняется (422).

## Блок 5–7

**Aggregation Service** — отдельный контейнер, данные берёт только из ClickHouse.
- Расписание: каждые `AGG_INTERVAL_SECONDS` (по умолчанию 60) пересчитывает последние `AGG_LOOKBACK_DAYS` дней (по умолчанию 9: чтобы retention по недавним когортам дозревал).
- Ручной пересчёт: `POST http://localhost:8001/recalculate?date=2026-11-20`.
- Логи: `aggregation started/finished: date=... raw_events=... elapsed=...`, и сводка по циклу.

**Метрики** (запросы в `aggregates.py`, функции `uniqExact`, `uniqExactIf`, `countIf`, `avgIf`, `dateDiff`, JOIN):

| Метрика | Как считается |
|---|---|
| `dau` | уникальные `user_id` за день (любые события) |
| `avg_watch_seconds` | среднее `progress_seconds` у `VIEW_FINISHED` |
| `movie_views` | количество `VIEW_STARTED` по фильму (в PostgreSQL — топ-10) |
| `view_conversion` | уникальные сессии с `VIEW_FINISHED` / уникальные сессии с `VIEW_STARTED` |
| `retention_d1`, `retention_d7` | доля пользователей когорты (день первого `VIEW_STARTED`), которые смотрели снова через 1 и 7 дней |

Retention и когорты считаются по просмотрам (`VIEW_STARTED`). Последняя диагональ когортной таблицы неполная: сегодняшний день ещё идёт.

**Хранение.** В ClickHouse: `agg_daily_metrics` и `agg_retention_cohorts` (ReplacingMergeTree по `computed_at`: пересчёт заменяет старые строки, читаем с `FINAL`). В PostgreSQL: `daily_metrics(metric_date, metric, dimension, value, computed_at)` с ключом `(metric_date, metric, dimension)`. Запись — `INSERT ... ON CONFLICT DO UPDATE`, поэтому повторный пересчёт обновляет значения, дублей нет. Сбой записи → 3 попытки с backoff, затем ошибка в логе (цикл продолжается).

**Экспорт в S3 (п. 7).** CSV со всеми метриками дня (`metric_date, metric, dimension, value, computed_at, exported_at`) → `s3://movie-analytics/daily/YYYY-MM-DD/aggregates.csv`. Ключ один и тот же, поэтому повторный экспорт перезаписывает файл. Запуск по расписанию (`EXPORT_INTERVAL_SECONDS`, в compose 300 с для демо, в бою 86400; выгружает вчерашний день) и вручную: `POST http://localhost:8001/export?date=2026-11-20`. Если PostgreSQL или S3 недоступны — ошибка в логе и повтор при следующем запуске.

## Блок 8–10

**Grafana.** Datasource ClickHouse и дашборд «Online Cinema Analytics» создаются автоматически. Все панели читают таблицы агрегатов, не raw-события:
- **Retention Cohort Heatmap** — таблица с цветовой заливкой: строки — когорты по дате первого просмотра, столбцы — Day 0…Day 7, ячейки — % вернувшихся;
- DAU, конверсия просмотра, топ-10 фильмов.

**Kafka.** Контроллер + 2 брокера (`kafka-1`, `kafka-2`), topic с `replication.factor=2`, `min.insync.replicas=1`, Schema Registry (его служебный topic тоже с репликацией 2). Health checks есть у всех долгоживущих сервисов (Kafka, Schema Registry, ClickHouse, PostgreSQL, MinIO, оба сервиса, Grafana); порядок старта задан через `depends_on: condition: service_healthy`.

## Как показать

1. `docker compose up --build`, подождать 1–2 минуты. `docker compose ps` — сервисы healthy.
2. Topic и схема: `docker compose exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --describe --topic movie-events` (3 партиции, `ReplicationFactor: 2`); http://localhost:8081/subjects/movie-events-value/versions.
3. Отправить событие в Swagger `POST /events`; в логах `docker compose logs movie-service` — строка `published ...`. Событие с неверным `event_type` даёт 422.
4. ClickHouse: `docker compose exec clickhouse clickhouse-client --user cinema --password cinema -q "SELECT event_type, count() FROM cinema.movie_events GROUP BY event_type"`.
5. Тест: `docker compose run --rm tests`.
6. Агрегаты в PostgreSQL: `docker compose exec postgres psql -U metrics -d metrics -c "SELECT * FROM daily_metrics WHERE metric IN ('dau','view_conversion','retention_d1','retention_d7') ORDER BY metric_date, metric"`. Логи пересчёта: `docker compose logs aggregation-service`.
7. Идемпотентность: дважды вызвать `POST /recalculate?date=...`, затем `SELECT count(*) FROM daily_metrics` — число строк не меняется, `computed_at` обновляется.
8. Grafana → дашборд (если панели пустые, подождать минуту и обновить страницу).
9. Экспорт: `POST /export?date=<вчера>`, затем в MinIO консоли открыть бакет `movie-analytics`, папку `daily/<дата>/`. Повторный вызов перезаписывает файл.
10. Отказоустойчивость: `docker compose stop kafka-2` — `POST /events` продолжает работать (leader переезжает на `kafka-1`); `docker compose start kafka-2`.

## Допущения и ограничения

- Схема `SEARCHED` не содержит фильма, поэтому для него `movie_id` пустой; для всех остальных событий он обязателен.
- Контроллер Kafka один (отдельный узел): отказ брокера переживается, отказ контроллера — нет. Для учебного задания достаточно.
- Доставка «минимум один раз»; идемпотентный продюсер защищает от дублей при повторных отправках внутри клиента, но не при повторном вызове `POST /events` с новым `event_id`.
- Все метрики дня считаются по UTC.
