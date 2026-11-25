CREATE DATABASE IF NOT EXISTS cinema;

-- 1. Kafka Engine: читает топик movie-events (Avro + Schema Registry).
--    Типы совпадают со схемой: uuid -> UUID, enum -> Enum8, timestamp-millis -> DateTime64(3), int -> Int32.
CREATE TABLE IF NOT EXISTS cinema.movie_events_queue
(
    event_id         UUID,
    user_id          String,
    movie_id         String,
    event_type       Enum8('VIEW_STARTED' = 1, 'VIEW_FINISHED' = 2, 'VIEW_PAUSED' = 3,
                           'VIEW_RESUMED' = 4, 'LIKED' = 5, 'SEARCHED' = 6),
    timestamp        DateTime64(3, 'UTC'),
    device_type      Enum8('MOBILE' = 1, 'DESKTOP' = 2, 'TV' = 3, 'TABLET' = 4),
    session_id       String,
    progress_seconds Int32
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-1:19092,kafka-2:19092',
    kafka_topic_list = 'movie-events',
    kafka_group_name = 'clickhouse-movie-events',
    kafka_format = 'AvroConfluent',
    kafka_num_consumers = 3,
    format_avro_schema_registry_url = 'http://schema-registry:8081';

-- 2. Постоянное хранилище.
--    Партиции по месяцам; сортировка под запросы агрегатов (фильтр по дате и типу события, затем пользователь).
CREATE TABLE IF NOT EXISTS cinema.movie_events
(
    event_id         UUID,
    user_id          String,
    movie_id         String,
    event_type       Enum8('VIEW_STARTED' = 1, 'VIEW_FINISHED' = 2, 'VIEW_PAUSED' = 3,
                           'VIEW_RESUMED' = 4, 'LIKED' = 5, 'SEARCHED' = 6),
    timestamp        DateTime64(3, 'UTC'),
    device_type      Enum8('MOBILE' = 1, 'DESKTOP' = 2, 'TV' = 3, 'TABLET' = 4),
    session_id       String,
    progress_seconds Int32,
    event_date       Date MATERIALIZED toDate(timestamp),
    ingested_at      DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (event_date, event_type, user_id, timestamp);

-- 3. Автоматическая перекладка Kafka -> MergeTree.
CREATE MATERIALIZED VIEW IF NOT EXISTS cinema.movie_events_mv
TO cinema.movie_events AS
SELECT event_id, user_id, movie_id, event_type, timestamp, device_type, session_id, progress_seconds
FROM cinema.movie_events_queue;
