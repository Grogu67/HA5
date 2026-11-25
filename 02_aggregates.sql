-- Материализованные агрегаты. ReplacingMergeTree(computed_at): повторный пересчёт за ту же дату
-- вставляет строки с тем же ключом и новым computed_at, при чтении (FINAL) остаётся последняя версия.

-- Метрики по дням в «длинном» виде: одна строка = одна метрика (и, если нужно, измерение, например фильм).
CREATE TABLE IF NOT EXISTS cinema.agg_daily_metrics
(
    metric_date Date,
    metric      LowCardinality(String),
    dimension   String DEFAULT '',
    value       Float64,
    computed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(metric_date)
ORDER BY (metric_date, metric, dimension);

-- Когортный retention: когорта = день первого VIEW_STARTED пользователя, day_n = день жизни 0..7.
CREATE TABLE IF NOT EXISTS cinema.agg_retention_cohorts
(
    cohort_date    Date,
    day_n          UInt8,
    cohort_size    UInt32,
    returned_users UInt32,
    retention_pct  Float64,
    computed_at    DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (cohort_date, day_n);
