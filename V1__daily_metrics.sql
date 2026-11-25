-- Готовые метрики. Одна строка = метрика за дату (и измерение, например фильм).
CREATE TABLE daily_metrics (
    metric_date date             NOT NULL,
    metric      varchar(64)      NOT NULL,
    dimension   varchar(128)     NOT NULL DEFAULT '',
    value       double precision NOT NULL,
    computed_at timestamptz      NOT NULL,
    PRIMARY KEY (metric_date, metric, dimension)  -- ключ нужен для идемпотентного UPSERT
);

CREATE INDEX idx_daily_metrics_metric ON daily_metrics (metric, metric_date);
