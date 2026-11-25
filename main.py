"""Aggregation Service: считает метрики из ClickHouse, пишет в PostgreSQL, экспортирует в S3."""
import logging
import os
import threading
import time
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException

import aggregates
import exporter
import pg_store
from util import with_retry

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("aggregation")

AGG_INTERVAL = int(os.getenv("AGG_INTERVAL_SECONDS", "60"))        # как часто пересчитывать
LOOKBACK_DAYS = int(os.getenv("AGG_LOOKBACK_DAYS", "9"))           # сколько последних дней пересчитывать
EXPORT_INTERVAL = int(os.getenv("EXPORT_INTERVAL_SECONDS", "3600"))  # как часто выгружать в S3
EXPORT_DAYS_BACK = int(os.getenv("EXPORT_DAYS_BACK", "1"))         # 1 = вчера

_lock = threading.Lock()  # пересчёты не идут параллельно


def process_date(d: date) -> dict:
    """Один цикл за день: ClickHouse -> метрики -> ClickHouse (агрегаты) -> PostgreSQL."""
    with _lock:
        t0 = time.time()
        log.info("aggregation started: date=%s", d)
        result = aggregates.aggregate_day(d)
        rows = with_retry(lambda: pg_store.save_day(d, result), f"save aggregates {d} to PostgreSQL")
        elapsed = time.time() - t0
        log.info("aggregation finished: date=%s raw_events=%d rows_saved=%d elapsed=%.2fs",
                 d, result["raw_events"], rows, elapsed)
        return {"date": d.isoformat(), "raw_events": result["raw_events"], "rows_saved": rows,
                "elapsed_seconds": round(elapsed, 2)}


def aggregation_loop():
    while True:
        today = datetime.now(timezone.utc).date()
        dates = [today - timedelta(days=i) for i in range(LOOKBACK_DAYS, -1, -1)]
        t0 = time.time()
        log.info("cycle started: %s .. %s", dates[0], dates[-1])
        total = 0
        for d in dates:
            try:
                total += process_date(d)["raw_events"]
            except Exception as e:  # сбой одного дня не останавливает цикл
                log.error("aggregation failed: date=%s error=%s", d, e)
        log.info("cycle finished: days=%d raw_events_processed=%d elapsed=%.2fs", len(dates), total, time.time() - t0)
        time.sleep(AGG_INTERVAL)


def export_loop():
    while True:
        time.sleep(EXPORT_INTERVAL)
        d = datetime.now(timezone.utc).date() - timedelta(days=EXPORT_DAYS_BACK)
        try:
            exporter.export_date(d)
        except Exception as e:  # ошибка логируется, повтор — при следующем запуске
            log.error("export failed: date=%s error=%s (retry on next run)", d, e)


@asynccontextmanager
async def lifespan(app: FastAPI):
    threading.Thread(target=aggregation_loop, daemon=True).start()
    threading.Thread(target=export_loop, daemon=True).start()
    yield


app = FastAPI(title="Aggregation Service", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/recalculate")
def recalculate(date: Optional[date] = None):
    """Ручной пересчёт метрик за дату (YYYY-MM-DD). Без параметра — за сегодня (UTC)."""
    d = date or datetime.now(timezone.utc).date()
    try:
        return process_date(d)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/export")
def export(date: Optional[date] = None):
    """Ручной экспорт агрегатов за дату в S3. Без параметра — за вчера (UTC)."""
    d = date or datetime.now(timezone.utc).date() - timedelta(days=1)
    try:
        return exporter.export_date(d)
    except LookupError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
