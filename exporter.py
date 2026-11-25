"""Экспорт агрегатов за день из PostgreSQL в S3 (MinIO): s3://movie-analytics/daily/YYYY-MM-DD/aggregates.csv"""
import csv
import io
import logging
import os
from datetime import date, datetime, timezone

import boto3
from botocore.config import Config

import pg_store
from util import with_retry

log = logging.getLogger("exporter")

BUCKET = os.getenv("S3_BUCKET", "movie-analytics")


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("S3_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY", "minioadmin"),
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}, retries={"max_attempts": 1}),
    )


def ensure_bucket(s3):
    names = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    if BUCKET not in names:
        s3.create_bucket(Bucket=BUCKET)
        log.info("bucket %s created", BUCKET)


def export_date(d: date) -> dict:
    """Читает агрегаты дня из PG и перезаписывает файл в S3. Возвращает {key, rows}."""
    rows = with_retry(lambda: pg_store.fetch_day(d), f"read aggregates {d} from PostgreSQL")
    if not rows:
        raise LookupError(f"no aggregates for {d} in PostgreSQL")

    exported_at = datetime.now(timezone.utc).isoformat()
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["metric_date", "metric", "dimension", "value", "computed_at", "exported_at"])
    for metric_date, metric, dimension, value, computed_at in rows:
        w.writerow([metric_date.isoformat(), metric, dimension, value, computed_at.isoformat(), exported_at])

    key = f"daily/{d.isoformat()}/aggregates.csv"  # один и тот же ключ -> повторный экспорт перезаписывает файл

    def upload():
        s3 = s3_client()
        ensure_bucket(s3)
        s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue().encode("utf-8"), ContentType="text/csv")

    with_retry(upload, f"upload {key} to S3")
    log.info("exported s3://%s/%s rows=%d", BUCKET, key, len(rows))
    return {"key": f"s3://{BUCKET}/{key}", "rows": len(rows)}
