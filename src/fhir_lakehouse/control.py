"""Control tables: API call log, step log and watermarks."""
import json
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql import types as T

from .config import ctl_table

API_CALL_LOG = "api_call_log"
LOAD_LOG = "load_log"
WATERMARK = "watermark"

API_CALL_SCHEMA = T.StructType([
    T.StructField("run_id", T.StringType()),
    T.StructField("resource_type", T.StringType()),
    T.StructField("response_format", T.StringType()),
    T.StructField("window_start", T.TimestampType()),
    T.StructField("window_end", T.TimestampType()),
    T.StructField("page_number", T.IntegerType()),
    T.StructField("api_url_or_params", T.StringType()),
    T.StructField("search_params", T.StringType()),
    T.StructField("http_status", T.IntegerType()),
    T.StructField("attempts", T.IntegerType()),
    T.StructField("entry_count", T.IntegerType()),
    T.StructField("response_bytes", T.LongType()),
    T.StructField("next_url", T.StringType()),
    T.StructField("bundle_id", T.StringType()),
    T.StructField("called_at", T.TimestampType()),
    T.StructField("received_at", T.TimestampType()),
    T.StructField("saved_at", T.TimestampType()),
    T.StructField("raw_path", T.StringType()),
    T.StructField("raw_key", T.StringType()),
    T.StructField("truncated", T.BooleanType()),
    T.StructField("status", T.StringType()),
    T.StructField("error", T.StringType()),
])

LOAD_LOG_SCHEMA = T.StructType([
    T.StructField("run_id", T.StringType()),
    T.StructField("step", T.StringType()),
    T.StructField("resource_type", T.StringType()),
    T.StructField("status", T.StringType()),
    T.StructField("started_at", T.TimestampType()),
    T.StructField("finished_at", T.TimestampType()),
    T.StructField("duration_sec", T.DoubleType()),
    T.StructField("rows_read", T.LongType()),
    T.StructField("rows_written", T.LongType()),
    T.StructField("rows_inserted", T.LongType()),
    T.StructField("rows_updated", T.LongType()),
    T.StructField("details", T.StringType()),
    T.StructField("error", T.StringType()),
])

WATERMARK_SCHEMA = T.StructType([
    T.StructField("layer", T.StringType()),
    T.StructField("resource_type", T.StringType()),
    T.StructField("watermark_ts", T.TimestampType()),
    T.StructField("run_id", T.StringType()),
    T.StructField("updated_at", T.TimestampType()),
])


def utcnow():
    return datetime.now(timezone.utc)


def new_run_id():
    return f"manual_{utcnow():%Y%m%dT%H%M%SZ}"


def _rows(records, schema):
    # dicts -> tuples in schema order, missing keys become null
    return [tuple(r.get(f.name) for f in schema.fields) for r in records]


def log_api_calls(lh, cfg, records):
    if not records:
        return
    df = lh.spark.createDataFrame(_rows(records, API_CALL_SCHEMA), API_CALL_SCHEMA)
    lh.append(df, ctl_table(cfg, API_CALL_LOG))


def get_watermark(lh, cfg, layer, resource_type):
    name = ctl_table(cfg, WATERMARK)
    if not lh.exists(name):
        return None
    row = (lh.read(name)
             .where((F.col("layer") == layer) & (F.col("resource_type") == resource_type))
             .select("watermark_ts")
             .first())
    if row is None or row[0] is None:
        return None
    # spark hands back a naive datetime in the driver's local tz
    return row[0].astimezone(timezone.utc)


def set_watermark(lh, cfg, layer, resource_type, ts, run_id):
    rec = {"layer": layer, "resource_type": resource_type, "watermark_ts": ts,
           "run_id": run_id, "updated_at": utcnow()}
    df = lh.spark.createDataFrame(_rows([rec], WATERMARK_SCHEMA), WATERMARK_SCHEMA)
    lh.upsert(df, ctl_table(cfg, WATERMARK), keys=["layer", "resource_type"])


class StepLog:
    """One row in ctl_load_log per notebook step."""

    def __init__(self, lh, cfg, run_id, step, resource_type=None):
        self.lh = lh
        self.cfg = cfg
        self.run_id = run_id
        self.step = step
        self.resource_type = resource_type
        self.started_at = utcnow()

    def finish(self, status, rows_read=None, rows_written=None, rows_inserted=None,
               rows_updated=None, details=None, error=None):
        finished_at = utcnow()
        rec = {
            "run_id": self.run_id,
            "step": self.step,
            "resource_type": self.resource_type,
            "status": status,
            "started_at": self.started_at,
            "finished_at": finished_at,
            "duration_sec": (finished_at - self.started_at).total_seconds(),
            "rows_read": rows_read,
            "rows_written": rows_written,
            "rows_inserted": rows_inserted,
            "rows_updated": rows_updated,
            "details": json.dumps(details, default=str) if details else None,
            "error": error[:4000] if error else None,
        }
        df = self.lh.spark.createDataFrame(_rows([rec], LOAD_LOG_SCHEMA), LOAD_LOG_SCHEMA)
        self.lh.append(df, ctl_table(self.cfg, LOAD_LOG))
        return rec
