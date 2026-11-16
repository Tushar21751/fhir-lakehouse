"""raw files -> bronze delta tables (one row per resource, payload kept as json text)."""
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .config import ctl_table, table_name
from .control import API_CALL_LOG
from .storage import ship_package_to_executors

# Only the envelope is parsed here. `resource` is declared as a string so Spark keeps the
# whole resource as json text - no schema inference, nothing breaks when HAPI data drifts.
ENTRY = T.StructType([
    T.StructField("fullUrl", T.StringType()),
    T.StructField("resource", T.StringType()),
    T.StructField("search", T.StructType([T.StructField("mode", T.StringType())])),
])
BUNDLE_SCHEMA = T.StructType([
    T.StructField("resourceType", T.StringType()),
    T.StructField("id", T.StringType()),
    T.StructField("entry", T.ArrayType(ENTRY)),
])
KEYS_SCHEMA = T.StructType([
    T.StructField("resourceType", T.StringType()),
    T.StructField("id", T.StringType()),
    T.StructField("meta", T.StructType([
        T.StructField("versionId", T.StringType()),
        T.StructField("lastUpdated", T.StringType()),
    ])),
])

BRONZE_SCHEMA = T.StructType([
    T.StructField("resource_type", T.StringType()),
    T.StructField("resource_id", T.StringType()),
    T.StructField("version_id", T.StringType()),
    T.StructField("last_updated", T.TimestampType()),
    T.StructField("resource_json", T.StringType()),
    T.StructField("payload_hash", T.StringType()),
    T.StructField("full_url", T.StringType()),
    T.StructField("search_mode", T.StringType()),
    T.StructField("response_format", T.StringType()),
    T.StructField("extraction_timestamp", T.TimestampType()),
    T.StructField("api_url_or_params", T.StringType()),
    T.StructField("window_start", T.TimestampType()),
    T.StructField("window_end", T.TimestampType()),
    T.StructField("page_number", T.IntegerType()),
    T.StructField("raw_path", T.StringType()),
    T.StructField("raw_key", T.StringType()),
    T.StructField("raw_saved_at", T.TimestampType()),
    T.StructField("ingest_run_id", T.StringType()),
    T.StructField("load_run_id", T.StringType()),
    T.StructField("bronze_loaded_at", T.TimestampType()),
    T.StructField("load_date", T.DateType()),
])


def empty_bronze(spark):
    return spark.createDataFrame([], BRONZE_SCHEMA)


def conform(df, schema):
    return df.select([F.col(f.name).cast(f.dataType).alias(f.name) for f in schema.fields])


def pending_files(lh, cfg, resource_type):
    """Pages that were saved by ingestion but are not in bronze yet."""
    calls = lh.read(ctl_table(cfg, API_CALL_LOG)).where(
        (F.col("resource_type") == resource_type)
        & (F.col("status") == "OK")
        & (F.col("entry_count") > 0)
    )
    bronze = table_name(cfg, "bronze", resource_type)
    if lh.exists(bronze):
        calls = calls.join(lh.read(bronze).select("raw_key").distinct(), "raw_key", "left_anti")
    return calls.dropDuplicates(["raw_key"])


def _entries(spark, paths, fmt):
    # wholetext has to go through text() itself - its default (False) overrides .option("wholetext")
    text = (spark.read.text(paths, wholetext=True)
                 .select(F.regexp_extract("_metadata.file_path", r"([^/]+/[^/]+)$", 1).alias("raw_key"),
                         "value"))
    if fmt == "xml":
        from .fhir_xml import bundle_entries_or_none

        ship_package_to_executors(spark)
        parse = F.udf(bundle_entries_or_none, T.ArrayType(ENTRY))
        entries = parse("value")
    else:
        entries = F.from_json("value", BUNDLE_SCHEMA).getField("entry")
    return text.select("raw_key", F.explode(entries).alias("e"))


def raw_to_bronze(lh, cfg, resource_type, run_id, log=print):
    spark = lh.spark
    bronze = table_name(cfg, "bronze", resource_type)
    stats = {"files": 0, "rows_read": 0, "rows_written": 0, "rows_skipped": 0, "files_with_mismatch": 0}

    if not lh.exists(ctl_table(cfg, API_CALL_LOG)):
        pending = []
    else:
        sel = pending_files(lh, cfg, resource_type).select(
            "raw_key", "raw_path", "response_format", "run_id", "window_start", "window_end",
            "page_number", "entry_count", "api_url_or_params", "called_at", "saved_at",
        )
        pending = sel.collect()

    if not pending:
        if not lh.exists(bronze):
            lh.append(empty_bronze(spark), bronze)
        log(f"{resource_type}: no new raw files")
        return stats

    meta = spark.createDataFrame(pending, sel.schema)
    stats["files"] = len(pending)
    formats = sorted({r["response_format"] for r in pending})
    entries = reduce(DataFrame.unionByName, [
        _entries(spark, [r["raw_path"] for r in pending if r["response_format"] == fmt], fmt)
        for fmt in formats
    ])

    rows = (
        entries
        .select("raw_key",
                F.col("e.fullUrl").alias("full_url"),
                F.col("e.search.mode").alias("search_mode"),
                F.col("e.resource").alias("resource_json"))
        .withColumn("k", F.from_json("resource_json", KEYS_SCHEMA))
        .join(F.broadcast(meta), "raw_key")
        .select(
            F.col("k.resourceType").alias("resource_type"),
            F.col("k.id").alias("resource_id"),
            F.col("k.meta.versionId").alias("version_id"),
            F.expr("try_cast(k.meta.lastUpdated AS timestamp)").alias("last_updated"),
            "resource_json",
            F.sha2("resource_json", 256).alias("payload_hash"),
            "full_url",
            "search_mode",
            "response_format",
            F.col("called_at").alias("extraction_timestamp"),
            "api_url_or_params",
            "window_start",
            "window_end",
            "page_number",
            "raw_path",
            "raw_key",
            F.col("saved_at").alias("raw_saved_at"),
            F.col("run_id").alias("ingest_run_id"),
            F.lit(run_id).alias("load_run_id"),
            F.current_timestamp().alias("bronze_loaded_at"),
            F.to_date("window_start").alias("load_date"),
            "entry_count",
        )
        .persist()
    )

    # sanity check: did we parse as many entries as the api call log says each page had?
    per_file = rows.groupBy("raw_key", "entry_count").count()
    stats["files_with_mismatch"] = per_file.where(F.col("count") != F.col("entry_count")).count()
    stats["files_with_mismatch"] += len(pending) - per_file.count()

    # HAPI sometimes adds OperationOutcome entries (search mode "outcome") - not ours
    good = rows.where(
        (F.col("resource_type") == resource_type)
        & (F.coalesce(F.col("search_mode"), F.lit("match")) == "match")
        & F.col("resource_id").isNotNull()
    )
    stats["rows_read"] = rows.count()
    stats["rows_written"] = good.count()
    stats["rows_skipped"] = stats["rows_read"] - stats["rows_written"]

    lh.append(conform(good, BRONZE_SCHEMA), bronze)
    rows.unpersist()

    if stats["files_with_mismatch"]:
        log(f"  !! {stats['files_with_mismatch']} file(s) where parsed entries != entry_count in the call log")
    log(f"{resource_type}: {stats['files']} file(s) -> {stats['rows_written']} bronze rows "
        f"({stats['rows_skipped']} skipped)")
    return stats
