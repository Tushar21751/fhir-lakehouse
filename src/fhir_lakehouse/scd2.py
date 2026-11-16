"""Generic SCD type 2 on top of Delta MERGE.

A batch can hold several versions of the same key (e.g. two daily loads processed in
one go), so versions are ordered by effective_from and chained, not just "latest wins".
"""
from pyspark.sql import Window
from pyspark.sql import functions as F

HIGH_DATE = "9999-12-31 00:00:00"
SCD_COLUMNS = ["record_hash", "effective_from", "effective_to", "is_current", "scd_version",
               "silver_loaded_at", "silver_updated_at", "load_run_id"]


def compute_changes(batch, current, key):
    """Work out what has to change in the target.

    batch   - candidate rows with key, record_hash, effective_from, version_seq
    current - open rows of the target (key, effective_from, record_hash, scd_version), or None
    Returns (inserts, closes):
      inserts - new version rows with effective_to / is_current / scd_version filled in
      closes  - (key, close_at) for every key whose open row has to be closed
    """
    if current is None:
        current = batch.sparkSession.createDataFrame(
            [], f"{key} string, effective_from timestamp, record_hash string, scd_version int")

    cur = current.select(
        F.col(key).alias("_cur_key"),
        F.col("effective_from").alias("_cur_from"),
        F.col("record_hash").alias("_cur_hash"),
        F.col("scd_version").alias("_cur_version"),
    )
    b = batch.join(cur, batch[key] == cur["_cur_key"], "left").drop("_cur_key")

    # anything not newer than the open row is a copy we've already got (re-runs, reloads)
    b = b.where(F.col("_cur_from").isNull() | (F.col("effective_from") > F.col("_cur_from")))

    w = Window.partitionBy(key).orderBy(F.col("effective_from"), F.col("version_seq").asc_nulls_first())

    # drop versions whose tracked columns didn't change compared to the previous one
    b = b.withColumn("_prev_hash", F.coalesce(F.lag("record_hash").over(w), F.col("_cur_hash")))
    b = b.where(F.col("_prev_hash").isNull() | (F.col("_prev_hash") != F.col("record_hash")))

    nxt = F.lead("effective_from").over(w)
    b = (b.withColumn("_rn", F.row_number().over(w))
          .withColumn("effective_to", F.coalesce(nxt, F.lit(HIGH_DATE).cast("timestamp")))
          .withColumn("is_current", nxt.isNull())
          .withColumn("scd_version", (F.coalesce(F.col("_cur_version"), F.lit(0)) + F.col("_rn")).cast("int")))

    closes = (b.where((F.col("_rn") == 1) & F.col("_cur_version").isNotNull())
               .select(key, F.col("effective_from").alias("close_at")))
    inserts = b.drop("_cur_from", "_cur_hash", "_cur_version", "_prev_hash", "_rn")
    return inserts, closes


def apply_scd2(lh, table, batch, key, run_id, columns):
    """Apply a prepared batch to `table` in one MERGE (close old versions + insert new ones).

    columns - business + lineage columns in the order they should be stored
    """
    from delta.tables import DeltaTable

    exists = lh.exists(table)
    current = None
    if exists:
        current = (lh.read(table).where(F.col("is_current"))
                     .select(key, "effective_from", "record_hash", "scd_version"))

    inserts, closes = compute_changes(batch, current, key)
    inserts = (inserts
               .withColumn("silver_loaded_at", F.current_timestamp())
               .withColumn("silver_updated_at", F.lit(None).cast("timestamp"))
               .withColumn("load_run_id", F.lit(run_id))
               .select(*columns, *SCD_COLUMNS)
               .persist())
    closes = closes.persist()

    stats = {
        "rows_inserted": inserts.count(),
        "rows_closed": closes.count(),
        "new_keys": inserts.where(F.col("scd_version") == 1).count(),
    }

    if not exists:
        lh.append(inserts, table)  # first load (also creates the table when the batch is empty)
    elif stats["rows_inserted"]:
        # insert rows get a null merge key so they never match; close rows match the open version
        staged = (inserts.withColumn("_merge_key", F.lit(None).cast("string"))
                  .unionByName(closes.select(F.col(key).alias("_merge_key"),
                                             F.col("close_at").alias("effective_from")),
                               allowMissingColumns=True))
        (DeltaTable.forPath(lh.spark, lh.table_path(table)).alias("t")
            .merge(staged.alias("s"), f"t.{key} = s._merge_key AND t.is_current = true")
            .whenMatchedUpdate(set={
                "effective_to": "s.effective_from",
                "is_current": "false",
                "silver_updated_at": "current_timestamp()",
            })
            .whenNotMatchedInsert(condition="s._merge_key IS NULL",
                                  values={c: f"s.{c}" for c in inserts.columns})
            .execute())

    inserts.unpersist()
    closes.unpersist()
    return stats
