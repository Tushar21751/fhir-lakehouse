"""silver -> gold: small star schema for reporting, rebuilt from the current silver rows."""
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .bronze import empty_bronze
from .config import gold_table, table_name
from .silver import MODELS

MIN_DATE, MAX_DATE = "1900-01-01", "2100-12-31"  # test server has some silly dates
SOURCE_LINEAGE = [
    F.col("version_id").alias("source_version_id"),
    F.col("last_updated").alias("source_last_updated"),
    "extraction_timestamp",
    "api_url_or_params",
]


def silver_current(lh, cfg, resource_type):
    name = table_name(cfg, "silver", resource_type)
    if lh.exists(name):
        return lh.read(name).where(F.col("is_current"))
    # not loaded yet -> empty frame with the right columns so gold still builds
    return MODELS[resource_type].flatten(empty_bronze(lh.spark))


def date_key(col):
    d = F.to_date(col)
    return F.when(d.between(F.lit(MIN_DATE).cast("date"), F.lit(MAX_DATE).cast("date")),
                  F.date_format(d, "yyyyMMdd").cast("int"))


def dim_patient(patients, referenced_ids):
    known = patients.select(
        "patient_id", "full_name", "family_name", "given_name",
        F.coalesce("gender", F.lit("unknown")).alias("gender"),
        "birth_date", "is_deceased", "deceased_at", "city", "state", "postal_code", "country",
        "marital_status", "language", "mrn", "active",
        *SOURCE_LINEAGE,
        F.lit(False).alias("is_inferred"),
    )
    # facts can point at patients that weren't updated inside the load window, add a
    # placeholder row for those so relationships in the model don't break
    inferred = (referenced_ids.join(known.select("patient_id"), "patient_id", "left_anti")
                .select("patient_id",
                        F.lit("Unknown (not loaded)").alias("full_name"),
                        F.lit("unknown").alias("gender"),
                        F.lit(True).alias("is_inferred")))
    dim = known.unionByName(inferred, allowMissingColumns=True)

    age = F.floor(F.months_between(F.coalesce(F.to_date("deceased_at"), F.current_date()),
                                   F.col("birth_date")) / 12).cast("int")
    dim = dim.withColumn("age", F.when(age >= 0, age))
    return dim.withColumn(
        "age_band",
        F.when(F.col("age").isNull(), "unknown")
         .when(F.col("age") < 18, "0-17")
         .when(F.col("age") < 35, "18-34")
         .when(F.col("age") < 50, "35-49")
         .when(F.col("age") < 65, "50-64")
         .otherwise("65+"),
    )


DIM_DATE_SCHEMA = T.StructType([
    T.StructField("date_key", T.IntegerType()),
    T.StructField("calendar_date", T.DateType()),
    T.StructField("year", T.IntegerType()),
    T.StructField("quarter", T.IntegerType()),
    T.StructField("month", T.IntegerType()),
    T.StructField("month_name", T.StringType()),
    T.StructField("year_month", T.StringType()),
    T.StructField("day_of_month", T.IntegerType()),
    T.StructField("day_of_week", T.IntegerType()),
    T.StructField("day_name", T.StringType()),
    T.StructField("is_weekend", T.BooleanType()),
])


def dim_date(spark, sources):
    """Calendar covering every date used by the facts. sources = [(df, column), ...]"""
    dates = reduce(DataFrame.unionByName, [df.select(F.to_date(c).alias("d")) for df, c in sources])
    lo, hi = (dates.where(F.col("d").between(F.lit(MIN_DATE).cast("date"), F.lit(MAX_DATE).cast("date")))
                   .agg(F.min("d"), F.max("d")).first())
    if lo is None:
        return spark.createDataFrame([], DIM_DATE_SCHEMA)

    d = F.col("calendar_date")
    cal = spark.range(1).select(
        F.explode(F.sequence(F.lit(lo), F.lit(hi), F.expr("INTERVAL 1 DAY"))).alias("calendar_date"))
    return cal.select(
        F.date_format(d, "yyyyMMdd").cast("int").alias("date_key"),
        d,
        F.year(d).alias("year"),
        F.quarter(d).alias("quarter"),
        F.month(d).alias("month"),
        F.date_format(d, "MMMM").alias("month_name"),
        F.date_format(d, "yyyy-MM").alias("year_month"),
        F.dayofmonth(d).alias("day_of_month"),
        ((F.dayofweek(d) + 5) % 7 + 1).alias("day_of_week"),  # 1 = Monday
        F.date_format(d, "EEEE").alias("day_name"),
        F.dayofweek(d).isin(1, 7).alias("is_weekend"),
    )


def fact_encounter(enc, obs, cond):
    obs_n = (obs.where(F.col("encounter_id").isNotNull()).groupBy("encounter_id")
                .agg(F.countDistinct("observation_id").alias("observation_count")))
    cond_n = (cond.where(F.col("encounter_id").isNotNull()).groupBy("encounter_id")
                  .agg(F.countDistinct("condition_id").alias("condition_count")))
    return enc.join(obs_n, "encounter_id", "left").join(cond_n, "encounter_id", "left").select(
        "encounter_id", "patient_id", "practitioner_id", "service_provider_id",
        date_key("period_start").alias("start_date_key"),
        "period_start", "period_end", "duration_minutes", "status",
        "class_code", "class_display", "type_code", "type_display",
        "reason_code", "reason_display", "discharge_disposition",
        F.coalesce(F.col("class_code").isin("IMP", "ACUTE", "NONAC"), F.lit(False)).alias("is_inpatient"),
        F.coalesce("observation_count", F.lit(0)).cast("int").alias("observation_count"),
        F.coalesce("condition_count", F.lit(0)).cast("int").alias("condition_count"),
        *SOURCE_LINEAGE,
    )


def fact_observation(obs):
    """One row per measured value. Panels like blood pressure have no value of their own,
    they become one row per component (component_seq 1..n)."""
    base = [
        "observation_id", "patient_id", "encounter_id",
        date_key("effective_at").alias("effective_date_key"), "effective_at", "status",
        "category_code", F.col("code").alias("observation_code"),
        F.col("code_display").alias("observation_display"), "interpretation_code",
        *SOURCE_LINEAGE,
    ]
    has_own_value = F.col("value_type").isNotNull() & (F.col("value_type") != "component")
    main = obs.where(has_own_value | (F.col("component_count") == 0)).select(
        *base,
        F.lit(0).alias("component_seq"),
        F.col("code").alias("measure_code"),
        F.col("code_display").alias("measure_display"),
        "value_numeric", "value_unit", "value_text", "value_type",
    )
    parts = (obs.where(F.col("component_count") > 0)
                .select(*base, F.posexplode("components").alias("pos", "c"))
                .select(
                    *[c for c in main.columns if c not in (
                        "component_seq", "measure_code", "measure_display",
                        "value_numeric", "value_unit", "value_text", "value_type")],
                    (F.col("pos") + 1).alias("component_seq"),
                    F.col("c.code").alias("measure_code"),
                    F.col("c.display").alias("measure_display"),
                    F.col("c.value_numeric").alias("value_numeric"),
                    F.col("c.value_unit").alias("value_unit"),
                    F.col("c.value_text").alias("value_text"),
                    F.when(F.col("c.value_numeric").isNotNull(), "Quantity")
                     .when(F.col("c.value_text").isNotNull(), "string").alias("value_type"),
                ))
    return main.unionByName(parts)


def fact_condition(cond):
    return cond.select(
        "condition_id", "patient_id", "encounter_id",
        date_key(F.coalesce("onset_at", "recorded_at")).alias("onset_date_key"),
        "onset_at", "abatement_at", "recorded_at",
        "clinical_status", "verification_status", "category_code", "severity",
        "code_system", "code", "code_display",
        F.coalesce(F.col("clinical_status").isin("active", "recurrence", "relapse"), F.lit(False)).alias("is_active"),
        F.when(F.col("onset_at").isNotNull() & F.col("abatement_at").isNotNull(),
               F.datediff(F.to_date("abatement_at"), F.to_date("onset_at"))).alias("duration_days"),
        *SOURCE_LINEAGE,
    )


def build_gold(lh, cfg, run_id, log=print):
    pat, enc, obs, cond = (silver_current(lh, cfg, r) for r in ("Patient", "Encounter", "Observation", "Condition"))
    referenced = (reduce(DataFrame.unionByName, [df.select("patient_id") for df in (enc, obs, cond)])
                  .where(F.col("patient_id").isNotNull()).distinct())

    frames = {
        "dim_patient": dim_patient(pat, referenced),
        "dim_date": dim_date(lh.spark, [(enc, "period_start"), (obs, "effective_at"),
                                        (cond, "onset_at"), (cond, "recorded_at")]),
        "fact_encounter": fact_encounter(enc, obs, cond),
        "fact_observation": fact_observation(obs),
        "fact_condition": fact_condition(cond),
    }

    stats = {}
    for name, df in frames.items():
        target = gold_table(cfg, name)
        out = df.withColumn("gold_loaded_at", F.current_timestamp()).withColumn("load_run_id", F.lit(run_id))
        lh.overwrite(out, target)
        stats[target] = lh.read(target).count()
        log(f"{target}: {stats[target]} rows")
    return stats
