"""bronze -> silver: flatten each resource type, dedupe, then SCD2 into silver_<resource>.

All the FHIR leaf fields are read as strings and cast here with try_cast, so the same
code works for json (real booleans/numbers) and xml (everything is text) and bad values
from the public test server end up as nulls instead of failing the job.
"""
from dataclasses import dataclass, field
from datetime import timezone
from typing import Callable, List

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .bronze import empty_bronze
from .config import table_name
from .control import get_watermark, set_watermark
from .scd2 import apply_scd2

S = T.StringType()


def _struct(*fields):
    return T.StructType([f if isinstance(f, T.StructField) else T.StructField(f, S) for f in fields])


def _f(name, dtype):
    return T.StructField(name, dtype)


CODING = T.ArrayType(_struct("system", "code", "display"))
CODEABLE = _struct("text", _f("coding", CODING))
REFERENCE = _struct("reference", "display")
PERIOD = _struct("start", "end")
QUANTITY = _struct("value", "unit", "system", "code")
META = _struct("versionId", "lastUpdated")

PATIENT_SCHEMA = _struct(
    "id", _f("meta", META), "active", "gender", "birthDate", "deceasedBoolean", "deceasedDateTime",
    _f("name", T.ArrayType(_struct("use", "text", "family",
                                   _f("given", T.ArrayType(S)), _f("prefix", T.ArrayType(S))))),
    _f("telecom", T.ArrayType(_struct("system", "value", "use"))),
    _f("address", T.ArrayType(_struct("use", _f("line", T.ArrayType(S)), "city", "district",
                                      "state", "postalCode", "country"))),
    _f("maritalStatus", CODEABLE),
    _f("identifier", T.ArrayType(_struct("system", "value", _f("type", CODEABLE)))),
    _f("communication", T.ArrayType(_struct(_f("language", CODEABLE)))),
    _f("managingOrganization", REFERENCE),
)

ENCOUNTER_SCHEMA = _struct(
    "id", _f("meta", META), "status", _f("class", _struct("system", "code", "display")),
    _f("type", T.ArrayType(CODEABLE)), _f("serviceType", CODEABLE), _f("priority", CODEABLE),
    _f("subject", REFERENCE),
    _f("participant", T.ArrayType(_struct(_f("individual", REFERENCE)))),
    _f("period", PERIOD), _f("reasonCode", T.ArrayType(CODEABLE)),
    _f("hospitalization", _struct(_f("dischargeDisposition", CODEABLE))),
    _f("location", T.ArrayType(_struct(_f("location", REFERENCE)))),
    _f("serviceProvider", REFERENCE),
)

OBSERVATION_SCHEMA = _struct(
    "id", _f("meta", META), "status", _f("category", T.ArrayType(CODEABLE)), _f("code", CODEABLE),
    _f("subject", REFERENCE), _f("encounter", REFERENCE),
    "effectiveDateTime", "effectiveInstant", _f("effectivePeriod", PERIOD), "issued",
    _f("valueQuantity", QUANTITY), _f("valueCodeableConcept", CODEABLE),
    "valueString", "valueBoolean", "valueInteger", "valueDateTime",
    _f("interpretation", T.ArrayType(CODEABLE)),
    _f("component", T.ArrayType(_struct(_f("code", CODEABLE), _f("valueQuantity", QUANTITY),
                                        _f("valueCodeableConcept", CODEABLE), "valueString"))),
)

CONDITION_SCHEMA = _struct(
    "id", _f("meta", META), _f("clinicalStatus", CODEABLE), _f("verificationStatus", CODEABLE),
    _f("category", T.ArrayType(CODEABLE)), _f("severity", CODEABLE), _f("code", CODEABLE),
    _f("subject", REFERENCE), _f("encounter", REFERENCE),
    "onsetDateTime", _f("onsetPeriod", PERIOD), "abatementDateTime", _f("abatementPeriod", PERIOD),
    "recordedDate",
)


# small sql snippets, keeps the select lists readable

def first(arr):
    return f"try_element_at({arr}, 1)"


def code(cc):
    return f"{first(cc + '.coding')}.code"


def display(cc):
    return f"coalesce({first(cc + '.coding')}.display, {cc}.text)"


def ts(x):
    return f"try_cast({x} AS timestamp)"


def ref_id(x, resource_type):
    # Patient/123, Patient/123/_history/2 and absolute urls all give 123
    return f"nullif(regexp_extract({x}, '(^|/){resource_type}/([^/]+)', 2), '')"


def E(sql, name):
    return F.expr(sql).alias(name)


LINEAGE = ["extraction_timestamp", "api_url_or_params", "raw_path", "response_format",
           "ingest_run_id", "bronze_loaded_at"]


def flatten_patient(df):
    name = "coalesce(try_element_at(filter(r.name, n -> n.use = 'official'), 1), try_element_at(r.name, 1))"
    addr = "coalesce(try_element_at(filter(r.address, a -> a.use = 'home'), 1), try_element_at(r.address, 1))"
    return df.withColumn("r", F.from_json("resource_json", PATIENT_SCHEMA)).select(
        F.col("resource_id").alias("patient_id"),
        "version_id",
        "last_updated",
        E("try_cast(r.active AS boolean)", "active"),
        E("lower(trim(r.gender))", "gender"),
        E("try_cast(r.birthDate AS date)", "birth_date"),
        E("coalesce(try_cast(r.deceasedBoolean AS boolean), r.deceasedDateTime IS NOT NULL)", "is_deceased"),
        E(ts("r.deceasedDateTime"), "deceased_at"),
        E(f"{name}.family", "family_name"),
        E(f"array_join({name}.given, ' ')", "given_name"),
        E(f"nullif(trim(coalesce({name}.text, concat_ws(' ', array_join({name}.prefix, ' '), "
          f"array_join({name}.given, ' '), {name}.family))), '')", "full_name"),
        E("try_element_at(filter(r.telecom, t -> t.system = 'phone'), 1).value", "phone"),
        E("try_element_at(filter(r.telecom, t -> t.system = 'email'), 1).value", "email"),
        E(f"nullif(array_join({addr}.line, ', '), '')", "address_line"),
        E(f"{addr}.city", "city"),
        E(f"{addr}.state", "state"),
        E(f"{addr}.postalCode", "postal_code"),
        E(f"{addr}.country", "country"),
        E(code("r.maritalStatus"), "marital_status_code"),
        E(display("r.maritalStatus"), "marital_status"),
        E("try_element_at(filter(r.identifier, i -> exists(i.type.coding, c -> c.code = 'MR')), 1).value", "mrn"),
        E(code(first("r.communication") + ".language"), "language"),
        E(ref_id("r.managingOrganization.reference", "Organization"), "managing_organization_id"),
        *LINEAGE,
    )


def flatten_encounter(df):
    start, end = ts("r.period.start"), ts("r.period.end")
    return df.withColumn("r", F.from_json("resource_json", ENCOUNTER_SCHEMA)).select(
        F.col("resource_id").alias("encounter_id"),
        "version_id",
        "last_updated",
        E("lower(r.status)", "status"),
        E("r.class.code", "class_code"),
        E("coalesce(r.class.display, r.class.code)", "class_display"),
        E(code(first("r.type")), "type_code"),
        E(display(first("r.type")), "type_display"),
        E(display("r.serviceType"), "service_type"),
        E(code("r.priority"), "priority_code"),
        E(ref_id("r.subject.reference", "Patient"), "patient_id"),
        E(ref_id(first("r.participant") + ".individual.reference", "Practitioner"), "practitioner_id"),
        E(start, "period_start"),
        E(end, "period_end"),
        E(f"CASE WHEN {end} >= {start} THEN (unix_seconds({end}) - unix_seconds({start})) / 60.0 END",
          "duration_minutes"),
        E(code(first("r.reasonCode")), "reason_code"),
        E(display(first("r.reasonCode")), "reason_display"),
        E(display("r.hospitalization.dischargeDisposition"), "discharge_disposition"),
        E(first("r.location") + ".location.display", "location_name"),
        E(ref_id("r.serviceProvider.reference", "Organization"), "service_provider_id"),
        E("r.serviceProvider.display", "service_provider_name"),
        *LINEAGE,
    )


def flatten_observation(df):
    cc = "r.valueCodeableConcept"
    components = (
        "transform(r.component, c -> named_struct("
        "'code', try_element_at(c.code.coding, 1).code, "
        "'display', coalesce(try_element_at(c.code.coding, 1).display, c.code.text), "
        "'value_numeric', try_cast(c.valueQuantity.value AS double), "
        "'value_unit', coalesce(c.valueQuantity.unit, c.valueQuantity.code), "
        "'value_text', coalesce(c.valueString, try_element_at(c.valueCodeableConcept.coding, 1).display, "
        "c.valueCodeableConcept.text)))"
    )
    return df.withColumn("r", F.from_json("resource_json", OBSERVATION_SCHEMA)).select(
        F.col("resource_id").alias("observation_id"),
        "version_id",
        "last_updated",
        E("lower(r.status)", "status"),
        E(code(first("r.category")), "category_code"),
        E(display(first("r.category")), "category_display"),
        E(f"{first('r.code.coding')}.system", "code_system"),
        E(code("r.code"), "code"),
        E(display("r.code"), "code_display"),
        E(ref_id("r.subject.reference", "Patient"), "patient_id"),
        E(ref_id("r.encounter.reference", "Encounter"), "encounter_id"),
        E(f"coalesce({ts('r.effectiveDateTime')}, {ts('r.effectiveInstant')}, {ts('r.effectivePeriod.start')})",
          "effective_at"),
        E(ts("r.issued"), "issued_at"),
        E("CASE WHEN r.valueQuantity IS NOT NULL THEN 'Quantity' "
          "WHEN r.valueCodeableConcept IS NOT NULL THEN 'CodeableConcept' "
          "WHEN r.valueString IS NOT NULL THEN 'string' "
          "WHEN r.valueBoolean IS NOT NULL THEN 'boolean' "
          "WHEN r.valueInteger IS NOT NULL THEN 'integer' "
          "WHEN r.valueDateTime IS NOT NULL THEN 'dateTime' "
          "WHEN size(r.component) > 0 THEN 'component' END", "value_type"),
        E("coalesce(try_cast(r.valueQuantity.value AS double), try_cast(r.valueInteger AS double))",
          "value_numeric"),
        E("coalesce(r.valueQuantity.unit, r.valueQuantity.code)", "value_unit"),
        E(code(cc), "value_code"),
        E(f"coalesce(r.valueString, {display(cc)}, r.valueBoolean, r.valueDateTime)", "value_text"),
        E(code(first("r.interpretation")), "interpretation_code"),
        E(components, "components"),
        E("CASE WHEN r.component IS NULL THEN 0 ELSE size(r.component) END", "component_count"),
        *LINEAGE,
    )


def flatten_condition(df):
    return df.withColumn("r", F.from_json("resource_json", CONDITION_SCHEMA)).select(
        F.col("resource_id").alias("condition_id"),
        "version_id",
        "last_updated",
        E(code("r.clinicalStatus"), "clinical_status"),
        E(code("r.verificationStatus"), "verification_status"),
        E(code(first("r.category")), "category_code"),
        E(display(first("r.category")), "category_display"),
        E(display("r.severity"), "severity"),
        E(f"{first('r.code.coding')}.system", "code_system"),
        E(code("r.code"), "code"),
        E(display("r.code"), "code_display"),
        E(ref_id("r.subject.reference", "Patient"), "patient_id"),
        E(ref_id("r.encounter.reference", "Encounter"), "encounter_id"),
        E(f"coalesce({ts('r.onsetDateTime')}, {ts('r.onsetPeriod.start')})", "onset_at"),
        E(f"coalesce({ts('r.abatementDateTime')}, {ts('r.abatementPeriod.start')})", "abatement_at"),
        E(ts("r.recordedDate"), "recorded_at"),
        *LINEAGE,
    )


@dataclass
class SilverModel:
    key: str
    flatten: Callable
    # columns that are not part of the change hash
    untracked: List[str] = field(default_factory=lambda: ["version_id", "last_updated", *LINEAGE])


MODELS = {
    "Patient": SilverModel("patient_id", flatten_patient),
    "Encounter": SilverModel("encounter_id", flatten_encounter),
    "Observation": SilverModel("observation_id", flatten_observation),
    "Condition": SilverModel("condition_id", flatten_condition),
}


def tracked_columns(df, model):
    return [c for c in df.columns if c != model.key and c not in model.untracked]


def prepare_batch(bronze_rows, model):
    """Flatten + dedupe a batch of bronze rows and add the columns SCD2 needs."""
    flat = model.flatten(bronze_rows).where(F.col(model.key).isNotNull())

    # same resource version can arrive more than once (re-runs, reloads) - keep the newest copy
    w = Window.partitionBy(model.key, "version_id").orderBy(
        F.col("extraction_timestamp").desc(), F.col("bronze_loaded_at").desc())
    flat = flat.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")

    tracked = tracked_columns(flat, model)
    return (flat
            .withColumn("record_hash", F.sha2(F.to_json(F.struct(*tracked)), 256))
            .withColumn("effective_from", F.coalesce("last_updated", "extraction_timestamp"))
            .withColumn("version_seq", F.expr("try_cast(version_id AS bigint)")))


def bronze_to_silver(lh, cfg, resource_type, run_id, log=print):
    """Take bronze rows loaded since the last silver run and SCD2 them into silver."""
    model = MODELS[resource_type]
    bronze_name = table_name(cfg, "bronze", resource_type)
    silver_name = table_name(cfg, "silver", resource_type)

    bronze = lh.read(bronze_name) if lh.exists(bronze_name) else empty_bronze(lh.spark)
    watermark = get_watermark(lh, cfg, "silver", resource_type)
    if watermark is not None:
        bronze = bronze.where(F.col("bronze_loaded_at") > F.lit(watermark))
    upper = bronze.agg(F.max("bronze_loaded_at")).first()[0]
    if upper is not None:
        upper = upper.astimezone(timezone.utc)
        bronze = bronze.where(F.col("bronze_loaded_at") <= F.lit(upper))

    batch = prepare_batch(bronze, model).persist()
    columns = [c for c in batch.columns if c not in ("record_hash", "effective_from", "version_seq")]
    stats = {"rows_read": bronze.count(), "rows_after_dedupe": batch.count()}
    stats.update(apply_scd2(lh, silver_name, batch, model.key, run_id, columns))
    batch.unpersist()

    if upper is not None:
        set_watermark(lh, cfg, "silver", resource_type, upper, run_id)
    log(f"{resource_type}: {stats['rows_read']} bronze rows -> {stats['rows_inserted']} new silver versions "
        f"({stats['new_keys']} new keys, {stats['rows_closed']} versions closed)")
    return stats
