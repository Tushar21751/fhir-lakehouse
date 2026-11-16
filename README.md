# FHIR to Fabric lakehouse

Incremental load of Patient, Encounter, Observation and Condition from the HAPI FHIR R4 API (https://hapi.fhir.org/baseR4) into a Fabric lakehouse, medallion style. Orchestrated with a Fabric Data Pipeline.

## Flow

Pipeline `pl_fhir_medallion`: ForEach resource (sequential: Patient -> Encounter -> Observation -> Condition) runs nb_01 -> nb_02 -> nb_03, then nb_04 builds gold.

- **Raw** (nb_01): calls the API by `_lastUpdated` window, one window per day, starting from the last watermark (first run goes back 3 days). Follows the Bundle `next` link for paging. Responses saved as-is to `Files/raw/fhir/<Resource>/yyyy/mm/dd/<run_id>/`. Every call logged in `ctl_api_call_log` (url/params, called_at, saved_at, status).
- **Bronze** (nb_02): raw json/xml -> one row per resource in `bronze_<resource>` (Delta, append), with `extraction_timestamp` and `api_url_or_params`.
- **Silver** (nb_03): flatten, clean, dedupe, then SCD Type 2 per resource id (hash compare; `effective_from`, `effective_to`, `is_current`, `scd_version`).
- **Gold** (nb_04): `gold_dim_patient`, `gold_dim_date`, `gold_fact_encounter`, `gold_fact_observation`, `gold_fact_condition`. Views in `sql/gold_views.sql`.

## Table relationships

- Encounter, Observation, Condition -> Patient on `patient_id`
- Observation, Condition -> Encounter on `encounter_id`
- Facts -> `gold_dim_date` on the date keys

## Setup

1. Create lakehouse `lh_fhir`. Upload `config/pipeline_config.json` to `Files/config/` and `src/fhir_lakehouse` to `Files/code/`.
2. Import the notebooks and attach `lh_fhir` as default lakehouse.
3. Create the pipeline from `pipeline/pl_fhir_medallion.json` (replace the notebook/workspace ids) and run it.

XML: set pipeline parameter `response_format = xml`.
