-- Reporting views on top of the gold tables.
-- Run in the lakehouse SQL analytics endpoint (New SQL query) or a Fabric warehouse that can see
-- the lakehouse. Each CREATE needs its own batch, hence the GO lines (or run them one at a time).

-- one row per patient with activity counts
CREATE OR ALTER VIEW dbo.vw_patient_summary AS
SELECT
    p.patient_id,
    p.full_name,
    p.gender,
    p.age,
    p.age_band,
    p.city,
    p.state,
    p.is_inferred,
    COALESCE(e.encounter_count, 0) AS encounter_count,
    e.first_encounter_at,
    e.last_encounter_at,
    COALESCE(c.condition_count, 0) AS condition_count,
    COALESCE(c.active_condition_count, 0) AS active_condition_count,
    COALESCE(o.observation_count, 0) AS observation_count
FROM dbo.gold_dim_patient AS p
LEFT JOIN (
    SELECT patient_id,
           COUNT(*) AS encounter_count,
           MIN(period_start) AS first_encounter_at,
           MAX(period_start) AS last_encounter_at
    FROM dbo.gold_fact_encounter
    GROUP BY patient_id
) AS e ON e.patient_id = p.patient_id
LEFT JOIN (
    SELECT patient_id,
           COUNT(*) AS condition_count,
           SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) AS active_condition_count
    FROM dbo.gold_fact_condition
    GROUP BY patient_id
) AS c ON c.patient_id = p.patient_id
LEFT JOIN (
    SELECT patient_id, COUNT(DISTINCT observation_id) AS observation_count
    FROM dbo.gold_fact_observation
    GROUP BY patient_id
) AS o ON o.patient_id = p.patient_id;
GO

-- encounters per day and class
CREATE OR ALTER VIEW dbo.vw_encounters_daily AS
SELECT
    d.calendar_date,
    d.year_month,
    e.class_code,
    e.class_display,
    COUNT(*) AS encounters,
    COUNT(DISTINCT e.patient_id) AS patients,
    AVG(CAST(e.duration_minutes AS FLOAT)) AS avg_duration_minutes,
    SUM(CASE WHEN e.is_inpatient = 1 THEN 1 ELSE 0 END) AS inpatient_encounters
FROM dbo.gold_fact_encounter AS e
JOIN dbo.gold_dim_date AS d ON d.date_key = e.start_date_key
GROUP BY d.calendar_date, d.year_month, e.class_code, e.class_display;
GO

-- how common each condition is, split by gender and age band
CREATE OR ALTER VIEW dbo.vw_condition_prevalence AS
SELECT
    c.code,
    c.code_display,
    p.gender,
    p.age_band,
    COUNT(DISTINCT c.patient_id) AS patients,
    COUNT(*) AS conditions,
    SUM(CASE WHEN c.is_active = 1 THEN 1 ELSE 0 END) AS active_conditions
FROM dbo.gold_fact_condition AS c
JOIN dbo.gold_dim_patient AS p ON p.patient_id = c.patient_id
GROUP BY c.code, c.code_display, p.gender, p.age_band;
GO

-- latest value per patient for the common vitals (BP comes from the panel components)
CREATE OR ALTER VIEW dbo.vw_latest_vitals AS
SELECT patient_id, measure_code, measure_display, value_numeric, value_unit, effective_at
FROM (
    SELECT
        o.patient_id,
        o.measure_code,
        o.measure_display,
        o.value_numeric,
        o.value_unit,
        o.effective_at,
        ROW_NUMBER() OVER (PARTITION BY o.patient_id, o.measure_code
                           ORDER BY o.effective_at DESC, o.observation_id DESC) AS rn
    FROM dbo.gold_fact_observation AS o
    WHERE o.measure_code IN ('8302-2', '29463-7', '39156-5', '8867-4', '8480-6', '8462-4')
      AND o.value_numeric IS NOT NULL
) AS x
WHERE x.rn = 1;
GO

-- data ops: pages / resources pulled per run, resource and day
CREATE OR ALTER VIEW dbo.vw_api_load_stats AS
SELECT
    run_id,
    resource_type,
    response_format,
    CAST(window_start AS DATE) AS window_date,
    COUNT(*) AS pages,
    SUM(entry_count) AS resources,
    SUM(response_bytes) AS response_bytes,
    SUM(CASE WHEN attempts > 1 THEN 1 ELSE 0 END) AS retried_pages,
    SUM(CASE WHEN status <> 'OK' THEN 1 ELSE 0 END) AS failed_calls,
    MIN(called_at) AS first_call_at,
    MAX(saved_at) AS last_saved_at
FROM dbo.ctl_api_call_log
GROUP BY run_id, resource_type, response_format, CAST(window_start AS DATE);
GO

-- data ops: one row per notebook step
CREATE OR ALTER VIEW dbo.vw_pipeline_steps AS
SELECT run_id, step, resource_type, status, started_at, finished_at, duration_sec,
       rows_read, rows_written, rows_inserted, rows_updated, error
FROM dbo.ctl_load_log;
GO
