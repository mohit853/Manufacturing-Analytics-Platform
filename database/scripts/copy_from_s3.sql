-- =============================================================
-- database/scripts/copy_from_s3.sql
-- COPY commands to load processed Parquet from S3 → Redshift
-- Replace placeholders before running:
--   :processed_bucket → your processed S3 bucket name
--   :iam_role         → Redshift IAM role ARN with S3 read access
--   :region           → AWS region (e.g. us-east-1)
-- =============================================================

SET search_path = manufacturing;

-- ─────────────────────────────────────────────
-- 1. DIMENSIONS (full refresh — small tables)
-- ─────────────────────────────────────────────

-- dim_machine
TRUNCATE TABLE manufacturing.stg_dim_machine;

COPY manufacturing.stg_dim_machine (
    machine_id, machine_name, machine_type, is_active,
    line_id, line_name,
    plant_id, plant_name, plant_location
)
FROM 's3://:processed_bucket/dim_machine/'
IAM_ROLE ':iam_role'
FORMAT AS PARQUET
REGION ':region';

-- Upsert into dim_machine
DELETE FROM manufacturing.dim_machine
WHERE machine_id IN (SELECT machine_id FROM manufacturing.stg_dim_machine);

INSERT INTO manufacturing.dim_machine (
    machine_id, machine_name, machine_type, is_active,
    line_id, line_name,
    plant_id, plant_name, plant_location
)
SELECT
    machine_id, machine_name, machine_type, is_active,
    line_id, line_name,
    plant_id, plant_name, plant_location
FROM manufacturing.stg_dim_machine;

-- dim_product
COPY manufacturing.dim_product (product_id, product_name, product_family, unit_of_measure)
FROM 's3://:processed_bucket/dim_product/'
IAM_ROLE ':iam_role'
FORMAT AS PARQUET
REGION ':region';

-- dim_date
COPY manufacturing.dim_date
FROM 's3://:processed_bucket/dim_date/'
IAM_ROLE ':iam_role'
FORMAT AS PARQUET
REGION ':region';

-- ─────────────────────────────────────────────
-- 2. FACT TABLE (incremental upsert)
-- ─────────────────────────────────────────────

TRUNCATE TABLE manufacturing.stg_fact_production;

-- Load current partition (e.g. today's date — parameterise in orchestration)
COPY manufacturing.stg_fact_production
FROM 's3://:processed_bucket/fact_production/'
IAM_ROLE ':iam_role'
FORMAT AS PARQUET
REGION ':region';

-- Delete rows that will be replaced
DELETE FROM manufacturing.fact_production
WHERE production_id IN (
    SELECT production_id FROM manufacturing.stg_fact_production
);

-- Insert from staging with FK lookups
INSERT INTO manufacturing.fact_production (
    production_id,
    date_key, machine_key, shift_key, product_key,
    planned_qty, actual_qty, good_qty,
    planned_runtime_min, actual_runtime_min,
    total_downtime_min, planned_downtime_min, unplanned_downtime_min, downtime_events,
    total_inspected_qty, total_defect_qty, first_pass_yield,
    availability, performance, quality_rate, oee_pct
)
SELECT
    s.production_id,
    s.date_key,
    m.machine_key,
    sh.shift_key,
    p.product_key,
    s.planned_qty,   s.actual_qty,   s.good_qty,
    s.planned_runtime_min, s.actual_runtime_min,
    s.total_downtime_min, s.planned_downtime_min, s.unplanned_downtime_min, s.downtime_events,
    s.total_inspected_qty, s.total_defect_qty, s.first_pass_yield,
    s.availability, s.performance, s.quality_rate, s.oee_pct
FROM manufacturing.stg_fact_production s
JOIN manufacturing.dim_machine  m  ON s.machine_id  = m.machine_id
JOIN manufacturing.dim_shift    sh ON s.shift_id    = sh.shift_id
JOIN manufacturing.dim_product  p  ON s.product_id  = p.product_id;

-- ─────────────────────────────────────────────
-- 3. POST-LOAD MAINTENANCE
-- ─────────────────────────────────────────────

ANALYZE manufacturing.fact_production;
VACUUM manufacturing.fact_production TO 100 PERCENT;

-- Row count verification
SELECT
    'fact_production' AS table_name,
    COUNT(*)          AS row_count,
    MAX(loaded_at)    AS last_loaded
FROM manufacturing.fact_production

UNION ALL

SELECT 'dim_machine',  COUNT(*), MAX(updated_at) FROM manufacturing.dim_machine
UNION ALL
SELECT 'dim_date',     COUNT(*), NULL            FROM manufacturing.dim_date
UNION ALL
SELECT 'dim_shift',    COUNT(*), NULL            FROM manufacturing.dim_shift
UNION ALL
SELECT 'dim_product',  COUNT(*), NULL            FROM manufacturing.dim_product

ORDER BY table_name;