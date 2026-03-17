-- =============================================================
-- Migration: 001_create_schema.sql
-- Creates the manufacturing schema and all dimension + fact tables
-- Run once on a fresh Redshift cluster before any data loads.
-- =============================================================

-- Idempotent schema creation
CREATE SCHEMA IF NOT EXISTS manufacturing;
SET search_path = manufacturing;

-- ── dim_date ──────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS manufacturing.dim_date (
    date_key        DATE        NOT NULL,
    year            SMALLINT    NOT NULL,
    quarter         SMALLINT    NOT NULL,
    month           SMALLINT    NOT NULL,
    month_name      VARCHAR(10) NOT NULL,
    week            SMALLINT    NOT NULL,
    day_of_week     SMALLINT    NOT NULL,
    day_name        VARCHAR(10) NOT NULL,
    is_weekend      BOOLEAN     NOT NULL DEFAULT FALSE,
    CONSTRAINT pk_dim_date PRIMARY KEY (date_key)
)
DISTSTYLE ALL
SORTKEY (date_key);

-- ── dim_machine ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS manufacturing.dim_machine (
    machine_key     INT         IDENTITY(1,1),
    machine_id      VARCHAR(50) NOT NULL,
    machine_name    VARCHAR(100),
    machine_type    VARCHAR(50),
    is_active       BOOLEAN     NOT NULL DEFAULT TRUE,
    line_id         VARCHAR(50) NOT NULL,
    line_name       VARCHAR(100),
    plant_id        VARCHAR(50) NOT NULL,
    plant_name      VARCHAR(100),
    plant_location  VARCHAR(200),
    created_at      TIMESTAMP   DEFAULT GETDATE(),
    updated_at      TIMESTAMP   DEFAULT GETDATE(),
    CONSTRAINT pk_dim_machine PRIMARY KEY (machine_key),
    CONSTRAINT uq_dim_machine_id UNIQUE (machine_id)
)
DISTSTYLE ALL
SORTKEY (machine_id);

-- ── dim_shift ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS manufacturing.dim_shift (
    shift_key       INT         IDENTITY(1,1),
    shift_id        VARCHAR(10) NOT NULL,
    shift_name      VARCHAR(50) NOT NULL,
    shift_start     VARCHAR(5),
    shift_hours     SMALLINT,
    CONSTRAINT pk_dim_shift PRIMARY KEY (shift_key),
    CONSTRAINT uq_dim_shift_id UNIQUE (shift_id)
)
DISTSTYLE ALL;

-- ── dim_product ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS manufacturing.dim_product (
    product_key     INT         IDENTITY(1,1),
    product_id      VARCHAR(50) NOT NULL,
    product_name    VARCHAR(200),
    product_family  VARCHAR(100),
    unit_of_measure VARCHAR(20) DEFAULT 'PCS',
    CONSTRAINT pk_dim_product PRIMARY KEY (product_key),
    CONSTRAINT uq_dim_product_id UNIQUE (product_id)
)
DISTSTYLE ALL;

-- ── fact_production ───────────────────────────────────────────
CREATE TABLE IF NOT EXISTS manufacturing.fact_production (
    production_sk           BIGINT      IDENTITY(1,1),
    production_id           VARCHAR(100) NOT NULL,
    date_key                DATE        NOT NULL REFERENCES manufacturing.dim_date(date_key),
    machine_key             INT         NOT NULL REFERENCES manufacturing.dim_machine(machine_key),
    shift_key               INT         NOT NULL REFERENCES manufacturing.dim_shift(shift_key),
    product_key             INT         NOT NULL REFERENCES manufacturing.dim_product(product_key),
    planned_qty             INTEGER,
    actual_qty              INTEGER,
    good_qty                INTEGER,
    planned_runtime_min     DECIMAL(10,2),
    actual_runtime_min      DECIMAL(10,2),
    total_downtime_min      DECIMAL(10,2) DEFAULT 0,
    planned_downtime_min    DECIMAL(10,2) DEFAULT 0,
    unplanned_downtime_min  DECIMAL(10,2) DEFAULT 0,
    downtime_events         SMALLINT      DEFAULT 0,
    total_inspected_qty     INTEGER       DEFAULT 0,
    total_defect_qty        INTEGER       DEFAULT 0,
    first_pass_yield        DECIMAL(6,2),
    availability            DECIMAL(6,2),
    performance             DECIMAL(6,2),
    quality_rate            DECIMAL(6,2),
    oee_pct                 DECIMAL(6,2),
    loaded_at               TIMESTAMP DEFAULT GETDATE(),
    CONSTRAINT pk_fact_production PRIMARY KEY (production_sk)
)
DISTKEY (machine_key)
SORTKEY (date_key, machine_key, shift_key);

-- ── Staging tables ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS manufacturing.stg_fact_production
    (LIKE manufacturing.fact_production);

CREATE TABLE IF NOT EXISTS manufacturing.stg_dim_machine
    (LIKE manufacturing.dim_machine);

-- ── Seed dim_shift ────────────────────────────────────────────
INSERT INTO manufacturing.dim_shift (shift_id, shift_name, shift_start, shift_hours)
SELECT * FROM (VALUES
    ('S1', 'Morning (06:00–14:00)',   '06:00', 8),
    ('S2', 'Afternoon (14:00–22:00)', '14:00', 8),
    ('S3', 'Night (22:00–06:00)',     '22:00', 8)
) AS t(shift_id, shift_name, shift_start, shift_hours)
WHERE NOT EXISTS (SELECT 1 FROM manufacturing.dim_shift WHERE shift_id = t.shift_id);