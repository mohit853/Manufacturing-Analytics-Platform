-- =============================================================
-- Migration: 002_create_views.sql
-- Analytical views on top of the star schema.
-- Run after 001_create_schema.sql.
-- =============================================================

SET search_path = manufacturing;

-- ── vw_daily_oee ──────────────────────────────────────────────
CREATE OR REPLACE VIEW manufacturing.vw_daily_oee AS
SELECT
    f.date_key,
    d.year,
    d.month,
    d.month_name,
    d.week,
    d.day_name,
    d.is_weekend,
    m.plant_name,
    m.line_name,
    m.machine_id,
    m.machine_name,
    m.machine_type,
    s.shift_name,
    AVG(f.availability)            AS avg_availability_pct,
    AVG(f.performance)             AS avg_performance_pct,
    AVG(f.quality_rate)            AS avg_quality_rate_pct,
    AVG(f.oee_pct)                 AS avg_oee_pct,
    SUM(f.planned_runtime_min)     AS total_planned_runtime_min,
    SUM(f.actual_runtime_min)      AS total_actual_runtime_min,
    SUM(f.total_downtime_min)      AS total_downtime_min,
    SUM(f.unplanned_downtime_min)  AS total_unplanned_downtime_min,
    SUM(f.planned_downtime_min)    AS total_planned_downtime_min,
    SUM(f.downtime_events)         AS total_downtime_events,
    SUM(f.planned_qty)             AS total_planned_qty,
    SUM(f.actual_qty)              AS total_actual_qty,
    SUM(f.good_qty)                AS total_good_qty,
    SUM(f.total_inspected_qty)     AS total_inspected_qty,
    SUM(f.total_defect_qty)        AS total_defects,
    AVG(f.first_pass_yield)        AS avg_first_pass_yield_pct
FROM manufacturing.fact_production f
JOIN manufacturing.dim_date    d ON f.date_key    = d.date_key
JOIN manufacturing.dim_machine m ON f.machine_key = m.machine_key
JOIN manufacturing.dim_shift   s ON f.shift_key   = s.shift_key
GROUP BY
    f.date_key, d.year, d.month, d.month_name, d.week, d.day_name, d.is_weekend,
    m.plant_name, m.line_name, m.machine_id, m.machine_name, m.machine_type,
    s.shift_name;

-- ── vw_monthly_summary ────────────────────────────────────────
CREATE OR REPLACE VIEW manufacturing.vw_monthly_summary AS
SELECT
    d.year,
    d.month,
    d.month_name,
    m.line_name,
    m.machine_name,
    p.product_name,
    p.product_family,
    SUM(f.planned_qty)              AS total_planned_qty,
    SUM(f.actual_qty)               AS total_actual_qty,
    SUM(f.good_qty)                 AS total_good_qty,
    SUM(f.total_defect_qty)         AS total_defects,
    AVG(f.first_pass_yield)         AS avg_first_pass_yield_pct,
    AVG(f.oee_pct)                  AS avg_oee_pct,
    SUM(f.unplanned_downtime_min)   AS total_unplanned_downtime_min,
    ROUND(
        100.0 * SUM(f.actual_qty) / NULLIF(SUM(f.planned_qty), 0), 2
    )                               AS attainment_pct
FROM manufacturing.fact_production f
JOIN manufacturing.dim_date    d ON f.date_key    = d.date_key
JOIN manufacturing.dim_machine m ON f.machine_key = m.machine_key
JOIN manufacturing.dim_product p ON f.product_key = p.product_key
GROUP BY d.year, d.month, d.month_name, m.line_name, m.machine_name, p.product_name, p.product_family;

-- ── vw_downtime_pareto ────────────────────────────────────────
CREATE OR REPLACE VIEW manufacturing.vw_downtime_pareto AS
SELECT
    m.machine_name,
    m.line_name,
    SUM(f.unplanned_downtime_min)   AS total_unplanned_min,
    SUM(f.downtime_events)          AS total_events,
    ROUND(
        100.0 * SUM(f.unplanned_downtime_min) /
        NULLIF(SUM(SUM(f.unplanned_downtime_min)) OVER (), 0),
        2
    )                               AS pct_of_total,
    ROUND(
        SUM(100.0 * SUM(f.unplanned_downtime_min) /
        NULLIF(SUM(SUM(f.unplanned_downtime_min)) OVER (), 0))
        OVER (ORDER BY SUM(f.unplanned_downtime_min) DESC
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
        2
    )                               AS cumulative_pct
FROM manufacturing.fact_production f
JOIN manufacturing.dim_machine m ON f.machine_key = m.machine_key
GROUP BY m.machine_name, m.line_name
ORDER BY total_unplanned_min DESC;

-- ── vw_shift_performance ─────────────────────────────────────
CREATE OR REPLACE VIEW manufacturing.vw_shift_performance AS
SELECT
    s.shift_name,
    m.line_name,
    m.machine_name,
    COUNT(*)                        AS shift_count,
    AVG(f.oee_pct)                  AS avg_oee_pct,
    AVG(f.availability)             AS avg_availability_pct,
    AVG(f.performance)              AS avg_performance_pct,
    AVG(f.quality_rate)             AS avg_quality_rate_pct,
    SUM(f.actual_qty)               AS total_actual_qty,
    SUM(f.total_defect_qty)         AS total_defects,
    SUM(f.unplanned_downtime_min)   AS total_unplanned_downtime_min
FROM manufacturing.fact_production f
JOIN manufacturing.dim_shift   s ON f.shift_key   = s.shift_key
JOIN manufacturing.dim_machine m ON f.machine_key = m.machine_key
GROUP BY s.shift_name, m.line_name, m.machine_name;