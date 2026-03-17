"""
glue_etl_job.py
Manufacturing Analytics Pipeline — AWS Glue PySpark ETL
--------------------------------------------------------
Reads raw CSVs (production, downtime, quality) from S3 raw zone,
applies transformations, computes OEE, and writes Parquet to processed zone.
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, DateType
)
from pyspark.sql.window import Window
import logging

# ─────────────────────────────────────────────
# INIT
# ─────────────────────────────────────────────
logger = logging.getLogger()
logger.setLevel(logging.INFO)

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "RAW_BUCKET",
    "PROCESSED_BUCKET",
    "GLUE_DATABASE",
])

sc         = SparkContext()
glueContext = GlueContext(sc)
spark      = glueContext.spark_session
job        = Job(glueContext)
job.init(args["JOB_NAME"], args)

RAW_BUCKET       = args["RAW_BUCKET"]
PROCESSED_BUCKET = args["PROCESSED_BUCKET"]
GLUE_DATABASE    = args["GLUE_DATABASE"]

RAW_BASE       = f"s3://{RAW_BUCKET}"
PROCESSED_BASE = f"s3://{PROCESSED_BUCKET}"

# ─────────────────────────────────────────────
# SCHEMAS
# ─────────────────────────────────────────────

production_schema = StructType([
    StructField("production_id",    StringType(),    False),
    StructField("plant_id",         StringType(),    False),
    StructField("line_id",          StringType(),    False),
    StructField("machine_id",       StringType(),    False),
    StructField("shift_id",         StringType(),    False),
    StructField("product_id",       StringType(),    False),
    StructField("production_date",  StringType(),    False),   # yyyy-MM-dd
    StructField("planned_qty",      IntegerType(),   True),
    StructField("actual_qty",       IntegerType(),   True),
    StructField("good_qty",         IntegerType(),   True),
    StructField("planned_runtime_min", DoubleType(), True),
    StructField("actual_runtime_min",  DoubleType(), True),
])

downtime_schema = StructType([
    StructField("downtime_id",      StringType(),    False),
    StructField("machine_id",       StringType(),    False),
    StructField("production_date",  StringType(),    False),
    StructField("shift_id",         StringType(),    False),
    StructField("downtime_reason",  StringType(),    True),
    StructField("downtime_minutes", DoubleType(),    True),
    StructField("is_planned",       StringType(),    True),   # "Y" / "N"
])

quality_schema = StructType([
    StructField("inspection_id",    StringType(),    False),
    StructField("machine_id",       StringType(),    False),
    StructField("product_id",       StringType(),    False),
    StructField("production_date",  StringType(),    False),
    StructField("shift_id",         StringType(),    False),
    StructField("inspected_qty",    IntegerType(),   True),
    StructField("defect_qty",       IntegerType(),   True),
    StructField("defect_category",  StringType(),    True),
])

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def read_csv(path: str, schema: StructType):
    logger.info(f"Reading CSV from: {path}")
    return (
        spark.read
        .option("header", "true")
        .option("nullValue", "")
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .csv(path)
    )

def write_parquet(df, path: str, partition_cols: list = None):
    logger.info(f"Writing Parquet to: {path}")
    writer = df.write.mode("overwrite").parquet
    if partition_cols:
        df.write.mode("overwrite").partitionBy(*partition_cols).parquet(path)
    else:
        df.write.mode("overwrite").parquet(path)

# ─────────────────────────────────────────────
# STEP 1 — READ RAW DATA
# ─────────────────────────────────────────────
logger.info("=== STEP 1: Reading raw data ===")

df_prod     = read_csv(f"{RAW_BASE}/production/",  production_schema)
df_downtime = read_csv(f"{RAW_BASE}/downtime/",    downtime_schema)
df_quality  = read_csv(f"{RAW_BASE}/quality/",     quality_schema)

# ─────────────────────────────────────────────
# STEP 2 — CLEAN & VALIDATE
# ─────────────────────────────────────────────
logger.info("=== STEP 2: Cleaning data ===")

# Production — cast date, drop nulls on key columns
df_prod = (
    df_prod
    .dropDuplicates(["production_id"])
    .filter(F.col("machine_id").isNotNull() & F.col("production_date").isNotNull())
    .withColumn("production_date", F.to_date("production_date", "yyyy-MM-dd"))
    .withColumn("planned_qty",  F.when(F.col("planned_qty") < 0, 0).otherwise(F.col("planned_qty")))
    .withColumn("actual_qty",   F.when(F.col("actual_qty")  < 0, 0).otherwise(F.col("actual_qty")))
    .withColumn("good_qty",     F.when(F.col("good_qty")    < 0, 0).otherwise(F.col("good_qty")))
)

# Downtime — cast boolean flag
df_downtime = (
    df_downtime
    .dropDuplicates(["downtime_id"])
    .filter(F.col("machine_id").isNotNull())
    .withColumn("production_date", F.to_date("production_date", "yyyy-MM-dd"))
    .withColumn("is_planned_flag", F.when(F.upper("is_planned") == "Y", True).otherwise(False))
    .withColumn("downtime_minutes", F.when(F.col("downtime_minutes") < 0, 0).otherwise(F.col("downtime_minutes")))
)

# Quality — cap defect_qty at inspected_qty
df_quality = (
    df_quality
    .dropDuplicates(["inspection_id"])
    .filter(F.col("machine_id").isNotNull())
    .withColumn("production_date", F.to_date("production_date", "yyyy-MM-dd"))
    .withColumn("defect_qty", F.least(F.col("defect_qty"), F.col("inspected_qty")))
)

# ─────────────────────────────────────────────
# STEP 3 — AGGREGATE DOWNTIME PER MACHINE/DATE/SHIFT
# ─────────────────────────────────────────────
logger.info("=== STEP 3: Aggregating downtime ===")

df_downtime_agg = (
    df_downtime
    .groupBy("machine_id", "production_date", "shift_id")
    .agg(
        F.sum("downtime_minutes").alias("total_downtime_min"),
        F.sum(F.when(F.col("is_planned_flag"), F.col("downtime_minutes")).otherwise(0))
         .alias("planned_downtime_min"),
        F.sum(F.when(~F.col("is_planned_flag"), F.col("downtime_minutes")).otherwise(0))
         .alias("unplanned_downtime_min"),
        F.count("downtime_id").alias("downtime_events"),
    )
)

# ─────────────────────────────────────────────
# STEP 4 — AGGREGATE QUALITY PER MACHINE/DATE/SHIFT
# ─────────────────────────────────────────────
logger.info("=== STEP 4: Aggregating quality ===")

df_quality_agg = (
    df_quality
    .groupBy("machine_id", "product_id", "production_date", "shift_id")
    .agg(
        F.sum("inspected_qty").alias("total_inspected_qty"),
        F.sum("defect_qty").alias("total_defect_qty"),
    )
    .withColumn(
        "first_pass_yield",
        F.round(
            (F.col("total_inspected_qty") - F.col("total_defect_qty")) / F.col("total_inspected_qty") * 100,
            2
        )
    )
)

# ─────────────────────────────────────────────
# STEP 5 — BUILD FACT TABLE
# ─────────────────────────────────────────────
logger.info("=== STEP 5: Building fact_production ===")

df_fact = (
    df_prod
    .join(
        df_downtime_agg,
        on=["machine_id", "production_date", "shift_id"],
        how="left"
    )
    .join(
        df_quality_agg,
        on=["machine_id", "product_id", "production_date", "shift_id"],
        how="left"
    )
    .withColumn("total_downtime_min",    F.coalesce("total_downtime_min",    F.lit(0.0)))
    .withColumn("planned_downtime_min",  F.coalesce("planned_downtime_min",  F.lit(0.0)))
    .withColumn("unplanned_downtime_min",F.coalesce("unplanned_downtime_min",F.lit(0.0)))
    .withColumn("total_defect_qty",      F.coalesce("total_defect_qty",      F.lit(0)))
    .withColumn("total_inspected_qty",   F.coalesce("total_inspected_qty",   F.lit(0)))
)

# ─── OEE Calculation ───────────────────────────────────────────────────────
# Availability = (Planned Runtime - Unplanned Downtime) / Planned Runtime
# Performance  = Actual Qty / Planned Qty
# Quality      = Good Qty / Actual Qty
# OEE          = Availability × Performance × Quality × 100

df_fact = (
    df_fact
    .withColumn(
        "availability",
        F.when(F.col("planned_runtime_min") > 0,
               (F.col("planned_runtime_min") - F.col("unplanned_downtime_min")) /
               F.col("planned_runtime_min")
        ).otherwise(F.lit(0.0))
    )
    .withColumn(
        "performance",
        F.when(F.col("planned_qty") > 0,
               F.col("actual_qty") / F.col("planned_qty")
        ).otherwise(F.lit(0.0))
    )
    .withColumn(
        "quality_rate",
        F.when(F.col("actual_qty") > 0,
               F.col("good_qty") / F.col("actual_qty")
        ).otherwise(F.lit(0.0))
    )
    .withColumn(
        "oee_pct",
        F.round(
            F.col("availability") * F.col("performance") * F.col("quality_rate") * 100,
            2
        )
    )
    # Clamp to [0, 100]
    .withColumn("availability",  F.round(F.least(F.greatest(F.col("availability"),  F.lit(0.0)), F.lit(1.0)) * 100, 2))
    .withColumn("performance",   F.round(F.least(F.greatest(F.col("performance"),   F.lit(0.0)), F.lit(1.0)) * 100, 2))
    .withColumn("quality_rate",  F.round(F.least(F.greatest(F.col("quality_rate"),  F.lit(0.0)), F.lit(1.0)) * 100, 2))
    .withColumn("oee_pct",       F.least(F.greatest(F.col("oee_pct"), F.lit(0.0)), F.lit(100.0)))
    # Derived date parts for Redshift partition alignment
    .withColumn("year",  F.year("production_date"))
    .withColumn("month", F.month("production_date"))
)

# Select final columns
df_fact = df_fact.select(
    "production_id",
    "plant_id", "line_id", "machine_id",
    "shift_id", "product_id",
    "production_date", "year", "month",
    "planned_qty", "actual_qty", "good_qty",
    "planned_runtime_min", "actual_runtime_min",
    "total_downtime_min", "planned_downtime_min", "unplanned_downtime_min",
    "downtime_events",
    "total_inspected_qty", "total_defect_qty",
    "first_pass_yield",
    "availability", "performance", "quality_rate", "oee_pct",
)

logger.info(f"Fact rows: {df_fact.count()}")

# ─────────────────────────────────────────────
# STEP 6 — BUILD DIMENSION TABLES
# ─────────────────────────────────────────────
logger.info("=== STEP 6: Building dimension tables ===")

# dim_machine
df_dim_machine = (
    df_prod
    .select("machine_id", "line_id", "plant_id")
    .dropDuplicates(["machine_id"])
    .withColumn("machine_name",  F.concat(F.lit("Machine-"), F.col("machine_id")))
    .withColumn("line_name",     F.concat(F.lit("Line-"),    F.col("line_id")))
    .withColumn("plant_name",    F.lit("Plant 001"))
    .withColumn("machine_type",  F.lit("CNC"))          # placeholder — enrich from master data
    .withColumn("is_active",     F.lit(True))
)

# dim_shift
df_dim_shift = (
    df_prod
    .select("shift_id")
    .dropDuplicates(["shift_id"])
    .withColumn("shift_name",
        F.when(F.col("shift_id") == "S1", "Morning (06:00–14:00)")
         .when(F.col("shift_id") == "S2", "Afternoon (14:00–22:00)")
         .when(F.col("shift_id") == "S3", "Night (22:00–06:00)")
         .otherwise("Unknown")
    )
    .withColumn("shift_start", F.when(F.col("shift_id") == "S1", F.lit("06:00"))
                                .when(F.col("shift_id") == "S2", F.lit("14:00"))
                                .otherwise(F.lit("22:00")))
    .withColumn("shift_hours", F.lit(8))
)

# dim_product
df_dim_product = (
    df_prod
    .select("product_id")
    .dropDuplicates(["product_id"])
    .withColumn("product_name",     F.concat(F.lit("Product-"), F.col("product_id")))
    .withColumn("product_family",   F.lit("General"))
    .withColumn("unit_of_measure",  F.lit("PCS"))
)

# dim_date (generated from distinct dates in production data)
df_dim_date = (
    df_prod
    .select(F.col("production_date").alias("date_key"))
    .dropDuplicates(["date_key"])
    .withColumn("year",         F.year("date_key"))
    .withColumn("quarter",      F.quarter("date_key"))
    .withColumn("month",        F.month("date_key"))
    .withColumn("month_name",   F.date_format("date_key", "MMMM"))
    .withColumn("week",         F.weekofyear("date_key"))
    .withColumn("day_of_week",  F.dayofweek("date_key"))
    .withColumn("day_name",     F.date_format("date_key", "EEEE"))
    .withColumn("is_weekend",
        F.when(F.dayofweek("date_key").isin([1, 7]), True).otherwise(False))
)

# ─────────────────────────────────────────────
# STEP 7 — WRITE TO PROCESSED ZONE
# ─────────────────────────────────────────────
logger.info("=== STEP 7: Writing to processed S3 zone ===")

df_fact.write.mode("overwrite").partitionBy("year", "month").parquet(
    f"{PROCESSED_BASE}/fact_production/"
)
df_dim_machine.write.mode("overwrite").parquet(f"{PROCESSED_BASE}/dim_machine/")
df_dim_shift.write.mode("overwrite").parquet(f"{PROCESSED_BASE}/dim_shift/")
df_dim_product.write.mode("overwrite").parquet(f"{PROCESSED_BASE}/dim_product/")
df_dim_date.write.mode("overwrite").parquet(f"{PROCESSED_BASE}/dim_date/")

logger.info("=== ETL Complete ===")
job.commit()