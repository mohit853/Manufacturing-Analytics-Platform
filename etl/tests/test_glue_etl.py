"""
etl/tests/test_glue_etl.py
Unit tests for manufacturing ETL transformations.
Run locally with: pytest etl/tests/test_glue_etl.py -v
Requires: pyspark, pytest
"""

import pytest
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, BooleanType, DateType
)


# ─────────────────────────────────────────────
# SPARK FIXTURE
# ─────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("manufacturing-etl-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


# ─────────────────────────────────────────────
# SAMPLE DATA FIXTURES
# ─────────────────────────────────────────────

@pytest.fixture
def production_df(spark):
    data = [
        ("P001", "PLT1", "L1", "M001", "S1", "PRD1", "2024-01-15", 500, 480, 470, 480.0, 460.0),
        ("P002", "PLT1", "L1", "M002", "S2", "PRD2", "2024-01-15", 300, 310, 295, 480.0, 480.0),
        ("P003", "PLT1", "L2", "M003", "S1", "PRD1", "2024-01-16", 400,   0, 380, 480.0, 420.0),
        # Duplicate — should be dropped
        ("P001", "PLT1", "L1", "M001", "S1", "PRD1", "2024-01-15", 500, 480, 470, 480.0, 460.0),
        # Negative qty — should be clamped to 0
        ("P004", "PLT1", "L1", "M001", "S3", "PRD2", "2024-01-17", -10, -5, -1, 480.0, 480.0),
    ]
    schema = StructType([
        StructField("production_id",       StringType(),  False),
        StructField("plant_id",            StringType(),  False),
        StructField("line_id",             StringType(),  False),
        StructField("machine_id",          StringType(),  False),
        StructField("shift_id",            StringType(),  False),
        StructField("product_id",          StringType(),  False),
        StructField("production_date",     StringType(),  False),
        StructField("planned_qty",         IntegerType(), True),
        StructField("actual_qty",          IntegerType(), True),
        StructField("good_qty",            IntegerType(), True),
        StructField("planned_runtime_min", DoubleType(),  True),
        StructField("actual_runtime_min",  DoubleType(),  True),
    ])
    return spark.createDataFrame(data, schema)


@pytest.fixture
def downtime_df(spark):
    data = [
        ("D001", "M001", "2024-01-15", "S1", "Mechanical", 30.0, "N"),
        ("D002", "M001", "2024-01-15", "S1", "Planned PM",  60.0, "Y"),
        ("D003", "M002", "2024-01-15", "S2", "Electrical",  15.0, "N"),
        ("D004", "M003", "2024-01-16", "S1", "Changeover",  45.0, "Y"),
        # Negative downtime — should be clamped
        ("D005", "M001", "2024-01-17", "S3", "Unknown",    -10.0, "N"),
    ]
    schema = StructType([
        StructField("downtime_id",      StringType(), False),
        StructField("machine_id",       StringType(), False),
        StructField("production_date",  StringType(), False),
        StructField("shift_id",         StringType(), False),
        StructField("downtime_reason",  StringType(), True),
        StructField("downtime_minutes", DoubleType(), True),
        StructField("is_planned",       StringType(), True),
    ])
    return spark.createDataFrame(data, schema)


@pytest.fixture
def quality_df(spark):
    data = [
        ("Q001", "M001", "PRD1", "2024-01-15", "S1", 470, 10, "Surface"),
        ("Q002", "M002", "PRD2", "2024-01-15", "S2", 295,  5, "Dimensional"),
        # defect_qty > inspected_qty — should be capped
        ("Q003", "M003", "PRD1", "2024-01-16", "S1", 100, 150, "Assembly"),
    ]
    schema = StructType([
        StructField("inspection_id",   StringType(),  False),
        StructField("machine_id",      StringType(),  False),
        StructField("product_id",      StringType(),  False),
        StructField("production_date", StringType(),  False),
        StructField("shift_id",        StringType(),  False),
        StructField("inspected_qty",   IntegerType(), True),
        StructField("defect_qty",      IntegerType(), True),
        StructField("defect_category", StringType(),  True),
    ])
    return spark.createDataFrame(data, schema)


# ─────────────────────────────────────────────
# CLEANING TESTS
# ─────────────────────────────────────────────

class TestProductionCleaning:

    def test_deduplication(self, spark, production_df):
        cleaned = production_df.dropDuplicates(["production_id"])
        assert cleaned.count() == 4  # P001 duplicate removed

    def test_date_cast(self, spark, production_df):
        cleaned = (
            production_df
            .dropDuplicates(["production_id"])
            .withColumn("production_date", F.to_date("production_date", "yyyy-MM-dd"))
        )
        assert cleaned.schema["production_date"].dataType == DateType()
        assert cleaned.filter(F.col("production_date").isNull()).count() == 0

    def test_negative_qty_clamped(self, spark, production_df):
        cleaned = (
            production_df
            .dropDuplicates(["production_id"])
            .withColumn("planned_qty", F.when(F.col("planned_qty") < 0, 0).otherwise(F.col("planned_qty")))
            .withColumn("actual_qty",  F.when(F.col("actual_qty")  < 0, 0).otherwise(F.col("actual_qty")))
            .withColumn("good_qty",    F.when(F.col("good_qty")    < 0, 0).otherwise(F.col("good_qty")))
        )
        assert cleaned.filter(F.col("planned_qty") < 0).count() == 0
        assert cleaned.filter(F.col("actual_qty")  < 0).count() == 0
        assert cleaned.filter(F.col("good_qty")    < 0).count() == 0


class TestDowntimeCleaning:

    def test_is_planned_flag(self, spark, downtime_df):
        cleaned = (
            downtime_df
            .withColumn("is_planned_flag", F.when(F.upper("is_planned") == "Y", True).otherwise(False))
        )
        planned   = cleaned.filter(F.col("is_planned_flag") == True).count()
        unplanned = cleaned.filter(F.col("is_planned_flag") == False).count()
        assert planned   == 2
        assert unplanned == 3

    def test_negative_downtime_clamped(self, spark, downtime_df):
        cleaned = downtime_df.withColumn(
            "downtime_minutes",
            F.when(F.col("downtime_minutes") < 0, 0).otherwise(F.col("downtime_minutes"))
        )
        assert cleaned.filter(F.col("downtime_minutes") < 0).count() == 0


class TestQualityCleaning:

    def test_defect_capped_at_inspected(self, spark, quality_df):
        cleaned = quality_df.withColumn(
            "defect_qty", F.least(F.col("defect_qty"), F.col("inspected_qty"))
        )
        invalid = cleaned.filter(F.col("defect_qty") > F.col("inspected_qty")).count()
        assert invalid == 0

    def test_defect_cap_value(self, spark, quality_df):
        cleaned = quality_df.withColumn(
            "defect_qty", F.least(F.col("defect_qty"), F.col("inspected_qty"))
        )
        row = cleaned.filter(F.col("inspection_id") == "Q003").collect()[0]
        assert row["defect_qty"] == 100  # capped from 150 to 100


# ─────────────────────────────────────────────
# AGGREGATION TESTS
# ─────────────────────────────────────────────

class TestDowntimeAggregation:

    def test_aggregation_grouping(self, spark, downtime_df):
        cleaned = (
            downtime_df
            .withColumn("is_planned_flag", F.when(F.upper("is_planned") == "Y", True).otherwise(False))
            .withColumn("downtime_minutes", F.when(F.col("downtime_minutes") < 0, 0).otherwise(F.col("downtime_minutes")))
            .withColumn("production_date", F.to_date("production_date", "yyyy-MM-dd"))
        )
        agg = cleaned.groupBy("machine_id", "production_date", "shift_id").agg(
            F.sum("downtime_minutes").alias("total_downtime_min"),
            F.sum(F.when(F.col("is_planned_flag"), F.col("downtime_minutes")).otherwise(0)).alias("planned_downtime_min"),
            F.sum(F.when(~F.col("is_planned_flag"), F.col("downtime_minutes")).otherwise(0)).alias("unplanned_downtime_min"),
        )
        m1 = agg.filter(F.col("machine_id") == "M001").collect()[0]
        assert m1["total_downtime_min"]     == 90.0
        assert m1["planned_downtime_min"]   == 60.0
        assert m1["unplanned_downtime_min"] == 30.0


# ─────────────────────────────────────────────
# OEE CALCULATION TESTS
# ─────────────────────────────────────────────

class TestOEECalculation:

    def _make_fact_row(self, spark, planned_runtime, unplanned_downtime, planned_qty, actual_qty, good_qty):
        data = [(float(planned_runtime), float(unplanned_downtime), int(planned_qty), int(actual_qty), int(good_qty))]
        schema = StructType([
            StructField("planned_runtime_min",      DoubleType(), True),
            StructField("unplanned_downtime_min",   DoubleType(), True),
            StructField("planned_qty",              IntegerType(), True),
            StructField("actual_qty",               IntegerType(), True),
            StructField("good_qty",                 IntegerType(), True),
        ])
        return spark.createDataFrame(data, schema)

    def test_perfect_oee(self, spark):
        df = self._make_fact_row(spark, 480, 0, 500, 500, 500)
        df = (
            df
            .withColumn("availability", F.when(F.col("planned_runtime_min") > 0,
                (F.col("planned_runtime_min") - F.col("unplanned_downtime_min")) / F.col("planned_runtime_min")).otherwise(0))
            .withColumn("performance",  F.when(F.col("planned_qty") > 0,
                F.col("actual_qty") / F.col("planned_qty")).otherwise(0))
            .withColumn("quality_rate", F.when(F.col("actual_qty") > 0,
                F.col("good_qty") / F.col("actual_qty")).otherwise(0))
            .withColumn("oee_pct", F.round(F.col("availability") * F.col("performance") * F.col("quality_rate") * 100, 2))
        )
        row = df.collect()[0]
        assert row["availability"] == 1.0
        assert row["performance"]  == 1.0
        assert row["quality_rate"] == 1.0
        assert row["oee_pct"]      == 100.0

    def test_partial_oee(self, spark):
        # Availability=0.9, Performance=0.96, Quality=0.979 → OEE ≈ 84.7
        df = self._make_fact_row(spark, 480, 48, 500, 480, 470)
        df = (
            df
            .withColumn("availability", F.when(F.col("planned_runtime_min") > 0,
                (F.col("planned_runtime_min") - F.col("unplanned_downtime_min")) / F.col("planned_runtime_min")).otherwise(0))
            .withColumn("performance",  F.when(F.col("planned_qty") > 0,
                F.col("actual_qty") / F.col("planned_qty")).otherwise(0))
            .withColumn("quality_rate", F.when(F.col("actual_qty") > 0,
                F.col("good_qty") / F.col("actual_qty")).otherwise(0))
            .withColumn("oee_pct", F.round(F.col("availability") * F.col("performance") * F.col("quality_rate") * 100, 2))
        )
        row = df.collect()[0]
        assert row["availability"] == pytest.approx(0.9,    rel=1e-3)
        assert row["performance"]  == pytest.approx(0.96,   rel=1e-3)
        assert row["oee_pct"]      == pytest.approx(84.74,  rel=1e-2)

    def test_zero_planned_qty(self, spark):
        df = self._make_fact_row(spark, 480, 0, 0, 100, 100)
        df = df.withColumn("performance",
            F.when(F.col("planned_qty") > 0, F.col("actual_qty") / F.col("planned_qty")).otherwise(0))
        assert df.collect()[0]["performance"] == 0.0