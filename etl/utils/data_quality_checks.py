"""
etl/utils/data_quality_checks.py
Pre-load data quality checks for manufacturing pipeline.
Raises DataQualityError if critical checks fail; logs warnings for soft failures.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from etl.utils.logger import get_logger

logger = get_logger(__name__)


class DataQualityError(Exception):
    """Raised when a critical data quality check fails."""
    pass


class DataQualityChecker:
    """
    Run a suite of quality checks on a PySpark DataFrame.
    Critical checks raise DataQualityError.
    Warning checks log but do not halt the job.
    """

    def __init__(self, df: DataFrame, table_name: str):
        self.df         = df
        self.table_name = table_name
        self.errors     = []
        self.warnings   = []

    # ─────────────────────────────────────────────
    # CRITICAL CHECKS
    # ─────────────────────────────────────────────

    def check_not_empty(self) -> "DataQualityChecker":
        count = self.df.count()
        if count == 0:
            self.errors.append(f"[{self.table_name}] DataFrame is empty.")
        else:
            logger.info(f"[{self.table_name}] Row count OK: {count:,}")
        return self

    def check_no_null_keys(self, key_columns: list) -> "DataQualityChecker":
        for col in key_columns:
            null_count = self.df.filter(F.col(col).isNull()).count()
            if null_count > 0:
                self.errors.append(
                    f"[{self.table_name}] Column '{col}' has {null_count:,} NULL values (key column)."
                )
            else:
                logger.info(f"[{self.table_name}] Key column '{col}' — no NULLs ✓")
        return self

    def check_no_duplicates(self, key_columns: list) -> "DataQualityChecker":
        total   = self.df.count()
        distinct = self.df.dropDuplicates(key_columns).count()
        dupes   = total - distinct
        if dupes > 0:
            self.errors.append(
                f"[{self.table_name}] {dupes:,} duplicate rows on keys {key_columns}."
            )
        else:
            logger.info(f"[{self.table_name}] No duplicates on {key_columns} ✓")
        return self

    def check_date_format(self, date_col: str, fmt: str = "yyyy-MM-dd") -> "DataQualityChecker":
        invalid = (
            self.df
            .withColumn("_parsed", F.to_date(F.col(date_col).cast("string"), fmt))
            .filter(F.col("_parsed").isNull() & F.col(date_col).isNotNull())
            .count()
        )
        if invalid > 0:
            self.errors.append(
                f"[{self.table_name}] Column '{date_col}' has {invalid:,} rows with invalid date format (expected {fmt})."
            )
        else:
            logger.info(f"[{self.table_name}] Date format '{date_col}' OK ✓")
        return self

    # ─────────────────────────────────────────────
    # WARNING CHECKS
    # ─────────────────────────────────────────────

    def check_value_range(
        self, col: str, min_val: float = 0, max_val: float = None
    ) -> "DataQualityChecker":
        condition = F.col(col) < min_val
        if max_val is not None:
            condition = condition | (F.col(col) > max_val)
        out_of_range = self.df.filter(condition).count()
        range_desc = f">= {min_val}" + (f" and <= {max_val}" if max_val else "")
        if out_of_range > 0:
            self.warnings.append(
                f"[{self.table_name}] Column '{col}' has {out_of_range:,} rows outside expected range ({range_desc})."
            )
        else:
            logger.info(f"[{self.table_name}] Value range '{col}' OK ✓")
        return self

    def check_null_ratio(self, col: str, threshold: float = 0.05) -> "DataQualityChecker":
        total     = self.df.count()
        nulls     = self.df.filter(F.col(col).isNull()).count()
        ratio     = nulls / total if total > 0 else 0
        if ratio > threshold:
            self.warnings.append(
                f"[{self.table_name}] Column '{col}' NULL ratio {ratio:.1%} exceeds threshold {threshold:.1%}."
            )
        else:
            logger.info(f"[{self.table_name}] NULL ratio '{col}' OK ({ratio:.1%}) ✓")
        return self

    def check_oee_bounds(self) -> "DataQualityChecker":
        for metric in ["availability", "performance", "quality_rate", "oee_pct"]:
            if metric not in [f.name for f in self.df.schema.fields]:
                continue
            out = self.df.filter(
                (F.col(metric) < 0) | (F.col(metric) > 100)
            ).count()
            if out > 0:
                self.warnings.append(
                    f"[{self.table_name}] OEE metric '{metric}' has {out:,} rows outside [0, 100]."
                )
            else:
                logger.info(f"[{self.table_name}] OEE metric '{metric}' bounds OK ✓")
        return self

    # ─────────────────────────────────────────────
    # FINALISE
    # ─────────────────────────────────────────────

    def run(self) -> None:
        """
        Log all warnings and raise DataQualityError if any critical checks failed.
        Call this after chaining all checks.
        """
        for w in self.warnings:
            logger.warning(w)

        if self.errors:
            error_msg = "\n".join(self.errors)
            logger.error(f"Data quality FAILED:\n{error_msg}")
            raise DataQualityError(f"Data quality checks failed:\n{error_msg}")

        logger.info(f"[{self.table_name}] All data quality checks passed ✅")


# ─────────────────────────────────────────────
# CONVENIENCE FUNCTIONS
# ─────────────────────────────────────────────

def check_production(df: DataFrame) -> None:
    DataQualityChecker(df, "production") \
        .check_not_empty() \
        .check_no_null_keys(["production_id", "machine_id", "production_date"]) \
        .check_no_duplicates(["production_id"]) \
        .check_date_format("production_date") \
        .check_value_range("planned_qty", min_val=0) \
        .check_value_range("actual_qty",  min_val=0) \
        .check_value_range("good_qty",    min_val=0) \
        .check_null_ratio("shift_id",     threshold=0.01) \
        .run()


def check_downtime(df: DataFrame) -> None:
    DataQualityChecker(df, "downtime") \
        .check_not_empty() \
        .check_no_null_keys(["downtime_id", "machine_id"]) \
        .check_no_duplicates(["downtime_id"]) \
        .check_date_format("production_date") \
        .check_value_range("downtime_minutes", min_val=0, max_val=1440) \
        .check_null_ratio("downtime_reason", threshold=0.10) \
        .run()


def check_quality(df: DataFrame) -> None:
    DataQualityChecker(df, "quality") \
        .check_not_empty() \
        .check_no_null_keys(["inspection_id", "machine_id"]) \
        .check_no_duplicates(["inspection_id"]) \
        .check_date_format("production_date") \
        .check_value_range("defect_qty",    min_val=0) \
        .check_value_range("inspected_qty", min_val=0) \
        .run()


def check_fact(df: DataFrame) -> None:
    DataQualityChecker(df, "fact_production") \
        .check_not_empty() \
        .check_no_null_keys(["production_id", "machine_id", "production_date"]) \
        .check_no_duplicates(["production_id"]) \
        .check_oee_bounds() \
        .check_value_range("oee_pct", min_val=0, max_val=100) \
        .run()