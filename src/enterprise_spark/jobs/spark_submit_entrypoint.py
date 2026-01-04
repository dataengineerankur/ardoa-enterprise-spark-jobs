from __future__ import annotations

import argparse
import json
import os
from typing import Any

from pyspark.sql import SparkSession

from enterprise_shared.validation import validate_customer_rows


def main() -> None:
    """
    Real Spark entrypoint for local validation (spark-submit).

    This is intentionally minimal and deterministic:
    - Creates a SparkSession
    - Builds a small DataFrame
    - Executes a transformation that can fail in realistic ways
    - Writes a tiny JSON artifact to the mounted output path
    """
    ap = argparse.ArgumentParser()
    ap.add_argument("--scenario", default="spark_missing_column", type=str)
    ap.add_argument("--out", default="/tmp/ardoa_spark_submit_out.json", type=str)
    args = ap.parse_args()

    scenario = (args.scenario or "").strip() or "spark_missing_column"
    out_path = args.out

    # Enable adaptive query execution for skew handling
    builder = SparkSession.builder.appName("enterprise_spark_submit_entrypoint")
    
    # Apply skew mitigation configs when needed
    if scenario == "data_skew_oom":
        builder = (
            builder.config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
            .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
        )
    
    spark = builder.getOrCreate()

    # Synthetic input (enterprise-like envelope)
    rows: list[dict[str, Any]] = [
        {"schema_version": 1, "id": 1, "email": "a@example.com", "country": "US"},
        {"schema_version": 1, "id": 2, "email": "b@example.com", "country": "CA"},
    ]
    # Shared lib validation (cross-repo dependency)
    validate_customer_rows(rows=rows)

    df = spark.createDataFrame(rows)

    # Intentional spark failure scenario (real Spark AnalysisException).
    # This mimics a common production issue: selecting a column that doesn't exist due to drift/typo.
    if scenario == "spark_missing_column":
        df = df.select("id", "email", "cntry")  # BUG: should be "country"
        # Trigger evaluation
        _ = df.count()

    # Happy path (should succeed after ARDOA fix)
    if scenario == "spark_happy":
        df = df.select("id", "email", "country")
        _ = df.count()

    # Data skew OOM scenario - demonstrates skew handling with AQE
    if scenario == "data_skew_oom":
        # Simulate aggregation that would cause skew without AQE
        # With adaptive query execution enabled, this should succeed
        df = df.groupBy("country").count()
        _ = df.collect()

    # Emit a small proof artifact for debugging.
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    payload = {"scenario": scenario, "row_count": df.count()}
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f)

    spark.stop()


if __name__ == "__main__":
    main()


