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

    spark = SparkSession.builder.appName("enterprise_spark_submit_entrypoint").getOrCreate()

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

    # Emit a small proof artifact for debugging.
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    payload = {"scenario": scenario, "row_count": df.count()}
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f)

    spark.stop()


if __name__ == "__main__":
    main()


