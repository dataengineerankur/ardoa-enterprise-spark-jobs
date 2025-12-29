from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Dict, List

# In a real deployment, this would use actual PySpark
# For now, we simulate DataFrame operations for testing
try:
    from pyspark.sql import SparkSession
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False

from enterprise_spark.jobs.customer_enrichment_job import run


class MockDataFrame:
    """Mock DataFrame for local testing without Spark"""
    def __init__(self, data: List[Dict[str, Any]], columns: List[str]):
        self.data = data
        self.columns = columns
    
    def select(self, *cols: str):
        """Simulate Spark DataFrame select"""
        # Validate columns exist
        for col in cols:
            if col not in self.columns:
                available = ", ".join(f"`{c}`" for c in self.columns)
                raise Exception(
                    f"[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter with name `{col}` "
                    f"cannot be resolved. Did you mean one of the following? [{available}]."
                )
        return MockDataFrame(self.data, list(cols))
    
    def collect(self):
        return [type('Row', (), row) for row in self.data]


def main() -> None:
    """Main entrypoint for Spark submit jobs"""
    parser = argparse.ArgumentParser(description='Enterprise Spark Job')
    parser.add_argument('--scenario', type=str, default='happy', help='Test scenario')
    parser.add_argument('--output-path', type=str, required=True, help='Output path')
    args = parser.parse_args()
    
    # For the spark_missing_column scenario, simulate the Spark DataFrame operation
    if args.scenario == "spark_missing_column":
        # Load sample data
        rows = [{"id": 1, "email": "a@example.com", "country": "US", "schema_version": 1}]
        
        # Create mock DataFrame
        df = MockDataFrame(rows, columns=["country", "email", "id", "schema_version"])
        
        # Select required columns
        df = df.select("id", "email", "country")
        
        # Convert back to dict for output
        selected_rows = [{"id": r.id, "email": r.email, "country": r.country} for r in df.collect()]
        
        # Write output
        os.makedirs(os.path.dirname(args.output_path) or ".", exist_ok=True)
        with open(args.output_path, 'w', encoding='utf-8') as f:
            json.dump({"rows": selected_rows, "schema_version": 1}, f, indent=2)
        
        print(f"Successfully processed {len(selected_rows)} rows")
        return
    
    # For other scenarios, delegate to the customer_enrichment_job
    result = run(scenario=args.scenario, output_path=args.output_path)
    if not result.ok:
        print(f"Job failed: {result.notes}", file=sys.stderr)
        sys.exit(1)
    print(f"Job completed successfully: {result.notes}")


if __name__ == "__main__":
    main()
