from __future__ import annotations

import json
import os
import random
import time
from dataclasses import dataclass
from typing import Any, Dict, List

from enterprise_shared.schemas import CustomerRowV1, CustomerRowV2
from enterprise_shared.validation import validate_customer_rows

from enterprise_spark.io.atomic_write import atomic_write_text


@dataclass(frozen=True)
class JobResult:
    ok: bool
    output_path: str
    row_count: int
    notes: str


def _load_input_rows(*, scenario: str) -> List[Dict[str, Any]]:
    # For demo/testing we synthesize rows in-memory.
    if scenario == "schema_drift_v2":
        return [{"id": 1, "email": "a@example.com", "country": "US", "schema_version": 2, "loyalty_tier": "gold"}]
    return [{"id": 1, "email": "a@example.com", "country": "US", "schema_version": 1}]


def run(*, scenario: str, output_path: str) -> JobResult:
    """
    Simulated Spark job entrypoint (callable from Airflow).
    - Validates inputs with shared library contracts.
    - Writes output JSON.
    - Includes realistic failure behaviors controlled by scenario (for test environment only).
    """
    rows = _load_input_rows(scenario=scenario)

    # Simulate flaky enrichment API behavior (rate limiting).
    if scenario == "api_flaky_429":
        if random.random() < 0.6:
            raise RuntimeError("Upstream enrichment API returned 429 Too Many Requests")
        time.sleep(0.15)

    # Contract validation (shared lib). This is where schema drift shows up.
    validate_customer_rows(rows=rows)

    # Silent corruption scenario: round currency values incorrectly (bug).
    if scenario == "silent_corruption_rounding":
        # Pretend job enriches monetary fields; the bug is subtle.
        for r in rows:
            r["credit_limit_usd"] = float(int(1234.56))  # incorrect truncation

    # Silent data corruption scenario: duplicate primary keys (idempotency bug).
    if scenario == "silent_data_corruption":
        # Simulate a bug that causes duplicate records with same primary key
        rows = rows + rows

    out = {"schema_version": rows[0].get("schema_version", 1), "rows": rows}

    # Partial write scenario: write half and crash.
    if scenario == "partial_write":
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        blob = json.dumps(out, indent=2)
        half = blob[: max(1, len(blob) // 2)]
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(half)
            f.flush()
        raise RuntimeError("Simulated crash during write (partial output emitted)")

    # Normal path: atomic write.
    res = atomic_write_text(output_path, json.dumps(out, indent=2))
    if not res.ok:
        raise RuntimeError(f"Failed to write output atomically: {output_path}")

    # Type hints used by downstream (kept realistic).
    if out["schema_version"] == 2:
        _ = [CustomerRowV2(**r) for r in out["rows"]]
    else:
        _ = [CustomerRowV1(**r) for r in out["rows"]]

    return JobResult(ok=True, output_path=output_path, row_count=len(rows), notes="ok")


