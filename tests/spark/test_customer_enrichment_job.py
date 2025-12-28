from __future__ import annotations

import json
import os
import tempfile

import pytest

from enterprise_spark.jobs.customer_enrichment_job import run


def test_happy_path_schema_v1_writes_json() -> None:
    with tempfile.TemporaryDirectory() as td:
        out = os.path.join(td, "out.json")
        res = run(scenario="happy", output_path=out)
        assert res.ok is True
        assert os.path.exists(out)
        payload = json.load(open(out, "r", encoding="utf-8"))
        assert payload["schema_version"] == 1


def test_partial_write_uses_atomic_write() -> None:
    """Test that partial_write scenario now uses atomic write (no longer crashes)."""
    with tempfile.TemporaryDirectory() as td:
        out = os.path.join(td, "out.json")
        res = run(scenario="partial_write", output_path=out)
        assert res.ok is True
        assert os.path.exists(out)
        # Verify the file is valid JSON and complete
        payload = json.load(open(out, "r", encoding="utf-8"))
        assert payload["schema_version"] == 1
        assert "rows" in payload


