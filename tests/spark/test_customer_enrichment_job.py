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


def test_partial_write_raises_and_leaves_file() -> None:
    with tempfile.TemporaryDirectory() as td:
        out = os.path.join(td, "out.json")
        with pytest.raises(RuntimeError):
            run(scenario="partial_write", output_path=out)
        assert os.path.exists(out)


def test_silent_data_corruption_no_duplicates() -> None:
    """Test that silent_data_corruption scenario produces unique IDs."""
    with tempfile.TemporaryDirectory() as td:
        out = os.path.join(td, "out.json")
        res = run(scenario="silent_data_corruption", output_path=out)
        assert res.ok is True
        assert os.path.exists(out)
        payload = json.load(open(out, "r", encoding="utf-8"))
        # Extract all IDs and verify they're unique
        ids = [row["id"] for row in payload["rows"]]
        assert len(ids) == len(set(ids)), "IDs should be unique"
        assert len(ids) == 3, "Should have 3 rows for this scenario"


