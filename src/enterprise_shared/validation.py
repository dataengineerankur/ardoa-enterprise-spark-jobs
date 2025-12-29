from __future__ import annotations

from typing import Any, Dict, List


def validate_customer_rows(*, rows: List[Dict[str, Any]]) -> None:
    """
    Validates customer rows against expected schema.
    Raises ValueError if validation fails.
    """
    if not rows:
        raise ValueError("No rows provided for validation")

    for idx, row in enumerate(rows):
        if not isinstance(row, dict):
            raise ValueError(f"Row {idx} is not a dictionary")

        # Check required fields
        required_fields = ["id", "email"]
        for field in required_fields:
            if field not in row:
                raise ValueError(f"Row {idx} missing required field: {field}")
