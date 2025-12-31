from __future__ import annotations

from typing import Any, Dict, List


def validate_customer_rows(*, rows: List[Dict[str, Any]]) -> None:
    """Validate customer rows against schema."""
    for row in rows:
        if "id" not in row:
            raise ValueError("Missing required field: id")
        if "email" not in row:
            raise ValueError("Missing required field: email")
        if "schema_version" not in row:
            raise ValueError("Missing required field: schema_version")
