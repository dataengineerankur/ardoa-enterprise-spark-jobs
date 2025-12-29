"""Customer data validation."""
from __future__ import annotations

from typing import Any, Dict, List

from enterprise_shared.schemas import ContractError, CustomerRowV1, CustomerRowV2


def validate_customer_rows(*, rows: List[Dict[str, Any]]) -> None:
    """
    Validate customer rows against schemas.
    
    Raises:
        ContractError: If validation fails.
    """
    if not rows:
        return
    
    for row in rows:
        schema_version = row.get("schema_version", 1)
        try:
            if schema_version == 2:
                CustomerRowV2(**row)
            else:
                CustomerRowV1(**row)
        except Exception as e:
            raise ContractError(f"Validation failed for row {row}: {e}") from e
