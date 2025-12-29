"""Customer data schemas."""
from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class CustomerRowV1(BaseModel):
    """Customer row schema version 1."""
    id: int
    email: str
    country: str
    schema_version: int = Field(default=1)


class CustomerRowV2(BaseModel):
    """Customer row schema version 2 with additional fields."""
    id: int
    email: str
    country: str
    schema_version: int = Field(default=2)
    loyalty_tier: Optional[str] = None


class ContractError(Exception):
    """Raised when data validation fails."""
    pass
