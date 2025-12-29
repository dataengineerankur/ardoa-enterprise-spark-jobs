from __future__ import annotations

from typing import Any, Dict

from pydantic import BaseModel


class CustomerRowV1(BaseModel):
    """Schema for customer row version 1."""

    id: int
    email: str
    country: str
    schema_version: int = 1


class CustomerRowV2(BaseModel):
    """Schema for customer row version 2."""

    id: int
    email: str
    country: str
    schema_version: int = 2
    loyalty_tier: str
