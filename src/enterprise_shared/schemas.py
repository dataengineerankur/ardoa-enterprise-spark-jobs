from __future__ import annotations

from typing import Any
from pydantic import BaseModel


class CustomerRowV1(BaseModel):
    id: int
    email: str
    country: str
    schema_version: int = 1


class CustomerRowV2(BaseModel):
    id: int
    email: str
    country: str
    schema_version: int = 2
    loyalty_tier: str
