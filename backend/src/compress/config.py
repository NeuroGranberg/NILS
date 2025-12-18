"""Configuration for the post-anonymization compression stage."""

from __future__ import annotations

from pathlib import Path
from typing import Literal, Optional

from pydantic import BaseModel, Field

Strategy = Literal["ordered", "ffd"]


class CompressionConfig(BaseModel):
    root: Path
    out_dir: Path
    chunk: str = Field("100GB", min_length=2)
    strategy: Strategy = "ordered"
    compression: int = Field(3, ge=0, le=9)
    workers: int = Field(2, ge=1, le=16)
    password: str = Field(..., min_length=1)
    verify: bool = True
    par2: int = Field(0, ge=0, le=50)

    model_config = {
        "arbitrary_types_allowed": True,
    }
