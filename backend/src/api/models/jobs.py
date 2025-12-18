"""Pydantic schemas for job management."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, model_validator


class UpdateExtractPerformancePayload(BaseModel):
    maxWorkers: int | None = Field(default=None, ge=1, le=128)
    batchSize: int | None = Field(default=None, ge=10, le=5000)
    queueSize: int | None = Field(default=None, ge=1, le=500)
    seriesWorkersPerSubject: int | None = Field(default=None, ge=1, le=16)
    adaptiveBatchingEnabled: bool | None = None
    adaptiveTargetTxMs: int | None = Field(default=None, ge=50, le=2000)
    adaptiveMinBatchSize: int | None = Field(default=None, ge=10, le=10000)
    adaptiveMaxBatchSize: int | None = Field(default=None, ge=50, le=20000)
    useProcessPool: bool | None = None
    processPoolWorkers: int | None = Field(default=None, ge=1, le=128)
    dbWriterPoolSize: int | None = Field(default=None, ge=1, le=16)

    @model_validator(mode="after")
    def _validate_adaptive_bounds(self):
        min_size = self.adaptiveMinBatchSize
        max_size = self.adaptiveMaxBatchSize
        if min_size is not None and max_size is not None and min_size > max_size:
            raise ValueError("adaptiveMinBatchSize cannot exceed adaptiveMaxBatchSize")
        return self

    def has_updates(self) -> bool:
        return any(
            getattr(self, field) is not None
            for field in (
                "maxWorkers",
                "batchSize",
                "queueSize",
                "seriesWorkersPerSubject",
                "adaptiveBatchingEnabled",
                "adaptiveTargetTxMs",
                "adaptiveMinBatchSize",
                "adaptiveMaxBatchSize",
                "useProcessPool",
                "processPoolWorkers",
                "dbWriterPoolSize",
            )
        )
