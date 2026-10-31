from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator

# catalog.schema.table (1-3 parts) of plain identifiers. Table names are interpolated
# into SQL, so anything else (spaces, quotes, semicolons, subqueries) is rejected.
_TABLE_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){0,2}$")


def validate_table_name(name: str) -> str:
    if not _TABLE_NAME.match(name):
        raise ValueError(f"Invalid table name '{name}': expected catalog.schema.table")
    return name


class PipelineRunRequest(BaseModel):
    pipeline_name: str = Field(..., description="Name of the pipeline to analyze")
    table_names: list[str] = Field(..., description="List of Delta table names to check")
    warehouse_id: str = Field(..., description="SQL warehouse ID for query execution")
    cluster_id: str = Field(default="", description="Cluster ID for compute analysis")
    job_id: int | None = Field(default=None, description="Databricks job ID for run history")

    @field_validator("table_names")
    @classmethod
    def _check_tables(cls, value: list[str]) -> list[str]:
        return [validate_table_name(t) for t in value]


class PipelineRunResponse(BaseModel):
    run_id: str = Field(..., description="Unique run identifier")
    status: str = Field(..., description="Initial status (PENDING)")
    pipeline_name: str
    started_at: datetime


class QualityCheckRequest(BaseModel):
    table_name: str = Field(..., description="Fully qualified table name")
    warehouse_id: str = Field(..., description="SQL warehouse ID")
    rules: list[dict[str, Any]] = Field(
        default_factory=list,
        description="List of quality rule dicts with keys: column, check_type, params",
    )

    @field_validator("table_name")
    @classmethod
    def _check_table(cls, value: str) -> str:
        return validate_table_name(value)


class QualityCheckResponse(BaseModel):
    table_name: str
    total_checks: int
    passed: int
    failed: int
    results: list[dict[str, Any]]


class CostAnalysisRequest(BaseModel):
    cluster_id: str = Field(..., description="Databricks cluster ID")
    warehouse_id: str = Field(..., description="SQL warehouse ID for table queries")
    tables: list[str] = Field(
        default_factory=list, description="Tables to analyze for storage costs"
    )

    @field_validator("tables")
    @classmethod
    def _check_tables(cls, value: list[str]) -> list[str]:
        return [validate_table_name(t) for t in value]


class CostAnalysisResponse(BaseModel):
    cluster_id: str
    total_monthly_estimate_usd: float
    recommendations: list[dict[str, Any]]
    potential_savings_usd: float


class AgentRunStatus(BaseModel):
    run_id: str
    status: str = Field(..., description="PENDING | RUNNING | COMPLETE | FAILED | ESCALATED")
    pipeline_name: str
    started_at: datetime
    completed_at: datetime | None = None
    report: str | None = None
    error: str | None = None
    quality_results: list[dict[str, Any]] = Field(default_factory=list)
    root_causes: list[dict[str, Any]] = Field(default_factory=list)
    cost_recommendations: list[dict[str, Any]] = Field(default_factory=list)
    remediation_actions: list[dict[str, Any]] = Field(default_factory=list)
    should_escalate: bool = False


class HealthResponse(BaseModel):
    status: str
    version: str = "1.0.0"
    demo_mode: bool = False
    databricks_connected: bool
    timestamp: datetime
