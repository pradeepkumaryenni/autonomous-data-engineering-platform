from __future__ import annotations

from dataclasses import asdict
from typing import Any

import pandas as pd
import structlog
from fastapi import APIRouter, HTTPException, Request

from src.api.models.schemas import (
    QualityCheckRequest,
    QualityCheckResponse,
    validate_table_name,
)
from src.monitoring.quality_rules import QualityRule

logger = structlog.get_logger(__name__)
router = APIRouter()

SUPPORTED_RULE_TYPES = [
    {
        "check_type": "nulls",
        "description": "Check null percentage in a column. Params: threshold (float, 0-1)",
        "example_params": {"threshold": 0.05},
    },
    {
        "check_type": "duplicates",
        "description": "Count duplicate rows based on key columns. Params: key_columns (list[str])",
        "example_params": {"key_columns": ["id"]},
    },
    {
        "check_type": "schema",
        "description": "Validate column data types. Params: expected_schema (dict col->dtype)",
        "example_params": {"expected_schema": {"id": "int", "amount": "float"}},
    },
    {
        "check_type": "range",
        "description": "Validate value range. Params: min_val, max_val",
        "example_params": {"min_val": 0, "max_val": 1000000},
    },
    {
        "check_type": "referential",
        "description": "Check referential integrity. Params: valid_values (list)",
        "example_params": {"valid_values": ["active", "inactive", "pending"]},
    },
    {
        "check_type": "drift",
        "description": (
            "Detect statistical drift via Z-score. "
            "Params: baseline_mean, baseline_std, z_threshold"
        ),
        "example_params": {"baseline_mean": 100.0, "baseline_std": 10.0, "z_threshold": 3.0},
    },
]


@router.post("/check", response_model=QualityCheckResponse)
async def run_quality_check(
    request: QualityCheckRequest,
    app_request: Request,
) -> QualityCheckResponse:
    """Run quality checks on a table synchronously and return results."""
    db_client = getattr(app_request.app.state, "db_client", None)
    quality_engine = getattr(app_request.app.state, "quality_engine", None)

    if not db_client or not quality_engine:
        raise HTTPException(status_code=503, detail="Services not initialized")

    try:
        sql = f"SELECT * FROM {request.table_name} LIMIT 10000"
        rows = await db_client.execute_sql(request.warehouse_id, sql)
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Failed to fetch data from Databricks: {exc}",
        )

    if not rows:
        return QualityCheckResponse(
            table_name=request.table_name,
            total_checks=0,
            passed=0,
            failed=0,
            results=[],
        )

    df = pd.DataFrame(rows)

    # Build rules from request or use defaults
    rules: list[QualityRule] = []
    for rule_dict in request.rules:
        rules.append(
            QualityRule(
                column=rule_dict.get("column", "*"),
                check_type=rule_dict.get("check_type", "nulls"),
                params=rule_dict.get("params", {}),
            )
        )

    # If no rules provided, run null checks on all columns
    if not rules:
        for col in df.columns:
            rules.append(QualityRule(column=col, check_type="nulls", params={"threshold": 0.05}))

    results = quality_engine.run_all_checks(df, rules)
    passed = sum(1 for r in results if r.passed)
    failed = len(results) - passed

    return QualityCheckResponse(
        table_name=request.table_name,
        total_checks=len(results),
        passed=passed,
        failed=failed,
        results=[asdict(r) for r in results],
    )


@router.get("/rules")
async def get_supported_rules() -> list[dict[str, Any]]:
    """Return the supported quality rule types with descriptions and example params."""
    return SUPPORTED_RULE_TYPES


@router.post("/profile")
async def profile_table(
    table_name: str,
    warehouse_id: str,
    app_request: Request,
) -> dict[str, Any]:
    """Run full statistical profile of a table."""
    try:
        validate_table_name(table_name)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    db_client = getattr(app_request.app.state, "db_client", None)
    if not db_client:
        raise HTTPException(status_code=503, detail="DB client not initialized")

    try:
        sql = f"SELECT * FROM {table_name} LIMIT 10000"
        rows = await db_client.execute_sql(warehouse_id, sql)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch data: {exc}")

    if not rows:
        return {"table_name": table_name, "row_count": 0, "columns": {}}

    df = pd.DataFrame(rows)
    profile: dict[str, Any] = {
        "table_name": table_name,
        "row_count": len(df),
        "column_count": len(df.columns),
        "columns": {},
    }

    for col in df.columns:
        col_profile: dict[str, Any] = {
            "dtype": str(df[col].dtype),
            "null_count": int(df[col].isna().sum()),
            "null_pct": round(df[col].isna().mean(), 4),
            "unique_count": int(df[col].nunique()),
        }

        if pd.api.types.is_numeric_dtype(df[col]):
            non_null = df[col].dropna()
            if len(non_null) > 0:
                col_profile.update(
                    {
                        "min": float(non_null.min()),
                        "max": float(non_null.max()),
                        "mean": round(float(non_null.mean()), 4),
                        "std": round(float(non_null.std()), 4) if len(non_null) > 1 else 0.0,
                        "median": round(float(non_null.median()), 4),
                        "p25": round(float(non_null.quantile(0.25)), 4),
                        "p75": round(float(non_null.quantile(0.75)), 4),
                        "p99": round(float(non_null.quantile(0.99)), 4),
                    }
                )
        else:
            # Categorical: top 10 values
            top_10 = df[col].value_counts().head(10).to_dict()
            col_profile["top_10_values"] = {str(k): int(v) for k, v in top_10.items()}

        profile["columns"][col] = col_profile

    return profile
