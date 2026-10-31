from __future__ import annotations

from typing import Any

import structlog
from fastapi import APIRouter, HTTPException, Request

from src.api.models.schemas import CostAnalysisRequest, CostAnalysisResponse

logger = structlog.get_logger(__name__)
router = APIRouter()


@router.post("/analyze", response_model=CostAnalysisResponse)
async def analyze_costs(
    request: CostAnalysisRequest,
    app_request: Request,
) -> CostAnalysisResponse:
    """Run cost analysis for a cluster and a set of tables."""
    cost_agent = getattr(app_request.app.state, "cost_agent", None)
    if not cost_agent:
        raise HTTPException(status_code=503, detail="Cost optimizer agent not initialized")

    try:
        cluster_analysis = await cost_agent.analyze_cluster_costs(request.cluster_id)
        storage_analysis: list[dict] = []
        if request.tables:
            storage_analysis = await cost_agent.analyze_storage_costs(
                request.tables, request.warehouse_id
            )
        recommendations = cost_agent.generate_recommendations(cluster_analysis, storage_analysis)
        # Remembered so POST /optimize?confirm=true can apply the SQL ones
        app_request.app.state.last_cost_recommendations = recommendations
    except Exception as exc:
        logger.error("Cost analysis failed", error=str(exc))
        raise HTTPException(status_code=502, detail=f"Cost analysis failed: {exc}")

    total_savings = sum(r.get("estimated_monthly_savings_usd", 0.0) for r in recommendations)

    return CostAnalysisResponse(
        cluster_id=request.cluster_id,
        total_monthly_estimate_usd=cluster_analysis.get("monthly_cost_usd", 0.0),
        recommendations=recommendations,
        potential_savings_usd=round(total_savings, 2),
    )


@router.get("/clusters/{cluster_id}")
async def get_cluster_cost(
    cluster_id: str,
    app_request: Request,
) -> dict[str, Any]:
    """Return detailed cost breakdown for a specific cluster."""
    cost_agent = getattr(app_request.app.state, "cost_agent", None)
    if not cost_agent:
        raise HTTPException(status_code=503, detail="Cost optimizer agent not initialized")

    try:
        analysis = await cost_agent.analyze_cluster_costs(cluster_id)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch cluster cost data: {exc}")

    return analysis


@router.post("/optimize")
async def apply_optimizations(
    warehouse_id: str,
    app_request: Request,
    confirm: bool = False,
) -> dict[str, Any]:
    """Apply the OPTIMIZE/VACUUM commands from the last /analyze call. Requires confirm=true."""
    if not confirm:
        raise HTTPException(
            status_code=400,
            detail=(
                "Set confirm=true to apply optimizations. "
                "This will run OPTIMIZE/VACUUM on tables."
            ),
        )

    db_client = getattr(app_request.app.state, "db_client", None)
    cost_agent = getattr(app_request.app.state, "cost_agent", None)
    if not db_client or not cost_agent:
        raise HTTPException(status_code=503, detail="Services not initialized")

    # This endpoint reads the last cost recommendations from app state if available
    last_recommendations = getattr(app_request.app.state, "last_cost_recommendations", [])
    applied = []
    errors = []

    for rec in last_recommendations:
        sql = rec.get("sql_command_or_api_call", "")
        if sql and sql.strip().upper().startswith(("OPTIMIZE", "VACUUM")):
            try:
                await db_client.execute_sql(warehouse_id, sql)
                applied.append({"sql": sql, "status": "success"})
            except Exception as exc:
                errors.append({"sql": sql, "error": str(exc)})

    return {
        "applied_count": len(applied),
        "error_count": len(errors),
        "applied": applied,
        "errors": errors,
    }
