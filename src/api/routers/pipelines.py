from __future__ import annotations

import asyncio
import time
import uuid
from datetime import UTC, datetime

import structlog
from fastapi import APIRouter, BackgroundTasks, HTTPException, Request

from src.agents.orchestrator import run_pipeline_analysis
from src.api.models.schemas import (
    AgentRunStatus,
    PipelineRunRequest,
    PipelineRunResponse,
)

logger = structlog.get_logger(__name__)
router = APIRouter()

# In-memory run store: lost on restart, not shared between workers
_runs: dict[str, AgentRunStatus] = {}


def _now() -> datetime:
    return datetime.now(UTC)


def _record_metrics(metrics, pipeline_name: str, status: str, seconds: float, result) -> None:
    if metrics is None:
        return
    metrics.record_pipeline_run(pipeline_name, status, seconds)
    for r in (result or {}).get("quality_results", []):
        metrics.record_quality_check(
            r.get("check_type", ""), r.get("passed", False), r.get("table_name", "")
        )
    for a in (result or {}).get("remediation_actions", []):
        metrics.record_agent_action(
            "remediation", a.get("action_taken", ""), a.get("success", False)
        )


async def _execute_pipeline_analysis(
    run_id: str,
    request: PipelineRunRequest,
    graph,
    demo_mode: bool = False,
    timeout_seconds: float = 300,
    metrics=None,
) -> None:
    """Background task that executes the agent graph and updates run status."""
    started = time.monotonic()
    result = None
    try:
        _runs[run_id].status = "RUNNING"
        logger.info("Pipeline analysis started", run_id=run_id, pipeline=request.pipeline_name)

        result = await asyncio.wait_for(
            run_pipeline_analysis(
                pipeline_name=request.pipeline_name,
                table_names=request.table_names,
                warehouse_id=request.warehouse_id,
                graph=graph,
                cluster_id=request.cluster_id or "",
                job_id=request.job_id,
                demo_mode=demo_mode,
            ),
            timeout=timeout_seconds,
        )

        final_status = "ESCALATED" if result.get("should_escalate") else "COMPLETE"
        run = _runs[run_id]
        run.status = final_status
        run.completed_at = _now()
        run.report = result.get("final_report", "")
        run.quality_results = result.get("quality_results", [])
        run.root_causes = result.get("root_causes", [])
        run.cost_recommendations = result.get("cost_recommendations", [])
        run.remediation_actions = result.get("remediation_actions", [])
        run.should_escalate = result.get("should_escalate", False)

        logger.info(
            "Pipeline analysis complete",
            run_id=run_id,
            status=final_status,
            pipeline=request.pipeline_name,
        )
    except Exception as exc:
        message = (
            f"Timed out after {timeout_seconds}s" if isinstance(exc, TimeoutError) else str(exc)
        )
        logger.error("Pipeline analysis failed", run_id=run_id, error=message)
        _runs[run_id].status = "FAILED"
        _runs[run_id].completed_at = _now()
        _runs[run_id].error = message
    finally:
        _record_metrics(
            metrics, request.pipeline_name, _runs[run_id].status, time.monotonic() - started, result
        )


@router.post("/run", response_model=PipelineRunResponse, status_code=202)
async def trigger_pipeline_run(
    request: PipelineRunRequest,
    background_tasks: BackgroundTasks,
    app_request: Request,
) -> PipelineRunResponse:
    """Trigger an async pipeline analysis run. Returns immediately with a run_id."""
    run_id = str(uuid.uuid4())
    started_at = _now()

    _runs[run_id] = AgentRunStatus(
        run_id=run_id,
        status="PENDING",
        pipeline_name=request.pipeline_name,
        started_at=started_at,
    )

    # The compiled graph lives in app state (None in the lightweight test app)
    state = app_request.app.state
    graph = getattr(state, "graph", None)
    if graph is not None:
        settings = getattr(state, "settings", None)
        background_tasks.add_task(
            _execute_pipeline_analysis,
            run_id,
            request,
            graph,
            demo_mode=bool(settings and settings.demo_mode),
            timeout_seconds=settings.agent_timeout_seconds if settings else 300,
            metrics=getattr(state, "metrics_collector", None),
        )

    logger.info("Pipeline run triggered", run_id=run_id, pipeline=request.pipeline_name)
    return PipelineRunResponse(
        run_id=run_id,
        status="PENDING",
        pipeline_name=request.pipeline_name,
        started_at=started_at,
    )


@router.get("/{run_id}/status", response_model=AgentRunStatus)
async def get_run_status(run_id: str) -> AgentRunStatus:
    """Get the current status of a pipeline analysis run."""
    run = _runs.get(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found")
    return run


@router.get("/", response_model=list[AgentRunStatus])
async def list_runs() -> list[AgentRunStatus]:
    """List all pipeline analysis runs with their current status."""
    return sorted(_runs.values(), key=lambda r: r.started_at, reverse=True)


@router.delete("/{run_id}")
async def cancel_run(run_id: str) -> dict:
    """Mark a PENDING/RUNNING run as cancelled.

    Note: this only updates the stored status; an analysis already running in the
    background is not interrupted.
    """
    run = _runs.get(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found")

    if run.status not in ("PENDING", "RUNNING"):
        raise HTTPException(
            status_code=409,
            detail=f"Cannot cancel run in status '{run.status}'",
        )

    _runs[run_id].status = "FAILED"
    _runs[run_id].completed_at = _now()
    _runs[run_id].error = "Cancelled by user request"
    logger.info("Pipeline run cancelled", run_id=run_id)
    return {"run_id": run_id, "status": "FAILED", "message": "Run cancelled"}
