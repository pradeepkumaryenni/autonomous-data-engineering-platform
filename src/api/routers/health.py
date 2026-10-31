from __future__ import annotations

from datetime import UTC, datetime

import structlog
from fastapi import APIRouter, Request
from fastapi.responses import PlainTextResponse, RedirectResponse

from src.api.models.schemas import HealthResponse

logger = structlog.get_logger(__name__)
router = APIRouter()


@router.get("/", include_in_schema=False)
async def root_redirect():
    """Redirect root to API docs."""
    return RedirectResponse(url="/docs")


@router.get("/health", response_model=HealthResponse)
async def health_check(app_request: Request) -> HealthResponse:
    """API health. In live mode also checks Databricks connectivity."""
    settings = getattr(app_request.app.state, "settings", None)
    if settings is not None and settings.demo_mode:
        # Nothing external to check: the demo serves sample data from memory
        return HealthResponse(
            status="ok",
            demo_mode=True,
            databricks_connected=False,
            timestamp=datetime.now(UTC),
        )

    db_client = getattr(app_request.app.state, "db_client", None)
    databricks_connected = False

    if db_client:
        try:
            cluster_id = getattr(app_request.app.state, "cluster_id", "")
            if cluster_id:
                await db_client.get_cluster_state(cluster_id)
            else:
                await db_client.list_jobs()
            databricks_connected = True
        except Exception as exc:
            logger.warning("Databricks connectivity check failed", error=str(exc))
            databricks_connected = False

    return HealthResponse(
        status="ok" if databricks_connected else "degraded",
        demo_mode=False,
        databricks_connected=databricks_connected,
        timestamp=datetime.now(UTC),
    )


@router.get("/metrics", response_class=PlainTextResponse)
async def get_metrics(app_request: Request) -> str:
    """Return Prometheus metrics in text format."""
    metrics_collector = getattr(app_request.app.state, "metrics_collector", None)
    if metrics_collector:
        return metrics_collector.get_prometheus_output()
    # Fall back to the default registry if the collector is not initialized
    from prometheus_client import REGISTRY, generate_latest

    return generate_latest(REGISTRY).decode("utf-8")
