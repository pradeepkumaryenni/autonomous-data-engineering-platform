from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse

from src.agents.cost_optimizer_agent import CostOptimizerAgent
from src.agents.data_quality_agent import DataQualityAgent
from src.agents.orchestrator import build_graph
from src.agents.remediation_agent import RemediationAgent
from src.agents.reporter_agent import ReporterAgent
from src.agents.root_cause_agent import RootCauseAgent
from src.api.routers import costs, health, pipelines, quality
from src.config.logging import configure_logging
from src.config.settings import Settings, get_settings
from src.databricks.client import DatabricksClient
from src.databricks.delta_utils import DeltaUtils
from src.databricks.demo_client import DemoDatabricksClient, load_quality_contracts
from src.monitoring.anomaly_detector import AnomalyDetector
from src.monitoring.metrics_collector import MetricsCollector
from src.monitoring.quality_rules import DataQualityEngine

logger = structlog.get_logger(__name__)


def build_services(settings: Settings) -> dict:
    """Create the Databricks client, LLM, agents and compiled graph for a settings object."""
    contracts: dict[str, list[dict]] = {}
    db_client: DatabricksClient | DemoDatabricksClient
    if settings.demo_mode:
        db_client = DemoDatabricksClient.from_samples()
        contracts = load_quality_contracts()
        llm = None
        logger.info("DEMO_MODE on: sample data, fixture workspace, rule-based reasoning")
    else:
        db_client = DatabricksClient(host=settings.databricks_host, token=settings.databricks_token)
        llm = None
        if settings.llm_configured:
            try:
                llm = settings.get_llm()
            except Exception as exc:
                logger.warning("LLM initialization failed, using rule-based logic", error=str(exc))
        else:
            logger.info("No AZURE_OPENAI_API_KEY set, agents use rule-based logic")

    quality_engine = DataQualityEngine()
    anomaly_detector = AnomalyDetector()
    delta_utils = DeltaUtils()

    agents = {
        "quality_agent": DataQualityAgent(
            llm=llm, db_client=db_client, quality_engine=quality_engine, contracts=contracts
        ),
        "rca_agent": RootCauseAgent(
            llm=llm, db_client=db_client, anomaly_detector=anomaly_detector
        ),
        "cost_agent": CostOptimizerAgent(llm=llm, db_client=db_client, delta_utils=delta_utils),
        "remediation_agent": RemediationAgent(llm=llm, db_client=db_client),
        "reporter_agent": ReporterAgent(llm=llm),
    }
    return {
        "db_client": db_client,
        "delta_utils": delta_utils,
        "quality_engine": quality_engine,
        "anomaly_detector": anomaly_detector,
        **agents,
        "graph": build_graph(**agents),
    }


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Application lifespan: initialize services on startup, clean up on shutdown."""
    settings: Settings = getattr(app.state, "settings", None) or get_settings()

    configure_logging(settings.log_level)
    logger.info("Starting ADEP platform", version="1.0.0", demo_mode=settings.demo_mode)

    services = build_services(settings)
    for name, value in services.items():
        setattr(app.state, name, value)
    app.state.metrics_collector = MetricsCollector()
    app.state.cluster_id = settings.databricks_cluster_id
    app.state.settings = settings

    logger.info("ADEP platform initialized successfully")
    yield

    logger.info("Shutting down ADEP platform")
    await services["db_client"].close()


@asynccontextmanager
async def testing_lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Minimal lifespan for test environments — skips external connections."""
    from unittest.mock import AsyncMock, MagicMock

    configure_logging("WARNING")
    app.state.db_client = MagicMock()
    app.state.db_client.close = AsyncMock()
    app.state.quality_engine = DataQualityEngine()
    app.state.anomaly_detector = AnomalyDetector()
    app.state.metrics_collector = MetricsCollector()
    app.state.pipeline_runs = {}
    app.state.graph = None
    app.state.cluster_id = "test-cluster"
    yield


def create_app(testing: bool = False, settings: Settings | None = None) -> FastAPI:
    """Create and configure the FastAPI application."""
    chosen_lifespan = testing_lifespan if testing else lifespan
    app = FastAPI(
        title="Autonomous Data Engineering Platform",
        description=(
            "Multi-agent pipeline monitoring for Azure Databricks: data quality checks, "
            "root-cause hypotheses, cost recommendations, guarded remediation and reports. "
            "Set DEMO_MODE=true to run offline on bundled sample data."
        ),
        version="1.0.0",
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=chosen_lifespan,
    )
    if settings is not None:
        app.state.settings = settings

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        logger.error("Unhandled exception", path=request.url.path, error=str(exc))
        return JSONResponse(
            status_code=500,
            content={"detail": f"Internal server error: {str(exc)}"},
        )

    app.include_router(health.router, prefix="/api/v1/health", tags=["health"])
    app.include_router(pipelines.router, prefix="/api/v1/pipelines", tags=["pipelines"])
    app.include_router(quality.router, prefix="/api/v1/quality", tags=["quality"])
    app.include_router(costs.router, prefix="/api/v1/costs", tags=["costs"])

    @app.get("/", include_in_schema=False)
    async def root():
        return RedirectResponse(url="/docs")

    return app


app = create_app(testing=False)
