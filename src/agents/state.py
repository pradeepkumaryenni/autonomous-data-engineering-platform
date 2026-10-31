from __future__ import annotations

from typing import Annotated, Any, TypedDict

from langgraph.graph.message import add_messages


class PipelineIssue(TypedDict):
    pipeline_name: str
    issue_type: str  # "quality", "latency", "volume", "schema", "cost"
    severity: str  # "LOW", "MEDIUM", "HIGH", "CRITICAL"
    description: str
    metadata: dict[str, Any]


class RunContext(TypedDict, total=False):
    """What the caller asked to analyze. Kept separate from detected issues."""

    pipeline_name: str
    tables: list[str]
    warehouse_id: str
    cluster_id: str
    job_id: int | None
    demo_mode: bool


class AgentState(TypedDict):
    messages: Annotated[list, add_messages]
    run_context: RunContext
    pipeline_issues: list[PipelineIssue]
    quality_results: list[dict]
    anomalies: list[dict]
    root_causes: list[dict]
    remediation_actions: list[dict]
    cost_recommendations: list[dict]
    final_report: str
    iteration_count: int
    should_escalate: bool
