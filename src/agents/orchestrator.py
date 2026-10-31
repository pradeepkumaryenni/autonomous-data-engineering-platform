from __future__ import annotations

from typing import Any

import structlog
from langgraph.graph import END, StateGraph

from src.agents.state import AgentState

logger = structlog.get_logger(__name__)

# Issues at these severities are never auto-remediated; the run is escalated instead.
ESCALATION_SEVERITIES = ("HIGH", "CRITICAL")


def should_escalate(state: AgentState) -> str:
    """Conditional routing after cost analysis.

    - any HIGH/CRITICAL issue  -> "escalate" (no automatic changes at all)
    - else LOW/MEDIUM quality or cost issues -> "remediate"
    - else -> "skip" straight to the reporter
    """
    issues = state.get("pipeline_issues", [])
    blocking = [i for i in issues if i.get("severity") in ESCALATION_SEVERITIES]
    if blocking:
        logger.info("HIGH/CRITICAL issues found, escalating", count=len(blocking))
        return "escalate"

    remediable = [
        i
        for i in issues
        if i.get("severity") in ("LOW", "MEDIUM") and i.get("issue_type") in ("quality", "cost")
    ]
    if remediable:
        return "remediate"

    return "skip"


async def escalation_node(state: AgentState) -> AgentState:
    """Mark the run for human review. Nothing is changed in the data."""
    blocking = [
        i for i in state.get("pipeline_issues", []) if i.get("severity") in ESCALATION_SEVERITIES
    ]
    logger.critical(
        "ESCALATING: HIGH/CRITICAL pipeline issues require human review",
        issue_count=len(blocking),
        issues=[i.get("description", "") for i in blocking],
    )
    new_state = dict(state)
    new_state["should_escalate"] = True
    new_state["messages"] = list(state.get("messages", [])) + [
        {
            "role": "system",
            "content": (
                f"ESCALATION REQUIRED: {len(blocking)} HIGH/CRITICAL issue(s) detected. "
                "Auto-remediation was skipped; a human must review before any fix. "
                f"Issues: {[i.get('description', '')[:100] for i in blocking]}"
            ),
        }
    ]
    return new_state  # type: ignore[return-value]


def build_graph(
    quality_agent: Any,
    rca_agent: Any,
    cost_agent: Any,
    remediation_agent: Any,
    reporter_agent: Any,
) -> Any:
    """Build and compile the LangGraph StateGraph for pipeline analysis."""
    graph = StateGraph(AgentState)

    graph.add_node("data_quality", quality_agent)
    graph.add_node("root_cause", rca_agent)
    graph.add_node("cost_optimizer", cost_agent)
    graph.add_node("remediation", remediation_agent)
    graph.add_node("reporter", reporter_agent)
    graph.add_node("escalation", escalation_node)

    graph.set_entry_point("data_quality")
    graph.add_edge("data_quality", "root_cause")
    graph.add_edge("root_cause", "cost_optimizer")
    graph.add_conditional_edges(
        "cost_optimizer",
        should_escalate,
        {
            "escalate": "escalation",
            "remediate": "remediation",
            "skip": "reporter",
        },
    )
    graph.add_edge("remediation", "reporter")
    graph.add_edge("escalation", "reporter")
    graph.add_edge("reporter", END)

    return graph.compile()


async def run_pipeline_analysis(
    pipeline_name: str,
    table_names: list[str],
    warehouse_id: str,
    graph: Any,
    cluster_id: str = "",
    job_id: int | None = None,
    demo_mode: bool = False,
) -> AgentState:
    """Initialize state and run the full agent graph for a pipeline."""
    initial_state: AgentState = {
        "messages": [],
        "run_context": {
            "pipeline_name": pipeline_name,
            "tables": list(table_names),
            "warehouse_id": warehouse_id,
            "cluster_id": cluster_id,
            "job_id": job_id,
            "demo_mode": demo_mode,
        },
        "pipeline_issues": [],
        "quality_results": [],
        "anomalies": [],
        "root_causes": [],
        "remediation_actions": [],
        "cost_recommendations": [],
        "final_report": "",
        "iteration_count": 0,
        "should_escalate": False,
    }

    logger.info(
        "Starting pipeline analysis",
        pipeline=pipeline_name,
        tables=table_names,
        warehouse_id=warehouse_id,
        demo_mode=demo_mode,
    )

    result = await graph.ainvoke(initial_state)
    return result
