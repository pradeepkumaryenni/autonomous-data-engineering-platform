from __future__ import annotations

import json
from datetime import UTC, datetime

import structlog
from langchain_core.messages import HumanMessage

from src.agents.orchestrator import ESCALATION_SEVERITIES
from src.agents.state import AgentState

logger = structlog.get_logger(__name__)

SEVERITY_ORDER = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}


def _cell(value: object, limit: int = 160) -> str:
    """Make a value safe for a Markdown table cell."""
    text = " ".join(str(value).split()).replace("|", "\\|")
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _sev(severity: str) -> str:
    return f"**{severity}**" if severity in ESCALATION_SEVERITIES else severity


def render_report(state: AgentState, note: str = "") -> str:
    """Deterministic Markdown report built only from the graph state."""
    ctx = state.get("run_context", {})
    issues = sorted(
        state.get("pipeline_issues", []),
        key=lambda i: SEVERITY_ORDER.get(i.get("severity", "LOW"), 3),
    )
    results = state.get("quality_results", [])
    failed = [r for r in results if not r.get("passed", True)]
    root_causes = state.get("root_causes", [])
    actions = state.get("remediation_actions", [])
    recs = state.get("cost_recommendations", [])
    escalated = state.get("should_escalate", False)
    savings = sum(r.get("estimated_monthly_savings_usd", 0.0) for r in recs)
    top = issues[0]["severity"] if issues else "none"
    mode = "offline demo (sample data, fixture workspace)" if ctx.get("demo_mode") else "live"
    now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")

    if escalated:
        outcome = "Escalated for human review. No automatic changes were made."
    elif actions:
        ok = sum(1 for a in actions if a.get("success"))
        outcome = f"Auto-remediation ran: {ok}/{len(actions)} actions succeeded."
    elif issues:
        outcome = "Issues found, but none were eligible for auto-remediation."
    else:
        outcome = "No issues found."

    lines = [
        f"# Pipeline Analysis Report: {ctx.get('pipeline_name', 'unknown')}",
        "",
        f"_Generated {now} · mode: {mode} · report: rule-based (no LLM)_",
    ]
    if note:
        lines += ["", f"> {note}"]
    lines += [
        "",
        "## Summary",
        "",
        f"- Tables checked: {len(ctx.get('tables', []))} "
        f"(`{'`, `'.join(ctx.get('tables', [])) or '-'}`)",
        f"- Quality checks: {len(results)} run, {len(failed)} failed",
        f"- Highest issue severity: {_sev(top)}",
        f"- Outcome: {outcome}",
        f"- Cost recommendations: {len(recs)} "
        f"(heuristic estimate ${savings:,.2f}/month, not measured)",
        "",
        "## Issues detected",
        "",
    ]
    if issues:
        lines += ["| Table | Type | Severity | Detail |", "|---|---|---|---|"]
        lines += [
            f"| {_cell(i.get('pipeline_name', ''))} | {i.get('issue_type', '')} | "
            f"{_sev(i.get('severity', ''))} | {_cell(i.get('description', ''), 200)} |"
            for i in issues
        ]
    else:
        lines.append("No issues detected.")

    lines += ["", "## Quality checks", ""]
    if results:
        lines += ["| Table | Check | Column | Result | Detail |", "|---|---|---|---|---|"]
        for r in sorted(results, key=lambda r: r.get("passed", True)):
            lines.append(
                f"| {_cell(r.get('table_name', ''))} | {r.get('check_type', '')} | "
                f"{_cell(r.get('column', ''))} | {'pass' if r.get('passed') else '**FAIL**'} | "
                f"{_cell(r.get('message', ''))} |"
            )
    else:
        lines.append("No quality checks ran (no tables, or the table could not be read).")

    lines += ["", "## Root cause hypotheses", ""]
    if root_causes:
        for n, rc in enumerate(root_causes, 1):
            conf = float(rc.get("confidence", 0) or 0)
            lines += [
                f"### {n}. {rc.get('check_type', rc.get('issue_type', ''))} on "
                f"`{rc.get('column', rc.get('pipeline_name', ''))}` "
                f"(confidence {conf:.0%}, {rc.get('method', 'llm')})",
                "",
                str(rc.get("root_cause", "")),
                "",
                "Evidence:",
            ]
            lines += [f"- {e}" for e in rc.get("evidence", []) if e]
            lines += ["", f"Suggested next step: {rc.get('remediation_hint', '')}", ""]
    else:
        lines += ["No root cause analysis needed.", ""]

    lines += ["## Escalation and remediation", ""]
    if escalated:
        blocking = [i for i in issues if i.get("severity") in ESCALATION_SEVERITIES]
        lines.append(
            f"{len(blocking)} issue(s) at HIGH/CRITICAL severity. Policy: these are never "
            "auto-remediated, so the remediation step was skipped for this run and the "
            "issues are handed to a human."
        )
    elif actions:
        lines += [
            "| Type | Action | Target | Status | Rows affected | SQL |",
            "|---|---|---|---|---|---|",
        ]
        for a in actions:
            status = "ok" if a.get("success") else f"failed: {a.get('error_message', '')}"
            target = a.get("pipeline") or a.get("table", "")
            lines.append(
                f"| {a.get('type', '')} | {_cell(a.get('action_taken', ''))} | {_cell(target)} | "
                f"{_cell(status)} | {a.get('rows_affected', 0)} | "
                f"`{_cell(a.get('sql_executed', '') or '-', 200)}` |"
            )
        if ctx.get("demo_mode"):
            lines += [
                "",
                "_Demo mode: SQL ran against an in-memory copy of the sample table. "
                "OPTIMIZE/VACUUM are no-ops there._",
            ]
    else:
        lines.append("No remediation was needed.")

    lines += ["", "## Cost recommendations", ""]
    if recs:
        lines += [
            "| Priority | Category | Target | Recommendation | Est. USD/month |",
            "|---|---|---|---|---|",
        ]
        for r in recs:
            target = r.get("table_name") or r.get("cluster_id", "")
            lines.append(
                f"| {r.get('priority', '')} | {r.get('category', '')} | {_cell(target)} | "
                f"{_cell(r.get('action', ''))}: {_cell(r.get('issue', ''), 200)} | "
                f"{r.get('estimated_monthly_savings_usd', 0.0):,.2f} |"
            )
        lines += [
            "",
            "_Savings are rough estimates from fixed assumptions in "
            "`cost_optimizer_agent.py`, useful for ranking only._",
        ]
    else:
        lines.append("No cost recommendations (no cluster or tables to analyze).")

    lines += ["", "## Next steps", ""]
    steps = []
    if escalated:
        steps.append("Review the HIGH/CRITICAL issues above before any data is changed.")
    for rc in root_causes:
        steps.append(
            f"`{rc.get('column', '')}` ({rc.get('check_type', '')}): "
            f"{rc.get('remediation_hint', '')}"
        )
    if any(r.get("priority") == "HIGH" for r in recs):
        steps.append(
            "Review the HIGH-priority cost actions; compute changes and HIGH-priority "
            "storage actions are never auto-applied."
        )
    lines += [f"{n}. {s}" for n, s in enumerate(steps, 1)] or ["Nothing to do."]
    return "\n".join(lines) + "\n"


class ReporterAgent:
    """LangGraph agent node that writes the final Markdown report.

    With an LLM configured it asks the model for a narrative report; otherwise (and
    whenever the LLM call fails) it renders a deterministic report from the state.
    """

    def __init__(self, llm) -> None:
        self.llm = llm

    async def generate_report(self, state: AgentState) -> str:
        if self.llm is None:
            return render_report(state)

        context = {
            "generated_at": datetime.now(UTC).isoformat(),
            "run_context": state.get("run_context", {}),
            "pipeline_issues": state.get("pipeline_issues", []),
            "quality_results_summary": {
                "total": len(state.get("quality_results", [])),
                "failed": sum(
                    1 for r in state.get("quality_results", []) if not r.get("passed", True)
                ),
            },
            "root_causes": state.get("root_causes", []),
            "remediation_actions": state.get("remediation_actions", []),
            "cost_recommendations": state.get("cost_recommendations", []),
            "should_escalate": state.get("should_escalate", False),
        }
        context_json = json.dumps(context, indent=2, default=str)

        prompt = f"""You are a data engineering platform reporting agent.

Write a Markdown report from the pipeline analysis results below. Use only facts
present in the JSON; do not invent numbers.

## Analysis Context
```json
{context_json}
```

Sections: Summary, Issues Detected (table), Root Cause Analysis (cause, confidence,
evidence, fix), Escalation / Remediations Applied, Cost Recommendations (table, and
state that savings are heuristic estimates), Next Steps.

Use Markdown tables and bold HIGH/CRITICAL severities. Return ONLY the Markdown report."""

        try:
            response = await self.llm.ainvoke([HumanMessage(content=prompt)])
            return response.content.strip()
        except Exception as exc:
            logger.error("Failed to generate LLM report, using rule-based report", error=str(exc))
            return render_report(state, note=f"LLM call failed ({type(exc).__name__}).")

    async def __call__(self, state: AgentState) -> AgentState:
        """LangGraph node — generates final report and updates state."""
        logger.info("ReporterAgent invoked", iteration=state.get("iteration_count", 0))
        report = await self.generate_report(state)
        new_state = dict(state)
        new_state["final_report"] = report
        new_state["iteration_count"] = state.get("iteration_count", 0) + 1
        return new_state  # type: ignore[return-value]
