from __future__ import annotations

import json
from datetime import UTC, datetime
from statistics import median
from typing import Any

import structlog
from langchain_core.messages import HumanMessage

from src.agents.state import AgentState, PipelineIssue
from src.monitoring.anomaly_detector import AnomalyDetector

logger = structlog.get_logger(__name__)


def _run_date(run: dict) -> str:
    start = run.get("start_time", 0)
    if not start:
        return "unknown date"
    return datetime.fromtimestamp(start / 1000, tz=UTC).strftime("%Y-%m-%d")


def find_failed_then_retried(run_history: list[dict]) -> list[str]:
    """Evidence lines for runs that failed and were followed by a retry."""
    ordered = sorted(run_history, key=lambda r: r.get("start_time", 0))
    evidence = []
    for i, run in enumerate(ordered):
        if run.get("result_state") != "FAILED":
            continue
        retry = next((r for r in ordered[i + 1 :] if r.get("trigger") == "RETRY"), None)
        msg = (run.get("error_message") or "no error message")[:200]
        line = f"Run {run.get('run_id')} on {_run_date(run)} FAILED: {msg}"
        if retry:
            line += f"; retried by run {retry.get('run_id')} ({retry.get('result_state')})"
        evidence.append(line)
    return evidence


def summarize_history(run_history: list[dict]) -> str | None:
    if not run_history:
        return None
    durations = [r["duration_seconds"] / 60 for r in run_history if r["duration_seconds"] > 0]
    failed = sum(1 for r in run_history if r.get("result_state") == "FAILED")
    text = f"Job history: {len(run_history)} recent runs, {failed} failed"
    if durations:
        text += f", median duration {median(durations):.1f} min"
    return text


def rule_based_root_causes(issues: list[PipelineIssue], run_history: list[dict]) -> list[dict]:
    """Deterministic root-cause hypotheses from failed checks and job history.

    Used when no LLM is configured (including DEMO_MODE) or the LLM call fails.
    Each hypothesis carries its evidence so a human can verify it quickly.
    """
    retry_evidence = find_failed_then_retried(run_history)
    history_line = summarize_history(run_history)
    causes: list[dict] = []

    for issue in issues:
        if issue.get("issue_type") in ("access", "volume"):
            reason = issue.get("metadata", {}).get("reason", "")
            causes.append(
                {
                    "issue_type": issue.get("issue_type"),
                    "pipeline_name": issue.get("pipeline_name", ""),
                    "check_type": issue.get("issue_type"),
                    "column": issue.get("pipeline_name", ""),
                    "root_cause": (
                        "The table could not be read, so no quality checks ran."
                        if issue.get("issue_type") == "access"
                        else "The table returned no rows, so no quality checks ran."
                    ),
                    "confidence": 0.9,
                    "evidence": [reason],
                    "remediation_hint": (
                        "Check the table name, the SQL warehouse and the token's permissions."
                        if issue.get("issue_type") == "access"
                        else "Check whether the upstream load ran and wrote any data."
                    ),
                    "estimated_impact": issue.get("severity", "MEDIUM"),
                    "method": "rule-based",
                }
            )
            continue
        failed = issue.get("metadata", {}).get("failed_checks", [])
        by_type: dict[str, list[dict]] = {}
        for check in failed:
            by_type.setdefault(check.get("check_type", ""), []).append(check)
        range_by_col = {c.get("column"): c for c in by_type.get("range", [])}

        def add(check_type, column, cause, confidence, evidence, hint, _issue=issue):
            if history_line:
                evidence = evidence + [history_line]
            causes.append(
                {
                    "issue_type": _issue.get("issue_type", "quality"),
                    "pipeline_name": _issue.get("pipeline_name", ""),
                    "check_type": check_type,
                    "column": column,
                    "root_cause": cause,
                    "confidence": confidence,
                    "evidence": evidence,
                    "remediation_hint": hint,
                    "estimated_impact": _issue.get("severity", "MEDIUM"),
                    "method": "rule-based",
                }
            )

        for check in by_type.pop("drift", []):
            col, d = check.get("column"), check.get("details", {})
            cur, base = d.get("current_mean", 0.0), d.get("baseline_mean", 0.0)
            ratio = cur / base if base else float("inf")
            evidence = [check.get("message", "")]
            rng = range_by_col.pop(col, None)
            if rng:
                evidence.append(rng.get("message", ""))
            add(
                "drift",
                col,
                f"The scale of '{col}' shifted: current mean {cur:,.2f} vs baseline "
                f"{base:,.2f} ({ratio:.1f}x). A sudden multiplicative shift usually means "
                "an upstream unit or currency change, or a new source feeding the table.",
                0.6 if rng else 0.5,
                evidence,
                "Not auto-fixable. Find the first load where the shift starts (e.g. average "
                f"'{col}' by load date), confirm the unit with the source owner, then correct "
                "and backfill the affected partitions.",
            )

        for check in by_type.pop("duplicates", []):
            col = check.get("column")
            evidence = [check.get("message", "")] + retry_evidence
            if retry_evidence:
                cause = (
                    "Likely a non-idempotent load: a run failed after writing part of its "
                    "output and the retry appended the same records again."
                )
                confidence = 0.6
            else:
                cause = (
                    "Duplicate keys are arriving from upstream, or the load appends "
                    "without a MERGE on the key."
                )
                confidence = 0.4
            add(
                "duplicates",
                col,
                cause,
                confidence,
                evidence,
                "Make the load idempotent (MERGE INTO on the key, or overwrite by "
                "partition), then dedup the existing rows once.",
            )

        for check in by_type.pop("nulls", []):
            col, d = check.get("column"), check.get("details", {})
            add(
                "nulls",
                col,
                f"Upstream is sending incomplete records: {check.get('metric_value', 0):.1%} "
                f"of '{col}' is null (allowed {check.get('threshold') or 0:.1%}).",
                0.4,
                [check.get("message", ""), f"{d.get('null_count', '?')} null rows"],
                f"Check the source extract/mapping for '{col}'. Quarantine failing rows "
                "instead of dropping them if downstream consumers need them.",
            )

        for check in by_type.pop("schema", []):
            d = check.get("details", {})
            add(
                "schema",
                "*",
                "The table no longer matches its declared schema "
                f"(mismatched: {sorted(d.get('mismatched_columns', {}))}, "
                f"missing: {d.get('missing_columns', [])}). Typical cause: an upstream "
                "schema change.",
                0.6,
                [check.get("message", "")],
                "Compare with the source schema; update the contract or add explicit casts "
                "before the bronze write.",
            )

        for check in by_type.pop("referential", []):
            col, d = check.get("column"), check.get("details", {})
            add(
                "referential",
                col,
                f"'{col}' contains values outside the allowed set "
                f"(e.g. {d.get('sample_invalid_values', [])[:3]}). Typical cause: a new "
                "category introduced upstream.",
                0.5,
                [check.get("message", "")],
                "Confirm whether the new values are valid; extend the allowed list or "
                "map them in the transformation.",
            )

        # Anything not explained above: range failures without drift, unknown check types
        by_type.pop("range", None)
        leftovers = list(range_by_col.values())
        for checks in by_type.values():
            leftovers.extend(checks)
        for check in leftovers:
            add(
                check.get("check_type", "unknown"),
                check.get("column"),
                f"Check '{check.get('check_type')}' on '{check.get('column')}' failed.",
                0.3,
                [check.get("message", "")],
                "Inspect the failing rows manually.",
            )

    return causes


class RootCauseAgent:
    """LangGraph agent node that performs root cause analysis using LLM and run history."""

    def __init__(
        self,
        llm,
        db_client: Any,
        anomaly_detector: AnomalyDetector,
    ) -> None:
        self.llm = llm
        self.db_client = db_client
        self.anomaly_detector = anomaly_detector

    async def analyze_job_run_history(self, job_id: int, lookback_runs: int = 20) -> list[dict]:
        """Fetch recent job runs from Databricks and extract key metrics."""
        logger.info("Fetching job run history", job_id=job_id, lookback=lookback_runs)
        try:
            runs_data = await self.db_client.list_job_runs(job_id, limit=lookback_runs)
        except Exception as exc:
            logger.warning("Could not fetch runs list", error=str(exc))
            return []

        run_summaries = []
        for run in runs_data:
            start_time = run.get("start_time", 0)
            end_time = run.get("end_time", 0)
            duration_ms = (end_time - start_time) if end_time > start_time else 0
            state = run.get("state", {})
            run_summaries.append(
                {
                    "run_id": run.get("run_id"),
                    "state": state.get("life_cycle_state", "UNKNOWN"),
                    "result_state": state.get("result_state", ""),
                    "duration_ms": duration_ms,
                    "duration_seconds": duration_ms / 1000.0,
                    "start_time": start_time,
                    "error_message": state.get("state_message", ""),
                    "trigger": run.get("trigger", ""),
                }
            )

        return run_summaries

    async def identify_root_cause(
        self,
        issues: list[PipelineIssue],
        run_history: list[dict],
    ) -> list[dict]:
        """Identify root causes with the LLM, or with deterministic rules if no LLM."""
        if not issues:
            return []
        if self.llm is None:
            return rule_based_root_causes(issues, run_history)

        durations = [r["duration_seconds"] for r in run_history if r["duration_seconds"] > 0]
        errors = [r["error_message"] for r in run_history if r.get("error_message")]
        failed_runs = [r for r in run_history if r.get("result_state") == "FAILED"]

        history_summary = {
            "total_runs_analyzed": len(run_history),
            "failed_runs": len(failed_runs),
            "avg_duration_seconds": sum(durations) / len(durations) if durations else 0,
            "max_duration_seconds": max(durations) if durations else 0,
            "recent_errors": errors[:5],
            "failure_rate": len(failed_runs) / len(run_history) if run_history else 0,
            "retried_failures": find_failed_then_retried(run_history),
        }

        issues_json = json.dumps(issues, indent=2, default=str)
        history_json = json.dumps(history_summary, indent=2)

        prompt = f"""You are a senior data engineering expert performing root cause analysis.

Analyze the following pipeline issues and job run history to identify root causes.

## Pipeline Issues
{issues_json}

## Job Run History Summary
{history_json}

For each issue, provide a root cause analysis in the following JSON format:
{{
  "root_cause_analyses": [
    {{
      "issue_type": "<issue type from above>",
      "pipeline_name": "<pipeline name>",
      "root_cause": "<specific technical root cause>",
      "confidence": <0.0-1.0>,
      "evidence": ["<evidence point 1>", "<evidence point 2>"],
      "remediation_hint": "<specific actionable fix>",
      "estimated_impact": "LOW|MEDIUM|HIGH|CRITICAL"
    }}
  ]
}}

Consider common causes: upstream data changes, schema drift, resource contention,
cluster configuration issues, upstream pipeline failures, data volume spikes,
network timeouts, and code deployment changes.

Respond ONLY with valid JSON, no additional text."""

        try:
            response = await self.llm.ainvoke([HumanMessage(content=prompt)])
            content = response.content.strip()
            # Strip markdown code blocks if present
            if content.startswith("```"):
                content = content.split("```")[1]
                if content.startswith("json"):
                    content = content[4:]
                content = content.strip()
            parsed = json.loads(content)
            analyses = parsed.get("root_cause_analyses", [])
            return [{**a, "method": "llm"} for a in analyses]
        except Exception as exc:
            logger.error("LLM root cause analysis failed, using rules", error=str(exc))
            return rule_based_root_causes(issues, run_history)

    async def __call__(self, state: AgentState) -> AgentState:
        """LangGraph node — performs root cause analysis and updates state."""
        logger.info("RootCauseAgent invoked", iteration=state.get("iteration_count", 0))

        issues = state.get("pipeline_issues", [])
        if not issues:
            logger.info("No pipeline issues to analyze for root cause")
            new_state = dict(state)
            new_state["root_causes"] = []
            new_state["iteration_count"] = state.get("iteration_count", 0) + 1
            return new_state  # type: ignore[return-value]

        job_id = state.get("run_context", {}).get("job_id")
        run_history: list[dict] = []
        if job_id:
            run_history = await self.analyze_job_run_history(int(job_id))

        root_causes = await self.identify_root_cause(issues, run_history)

        new_state = dict(state)
        new_state["root_causes"] = root_causes
        new_state["iteration_count"] = state.get("iteration_count", 0) + 1
        return new_state  # type: ignore[return-value]
