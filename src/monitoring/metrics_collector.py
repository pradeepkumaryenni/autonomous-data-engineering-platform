from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest


class MetricsCollector:
    """Prometheus metrics collector for the ADEP platform.

    Uses its own registry so that creating a second collector (e.g. one app per
    test) does not fail with "Duplicated timeseries in CollectorRegistry".
    """

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()
        reg = self.registry
        # Pipeline run metrics
        self.pipeline_runs_total = Counter(
            "adep_pipeline_runs_total",
            "Total number of pipeline runs",
            ["pipeline_name", "status"],
            registry=reg,
        )
        self.pipeline_duration_seconds = Histogram(
            "adep_pipeline_duration_seconds",
            "Pipeline run duration in seconds",
            ["pipeline_name"],
            buckets=[1, 5, 10, 30, 60, 120, 300, 600, 1800, 3600],
            registry=reg,
        )

        # Quality check metrics
        self.quality_checks_total = Counter(
            "adep_quality_checks_total",
            "Total number of quality checks run",
            ["check_name", "table_name", "result"],
            registry=reg,
        )

        # Agent action metrics
        self.agent_actions_total = Counter(
            "adep_agent_actions_total",
            "Total number of agent actions",
            ["agent_name", "action", "success"],
            registry=reg,
        )

        # Cost tracking
        self.resource_cost_usd_total = Counter(
            "adep_resource_cost_usd_total",
            "Total cost in USD by resource",
            ["resource_name"],
            registry=reg,
        )
        self.resource_cost_gauge = Gauge(
            "adep_resource_cost_usd_current",
            "Current period cost in USD by resource",
            ["resource_name"],
            registry=reg,
        )

        # Internal tracking for summary
        self._cost_totals: dict[str, float] = {}
        self._run_counts: dict[str, int] = {}

    def record_pipeline_run(self, pipeline_name: str, status: str, duration_seconds: float) -> None:
        """Record a pipeline run completion."""
        self.pipeline_runs_total.labels(pipeline_name=pipeline_name, status=status).inc()
        self.pipeline_duration_seconds.labels(pipeline_name=pipeline_name).observe(duration_seconds)
        key = f"{pipeline_name}:{status}"
        self._run_counts[key] = self._run_counts.get(key, 0) + 1

    def record_quality_check(self, check_name: str, passed: bool, table_name: str) -> None:
        """Record a quality check result."""
        result = "passed" if passed else "failed"
        self.quality_checks_total.labels(
            check_name=check_name, table_name=table_name, result=result
        ).inc()

    def record_agent_action(self, agent_name: str, action: str, success: bool) -> None:
        """Record an agent action."""
        self.agent_actions_total.labels(
            agent_name=agent_name,
            action=action,
            success=str(success).lower(),
        ).inc()

    def record_cost(self, resource_name: str, cost_usd: float) -> None:
        """Record a cost measurement."""
        self.resource_cost_usd_total.labels(resource_name=resource_name).inc(cost_usd)
        self._cost_totals[resource_name] = self._cost_totals.get(resource_name, 0.0) + cost_usd
        self.resource_cost_gauge.labels(resource_name=resource_name).set(
            self._cost_totals[resource_name]
        )

    def get_metrics_summary(self) -> dict:
        """Return current metric values as a summary dict."""
        return {
            "cost_totals_usd": dict(self._cost_totals),
            "pipeline_run_counts": dict(self._run_counts),
            "total_cost_usd": sum(self._cost_totals.values()),
        }

    def get_prometheus_output(self) -> str:
        """Return prometheus text format metrics."""
        return generate_latest(self.registry).decode("utf-8")
