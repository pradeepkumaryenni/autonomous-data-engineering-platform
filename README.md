# Autonomous Data Engineering Platform

[![CI](https://github.com/pradeepkumaryenni/autonomous-data-engineering-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/pradeepkumaryenni/autonomous-data-engineering-platform/actions/workflows/ci.yml)
[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A FastAPI service that runs a LangGraph network of agents over Azure Databricks pipelines. The agents check data quality, form root-cause hypotheses, estimate cost savings, apply a small set of safe fixes automatically, and write a Markdown report. Anything HIGH or CRITICAL is escalated to a human instead of being fixed.

> **Portfolio project, built with AI-assisted development (Claude Code).** It runs fully offline in a demo mode, on bundled synthetic data with a fixture workspace and no LLM ([captured demo output](docs/demo.md)). The same code can connect to a real Databricks workspace, and optionally Azure OpenAI, through `.env`. The live path has not yet been run against a real Databricks workspace or Azure OpenAI (see [Limitations](#limitations--what-id-do-next)).

---

## What it does

```
Data Quality → Root Cause → Cost Optimizer → (Remediation | Escalation) → Reporter
```

1. **Data quality:** reads up to 10,000 rows per table and runs null-rate and duplicate-key checks on any table. Tables with a declared contract also get range, allowed-value and drift checks against a stored baseline.
2. **Root cause:** pulls recent job runs from the Jobs API and produces hypotheses with evidence and a confidence score. With Azure OpenAI configured, an LLM writes them; otherwise (and whenever the LLM call fails) deterministic rules do.
3. **Cost optimizer:** looks at the cluster config (autoscaling, auto-termination) and Delta table layout (`DESCRIBE DETAIL`: file count and size). It produces recommendations with *heuristic* savings estimates.
4. **Routing:** if any issue is HIGH or CRITICAL, the run is **escalated**: nothing is changed and the report asks for human review. Otherwise LOW/MEDIUM quality issues go to remediation.
5. **Remediation:** runs a limited set of SQL fixes (dedup on the key, delete rows with a null key, LOW/MEDIUM-priority `OPTIMIZE`/`VACUUM`).
6. **Reporter:** writes a Markdown report (rule-based, or LLM-written when configured).

Runs are triggered through the API; there is no built-in scheduler.

## Architecture

```
POST /api/v1/pipelines/run  (returns run_id, analysis runs as a background task)
         │
         ▼
┌──────────────────── LangGraph StateGraph ────────────────────┐
│                                                               │
│  data_quality ──▶ root_cause ──▶ cost_optimizer               │
│                                      │                        │
│                     ┌────────────────┼──────────────┐         │
│              any HIGH/CRITICAL?  LOW/MEDIUM only?  no issues  │
│                     │                │              │         │
│                escalation       remediation         │         │
│               (no changes)     (guarded SQL)        │         │
│                     └────────────────┼──────────────┘         │
│                                      ▼                        │
│                                  reporter                     │
└───────────────────────────────────────────────────────────────┘
         │
         ▼
GET /api/v1/pipelines/{run_id}/status  →  { status: COMPLETE | ESCALATED | FAILED, report: "..." }
```

The Databricks client is the only I/O boundary. In demo mode `DemoDatabricksClient` replaces it, with the same methods, so the agents run unchanged. More detail is in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

---

## Quick start

### 1. Run the offline demo (no Databricks, no API keys)

Requires Python 3.11.

```bash
git clone https://github.com/pradeepkumaryenni/autonomous-data-engineering-platform.git
cd autonomous-data-engineering-platform
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt

DEMO_MODE=true uvicorn src.api.main:app --port 8000  # PowerShell: $env:DEMO_MODE="true"; uvicorn src.api.main:app --port 8000
```

On first start, the demo generates `data/samples/transactions.csv`: 10,000 synthetic rows with injected nulls, duplicates and a drift in `amount`. Generation is deterministic (seed 42). In a second terminal:

```bash
curl -X POST http://localhost:8000/api/v1/pipelines/run \
  -H "Content-Type: application/json" \
  -d '{"pipeline_name": "bronze_transactions",
       "table_names": ["demo.bronze.transactions"],
       "warehouse_id": "demo-warehouse",
       "cluster_id": "0101-demo-etl01",
       "job_id": 1001}'
# → {"run_id": "...", "status": "PENDING", ...}

curl http://localhost:8000/api/v1/pipelines/<run_id>/status
# → {"status": "ESCALATED", "report": "# Pipeline Analysis Report: bronze_transactions ...", ...}
```

The run ends `ESCALATED` because the injected drift is a HIGH-severity issue. [docs/demo.md](docs/demo.md) has the full captured report. Swagger UI is at http://localhost:8000/docs.

### 2. Run with Docker Compose (API + Prometheus + Grafana)

```bash
cp .env.example .env      # DEMO_MODE=true by default
# set GRAFANA_ADMIN_PASSWORD in .env (compose refuses to start without it)
docker compose up --build -d
```

- **API:** http://localhost:8000/docs
- **Prometheus:** http://localhost:9090 (scrapes `/api/v1/health/metrics`)
- **Grafana:** http://localhost:3000, user `admin` and the password from your `.env`. No dashboards are bundled yet; add Prometheus (`http://prometheus:9090`) as a data source.

### 3. Connect to a real workspace

In `.env`, set `DEMO_MODE=false` and fill in `DATABRICKS_HOST`, `DATABRICKS_TOKEN` and optionally `DATABRICKS_CLUSTER_ID`. Without `AZURE_OPENAI_API_KEY`, the agents use the rule-based path. With it, root-cause analysis and the report are written by your Azure OpenAI deployment. Pass real table names (`catalog.schema.table`), a SQL warehouse id, and optionally a cluster id and job id to `POST /api/v1/pipelines/run`.

> Remediation runs real SQL (`DELETE`, `CREATE OR REPLACE TABLE`, `OPTIMIZE`, `VACUUM`) on LOW/MEDIUM issues. Point it at a dev workspace, using a token that can only touch the tables you are testing.

---

## Tests and checks

```bash
pip install -r requirements.txt pytest==8.3.3 pytest-asyncio==0.24.0 pytest-cov==5.0.0
pytest tests/ -v --cov=src
```

The suite includes full LangGraph runs in demo mode, covering the escalation branch, the auto-remediation branch and the clean-table branch, plus an end-to-end API run (`tests/test_agents/test_demo_graph.py`). CI runs `ruff check`, `ruff format --check`, `mypy`, the tests, and a Docker build.

---

## Design decisions and trade-offs

- **Deterministic first, LLM optional.** Root-cause analysis and the report have rule-based implementations that always work. The LLM is an add-on for narrative and broader reasoning, and any failure falls back to the rules. This makes the system testable and reproducible, and keeps LLM output in the role of a *hypothesis* rather than a decision.
- **Conservative remediation.** Only LOW/MEDIUM quality issues and LOW/MEDIUM-priority storage maintenance are applied automatically. If *any* issue in a run is HIGH or CRITICAL, nothing is changed and the run is escalated. The trade-off: one bad table blocks safe fixes on others in the same run, which I accept for a system that writes to data.
- **Generic checks plus declared contracts.** Null and duplicate checks need no configuration. Drift needs a baseline, so it only runs for tables with a declared contract (`data/samples/schema.json` in the demo, baseline computed from the pre-drift data). An earlier version compared each sample with a baseline computed *from the same sample*, so it could never fail; it was removed.
- **A simple graph.** The agents form a linear DAG with one conditional edge: no loops, no agent-to-agent negotiation. The state is a `TypedDict`, and the request (`run_context`) is kept separate from detected issues, so a run with clean data reports "no issues" instead of a placeholder issue.
- **Fake at the I/O boundary, not inside the agents.** The demo replaces only the Databricks client, so the demo exercises the same agent, routing and report code as live mode.
- **Cost numbers are explicitly heuristic.** The savings assumptions are named constants in `cost_optimizer_agent.py` (for example, autoscaling saves 35% of worker-hours) and the report labels them as such. `OPTIMIZE` is credited with no storage saving, because compaction speeds up reads but old files remain until `VACUUM`.

## Limitations / what I'd do next

- **Live mode is unverified against a real workspace.** The Databricks client follows the REST API docs (Statement Execution API, `DESCRIBE DETAIL`, Jobs runs/list) but has only been exercised through tests and the demo. Known gaps: only the first result chunk of a SQL statement is read, and there is no pagination for job runs.
- **Checks run in pandas on a `LIMIT 10000` sample**, not the full table. Next: push the checks down into SQL or Spark, or use Databricks Lakehouse Monitoring or DLT expectations.
- **Drift detection is crude**: it compares the sample mean with a baseline mean in units of the baseline std. Next: per-partition or per-load-date comparisons, PSI or KS tests.
- **Severity is one value per table**, so routing is coarse.
- **The remediation SQL is blunt.** Null handling deletes rows (a quarantine table would be better), and dedup keeps an arbitrary row per key.
- **No authentication on the API**, and CORS is open. Table names are validated against a strict pattern before being put into SQL. Next: Entra ID auth, a least-privilege service principal, and an approval step that resumes an escalated run (LangGraph interrupts/checkpointing).
- **Runs are stored in memory** (lost on restart, single worker), and `DELETE /pipelines/{id}` only marks a run cancelled. Next: persist runs to a Delta or Postgres table.
- **Observability is minimal**: a few Prometheus counters and no bundled Grafana dashboards.

---

## Project structure

```
autonomous-data-engineering-platform/
├── src/
│   ├── agents/
│   │   ├── state.py                  # AgentState + RunContext TypedDicts
│   │   ├── orchestrator.py           # StateGraph, routing, escalation node
│   │   ├── data_quality_agent.py     # Generic + contract-based checks, severity
│   │   ├── root_cause_agent.py       # Job history + LLM or rule-based hypotheses
│   │   ├── cost_optimizer_agent.py   # Cluster + Delta layout recommendations
│   │   ├── remediation_agent.py      # Guarded SQL fixes
│   │   └── reporter_agent.py         # Markdown report (rule-based or LLM)
│   ├── monitoring/
│   │   ├── quality_rules.py          # DataQualityEngine (6 check types)
│   │   ├── anomaly_detector.py       # IQR + Z-score detectors (not yet wired into the graph)
│   │   └── metrics_collector.py      # Prometheus metrics
│   ├── databricks/
│   │   ├── client.py                 # Async Databricks REST client (httpx + tenacity)
│   │   ├── demo_client.py            # Offline stand-in used when DEMO_MODE=true
│   │   └── delta_utils.py            # DESCRIBE DETAIL parsing, OPTIMIZE/VACUUM helpers
│   ├── api/
│   │   ├── main.py                   # FastAPI app factory + lifespan wiring
│   │   ├── routers/                  # pipelines, quality, costs, health
│   │   └── models/schemas.py         # Pydantic v2 request/response models
│   └── config/
│       ├── settings.py               # Pydantic settings (.env)
│       └── logging.py                # structlog JSON logging
├── tests/
│   ├── conftest.py                   # Shared fixtures
│   ├── test_agents/                  # Full-graph demo runs + API end-to-end
│   ├── test_api/                     # Endpoint tests
│   └── test_monitoring/              # DataQualityEngine + AnomalyDetector
├── data/samples/
│   ├── generate_sample_data.py       # Synthetic transactions + schema.json
│   ├── schema.json                   # Declared schema + quality rules (generated)
│   └── demo_fixtures.json            # Cluster, table detail, job-run history
├── monitoring/prometheus.yml         # Prometheus scrape config
├── docs/
│   ├── ARCHITECTURE.md
│   └── demo.md                       # Captured offline demo output
├── .github/workflows/
│   ├── ci.yml                        # ruff, mypy, pytest, docker build
│   └── docker-publish.yml            # Push image to GHCR on main / tags
├── Dockerfile                        # Multi-stage build, non-root user
├── docker-compose.yml                # API + Prometheus + Grafana
├── .env.example
├── pyproject.toml
├── requirements.txt
└── LICENSE
```

## API reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/v1/pipelines/run` | Start an analysis (runs in the background) |
| `GET` | `/api/v1/pipelines/{run_id}/status` | Run status, results and report |
| `GET` | `/api/v1/pipelines/` | List runs |
| `DELETE` | `/api/v1/pipelines/{run_id}` | Mark a pending/running run as cancelled |
| `POST` | `/api/v1/quality/check` | Run quality checks on one table |
| `GET` | `/api/v1/quality/rules` | Supported check types |
| `POST` | `/api/v1/quality/profile` | Column profile of a table |
| `POST` | `/api/v1/costs/analyze` | Cluster + storage recommendations |
| `GET` | `/api/v1/costs/clusters/{cluster_id}` | Cluster cost estimate |
| `POST` | `/api/v1/costs/optimize` | Apply OPTIMIZE/VACUUM from the last analysis (`confirm=true`) |
| `GET` | `/api/v1/health/health` | Health (and Databricks connectivity in live mode) |
| `GET` | `/api/v1/health/metrics` | Prometheus metrics |

## Tech stack

| Layer | Technology |
|-------|------------|
| Agent orchestration | LangGraph 0.2 |
| LLM (optional) | Azure OpenAI via `langchain-openai` |
| Data platform | Azure Databricks REST APIs, Delta Lake |
| API | FastAPI + Uvicorn, Pydantic v2 |
| Data checks | pandas, NumPy |
| HTTP client | httpx (async) + tenacity retries |
| Observability | Prometheus client, structlog, Grafana (compose) |
| Packaging / CI | Docker multi-stage, Docker Compose, GitHub Actions |

---

## License

MIT, see [LICENSE](LICENSE).

## Author

**Pradeep Kumar Yenni**, Data Engineer · [LinkedIn](https://www.linkedin.com/in/yenni-pradeep-kumar/)
