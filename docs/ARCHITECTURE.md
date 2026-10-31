# Architecture: Autonomous Data Engineering Platform

## System overview

ADEP is a FastAPI service that runs a LangGraph network of agents against Azure
Databricks tables and jobs. One run checks data quality, forms root-cause hypotheses,
estimates cost savings, applies a small set of guarded fixes for LOW/MEDIUM issues,
and writes a Markdown report. HIGH and CRITICAL issues are escalated to a human and
nothing is changed.

It is a portfolio project. `DEMO_MODE=true` runs it offline on synthetic data; see
[demo.md](demo.md) for captured output.

---

## Component diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    EXTERNAL SYSTEMS (live mode)                          │
│  ┌──────────────────────────────┐  ┌──────────────────────────────────┐ │
│  │  Azure Databricks            │  │  Azure OpenAI (optional)         │ │
│  │  SQL warehouse, Jobs API,    │  │  root-cause analysis + report    │ │
│  │  Clusters API, Delta tables  │  │  narrative                       │ │
│  └──────────────┬───────────────┘  └────────────────┬─────────────────┘ │
└─────────────────┼───────────────────────────────────┼───────────────────┘
                  │  DatabricksClient                 │  AzureChatOpenAI
                  │  (or DemoDatabricksClient)        │  (or rule-based code)
                  ▼                                   ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    ADEP CORE (FastAPI + LangGraph)                       │
│                                                                          │
│  data_quality ──▶ root_cause ──▶ cost_optimizer ──▶ route                │
│                                                      │                   │
│                              ┌───────────────────────┼──────────┐        │
│                              ▼                       ▼          │        │
│                        escalation              remediation      │        │
│                     (HIGH/CRITICAL)           (LOW/MEDIUM)      │        │
│                              └───────────┬───────────┘          │        │
│                                          ▼◀─────────────────────┘        │
│                                       reporter                           │
│                                                                          │
│  DataQualityEngine · AnomalyDetector (not yet in the graph) ·            │
│  MetricsCollector (Prometheus)                                           │
│                                                                          │
│  REST API v1: /pipelines  /quality  /costs  /health                     │
└─────────────────────────────────────────────────────────────────────────┘
          │
          ▼
┌──────────────────────┐        ┌───────────────────────────────────┐
│  Prometheus :9090    │───────▶│  Grafana :3000 (no dashboards     │
│  scrapes /metrics    │        │  bundled; add Prometheus source)  │
└──────────────────────┘        └───────────────────────────────────┘
```

---

## Data flow

```
1. TRIGGER
   POST /api/v1/pipelines/run
   { pipeline_name, table_names, warehouse_id, cluster_id?, job_id? }
   → stored as state.run_context; the graph runs as a background task
         │
         ▼
2. DATA QUALITY AGENT
   ├── SELECT * FROM <table> LIMIT 10000 for each table
   ├── Generic rules: null rate per column (5%), duplicates on the key column
   ├── Declared rules for tables with a contract: range, allowed values,
   │   drift against a stored baseline (demo: data/samples/schema.json)
   ├── Unreadable table → HIGH "access" issue; empty table → MEDIUM "volume" issue
   └── Emits one PipelineIssue per failing table, severity from the failed checks
         │
         ▼
3. ROOT CAUSE AGENT
   ├── Last 20 runs of job_id from the Jobs API (runs/list)
   ├── LLM configured → JSON hypotheses from Azure OpenAI
   └── Otherwise / on LLM error → rule-based hypotheses with evidence
       (e.g. duplicates + a failed-then-retried run → non-idempotent load)
         │
         ▼
4. COST OPTIMIZER AGENT
   ├── Cluster config: autoscaling off, auto-termination disabled
   ├── DESCRIBE DETAIL per table: file count/size → OPTIMIZE, plus VACUUM
   └── Savings are heuristic estimates from constants in the module
         │
         ▼
5. ROUTING (conditional edge)
   ├── any HIGH/CRITICAL issue   → escalation → reporter
   ├── LOW/MEDIUM quality issues → remediation → reporter
   └── no issues                 → reporter
         │
         ▼
6. REMEDIATION AGENT (only on the LOW/MEDIUM path)
   ├── Duplicates → CREATE OR REPLACE TABLE ... keep one row per key
   ├── Nulls in a key column → DELETE rows with a null key (other columns: no action)
   └── LOW/MEDIUM-priority storage recommendations → OPTIMIZE / VACUUM
         │
         ▼
7. REPORTER AGENT
   └── Markdown report: rule-based renderer, or LLM-written when configured
         │
         ▼
8. GET /api/v1/pipelines/{run_id}/status
   { status: COMPLETE | ESCALATED | FAILED, report: "# Pipeline Analysis Report..." }
```

---

## Agent descriptions

### 1. DataQualityAgent
- **Inputs:** `run_context.tables`, `run_context.warehouse_id`
- **Outputs:** `quality_results` (one row per check, tagged with the table), `pipeline_issues`
- **Severity:** schema failure or >50% of checks failing → CRITICAL; drift or >30% → HIGH;
  >15% → MEDIUM; otherwise LOW

### 2. RootCauseAgent
- **Inputs:** `pipeline_issues`, job run history for `run_context.job_id`
- **Outputs:** `root_causes` with confidence, evidence, a suggested next step and
  `method` (`llm` or `rule-based`)

### 3. CostOptimizerAgent
- **Inputs:** `run_context.cluster_id`, `run_context.tables`
- **Outputs:** `cost_recommendations` with an action (API payload or SQL) and an estimated
  monthly saving. `OPTIMIZE` is credited with no storage saving: it improves reads,
  and old files remain until `VACUUM`.

### 4. RemediationAgent
- **Safety policy:**
  - Any HIGH or CRITICAL issue in the run → the graph routes to escalation; remediation does not run
  - LOW/MEDIUM quality issues → dedup on the key, delete rows with a null key
  - Storage recommendations with LOW/MEDIUM priority → `OPTIMIZE` / `VACUUM RETAIN 168 HOURS`
  - Compute changes (autoscaling, auto-termination) are never applied automatically

### 5. ReporterAgent
- **Inputs:** the whole state
- **Outputs:** `final_report` (Markdown): summary, issues, quality checks, root-cause
  hypotheses, escalation/remediation, cost recommendations, next steps

### Escalation node
Sets `should_escalate=true` and adds a system message. The run's status becomes
`ESCALATED`. There is no notification channel or resume-after-approval step yet.

---

## Demo mode

`DEMO_MODE=true` changes three things in `src/api/main.py`:

| Live | Demo |
|---|---|
| `DatabricksClient` (REST over httpx) | `DemoDatabricksClient`: serves `data/samples/transactions.csv`; cluster, `DESCRIBE DETAIL` and job runs come from `data/samples/demo_fixtures.json`. Remediation SQL changes an in-memory copy only. |
| LLM if `AZURE_OPENAI_API_KEY` is set | no LLM: rule-based root causes and report |
| no declared contracts | rules from `data/samples/schema.json` for the `transactions` table |

The agents, routing and report code are the same in both modes.

---

## Deployment

### Local (Docker Compose)
```bash
cp .env.example .env
# DEMO_MODE=true by default; set GRAFANA_ADMIN_PASSWORD
docker compose up --build -d
# API: http://localhost:8000/docs
# Prometheus: http://localhost:9090
# Grafana: http://localhost:3000 (admin / GRAFANA_ADMIN_PASSWORD from .env)
```

### Possible Azure deployment (not implemented)
```
Azure Container Registry → Container App (adep-api)
                                │
                    Managed identity / service principal
                                │
                    ┌───────────┴────────────┐
              Azure Databricks          Azure OpenAI
```

---

## Configuration reference

| Variable | Required | Description |
|----------|----------|-------------|
| `DEMO_MODE` | No | `true` = offline demo (default in `.env.example`; the app default is `false`) |
| `DATABRICKS_HOST` | Live mode | Workspace URL, e.g. `https://adb-xxx.azuredatabricks.net` |
| `DATABRICKS_TOKEN` | Live mode | Personal access token or service principal token |
| `DATABRICKS_CLUSTER_ID` | No | Cluster used by the health check |
| `AZURE_OPENAI_ENDPOINT` | No | Azure OpenAI resource endpoint |
| `AZURE_OPENAI_API_KEY` | No | Without it, the agents use rule-based logic |
| `AZURE_OPENAI_DEPLOYMENT` | No | Deployment name (default: `gpt-4o`) |
| `AZURE_OPENAI_API_VERSION` | No | API version (default: `2024-08-01-preview`) |
| `AGENT_TIMEOUT_SECONDS` | No | Timeout for one graph run (default: `300`) |
| `LOG_LEVEL` | No | Logging level (default: `INFO`) |
| `GRAFANA_ADMIN_PASSWORD` | For compose | Grafana admin password; compose refuses to start without it |
