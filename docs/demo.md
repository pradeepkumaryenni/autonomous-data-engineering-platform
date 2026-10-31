# Offline demo: captured output

This is real output from one run of the API in demo mode (local `uvicorn`, Python
3.11), captured on 2026-11-01 IST (2026-10-31 UTC, hence the timestamps). Nothing
was edited, except that the long list fields in the status response are summarized.

**What demo mode uses:** `DEMO_MODE=true` swaps the Databricks client for
`DemoDatabricksClient` (`src/databricks/demo_client.py`). It serves
`data/samples/transactions.csv` as the table `demo.bronze.transactions` and answers
cluster, table-detail and job-history calls from `data/samples/demo_fixtures.json`.
No LLM is called: root-cause analysis and the report use the deterministic,
rule-based code paths. The data is synthetic, and its problems were injected on
purpose (see `data/samples/generate_sample_data.py`):

| Injected problem | Size | Expected result |
|---|---|---|
| Duplicate rows | 100 of 10,000 (1%) | duplicates check fails |
| Null `amount` | 298 rows (3%) | passes the 5% threshold |
| Null `country` | 100 rows (1%) | passes the 5% threshold |
| `amount` x100 from 2024-12-01 | 839 rows | drift and range checks fail |
| Failed run + retry in job history | runs 880107 / 880108 | evidence for the duplicates hypothesis |

## How it was produced

```bash
DEMO_MODE=true uvicorn src.api.main:app --port 8765

curl -X POST http://localhost:8765/api/v1/pipelines/run \
  -H "Content-Type: application/json" \
  -d '{"pipeline_name": "bronze_transactions", "table_names": ["demo.bronze.transactions"], "warehouse_id": "demo-warehouse", "cluster_id": "0101-demo-etl01", "job_id": 1001}'
```

Response (`202 Accepted`):

```json
{
  "run_id": "178d2e65-14ce-4ea7-8612-77a6a33c6b89",
  "status": "PENDING",
  "pipeline_name": "bronze_transactions",
  "started_at": "2026-10-31T19:45:35.964726Z"
}
```

```bash
curl http://localhost:8765/api/v1/pipelines/178d2e65-14ce-4ea7-8612-77a6a33c6b89/status
```

Status response (list fields summarized):

```json
{
  "run_id": "178d2e65-14ce-4ea7-8612-77a6a33c6b89",
  "status": "ESCALATED",
  "pipeline_name": "bronze_transactions",
  "started_at": "2026-10-31T19:45:35.964726Z",
  "completed_at": "2026-10-31T19:45:36.413763Z",
  "should_escalate": true,
  "quality_results": "[11 items]",
  "root_causes": "[2 items]",
  "cost_recommendations": "[4 items]",
  "remediation_actions": [],
  "report": "(shown below)"
}
```

The status is `ESCALATED`, not `COMPLETE`. Drift makes the table's issue HIGH
severity, and HIGH/CRITICAL issues are never auto-remediated. The auto-remediation
path (dedup of a LOW-severity table, then VACUUM) is exercised by
`tests/test_agents/test_demo_graph.py::test_demo_run_auto_remediates_low_severity_duplicates`.

## The `report` field, verbatim

---

# Pipeline Analysis Report: bronze_transactions

_Generated 2026-10-31 19:45 UTC · mode: offline demo (sample data, fixture workspace) · report: rule-based (no LLM)_

## Summary

- Tables checked: 1 (`demo.bronze.transactions`)
- Quality checks: 11 run, 3 failed
- Highest issue severity: **HIGH**
- Outcome: Escalated for human review. No automatic changes were made.
- Cost recommendations: 4 (heuristic estimate $2,981.15/month, not measured)

## Issues detected

| Table | Type | Severity | Detail |
|---|---|---|---|
| demo.bronze.transactions | quality | **HIGH** | Table 'demo.bronze.transactions' failed 3/11 quality checks: Duplicate check FAILED: 100 duplicates found (1.00%) on columns ['id']; Range check FAILED: 65 values outside [0.01, 50000.0] in 'amount';… |

## Quality checks

| Table | Check | Column | Result | Detail |
|---|---|---|---|---|
| demo.bronze.transactions | duplicates | id | **FAIL** | Duplicate check FAILED: 100 duplicates found (1.00%) on columns ['id'] |
| demo.bronze.transactions | range | amount | **FAIL** | Range check FAILED: 65 values outside [0.01, 50000.0] in 'amount' |
| demo.bronze.transactions | drift | amount | **FAIL** | Statistical drift check FAILED: Z-score=4.31 (threshold=3.0) for 'amount'. Current mean=1657.4056, baseline mean=185.0800 |
| demo.bronze.transactions | nulls | id | pass | Null check passed: 0.00% nulls in 'id' (threshold: 0.00%) |
| demo.bronze.transactions | nulls | amount | pass | Null check passed: 2.98% nulls in 'amount' (threshold: 5.00%) |
| demo.bronze.transactions | nulls | customer_id | pass | Null check passed: 0.00% nulls in 'customer_id' (threshold: 5.00%) |
| demo.bronze.transactions | nulls | status | pass | Null check passed: 0.00% nulls in 'status' (threshold: 5.00%) |
| demo.bronze.transactions | nulls | timestamp | pass | Null check passed: 0.00% nulls in 'timestamp' (threshold: 5.00%) |
| demo.bronze.transactions | nulls | product_id | pass | Null check passed: 0.00% nulls in 'product_id' (threshold: 5.00%) |
| demo.bronze.transactions | nulls | country | pass | Null check passed: 1.00% nulls in 'country' (threshold: 5.00%) |
| demo.bronze.transactions | referential | status | pass | Referential integrity check passed: 0 invalid values in 'status' |

## Root cause hypotheses

### 1. drift on `amount` (confidence 60%, rule-based)

The scale of 'amount' shifted: current mean 1,657.41 vs baseline 185.08 (9.0x). A sudden multiplicative shift usually means an upstream unit or currency change, or a new source feeding the table.

Evidence:
- Statistical drift check FAILED: Z-score=4.31 (threshold=3.0) for 'amount'. Current mean=1657.4056, baseline mean=185.0800
- Range check FAILED: 65 values outside [0.01, 50000.0] in 'amount'
- Job history: 15 recent runs, 1 failed, median duration 12.4 min

Suggested next step: Not auto-fixable. Find the first load where the shift starts (e.g. average 'amount' by load date), confirm the unit with the source owner, then correct and backfill the affected partitions.

### 2. duplicates on `id` (confidence 60%, rule-based)

Likely a non-idempotent load: a run failed after writing part of its output and the retry appended the same records again.

Evidence:
- Duplicate check FAILED: 100 duplicates found (1.00%) on columns ['id']
- Run 880107 on 2024-11-26 FAILED: Task ingest_transactions failed: java.net.SocketTimeoutException: Read timed out while writing to ADLS (stage 4, partial output committed); retried by run 880108 (SUCCESS)
- Job history: 15 recent runs, 1 failed, median duration 12.4 min

Suggested next step: Make the load idempotent (MERGE INTO on the key, or overwrite by partition), then dedup the existing rows once.

## Escalation and remediation

1 issue(s) at HIGH/CRITICAL severity. Policy: these are never auto-remediated, so the remediation step was skipped for this run and the issues are handed to a human.

## Cost recommendations

| Priority | Category | Target | Recommendation | Est. USD/month |
|---|---|---|---|---|
| HIGH | compute | 0101-demo-etl01 | Enable autoscaling to reduce idle compute costs: Autoscale not enabled (8 fixed workers); saving assumes 35% fewer worker-hours | 1,897.09 |
| HIGH | storage | demo.bronze.transactions | OPTIMIZE: 2400 files, avg 1.3 MB (target ~128 MB): small files slow down scans | 0.00 |
| MEDIUM | compute | 0101-demo-etl01 | Set auto-termination to 30 minutes to reduce idle costs: Auto-termination is disabled; saving assumes 20% of cluster hours are idle | 1,084.05 |
| LOW | storage | demo.bronze.transactions | VACUUM: Remove data files no longer referenced by the Delta log (7-day retention). Saving assumes 10% of table size is unreferenced; VACUUM ... DRY RUN gives the real figure | 0.01 |

_Savings are rough estimates from fixed assumptions in `cost_optimizer_agent.py`, useful for ranking only._

## Next steps

1. Review the HIGH/CRITICAL issues above before any data is changed.
2. `amount` (drift): Not auto-fixable. Find the first load where the shift starts (e.g. average 'amount' by load date), confirm the unit with the source owner, then correct and backfill the affected partitions.
3. `id` (duplicates): Make the load idempotent (MERGE INTO on the key, or overwrite by partition), then dedup the existing rows once.
4. Review the HIGH-priority cost actions; compute changes and HIGH-priority storage actions are never auto-applied.
