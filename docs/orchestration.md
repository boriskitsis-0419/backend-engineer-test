# Orchestration: scheduling and monitoring in production

The workflow is defined in
[`workflows.py`](../src/ecommerce_pipeline/orchestration/workflows.py). The
stages it calls live in [`steps.py`](../src/ecommerce_pipeline/orchestration/steps.py),
so the Flyte layer contributes orchestration and nothing else — the same
stages are exercised directly by the test suite, without a cluster.

```
ensure_schema → verify_source → load_data → transform_data → quality_gate → finalise
```

## Why these are separate tasks

Running the whole pipeline as one task would be simpler to write and worse to
operate:

- **Attributable failure.** "The load succeeded and the quality gate failed" is
  a different incident, with a different response, from "the load failed".
  Flyte's UI shows which node failed and its inputs.
- **Cheap retries.** Retrying a failed quality gate re-runs a few seconds of
  SQL. Inside a single task it would re-`COPY` 20M rows.
- **Right-sized resources.** `load_data` requests 4Gi because it holds a CSV
  chunk plus its staging buffer; `quality_gate` requests 1Gi. One task would
  have to request the maximum for the whole duration.

Retries are safe because every stage is idempotent: the load upserts on the
primary key, and the aggregate refresh deletes and rebuilds its date window.

## Scheduling

Nightly incremental load, registered as a Flyte `LaunchPlan`:

```python
from datetime import timedelta
from flytekit import LaunchPlan, FixedRate
from ecommerce_pipeline.orchestration.workflows import ecommerce_etl

nightly = LaunchPlan.get_or_create(
    name="ecommerce_etl_nightly",
    workflow=ecommerce_etl,
    default_inputs={"source": "s3://warehouse-landing/ecommerce/",
                    "incremental": True},
    schedule=FixedRate(duration=timedelta(days=1)),
)
```

Registration and activation:

```bash
pyflyte register --project ecommerce --domain production \
    src/ecommerce_pipeline/orchestration/workflows.py
flytectl update launchplan --project ecommerce --domain production \
    ecommerce_etl_nightly --activate
```

Points worth making explicit:

- **A `CronSchedule` with `kickoff_time_input_arg` is preferable** to
  `FixedRate` once loads are partitioned by day, because it passes the
  scheduled time into the workflow. That makes a re-run of a specific day
  deterministic rather than dependent on when it happens to execute.
- **Backfills use a separate entry point.** `ecommerce_backfill` exists so a
  full reload cannot be triggered by flipping `incremental` on the nightly
  schedule by accident.
- **Concurrency must be capped at one.** Two concurrent runs would race on the
  watermark. Flyte's `max_parallelism` on the launch plan, or an advisory lock
  in the load stage, both work; the advisory lock is the safer of the two
  because it holds regardless of what triggered the second run.
- **Late-arriving data** is handled by the watermark, not by the schedule. A
  run that fails leaves the watermark where it was, so the next run re-reads
  the same window rather than skipping it.

## Monitoring

Three layers, because each catches something the others miss.

### 1. Workflow execution (Flyte)

Node-level status, durations, inputs and outputs are in the Flyte console.
Alert on: workflow failure, and — more importantly — on a run *not starting*.
A pipeline that silently stops being scheduled looks exactly like a pipeline
with no new data.

Notifications attach to the launch plan:

```python
from flytekit import Email, WorkflowExecutionPhase

notifications=[
    Email(phases=[WorkflowExecutionPhase.FAILED, WorkflowExecutionPhase.TIMED_OUT],
          recipients_email=["data-oncall@example.com"]),
]
```

### 2. Pipeline state (the database)

The pipeline records its own history, so monitoring does not depend on log
scraping or on Flyte being reachable:

```sql
-- Recent runs and their verdicts.
SELECT run_id, workflow, status, started_at, finished_at,
       rows_extracted, rows_loaded, rows_rejected, error_message
FROM etl_run ORDER BY started_at DESC LIMIT 20;

-- Failing checks over the last week.
SELECT check_name, severity, observed, threshold, checked_at
FROM data_quality_check
WHERE NOT passed AND checked_at > NOW() - INTERVAL '7 days'
ORDER BY checked_at DESC;

-- Freshness: how far behind is each entity?
SELECT entity, watermark_ts, NOW() - watermark_ts AS lag
FROM etl_watermark ORDER BY lag DESC;
```

Alerting thresholds worth setting:

| Signal | Condition | Severity |
| --- | --- | --- |
| Run failed | `etl_run.status = 'failed'` | page |
| No successful run | none in 26h | page |
| Watermark lag | `NOW() - watermark_ts > 26h` | page |
| Blocking check failed | `data_quality_check` error severity | page |
| Reject rate | `rows_rejected / rows_extracted > 1%` | ticket |
| Warning check failed | `data_quality_check` warning severity | ticket |
| Load duration | > 2× trailing 7-day median | ticket |

The "no successful run" and watermark-lag alerts are the ones that catch a
silently dead scheduler; run-failure alerts alone will not.

### 3. Database health

Standard PostgreSQL monitoring, with two pipeline-specific additions:

```sql
-- Rows that missed every declared partition. Should always be zero; a
-- non-zero value means partition creation fell behind ingestion.
SELECT * FROM count_default_partition_rows();

-- Partitions eligible for archival under a retention policy.
SELECT * FROM orders_partitions_older_than(CURRENT_DATE - INTERVAL '3 years');
```

Also watch table and index bloat on `orders`/`order_items`, autovacuum lag on
the partitions being written, and replication lag if reads are served from a
replica.

## Failure handling

| Failure | Behaviour | Recovery |
| --- | --- | --- |
| Transient DB error | Task retries twice | Automatic |
| Malformed source file | `verify_source` fails before loading | Fix upstream, re-run |
| Individual bad rows | Quarantined to `--reject-dir`, run continues | Inspect, correct, replay |
| Missing FK parent | Row quarantined, rest of batch loads | Load the parent, re-run |
| Blocking quality failure | Workflow fails, `etl_run` marked failed | Investigate, re-run |
| Load fails part-way | Earlier entities stay committed | Re-run; upserts are idempotent |

The watermark advances only after the load, transformations and checks have
all succeeded, so any failure leaves the next run reading the same window.

One subtlety worth knowing: the load stage closes its own `etl_run` row as
succeeded when the `COPY` finishes, because that is true of the load. A later
quality-gate failure therefore has to *overwrite* that verdict — which
`mark_run_failed` does deliberately. Without it, a run that failed its gate
would still read as `succeeded` in the history, and the run history is the one
thing an operator has to be able to trust.

## Running it

```bash
docker compose run --rm workflow                      # incremental, local
docker compose run --rm workflow python -m \
    ecommerce_pipeline.orchestration.workflows \
    --source /app/data/sample                         # full reload
```

Exit codes: `0` success, `1` unexpected failure, `3` blocking data-quality
failure.
