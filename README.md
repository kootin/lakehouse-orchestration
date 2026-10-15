# lakehouse-orchestration

A daily card-transactions pipeline. Airflow orchestrates it, Polars transforms the data, and the results land in Apache Iceberg tables managed with PyIceberg. An AWS Lambda handler runs the same steps when a file lands in S3.

## Posting skills this demonstrates

From JPMorgan Chase's **Lead Data Engineer** posting:

| Posting asks for | Where it shows up here |
| --- | --- |
| Data pipelines in Python, with orchestration tools such as Airflow or Argo | `dags/card_transactions_dag.py`, an Airflow 3 TaskFlow DAG with retries, catchup and serialized runs |
| Table formats such as Apache Iceberg | Two Iceberg tables with day and identity partitioning, MERGE-style upserts, dynamic partition overwrite and snapshot time travel |
| Libraries such as Polars and PyIceberg | All transforms are Polars; all table I/O is PyIceberg |
| AWS tools such as Glue and Lambda | `aws_lambda.py` handles S3 `ObjectCreated` events; swapping the SQL catalog for `GlueCatalog` and the warehouse path for S3 is the only production change |
| Data lifecycle; data quality and integrity | Corrupt-line handling, late-arriving updates, idempotent re-runs and a quality gate that fails the DAG run |
| Git and CI/CD | GitHub Actions runs the tests and an end-to-end load on every push |

## The pipeline

```
landing/dt=YYYY-MM-DD/*.jsonl          (S3 landing bucket in production)
        │  AUTHORIZED events (nested JSON)  +  late SETTLED / DECLINED events for yesterday
        ▼
ingest ──► payments.transactions          Iceberg, partitioned by day(authorized_at)
        │     new txns appended; late settlements MERGEd on txn_id
        ▼
summarise ──► payments.daily_merchant_summary   Iceberg, partitioned by txn_date
        │     today and yesterday rebuilt with dynamic partition overwrite
        ▼
quality_gate    duplicates, non-positive amounts, summary-to-detail reconciliation
```

**Why upserts.** Settlement events arrive the day after authorization. An append-only table would end up holding two versions of each transaction. Iceberg's row-level upsert keeps one row per `txn_id` with its latest status, and every commit is still an immutable snapshot you can query back in time.

## Run it

```bash
pip install -e ".[dev,airflow]"
make land      # writes 3 days of landing files
make run       # runs ingest, summary and quality checks for each day without Airflow
make test
make airflow   # optional: airflow standalone with this DAG folder, then trigger card_transactions_lakehouse
```

Sample output:

```
{'day': '2026-10-07', 'files': 1, 'bad_lines': 1, 'inserted': 500, 'updated': 300, 'unmatched': 0}
  {'day': '2026-10-07', 'rows': 10}
  {'day': '2026-10-07', 'transactions': 500, 'summary_rows': 5}

Snapshot history of payments.transactions:
  266152221968016622   append    added=500
  8091730352229654887  overwrite added=200
  ...
Status breakdown:  AUTHORIZED 900 · SETTLED 574 · DECLINED 26
```

## Tests

| Test | What it proves |
| --- | --- |
| `test_late_settlements_upsert_into_previous_day` | 60 settlements update day-1 rows in place, with no duplicates |
| `test_reingesting_a_day_is_idempotent` | Re-running a day inserts nothing |
| `test_time_travel_sees_pre_settlement_state` | An older snapshot still shows every transaction as AUTHORIZED |
| `test_summary_and_quality_gate` | Yesterday's summary picks up today's settlements |
| `test_lambda_handler_processes_s3_event` | A URL-encoded S3 key is routed to the right partition |
| `test_dag_structure` | The DAG imports cleanly and has the expected task order and retries |
