<p align="center">
  <img src="https://query.farm/media-kit/products/vgi/vgi-logo.png" alt="Vector Gateway Interface (VGI)" width="320">
</p>

# Query OpenLineage Runs, Datasets and Column Lineage in DuckDB

> **vgi-openlineage** · a [Query.Farm](https://query.farm) VGI worker

[![CI](https://github.com/Query-farm/vgi-openlineage/actions/workflows/ci.yml/badge.svg)](https://github.com/Query-farm/vgi-openlineage/actions/workflows/ci.yml)
[![vgi worker: query.farm](https://query.farm/media-kit/shields/vgi-worker.svg)](https://query.farm)

[OpenLineage](https://openlineage.io) is the open standard Airflow, Spark, dbt, Dagster,
Flink and others use to report what ran and what it read and wrote. This worker turns that
lineage into SQL two ways:

- **Event files, no server.** Point a table function at a file, directory, glob or URL of
  the RunEvent / DatasetEvent / JobEvent JSON that the OpenLineage `file` or `http`
  transport writes (single objects, JSON arrays, NDJSON, gzip) and get typed runs, job
  inputs and outputs, schemas, column lineage, facets and a multi-hop lineage walk.
- **Marquez.** Read a [Marquez](https://marquezproject.ai) API server: namespaces,
  sources, datasets and versions, jobs and versions, runs and facets, raw events, the
  lineage graph, column lineage and search.
- **Unified runs.** `openlineage.unified.runs` / `task_runs` use the columns every
  Query.Farm orchestrator worker shares, so lineage-derived runs `UNION ALL` with the
  Airflow, Dagster and Prefect workers.

## Quick start

```sql
INSTALL vgi FROM community;
LOAD vgi;
ATTACH 'openlineage' (TYPE vgi, LOCATION 'uvx --from git+https://github.com/Query-farm/vgi-openlineage@v0.1.1 vgi-openlineage-worker');

-- Event files: what failed, and why?
SELECT job_name, ended_at, error_message
FROM openlineage.main.runs('/var/log/openlineage/*.ndjson')
WHERE status = 'failed' ORDER BY ended_at DESC;

-- Everything upstream of a table.
SELECT namespace, name, hops
FROM openlineage.main.lineage('/var/log/openlineage/*.ndjson', 'analytics.customers',
                              depth := 20, direction := 'upstream')
WHERE node_type = 'dataset' ORDER BY hops;

-- Marquez.
CREATE SECRET marquez (TYPE marquez, BASE_URL 'http://localhost:5000');
SELECT namespace, name, latest_run_state FROM openlineage.marquez.jobs WHERE latest_run_state = 'FAILED';
```

With DuckDB 2.0 instead of Haybarn, load the extension from the Query.Farm repository:

```sql
SET allow_extension_repositories = 'allowed';
CREATE EXTENSION REPOSITORY IF NOT EXISTS query_farm WITH PREFIX 'https://duckdb-extensions.query.farm';
INSTALL vgi FROM query_farm;
LOAD vgi FROM query_farm;
```

From a source checkout: `make build`, then
`LOCATION '/abs/path/to/vgi-openlineage/.venv/bin/vgi-openlineage-worker'`.

## Event files (`openlineage.main`)

Every function takes `path` first: a file, a directory (every `.json`, `.ndjson`,
`.jsonl`, optionally `.gz`, recursively), a glob (`/logs/**/*.ndjson`) or an http(s) URL. A
source may hold one event, a JSON array, NDJSON lines or a Marquez `{"events": [...]}`
page. `ignore_errors := true` skips malformed lines.

| Function | One row per |
|---|---|
| `read_events(path)` | event: kind, type, time, run/job identity, parent facet, orchestrator, inputs/outputs, facets as `JSON`, the whole event |
| `runs(path)` | run (latest state): state + normalized status, start/end/duration, parent and root run, job type, nominal time, error message, datasets |
| `job_io(path)` | job → dataset edge (`input` / `output`) with run count and first/last seen |
| `dataset_schema(path)` | field of the latest `schema` facet of each dataset (nested fields dotted) |
| `column_lineage(path)` | output column ← input column edge from `columnLineage` facets, with transformation type/subtype |
| `facets(path)` | facet (run, job, dataset, input, output) as `JSON` |
| `lineage(path, node, depth := 5, direction := 'both')` | node reachable from `node`, with hop count |
| `unified_runs(path)` / `unified_task_runs(path)` | run / task run in the shared orchestrator layout |
| `orchestrator(producer)` | scalar: airflow, dagster, dbt, spark, ... from a producer URI |
| `run_status(event_type)` | scalar: eventType → queued / running / success / failed / cancelled |

Run rules: START and RUNNING mean running, COMPLETE success, FAIL failed, ABORT cancelled
and OTHER-only queued; a terminal event wins over a START that arrives late. A run with a
`parent` facet is a task run; its unified `run_id` is the root run when the facet names
one, else the parent. The orchestrator comes from the `jobType` facet's integration, then
the `processing_engine` facet, then Airflow facets, then the producer URI.

## Marquez (`openlineage.marquez`, `openlineage.unified`)

```sql
CREATE SECRET marquez (TYPE marquez, BASE_URL 'http://localhost:5000');
-- Behind an auth proxy or a hosted backend that takes a bearer token:
CREATE SECRET marquez (TYPE marquez, BASE_URL 'https://lineage.example.com', API_KEY '...');
```

| Field | Meaning |
|---|---|
| `BASE_URL` | Marquez API root (`/api/v1` is appended; a trailing `/api/v1` is fine) |
| `API_KEY` | optional, sent as `Authorization: Bearer <key>`; redacted in `duckdb_secrets()` |
| `PAGE_SIZE` | optional, rows per page (`limit`), 1–1000, default 100 |

Tables: `namespaces`, `sources`, `datasets` (fields as `LIST(STRUCT)`, facets `JSON`),
`jobs`, `runs` (every run of every job), `lineage_events` (the raw events Marquez stored, in
the `read_events` layout), and `unified.runs` / `unified.task_runs`.

Functions: `dataset_versions(namespace, name)`, `job_versions(namespace, name)`,
`run_facets(run_id)`, `lineage_graph(node_id, depth := 20)`,
`column_lineage(node_id, depth := 20, with_downstream := false)`,
`search(query, filter := 'ALL', sort := 'UPDATE_AT', max_results := 100)`.

Node ids follow Marquez: `dataset:<namespace>:<name>`, `job:<namespace>:<name>`,
`datasetField:<namespace>:<dataset>:<field>`. Marquez prefixes child jobs with their
parent job's name (`etl_orders.extract`).

## What pushes down

| Relation | Pushed to Marquez |
|---|---|
| `marquez.datasets`, `marquez.jobs` | `namespace = ...` / `IN (...)` → one listing per namespace |
| `marquez.runs` | `namespace` and `job_name` (`=` / `IN`) → only those jobs' runs are listed |
| `unified.runs` / `unified.task_runs` | `pipeline` / `task_name` equality → only those jobs |
| `marquez.lineage_events` | `event_time` ranges → `after` / `before` |
| every table and function | projection (only requested columns are built) |

The SDK re-applies every predicate, so pushdown only narrows what is fetched. Marquez
lists are paged with `limit`/`offset`; the cursor (current listing and offset) lives in
the table-function state, so a long scan resumes in any worker process over HTTP. 429 and
5xx responses are retried with backoff that honours `Retry-After`.

The event-file functions read their sources on every call; `read_events` streams one
source per batch (20,000 events at a time), the derived functions emit 20,000 rows per
batch.

## Testing

`make check` runs ruff, mypy, pydoclint, the unit tests, the SQL suite over the stdio,
launch and HTTP transports, and the metadata lint.

- `test/fixtures/events` holds hand-built OpenLineage events (regenerate with
  `test/fixtures/make_events.py`): two Airflow DAG runs (one fails in a task, with an
  `errorMessage` facet), a Spark job launched by an Airflow task (`parent` + `root`), a
  dbt run with a still-running model, an aborted Dagster run, a DatasetEvent and a
  JobEvent, with schema and column-lineage facets.
- `test/fixtures/marquez/recordings.json` holds the responses of a real Marquez 0.51.1
  (`test/marquez/docker-compose.yml`, seeded by `test/marquez/seed.py` with the same
  events). `test/mock_marquez/server.py` replays them, rate-limits each URL once and
  checks a bearer token, so CI needs no Docker. Re-record with
  `MOCK_RECORD=http://127.0.0.1:5055 make test-sql` against a seeded Marquez.
- The metadata lint runs with execution disabled (`vgi-lint.toml`): the Marquez examples
  need a running server and a `marquez` secret. `make lint-ai` starts the replayed Marquez
  on port 5056 for the agent tasks.

## Support

Open an issue at https://github.com/Query-farm/vgi-openlineage/issues.

## Authorship & License

Copyright 2026 Query Farm LLC - https://query.farm

Licensed under the BSD Zero Clause License (0BSD); see [LICENSE](LICENSE).
