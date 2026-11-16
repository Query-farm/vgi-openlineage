# Changelog

## 0.1.2 (2026-11-16)

- Bump vgi-python to 0.52.2.
- Requires Python 3.14 (CI, Docker image and tooling moved to 3.14).

## 0.1.1

- Fix: every query whose filter is pushed down to the worker (a `WHERE` clause, or
  `ORDER BY … LIMIT`) failed with "No DuckDB-compatible engine is installed". The SDK binds
  and evaluates pushed predicates with a local engine, which was only in the dev
  dependency group; the runtime dependency is now `vgi-python[http,haybarn]`.

## 0.1.0

Initial release: OpenLineage event files (read_events, runs, job_io, dataset_schema,
column_lineage, facets, lineage, unified_runs, unified_task_runs, orchestrator, run_status),
a Marquez API server (namespaces, sources, datasets, jobs, runs, lineage_events,
dataset_versions, job_versions, run_facets, lineage_graph, column_lineage, search) and the
shared `unified.runs` / `unified.task_runs` orchestrator tables.

Targets: vgi-python 0.51 · VGI protocol 3.3 · Haybarn 2.0.0rc11 · Marquez 0.51 · vgi-lint-check 2.x (unpinned)
