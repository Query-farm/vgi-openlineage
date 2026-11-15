"""Discovery metadata for the ``openlineage`` catalog: categories, docs and helpers.

DuckDB, coding agents and the ``vgi-lint`` metadata checker read these tags.
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa

CATALOG_NAME = "openlineage"

SOURCE_URL = "https://github.com/Query-farm/vgi-openlineage"
OPENLINEAGE_SPEC_URL = "https://openlineage.io/docs/spec/object-model"
MARQUEZ_API_URL = "https://marquezproject.github.io/marquez/openapi.html"

# Every object's ``vgi.category`` names one of these.
SCHEMA_CATEGORIES = [
    {
        "name": "events",
        "title": "Event Files",
        "description": "Read raw OpenLineage RunEvent, DatasetEvent and JobEvent JSON from files, globs and URLs.",
    },
    {
        "name": "lineage",
        "title": "Lineage",
        "description": "Job-to-dataset and column-level lineage edges and graph traversal.",
    },
    {
        "name": "marquez",
        "title": "Marquez",
        "description": "Namespaces, sources, datasets, jobs, runs and lineage from a Marquez API server.",
    },
    {
        "name": "unified",
        "title": "Unified Orchestrator Runs",
        "description": "Runs and task runs in the column layout shared by every orchestrator worker.",
    },
]


def keywords_array(keywords: str) -> str:
    """Turn ``"a, b, c"`` into the JSON array string ``vgi.keywords`` requires."""
    return json.dumps([k.strip() for k in keywords.split(",") if k.strip()])


def object_tags(*, title: str, category: str, keywords: str, doc_llm: str, doc_md: str) -> dict[str, str]:
    """Build the standard per-object discovery tags."""
    if category not in {c["name"] for c in SCHEMA_CATEGORIES}:
        raise ValueError(f"unknown category {category!r}; add it to SCHEMA_CATEGORIES")
    return {
        "vgi.title": title,
        "vgi.category": category,
        "vgi.keywords": keywords_array(keywords),
        "vgi.doc_llm": doc_llm,
        "vgi.doc_md": doc_md,
    }


def column(name: str, type_: pa.DataType, comment: str, *, nullable: bool = True) -> pa.Field[Any]:
    """Build an Arrow field whose ``comment`` shows up in DuckDB's ``DESCRIBE`` and agent tools."""
    return pa.field(name, type_, nullable=nullable, metadata={b"comment": comment.encode()})


def categories_for(*names: str) -> list[dict[str, str]]:
    """Return the subset of ``SCHEMA_CATEGORIES`` a schema uses."""
    return [c for c in SCHEMA_CATEGORIES if c["name"] in names]


CATALOG_COMMENT = (
    "OpenLineage lineage as SQL: query raw RunEvent, DatasetEvent and JobEvent JSON files or a Marquez "
    "server's namespaces, datasets, jobs, runs and lineage graph from DuckDB."
)

CATALOG_DOC_LLM = (
    "Query OpenLineage lineage metadata with SQL. OpenLineage is the open standard that Airflow, Spark, "
    "dbt, Dagster, Flink and others use to emit RunEvents (a job run's START/RUNNING/COMPLETE/FAIL/ABORT "
    "with its input and output datasets), DatasetEvents and JobEvents, each carrying open-ended facets.\n\n"
    "Two sources, one vocabulary:\n"
    "- Schema `main` needs no credentials: table functions take a `path` (file, directory, glob or http(s) URL "
    "of event JSON or NDJSON, as written by the OpenLineage file and HTTP transports) and return events, the "
    "latest state per run, job-to-dataset edges, dataset schemas, column lineage, facets, a multi-hop lineage "
    "walk and the cross-orchestrator run shape. Two scalars classify events you loaded yourself.\n"
    "- Schema `marquez` reads a Marquez API server (the OpenLineage reference backend) and needs a DuckDB "
    "secret of type `marquez` with `BASE_URL` (and optionally `API_KEY`).\n"
    "- Schema `unified` exposes Marquez runs and task runs with the columns every Query.Farm orchestrator worker "
    "shares, so they `UNION ALL` with Airflow, Dagster or Prefect runs.\n\n"
    "Key concepts: a run is identified by its runId; a run with a `parent` facet is a task run of its parent "
    "(and root) run; status is normalized to queued, running, success, failed, cancelled or skipped; lineage "
    "node ids look like 'dataset:<namespace>:<name>' and 'job:<namespace>:<name>'; facets are `JSON`, so "
    "extract fields with `->>`."
)

CATALOG_DOC_MD = (
    "# OpenLineage\n\n"
    "Query [OpenLineage](https://openlineage.io) lineage from DuckDB: the runs, datasets, schemas and "
    "column-level lineage that Airflow, Spark, dbt, Dagster and other integrations emit.\n\n"
    "**Event files** (schema `main`) need nothing but the events: point a table function at a file, glob, "
    "directory or URL of what the OpenLineage `file` or `http` transport wrote.\n\n"
    "**Marquez** (schema `marquez`) reads a [Marquez](https://marquezproject.ai) API server: namespaces, "
    "datasets and their versions, jobs, runs and facets, the lineage graph, column lineage and search. Create "
    "a secret of type `marquez` with the server's `BASE_URL` first.\n\n"
    "**Unified** (schema `unified`) puts Marquez runs in the column layout shared by the Query.Farm "
    "orchestrator workers, ready for `UNION ALL`."
)

MAIN_DOC_LLM = (
    "Table functions over raw OpenLineage event JSON; no credentials. The first argument `path` is a file, a "
    "directory (every .json/.ndjson/.jsonl[.gz] inside, recursively), a glob such as '/var/log/ol/**/*.ndjson', "
    "or an http(s) URL. A source can hold one event object, a JSON array, NDJSON lines or a Marquez "
    '`{"events": [...]}` page; `ignore_errors := true` skips malformed lines. Each call re-reads the sources, '
    "so point it at the files once and join the results freely.\n\n"
    "Run state rules: START/RUNNING mean running, COMPLETE success, FAIL failed, ABORT cancelled, and a run with "
    "only OTHER events is queued; a terminal event wins over a START that arrives later. A run with a parent "
    "facet is a task run. The orchestrator comes from the jobType facet's integration, then the "
    "processing_engine facet, then the producer URI. Inputs and outputs are `LIST` of `STRUCT(namespace, name)`; "
    "facets are `JSON`."
)

MAIN_DOC_MD = (
    "## main: OpenLineage event files\n\n"
    "Every function reads `path`: a file, directory, glob or http(s) URL of OpenLineage events (single "
    "objects, JSON arrays or NDJSON, optionally gzipped).\n\n"
    "Results cover single events, run state, job inputs and outputs, schemas, column lineage, facets and "
    "multi-hop lineage, plus the shared orchestrator run shape."
)

MARQUEZ_DOC_LLM = (
    "Tables and functions over a Marquez API server (the OpenLineage reference backend). They need one DuckDB "
    "secret of type `marquez`: `BASE_URL` (the API root, e.g. 'http://localhost:5000'), optional `API_KEY` "
    "(sent as a bearer token) and optional `PAGE_SIZE` (default 100). Lists are paged with Marquez's "
    "limit/offset and the cursor travels with the scan.\n\n"
    "Pushdown: `namespace` equality on datasets, jobs and runs, `job_name` on runs, and `event_time` ranges on "
    "the raw event table (Marquez after/before). Marquez prefixes child job names with the parent job, e.g. "
    "'etl_orders.extract'. Lineage node ids are 'dataset:<ns>:<name>', 'job:<ns>:<name>' and "
    "'datasetField:<ns>:<dataset>:<field>'. Columns are `LIST` of `STRUCT` values; facets are `JSON`."
)

MARQUEZ_DOC_MD = (
    "## marquez: a Marquez API server\n\n"
    "Namespaces, sources, datasets, jobs, runs and raw events as tables; versions, run facets, the lineage "
    "graph, column lineage and search as functions.\n\n"
    "Create a secret of type `marquez` with the server's `BASE_URL` (and `API_KEY` if it sits behind an auth "
    "proxy) before querying."
)

UNIFIED_DOC_LLM = (
    "Marquez runs in the column layout shared by every Query.Farm orchestrator worker, so `UNION ALL` with "
    "dagster.unified.runs, airflow.unified.runs etc. works. Top-level runs are runs without an OpenLineage "
    "parent facet (DAG runs, dbt invocations, standalone jobs); task runs carry a parent facet (Airflow tasks, "
    "dbt models, Spark jobs launched by a task), and their run_id is the root (or parent) run. The "
    "orchestrator comes from the jobType facet's integration, the processing_engine facet or Airflow facets "
    "('unknown' when Marquez stored none). Needs the `marquez` secret. For event files use the "
    "`unified_runs(path)` and `unified_task_runs(path)` table functions in schema `main`."
)

UNIFIED_DOC_MD = (
    "## unified: cross-orchestrator runs\n\n"
    "Runs and task runs from Marquez in the shared orchestrator shape.\n\n"
    "`UNION ALL` them with the Airflow, Dagster, Prefect and other orchestrator workers."
)

AGENT_TEST_TASKS = [
    {
        "name": "failed_runs_from_files",
        "prompt": (
            "OpenLineage events were written to the files under test/fixtures/events/ (relative to the current "
            "directory). Which jobs have a failed run? Return job_name and the error message."
        ),
    },
    {
        "name": "events_per_type",
        "prompt": (
            "How many OpenLineage events of each eventType are in the files under test/fixtures/events/? "
            "Count events with no eventType (dataset and job events) as their own group."
        ),
    },
    {
        "name": "datasets_read_by_job",
        "prompt": (
            "Using the OpenLineage event files in test/fixtures/events/, list the input datasets "
            "(namespace and name) of the job etl_orders.extract."
        ),
    },
    {
        "name": "customers_schema",
        "prompt": (
            "Using the OpenLineage event files in test/fixtures/events/, what columns (name and type) does the "
            "dataset analytics.customers have, in schema order?"
        ),
    },
    {
        "name": "segment_column_sources",
        "prompt": (
            "From the OpenLineage events in test/fixtures/events/, which input columns feed the segment column "
            "of analytics.orders_enriched? Give input dataset and field."
        ),
    },
    {
        "name": "spark_engine_version",
        "prompt": (
            "In the OpenLineage events under test/fixtures/events/, which Spark version (from the "
            "processing_engine run facet) ran the orders_enrich job?"
        ),
    },
    {
        "name": "upstream_of_customers",
        "prompt": (
            "Using the OpenLineage events in test/fixtures/events/, list every dataset upstream of "
            "analytics.customers (any number of hops) with how many hops away it is."
        ),
    },
    {
        "name": "orchestrator_run_counts",
        "prompt": (
            "From the OpenLineage event files in test/fixtures/events/, how many top-level runs (not task runs) "
            "does each orchestrator have, and how many of them succeeded?"
        ),
    },
    {
        "name": "slowest_task_runs",
        "prompt": (
            "Using the OpenLineage events in test/fixtures/events/, which three finished task runs took the "
            "longest? Give task name and duration in seconds."
        ),
    },
    {
        "name": "producer_orchestrators",
        "prompt": (
            "Load the NDJSON file test/fixtures/all-events.ndjson with DuckDB's read_json and, using the openlineage "
            "worker to classify each event's producer URI, count the events per orchestrator."
        ),
    },
    {
        "name": "normalized_event_statuses",
        "prompt": (
            "Load test/fixtures/all-events.ndjson with DuckDB's read_json and count its events by the normalized "
            "run status of their eventType (as the openlineage worker defines it), leaving out events without one."
        ),
    },
    {
        "name": "marquez_namespaces",
        "prompt": (
            "A Marquez server is running at http://127.0.0.1:5056. How many datasets does each of its "
            "namespaces hold? Include namespaces with none."
        ),
    },
    {
        "name": "marquez_failed_runs",
        "prompt": (
            "A Marquez server is running at http://127.0.0.1:5056. Which jobs have a FAILED run, and how long "
            "did each failed run take in milliseconds?"
        ),
    },
    {
        "name": "marquez_unified_status",
        "prompt": (
            "A Marquez server is running at http://127.0.0.1:5056. Using the cross-orchestrator run layout, how "
            "many top-level runs and how many task runs are in each normalized status?"
        ),
    },
    {
        "name": "marquez_sources",
        "prompt": "A Marquez server is running at http://127.0.0.1:5056. Which data sources does it know, and what "
        "is each one's connection URL?",
    },
    {
        "name": "marquez_latest_failed_jobs",
        "prompt": "A Marquez server is running at http://127.0.0.1:5056. Which jobs (namespace and name) have a "
        "latest run that failed?",
    },
    {
        "name": "marquez_events_on_day",
        "prompt": "A Marquez server is running at http://127.0.0.1:5056. How many raw OpenLineage events did it "
        "receive with an event time on 2026-11-03 (UTC), per event type?",
    },
    {
        "name": "marquez_customers_versions",
        "prompt": "A Marquez server is running at http://127.0.0.1:5056. How many versions of the dataset "
        "analytics.customers (namespace bigquery) has it recorded?",
    },
    {
        "name": "marquez_transform_versions",
        "prompt": "A Marquez server is running at http://127.0.0.1:5056. List the versions of the job "
        "etl_orders.transform in namespace airflow-prod with their latest run state.",
    },
    {
        "name": "marquez_run_error",
        "prompt": "A Marquez server is running at http://127.0.0.1:5056. What error message is recorded in the "
        "facets of run 01930a6e-0000-7000-8000-000000000005?",
    },
    {
        "name": "marquez_downstream_datasets",
        "prompt": "A Marquez server is running at http://127.0.0.1:5056. Using its lineage graph, which datasets "
        "are downstream of dataset analytics.orders_daily (namespace bigquery)?",
    },
    {
        "name": "marquez_customer_column_sources",
        "prompt": "A Marquez server is running at http://127.0.0.1:5056. According to its column lineage, which "
        "input columns feed each column of bigquery dataset analytics.customers?",
    },
    {
        "name": "marquez_search_orders",
        "prompt": "A Marquez server is running at http://127.0.0.1:5056. Search it for 'orders': which datasets "
        "match (namespace and name)?",
    },
]

CATALOG_TAGS = {
    "vgi.title": "OpenLineage Lineage",
    "vgi.subtitle": "OpenLineage events and Marquez lineage as DuckDB tables: runs, datasets, column lineage.",
    "vgi.doc_llm": CATALOG_DOC_LLM,
    "vgi.doc_md": CATALOG_DOC_MD,
    "vgi.source_url": SOURCE_URL,
    "vgi.icon_url": "https://query.farm/media-kit/products/vgi/vgi-logo.png",
    "vgi.license": "0BSD",
    "vgi.author": "Query.Farm",
    "vgi.copyright": "Copyright 2026 Query Farm LLC - https://query.farm",
    "vgi.keywords": keywords_array(
        "openlineage, marquez, data lineage, column lineage, run events, airflow, spark, dbt, dagster, metadata"
    ),
    "vgi.support_contact": f"{SOURCE_URL}/issues",
    "vgi.support_policy_url": f"{SOURCE_URL}#support",
    "vgi.agent_test_tasks": json.dumps(AGENT_TEST_TASKS),
}
