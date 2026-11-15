"""Marquez tables and functions (schema ``marquez``) and the ``unified`` tables.

Paged scans follow one ``PageCursor`` through a list of *listings* (API paths): the
listings are resolved once in ``initial_state`` (e.g. every namespace, or every job for
``runs``), then each ``process`` call fetches one ``limit``/``offset`` page of the current
listing. The cursor lives in table-function state, so long scans resume on any worker
process over HTTP.

Pushdown: ``namespace = / IN`` on datasets, jobs and runs (and ``job_name`` on runs)
narrows the listings; ``event_time`` ranges on ``lineage_events`` become Marquez's
``after`` / ``before`` parameters. The SDK re-applies every predicate afterwards.
"""

from __future__ import annotations

import json
from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Annotated, Any, ClassVar

import pyarrow as pa
from vgi.arguments import Arg
from vgi.metadata import FunctionExample
from vgi.table_function import (
    OutputCollector,
    ProcessParams,
    TableFunctionGenerator,
    bind_fixed_schema,
    init_single_worker,
)
from vgi_rpc import ArrowSerializableDataclass

from . import events as ev
from .common import (
    DATASET_REFS,
    DATASET_VERSIONS,
    FIELDS,
    JSON,
    STR_LIST,
    TSTZ,
    NoArgs,
    eq_values,
    range_bounds,
    result_columns,
    rows_to_batch,
)
from .event_functions import EVENTS_SCHEMA, UNIFIED_RUNS_SCHEMA, UNIFIED_TASK_RUNS_SCHEMA
from .marquez_client import SECRET_REQUIREMENT, Endpoint, endpoint, get_json, quote
from .meta import column, object_tags

MAX_EMPTY_PAGES = 50

# Marquez RunState -> the shared orchestrator vocabulary.
RUN_STATE = {
    "NEW": "queued",
    "RUNNING": "running",
    "COMPLETED": "success",
    "FAILED": "failed",
    "ABORTED": "cancelled",
}


@dataclass(kw_only=True)
class PageCursor(ArrowSerializableDataclass):
    """Listings to walk (``[path, query, context]`` JSON), the current one, and its offset."""

    listings_json: str = "[]"
    index: int = 0
    offset: int = 0


def _ts(value: Any) -> datetime | None:
    return ev.parse_time(value)


def _clean(facets: Any) -> dict[str, Any]:
    return ev.clean_facets(facets)


def _refs(items: Any) -> list[dict[str, Any]]:
    out = []
    for item in items or []:
        if isinstance(item, dict):
            out.append({"namespace": item.get("namespace"), "name": item.get("name")})
    return out


def _versions(items: Any) -> list[dict[str, Any]]:
    out = []
    for item in items or []:
        vid = item.get("datasetVersionId") if isinstance(item, dict) else None
        if isinstance(vid, dict):
            out.append({"namespace": vid.get("namespace"), "name": vid.get("name"), "version": vid.get("version")})
    return out


def _fields(items: Any) -> list[dict[str, Any]]:
    out = []
    for f in items or []:
        if isinstance(f, dict):
            out.append(
                {
                    "name": f.get("name"),
                    "type": f.get("type"),
                    "description": f.get("description"),
                    "tags": [str(t) for t in f.get("tags") or []],
                }
            )
    return out


def _tag_names(items: Any) -> list[str]:
    return [str(t.get("name") if isinstance(t, dict) else t) for t in items or []]


class PagedScan[A](TableFunctionGenerator[A, PageCursor]):
    """Walk ``limit``/``offset`` pages of one or more Marquez listings."""

    FIXED_SCHEMA: ClassVar[pa.Schema]
    ITEMS_KEY: ClassVar[str]

    @classmethod
    def listings(cls, ep: Endpoint, params: ProcessParams[Any]) -> list[list[Any]]:
        """``[path, query, context]`` entries to page through."""
        raise NotImplementedError

    @classmethod
    def build_rows(cls, ep: Endpoint, items: list[dict[str, Any]], context: Any) -> list[dict[str, Any]]:
        """Rows for one page of items."""
        raise NotImplementedError

    @classmethod
    def initial_state(cls, params: ProcessParams[Any]) -> PageCursor:
        """Resolve the listings (this may page through namespaces or jobs)."""
        ep = endpoint(params.secrets)
        return PageCursor(listings_json=json.dumps(cls.listings(ep, params), separators=(",", ":")))

    @classmethod
    def process(cls, params: ProcessParams[Any], state: PageCursor, out: OutputCollector) -> None:
        """Fetch pages until one yields rows (or the listings run out) and emit them."""
        listings = json.loads(state.listings_json)
        ep = endpoint(params.secrets)
        for _ in range(MAX_EMPTY_PAGES):
            if state.index >= len(listings):
                out.finish()
                return
            path, query, context = listings[state.index]
            body = get_json(ep, path, {**query, "limit": ep.page_size, "offset": state.offset}, missing_ok=True)
            items = [i for i in (body or {}).get(cls.ITEMS_KEY) or [] if isinstance(i, dict)]
            if len(items) < ep.page_size:
                state.index += 1
                state.offset = 0
            else:
                state.offset += ep.page_size
            rows = cls.build_rows(ep, items, context)
            if rows:
                out.emit(rows_to_batch(rows, params.output_schema))
                return
        out.emit(rows_to_batch([], params.output_schema))


def all_namespaces(ep: Endpoint) -> list[str]:
    """Every namespace name (paged)."""
    names: list[str] = []
    offset = 0
    while True:
        body = get_json(ep, "/namespaces", {"limit": ep.page_size, "offset": offset}) or {}
        page = [n.get("name") for n in body.get("namespaces") or [] if isinstance(n, dict)]
        names.extend(str(n) for n in page if n)
        if len(page) < ep.page_size:
            return names
        offset += ep.page_size


def all_jobs(ep: Endpoint, namespaces: list[str] | None) -> list[dict[str, Any]]:
    """Every job (paged), optionally only in ``namespaces``."""
    paths = [f"/namespaces/{quote(ns)}/jobs" for ns in namespaces] if namespaces is not None else ["/jobs"]
    jobs: list[dict[str, Any]] = []
    for path in paths:
        offset = 0
        while True:
            body = get_json(ep, path, {"limit": ep.page_size, "offset": offset}, missing_ok=True) or {}
            page = [j for j in body.get("jobs") or [] if isinstance(j, dict)]
            jobs.extend(page)
            if len(page) < ep.page_size:
                break
            offset += ep.page_size
    return jobs


def _namespace_filter(params: ProcessParams[Any], column_name: str = "namespace") -> list[str] | None:
    values = eq_values(params.current_pushdown_filters, column_name)
    return None if values is None else [str(v) for v in values]


# ----------------------------------------------------------------------------- namespaces / sources

NAMESPACES_SCHEMA = pa.schema(
    [
        column("name", pa.string(), "Namespace name, e.g. 'airflow-prod' or 'postgres://db:5432'.", nullable=False),
        column("owner_name", pa.string(), "Owner recorded by Marquez."),
        column("description", pa.string(), "Namespace description."),
        column("is_hidden", pa.bool_(), "Whether the namespace is hidden in the Marquez UI."),
        column("created_at", TSTZ, "When Marquez first saw the namespace."),
        column("updated_at", TSTZ, "Last update."),
    ]
)


@init_single_worker
@bind_fixed_schema
class NamespacesScan(PagedScan[NoArgs]):
    """Rows of ``marquez.namespaces``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = NAMESPACES_SCHEMA
    ITEMS_KEY: ClassVar[str] = "namespaces"

    class Meta:
        """Scan metadata."""

        name = "namespaces_scan"
        description = "Marquez namespaces (job and dataset namespaces)"
        required_secrets = list(SECRET_REQUIREMENT)
        projection_pushdown = True
        tags = object_tags(
            title="Marquez Namespaces",
            category="marquez",
            keywords="marquez, namespaces, openlineage namespace, job namespace, dataset namespace",
            doc_llm=(
                "Every Marquez namespace: name, owner_name, description, is_hidden, created_at, updated_at. Job "
                "namespaces name the scheduler (e.g. 'airflow-prod'); dataset namespaces name the storage "
                "('postgres://host:5432', 'bigquery', 's3://bucket')."
            ),
            doc_md="Every namespace in the Marquez server.",
        )
        examples = [
            FunctionExample(
                sql="SELECT name, created_at FROM openlineage.marquez.namespaces ORDER BY created_at",
                description="Namespaces in the order Marquez first saw them",
            ),
        ]

    @classmethod
    def listings(cls, ep: Endpoint, params: ProcessParams[Any]) -> list[list[Any]]:
        """One listing."""
        return [["/namespaces", {}, None]]

    @classmethod
    def build_rows(cls, ep: Endpoint, items: list[dict[str, Any]], context: Any) -> list[dict[str, Any]]:
        """One row per namespace."""
        return [
            {
                "name": n.get("name"),
                "owner_name": n.get("ownerName"),
                "description": n.get("description"),
                "is_hidden": n.get("isHidden"),
                "created_at": _ts(n.get("createdAt")),
                "updated_at": _ts(n.get("updatedAt")),
            }
            for n in items
        ]


SOURCES_SCHEMA = pa.schema(
    [
        column("name", pa.string(), "Source name (from the dataSource facet).", nullable=False),
        column("type", pa.string(), "Source type as Marquez records it."),
        column("connection_url", pa.string(), "Connection URI, e.g. 'postgres://db:5432'."),
        column("description", pa.string(), "Source description."),
        column("created_at", TSTZ, "When Marquez first saw the source."),
        column("updated_at", TSTZ, "Last update."),
    ]
)


@init_single_worker
@bind_fixed_schema
class SourcesScan(PagedScan[NoArgs]):
    """Rows of ``marquez.sources``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = SOURCES_SCHEMA
    ITEMS_KEY: ClassVar[str] = "sources"

    class Meta:
        """Scan metadata."""

        name = "sources_scan"
        description = "Marquez data sources (databases, warehouses, object stores)"
        required_secrets = list(SECRET_REQUIREMENT)
        projection_pushdown = True
        tags = object_tags(
            title="Marquez Sources",
            category="marquez",
            keywords="marquez, data sources, connection url, databases, warehouses, dataSource facet",
            doc_llm=(
                "Every Marquez source (built from datasets' dataSource facets): name, type, connection_url, "
                "description, created_at, updated_at. Join to datasets on datasets.source_name = sources.name."
            ),
            doc_md="Data sources known to Marquez.",
        )
        examples = [
            FunctionExample(
                sql="SELECT name, connection_url FROM openlineage.marquez.sources ORDER BY name",
                description="Every source and its connection URI",
            ),
        ]

    @classmethod
    def listings(cls, ep: Endpoint, params: ProcessParams[Any]) -> list[list[Any]]:
        """One listing."""
        return [["/sources", {}, None]]

    @classmethod
    def build_rows(cls, ep: Endpoint, items: list[dict[str, Any]], context: Any) -> list[dict[str, Any]]:
        """One row per source."""
        return [
            {
                "name": s.get("name"),
                "type": s.get("type"),
                "connection_url": s.get("connectionUrl"),
                "description": s.get("description"),
                "created_at": _ts(s.get("createdAt")),
                "updated_at": _ts(s.get("updatedAt")),
            }
            for s in items
        ]


# ----------------------------------------------------------------------------- datasets

DATASET_COLUMNS = [
    column("namespace", pa.string(), "Dataset namespace.", nullable=False),
    column("name", pa.string(), "Dataset name.", nullable=False),
    column("type", pa.string(), "DB_TABLE or STREAM."),
    column("physical_name", pa.string(), "Name of the dataset in its storage system (usually equal to name)."),
    column("source_name", pa.string(), "Source (sources.name)."),
    column("description", pa.string(), "Dataset description."),
    column("fields", FIELDS, "Columns as `LIST(STRUCT(name, type, description, tags))`."),
    column("tags", STR_LIST, "Dataset tags."),
    column("created_at", TSTZ, "When Marquez first saw the dataset."),
    column("updated_at", TSTZ, "Last update."),
    column("last_modified_at", TSTZ, "Last time a run wrote the dataset."),
    column("last_lifecycle_state", pa.string(), "Latest lifecycleStateChange (CREATE, OVERWRITE, DROP, ...)."),
    column("current_version", pa.string(), "Current dataset version UUID."),
    column("deleted", pa.bool_(), "Whether the dataset is soft-deleted."),
    column("column_lineage", JSON, "Marquez's column lineage for the dataset (JSON)."),
    column("facets", JSON, "Dataset facets (JSON)."),
]
DATASETS_SCHEMA = pa.schema(DATASET_COLUMNS)


def dataset_row(d: dict[str, Any]) -> dict[str, Any]:
    """One ``datasets`` row from a Marquez dataset object."""
    ident = d.get("id") or {}
    return {
        "namespace": d.get("namespace") or ident.get("namespace"),
        "name": d.get("name") or ident.get("name"),
        "type": d.get("type"),
        "physical_name": d.get("physicalName"),
        "source_name": d.get("sourceName"),
        "description": d.get("description"),
        "fields": _fields(d.get("fields")),
        "tags": _tag_names(d.get("tags")),
        "created_at": _ts(d.get("createdAt")),
        "updated_at": _ts(d.get("updatedAt")),
        "last_modified_at": _ts(d.get("lastModifiedAt")),
        "last_lifecycle_state": d.get("lastLifecycleState") or None,
        "current_version": d.get("currentVersion"),
        "deleted": d.get("deleted"),
        "column_lineage": ev.json_text(d.get("columnLineage")),
        "facets": ev.json_text(_clean(d.get("facets"))),
    }


@init_single_worker
@bind_fixed_schema
class DatasetsScan(PagedScan[NoArgs]):
    """Rows of ``marquez.datasets``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = DATASETS_SCHEMA
    ITEMS_KEY: ClassVar[str] = "datasets"

    class Meta:
        """Scan metadata."""

        name = "datasets_scan"
        description = "Every Marquez dataset with its fields, tags and facets"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = object_tags(
            title="Marquez Datasets",
            category="marquez",
            keywords="marquez, datasets, tables, columns, schema, data catalog, lineage metadata",
            doc_llm=(
                "Every dataset in every namespace: namespace, name, type, physical_name, source_name, description, "
                "fields (`LIST(STRUCT(name, type, description, tags))`; UNNEST it for one row per column), tags, "
                "created_at, updated_at, last_modified_at, last_lifecycle_state, current_version, deleted, "
                "column_lineage JSON and facets JSON. `namespace = ...` / `IN (...)` pushes down to one listing "
                "per namespace."
            ),
            doc_md="Datasets across namespaces, with columns as a list of structs.",
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT namespace, name, len(fields) AS columns FROM openlineage.marquez.datasets "
                    "WHERE namespace = 'bigquery' ORDER BY name"
                ),
                description="Datasets in one namespace with their column counts",
            ),
        ]

    @classmethod
    def listings(cls, ep: Endpoint, params: ProcessParams[Any]) -> list[list[Any]]:
        """One listing per (pushed-down or every) namespace."""
        namespaces = _namespace_filter(params)
        if namespaces is None:
            namespaces = all_namespaces(ep)
        return [[f"/namespaces/{quote(ns)}/datasets", {}, None] for ns in namespaces]

    @classmethod
    def build_rows(cls, ep: Endpoint, items: list[dict[str, Any]], context: Any) -> list[dict[str, Any]]:
        """One row per dataset."""
        return [dataset_row(d) for d in items]


# ----------------------------------------------------------------------------- jobs

JOBS_SCHEMA = pa.schema(
    [
        column("namespace", pa.string(), "Job namespace.", nullable=False),
        column("name", pa.string(), "Job name; Marquez prefixes child jobs with the parent job name.", nullable=False),
        column("simple_name", pa.string(), "Name without the parent prefix."),
        column("parent_job_name", pa.string(), "Parent job (from the parent run facet)."),
        column("type", pa.string(), "BATCH, STREAM or SERVICE."),
        column("description", pa.string(), "Job description."),
        column("location", pa.string(), "Source-code location URL."),
        column("created_at", TSTZ, "When Marquez first saw the job."),
        column("updated_at", TSTZ, "Last update."),
        column("current_version", pa.string(), "Current job version UUID."),
        column("inputs", DATASET_REFS, "Input datasets of the current version."),
        column("outputs", DATASET_REFS, "Output datasets of the current version."),
        column("latest_run_id", pa.string(), "Most recent run id."),
        column("latest_run_state", pa.string(), "NEW, RUNNING, COMPLETED, FAILED or ABORTED."),
        column("latest_run_ended_at", TSTZ, "When the latest run ended."),
        column("labels", STR_LIST, "Job labels."),
        column("tags", STR_LIST, "Job tags."),
        column("facets", JSON, "Job facets (JSON)."),
    ]
)


def job_row(j: dict[str, Any]) -> dict[str, Any]:
    """One ``jobs`` row from a Marquez job object."""
    ident = j.get("id") or {}
    latest = j.get("latestRun") or {}
    return {
        "namespace": j.get("namespace") or ident.get("namespace"),
        "name": j.get("name") or ident.get("name"),
        "simple_name": j.get("simpleName"),
        "parent_job_name": j.get("parentJobName"),
        "type": j.get("type"),
        "description": j.get("description"),
        "location": j.get("location"),
        "created_at": _ts(j.get("createdAt")),
        "updated_at": _ts(j.get("updatedAt")),
        "current_version": j.get("currentVersion"),
        "inputs": _refs(j.get("inputs")),
        "outputs": _refs(j.get("outputs")),
        "latest_run_id": latest.get("id"),
        "latest_run_state": latest.get("state"),
        "latest_run_ended_at": _ts(latest.get("endedAt")),
        "labels": [str(x) for x in j.get("labels") or []],
        "tags": _tag_names(j.get("tags")),
        "facets": ev.json_text(_clean(j.get("facets"))),
    }


@init_single_worker
@bind_fixed_schema
class JobsScan(PagedScan[NoArgs]):
    """Rows of ``marquez.jobs``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = JOBS_SCHEMA
    ITEMS_KEY: ClassVar[str] = "jobs"

    class Meta:
        """Scan metadata."""

        name = "jobs_scan"
        description = "Every Marquez job with its inputs, outputs and latest run"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = object_tags(
            title="Marquez Jobs",
            category="marquez",
            keywords="marquez, jobs, pipelines, latest run, job inputs, job outputs, airflow tasks",
            doc_llm=(
                "Every job: namespace, name (child jobs are prefixed with the parent, e.g. 'etl_orders.extract'), "
                "simple_name, parent_job_name, type, description, location, created_at, updated_at, "
                "current_version, inputs/outputs (`LIST(STRUCT(namespace, name))`), latest_run_id, "
                "latest_run_state (NEW/RUNNING/COMPLETED/FAILED/ABORTED), latest_run_ended_at, labels, tags, facets "
                "JSON. `namespace = ...` pushes down."
            ),
            doc_md="Jobs across namespaces with their latest run state.",
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT namespace, name, latest_run_state FROM openlineage.marquez.jobs "
                    "WHERE latest_run_state = 'FAILED' ORDER BY namespace, name"
                ),
                description="Jobs whose latest run failed",
            ),
        ]

    @classmethod
    def listings(cls, ep: Endpoint, params: ProcessParams[Any]) -> list[list[Any]]:
        """``/jobs`` or one listing per pushed-down namespace."""
        namespaces = _namespace_filter(params)
        if namespaces is None:
            return [["/jobs", {}, None]]
        return [[f"/namespaces/{quote(ns)}/jobs", {}, None] for ns in namespaces]

    @classmethod
    def build_rows(cls, ep: Endpoint, items: list[dict[str, Any]], context: Any) -> list[dict[str, Any]]:
        """One row per job."""
        return [job_row(j) for j in items]


# ----------------------------------------------------------------------------- runs

RUNS_SCHEMA = pa.schema(
    [
        column("run_id", pa.string(), "Run id (OpenLineage runId).", nullable=False),
        column("namespace", pa.string(), "Job namespace.", nullable=False),
        column("job_name", pa.string(), "Marquez job name (child jobs carry the parent prefix).", nullable=False),
        column("state", pa.string(), "NEW, RUNNING, COMPLETED, FAILED or ABORTED."),
        column("created_at", TSTZ, "When the run was created."),
        column("updated_at", TSTZ, "Last update."),
        column("started_at", TSTZ, "When the run started."),
        column("ended_at", TSTZ, "When the run ended."),
        column("duration_ms", pa.int64(), "Run duration in milliseconds."),
        column("nominal_start_time", TSTZ, "Scheduled start (nominalTime facet)."),
        column("nominal_end_time", TSTZ, "Scheduled end."),
        column("job_version", pa.string(), "Job version UUID the run executed."),
        column("parent_run_id", pa.string(), "Parent runId (parent run facet)."),
        column("parent_job_name", pa.string(), "Job of the parent run, from the parent run facet."),
        column("root_run_id", pa.string(), "Root runId (parent facet's root)."),
        column("orchestrator", pa.string(), "airflow, dagster, dbt, spark, ... or 'unknown'.", nullable=False),
        column("error_message", pa.string(), "errorMessage facet message."),
        column("input_versions", DATASET_VERSIONS, "Input dataset versions as STRUCT(namespace, name, version)."),
        column("output_versions", DATASET_VERSIONS, "Output dataset versions."),
        column("args", JSON, "Run args (JSON)."),
        column("facets", JSON, "Run facets (JSON)."),
    ]
)


def run_row(ep: Endpoint, r: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    """One ``runs`` row from a Marquez run object and its job context."""
    facets = _clean(r.get("facets"))
    parent = facets.get("parent") or {}
    nominal = r.get("nominalStartTime"), r.get("nominalEndTime")
    duration = r.get("durationMs")
    return {
        "run_id": r.get("id"),
        "namespace": context["namespace"],
        "job_name": context["name"],
        "state": r.get("state"),
        "created_at": _ts(r.get("createdAt")),
        "updated_at": _ts(r.get("updatedAt")),
        "started_at": _ts(r.get("startedAt")),
        "ended_at": _ts(r.get("endedAt")),
        "duration_ms": int(duration) if isinstance(duration, int | float) else None,
        "nominal_start_time": _ts(nominal[0]),
        "nominal_end_time": _ts(nominal[1]),
        "job_version": (r.get("jobVersion") or {}).get("version"),
        "parent_run_id": (parent.get("run") or {}).get("runId"),
        "parent_job_name": (parent.get("job") or {}).get("name"),
        "root_run_id": ((parent.get("root") or {}).get("run") or {}).get("runId"),
        "orchestrator": ev.detect_orchestrator(None, facets, context.get("job_facets") or {}),
        "error_message": (facets.get("errorMessage") or {}).get("message"),
        "input_versions": _versions(r.get("inputDatasetVersions")),
        "output_versions": _versions(r.get("outputDatasetVersions")),
        "args": ev.json_text(r.get("args")),
        "facets": ev.json_text(facets),
        "_facets": facets,
        "_url": f"{ep.api_url}/jobs/runs/{r.get('id')}",
    }


class RunsPager(PagedScan[NoArgs]):
    """Pages through every run of every (pushed-down) job."""

    ITEMS_KEY: ClassVar[str] = "runs"

    @classmethod
    def listings(cls, ep: Endpoint, params: ProcessParams[Any]) -> list[list[Any]]:
        """One listing per job; ``namespace`` / ``job_name`` filters narrow the jobs."""
        ns_filter = _namespace_filter(params, cls.NAMESPACE_COLUMN)
        job_filter = eq_values(params.current_pushdown_filters, cls.JOB_COLUMN) if cls.JOB_COLUMN else None
        jobs = all_jobs(ep, ns_filter)
        out = []
        for j in jobs:
            ns, name = j.get("namespace"), j.get("name")
            if not ns or not name or (job_filter is not None and name not in job_filter):
                continue
            context = {"namespace": ns, "name": name, "job_facets": {"jobType": _clean(j.get("facets")).get("jobType")}}
            if context["job_facets"]["jobType"] is None:
                context["job_facets"] = {}
            out.append([f"/namespaces/{quote(ns)}/jobs/{quote(name)}/runs", {}, context])
        return out

    NAMESPACE_COLUMN: ClassVar[str] = "namespace"
    JOB_COLUMN: ClassVar[str | None] = "job_name"


@init_single_worker
@bind_fixed_schema
class RunsScan(RunsPager):
    """Rows of ``marquez.runs``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = RUNS_SCHEMA

    class Meta:
        """Scan metadata."""

        name = "runs_scan"
        description = "Every run of every Marquez job, with state, timing, parent and dataset versions"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = object_tags(
            title="Marquez Runs",
            category="marquez",
            keywords="marquez, runs, job runs, failed runs, run state, duration, parent run, dataset versions",
            doc_llm=(
                "Every run of every job (newest first per job): run_id, namespace, job_name, state "
                "(NEW/RUNNING/COMPLETED/FAILED/ABORTED), created_at, updated_at, started_at, ended_at, duration_ms, "
                "nominal_start_time, nominal_end_time, job_version, parent_run_id, parent_job_name, root_run_id, "
                "orchestrator, error_message (errorMessage facet), input_versions/output_versions "
                "(`LIST(STRUCT(namespace, name, version))`), args `JSON`, facets `JSON`. "
                "Filter on namespace and job_name to "
                "push down (otherwise every job is listed, then its runs)."
            ),
            doc_md="Runs of every job; filter on `namespace` / `job_name` to read fewer jobs.",
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT job_name, state, duration_ms FROM openlineage.marquez.runs "
                    "WHERE namespace = 'airflow-prod' AND state = 'FAILED' ORDER BY ended_at DESC"
                ),
                description="Failed runs in one namespace",
            ),
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, items: list[dict[str, Any]], context: Any) -> list[dict[str, Any]]:
        """One row per run."""
        return [run_row(ep, r, context) for r in items]


# ----------------------------------------------------------------------------- unified


def _unified(ep: Endpoint, items: list[dict[str, Any]], context: Any) -> tuple[list[Any], list[Any]]:
    runs: list[dict[str, Any]] = []
    tasks: list[dict[str, Any]] = []
    for r in items:
        row = run_row(ep, r, context)
        facets = row["_facets"]
        base = {
            "orchestrator": row["orchestrator"],
            "status": RUN_STATE.get(str(row["state"]), "queued"),
            "raw_status": row["state"],
            "started_at": row["started_at"],
            "ended_at": row["ended_at"],
            "duration_seconds": row["duration_ms"] / 1000.0 if row["duration_ms"] is not None else None,
            "url": row["_url"],
        }
        if row["parent_run_id"]:
            tasks.append(
                {
                    **base,
                    "run_id": row["root_run_id"] or row["parent_run_id"],
                    "task_run_id": row["run_id"],
                    "task_name": row["job_name"],
                    "attempt": ev.airflow_attempt(facets),
                }
            )
        else:
            runs.append(
                {
                    **base,
                    "run_id": row["run_id"],
                    "pipeline": row["job_name"],
                    "trigger": ev.trigger_of(facets),
                    "queued_at": row["created_at"],
                }
            )
    return runs, tasks


@init_single_worker
@bind_fixed_schema
class UnifiedRunsScan(RunsPager):
    """Rows of ``unified.runs``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = UNIFIED_RUNS_SCHEMA
    JOB_COLUMN: ClassVar[str | None] = "pipeline"
    NAMESPACE_COLUMN: ClassVar[str] = "_none"

    class Meta:
        """Scan metadata."""

        name = "unified_runs_scan"
        description = "Marquez top-level runs in the shared cross-orchestrator shape"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = object_tags(
            title="Unified Runs",
            category="unified",
            keywords="unified, cross orchestrator, union all, airflow, dagster, dbt, pipeline runs, run status",
            doc_llm=(
                "Marquez runs without a parent facet in the column layout shared by every Query.Farm orchestrator "
                "worker, so `UNION ALL` with airflow/dagster/prefect workers works: orchestrator, run_id, pipeline "
                "(job name), status (queued/running/success/failed/cancelled/skipped), raw_status (Marquez "
                "RunState), trigger (Airflow run_type, else NULL), queued_at (created), started_at, ended_at, "
                "duration_seconds, url (Marquez API link). `pipeline = ...` pushes down."
            ),
            doc_md="Top-level runs in the shared orchestrator shape.",
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT orchestrator, status, count(*) AS runs FROM "
                    "openlineage.unified.runs GROUP BY ALL ORDER BY ALL"
                ),
                description="Run counts by orchestrator and status",
            ),
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, items: list[dict[str, Any]], context: Any) -> list[dict[str, Any]]:
        """Top-level runs only."""
        return _unified(ep, items, context)[0]


@init_single_worker
@bind_fixed_schema
class UnifiedTaskRunsScan(RunsPager):
    """Rows of ``unified.task_runs``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = UNIFIED_TASK_RUNS_SCHEMA
    JOB_COLUMN: ClassVar[str | None] = "task_name"
    NAMESPACE_COLUMN: ClassVar[str] = "_none"

    class Meta:
        """Scan metadata."""

        name = "unified_task_runs_scan"
        description = "Marquez task runs (runs with a parent facet) in the shared cross-orchestrator shape"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = object_tags(
            title="Unified Task Runs",
            category="unified",
            keywords="unified, task runs, airflow tasks, dbt models, spark jobs, union all, task status",
            doc_llm=(
                "Marquez runs that carry a parent facet (Airflow tasks, dbt models, Spark jobs launched by a task) "
                "in the shared task-run layout: orchestrator, run_id (root run, else parent run), task_run_id, "
                "task_name (Marquez job name), status, raw_status, attempt (Airflow try_number), started_at, "
                "ended_at, duration_seconds, url. Join to openlineage.unified.runs on run_id. `task_name = ...` "
                "pushes down."
            ),
            doc_md="Task runs keyed to their root run, in the shared orchestrator shape.",
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT t.task_name, t.status, r.pipeline FROM openlineage.unified.task_runs t "
                    "JOIN openlineage.unified.runs r USING (run_id) WHERE t.status = 'failed' ORDER BY t.task_name"
                ),
                description="Failed task runs with their pipeline",
            ),
        ]

    @classmethod
    def build_rows(cls, ep: Endpoint, items: list[dict[str, Any]], context: Any) -> list[dict[str, Any]]:
        """Task runs only."""
        return _unified(ep, items, context)[1]


# ----------------------------------------------------------------------------- lineage events


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        moment: datetime = value if value.tzinfo else value.replace(tzinfo=UTC)
        return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return str(value)


@init_single_worker
@bind_fixed_schema
class LineageEventsScan(PagedScan[NoArgs]):
    """Rows of ``marquez.lineage_events``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = EVENTS_SCHEMA
    ITEMS_KEY: ClassVar[str] = "events"

    class Meta:
        """Scan metadata."""

        name = "lineage_events_scan"
        description = "Raw OpenLineage events stored in Marquez, in the read_events layout"
        required_secrets = list(SECRET_REQUIREMENT)
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = object_tags(
            title="Marquez Lineage Events",
            category="marquez",
            keywords="marquez, lineage events, openlineage events, run events, event log, audit",
            doc_llm=(
                "The raw OpenLineage events Marquez received (GET /api/v1/events/lineage, oldest first), with "
                "exactly the columns of openlineage.main.read_events: event_kind, event_type, event_time, "
                "producer, run_id, job_namespace, job_name, parent_*, orchestrator, inputs, outputs, run_facets, "
                "job_facets, event (JSON); `source` is 'marquez'. Range filters on event_time push down to "
                "Marquez's after/before parameters - always filter on event_time for large servers."
            ),
            doc_md="Raw events from Marquez; `event_time` ranges push down.",
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT job_name, event_type, event_time FROM openlineage.marquez.lineage_events "
                    "WHERE event_time >= TIMESTAMPTZ '2026-11-03 00:00:00+00' AND event_type = 'FAIL' "
                    "ORDER BY event_time"
                ),
                description="FAIL events since a point in time",
            ),
        ]

    @classmethod
    def listings(cls, ep: Endpoint, params: ProcessParams[Any]) -> list[list[Any]]:
        """One listing with the pushed-down time window."""
        lower, upper = range_bounds(params.current_pushdown_filters, "event_time")
        query = {"sortDirection": "asc", "after": _iso(lower), "before": _iso(upper)}
        return [["/events/lineage", {k: v for k, v in query.items() if v is not None}, None]]

    @classmethod
    def build_rows(cls, ep: Endpoint, items: list[dict[str, Any]], context: Any) -> list[dict[str, Any]]:
        """One row per event (``event_index`` is not meaningful here and is NULL)."""
        rows = []
        for e in items:
            row = ev.event_row(ev.LoadedEvent(source="marquez", index=0, event=e))
            row["event_index"] = None
            rows.append(row)
        return rows


# ----------------------------------------------------------------------------- versions


@dataclass(slots=True, frozen=True, kw_only=True)
class NameArgs:
    """``fn(namespace, name)``."""

    namespace: Annotated[str, Arg(0, doc="Namespace, e.g. 'bigquery' or 'airflow-prod'.")]
    name: Annotated[str, Arg(1, doc="Dataset or job name (full Marquez name for child jobs).")]


DATASET_VERSIONS_SCHEMA = pa.schema(
    [
        column("namespace", pa.string(), "Dataset namespace.", nullable=False),
        column("name", pa.string(), "Dataset name.", nullable=False),
        column("version", pa.string(), "Dataset version UUID.", nullable=False),
        column("created_at", TSTZ, "When the version was created."),
        column("type", pa.string(), "DB_TABLE or STREAM."),
        column("source_name", pa.string(), "Source name."),
        column("fields", FIELDS, "Columns of this version."),
        column("schema_version", pa.string(), "currentSchemaVersion UUID."),
        column("lifecycle_state", pa.string(), "lifecycleStateChange of this version."),
        column("created_by_run_id", pa.string(), "Run that produced this version."),
        column("created_by_run_state", pa.string(), "State of that run."),
        column("facets", JSON, "Dataset facets of this version (JSON)."),
    ]
)


@init_single_worker
@bind_fixed_schema
class DatasetVersions(PagedScan[NameArgs]):
    """``dataset_versions(namespace, name)``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = DATASET_VERSIONS_SCHEMA
    ITEMS_KEY: ClassVar[str] = "versions"

    class Meta:
        """Function metadata."""

        name = "dataset_versions"
        description = "Every version of one Marquez dataset, newest first, with the run that created it"
        required_secrets = list(SECRET_REQUIREMENT)
        projection_pushdown = True
        tags = {
            **object_tags(
                title="Dataset Versions",
                category="marquez",
                keywords="marquez, dataset versions, schema history, schema evolution, created by run",
                doc_llm=(
                    "Versions of dataset (`namespace`, `name`), newest first: version, created_at, type, "
                    "source_name, fields (`LIST(STRUCT(name, type, description, tags))`), schema_version, "
                    "lifecycle_state, created_by_run_id, created_by_run_state, facets JSON. Compare `fields` "
                    "across versions to see schema changes."
                ),
                doc_md="## dataset_versions(namespace, name)\n\nVersion history of one dataset.",
            ),
            "vgi.result_columns_schema": result_columns(DATASET_VERSIONS_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT version, created_at, len(fields) AS columns, created_by_run_id "
                    "FROM openlineage.marquez.dataset_versions('bigquery', 'analytics.customers') ORDER BY created_at"
                ),
                description="Version history of a table",
            ),
        ]

    @classmethod
    def listings(cls, ep: Endpoint, params: ProcessParams[Any]) -> list[list[Any]]:
        """One listing."""
        a = params.args
        return [[f"/namespaces/{quote(a.namespace)}/datasets/{quote(a.name)}/versions", {}, None]]

    @classmethod
    def build_rows(cls, ep: Endpoint, items: list[dict[str, Any]], context: Any) -> list[dict[str, Any]]:
        """One row per version."""
        rows = []
        for v in items:
            ident = v.get("id") or {}
            run = v.get("createdByRun") or {}
            rows.append(
                {
                    "namespace": v.get("namespace") or ident.get("namespace"),
                    "name": v.get("name") or ident.get("name"),
                    "version": v.get("version"),
                    "created_at": _ts(v.get("createdAt")),
                    "type": v.get("type"),
                    "source_name": v.get("sourceName"),
                    "fields": _fields(v.get("fields")),
                    "schema_version": v.get("currentSchemaVersion"),
                    "lifecycle_state": v.get("lifecycleState") or None,
                    "created_by_run_id": run.get("id"),
                    "created_by_run_state": run.get("state"),
                    "facets": ev.json_text(_clean(v.get("facets"))),
                }
            )
        return rows


JOB_VERSIONS_SCHEMA = pa.schema(
    [
        column("namespace", pa.string(), "Job namespace.", nullable=False),
        column("name", pa.string(), "Job name.", nullable=False),
        column("version", pa.string(), "Job version UUID.", nullable=False),
        column("created_at", TSTZ, "When the version was created."),
        column("location", pa.string(), "Source-code location."),
        column("inputs", DATASET_REFS, "Inputs of this version."),
        column("outputs", DATASET_REFS, "Outputs of this version."),
        column("latest_run_id", pa.string(), "Latest run of this version."),
        column("latest_run_state", pa.string(), "Its state."),
    ]
)


@init_single_worker
@bind_fixed_schema
class JobVersions(PagedScan[NameArgs]):
    """``job_versions(namespace, name)``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = JOB_VERSIONS_SCHEMA
    ITEMS_KEY: ClassVar[str] = "versions"

    class Meta:
        """Function metadata."""

        name = "job_versions"
        description = "Every version of one Marquez job with its inputs, outputs and latest run"
        required_secrets = list(SECRET_REQUIREMENT)
        projection_pushdown = True
        tags = {
            **object_tags(
                title="Job Versions",
                category="marquez",
                keywords="marquez, job versions, job history, inputs, outputs, code changes",
                doc_llm=(
                    "Versions of job (`namespace`, `name`; use the full Marquez name for child jobs, e.g. "
                    "'etl_orders.extract'), newest first: version, created_at, location, inputs/outputs "
                    "(`LIST(STRUCT(namespace, name))`), latest_run_id, latest_run_state. "
                    "A new version appears when the "
                    "job's inputs, outputs or source location change."
                ),
                doc_md="## job_versions(namespace, name)\n\nVersion history of one job.",
            ),
            "vgi.result_columns_schema": result_columns(JOB_VERSIONS_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT version, created_at, len(inputs) AS inputs, latest_run_state "
                    "FROM openlineage.marquez.job_versions('airflow-prod', 'etl_orders.transform') ORDER BY created_at"
                ),
                description="Version history of a job",
            ),
        ]

    @classmethod
    def listings(cls, ep: Endpoint, params: ProcessParams[Any]) -> list[list[Any]]:
        """One listing."""
        a = params.args
        return [[f"/namespaces/{quote(a.namespace)}/jobs/{quote(a.name)}/versions", {}, None]]

    @classmethod
    def build_rows(cls, ep: Endpoint, items: list[dict[str, Any]], context: Any) -> list[dict[str, Any]]:
        """One row per version."""
        rows = []
        for v in items:
            ident = v.get("id") or {}
            latest = v.get("latestRun") or {}
            rows.append(
                {
                    "namespace": v.get("namespace") or ident.get("namespace"),
                    "name": v.get("name") or ident.get("name"),
                    "version": v.get("version"),
                    "created_at": _ts(v.get("createdAt")),
                    "location": v.get("location"),
                    "inputs": _refs(v.get("inputs")),
                    "outputs": _refs(v.get("outputs")),
                    "latest_run_id": latest.get("id"),
                    "latest_run_state": latest.get("state"),
                }
            )
        return rows


# ----------------------------------------------------------------------------- single-call functions


class SingleCall[A](TableFunctionGenerator[A, None]):
    """One request, every row in one batch."""

    FIXED_SCHEMA: ClassVar[pa.Schema]

    @classmethod
    def fetch_rows(cls, ep: Endpoint, args: Any) -> list[dict[str, Any]]:
        """All rows."""
        raise NotImplementedError

    @classmethod
    def process(cls, params: ProcessParams[Any], state: None, out: OutputCollector) -> None:
        """Fetch and emit once."""
        out.emit(rows_to_batch(cls.fetch_rows(endpoint(params.secrets), params.args), params.output_schema))
        out.finish()


@dataclass(slots=True, frozen=True, kw_only=True)
class RunFacetArgs:
    """``run_facets(run_id)``."""

    run_id: Annotated[str, Arg(0, doc="The OpenLineage runId of the run, as shown in runs.run_id.")]


RUN_FACETS_SCHEMA = pa.schema(
    [
        column("run_id", pa.string(), "Run id.", nullable=False),
        column(
            "facet_type", pa.string(), "'run' (run facets) or 'job' (job facets recorded with the run).", nullable=False
        ),
        column(
            "facet_name", pa.string(), "Facet key, e.g. 'parent', 'errorMessage', 'processing_engine'.", nullable=False
        ),
        column("facet_producer", pa.string(), "The facet's _producer."),
        column("facet_schema_url", pa.string(), "The facet's _schemaURL."),
        column("facet", JSON, "The facet body (JSON).", nullable=False),
    ]
)


@init_single_worker
@bind_fixed_schema
class RunFacets(SingleCall[RunFacetArgs]):
    """``run_facets(run_id)``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = RUN_FACETS_SCHEMA

    class Meta:
        """Function metadata."""

        name = "run_facets"
        description = "The run and job facets Marquez recorded for one run, one JSON row per facet"
        required_secrets = list(SECRET_REQUIREMENT)
        projection_pushdown = True
        tags = {
            **object_tags(
                title="Run Facets",
                category="marquez",
                keywords="marquez, run facets, job facets, errorMessage, processing_engine, spark properties",
                doc_llm=(
                    "Facets of run `run_id` from GET /api/v1/jobs/runs/{id}/facets?type=run and ?type=job: run_id, "
                    "facet_type ('run'/'job'), facet_name, facet_producer, facet_schema_url, facet JSON. Use ->> to "
                    "extract, e.g. facet->>'$.message' for errorMessage."
                ),
                doc_md="## run_facets(run_id)\n\nEvery facet of one run as JSON.",
            ),
            "vgi.result_columns_schema": result_columns(RUN_FACETS_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT facet_name, facet->>'$.message' AS message "
                    "FROM openlineage.marquez.run_facets('01930a6e-0000-7000-8000-000000000005') "
                    "WHERE facet_type = 'run' ORDER BY facet_name"
                ),
                description="Run facets of one run, with the error message if it failed",
            ),
        ]

    @classmethod
    def fetch_rows(cls, ep: Endpoint, args: Any) -> list[dict[str, Any]]:
        """Run facets then job facets."""
        rows = []
        for facet_type in ("run", "job"):
            body = get_json(ep, f"/jobs/runs/{quote(args.run_id)}/facets", {"type": facet_type}, missing_ok=True)
            for name, facet in sorted(_clean((body or {}).get("facets")).items()):
                rows.append(
                    {
                        "run_id": args.run_id,
                        "facet_type": facet_type,
                        "facet_name": name,
                        "facet_producer": facet.get("_producer"),
                        "facet_schema_url": facet.get("_schemaURL"),
                        "facet": ev.json_text(facet) or "{}",
                    }
                )
        return rows


@dataclass(slots=True, frozen=True, kw_only=True)
class GraphArgs:
    """``lineage_graph(node_id, depth := 20)``."""

    node_id: Annotated[
        str,
        Arg(0, doc="Start node: 'dataset:<namespace>:<name>' or 'job:<namespace>:<name>'.", pattern=r"^(dataset|job):"),
    ]
    depth: Annotated[int, Arg("depth", default=20, doc="Marquez traversal depth; 1 to 100.", ge=1, le=100)]


LINEAGE_GRAPH_SCHEMA = pa.schema(
    [
        column("node_id", pa.string(), "'dataset:<ns>:<name>' or 'job:<ns>:<name>'.", nullable=False),
        column("node_type", pa.string(), "DATASET or JOB.", nullable=False),
        column("namespace", pa.string(), "Node namespace."),
        column("name", pa.string(), "Node name."),
        column("direction", pa.string(), "'start', 'upstream', 'downstream' or NULL (reachable only sideways)."),
        column("hops", pa.int32(), "Edges from the start node along that direction."),
        column("in_edges", STR_LIST, "node_ids with an edge into this node."),
        column("out_edges", STR_LIST, "node_ids this node has an edge to."),
        column("data", JSON, "Marquez's dataset/job object for the node (JSON)."),
    ]
)


def graph_rows(graph: list[dict[str, Any]], start: str) -> list[dict[str, Any]]:
    """Rows for a Marquez lineage graph, with BFS hops from ``start`` both ways."""
    ins = {n["id"]: [e.get("origin") for e in n.get("inEdges") or []] for n in graph}
    outs = {n["id"]: [e.get("destination") for e in n.get("outEdges") or []] for n in graph}
    placed: dict[str, tuple[str, int]] = {start: ("start", 0)}
    for label, adj in (("upstream", ins), ("downstream", outs)):
        queue: deque[tuple[str, int]] = deque([(start, 0)])
        seen = {start}
        while queue:
            cur, hops = queue.popleft()
            for nxt in adj.get(cur, []):
                if nxt and nxt not in seen:
                    seen.add(nxt)
                    placed.setdefault(nxt, (label, hops + 1))
                    queue.append((nxt, hops + 1))
    rows = []
    for n in graph:
        data = n.get("data") or {}
        ident = data.get("id") or {}
        place = placed.get(n["id"])
        direction, distance = place if place else (None, None)
        rows.append(
            {
                "node_id": n["id"],
                "node_type": n.get("type"),
                "namespace": data.get("namespace") or ident.get("namespace"),
                "name": data.get("name") or ident.get("name"),
                "direction": direction,
                "hops": distance,
                "in_edges": [x for x in ins[n["id"]] if x],
                "out_edges": [x for x in outs[n["id"]] if x],
                "data": ev.json_text(data),
            }
        )
    rows.sort(key=lambda r: (r["hops"] if r["hops"] is not None else 1 << 30, r["node_id"]))
    return rows


@init_single_worker
@bind_fixed_schema
class LineageGraph(SingleCall[GraphArgs]):
    """``lineage_graph(node_id, depth := 20)``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = LINEAGE_GRAPH_SCHEMA

    class Meta:
        """Function metadata."""

        name = "lineage_graph"
        description = "The Marquez lineage graph around one dataset or job, one row per node with hop counts"
        required_secrets = list(SECRET_REQUIREMENT)
        projection_pushdown = True
        tags = {
            **object_tags(
                title="Marquez Lineage Graph",
                category="lineage",
                keywords="marquez, lineage graph, upstream, downstream, impact analysis, dependencies",
                doc_llm=(
                    "GET /api/v1/lineage?nodeId=...&depth=...: every node Marquez returns around `node_id` "
                    "('dataset:<namespace>:<name>' or 'job:<namespace>:<name>'), named arg depth := 20. Columns: "
                    "node_id, node_type (DATASET/JOB), namespace, name, direction ('start'/'upstream'/'downstream', "
                    "computed by walking the edges from the start; NULL for nodes reached only sideways), hops "
                    "(edges from the start), in_edges, out_edges (`LIST` of node_ids) and data (the node's Marquez "
                    "object as JSON)."
                ),
                doc_md="## lineage_graph(node_id, depth := 20)\n\nThe lineage graph around a node, with hop counts.",
            ),
            "vgi.result_columns_schema": result_columns(LINEAGE_GRAPH_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT direction, hops, node_id FROM openlineage.marquez.lineage_graph("
                    "'dataset:bigquery:analytics.orders_daily') WHERE node_type = 'DATASET' ORDER BY direction, hops"
                ),
                description="Datasets upstream and downstream of one table",
            ),
        ]

    @classmethod
    def fetch_rows(cls, ep: Endpoint, args: Any) -> list[dict[str, Any]]:
        """Fetch and flatten the graph."""
        body = get_json(ep, "/lineage", {"nodeId": args.node_id, "depth": args.depth}) or {}
        graph = [n for n in body.get("graph") or [] if isinstance(n, dict) and n.get("id")]
        return graph_rows(graph, args.node_id)


@dataclass(slots=True, frozen=True, kw_only=True)
class ColumnLineageArgs:
    """``column_lineage(node_id, depth := 20, with_downstream := false)``."""

    node_id: Annotated[
        str,
        Arg(
            0,
            doc="'dataset:<namespace>:<name>' (every column) or 'datasetField:<namespace>:<dataset>:<field>'.",
            pattern=r"^(dataset|datasetField):",
        ),
    ]
    depth: Annotated[int, Arg("depth", default=20, doc="Traversal depth; 1 to 100.", ge=1, le=100)]
    with_downstream: Annotated[
        bool, Arg("with_downstream", default=False, doc="Also return downstream columns (default: upstream only).")
    ]


MARQUEZ_COLUMN_LINEAGE_SCHEMA = pa.schema(
    [
        column("node_id", pa.string(), "'datasetField:<ns>:<dataset>:<field>' of the output column.", nullable=False),
        column("output_namespace", pa.string(), "Namespace of the output dataset."),
        column("output_dataset", pa.string(), "Output dataset."),
        column("output_field", pa.string(), "Output column."),
        column("output_field_type", pa.string(), "Type of the output column."),
        column("input_namespace", pa.string(), "Namespace of the input dataset (NULL: no inputs)."),
        column("input_dataset", pa.string(), "Input dataset."),
        column("input_field", pa.string(), "Input column."),
        column("transformation_type", pa.string(), "Transformation type, if recorded."),
        column("transformation_description", pa.string(), "Transformation description, if recorded."),
        column("dataset_version", pa.string(), "Version of the output dataset."),
    ]
)


@init_single_worker
@bind_fixed_schema
class MarquezColumnLineage(SingleCall[ColumnLineageArgs]):
    """``column_lineage(node_id, ...)``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = MARQUEZ_COLUMN_LINEAGE_SCHEMA

    class Meta:
        """Function metadata."""

        name = "column_lineage"
        description = "Column-level lineage from Marquez for a dataset or one column, one row per input column"
        required_secrets = list(SECRET_REQUIREMENT)
        projection_pushdown = True
        tags = {
            **object_tags(
                title="Marquez Column Lineage",
                category="lineage",
                keywords="marquez, column lineage, field lineage, upstream columns, impact analysis",
                doc_llm=(
                    "GET /api/v1/column-lineage: for `node_id` ('dataset:<ns>:<name>' or "
                    "'datasetField:<ns>:<dataset>:<field>'), named args depth := 20 and with_downstream := false. "
                    "One row per (column node, input column): node_id, output_namespace, output_dataset, "
                    "output_field, output_field_type, input_namespace, input_dataset, input_field, "
                    "transformation_type, transformation_description, dataset_version. Columns with no inputs "
                    "appear once with NULL input_*. Rows cover every column node in the traversal, not only the "
                    "start dataset."
                ),
                doc_md=(
                    "## column_lineage(node_id, depth := 20, with_downstream := false)\n\nColumn lineage from Marquez."
                ),
            ),
            "vgi.result_columns_schema": result_columns(MARQUEZ_COLUMN_LINEAGE_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT output_field, input_dataset, input_field FROM openlineage.marquez.column_lineage("
                    "'dataset:bigquery:analytics.customers') WHERE output_dataset = 'analytics.customers' "
                    "ORDER BY output_field"
                ),
                description="Which upstream columns feed each column of a table",
            ),
        ]

    @classmethod
    def fetch_rows(cls, ep: Endpoint, args: Any) -> list[dict[str, Any]]:
        """Flatten every column node's inputFields."""
        query = {"nodeId": args.node_id, "depth": args.depth, "withDownstream": str(args.with_downstream).lower()}
        body = get_json(ep, "/column-lineage", query) or {}
        rows = []
        for n in body.get("graph") or []:
            data = n.get("data") if isinstance(n, dict) else None
            if not isinstance(data, dict):
                continue
            base = {
                "node_id": n.get("id"),
                "output_namespace": data.get("namespace"),
                "output_dataset": data.get("dataset"),
                "output_field": data.get("field"),
                "output_field_type": data.get("fieldType"),
                "dataset_version": data.get("datasetVersion"),
            }
            inputs = [i for i in data.get("inputFields") or [] if isinstance(i, dict)]
            if not inputs:
                rows.append(base)
            for i in inputs:
                rows.append(
                    {
                        **base,
                        "input_namespace": i.get("namespace"),
                        "input_dataset": i.get("dataset"),
                        "input_field": i.get("field"),
                        "transformation_type": i.get("transformationType") or data.get("transformationType"),
                        "transformation_description": i.get("transformationDescription")
                        or data.get("transformationDescription"),
                    }
                )
        return rows


@dataclass(slots=True, frozen=True, kw_only=True)
class SearchArgs:
    """``search(query, filter := 'ALL', sort := 'UPDATE_AT', max_results := 100)``."""

    query: Annotated[str, Arg(0, doc="Text to search for in dataset and job names.")]
    filter: Annotated[
        str,
        Arg("filter", default="ALL", doc="'DATASET', 'JOB' or 'ALL' (default).", choices=["ALL", "DATASET", "JOB"]),
    ]
    sort: Annotated[
        str,
        Arg("sort", default="UPDATE_AT", doc="'UPDATE_AT' (newest first) or 'NAME'.", choices=["UPDATE_AT", "NAME"]),
    ]
    max_results: Annotated[int, Arg("max_results", default=100, doc="Maximum results; 1 to 1000.", ge=1, le=1000)]


SEARCH_SCHEMA = pa.schema(
    [
        column("type", pa.string(), "DATASET or JOB.", nullable=False),
        column("namespace", pa.string(), "Namespace."),
        column("name", pa.string(), "Dataset or job name.", nullable=False),
        column("node_id", pa.string(), "Lineage node id, ready for lineage_graph()."),
        column("updated_at", TSTZ, "Last update."),
    ]
)


@init_single_worker
@bind_fixed_schema
class Search(SingleCall[SearchArgs]):
    """``search(query, ...)``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = SEARCH_SCHEMA

    class Meta:
        """Function metadata."""

        name = "search"
        description = "Search Marquez dataset and job names"
        required_secrets = list(SECRET_REQUIREMENT)
        projection_pushdown = True
        tags = {
            **object_tags(
                title="Marquez Search",
                category="marquez",
                keywords="marquez, search, find dataset, find job, lookup, node id",
                doc_llm=(
                    "GET /api/v1/search?q=...: datasets and jobs whose name matches `query`. Named args: filter := "
                    "'ALL' | 'DATASET' | 'JOB', sort := 'UPDATE_AT' | 'NAME', max_results := 100. Columns: type, "
                    "namespace, name, node_id (feed it to lineage_graph), updated_at."
                ),
                doc_md=(
                    "## search(query, filter := 'ALL', sort := 'UPDATE_AT', max_results := "
                    "100)\n\nFind datasets and jobs."
                ),
            ),
            "vgi.result_columns_schema": result_columns(SEARCH_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT namespace, name, node_id FROM openlineage.marquez.search('orders', filter := 'DATASET') "
                    "ORDER BY name"
                ),
                description="Datasets whose name mentions orders",
            ),
        ]

    @classmethod
    def fetch_rows(cls, ep: Endpoint, args: Any) -> list[dict[str, Any]]:
        """One row per hit."""
        query = {
            "q": args.query,
            "filter": None if args.filter == "ALL" else args.filter,
            "sort": args.sort,
            "limit": args.max_results,
        }
        body = get_json(ep, "/search", query) or {}
        return [
            {
                "type": r.get("type"),
                "namespace": r.get("namespace"),
                "name": r.get("name"),
                "node_id": r.get("nodeId"),
                "updated_at": _ts(r.get("updatedAt")),
            }
            for r in body.get("results") or []
            if isinstance(r, dict)
        ]


MARQUEZ_FUNCTIONS: list[type[Any]] = [
    DatasetVersions,
    JobVersions,
    RunFacets,
    LineageGraph,
    MarquezColumnLineage,
    Search,
]

# (table name, scan, primary key)
MARQUEZ_TABLES: list[tuple[str, type[Any], list[str] | None]] = [
    ("namespaces", NamespacesScan, ["name"]),
    ("sources", SourcesScan, ["name"]),
    ("datasets", DatasetsScan, ["namespace", "name"]),
    ("jobs", JobsScan, ["namespace", "name"]),
    ("runs", RunsScan, ["run_id"]),
    ("lineage_events", LineageEventsScan, None),
]
UNIFIED_TABLES: list[tuple[str, type[Any], list[str] | None]] = [
    ("runs", UnifiedRunsScan, ["run_id"]),
    ("task_runs", UnifiedTaskRunsScan, ["task_run_id"]),
]

__all__ = ["MARQUEZ_FUNCTIONS", "MARQUEZ_TABLES", "UNIFIED_TABLES", "NoArgs"]
