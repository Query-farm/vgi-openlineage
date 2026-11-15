"""Table functions over raw OpenLineage event JSON (schema ``main``; no credentials).

``read_events`` streams one source (file or URL) per batch, chunked for large files,
with the source index and offset in the table-function state. The derived functions
(``runs``, ``job_io``, ...) need every event before they can answer, so each call
re-reads the sources and emits the next chunk of the derived rows; the cursor is the
row offset, so a scan resumes on any worker process over HTTP.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
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
from .common import DATASET_REFS, JSON, TSTZ, result_columns, rows_to_batch
from .meta import column, object_tags

CHUNK_ROWS = 20_000
EXAMPLE_PATH = "https://raw.githubusercontent.com/Query-farm/vgi-openlineage/v0.1.0/test/fixtures/all-events.ndjson"

PATH_DOC = (
    "OpenLineage events to read: a file, a directory (every .json/.ndjson/.jsonl[.gz] inside, recursively), "
    "a glob such as '/var/log/ol/**/*.ndjson', or an http(s) URL."
)
IGNORE_DOC = "Skip lines/items that are not JSON event objects instead of failing (default false)."


@dataclass(slots=True, frozen=True, kw_only=True)
class EventArgs:
    """``fn(path, ignore_errors := false)``."""

    path: Annotated[str, Arg(0, doc=PATH_DOC)]
    ignore_errors: Annotated[bool, Arg("ignore_errors", default=False, doc=IGNORE_DOC)]


@dataclass(slots=True, frozen=True, kw_only=True)
class LineageArgs:
    """``lineage(path, node, depth := 5, direction := 'both', ignore_errors := false)``."""

    path: Annotated[str, Arg(0, doc=PATH_DOC)]
    node: Annotated[
        str,
        Arg(
            1,
            doc="Start node id in Marquez form (dataset or job, then namespace, then name, joined by colons), "
            "or a bare dataset or job name that identifies exactly one node.",
        ),
    ]
    depth: Annotated[int, Arg("depth", default=5, doc="Maximum hops (edges) to walk; 1 to 100.", ge=1, le=100)]
    direction: Annotated[
        str,
        Arg(
            "direction",
            default="both",
            doc="'upstream' (what feeds the node), 'downstream' (what it feeds) or 'both'.",
            choices=["upstream", "downstream", "both"],
        ),
    ]
    ignore_errors: Annotated[bool, Arg("ignore_errors", default=False, doc=IGNORE_DOC)]


@dataclass(kw_only=True)
class SourceCursor(ArrowSerializableDataclass):
    """``read_events`` cursor: which source, and how many of its events were emitted."""

    sources_json: str = "[]"
    source_index: int = 0
    offset: int = 0


@dataclass(kw_only=True)
class RowCursor(ArrowSerializableDataclass):
    """Derived-function cursor: rows already emitted, or -1 once done."""

    offset: int = 0


# ----------------------------------------------------------------------------- schemas

PATH_COL = column("source", pa.string(), "File path or URL the event was read from.")
EVENT_INDEX_COL = column("event_index", pa.int64(), "Position of the event within its source (0-based).")

EVENTS_SCHEMA = pa.schema(
    [
        PATH_COL,
        EVENT_INDEX_COL,
        column(
            "event_kind", pa.string(), "'run' (RunEvent), 'dataset' (DatasetEvent) or 'job' (JobEvent).", nullable=False
        ),
        column(
            "event_type",
            pa.string(),
            "RunEvent eventType: START, RUNNING, COMPLETE, FAIL, ABORT or OTHER; NULL otherwise.",
        ),
        column("event_time", TSTZ, "When the event happened (the event's eventTime, UTC)."),
        column("producer", pa.string(), "Producer URI of the integration that emitted the event."),
        column("schema_url", pa.string(), "schemaURL of the event."),
        column("run_id", pa.string(), "run.runId (RunEvents)."),
        column("job_namespace", pa.string(), "Namespace of the job the event reports on, e.g. 'airflow-prod'."),
        column("job_name", pa.string(), "Name of the job the event reports on, e.g. 'etl_orders.extract'."),
        column("parent_run_id", pa.string(), "runId from the parent run facet."),
        column("parent_job_namespace", pa.string(), "Job namespace from the parent run facet."),
        column("parent_job_name", pa.string(), "Job name from the parent run facet."),
        column("root_run_id", pa.string(), "Root runId from the parent facet's root (OpenLineage 1.18+)."),
        column(
            "orchestrator", pa.string(), "airflow, dagster, dbt, spark, flink, ... derived from facets and producer."
        ),
        column("dataset_namespace", pa.string(), "dataset.namespace (DatasetEvents)."),
        column("dataset_name", pa.string(), "dataset.name (DatasetEvents)."),
        column("inputs", DATASET_REFS, "Input datasets as {namespace, name}."),
        column("outputs", DATASET_REFS, "Output datasets as {namespace, name}."),
        column("run_facets", JSON, "run.facets (JSON object)."),
        column("job_facets", JSON, "job.facets (JSON object)."),
        column("event", JSON, "The whole event (JSON)."),
    ]
)

RUN_COLUMNS = [
    column("run_id", pa.string(), "OpenLineage runId.", nullable=False),
    column("job_namespace", pa.string(), "Job namespace."),
    column("job_name", pa.string(), "Job name."),
    column("state", pa.string(), "Latest eventType: START, RUNNING, COMPLETE, FAIL, ABORT or OTHER.", nullable=False),
    column("status", pa.string(), "queued, running, success, failed or cancelled.", nullable=False),
    column("started_at", TSTZ, "Earliest START/RUNNING eventTime."),
    column("ended_at", TSTZ, "eventTime of the terminal (COMPLETE/FAIL/ABORT) event."),
    column("duration_seconds", pa.float64(), "ended_at - started_at in seconds."),
    column("first_event_at", TSTZ, "Earliest eventTime for the run."),
    column("last_event_at", TSTZ, "Latest eventTime for the run."),
    column("event_count", pa.int64(), "Events seen for the run.", nullable=False),
    column("parent_run_id", pa.string(), "Parent runId (parent run facet)."),
    column("parent_job_namespace", pa.string(), "Parent job namespace."),
    column("parent_job_name", pa.string(), "Parent job name."),
    column("root_run_id", pa.string(), "Root runId (parent facet's root)."),
    column("orchestrator", pa.string(), "airflow, dagster, dbt, spark, flink, ... or 'unknown'.", nullable=False),
    column("job_type", pa.string(), "jobType facet's jobType: DAG, TASK, MODEL, QUERY, ..."),
    column("nominal_start_time", TSTZ, "nominalTime facet's nominalStartTime (the scheduled time)."),
    column("nominal_end_time", TSTZ, "nominalTime facet's nominalEndTime."),
    column("error_message", pa.string(), "errorMessage facet's message (failed runs)."),
    column("inputs", DATASET_REFS, "Input datasets across all the run's events."),
    column("outputs", DATASET_REFS, "Output datasets across all the run's events."),
    column("producer", pa.string(), "Producer URI of the latest event."),
    column("run_facets", JSON, "Run facets merged across events, later values winning (JSON)."),
    column("job_facets", JSON, "Job facets merged across events (JSON)."),
]
RUNS_SCHEMA = pa.schema(RUN_COLUMNS)

JOB_IO_SCHEMA = pa.schema(
    [
        column("job_namespace", pa.string(), "Job namespace."),
        column("job_name", pa.string(), "Job name.", nullable=False),
        column(
            "direction", pa.string(), "'input' (job reads the dataset) or 'output' (job writes it).", nullable=False
        ),
        column("dataset_namespace", pa.string(), "Dataset namespace, e.g. 'postgres://db:5432' or 'bigquery'."),
        column("dataset_name", pa.string(), "Dataset name, e.g. 'analytics.orders'.", nullable=False),
        column("run_count", pa.int64(), "Distinct runs that recorded this edge (0 for JobEvents).", nullable=False),
        column("first_seen", TSTZ, "Earliest eventTime carrying the edge."),
        column("last_seen", TSTZ, "Latest eventTime carrying the edge."),
    ]
)

SCHEMA_SCHEMA = pa.schema(
    [
        column("dataset_namespace", pa.string(), "Dataset namespace."),
        column("dataset_name", pa.string(), "Dataset name.", nullable=False),
        column(
            "ordinal",
            pa.int32(),
            "1-based position in the schema facet (nested fields follow their parent).",
            nullable=False,
        ),
        column("field_name", pa.string(), "Field name; nested fields are dotted, e.g. 'address.city'.", nullable=False),
        column("field_type", pa.string(), "Field type as the producer reported it."),
        column("description", pa.string(), "Field description."),
        column("observed_at", TSTZ, "eventTime of the event carrying this (latest) schema facet."),
        column("run_id", pa.string(), "runId of that event (NULL for DatasetEvents/JobEvents)."),
    ]
)

COLUMN_LINEAGE_SCHEMA = pa.schema(
    [
        column("output_namespace", pa.string(), "Namespace of the dataset carrying the columnLineage facet."),
        column("output_dataset", pa.string(), "Name of that dataset.", nullable=False),
        column("output_field", pa.string(), "Output column; NULL for a dataset-wide dependency."),
        column("input_namespace", pa.string(), "Namespace of the input dataset."),
        column("input_dataset", pa.string(), "Input dataset name."),
        column("input_field", pa.string(), "Input column."),
        column("transformation_type", pa.string(), "DIRECT or INDIRECT (or the legacy field-level type)."),
        column("transformation_subtype", pa.string(), "IDENTITY, TRANSFORMATION, AGGREGATION, FILTER, JOIN, ..."),
        column("transformation_description", pa.string(), "Free-text description of the transformation."),
        column("masking", pa.bool_(), "Whether the transformation masks the input."),
        column("job_namespace", pa.string(), "Namespace of the job that wrote the output."),
        column("job_name", pa.string(), "Job that wrote the output."),
        column("last_seen", TSTZ, "Latest eventTime carrying this edge."),
    ]
)

FACETS_SCHEMA = pa.schema(
    [
        PATH_COL,
        EVENT_INDEX_COL,
        column("event_time", TSTZ, "When the event happened (the event's eventTime, UTC)."),
        column("event_type", pa.string(), "eventType (RunEvents)."),
        column("run_id", pa.string(), "runId (RunEvents)."),
        column("job_namespace", pa.string(), "Job namespace."),
        column("job_name", pa.string(), "Job name."),
        column(
            "entity",
            pa.string(),
            "Where the facet sits: run, job, dataset, input, output, input_facet (inputFacets) or output_facet.",
            nullable=False,
        ),
        column("dataset_namespace", pa.string(), "Dataset namespace for dataset/input/output facets."),
        column("dataset_name", pa.string(), "Dataset name for dataset/input/output facets."),
        column(
            "facet_name",
            pa.string(),
            "Facet key, e.g. 'parent', 'schema', 'columnLineage', 'errorMessage'.",
            nullable=False,
        ),
        column("facet_producer", pa.string(), "The facet's _producer."),
        column("facet_schema_url", pa.string(), "The facet's _schemaURL."),
        column("facet", JSON, "The facet body (JSON).", nullable=False),
    ]
)

LINEAGE_SCHEMA = pa.schema(
    [
        column("node_id", pa.string(), "'dataset:<namespace>:<name>' or 'job:<namespace>:<name>'.", nullable=False),
        column("node_type", pa.string(), "'dataset' or 'job'.", nullable=False),
        column("namespace", pa.string(), "Node namespace."),
        column("name", pa.string(), "Node name."),
        column("direction", pa.string(), "'start' (the start node), 'upstream' or 'downstream'.", nullable=False),
        column("hops", pa.int32(), "Edges between the start node and this node.", nullable=False),
        column("reached_from", pa.string(), "node_id of the neighbour this node was reached from."),
    ]
)

UNIFIED_RUNS_SCHEMA = pa.schema(
    [
        column(
            "orchestrator", pa.string(), "airflow, dagster, dbt, spark, ... (from facets and producer).", nullable=False
        ),
        column("run_id", pa.string(), "OpenLineage runId.", nullable=False),
        column("pipeline", pa.string(), "Job name (DAG, dbt invocation, job)."),
        column("status", pa.string(), "queued, running, success, failed, cancelled or skipped.", nullable=False),
        column("raw_status", pa.string(), "OpenLineage state (eventType or Marquez RunState)."),
        column("trigger", pa.string(), "schedule, manual, event or other (Airflow run_type), else NULL."),
        column("queued_at", TSTZ, "First event (or creation) time."),
        column("started_at", TSTZ, "When the run started."),
        column("ended_at", TSTZ, "When the run finished."),
        column("duration_seconds", pa.float64(), "ended_at - started_at in seconds."),
        column("url", pa.string(), "Marquez API link to the run (NULL for event files)."),
    ]
)

UNIFIED_TASK_RUNS_SCHEMA = pa.schema(
    [
        column(
            "orchestrator", pa.string(), "airflow, dagster, dbt, spark, ... (from facets and producer).", nullable=False
        ),
        column("run_id", pa.string(), "Root runId (parent facet's root) or the parent runId.", nullable=False),
        column("task_run_id", pa.string(), "This run's OpenLineage runId.", nullable=False),
        column("task_name", pa.string(), "Job name of the task run."),
        column("status", pa.string(), "queued, running, success, failed, cancelled or skipped.", nullable=False),
        column("raw_status", pa.string(), "OpenLineage state (eventType or Marquez RunState)."),
        column("attempt", pa.int32(), "Attempt number, counting from 1 (Airflow try_number); NULL when not reported."),
        column("started_at", TSTZ, "When the task run started."),
        column("ended_at", TSTZ, "When it finished."),
        column("duration_seconds", pa.float64(), "ended_at - started_at in seconds."),
        column("url", pa.string(), "Marquez API link to the run (NULL for event files)."),
    ]
)


# ----------------------------------------------------------------------------- read_events


@init_single_worker
@bind_fixed_schema
class ReadEvents(TableFunctionGenerator[EventArgs, SourceCursor]):
    """One row per OpenLineage event."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = EVENTS_SCHEMA

    class Meta:
        """Function metadata."""

        name = "read_events"
        description = "Read raw OpenLineage events from files, globs, directories or URLs, one row per event"
        projection_pushdown = True
        tags = {
            **object_tags(
                title="Read OpenLineage Events",
                category="events",
                keywords="openlineage, run event, dataset event, job event, ndjson, file transport, lineage log",
                doc_llm=(
                    "One row per OpenLineage event (RunEvent, DatasetEvent or JobEvent) read from `path`: a file, "
                    "directory, glob or http(s) URL holding single JSON objects, JSON arrays, NDJSON or a Marquez "
                    "events page. Columns: source, event_index, event_kind, event_type, event_time, producer, "
                    "schema_url, run_id, job_namespace, job_name, parent_run_id / parent_job_* / root_run_id (from "
                    "the parent facet), orchestrator, dataset_namespace/dataset_name (DatasetEvents), inputs and "
                    "outputs (`LIST(STRUCT(namespace, name))`), run_facets, job_facets and the whole event as "
                    "JSON. Use `runs(path)` for one row per run and `facets(path)` to query facets."
                ),
                doc_md=(
                    "## read_events(path, ignore_errors := false)\n\n"
                    "Every event in a file, glob, directory or URL, with typed identity columns and the facets "
                    "as JSON."
                ),
            ),
            "vgi.result_columns_schema": result_columns(EVENTS_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    f"SELECT event_type, count(*) AS events FROM openlineage.main.read_events('{EXAMPLE_PATH}') "
                    "WHERE event_kind = 'run' GROUP BY event_type ORDER BY events DESC"
                ),
                description="Count RunEvents by eventType",
            ),
            FunctionExample(
                sql=(
                    "SELECT job_name, event_time, run_facets->>'$.errorMessage.message' AS error "
                    f"FROM openlineage.main.read_events('{EXAMPLE_PATH}') WHERE event_type = 'FAIL' "
                    "ORDER BY event_time DESC LIMIT 20"
                ),
                description="The latest FAIL events with their error message",
            ),
        ]

    @classmethod
    def initial_state(cls, params: ProcessParams[EventArgs]) -> SourceCursor:
        """Resolve the sources once (the glob is expanded at the start of the scan)."""
        return SourceCursor(sources_json=json.dumps(ev.resolve_sources(params.args.path)))

    @classmethod
    def process(cls, params: ProcessParams[EventArgs], state: SourceCursor, out: OutputCollector) -> None:
        """Emit the next chunk of the current source."""
        sources: list[str] = json.loads(state.sources_json)
        if state.source_index >= len(sources):
            out.finish()
            return
        source = sources[state.source_index]
        parsed = ev.parse_text(ev.read_source(source), source, ignore_errors=params.args.ignore_errors)
        chunk = parsed[state.offset : state.offset + CHUNK_ROWS]
        rows = [
            ev.event_row(ev.LoadedEvent(source=source, index=state.offset + i, event=e)) for i, e in enumerate(chunk)
        ]
        if state.offset + CHUNK_ROWS >= len(parsed):
            state.source_index += 1
            state.offset = 0
        else:
            state.offset += CHUNK_ROWS
        out.emit(rows_to_batch(rows, params.output_schema))


# ----------------------------------------------------------------------------- derived functions


class _Derived[A: (EventArgs, LineageArgs)](TableFunctionGenerator[A, RowCursor]):
    """Re-reads every event per call and emits the next chunk of derived rows."""

    FIXED_SCHEMA: ClassVar[pa.Schema]

    @classmethod
    def derive(cls, args: Any, events: list[ev.LoadedEvent]) -> list[dict[str, Any]]:
        """All output rows for these events."""
        raise NotImplementedError

    @classmethod
    def initial_state(cls, params: ProcessParams[Any]) -> RowCursor:
        """Start at row 0."""
        return RowCursor()

    @classmethod
    def process(cls, params: ProcessParams[Any], state: RowCursor, out: OutputCollector) -> None:
        """Emit rows ``[offset, offset + CHUNK_ROWS)``."""
        if state.offset < 0:
            out.finish()
            return
        loaded = ev.load_events(params.args.path, ignore_errors=params.args.ignore_errors)
        rows = cls.derive(params.args, loaded)
        chunk = rows[state.offset : state.offset + CHUNK_ROWS]
        state.offset = state.offset + CHUNK_ROWS if state.offset + CHUNK_ROWS < len(rows) else -1
        out.emit(rows_to_batch(chunk, params.output_schema))


@init_single_worker
@bind_fixed_schema
class Runs(_Derived[EventArgs]):
    """Latest state of every run."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = RUNS_SCHEMA

    class Meta:
        """Function metadata."""

        name = "runs"
        description = "The latest state of every OpenLineage run in a set of event files, one row per runId"
        projection_pushdown = True
        tags = {
            **object_tags(
                title="Runs From Events",
                category="events",
                keywords="openlineage, runs, run state, failed runs, duration, parent run, job runs",
                doc_llm=(
                    "One row per runId across the events in `path`. `state` is the deciding eventType (a terminal "
                    "COMPLETE/FAIL/ABORT wins over a later START/RUNNING; otherwise the latest event); `status` maps "
                    "it to queued/running/success/failed/cancelled. started_at = earliest START/RUNNING, ended_at = "
                    "terminal event time, duration_seconds, event_count, parent_run_id/parent_job_*/root_run_id from "
                    "the parent facet, orchestrator, job_type (jobType facet), nominal_start_time/nominal_end_time, "
                    "error_message (errorMessage facet), inputs/outputs (all datasets seen), and merged run_facets / "
                    "job_facets JSON. Filter `status = 'failed'` for failures."
                ),
                doc_md=(
                    "## runs(path, ignore_errors := false)\n\nOne row per run with its latest state, timing and parent."
                ),
            ),
            "vgi.result_columns_schema": result_columns(RUNS_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT job_name, ended_at, error_message "
                    f"FROM openlineage.main.runs('{EXAMPLE_PATH}') WHERE status = 'failed' ORDER BY ended_at DESC"
                ),
                description="Failed runs with the error message from the errorMessage facet",
            ),
            FunctionExample(
                sql=(
                    "SELECT job_name, count(*) AS runs, round(avg(duration_seconds), 1) AS avg_seconds "
                    f"FROM openlineage.main.runs('{EXAMPLE_PATH}') WHERE status = 'success' "
                    "GROUP BY job_name ORDER BY avg_seconds DESC LIMIT 10"
                ),
                description="The ten slowest jobs by average successful run time",
            ),
        ]

    @classmethod
    def derive(cls, args: Any, events: list[ev.LoadedEvent]) -> list[dict[str, Any]]:
        """Run rows."""
        return ev.run_rows(events)


@init_single_worker
@bind_fixed_schema
class JobIO(_Derived[EventArgs]):
    """Job -> dataset edges."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = JOB_IO_SCHEMA

    class Meta:
        """Function metadata."""

        name = "job_io"
        description = "Job-to-dataset lineage edges (inputs and outputs) from OpenLineage events"
        projection_pushdown = True
        tags = {
            **object_tags(
                title="Job Inputs And Outputs",
                category="lineage",
                keywords="openlineage, job inputs, job outputs, dataset lineage, table lineage, reads, writes",
                doc_llm=(
                    "Distinct (job, direction, dataset) edges from RunEvents and JobEvents in `path`: "
                    "job_namespace, job_name, direction ('input' = the job reads the dataset, 'output' = it writes "
                    "it), dataset_namespace, dataset_name, run_count (distinct runs), first_seen, last_seen. Use it "
                    "to answer 'what does job X read/write' and 'who writes table Y'. For multi-hop walks use "
                    "`lineage(path, node)`."
                ),
                doc_md="## job_io(path, ignore_errors := false)\n\nWhich jobs read and write which datasets.",
            ),
            "vgi.result_columns_schema": result_columns(JOB_IO_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT dataset_namespace, dataset_name, list(job_name ORDER BY job_name) AS writers "
                    f"FROM openlineage.main.job_io('{EXAMPLE_PATH}') WHERE direction = 'output' "
                    "GROUP BY ALL ORDER BY dataset_name"
                ),
                description="Which jobs write each dataset",
            ),
        ]

    @classmethod
    def derive(cls, args: Any, events: list[ev.LoadedEvent]) -> list[dict[str, Any]]:
        """Edge rows."""
        return ev.job_io_rows(events)


@init_single_worker
@bind_fixed_schema
class DatasetSchema(_Derived[EventArgs]):
    """Latest schema facet fields per dataset."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = SCHEMA_SCHEMA

    class Meta:
        """Function metadata."""

        name = "dataset_schema"
        description = "Dataset columns from the latest OpenLineage schema facet of each dataset"
        projection_pushdown = True
        tags = {
            **object_tags(
                title="Dataset Schemas",
                category="events",
                keywords="openlineage, schema facet, dataset columns, fields, column types, data catalog",
                doc_llm=(
                    "One row per field of the latest `schema` facet seen for each dataset in `path` (on inputs, "
                    "outputs or DatasetEvents): dataset_namespace, dataset_name, ordinal (1-based schema order; "
                    "nested fields follow their parent with dotted names), field_name, field_type, description, "
                    "observed_at and run_id of the event carrying the schema."
                ),
                doc_md="## dataset_schema(path, ignore_errors := false)\n\nColumns and types of each dataset.",
            ),
            "vgi.result_columns_schema": result_columns(SCHEMA_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT dataset_name, count(*) AS columns, list(field_name ORDER BY ordinal) AS fields "
                    f"FROM openlineage.main.dataset_schema('{EXAMPLE_PATH}') GROUP BY "
                    f"dataset_name ORDER BY dataset_name"
                ),
                description="Column lists of every dataset with a schema facet",
            ),
        ]

    @classmethod
    def derive(cls, args: Any, events: list[ev.LoadedEvent]) -> list[dict[str, Any]]:
        """Return the schema rows."""
        return ev.dataset_schema_rows(events)


@init_single_worker
@bind_fixed_schema
class ColumnLineage(_Derived[EventArgs]):
    """Column-level lineage edges."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = COLUMN_LINEAGE_SCHEMA

    class Meta:
        """Function metadata."""

        name = "column_lineage"
        description = "Column-level lineage edges from the columnLineage facets in OpenLineage events"
        projection_pushdown = True
        tags = {
            **object_tags(
                title="Column Lineage From Events",
                category="lineage",
                keywords="openlineage, column lineage, field lineage, columnLineage facet, transformations, impact",
                doc_llm=(
                    "One row per (output column, input column, transformation) from the `columnLineage` facets of "
                    "output datasets and DatasetEvents in `path`: output_namespace, output_dataset, output_field "
                    "(NULL for dataset-wide dependencies), input_namespace, input_dataset, input_field, "
                    "transformation_type (DIRECT/INDIRECT), transformation_subtype (IDENTITY, TRANSFORMATION, "
                    "AGGREGATION, ...), transformation_description, masking, job_namespace/job_name of the writer, "
                    "last_seen. Use recursive CTEs over it for multi-hop column impact."
                ),
                doc_md=(
                    "## column_lineage(path, ignore_errors := false)\n\nWhich input columns feed each output column."
                ),
            ),
            "vgi.result_columns_schema": result_columns(COLUMN_LINEAGE_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT output_field, input_dataset, input_field, transformation_subtype "
                    f"FROM openlineage.main.column_lineage('{EXAMPLE_PATH}') "
                    "WHERE output_dataset = 'analytics.orders_enriched' ORDER BY output_field, input_field"
                ),
                description="Where each column of one table comes from",
            ),
        ]

    @classmethod
    def derive(cls, args: Any, events: list[ev.LoadedEvent]) -> list[dict[str, Any]]:
        """Column edge rows."""
        return ev.column_lineage_rows(events)


@init_single_worker
@bind_fixed_schema
class Facets(_Derived[EventArgs]):
    """Every facet as a JSON row."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = FACETS_SCHEMA

    class Meta:
        """Function metadata."""

        name = "facets"
        description = "Every run, job and dataset facet in OpenLineage events, one JSON row per facet"
        projection_pushdown = True
        tags = {
            **object_tags(
                title="Facets From Events",
                category="events",
                keywords="openlineage, facets, custom facets, processing_engine, errorMessage, sql facet, metadata",
                doc_llm=(
                    "One row per facet on every event in `path`, in source order: source, event_index, event_time, "
                    "event_type, run_id, job_namespace, job_name, entity (run, job, dataset, input, output, "
                    "input_facet, output_facet), dataset_namespace/dataset_name for dataset-level facets, facet_name "
                    "(e.g. 'processing_engine', 'sql', 'errorMessage', 'dataQualityMetrics'), facet_producer, "
                    "facet_schema_url and the facet body as JSON. Extract values with ->> e.g. "
                    "facet->>'$.version'."
                ),
                doc_md="## facets(path, ignore_errors := false)\n\nOpen-ended facets as JSON, ready for `->>`.",
            ),
            "vgi.result_columns_schema": result_columns(FACETS_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT DISTINCT job_name, facet->>'$.name' AS engine, facet->>'$.version' AS version "
                    f"FROM openlineage.main.facets('{EXAMPLE_PATH}') WHERE facet_name = 'processing_engine' "
                    "ORDER BY job_name"
                ),
                description="Processing engine and version per job",
            ),
        ]

    @classmethod
    def derive(cls, args: Any, events: list[ev.LoadedEvent]) -> list[dict[str, Any]]:
        """Facet rows."""
        return ev.facet_rows(events)


@init_single_worker
@bind_fixed_schema
class Lineage(_Derived[LineageArgs]):
    """Recursive lineage traversal."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = LINEAGE_SCHEMA

    class Meta:
        """Function metadata."""

        name = "lineage"
        description = "Walk the job/dataset lineage graph upstream or downstream from one node, N hops"
        projection_pushdown = True
        tags = {
            **object_tags(
                title="Lineage Traversal",
                category="lineage",
                keywords="openlineage, upstream, downstream, impact analysis, lineage graph, dependencies, hops",
                doc_llm=(
                    "Breadth-first walk of the bipartite job <-> dataset graph built from the events in `path` "
                    "(dataset -> job for inputs, job -> dataset for outputs). `node` is "
                    "'dataset:<namespace>:<name>', 'job:<namespace>:<name>' or a unique dataset/job name. Named "
                    "args: depth := 5 (max hops, 1-100), direction := 'upstream' | 'downstream' | 'both'. Returns "
                    "node_id, node_type ('dataset'/'job'), namespace, name, direction ('start' for the node "
                    "itself), hops (edges from the start: a dataset's writer job is 1 hop, the datasets that job "
                    "reads are 2) and reached_from (the neighbour it was reached through)."
                ),
                doc_md=(
                    "## lineage(path, node, depth := 5, direction := 'both')\n\n"
                    "Everything upstream and/or downstream of a dataset or job, with the hop count."
                ),
            ),
            "vgi.result_columns_schema": result_columns(LINEAGE_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT namespace, name, hops "
                    f"FROM openlineage.main.lineage('{EXAMPLE_PATH}', 'analytics.customers', depth := 10, "
                    "direction := 'upstream') WHERE node_type = 'dataset' AND hops > 0 ORDER BY hops, name"
                ),
                description="Every dataset upstream of analytics.customers",
            ),
            FunctionExample(
                sql=(
                    "SELECT name, hops FROM openlineage.main.lineage("
                    f"'{EXAMPLE_PATH}', 'dataset:postgres://db.internal:5432:shop.public.orders', "
                    "direction := 'downstream') WHERE node_type = 'job' ORDER BY hops, name"
                ),
                description="Impact analysis: jobs downstream of a source table",
            ),
        ]

    @classmethod
    def derive(cls, args: Any, events: list[ev.LoadedEvent]) -> list[dict[str, Any]]:
        """Traversal rows."""
        return ev.lineage_rows(ev.job_io_rows(events), args.node, args.depth, args.direction)


@init_single_worker
@bind_fixed_schema
class UnifiedRunsFromEvents(_Derived[EventArgs]):
    """Top-level runs in the shared orchestrator shape."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = UNIFIED_RUNS_SCHEMA

    class Meta:
        """Function metadata."""

        name = "unified_runs"
        description = "Top-level runs from OpenLineage events in the shared cross-orchestrator run layout"
        projection_pushdown = True
        tags = {
            **object_tags(
                title="Unified Runs From Events",
                category="unified",
                keywords="unified, cross orchestrator, union all, airflow, dagster, dbt, pipeline runs, run status",
                doc_llm=(
                    "Runs without a parent facet (DAG runs, dbt invocations, standalone jobs) from the events in "
                    "`path`, in the column layout every Query.Farm orchestrator worker shares: orchestrator, run_id, "
                    "pipeline (job name), status (queued/running/success/failed/cancelled/skipped), raw_status "
                    "(eventType), trigger (Airflow run_type -> schedule/manual/event/other, else NULL), queued_at "
                    "(first event), started_at, ended_at, duration_seconds, url (NULL). Child runs are in "
                    "`unified_task_runs(path)`."
                ),
                doc_md="## unified_runs(path)\n\nTop-level runs, ready to `UNION ALL` with other orchestrators.",
            ),
            "vgi.result_columns_schema": result_columns(UNIFIED_RUNS_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT orchestrator, status, count(*) AS runs "
                    f"FROM openlineage.main.unified_runs('{EXAMPLE_PATH}') GROUP BY ALL ORDER BY orchestrator, status"
                ),
                description="Run counts by orchestrator and normalized status",
            ),
        ]

    @classmethod
    def derive(cls, args: Any, events: list[ev.LoadedEvent]) -> list[dict[str, Any]]:
        """Top-level run rows."""
        return ev.unified_split(ev.run_rows(events))[0]


@init_single_worker
@bind_fixed_schema
class UnifiedTaskRunsFromEvents(_Derived[EventArgs]):
    """Child runs in the shared orchestrator shape."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = UNIFIED_TASK_RUNS_SCHEMA

    class Meta:
        """Function metadata."""

        name = "unified_task_runs"
        description = "Task runs (runs with a parent facet) from OpenLineage events in the shared layout"
        projection_pushdown = True
        tags = {
            **object_tags(
                title="Unified Task Runs From Events",
                category="unified",
                keywords="unified, task runs, airflow tasks, dbt models, spark jobs, parent run, union all",
                doc_llm=(
                    "Runs that carry an OpenLineage parent facet (Airflow tasks, dbt models, Spark jobs launched by a "
                    "task) from the events in `path`, in the shared task-run layout: orchestrator, run_id (the root "
                    "run when the facet names one, else the parent run), task_run_id (this runId), task_name (job "
                    "name), status, raw_status, attempt (Airflow try_number), started_at, ended_at, "
                    "duration_seconds, url (NULL). Join to `unified_runs(path)` on run_id."
                ),
                doc_md="## unified_task_runs(path)\n\nChild runs keyed to their root run.",
            ),
            "vgi.result_columns_schema": result_columns(UNIFIED_TASK_RUNS_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT task_name, status, attempt, duration_seconds "
                    f"FROM openlineage.main.unified_task_runs('{EXAMPLE_PATH}') "
                    "WHERE status = 'failed' ORDER BY started_at DESC LIMIT 20"
                ),
                description="The latest failed task runs with their attempt number",
            ),
        ]

    @classmethod
    def derive(cls, args: Any, events: list[ev.LoadedEvent]) -> list[dict[str, Any]]:
        """Task run rows."""
        return ev.unified_split(ev.run_rows(events))[1]


EVENT_FUNCTIONS: list[type[Any]] = [
    ReadEvents,
    Runs,
    JobIO,
    DatasetSchema,
    ColumnLineage,
    Facets,
    Lineage,
    UnifiedRunsFromEvents,
    UnifiedTaskRunsFromEvents,
]
