"""OpenLineage event loading and normalization (no VGI imports: pure, unit-testable logic).

Events come from files, globs, directories or http(s) URLs. A source may hold one JSON
event, a JSON array of events, newline-delimited JSON (the OpenLineage file transport's
format) or a Marquez ``/api/v1/events/lineage`` page (``{"events": [...]}``). ``.gz``
files are decompressed.

Everything here turns raw events into plain row dicts: one per event, the latest state
per run, job -> dataset edges, dataset schemas, column-lineage edges, facets, a lineage
traversal, and the shared ``unified`` orchestrator shape.
"""

from __future__ import annotations

import glob
import gzip
import json
import os
import re
import time
import urllib.error
import urllib.request
from collections import deque
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

USER_AGENT = "vgi-openlineage-worker/0.1"
HTTP_TIMEOUT_SECONDS = 60.0
HTTP_ATTEMPTS = 4
EVENT_SUFFIXES = (".json", ".ndjson", ".jsonl", ".json.gz", ".ndjson.gz", ".jsonl.gz")

# eventType -> the shared orchestrator status vocabulary.
EVENT_STATUS = {
    "START": "running",
    "RUNNING": "running",
    "COMPLETE": "success",
    "FAIL": "failed",
    "ABORT": "cancelled",
    "OTHER": "queued",
}
TERMINAL = {"COMPLETE", "FAIL", "ABORT"}

# Airflow DagRun.run_type -> unified trigger.
AIRFLOW_TRIGGER = {
    "scheduled": "schedule",
    "manual": "manual",
    "backfill": "other",
    "dataset_triggered": "event",
    "asset_triggered": "event",
}

# Producer-URI fragments -> orchestrator / engine name (first match wins).
PRODUCER_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"apache/airflow|integration/airflow|astronomer", re.I), "airflow"),
    (re.compile(r"dagster", re.I), "dagster"),
    (re.compile(r"integration/dbt|dbt-labs|/dbt\b", re.I), "dbt"),
    (re.compile(r"integration/spark|databricks", re.I), "spark"),
    (re.compile(r"integration/flink|apache/flink", re.I), "flink"),
    (re.compile(r"prefect", re.I), "prefect"),
    (re.compile(r"great[_-]expectations", re.I), "great_expectations"),
    (re.compile(r"trino", re.I), "trino"),
    (re.compile(r"integration/hive|apache/hive", re.I), "hive"),
    (re.compile(r"integration/([a-z0-9_-]+)", re.I), ""),  # generic: the integration's directory name
]


class EventSourceError(ValueError):
    """A source could not be read or holds something that is not an OpenLineage event."""


# ----------------------------------------------------------------------------- loading


@dataclass(frozen=True, slots=True)
class LoadedEvent:
    """One raw event and where it came from."""

    source: str
    index: int
    event: dict[str, Any]


def is_url(path: str) -> bool:
    """Return True for http:// and https:// sources."""
    return path.startswith(("http://", "https://"))


def resolve_sources(path: str) -> list[str]:
    """Expand ``path`` into the concrete files (sorted) or the single URL to read.

    Args:
        path: A file, directory, glob (``*``, ``?``, ``[...]``, ``**``) or http(s) URL.

    Returns:
        The sources to read, in a stable order.

    Raises:
        EventSourceError: Nothing matched.

    """
    path = path.strip()
    if not path:
        raise EventSourceError("path is empty: pass a file, directory, glob or http(s) URL")
    if is_url(path):
        return [path]
    expanded = os.path.expanduser(path)
    if any(ch in expanded for ch in "*?["):
        matches = sorted(p for p in glob.glob(expanded, recursive=True) if os.path.isfile(p))
        if not matches:
            raise EventSourceError(f"no files match {path!r}")
        return matches
    p = Path(expanded)
    if p.is_dir():
        files = sorted(str(f) for f in p.rglob("*") if f.is_file() and f.name.endswith(EVENT_SUFFIXES))
        if not files:
            raise EventSourceError(f"directory {path!r} holds no .json/.ndjson/.jsonl files")
        return files
    if not p.exists():
        raise EventSourceError(f"no such file: {path!r}")
    return [str(p)]


def fetch_url(url: str) -> bytes:
    """GET ``url``, retrying 429/5xx with backoff that honours ``Retry-After``.

    Args:
        url: The http(s) URL.

    Returns:
        The response body.

    Raises:
        EventSourceError: The request failed after retries or with a non-retryable status.

    """
    headers = {"Accept": "application/json, application/x-ndjson, */*", "User-Agent": USER_AGENT}
    for attempt in range(1, HTTP_ATTEMPTS + 1):
        try:
            with urllib.request.urlopen(
                urllib.request.Request(url, headers=headers), timeout=HTTP_TIMEOUT_SECONDS
            ) as r:
                body: bytes = r.read()
                return body
        except urllib.error.HTTPError as err:
            if (err.code != 429 and err.code < 500) or attempt == HTTP_ATTEMPTS:
                raise EventSourceError(f"GET {url} failed with HTTP {err.code}") from err
            retry_after = str(err.headers.get("Retry-After") or "")
            delay = float(retry_after) if retry_after.isdigit() else 0.5 * 2**attempt
            time.sleep(min(delay, 30.0))
        except (urllib.error.URLError, TimeoutError, ConnectionError) as err:
            if attempt == HTTP_ATTEMPTS:
                raise EventSourceError(f"GET {url} failed: {getattr(err, 'reason', err)}") from err
            time.sleep(0.5 * 2**attempt)
    raise EventSourceError(f"GET {url} failed")


def read_source(source: str) -> str:
    """Return the decoded text of one file or URL (gunzipping ``.gz`` / gzip bodies)."""
    raw = fetch_url(source) if is_url(source) else Path(source).read_bytes()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return raw.decode("utf-8-sig")


def parse_text(text: str, source: str, *, ignore_errors: bool = False) -> list[dict[str, Any]]:
    """Parse one source's text into event objects.

    Accepts a single JSON object, a JSON array, a ``{"events": [...]}`` page, or
    newline-delimited JSON.

    Args:
        text: The source text.
        source: Its name, for error messages.
        ignore_errors: Skip lines that are not JSON objects instead of failing.

    Returns:
        The event objects, in source order.

    Raises:
        EventSourceError: Malformed JSON (unless ``ignore_errors``).

    """
    stripped = text.strip()
    if not stripped:
        return []
    try:
        whole = json.loads(stripped)
    except json.JSONDecodeError:
        whole = None
    else:
        return _unwrap(whole, source, ignore_errors)
    events: list[dict[str, Any]] = []
    for lineno, line in enumerate(stripped.splitlines(), 1):
        line = line.strip()
        if not line:
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as err:
            if ignore_errors:
                continue
            raise EventSourceError(f"{source}:{lineno}: not valid JSON ({err.msg})") from err
        events.extend(_unwrap(value, f"{source}:{lineno}", ignore_errors))
    return events


def _unwrap(value: Any, where: str, ignore_errors: bool) -> list[dict[str, Any]]:
    if isinstance(value, dict) and "eventTime" not in value and isinstance(value.get("events"), list):
        value = value["events"]
    items = value if isinstance(value, list) else [value]
    out: list[dict[str, Any]] = []
    for item in items:
        if isinstance(item, dict) and ("eventTime" in item or "run" in item or "job" in item or "dataset" in item):
            out.append(item)
        elif not ignore_errors:
            raise EventSourceError(f"{where}: expected an OpenLineage event object, got {type(item).__name__}")
    return out


def load_events(path: str, *, ignore_errors: bool = False) -> list[LoadedEvent]:
    """Load every event from ``path`` (see `resolve_sources`)."""
    out: list[LoadedEvent] = []
    for source in resolve_sources(path):
        for i, event in enumerate(parse_text(read_source(source), source, ignore_errors=ignore_errors)):
            out.append(LoadedEvent(source=source, index=i, event=event))
    return out


# ----------------------------------------------------------------------------- field helpers


def parse_time(value: Any) -> datetime | None:
    """Parse an ISO-8601 timestamp (``Z`` or offset; naive is taken as UTC)."""
    if not isinstance(value, str) or not value:
        return None
    text = value.strip().replace("Z", "+00:00").replace("z", "+00:00")
    # Python < 3.11-style fromisoformat limits: trim fractional seconds beyond 6 digits.
    text = re.sub(r"(\.\d{6})\d+", r"\1", text)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def clean_facets(facets: Any) -> dict[str, Any]:
    """Drop null facets (Marquez serializes unknown ones as null) and non-dict values."""
    if not isinstance(facets, dict):
        return {}
    return {k: v for k, v in facets.items() if isinstance(v, dict)}


def json_text(value: Any) -> str | None:
    """Compact, stable JSON for a JSON column (None for empty)."""
    if value is None or value == {} or value == []:
        return None
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def event_kind(event: dict[str, Any]) -> str:
    """'run' (RunEvent), 'dataset' (DatasetEvent) or 'job' (JobEvent)."""
    if isinstance(event.get("run"), dict):
        return "run"
    if isinstance(event.get("dataset"), dict):
        return "dataset"
    return "job"


def _get(d: Any, *keys: str) -> Any:
    for key in keys:
        if not isinstance(d, dict):
            return None
        d = d.get(key)
    return d


def _str(value: Any) -> str | None:
    return None if value is None else str(value)


def dataset_refs(items: Any) -> list[dict[str, str | None]]:
    """``[{namespace, name}]`` for an event's inputs/outputs."""
    if not isinstance(items, list):
        return []
    return [
        {"namespace": _str(d.get("namespace")), "name": _str(d.get("name"))}
        for d in items
        if isinstance(d, dict) and d.get("name") is not None
    ]


def detect_orchestrator(producer: Any, run_facets: dict[str, Any], job_facets: dict[str, Any]) -> str:
    """Name the system that emitted a run: airflow, dagster, dbt, spark, flink, ...

    Precedence: the ``jobType`` job facet's ``integration``, then the
    ``processing_engine`` run facet's ``name``, then an ``airflow`` run facet, then
    the event's (or any facet's) producer URI. Falls back to 'unknown'.
    """
    integration = _get(job_facets, "jobType", "integration")
    if isinstance(integration, str) and integration.strip():
        return integration.strip().lower()
    engine = _get(run_facets, "processing_engine", "name")
    if isinstance(engine, str) and engine.strip():
        return engine.strip().lower()
    if "airflow" in run_facets or "airflowDagRun" in run_facets:
        return "airflow"
    producers = [producer] + [f.get("_producer") for f in (*run_facets.values(), *job_facets.values())]
    for value in producers:
        if not isinstance(value, str):
            continue
        for pattern, name in PRODUCER_PATTERNS:
            match = pattern.search(value)
            if match:
                return name or match.group(1).lower()
    return "unknown"


# ----------------------------------------------------------------------------- per-event rows


def event_row(loaded: LoadedEvent) -> dict[str, Any]:
    """One ``read_events`` row."""
    e = loaded.event
    kind = event_kind(e)
    run_facets = clean_facets(_get(e, "run", "facets"))
    job_facets = clean_facets(_get(e, "job", "facets"))
    parent = run_facets.get("parent") or {}
    return {
        "source": loaded.source,
        "event_index": loaded.index,
        "event_kind": kind,
        "event_type": _str(e.get("eventType")),
        "event_time": parse_time(e.get("eventTime")),
        "producer": _str(e.get("producer")),
        "schema_url": _str(e.get("schemaURL")),
        "run_id": _str(_get(e, "run", "runId")),
        "job_namespace": _str(_get(e, "job", "namespace")),
        "job_name": _str(_get(e, "job", "name")),
        "parent_run_id": _str(_get(parent, "run", "runId")),
        "parent_job_namespace": _str(_get(parent, "job", "namespace")),
        "parent_job_name": _str(_get(parent, "job", "name")),
        "root_run_id": _str(_get(parent, "root", "run", "runId")),
        "orchestrator": detect_orchestrator(e.get("producer"), run_facets, job_facets) if kind != "dataset" else None,
        "dataset_namespace": _str(_get(e, "dataset", "namespace")),
        "dataset_name": _str(_get(e, "dataset", "name")),
        "inputs": dataset_refs(e.get("inputs")),
        "outputs": dataset_refs(e.get("outputs")),
        "run_facets": json_text(run_facets),
        "job_facets": json_text(job_facets),
        "event": json.dumps(e, sort_keys=True, separators=(",", ":")),
    }


def _sort_key(loaded: LoadedEvent) -> tuple[datetime, str, int]:
    t = parse_time(loaded.event.get("eventTime")) or datetime.min.replace(tzinfo=UTC)
    return (t, loaded.source, loaded.index)


def sorted_events(events: Iterable[LoadedEvent]) -> list[LoadedEvent]:
    """Events in eventTime order (ties keep source order)."""
    return sorted(events, key=_sort_key)


# ----------------------------------------------------------------------------- runs


@dataclass
class _RunAcc:
    run_id: str
    job_namespace: str | None = None
    job_name: str | None = None
    events: int = 0
    first: datetime | None = None
    last: datetime | None = None
    started: datetime | None = None
    terminal_type: str | None = None
    terminal_time: datetime | None = None
    latest_type: str | None = None
    producer: str | None = None
    run_facets: dict[str, Any] = field(default_factory=dict)
    job_facets: dict[str, Any] = field(default_factory=dict)
    inputs: dict[tuple[str | None, str | None], None] = field(default_factory=dict)
    outputs: dict[tuple[str | None, str | None], None] = field(default_factory=dict)


def _min(a: datetime | None, b: datetime | None) -> datetime | None:
    return b if a is None else a if b is None else min(a, b)


def _max(a: datetime | None, b: datetime | None) -> datetime | None:
    return b if a is None else a if b is None else max(a, b)


def run_rows(events: Iterable[LoadedEvent]) -> list[dict[str, Any]]:
    """Return the latest state of every run (one row per runId), ordered by first event time.

    A terminal event (COMPLETE / FAIL / ABORT) decides the state even if a START or
    RUNNING event arrives after it; otherwise the latest event does. Facets are merged
    across the run's events, later values winning.
    """
    runs: dict[str, _RunAcc] = {}
    for loaded in sorted_events(events):
        e = loaded.event
        if event_kind(e) != "run":
            continue
        run_id = _str(_get(e, "run", "runId"))
        if not run_id:
            continue
        acc = runs.setdefault(run_id, _RunAcc(run_id=run_id))
        t = parse_time(e.get("eventTime"))
        etype = str(e.get("eventType") or "OTHER").upper()
        acc.events += 1
        acc.job_namespace = _str(_get(e, "job", "namespace")) or acc.job_namespace
        acc.job_name = _str(_get(e, "job", "name")) or acc.job_name
        acc.producer = _str(e.get("producer")) or acc.producer
        acc.first = _min(acc.first, t)
        acc.last = _max(acc.last, t)
        if etype in ("START", "RUNNING"):
            acc.started = _min(acc.started, t)
        if etype in TERMINAL:
            acc.terminal_type, acc.terminal_time = etype, t
        acc.latest_type = etype
        acc.run_facets.update(clean_facets(_get(e, "run", "facets")))
        acc.job_facets.update(clean_facets(_get(e, "job", "facets")))
        for ref in dataset_refs(e.get("inputs")):
            acc.inputs[(ref["namespace"], ref["name"])] = None
        for ref in dataset_refs(e.get("outputs")):
            acc.outputs[(ref["namespace"], ref["name"])] = None
    return [_run_row(acc) for acc in runs.values()]


def _run_row(acc: _RunAcc) -> dict[str, Any]:
    state = acc.terminal_type or acc.latest_type or "OTHER"
    started = acc.started
    ended = acc.terminal_time
    parent = acc.run_facets.get("parent") or {}
    nominal = acc.run_facets.get("nominalTime") or {}
    error = acc.run_facets.get("errorMessage") or {}
    return {
        "run_id": acc.run_id,
        "job_namespace": acc.job_namespace,
        "job_name": acc.job_name,
        "state": state,
        "status": EVENT_STATUS.get(state, "queued"),
        "started_at": started,
        "ended_at": ended,
        "duration_seconds": (ended - started).total_seconds() if started and ended else None,
        "first_event_at": acc.first,
        "last_event_at": acc.last,
        "event_count": acc.events,
        "parent_run_id": _str(_get(parent, "run", "runId")),
        "parent_job_namespace": _str(_get(parent, "job", "namespace")),
        "parent_job_name": _str(_get(parent, "job", "name")),
        "root_run_id": _str(_get(parent, "root", "run", "runId")),
        "orchestrator": detect_orchestrator(acc.producer, acc.run_facets, acc.job_facets),
        "job_type": _str(_get(acc.job_facets, "jobType", "jobType")),
        "nominal_start_time": parse_time(nominal.get("nominalStartTime")),
        "nominal_end_time": parse_time(nominal.get("nominalEndTime")),
        "error_message": _str(error.get("message")),
        "inputs": [{"namespace": ns, "name": n} for ns, n in acc.inputs],
        "outputs": [{"namespace": ns, "name": n} for ns, n in acc.outputs],
        "producer": acc.producer,
        "run_facets": json_text(acc.run_facets),
        "job_facets": json_text(acc.job_facets),
    }


# ----------------------------------------------------------------------------- unified


def airflow_attempt(run_facets: dict[str, Any]) -> int | None:
    """Airflow's try_number from the ``airflow`` run facet."""
    value = _get(run_facets, "airflow", "taskInstance", "try_number")
    return int(value) if isinstance(value, int | float) or (isinstance(value, str) and value.isdigit()) else None


def trigger_of(run_facets: dict[str, Any]) -> str | None:
    """Unified trigger from the ``airflow`` facet's DagRun.run_type, else NULL."""
    run_type = _get(run_facets, "airflow", "dagRun", "run_type") or _get(
        run_facets, "airflowDagRun", "dagRun", "run_type"
    )
    if not isinstance(run_type, str):
        return None
    return AIRFLOW_TRIGGER.get(run_type.lower(), "other")


def unified_split(run_dicts: Iterable[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Split normalized runs into ``unified.runs`` rows (no parent) and ``task_runs`` rows.

    A run with a ``parent`` facet is a task run; its ``run_id`` is the root run when the
    facet names one, otherwise the parent run.
    """
    runs: list[dict[str, Any]] = []
    tasks: list[dict[str, Any]] = []
    for r in run_dicts:
        facets = json.loads(r["run_facets"]) if r.get("run_facets") else {}
        base = {
            "orchestrator": r["orchestrator"],
            "status": r["status"],
            "raw_status": r["state"],
            "started_at": r["started_at"],
            "ended_at": r["ended_at"],
            "duration_seconds": r["duration_seconds"],
            "url": r.get("url"),
        }
        if r.get("parent_run_id"):
            tasks.append(
                {
                    **base,
                    "run_id": r.get("root_run_id") or r["parent_run_id"],
                    "task_run_id": r["run_id"],
                    "task_name": r["job_name"],
                    "attempt": airflow_attempt(facets),
                }
            )
        else:
            runs.append(
                {
                    **base,
                    "run_id": r["run_id"],
                    "pipeline": r["job_name"],
                    "trigger": trigger_of(facets),
                    "queued_at": r.get("first_event_at") or r["started_at"],
                }
            )
    return runs, tasks


# ----------------------------------------------------------------------------- job I/O


def job_io_rows(events: Iterable[LoadedEvent]) -> list[dict[str, Any]]:
    """Distinct job -> dataset edges (direction input/output) from RunEvents and JobEvents."""
    edges: dict[tuple[Any, ...], dict[str, Any]] = {}
    for loaded in sorted_events(events):
        e = loaded.event
        if event_kind(e) == "dataset":
            continue
        ns, name = _str(_get(e, "job", "namespace")), _str(_get(e, "job", "name"))
        if name is None:
            continue
        t = parse_time(e.get("eventTime"))
        run_id = _str(_get(e, "run", "runId"))
        for direction, key in (("input", "inputs"), ("output", "outputs")):
            for ref in dataset_refs(e.get(key)):
                k = (ns, name, direction, ref["namespace"], ref["name"])
                row = edges.get(k)
                if row is None:
                    row = edges[k] = {
                        "job_namespace": ns,
                        "job_name": name,
                        "direction": direction,
                        "dataset_namespace": ref["namespace"],
                        "dataset_name": ref["name"],
                        "first_seen": t,
                        "last_seen": t,
                        "_runs": set(),
                    }
                row["first_seen"] = _min(row["first_seen"], t)
                row["last_seen"] = _max(row["last_seen"], t)
                if run_id:
                    row["_runs"].add(run_id)
    out = []
    for row in edges.values():
        runs = row.pop("_runs")
        out.append({**row, "run_count": len(runs)})
    return out


# ----------------------------------------------------------------------------- datasets


def _dataset_facet_sources(e: dict[str, Any]) -> Iterator[tuple[str, dict[str, Any]]]:
    """(entity, dataset dict) for every dataset an event carries."""
    if event_kind(e) == "dataset":
        yield "dataset", e["dataset"]
    for entity, key in (("input", "inputs"), ("output", "outputs")):
        for d in e.get(key) or []:
            if isinstance(d, dict):
                yield entity, d


def _flatten_fields(fields: Any, prefix: str = "") -> Iterator[dict[str, Any]]:
    for f in fields if isinstance(fields, list) else []:
        if not isinstance(f, dict) or f.get("name") is None:
            continue
        name = f"{prefix}{f['name']}"
        yield {"field_name": name, "field_type": _str(f.get("type")), "description": _str(f.get("description"))}
        yield from _flatten_fields(f.get("fields"), f"{name}.")


def dataset_schema_rows(events: Iterable[LoadedEvent]) -> list[dict[str, Any]]:
    """Return the latest ``schema`` facet of each dataset, one row per (nested, dotted) field."""
    latest: dict[tuple[str | None, str | None], tuple[datetime | None, str | None, Any]] = {}
    for loaded in sorted_events(events):
        e = loaded.event
        t = parse_time(e.get("eventTime"))
        for _, d in _dataset_facet_sources(e):
            fields = _get(d, "facets", "schema", "fields")
            if isinstance(fields, list):
                latest[(_str(d.get("namespace")), _str(d.get("name")))] = (t, _str(_get(e, "run", "runId")), fields)
    rows = []
    for (ns, name), (t, run_id, fields) in latest.items():
        for ordinal, f in enumerate(_flatten_fields(fields), 1):
            rows.append(
                {
                    "dataset_namespace": ns,
                    "dataset_name": name,
                    "ordinal": ordinal,
                    **f,
                    "observed_at": t,
                    "run_id": run_id,
                }
            )
    return rows


def column_lineage_rows(events: Iterable[LoadedEvent]) -> list[dict[str, Any]]:
    """Column-level edges from ``columnLineage`` facets (latest description of each edge).

    Field-level entries give ``output_field``; dataset-wide dependencies (the facet's
    ``dataset`` list) have a NULL ``output_field``. One row per input field and
    transformation (a field without transformations gets one row with NULLs, using the
    legacy ``transformationType`` / ``transformationDescription`` keys when present).
    """
    edges: dict[tuple[Any, ...], dict[str, Any]] = {}
    for loaded in sorted_events(events):
        e = loaded.event
        t = parse_time(e.get("eventTime"))
        job_ns, job_name = _str(_get(e, "job", "namespace")), _str(_get(e, "job", "name"))
        for entity, d in _dataset_facet_sources(e):
            if entity == "input":
                continue
            facet = _get(d, "facets", "columnLineage")
            if not isinstance(facet, dict):
                continue
            out_ns, out_name = _str(d.get("namespace")), _str(d.get("name"))
            entries: list[tuple[str | None, dict[str, Any]]] = []
            fields = facet.get("fields")
            if isinstance(fields, dict):
                entries.extend((str(k), v) for k, v in fields.items() if isinstance(v, dict))
            if isinstance(facet.get("dataset"), list):
                entries.append((None, {"inputFields": facet["dataset"]}))
            for out_field, spec in entries:
                for inp in spec.get("inputFields") or []:
                    if not isinstance(inp, dict):
                        continue
                    transforms = [x for x in inp.get("transformations") or [] if isinstance(x, dict)] or [
                        {
                            "type": spec.get("transformationType"),
                            "subtype": None,
                            "description": spec.get("transformationDescription"),
                            "masking": None,
                        }
                    ]
                    for tr in transforms:
                        key = (
                            out_ns,
                            out_name,
                            out_field,
                            _str(inp.get("namespace")),
                            _str(inp.get("name")),
                            _str(inp.get("field")),
                            _str(tr.get("type")),
                            _str(tr.get("subtype")),
                        )
                        edges[key] = {
                            "output_namespace": out_ns,
                            "output_dataset": out_name,
                            "output_field": out_field,
                            "input_namespace": key[3],
                            "input_dataset": key[4],
                            "input_field": key[5],
                            "transformation_type": key[6],
                            "transformation_subtype": key[7],
                            "transformation_description": _str(tr.get("description")) or None,
                            "masking": tr.get("masking") if isinstance(tr.get("masking"), bool) else None,
                            "job_namespace": job_ns,
                            "job_name": job_name,
                            "last_seen": t,
                        }
    return list(edges.values())


def facet_rows(events: Iterable[LoadedEvent]) -> list[dict[str, Any]]:
    """One row per facet on every event: run, job, dataset, input and output facets."""
    rows: list[dict[str, Any]] = []
    for loaded in events:
        e = loaded.event
        base = {
            "source": loaded.source,
            "event_index": loaded.index,
            "event_time": parse_time(e.get("eventTime")),
            "event_type": _str(e.get("eventType")),
            "run_id": _str(_get(e, "run", "runId")),
            "job_namespace": _str(_get(e, "job", "namespace")),
            "job_name": _str(_get(e, "job", "name")),
        }
        groups: list[tuple[str, str | None, str | None, Any]] = [
            ("run", None, None, _get(e, "run", "facets")),
            ("job", None, None, _get(e, "job", "facets")),
        ]
        for entity, d in _dataset_facet_sources(e):
            ns, name = _str(d.get("namespace")), _str(d.get("name"))
            groups.append((entity, ns, name, d.get("facets")))
            extra_key = {"input": "inputFacets", "output": "outputFacets"}.get(entity)
            if extra_key:
                groups.append((f"{entity}_facet", ns, name, d.get(extra_key)))
        for entity, ns, name, facets in groups:
            for facet_name, facet in clean_facets(facets).items():
                rows.append(
                    {
                        **base,
                        "entity": entity,
                        "dataset_namespace": ns,
                        "dataset_name": name,
                        "facet_name": facet_name,
                        "facet_producer": _str(facet.get("_producer")),
                        "facet_schema_url": _str(facet.get("_schemaURL")),
                        "facet": json_text(facet) or "{}",
                    }
                )
    return rows


# ----------------------------------------------------------------------------- lineage traversal


def node_id(kind: str, namespace: str | None, name: str | None) -> str:
    """Marquez-style node id: ``dataset:<namespace>:<name>`` or ``job:<namespace>:<name>``."""
    return f"{kind}:{namespace or ''}:{name or ''}"


def lineage_rows(edges: list[dict[str, Any]], start: str, depth: int, direction: str) -> list[dict[str, Any]]:
    """Walk the job <-> dataset graph from ``start`` up to ``depth`` hops.

    Args:
        edges: ``job_io_rows`` output.
        start: A node id (``dataset:<ns>:<name>`` / ``job:<ns>:<name>``) or a bare
            dataset/job name that identifies exactly one node.
        depth: Maximum hops (edges) from the start node.
        direction: 'upstream', 'downstream' or 'both'.

    Returns:
        One row per reachable node (the start node at hops 0), nearest first. An
        unknown or ambiguous start node raises ``EventSourceError``.

    """
    nodes: dict[str, tuple[str, str | None, str | None]] = {}
    down: dict[str, set[str]] = {}
    up: dict[str, set[str]] = {}
    for e in edges:
        job = node_id("job", e["job_namespace"], e["job_name"])
        ds = node_id("dataset", e["dataset_namespace"], e["dataset_name"])
        nodes[job] = ("job", e["job_namespace"], e["job_name"])
        nodes[ds] = ("dataset", e["dataset_namespace"], e["dataset_name"])
        src, dst = (ds, job) if e["direction"] == "input" else (job, ds)
        down.setdefault(src, set()).add(dst)
        up.setdefault(dst, set()).add(src)
    start_id = _resolve_node(nodes, start)
    rows = [_lineage_row(nodes, start_id, "start", 0, None)]
    walks = {"upstream": [("upstream", up)], "downstream": [("downstream", down)]}.get(
        direction, [("upstream", up), ("downstream", down)]
    )
    for label, adjacency in walks:
        seen = {start_id}
        queue: deque[tuple[str, int]] = deque([(start_id, 0)])
        while queue:
            current, hops = queue.popleft()
            if hops >= depth:
                continue
            for nxt in sorted(adjacency.get(current, ())):
                if nxt in seen:
                    continue
                seen.add(nxt)
                rows.append(_lineage_row(nodes, nxt, label, hops + 1, current))
                queue.append((nxt, hops + 1))
    return rows


def _resolve_node(nodes: dict[str, tuple[str, str | None, str | None]], start: str) -> str:
    if start in nodes:
        return start
    by_name = [nid for nid, (_, _, name) in nodes.items() if name == start]
    if len(by_name) == 1:
        return by_name[0]
    if len(by_name) > 1:
        raise EventSourceError(f"{start!r} names several nodes; pass one of: {', '.join(sorted(by_name))}")
    raise EventSourceError(
        f"node {start!r} does not appear in these events; pass 'dataset:<namespace>:<name>', "
        "'job:<namespace>:<name>' or a unique dataset/job name"
    )


def _lineage_row(
    nodes: dict[str, tuple[str, str | None, str | None]], nid: str, direction: str, hops: int, via: str | None
) -> dict[str, Any]:
    kind, ns, name = nodes[nid]
    return {
        "node_id": nid,
        "node_type": kind,
        "namespace": ns,
        "name": name,
        "direction": direction,
        "hops": hops,
        "reached_from": via,
    }
