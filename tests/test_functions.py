"""Unit tests. The SQL-level tests live in test/sql and run through DuckDB."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from vgi.arguments import Arguments
from vgi.client import Client

from openlineage_worker import events as ev
from openlineage_worker.marquez import graph_rows
from openlineage_worker.marquez_client import Endpoint, MarquezError, endpoint_from_secret, get_json

ROOT = Path(__file__).resolve().parent.parent
FIXTURES = ROOT / "test" / "fixtures"
EVENTS = FIXTURES / "events"


def _loaded(*events: dict[str, Any]) -> list[ev.LoadedEvent]:
    return [ev.LoadedEvent(source="t", index=i, event=e) for i, e in enumerate(events)]


def _run_event(run: str, etype: str, time: str, **extra: Any) -> dict[str, Any]:
    return {
        "eventType": etype,
        "eventTime": time,
        "run": {"runId": run, "facets": extra.pop("run_facets", {})},
        "job": {"namespace": "ns", "name": extra.pop("job", "j"), "facets": extra.pop("job_facets", {})},
        "inputs": extra.pop("inputs", []),
        "outputs": extra.pop("outputs", []),
        "producer": extra.pop("producer", "https://example.com/producer"),
        "schemaURL": "s",
    }


# ----------------------------------------------------------------------------- loading


def test_resolve_sources_directory_glob_and_errors(tmp_path: Path) -> None:
    assert len(ev.resolve_sources(str(EVENTS))) == 8
    assert [Path(p).name for p in ev.resolve_sources(str(EVENTS / "dbt-*.json"))] == [
        "dbt-01.json",
        "dbt-02.json",
        "dbt-03.json",
        "dbt-04.json",
    ]
    with pytest.raises(ev.EventSourceError, match="no files match"):
        ev.resolve_sources(str(tmp_path / "*.json"))
    with pytest.raises(ev.EventSourceError, match="no such file"):
        ev.resolve_sources(str(tmp_path / "x.json"))
    with pytest.raises(ev.EventSourceError, match="holds no"):
        ev.resolve_sources(str(tmp_path))
    with pytest.raises(ev.EventSourceError, match="empty"):
        ev.resolve_sources("  ")


def test_parse_text_shapes() -> None:
    one = {"eventTime": "2026-01-01T00:00:00Z", "run": {"runId": "a"}}
    assert ev.parse_text(json.dumps(one), "s") == [one]
    assert ev.parse_text(json.dumps([one, one]), "s") == [one, one]
    assert ev.parse_text(json.dumps({"events": [one], "totalCount": 1}), "s") == [one]
    assert ev.parse_text(json.dumps(one) + "\n\n" + json.dumps(one) + "\n", "s") == [one, one]
    assert ev.parse_text("", "s") == []
    with pytest.raises(ev.EventSourceError, match=r"s:2: not valid JSON"):
        ev.parse_text(json.dumps(one) + "\n{oops\n", "s")
    assert ev.parse_text(json.dumps(one) + "\n{oops\n", "s", ignore_errors=True) == [one]
    with pytest.raises(ev.EventSourceError, match="expected an OpenLineage event"):
        ev.parse_text("[1, 2]", "s")


def test_gzip_and_load_all_fixtures() -> None:
    assert len(ev.load_events(str(FIXTURES / "gz"))) == 3
    assert len(ev.load_events(str(EVENTS))) == 22


def test_parse_time_variants() -> None:
    assert ev.parse_time("2026-11-02T02:00:05Z") == datetime(2026, 11, 2, 2, 0, 5, tzinfo=UTC)
    assert ev.parse_time("2026-11-02T02:00:05.123456789+00:00") == datetime(2026, 11, 2, 2, 0, 5, 123456, tzinfo=UTC)
    assert ev.parse_time("2026-11-02T02:00:05") == datetime(2026, 11, 2, 2, 0, 5, tzinfo=UTC)
    assert ev.parse_time("nope") is None
    assert ev.parse_time(None) is None


# ----------------------------------------------------------------------------- normalization


@pytest.mark.parametrize(
    ("producer", "run_facets", "job_facets", "expected"),
    [
        ("x", {}, {"jobType": {"integration": "SPARK"}}, "spark"),
        ("x", {"processing_engine": {"name": "Flink"}}, {}, "flink"),
        ("x", {"airflow": {}}, {}, "airflow"),
        ("https://github.com/apache/airflow/tree/providers-openlineage/1.14.0", {}, {}, "airflow"),
        ("https://github.com/OpenLineage/OpenLineage/tree/1.24.2/integration/dbt", {}, {}, "dbt"),
        ("https://github.com/OpenLineage/OpenLineage/tree/1.24.2/integration/sql", {}, {}, "sql"),
        ("https://example.com/custom", {}, {}, "unknown"),
        (None, {"x": {"_producer": "https://github.com/dagster-io/dagster"}}, {}, "dagster"),
    ],
)
def test_detect_orchestrator(
    producer: Any, run_facets: dict[str, Any], job_facets: dict[str, Any], expected: str
) -> None:
    assert ev.detect_orchestrator(producer, run_facets, job_facets) == expected


def test_terminal_state_wins_over_late_start() -> None:
    rows = ev.run_rows(
        _loaded(
            _run_event("r", "COMPLETE", "2026-01-01T00:10:00Z"),
            _run_event("r", "START", "2026-01-01T00:00:00Z"),
            _run_event("r", "RUNNING", "2026-01-01T00:20:00Z"),  # arrives late
        )
    )
    assert len(rows) == 1
    assert rows[0]["state"] == "COMPLETE"
    assert rows[0]["status"] == "success"
    assert rows[0]["duration_seconds"] == 600.0
    assert rows[0]["event_count"] == 3


def test_other_only_run_is_queued_and_facets_merge() -> None:
    rows = ev.run_rows(
        _loaded(
            _run_event("r", "OTHER", "2026-01-01T00:00:00Z", run_facets={"a": {"v": 1}, "gone": None}),
            _run_event("r", "OTHER", "2026-01-01T00:01:00Z", run_facets={"a": {"v": 2}, "b": {"v": 3}}),
        )
    )
    assert rows[0]["status"] == "queued"
    assert rows[0]["started_at"] is None
    assert json.loads(rows[0]["run_facets"]) == {"a": {"v": 2}, "b": {"v": 3}}


def test_unified_split_parent_and_root() -> None:
    parent = {"run": {"runId": "p"}, "job": {"namespace": "ns", "name": "dag"}}
    rooted = {**parent, "root": {"run": {"runId": "root"}, "job": {"namespace": "ns", "name": "top"}}}
    rows = ev.run_rows(
        _loaded(
            _run_event(
                "p",
                "START",
                "2026-01-01T00:00:00Z",
                job="dag",
                run_facets={"airflow": {"dagRun": {"run_type": "backfill"}}},
            ),
            _run_event("c1", "FAIL", "2026-01-01T00:01:00Z", run_facets={"parent": parent}),
            _run_event("c2", "ABORT", "2026-01-01T00:02:00Z", run_facets={"parent": rooted}),
        )
    )
    runs, tasks = ev.unified_split(rows)
    assert [(r["run_id"], r["trigger"], r["status"]) for r in runs] == [("p", "other", "running")]
    assert [(t["run_id"], t["task_run_id"], t["status"]) for t in tasks] == [
        ("p", "c1", "failed"),
        ("root", "c2", "cancelled"),
    ]


def test_column_lineage_legacy_and_dataset_wide() -> None:
    facet = {
        "fields": {
            "out": {"inputFields": [{"namespace": "n", "name": "in", "field": "a"}], "transformationType": "IDENTITY"}
        },
        "dataset": [
            {
                "namespace": "n",
                "name": "in",
                "field": "flag",
                "transformations": [{"type": "INDIRECT", "subtype": "FILTER"}],
            }
        ],
    }
    out = {"namespace": "n", "name": "o", "facets": {"columnLineage": facet}}
    rows = ev.column_lineage_rows(_loaded(_run_event("r", "COMPLETE", "2026-01-01T00:00:00Z", outputs=[out])))
    by_field = {r["output_field"]: r for r in rows}
    assert by_field["out"]["transformation_type"] == "IDENTITY"
    assert by_field[None]["transformation_subtype"] == "FILTER"


def test_nested_schema_fields_are_dotted() -> None:
    schema = {"fields": [{"name": "addr", "type": "STRUCT", "fields": [{"name": "city", "type": "STRING"}]}]}
    out = {"namespace": "n", "name": "o", "facets": {"schema": schema}}
    rows = ev.dataset_schema_rows(_loaded(_run_event("r", "COMPLETE", "2026-01-01T00:00:00Z", outputs=[out])))
    assert [(r["ordinal"], r["field_name"]) for r in rows] == [(1, "addr"), (2, "addr.city")]


def test_lineage_traversal_cycles_depth_and_ambiguity() -> None:
    edges = [
        {"job_namespace": "j", "job_name": "a", "direction": "input", "dataset_namespace": "d", "dataset_name": "x"},
        {"job_namespace": "j", "job_name": "a", "direction": "output", "dataset_namespace": "d", "dataset_name": "y"},
        {"job_namespace": "j", "job_name": "b", "direction": "input", "dataset_namespace": "d", "dataset_name": "y"},
        {"job_namespace": "j", "job_name": "b", "direction": "output", "dataset_namespace": "d", "dataset_name": "x"},
    ]
    rows = ev.lineage_rows(edges, "x", 10, "downstream")
    assert [(r["name"], r["hops"]) for r in rows] == [("x", 0), ("a", 1), ("y", 2), ("b", 3)]
    assert len(ev.lineage_rows(edges, "dataset:d:x", 1, "both")) == 3
    dup = [*edges, {**edges[0], "dataset_namespace": "other"}]
    with pytest.raises(ev.EventSourceError, match="several nodes"):
        ev.lineage_rows(dup, "x", 1, "both")


# ----------------------------------------------------------------------------- marquez client


def test_endpoint_from_secret() -> None:
    ep = endpoint_from_secret({"base_url": "http://h:5000/api/v1/", "api_key": "k", "page_size": "5"})
    assert ep == Endpoint(api_url="http://h:5000/api/v1", token="k", page_size=5)
    assert ep.url("/x", {"a": 1, "b": None}) == "http://h:5000/api/v1/x?a=1"
    for bad, msg in [
        (None, "no marquez secret"),
        ({}, "no marquez secret"),
        ({"api_key": "k"}, "needs BASE_URL"),
        ({"base_url": "h:5000"}, "must start with"),
        ({"base_url": "http://h", "page_size": "x"}, "must be an integer"),
        ({"base_url": "http://h", "page_size": "0"}, "between 1 and 1000"),
    ]:
        with pytest.raises(MarquezError, match=msg):
            endpoint_from_secret(bad)


def test_get_json_replay_and_404(mock_marquez: str) -> None:
    ep = endpoint_from_secret({"base_url": mock_marquez})
    body = get_json(ep, "/namespaces", {"limit": 100, "offset": 0})
    assert len(body["namespaces"]) == 9
    assert get_json(ep, "/nothing", missing_ok=True) is None
    with pytest.raises(MarquezError, match="HTTP 404"):
        get_json(ep, "/nothing")


def test_graph_rows_hops() -> None:
    graph = [
        {
            "id": "dataset:n:a",
            "type": "DATASET",
            "data": {"namespace": "n", "name": "a"},
            "inEdges": [],
            "outEdges": [{"destination": "job:n:j"}],
        },
        {
            "id": "job:n:j",
            "type": "JOB",
            "data": {"namespace": "n", "name": "j"},
            "inEdges": [{"origin": "dataset:n:a"}],
            "outEdges": [{"destination": "dataset:n:b"}],
        },
        {
            "id": "dataset:n:b",
            "type": "DATASET",
            "data": {"namespace": "n", "name": "b"},
            "inEdges": [{"origin": "job:n:j"}],
            "outEdges": [],
        },
    ]
    rows = graph_rows(graph, "dataset:n:b")
    assert [(r["node_id"], r["direction"], r["hops"]) for r in rows] == [
        ("dataset:n:b", "start", 0),
        ("job:n:j", "upstream", 1),
        ("dataset:n:a", "upstream", 2),
    ]


# ----------------------------------------------------------------------------- through the worker

SCHEMA = ["main"]


def _table(client: Client, name: str, *positional: Any, **named: Any) -> pa.Table:
    arguments = Arguments(
        positional=tuple(pa.scalar(v) for v in positional),
        named={k: pa.scalar(v) for k, v in named.items()},
    )
    batches = list(client.table_function(function_name=name, schema_path=SCHEMA, arguments=arguments))
    return pa.Table.from_batches(batches)


def test_worker_read_events_and_runs(client: Client) -> None:
    events = _table(client, "read_events", str(EVENTS))
    assert events.num_rows == 22
    runs = _table(client, "runs", str(EVENTS))
    assert runs.num_rows == 10
    statuses = sorted(str(s) for s in set(runs.column("status").to_pylist()))
    assert statuses == ["cancelled", "failed", "running", "success"]


def test_worker_lineage_named_args(client: Client) -> None:
    table = _table(client, "lineage", str(EVENTS), "analytics.customers", depth=2, direction="upstream")
    assert table.column("hops").to_pylist() == [0, 1, 2]


def test_worker_multi_batch_streams(client: Client, tmp_path: Path) -> None:
    """More than CHUNK_ROWS events and runs: several batches, resumed from the cursor."""
    lines = [
        json.dumps(_run_event(f"run-{i}", "COMPLETE", "2026-01-01T00:00:00Z", job=f"j{i % 7}")) for i in range(41_000)
    ]
    (tmp_path / "big.ndjson").write_text("\n".join(lines) + "\n")
    events = _table(client, "read_events", str(tmp_path / "big.ndjson"))
    assert events.num_rows == 41_000
    assert events.column("event_index").to_pylist()[-1] == 40_999
    runs = _table(client, "runs", str(tmp_path))
    assert runs.num_rows == 41_000
    assert len(set(runs.column("run_id").to_pylist())) == 41_000
