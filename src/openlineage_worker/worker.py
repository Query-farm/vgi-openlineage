"""Assemble the ``openlineage`` catalog and run the worker.

Run it:

    vgi-openlineage-worker                      # stdio: what DuckDB launches by default
    vgi-openlineage-worker --http --port 8000   # HTTP: for Docker / hosted deploys

Attach it from DuckDB:

    ATTACH 'openlineage' (TYPE vgi, LOCATION '/abs/path/to/.venv/bin/vgi-openlineage-worker');
    SELECT * FROM openlineage.main.runs('/var/log/openlineage/*.ndjson') WHERE status = 'failed';

    CREATE SECRET (TYPE marquez, BASE_URL 'http://localhost:5000');
    SELECT * FROM openlineage.marquez.jobs;
"""

from __future__ import annotations

import json
from typing import Any

from vgi import Worker
from vgi.catalog import Catalog, Schema, Table

from .common import catalog_table
from .event_functions import EVENT_FUNCTIONS
from .marquez import MARQUEZ_FUNCTIONS, MARQUEZ_TABLES, UNIFIED_TABLES
from .marquez_client import MARQUEZ_SECRET_TYPE
from .meta import (
    CATALOG_COMMENT,
    CATALOG_NAME,
    CATALOG_TAGS,
    MAIN_DOC_LLM,
    MAIN_DOC_MD,
    MARQUEZ_DOC_LLM,
    MARQUEZ_DOC_MD,
    SOURCE_URL,
    UNIFIED_DOC_LLM,
    UNIFIED_DOC_MD,
    categories_for,
    keywords_array,
)
from .scalars import Orchestrator, RunStatus


def _example_list(objects: list[type[Any]]) -> list[dict[str, str]]:
    return [{"description": ex.description, "sql": ex.sql} for fn in objects for ex in getattr(fn.Meta, "examples", [])]


def _stamp_example_queries(functions: list[type[Any]]) -> None:
    """Publish each object's examples as ``vgi.example_queries``."""
    for fn in functions:
        examples = [{"description": ex.description, "sql": ex.sql} for ex in getattr(fn.Meta, "examples", [])]
        fn.Meta.tags = {**fn.Meta.tags, "vgi.example_queries": json.dumps(examples)}


def _tables(entries: list[tuple[str, type[Any], list[str] | None]], schema: str) -> list[Table]:
    tables = []
    for name, scan, pk in entries:
        tags = {**scan.Meta.tags, "domain": "orchestration" if schema == "unified" else "data lineage"}
        tables.append(
            catalog_table(
                name,
                scan,
                comment=scan.Meta.description,
                tags=tags,
                schema_path=[schema],
                primary_key=pk,
            )
        )
    return tables


_MARQUEZ_SCANS = [scan for _, scan, _ in MARQUEZ_TABLES]
_UNIFIED_SCANS = [scan for _, scan, _ in UNIFIED_TABLES]
MAIN_FUNCTIONS: list[type[Any]] = [*EVENT_FUNCTIONS, Orchestrator, RunStatus]
_stamp_example_queries(MAIN_FUNCTIONS + MARQUEZ_FUNCTIONS + _MARQUEZ_SCANS + _UNIFIED_SCANS)


class OpenlineageWorker(Worker):
    """The worker process: the ``openlineage`` catalog with ``main``, ``marquez`` and ``unified``."""

    catalog = Catalog(
        name=CATALOG_NAME,
        comment=CATALOG_COMMENT,
        source_url=SOURCE_URL,
        tags=CATALOG_TAGS,
        schemas=[
            Schema(
                path=["main"],
                comment="Table functions over raw OpenLineage event JSON files, globs and URLs.",
                functions=MAIN_FUNCTIONS,
                tags={
                    "vgi.title": "OpenLineage Event Files",
                    "domain": "data lineage",
                    "vgi.keywords": keywords_array(
                        "openlineage events, ndjson, file transport, runs, column lineage, lineage traversal"
                    ),
                    "vgi.doc_llm": MAIN_DOC_LLM,
                    "vgi.doc_md": MAIN_DOC_MD,
                    "vgi.categories": json.dumps(categories_for("events", "lineage", "unified")),
                    "vgi.example_queries": json.dumps(_example_list(MAIN_FUNCTIONS)),
                },
            ),
            Schema(
                path=["marquez"],
                comment="Namespaces, sources, datasets, jobs, runs and lineage from a Marquez API server.",
                functions=MARQUEZ_FUNCTIONS,
                tables=_tables(MARQUEZ_TABLES, "marquez"),
                tags={
                    "vgi.title": "Marquez Lineage Server",
                    "domain": "data lineage",
                    "vgi.keywords": keywords_array(
                        "marquez, datasets, jobs, runs, lineage graph, column lineage, search"
                    ),
                    "vgi.doc_llm": MARQUEZ_DOC_LLM,
                    "vgi.doc_md": MARQUEZ_DOC_MD,
                    "vgi.categories": json.dumps(categories_for("marquez", "lineage")),
                    "vgi.example_queries": json.dumps(_example_list(_MARQUEZ_SCANS + MARQUEZ_FUNCTIONS)),
                },
            ),
            Schema(
                path=["unified"],
                comment="Marquez runs and task runs in the column layout shared by every orchestrator worker.",
                tables=_tables(UNIFIED_TABLES, "unified"),
                tags={
                    "vgi.title": "Unified Orchestrator Runs",
                    "domain": "orchestration",
                    "vgi.keywords": keywords_array("unified, cross orchestrator, runs, task runs, union all"),
                    "vgi.doc_llm": UNIFIED_DOC_LLM,
                    "vgi.doc_md": UNIFIED_DOC_MD,
                    "vgi.categories": json.dumps(categories_for("unified")),
                    "vgi.example_queries": json.dumps(_example_list(_UNIFIED_SCANS)),
                },
            ),
        ],
    )

    secret_types = [MARQUEZ_SECRET_TYPE]


def main() -> None:
    """Console-script entry point: parse ``--http`` / ``--unix`` / logging flags and serve."""
    OpenlineageWorker.main()


if __name__ == "__main__":
    main()
