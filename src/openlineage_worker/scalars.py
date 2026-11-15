"""Scalars for OpenLineage events already loaded into DuckDB (e.g. with ``read_json``).

``orchestrator(producer)`` names the system behind a producer URI and
``run_status(event_type)`` maps an eventType to the shared orchestrator status
vocabulary. Both apply the same rules as the table functions, so a hand-loaded
event table classifies exactly like ``openlineage.main.runs``.
"""

from __future__ import annotations

import json
from typing import Annotated

import pyarrow as pa
from vgi.arguments import Param, Returns
from vgi.metadata import FunctionExample
from vgi.scalar_function import ScalarFunction

from . import events as ev
from .meta import object_tags

EVENTS_URL = "https://raw.githubusercontent.com/Query-farm/vgi-openlineage/v0.1.0/test/fixtures/all-events.ndjson"


class Orchestrator(ScalarFunction):
    """``orchestrator(producer)``."""

    class Meta:
        """Function metadata."""

        name = "orchestrator"
        description = "Name the orchestrator or engine (airflow, dbt, spark, ...) behind an OpenLineage producer URI"
        tags = {
            **object_tags(
                title="Orchestrator From Producer",
                category="events",
                keywords="openlineage, producer uri, orchestrator, integration, airflow, spark, dbt, classify",
                doc_llm=(
                    "Classify an OpenLineage `producer` URI: 'airflow' (apache/airflow, astronomer), 'dagster', "
                    "'dbt', 'spark' (also databricks), 'flink', 'prefect', 'great_expectations', 'trino', 'hive', "
                    "else the directory after 'integration/' in OpenLineage repo URIs, else 'unknown'. NULL in, NULL "
                    "out. The table functions also look at the jobType and processing_engine facets first; use this "
                    "on events you loaded yourself."
                ),
                doc_md=(
                    "## orchestrator(producer)\n\n"
                    "The system behind a producer URI, e.g. 'airflow' or 'spark'.\n\n"
                    "NULL in, NULL out; unrecognised producers give 'unknown'."
                ),
            ),
            "vgi.executable_examples": json.dumps(
                [
                    {
                        "description": "Classify an Airflow provider and the Spark integration",
                        "sql": (
                            "SELECT openlineage.main.orchestrator("
                            "'https://github.com/apache/airflow/tree/providers-openlineage/1.14.0') AS a, "
                            "openlineage.main.orchestrator("
                            "'https://github.com/OpenLineage/OpenLineage/tree/1.24.2/integration/spark') AS b"
                        ),
                        "expected_result": [["airflow", "spark"]],
                    }
                ]
            ),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT openlineage.main.orchestrator(producer) AS orchestrator, count(*) AS events "
                    f"FROM read_json('{EVENTS_URL}', format = 'newline_delimited') GROUP BY ALL ORDER BY events DESC"
                ),
                description="Events per orchestrator in a hand-loaded NDJSON file",
            ),
        ]

    @classmethod
    def compute(
        cls,
        producer: Annotated[pa.StringArray, Param(doc="The event's producer URI; NULL in, NULL out.")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Classify every producer."""
        values = [None if p is None else ev.detect_orchestrator(p, {}, {}) for p in producer.to_pylist()]
        return pa.array(values, type=pa.string())


class RunStatus(ScalarFunction):
    """``run_status(event_type)``."""

    class Meta:
        """Function metadata."""

        name = "run_status"
        description = "Map an OpenLineage eventType to queued, running, success, failed or cancelled"
        tags = {
            **object_tags(
                title="Run Status From Event Type",
                category="events",
                keywords="openlineage, event type, run state, status, complete, fail, abort, normalize",
                doc_llm=(
                    "Map an OpenLineage RunEvent eventType (case-insensitive) to the shared orchestrator status: "
                    "START and RUNNING -> 'running', COMPLETE -> 'success', FAIL -> 'failed', ABORT -> 'cancelled', "
                    "OTHER -> 'queued'. Unknown values and NULL give NULL."
                ),
                doc_md=(
                    "## run_status(event_type)\n\n"
                    "The normalized status for an eventType: `COMPLETE` gives 'success'.\n\n"
                    "Unknown values and NULL give NULL."
                ),
            ),
            "vgi.executable_examples": json.dumps(
                [
                    {
                        "description": "Normalize every OpenLineage eventType",
                        "sql": (
                            "SELECT t, openlineage.main.run_status(t) AS status FROM (VALUES ('START'), ('RUNNING'), "
                            "('COMPLETE'), ('FAIL'), ('ABORT'), ('OTHER')) v(t) ORDER BY t"
                        ),
                        "expected_result": [
                            ["ABORT", "cancelled"],
                            ["COMPLETE", "success"],
                            ["FAIL", "failed"],
                            ["OTHER", "queued"],
                            ["RUNNING", "running"],
                            ["START", "running"],
                        ],
                    }
                ]
            ),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT openlineage.main.run_status(eventType) AS status, count(*) AS events "
                    f"FROM read_json('{EVENTS_URL}', format = 'newline_delimited') GROUP BY ALL ORDER BY status"
                ),
                description="Events per normalized status in a hand-loaded NDJSON file",
            ),
        ]

    @classmethod
    def compute(
        cls,
        event_type: Annotated[pa.StringArray, Param(doc="An eventType such as 'COMPLETE'; NULL in, NULL out.")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Normalize every eventType."""
        values = [None if t is None else ev.EVENT_STATUS.get(str(t).upper()) for t in event_type.to_pylist()]
        return pa.array(values, type=pa.string())
