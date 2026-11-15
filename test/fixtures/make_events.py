"""Regenerate the checked-in OpenLineage event fixtures in ``test/fixtures/events``.

The events model a small, realistic deployment:

* Airflow DAG ``etl_orders`` (two DAG runs; the second fails in ``extract``),
  each task run carrying a ``parent`` facet that points at its DAG run.
* A Spark job launched by the Airflow ``transform`` task (``parent`` + ``root``).
* A dbt run with two model runs; one model is still running.
* A Dagster run that was aborted.
* One DatasetEvent and one JobEvent (static lineage, no run).

Column lineage is carried on the outputs' ``columnLineage`` facets and schemas on
``schema`` facets. Run: ``uv run --no-project python test/fixtures/make_events.py``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

OUT = Path(__file__).parent / "events"
SPEC = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/definitions/RunEvent"
DS_SPEC = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/definitions/DatasetEvent"
JOB_SPEC = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/definitions/JobEvent"
AIRFLOW = "https://github.com/apache/airflow/tree/providers-openlineage/1.14.0"
SPARK = "https://github.com/OpenLineage/OpenLineage/tree/1.24.2/integration/spark"
DBT = "https://github.com/OpenLineage/OpenLineage/tree/1.24.2/integration/dbt"
DAGSTER = "https://github.com/OpenLineage/OpenLineage/tree/1.24.2/integration/dagster"
FACET = "https://openlineage.io/spec/facets/1-0-1/"

RUN = {
    "dag1": "01930a6e-0000-7000-8000-000000000001",
    "extract1": "01930a6e-0000-7000-8000-000000000002",
    "transform1": "01930a6e-0000-7000-8000-000000000003",
    "dag2": "01930a6e-0000-7000-8000-000000000004",
    "extract2": "01930a6e-0000-7000-8000-000000000005",
    "spark": "01930a6e-0000-7000-8000-000000000006",
    "dbt": "01930a6e-0000-7000-8000-000000000007",
    "customers": "01930a6e-0000-7000-8000-000000000008",
    "ltv": "01930a6e-0000-7000-8000-000000000009",
    "dagster": "01930a6e-0000-7000-8000-00000000000a",
}

PG = "postgres://db.internal:5432"
S3 = "s3://lake"
BQ = "bigquery"


def facet(facet_name: str, /, **body: Any) -> dict[str, Any]:
    """A facet with the required ``_producer`` / ``_schemaURL`` keys."""
    return {"_producer": AIRFLOW, "_schemaURL": f"{FACET}{facet_name}.json", **body}


def schema(*fields: tuple[str, str, str | None]) -> dict[str, Any]:
    """A ``schema`` dataset facet."""
    return facet(
        "SchemaDatasetFacet",
        fields=[{"name": n, "type": t, **({"description": d} if d else {})} for n, t, d in fields],
    )


def col_lineage(mapping: dict[str, list[tuple[str, str, str, str, str]]]) -> dict[str, Any]:
    """A ``columnLineage`` facet: out field -> [(ns, name, field, type, subtype)]."""
    return facet(
        "ColumnLineageDatasetFacet",
        fields={
            out: {
                "inputFields": [
                    {
                        "namespace": ns,
                        "name": name,
                        "field": field,
                        "transformations": [{"type": t, "subtype": st, "description": "", "masking": False}],
                    }
                    for ns, name, field, t, st in inputs
                ]
            }
            for out, inputs in mapping.items()
        },
    )


def ds(ns: str, name: str, **facets: Any) -> dict[str, Any]:
    """A dataset reference (every one names its ``dataSource``, as real integrations do)."""
    source = facet("DatasourceDatasetFacet", name=ns.split("://")[0] if "://" in ns else ns, uri=ns)
    return {"namespace": ns, "name": name, "facets": {"dataSource": source, **facets}}


ORDERS_SCHEMA = schema(
    ("order_id", "INTEGER", "Primary key"),
    ("customer_id", "INTEGER", None),
    ("amount", "NUMERIC(12,2)", "Order total in USD"),
    ("ordered_at", "TIMESTAMP", None),
)
DAILY_SCHEMA = schema(("order_date", "DATE", None), ("customer_id", "INT64", None), ("revenue", "NUMERIC", None))
ENRICHED_SCHEMA = schema(
    ("order_date", "DATE", None),
    ("customer_id", "INT64", None),
    ("revenue", "NUMERIC", None),
    ("segment", "STRING", "Customer segment"),
)
CUSTOMERS_SCHEMA = schema(("customer_id", "INT64", None), ("lifetime_revenue", "NUMERIC", None))


def run_event(
    event_type: str,
    time: str,
    run: str,
    ns: str,
    job: str,
    *,
    producer: str = AIRFLOW,
    run_facets: dict[str, Any] | None = None,
    job_facets: dict[str, Any] | None = None,
    inputs: list[dict[str, Any]] | None = None,
    outputs: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """One RunEvent."""
    return {
        "eventType": event_type,
        "eventTime": time,
        "run": {"runId": RUN[run], "facets": run_facets or {}},
        "job": {"namespace": ns, "name": job, "facets": job_facets or {}},
        "inputs": inputs or [],
        "outputs": outputs or [],
        "producer": producer,
        "schemaURL": SPEC,
    }


def parent(run: str, ns: str, job: str, root: tuple[str, str, str] | None = None) -> dict[str, Any]:
    """A ``parent`` run facet."""
    body: dict[str, Any] = {"run": {"runId": RUN[run]}, "job": {"namespace": ns, "name": job}}
    if root:
        body["root"] = {"run": {"runId": RUN[root[0]]}, "job": {"namespace": root[1], "name": root[2]}}
    return facet("ParentRunFacet", **body)


def job_type(integration: str, kind: str, processing: str = "BATCH") -> dict[str, Any]:
    """A ``jobType`` job facet."""
    return facet("JobTypeJobFacet", processingType=processing, integration=integration, jobType=kind)


def airflow_task(try_number: int, run_type: str) -> dict[str, Any]:
    """A trimmed Airflow ``airflow`` run facet."""
    return facet("AirflowRunFacet", dagRun={"run_type": run_type}, taskInstance={"try_number": try_number})


def airflow() -> list[dict[str, Any]]:
    """Two DAG runs of etl_orders; the second fails in extract."""
    ns = "airflow-prod"
    dag_facets = {"jobType": job_type("AIRFLOW", "DAG")}
    task_facets = {"jobType": job_type("AIRFLOW", "TASK")}
    nominal = facet(
        "NominalTimeRunFacet", nominalStartTime="2026-11-01T02:00:00Z", nominalEndTime="2026-11-02T02:00:00Z"
    )
    raw_orders = ds(S3, "raw/orders", schema=ORDERS_SCHEMA)
    daily = ds(
        BQ,
        "analytics.orders_daily",
        schema=DAILY_SCHEMA,
        columnLineage=col_lineage(
            {
                "order_date": [(S3, "raw/orders", "ordered_at", "DIRECT", "TRANSFORMATION")],
                "customer_id": [(S3, "raw/orders", "customer_id", "DIRECT", "IDENTITY")],
                "revenue": [(S3, "raw/orders", "amount", "DIRECT", "AGGREGATION")],
            }
        ),
    )
    p1 = parent("dag1", ns, "etl_orders")
    p2 = parent("dag2", ns, "etl_orders")
    return [
        run_event(
            "START",
            "2026-11-02T02:00:05Z",
            "dag1",
            ns,
            "etl_orders",
            run_facets={"nominalTime": nominal, "airflow": airflow_task(1, "scheduled")},
            job_facets=dag_facets,
        ),
        run_event(
            "START",
            "2026-11-02T02:00:10Z",
            "extract1",
            ns,
            "etl_orders.extract",
            run_facets={"parent": p1, "airflow": airflow_task(1, "scheduled")},
            job_facets=task_facets,
            inputs=[ds(PG, "shop.public.orders", schema=ORDERS_SCHEMA)],
        ),
        run_event(
            "COMPLETE",
            "2026-11-02T02:03:10Z",
            "extract1",
            ns,
            "etl_orders.extract",
            run_facets={"parent": p1},
            job_facets=task_facets,
            inputs=[ds(PG, "shop.public.orders", schema=ORDERS_SCHEMA)],
            outputs=[
                ds(
                    S3,
                    "raw/orders",
                    schema=ORDERS_SCHEMA,
                    columnLineage=col_lineage(
                        {
                            c: [(PG, "shop.public.orders", c, "DIRECT", "IDENTITY")]
                            for c in ("order_id", "customer_id", "amount", "ordered_at")
                        }
                    ),
                )
            ],
        ),
        run_event(
            "START",
            "2026-11-02T02:03:20Z",
            "transform1",
            ns,
            "etl_orders.transform",
            run_facets={"parent": p1, "airflow": airflow_task(2, "scheduled")},
            job_facets=task_facets,
            inputs=[raw_orders],
        ),
        run_event(
            "COMPLETE",
            "2026-11-02T02:10:20Z",
            "transform1",
            ns,
            "etl_orders.transform",
            run_facets={"parent": p1},
            job_facets=task_facets,
            inputs=[raw_orders],
            outputs=[daily],
        ),
        run_event("COMPLETE", "2026-11-02T02:10:30Z", "dag1", ns, "etl_orders", job_facets=dag_facets),
        run_event(
            "START",
            "2026-11-03T02:00:05Z",
            "dag2",
            ns,
            "etl_orders",
            run_facets={"airflow": airflow_task(1, "manual")},
            job_facets=dag_facets,
        ),
        run_event(
            "START",
            "2026-11-03T02:00:10Z",
            "extract2",
            ns,
            "etl_orders.extract",
            run_facets={"parent": p2, "airflow": airflow_task(3, "manual")},
            job_facets=task_facets,
            inputs=[ds(PG, "shop.public.orders")],
        ),
        run_event(
            "FAIL",
            "2026-11-03T02:01:40Z",
            "extract2",
            ns,
            "etl_orders.extract",
            run_facets={
                "parent": p2,
                "errorMessage": facet(
                    "ErrorMessageRunFacet",
                    message="psycopg2.OperationalError: connection refused",
                    programmingLanguage="python",
                    stackTrace="Traceback (most recent call last): ...",
                ),
            },
            job_facets=task_facets,
            inputs=[ds(PG, "shop.public.orders")],
        ),
        run_event("FAIL", "2026-11-03T02:01:50Z", "dag2", ns, "etl_orders", job_facets=dag_facets),
    ]


def spark() -> list[dict[str, Any]]:
    """A Spark job launched by the Airflow transform task."""
    ns = "spark-cluster"
    job = "orders_enrich.execute_insert_into_hadoop_fs_relation_command"
    engine = facet("ProcessingEngineRunFacet", version="3.5.1", name="spark", openlineageAdapterVersion="1.24.2")
    p = parent("transform1", "airflow-prod", "etl_orders.transform", root=("dag1", "airflow-prod", "etl_orders"))
    facets = {"jobType": job_type("SPARK", "SQL_JOB")}
    inputs = [ds(BQ, "analytics.orders_daily", schema=DAILY_SCHEMA), ds(PG, "crm.public.customers")]
    out = ds(
        BQ,
        "analytics.orders_enriched",
        schema=ENRICHED_SCHEMA,
        columnLineage=col_lineage(
            {
                "order_date": [(BQ, "analytics.orders_daily", "order_date", "DIRECT", "IDENTITY")],
                "customer_id": [(BQ, "analytics.orders_daily", "customer_id", "DIRECT", "IDENTITY")],
                "revenue": [(BQ, "analytics.orders_daily", "revenue", "DIRECT", "IDENTITY")],
                "segment": [
                    (PG, "crm.public.customers", "tier", "DIRECT", "TRANSFORMATION"),
                    (BQ, "analytics.orders_daily", "revenue", "INDIRECT", "CONDITIONAL"),
                ],
            }
        ),
    )
    return [
        run_event(
            "START",
            "2026-11-02T02:04:00Z",
            "spark",
            ns,
            job,
            producer=SPARK,
            run_facets={
                "parent": p,
                "processing_engine": engine,
                "spark_properties": facet(
                    "SparkPropertiesRunFacet", properties={"spark.master": "yarn", "spark.app.name": "orders_enrich"}
                ),
            },
            job_facets=facets,
            inputs=inputs,
        ),
        run_event(
            "RUNNING",
            "2026-11-02T02:06:00Z",
            "spark",
            ns,
            job,
            producer=SPARK,
            run_facets={"parent": p, "processing_engine": engine},
            job_facets=facets,
            inputs=inputs,
        ),
        run_event(
            "COMPLETE",
            "2026-11-02T02:09:30Z",
            "spark",
            ns,
            job,
            producer=SPARK,
            run_facets={"parent": p, "processing_engine": engine},
            job_facets=facets,
            inputs=inputs,
            outputs=[out],
        ),
    ]


def dbt() -> list[dict[str, Any]]:
    """A dbt run with two model runs; ``ltv`` has started but not finished."""
    ns = "dbt"
    run_facets = {
        "processing_engine": facet(
            "ProcessingEngineRunFacet", version="1.8.2", name="dbt", openlineageAdapterVersion="1.24.2"
        )
    }
    p = parent("dbt", ns, "dbt-run-jaffle_shop")
    model = {"jobType": job_type("DBT", "MODEL")}
    customers_out = ds(
        BQ,
        "analytics.customers",
        schema=CUSTOMERS_SCHEMA,
        columnLineage=col_lineage(
            {
                "customer_id": [(BQ, "analytics.orders_enriched", "customer_id", "DIRECT", "IDENTITY")],
                "lifetime_revenue": [(BQ, "analytics.orders_enriched", "revenue", "DIRECT", "AGGREGATION")],
            }
        ),
    )
    return [
        run_event(
            "START",
            "2026-11-02T03:00:00Z",
            "dbt",
            ns,
            "dbt-run-jaffle_shop",
            producer=DBT,
            run_facets=run_facets,
            job_facets={"jobType": job_type("DBT", "JOB")},
        ),
        run_event(
            "START",
            "2026-11-02T03:00:02Z",
            "customers",
            ns,
            "jaffle_shop.customers",
            producer=DBT,
            run_facets={**run_facets, "parent": p},
            job_facets=model,
            inputs=[ds(BQ, "analytics.orders_enriched")],
        ),
        run_event(
            "COMPLETE",
            "2026-11-02T03:00:40Z",
            "customers",
            ns,
            "jaffle_shop.customers",
            producer=DBT,
            run_facets={**run_facets, "parent": p},
            job_facets=model,
            inputs=[ds(BQ, "analytics.orders_enriched")],
            outputs=[customers_out],
        ),
        run_event(
            "START",
            "2026-11-02T03:00:41Z",
            "ltv",
            ns,
            "jaffle_shop.customer_ltv",
            producer=DBT,
            run_facets={**run_facets, "parent": p},
            job_facets=model,
            inputs=[ds(BQ, "analytics.customers")],
        ),
    ]


def dagster() -> list[dict[str, Any]]:
    """An aborted Dagster run (OTHER before START, as some integrations emit)."""
    ns = "dagster"
    return [
        run_event("OTHER", "2026-11-04T10:00:00Z", "dagster", ns, "nightly_snapshot", producer=DAGSTER),
        run_event(
            "START",
            "2026-11-04T10:00:03Z",
            "dagster",
            ns,
            "nightly_snapshot",
            producer=DAGSTER,
            inputs=[ds(BQ, "analytics.customers")],
        ),
        run_event(
            "ABORT",
            "2026-11-04T10:05:03Z",
            "dagster",
            ns,
            "nightly_snapshot",
            producer=DAGSTER,
            inputs=[ds(BQ, "analytics.customers")],
        ),
    ]


def static() -> list[dict[str, Any]]:
    """One DatasetEvent and one JobEvent (lineage without a run)."""
    return [
        {
            "eventTime": "2026-11-05T09:00:00Z",
            "dataset": ds(
                BQ,
                "analytics.customers",
                schema=CUSTOMERS_SCHEMA,
                documentation=facet(
                    "DocumentationDatasetFacet", description="One row per customer with lifetime revenue."
                ),
            ),
            "producer": "https://github.com/OpenLineage/OpenLineage/tree/1.24.2/client/python",
            "schemaURL": DS_SPEC,
        },
        {
            "eventTime": "2026-11-05T09:30:00Z",
            "job": {
                "namespace": "reports",
                "name": "weekly_revenue",
                "facets": {
                    "jobType": facet("JobTypeJobFacet", processingType="BATCH", integration="LOOKER", jobType="QUERY")
                },
            },
            "inputs": [ds(BQ, "analytics.customers")],
            "outputs": [],
            "producer": "https://github.com/OpenLineage/OpenLineage/tree/1.24.2/client/python",
            "schemaURL": JOB_SPEC,
        },
    ]


def main() -> None:
    """Write the fixture files."""
    OUT.mkdir(exist_ok=True)
    # NDJSON (the OpenLineage file transport's format)
    (OUT / "airflow.ndjson").write_text("".join(json.dumps(e, sort_keys=True) + "\n" for e in airflow()))
    # A JSON array
    (OUT / "spark.json").write_text(json.dumps(spark(), indent=2, sort_keys=True) + "\n")
    # One event per file
    for i, event in enumerate(dbt(), 1):
        (OUT / f"dbt-{i:02d}.json").write_text(json.dumps(event, indent=2, sort_keys=True) + "\n")
    (OUT / "dagster.ndjson").write_text("".join(json.dumps(e, sort_keys=True) + "\n" for e in dagster()))
    (OUT / "static.ndjson").write_text("".join(json.dumps(e, sort_keys=True) + "\n" for e in static()))
    # Everything in one NDJSON file, oldest first: what the metadata examples read by URL.
    every = sorted(airflow() + spark() + dbt() + dagster() + static(), key=lambda e: str(e["eventTime"]))
    (OUT.parent / "all-events.ndjson").write_text("".join(json.dumps(e, sort_keys=True) + "\n" for e in every))


if __name__ == "__main__":
    main()
