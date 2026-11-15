"""Shared building blocks: Arrow types, batch building, catalog tables and pushdown helpers."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import pyarrow as pa
from vgi.catalog import Table
from vgi.catalog.constraints import constraint_record
from vgi.table_filter_pushdown import ComparisonOp, ConstantFilter, InFilter
from vgi.table_function import TableFunctionGenerator

TSTZ = pa.timestamp("us", tz="UTC")
JSON = pa.json_()
STR_LIST = pa.list_(pa.string())
DATASET_REF = pa.struct([pa.field("namespace", pa.string()), pa.field("name", pa.string())])
DATASET_REFS = pa.list_(DATASET_REF)
DATASET_VERSION = pa.struct(
    [pa.field("namespace", pa.string()), pa.field("name", pa.string()), pa.field("version", pa.string())]
)
DATASET_VERSIONS = pa.list_(DATASET_VERSION)
FIELD = pa.struct(
    [
        pa.field("name", pa.string()),
        pa.field("type", pa.string()),
        pa.field("description", pa.string()),
        pa.field("tags", STR_LIST),
    ]
)
FIELDS = pa.list_(FIELD)


@dataclass(slots=True, frozen=True, kw_only=True)
class NoArgs:
    """Catalog tables take no arguments."""


def rows_to_batch(rows: list[dict[str, Any]], schema: pa.Schema) -> pa.RecordBatch:
    """Build a batch of ``schema`` (the projected output schema) from row dicts."""
    columns = {f.name: [row.get(f.name) for row in rows] for f in schema}
    return pa.RecordBatch.from_pydict(columns, schema=schema)


def duckdb_type(t: pa.DataType) -> str:
    """Return the DuckDB SQL name of an Arrow type used in this worker's schemas."""
    if isinstance(t, pa.BaseExtensionType) and t.extension_name == "arrow.json":
        return "JSON"
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return "VARCHAR"
    if pa.types.is_int64(t):
        return "BIGINT"
    if pa.types.is_int32(t):
        return "INTEGER"
    if pa.types.is_float64(t):
        return "DOUBLE"
    if pa.types.is_boolean(t):
        return "BOOLEAN"
    if pa.types.is_timestamp(t):
        return "TIMESTAMP WITH TIME ZONE" if t.tz else "TIMESTAMP"
    if pa.types.is_list(t):
        return f"{duckdb_type(t.value_type)}[]"
    if pa.types.is_struct(t):
        return (
            "STRUCT("
            + ", ".join(f"{t.field(i).name} {duckdb_type(t.field(i).type)}" for i in range(t.num_fields))
            + ")"
        )
    raise TypeError(f"no DuckDB name for {t}")


def result_columns(schema: pa.Schema) -> str:
    """``vgi.result_columns_schema`` JSON for a fixed output schema."""
    return json.dumps(
        [
            {
                "name": f.name,
                "type": duckdb_type(f.type),
                "description": (f.metadata or {}).get(b"comment", b"").decode(),
            }
            for f in schema
        ]
    )


def catalog_table(
    name: str,
    function: type[TableFunctionGenerator[Any, Any]],
    *,
    comment: str,
    tags: dict[str, str],
    schema_path: list[str],
    primary_key: list[str] | None = None,
) -> Table:
    """Build a catalog table over ``function`` with NOT NULL records for its non-nullable columns."""
    fields: pa.Schema = function.FIXED_SCHEMA  # type: ignore[attr-defined]
    constraints = [
        constraint_record(
            schema_path,
            name,
            f"{f.name}_not_null",
            "NOT_NULL",
            [[f.name]],
            enforcement="ENFORCED",
            validation="ALL_ROWS",
        )
        for f in fields
        if not f.nullable
    ]
    if primary_key:
        constraints.append(
            constraint_record(
                schema_path,
                name,
                "pk",
                "PRIMARY_KEY",
                [[c] for c in primary_key],
                enforcement="ENFORCED",
                validation="ALL_ROWS",
            )
        )
    comments = {
        f.name: (f.metadata or {}).get(b"comment", b"").decode() for f in fields if (f.metadata or {}).get(b"comment")
    }
    return Table(
        name=name,
        function=function,
        comment=comment,
        tags=tags,
        constraints=tuple(constraints),
        column_comments=comments,
    )


# ----------------------------------------------------------------------------- pushdown helpers


def _scalar(value: Any) -> Any:
    return value.as_py() if hasattr(value, "as_py") else value


def eq_values(filters: Any, column: str) -> list[Any] | None:
    """Values ``column`` is constrained to by top-level ``=`` / ``IN`` filters, else None.

    Several such filters on one column intersect; an empty list means no row matches.
    """
    found: set[Any] | None = None
    for f in filters.filters if filters else ():
        if f.column_name != column:
            continue
        if isinstance(f, ConstantFilter) and f.op is ComparisonOp.EQ:
            values = {_scalar(f.value)}
        elif isinstance(f, InFilter):
            values = {v for v in f.values.to_pylist() if v is not None}
        else:
            continue
        found = values if found is None else found & values
    return None if found is None else sorted(found, key=str)


def range_bounds(filters: Any, column: str) -> tuple[Any, Any]:
    """Loosest-safe ``(lower, upper)`` bounds from top-level comparisons on ``column``.

    Both bounds are inclusive; the SDK re-applies the exact predicates afterwards.
    """
    lower: Any = None
    upper: Any = None
    for f in filters.filters if filters else ():
        if f.column_name != column or not isinstance(f, ConstantFilter):
            continue
        value = _scalar(f.value)
        if value is None:
            continue
        if f.op in (ComparisonOp.GT, ComparisonOp.GE, ComparisonOp.EQ):
            lower = value if lower is None else max(lower, value)
        if f.op in (ComparisonOp.LT, ComparisonOp.LE, ComparisonOp.EQ):
            upper = value if upper is None else min(upper, value)
    return lower, upper
