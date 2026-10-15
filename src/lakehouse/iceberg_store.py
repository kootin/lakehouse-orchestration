"""Iceberg tables managed with PyIceberg: partitioned writes, upserts, time travel."""
from __future__ import annotations

from pathlib import Path

import polars as pl
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.expressions import EqualTo
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.types import (
    DateType, DoubleType, LongType, NestedField, StringType, TimestampType,
)

from lakehouse import config

NAMESPACE = "payments"

TXN_SCHEMA = Schema(
    NestedField(1, "txn_id", StringType(), required=True),
    NestedField(2, "authorized_at", TimestampType(), required=True),
    NestedField(3, "card_last4", StringType()),
    NestedField(4, "amount", DoubleType()),
    NestedField(5, "currency", StringType()),
    NestedField(6, "amount_usd", DoubleType()),
    NestedField(7, "merchant_id", StringType()),
    NestedField(8, "merchant_name", StringType()),
    NestedField(9, "merchant_mcc", StringType()),
    NestedField(10, "merchant_country", StringType()),
    NestedField(11, "status", StringType()),
    NestedField(12, "status_updated_at", TimestampType()),
    identifier_field_ids=[1],
)
TXN_SPEC = PartitionSpec(PartitionField(source_id=2, field_id=1000, transform=DayTransform(), name="authorized_day"))

SUMMARY_SCHEMA = Schema(
    NestedField(1, "txn_date", DateType(), required=True),
    NestedField(2, "merchant_id", StringType(), required=True),
    NestedField(3, "merchant_name", StringType()),
    NestedField(4, "txn_count", LongType()),
    NestedField(5, "gross_usd", DoubleType()),
    NestedField(6, "settled_count", LongType()),
    NestedField(7, "declined_count", LongType()),
)
SUMMARY_SPEC = PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="txn_date"))


def get_catalog(root: Path | None = None) -> SqlCatalog:
    root = root or config.ROOT
    (root / "warehouse").mkdir(parents=True, exist_ok=True)
    return SqlCatalog(
        "lake", uri=f"sqlite:///{root / 'catalog.db'}", warehouse=f"file://{root / 'warehouse'}"
    )


def ensure_tables(catalog: SqlCatalog) -> tuple[Table, Table]:
    catalog.create_namespace_if_not_exists(NAMESPACE)
    txns = catalog.create_table_if_not_exists(f"{NAMESPACE}.transactions", TXN_SCHEMA, partition_spec=TXN_SPEC)
    summary = catalog.create_table_if_not_exists(
        f"{NAMESPACE}.daily_merchant_summary", SUMMARY_SCHEMA, partition_spec=SUMMARY_SPEC
    )
    return txns, summary


def _arrow(df: pl.DataFrame, schema: Schema) -> pa.Table:
    return df.to_arrow().cast(schema.as_arrow())


def upsert_authorizations(table: Table, auths: pl.DataFrame) -> dict:
    """Insert new transactions; re-delivered ones are matched on txn_id and left as-is."""
    if auths.is_empty():
        return {"inserted": 0}
    existing = set(table.scan(selected_fields=("txn_id",)).to_arrow()["txn_id"].to_pylist())
    new = auths.filter(~pl.col("txn_id").is_in(list(existing)))
    if not new.is_empty():
        table.append(_arrow(new, TXN_SCHEMA))
    return {"inserted": new.height}


def apply_status_updates(table: Table, updates: pl.DataFrame) -> dict:
    """MERGE late-arriving settlements into existing rows (copy-on-write upsert on txn_id)."""
    if updates.is_empty():
        return {"updated": 0, "unmatched": 0}
    current = pl.from_arrow(
        table.scan(row_filter="txn_id IS NOT NULL").to_arrow()
    ).filter(pl.col("txn_id").is_in(updates["txn_id"].implode()))
    merged = (
        current.drop("status", "status_updated_at")
        .join(updates, on="txn_id", how="inner")
        .select(TXN_SCHEMA.column_names)
    )
    if not merged.is_empty():
        result = table.upsert(_arrow(merged, TXN_SCHEMA), join_cols=["txn_id"])
        updated = result.rows_updated
    else:
        updated = 0
    return {"updated": updated, "unmatched": updates.height - merged.height}


def overwrite_summary_partitions(table: Table, summary: pl.DataFrame) -> None:
    """Replace only the txn_date partitions present in `summary` (idempotent rebuilds)."""
    table.dynamic_partition_overwrite(_arrow(summary, SUMMARY_SCHEMA))


def read_table(table: Table, snapshot_id: int | None = None, **filters) -> pl.DataFrame:
    expr = None
    for col, val in filters.items():
        expr = EqualTo(col, val) if expr is None else expr & EqualTo(col, val)
    scan = table.scan(snapshot_id=snapshot_id) if expr is None else table.scan(row_filter=expr, snapshot_id=snapshot_id)
    return pl.from_arrow(scan.to_arrow())
