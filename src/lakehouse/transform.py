"""Polars transforms from raw JSON events to typed, conformed rows."""
from __future__ import annotations

import json
from pathlib import Path

import polars as pl

FX_TO_USD = {"USD": 1.0, "GBP": 1.27}


def read_events(path: Path) -> tuple[pl.DataFrame, int]:
    """Parse NDJSON, skipping corrupt lines. Returns (events, bad_line_count)."""
    good, bad = [], 0
    for line in path.read_text().splitlines():
        try:
            good.append(json.loads(line))
        except json.JSONDecodeError:
            bad += 1
    return pl.from_dicts(good, infer_schema_length=None), bad


def authorizations(events: pl.DataFrame) -> pl.DataFrame:
    """New transactions: flatten nested structs, cast types, convert to USD."""
    fx = pl.DataFrame({"currency": list(FX_TO_USD), "fx_rate": list(FX_TO_USD.values())})
    return (
        events.filter(pl.col("event_type") == "AUTHORIZED")
        .unnest("amount")
        .rename({"value": "amount"})
        .unnest("merchant", separator="_")
        .with_columns(pl.col("event_ts").str.to_datetime(time_unit="us"))
        .join(fx, on="currency", how="left")
        .with_columns((pl.col("amount") * pl.col("fx_rate")).round(2).alias("amount_usd"))
        .select(
            "txn_id",
            pl.col("event_ts").alias("authorized_at"),
            "card_last4",
            "amount",
            "currency",
            "amount_usd",
            "merchant_id",
            "merchant_name",
            "merchant_mcc",
            "merchant_country",
            pl.lit("AUTHORIZED").alias("status"),
            pl.lit(None, dtype=pl.Datetime("us")).alias("status_updated_at"),
        )
        .unique("txn_id", keep="first")
    )


def status_updates(events: pl.DataFrame) -> pl.DataFrame:
    """Latest settlement/decline per transaction."""
    return (
        events.filter(pl.col("event_type") != "AUTHORIZED")
        .with_columns(pl.col("event_ts").str.to_datetime(time_unit="us"))
        .sort("event_ts")
        .group_by("txn_id")
        .agg(pl.col("event_type").last().alias("status"), pl.col("event_ts").last().alias("status_updated_at"))
    )


def daily_merchant_summary(txns: pl.DataFrame) -> pl.DataFrame:
    return (
        txns.with_columns(pl.col("authorized_at").dt.date().alias("txn_date"))
        .group_by("txn_date", "merchant_id", "merchant_name")
        .agg(
            pl.len().cast(pl.Int64).alias("txn_count"),
            pl.col("amount_usd").sum().round(2).alias("gross_usd"),
            (pl.col("status") == "SETTLED").sum().cast(pl.Int64).alias("settled_count"),
            (pl.col("status") == "DECLINED").sum().cast(pl.Int64).alias("declined_count"),
        )
        .sort("txn_date", "merchant_id")
    )
