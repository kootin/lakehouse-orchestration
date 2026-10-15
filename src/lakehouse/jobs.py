"""Pipeline steps, kept free of Airflow so they run anywhere (CLI, Lambda, tests)."""
from __future__ import annotations

import argparse
from datetime import date, timedelta
from pathlib import Path

import polars as pl

from lakehouse import config, iceberg_store, transform


class DataQualityError(Exception):
    pass


def ingest_day(day: date, root: Path | None = None) -> dict:
    root = root or config.ROOT
    files = sorted((root / "landing" / f"dt={day.isoformat()}").glob("*.jsonl"))
    if not files:
        raise FileNotFoundError(f"no landing files for {day}")
    txns, _ = iceberg_store.ensure_tables(iceberg_store.get_catalog(root))
    stats = {"day": day.isoformat(), "files": len(files), "bad_lines": 0, "inserted": 0, "updated": 0, "unmatched": 0}
    for f in files:
        events, bad = transform.read_events(f)
        stats["bad_lines"] += bad
        stats["inserted"] += iceberg_store.upsert_authorizations(txns, transform.authorizations(events))["inserted"]
        r = iceberg_store.apply_status_updates(txns, transform.status_updates(events))
        stats["updated"] += r["updated"]
        stats["unmatched"] += r["unmatched"]
    return stats


def build_summary(day: date, root: Path | None = None) -> dict:
    """Rebuild summaries for `day` and the day before (settlements change yesterday's numbers)."""
    txns, summary = iceberg_store.ensure_tables(iceberg_store.get_catalog(root or config.ROOT))
    df = iceberg_store.read_table(txns)
    df = df.filter(pl.col("authorized_at").dt.date().is_in([day - timedelta(days=1), day]))
    out = transform.daily_merchant_summary(df)
    if not out.is_empty():
        iceberg_store.overwrite_summary_partitions(summary, out)
    return {"day": day.isoformat(), "rows": out.height}


def quality_check(day: date, root: Path | None = None) -> dict:
    txns, summary = iceberg_store.ensure_tables(iceberg_store.get_catalog(root or config.ROOT))
    t = iceberg_store.read_table(txns).filter(pl.col("authorized_at").dt.date() == day)
    s = iceberg_store.read_table(summary, txn_date=day)
    problems = []
    if t.is_empty():
        problems.append("no transactions for day")
    if t["txn_id"].n_unique() != t.height:
        problems.append("duplicate txn_id")
    if t["amount_usd"].null_count() or (t["amount_usd"] <= 0).any():
        problems.append("missing or non-positive amount_usd")
    if s["txn_count"].sum() != t.height:
        problems.append(f"summary count {s['txn_count'].sum()} != transactions {t.height}")
    if problems:
        raise DataQualityError("; ".join(problems))
    return {"day": day.isoformat(), "transactions": t.height, "summary_rows": s.height}


def main() -> None:
    p = argparse.ArgumentParser(description="Run the pipeline for a range of days without Airflow")
    p.add_argument("--start", type=date.fromisoformat, default=date(2026, 10, 5))
    p.add_argument("--days", type=int, default=3)
    a = p.parse_args()
    for i in range(a.days):
        d = a.start + timedelta(days=i)
        print(ingest_day(d), build_summary(d), quality_check(d), sep="\n  ")
    txns, summary = iceberg_store.ensure_tables(iceberg_store.get_catalog())
    print("\nSnapshot history of payments.transactions:")
    for s in txns.snapshots():
        print(f"  {s.snapshot_id}  {s.summary.operation.value:<9} added={s.summary.get('added-records', 0)}")
    print("\nStatus breakdown:")
    print(iceberg_store.read_table(txns).group_by("status").len().sort("status"))


if __name__ == "__main__":
    main()
