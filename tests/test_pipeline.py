from datetime import date

import polars as pl
import pytest

from lakehouse import aws_lambda, iceberg_store, jobs, landing, transform

D1, D2 = date(2026, 10, 5), date(2026, 10, 6)


def test_transform_flattens_and_converts(lake):
    path = landing.land_day(D1, n=50, root=lake / "landing")
    events, bad = transform.read_events(path)
    auths = transform.authorizations(events)
    assert bad == 1
    assert auths.height == 50
    assert {"merchant_id", "merchant_country", "amount_usd"} <= set(auths.columns)
    gbp = auths.filter(pl.col("currency") == "GBP")
    assert (gbp["amount_usd"] > gbp["amount"]).all()


def test_late_settlements_upsert_into_previous_day(lake):
    landing.land_day(D1, n=100, root=lake / "landing")
    landing.land_day(D2, n=100, root=lake / "landing")
    s1 = jobs.ingest_day(D1)
    s2 = jobs.ingest_day(D2)
    assert s1["inserted"] == 100 and s1["updated"] == 0
    assert s2["inserted"] == 100 and s2["updated"] == 60 and s2["unmatched"] == 0

    txns, _ = iceberg_store.ensure_tables(iceberg_store.get_catalog(lake))
    df = iceberg_store.read_table(txns)
    assert df.height == 200 and df["txn_id"].n_unique() == 200
    day1 = df.filter(pl.col("authorized_at").dt.date() == D1)
    assert day1.filter(pl.col("status") != "AUTHORIZED").height == 60


def test_reingesting_a_day_is_idempotent(lake):
    landing.land_day(D1, n=40, root=lake / "landing")
    jobs.ingest_day(D1)
    again = jobs.ingest_day(D1)
    assert again["inserted"] == 0
    txns, _ = iceberg_store.ensure_tables(iceberg_store.get_catalog(lake))
    assert iceberg_store.read_table(txns).height == 40


def test_time_travel_sees_pre_settlement_state(lake):
    landing.land_day(D1, n=30, root=lake / "landing")
    landing.land_day(D2, n=30, root=lake / "landing")
    jobs.ingest_day(D1)
    txns, _ = iceberg_store.ensure_tables(iceberg_store.get_catalog(lake))
    snap_after_day1 = txns.current_snapshot().snapshot_id
    jobs.ingest_day(D2)
    txns.refresh()
    old = iceberg_store.read_table(txns, snapshot_id=snap_after_day1)
    assert old.height == 30 and set(old["status"]) == {"AUTHORIZED"}
    assert set(iceberg_store.read_table(txns)["status"]) > {"AUTHORIZED"}


def test_summary_and_quality_gate(lake):
    landing.land_day(D1, n=80, root=lake / "landing")
    landing.land_day(D2, n=80, root=lake / "landing")
    for d in (D1, D2):
        jobs.ingest_day(d)
        jobs.build_summary(d)
    # Day 1's settled counts were refreshed after day 2's settlements arrived.
    _, summary = iceberg_store.ensure_tables(iceberg_store.get_catalog(lake))
    s1 = iceberg_store.read_table(summary, txn_date=D1)
    assert s1["txn_count"].sum() == 80 and s1["settled_count"].sum() + s1["declined_count"].sum() == 48
    assert jobs.quality_check(D1)["transactions"] == 80


def test_quality_gate_fails_without_data(lake):
    iceberg_store.ensure_tables(iceberg_store.get_catalog(lake))
    with pytest.raises(jobs.DataQualityError):
        jobs.quality_check(D1)


def test_lambda_handler_processes_s3_event(lake):
    landing.land_day(D1, n=20, root=lake / "landing")
    event = {"Records": [{"s3": {"bucket": {"name": "landing"}, "object": {"key": "landing/dt%3D2026-10-05/events-0001.jsonl"}}}]}
    out = aws_lambda.handler(event)
    assert out["statusCode"] == 200
    assert out["processed"][0]["inserted"] == 20
