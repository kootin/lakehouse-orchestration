"""AWS Lambda entry point: triggered by an S3 ObjectCreated event on the landing bucket.

In AWS the catalog would be AWS Glue (`pyiceberg.catalog.glue.GlueCatalog`) and the
warehouse an S3 prefix; locally the same handler runs against the SQL catalog, which is
how the tests exercise it.
"""
from __future__ import annotations

import re
from datetime import date
from urllib.parse import unquote_plus

from lakehouse import jobs

DT_RE = re.compile(r"dt=(\d{4}-\d{2}-\d{2})/")


def handler(event: dict, context=None) -> dict:
    days = set()
    for record in event.get("Records", []):
        key = unquote_plus(record["s3"]["object"]["key"])
        m = DT_RE.search(key)
        if not m:
            continue
        days.add(date.fromisoformat(m.group(1)))
    results = []
    for d in sorted(days):
        results.append({**jobs.ingest_day(d), **jobs.build_summary(d)})
    return {"statusCode": 200, "processed": results}
