"""Simulate card-transaction events landing as newline-delimited JSON, partitioned by date.

Each day carries new authorisations plus late *settlement* events for transactions
from earlier days, so the lake has to upsert rather than append.
"""
from __future__ import annotations

import argparse
import json
import random
from datetime import date, datetime, timedelta
from pathlib import Path

from lakehouse.config import LANDING

MERCHANTS = [
    ("M001", "Blue Bottle Coffee", "5814", "US"),
    ("M002", "Delta Air Lines", "4511", "US"),
    ("M003", "Whole Foods", "5411", "US"),
    ("M004", "Uber", "4121", "US"),
    ("M005", "Marriott London", "7011", "GB"),
]


def land_day(day: date, n: int = 500, root: Path = LANDING, seed: int | None = None) -> Path:
    rng = random.Random(seed if seed is not None else day.toordinal())
    out = root / f"dt={day.isoformat()}"
    out.mkdir(parents=True, exist_ok=True)
    path = out / "events-0001.jsonl"
    start = datetime.combine(day, datetime.min.time())
    with path.open("w") as f:
        for i in range(n):
            m = rng.choice(MERCHANTS)
            f.write(json.dumps({
                "event_id": f"E{day:%Y%m%d}{i:05d}",
                "txn_id": f"TX{day:%Y%m%d}{i:05d}",
                "event_type": "AUTHORIZED",
                "event_ts": (start + timedelta(seconds=rng.randrange(86400))).isoformat(),
                "card_last4": f"{rng.randrange(10000):04d}",
                "amount": {"value": round(rng.lognormvariate(3.5, 1.0), 2), "currency": "GBP" if m[3] == "GB" else "USD"},
                "merchant": {"id": m[0], "name": m[1], "mcc": m[2], "country": m[3]},
            }) + "\n")
        # Settlements for ~60% of yesterday's transactions arrive today (late-arriving updates).
        prev = day - timedelta(days=1)
        for i in rng.sample(range(n), int(n * 0.6)):
            f.write(json.dumps({
                "event_id": f"S{day:%Y%m%d}{i:05d}",
                "txn_id": f"TX{prev:%Y%m%d}{i:05d}",
                "event_type": rng.choice(["SETTLED"] * 19 + ["DECLINED"]),
                "event_ts": (start + timedelta(hours=2, seconds=rng.randrange(3600))).isoformat(),
            }) + "\n")
        f.write("{not valid json\n")  # one corrupt line per file, the transform must skip it
    return path


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--start", type=date.fromisoformat, default=date(2026, 10, 5))
    p.add_argument("--days", type=int, default=3)
    a = p.parse_args()
    for i in range(a.days):
        print(land_day(a.start + timedelta(days=i)))


if __name__ == "__main__":
    main()
