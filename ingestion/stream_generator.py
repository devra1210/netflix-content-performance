#!/usr/bin/env python3
"""Generate fake Netflix streaming play events and upload them as JSON Lines to S3."""

from __future__ import annotations

import argparse
import json
import os
import random
from datetime import datetime, timedelta
from pathlib import Path

import boto3
import pandas as pd
from dotenv import load_dotenv
from faker import Faker


REGIONS = ("US", "CA", "GB", "IN", "BR", "MX", "DE", "JP")
DEVICES = ("TV", "Mobile", "Tablet", "Web", "Game Console")


def load_title_ids(data_dir: Path) -> list[str]:
    for filename in ("licensing_costs.csv", "movies.csv"):
        path = data_dir / filename
        if path.exists():
            frame = pd.read_csv(path, usecols=lambda column: column.lower() in {"title_id", "imdb_id", "id"})
            if "title_id" in frame.columns:
                return frame["title_id"].dropna().astype(str).unique().tolist()
            if "imdb_id" in frame.columns:
                return frame["imdb_id"].dropna().astype(str).unique().tolist()
            if "id" in frame.columns:
                return frame["id"].dropna().astype(str).unique().tolist()
    return ["tt0111161", "tt0068646", "tt0468569", "tt0109830", "tt0137523"]


def generate_events(count: int, data_dir: Path, start: datetime, seed: int) -> list[dict[str, object]]:
    fake = Faker()
    Faker.seed(seed)
    rng = random.Random(seed)
    title_ids = load_title_ids(data_dir)
    events: list[dict[str, object]] = []

    for _ in range(count):
        timestamp = start + timedelta(minutes=rng.randint(0, 43_199))
        events.append(
            {
                "user_id": f"u_{fake.random_int(min=1, max=250_000)}",
                "title_id": rng.choice(title_ids),
                "event": "play",
                "watch_duration_mins": rng.randint(1, 180),
                "timestamp": timestamp.replace(microsecond=0).isoformat(),
                "region": rng.choice(REGIONS),
                "device": rng.choice(DEVICES),
            }
        )
    return events


def upload_events(events: list[dict[str, object]], bucket: str, key: str) -> str:
    body = "\n".join(json.dumps(event, separators=(",", ":")) for event in events) + "\n"
    boto3.client("s3").put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson",
    )
    return f"s3://{bucket}/{key}"


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", default=os.getenv("RAW_BUCKET"))
    parser.add_argument("--data-dir", default="data", type=Path)
    parser.add_argument("--count", default=10_000, type=int)
    parser.add_argument("--start", default="2024-03-01T00:00:00")
    parser.add_argument("--seed", default=42, type=int)
    args = parser.parse_args()

    if not args.bucket:
        raise ValueError("RAW_BUCKET must be set or passed with --bucket")

    start = datetime.fromisoformat(args.start)
    events = generate_events(args.count, args.data_dir, start, args.seed)
    key = f"streaming_events/year={start.year}/month={start.month:02d}/day={start.day:02d}/streaming_events_{datetime.utcnow():%Y%m%dT%H%M%SZ}.jsonl"
    print(f"Uploaded {len(events):,} events to {upload_events(events, args.bucket, key)}")


if __name__ == "__main__":
    main()
