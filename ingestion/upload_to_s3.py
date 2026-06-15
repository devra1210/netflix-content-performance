#!/usr/bin/env python3
"""Upload local raw CSV files to the S3 raw zone with year/month partitioning."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import boto3
from dotenv import load_dotenv


DATASETS = {
    "watch_history.csv": "watch_history",
    "movies.csv": "movies",
    "reviews.csv": "reviews",
    "licensing_costs.csv": "licensing",
    "search_logs.csv": "search_logs",
    "users.csv": "users",
    "recommendation_logs.csv": "recommendation_logs"
}


def upload_files(data_dir: Path, bucket: str, year: int, month: int) -> list[str]:
    s3 = boto3.client("s3")
    uploaded: list[str] = []
    for filename, prefix in DATASETS.items():
        source = data_dir / filename
        if not source.exists():
            raise FileNotFoundError(f"Missing expected data file: {source}")

        key = f"{prefix}/year={year}/month={month:02d}/{filename}"
        s3.upload_file(str(source), bucket, key)
        uploaded.append(f"s3://{bucket}/{key}")
    return uploaded


def main() -> None:
    load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=True)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", default="data", type=Path)
    parser.add_argument("--bucket", default=os.getenv("RAW_BUCKET"))
    parser.add_argument("--year", default=int(os.getenv("INGEST_YEAR", "2024")), type=int)
    parser.add_argument("--month", default=int(os.getenv("INGEST_MONTH", "3")), type=int)
    args = parser.parse_args()

    if not args.bucket:
        raise ValueError("RAW_BUCKET must be set or passed with --bucket")

    for uri in upload_files(args.data_dir, args.bucket, args.year, args.month):
        print(f"Uploaded {uri}")


if __name__ == "__main__":
    main()
