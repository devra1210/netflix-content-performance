#!/usr/bin/env python3
"""Generate simulated Netflix licensing costs from TMDB title metadata."""

from __future__ import annotations

import argparse
import random
from pathlib import Path

import pandas as pd


DEFAULT_REGIONS = ("US", "CA", "GB", "IN", "BR", "MX", "DE", "JP")
MOVIE_COST_RANGE = (1_000_000, 15_000_000)
SERIES_COST_RANGE = (3_000_000, 25_000_000)


def first_existing_column(columns: list[str], candidates: tuple[str, ...]) -> str | None:
    normalized = {column.lower().strip(): column for column in columns}
    for candidate in candidates:
        if candidate in normalized:
            return normalized[candidate]
    return None


def normalize_content_type(value: object) -> str:
    text = str(value or "").strip().lower()
    if text in {"tv", "show", "series", "tv_series", "tv show", "miniseries"}:
        return "series"
    return "movie"


def generate_licensing(
    tmdb_path: Path,
    output_path: Path,
    regions: tuple[str, ...],
    license_year: int,
    seed: int,
    max_titles: int | None,
) -> pd.DataFrame:
    tmdb = pd.read_csv(tmdb_path, low_memory=False)
    columns = list(tmdb.columns)

    id_column = first_existing_column(
        columns,
        ("title_id", "imdb_id", "imdbid", "id", "tmdb_id", "movie_id"),
    )
    title_column = first_existing_column(
        columns,
        ("title_name", "title", "name", "original_title", "movie_name"),
    )
    type_column = first_existing_column(
        columns,
        ("content_type", "media_type", "type", "title_type"),
    )

    if id_column is None or title_column is None:
        raise ValueError(
            "TMDB CSV must include an id column and a title/name column. "
            f"Found columns: {', '.join(columns[:20])}"
        )

    work = tmdb[[id_column, title_column] + ([type_column] if type_column else [])].copy()
    work = work.rename(columns={id_column: "title_id", title_column: "title_name"})
    work["title_id"] = work["title_id"].astype(str).str.strip()
    work["title_name"] = work["title_name"].astype(str).str.strip()
    work = work[(work["title_id"] != "") & (work["title_name"] != "")]
    work = work.drop_duplicates(subset=["title_id"])

    if type_column:
        work["content_type"] = work[type_column].map(normalize_content_type)
    else:
        work["content_type"] = "movie"

    if max_titles:
        work = work.head(max_titles)

    rng = random.Random(seed)
    rows: list[dict[str, object]] = []
    for record in work.to_dict("records"):
        cost_range = SERIES_COST_RANGE if record["content_type"] == "series" else MOVIE_COST_RANGE
        rows.append(
            {
                "title_id": record["title_id"],
                "title_name": record["title_name"],
                "license_cost_usd": rng.randrange(cost_range[0], cost_range[1] + 1, 10_000),
                "license_year": license_year,
                "region": rng.choice(regions),
            }
        )

    licensing = pd.DataFrame(rows)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    licensing.to_csv(output_path, index=False)
    return licensing


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tmdb-path", default="data/tmdb_movies.csv", type=Path)
    parser.add_argument("--output-path", default="data/licensing_costs.csv", type=Path)
    parser.add_argument("--regions", default=",".join(DEFAULT_REGIONS))
    parser.add_argument("--license-year", default=2024, type=int)
    parser.add_argument("--seed", default=42, type=int)
    parser.add_argument("--max-titles", type=int)
    args = parser.parse_args()

    regions = tuple(region.strip().upper() for region in args.regions.split(",") if region.strip())
    generated = generate_licensing(
        args.tmdb_path,
        args.output_path,
        regions,
        args.license_year,
        args.seed,
        args.max_titles,
    )
    print(f"Wrote {len(generated):,} licensing rows to {args.output_path}")


if __name__ == "__main__":
    main()
