from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_each_raw_csv_has_a_cleaner() -> None:
    expected_jobs = {
        "licensing_costs.csv": "clean_licensing.py",
        "movies.csv": "clean_movies.py",
        "recommendation_logs.csv": "clean_recommendation_logs.py",
        "reviews.csv": "clean_reviews.py",
        "search_logs.csv": "clean_search_logs.py",
        "users.csv": "clean_users.py",
        "watch_history.csv": "clean_watch_history.py",
    }

    for csv_name, job_name in expected_jobs.items():
        assert (ROOT / "data" / csv_name).exists()
        assert (ROOT / "transform" / "glue_jobs" / job_name).exists()


def test_deleted_tmdb_names_are_not_referenced() -> None:
    checked_paths = [
        *Path(ROOT / "transform" / "glue_jobs").glob("*.py"),
        *Path(ROOT / "transform" / "dbt" / "models").rglob("*.sql"),
        *Path(ROOT / "ingestion").glob("*.py"),
    ]

    for path in checked_paths:
        content = path.read_text()
        assert "tmdb" not in content.lower(), path
        assert "imdb_sentiment" not in content.lower(), path
