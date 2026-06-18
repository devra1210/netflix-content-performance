from __future__ import annotations

import os

import pytest
from dotenv import load_dotenv


load_dotenv()
psycopg2 = pytest.importorskip("psycopg2")


@pytest.fixture(scope="module")
def redshift_connection():
    required = ["REDSHIFT_ENDPOINT", "REDSHIFT_DB", "REDSHIFT_USER", "REDSHIFT_PASSWORD"]
    missing = [
        name
        for name in required
        if not os.getenv(name) or os.getenv(name, "").strip().startswith("<")
    ]
    if missing:
        pytest.skip(f"Missing Redshift environment variables: {', '.join(missing)}")

    connection = psycopg2.connect(
        host=os.environ["REDSHIFT_ENDPOINT"],
        dbname=os.environ["REDSHIFT_DB"],
        user=os.environ["REDSHIFT_USER"],
        password=os.environ["REDSHIFT_PASSWORD"],
        port=int(os.getenv("REDSHIFT_PORT", "5439")),
        connect_timeout=10,
    )
    try:
        yield connection
    finally:
        connection.close()


def scalar(connection, query: str):
    with connection.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchone()[0]


def test_fact_title_id_has_no_nulls(redshift_connection):
    assert scalar(
        redshift_connection,
        "select count(*) from analytics.fact_content_performance where title_id is null",
    ) == 0


def test_cost_per_hour_watched_is_never_negative(redshift_connection):
    assert scalar(
        redshift_connection,
        "select count(*) from analytics.fact_content_performance where cost_per_hour_watched < 0",
    ) == 0


def test_sentiment_score_is_between_zero_and_one(redshift_connection):
    assert scalar(
        redshift_connection,
        """
        select count(*)
        from analytics.fact_content_performance
        where sentiment_score is not null
          and (sentiment_score < 0 or sentiment_score > 1)
        """,
    ) == 0


def test_performance_id_has_no_duplicates(redshift_connection):
    assert scalar(
        redshift_connection,
        """
        select count(*)
        from (
            select performance_id
            from analytics.fact_content_performance
            group by performance_id
            having count(*) > 1
        ) duplicates
        """,
    ) == 0


def test_fact_row_count_is_large_enough(redshift_connection):
    assert scalar(redshift_connection, "select count(*) from analytics.fact_content_performance") > 1000
