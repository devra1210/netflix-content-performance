"""AWS Glue job to normalize IMDB sentiment reviews and aggregate by title."""

from __future__ import annotations

import re
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F


def arg(name: str, default: str | None = None) -> str | None:
    flag = f"--{name}"
    if flag in sys.argv:
        return sys.argv[sys.argv.index(flag) + 1]
    return default


def snake_case(name: str) -> str:
    return re.sub(r"_+", "_", re.sub(r"[^0-9a-zA-Z]+", "_", name.strip())).strip("_").lower()


def first_column(columns: list[str], candidates: tuple[str, ...]) -> str | None:
    lookup = {column.lower(): column for column in columns}
    for candidate in candidates:
        if candidate in lookup:
            return lookup[candidate]
    return None


def main() -> None:
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(arg("JOB_NAME", "clean_imdb_sentiment"), {})

    raw_bucket = arg("RAW_BUCKET")
    curated_bucket = arg("CURATED_BUCKET")
    source_path = arg("SOURCE_PATH") or (f"s3://{raw_bucket}/imdb_reviews/" if raw_bucket else None)
    output_path = arg("OUTPUT_PATH") or (f"s3://{curated_bucket}/imdb_sentiment/" if curated_bucket else None)

    if not source_path:
        raise ValueError("RAW_BUCKET or SOURCE_PATH is required")
    if not output_path:
        raise ValueError("CURATED_BUCKET or OUTPUT_PATH is required")

    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .option("multiLine", True)
        .option("escape", '"')
        .option("recursiveFileLookup", True)
        .csv(source_path)
    )
    for column in df.columns:
        df = df.withColumnRenamed(column, snake_case(column))

    title_id = first_column(df.columns, ("title_id", "imdb_id", "imdbid", "movie_id"))
    sentiment = first_column(df.columns, ("sentiment", "label", "polarity"))
    review = first_column(df.columns, ("review", "review_text", "text"))

    if title_id is None:
        raise ValueError("IMDB reviews need title_id/imdb_id to aggregate sentiment by title")
    if sentiment is None:
        raise ValueError("IMDB reviews need a sentiment/label/polarity column")

    scored = df.withColumn("title_id", F.col(title_id).cast("string"))
    scored = scored.withColumn(
        "sentiment_value",
        F.when(F.lower(F.col(sentiment).cast("string")).isin("positive", "pos", "1", "true"), F.lit(1.0))
        .when(F.lower(F.col(sentiment).cast("string")).isin("negative", "neg", "0", "false"), F.lit(0.0))
        .otherwise(F.col(sentiment).cast("double")),
    )
    if review:
        scored = scored.withColumn("review_length", F.length(F.col(review).cast("string")))
    else:
        scored = scored.withColumn("review_length", F.lit(None).cast("int"))

    aggregated = (
        scored.filter(F.col("title_id").isNotNull() & F.col("sentiment_value").between(0, 1))
        .groupBy("title_id")
        .agg(
            F.avg("sentiment_value").alias("sentiment_score"),
            F.count("*").alias("review_count"),
            F.avg("review_length").alias("avg_review_length"),
        )
    )
    aggregated.write.mode("overwrite").format("parquet").save(output_path)
    job.commit()


if __name__ == "__main__":
    main()
