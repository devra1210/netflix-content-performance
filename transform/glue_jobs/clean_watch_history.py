"""AWS Glue job to clean watch history into curated Parquet."""

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


def main() -> None:
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(arg("JOB_NAME", "clean_watch_history"), {})

    raw_bucket = arg("RAW_BUCKET")
    curated_bucket = arg("CURATED_BUCKET")
    source_path = arg("SOURCE_PATH") or (f"s3://{raw_bucket}/watch_history/" if raw_bucket else None)
    output_path = arg("OUTPUT_PATH") or (f"s3://{curated_bucket}/watch_history/" if curated_bucket else None)

    if not source_path:
        raise ValueError("RAW_BUCKET or SOURCE_PATH is required")
    if not output_path:
        raise ValueError("CURATED_BUCKET or OUTPUT_PATH is required")

    df = spark.read.option("header", True).option("inferSchema", True).option("recursiveFileLookup", True).csv(source_path)
    for column in df.columns:
        df = df.withColumnRenamed(column, snake_case(column))

    required = {"session_id", "user_id", "movie_id", "watch_date", "watch_duration_minutes"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"Missing required watch history columns: {', '.join(missing)}. Found: {df.columns}")

    cleaned = (
        df.select(
            F.col("session_id").cast("string").alias("session_id"),
            F.col("user_id").cast("string").alias("user_id"),
            F.col("movie_id").cast("string").alias("title_id"),
            F.to_timestamp(F.col("watch_date")).alias("watched_at"),
            F.col("device_type").cast("string").alias("device_type"),
            F.col("watch_duration_minutes").cast("double").alias("watch_duration_mins"),
            F.col("progress_percentage").cast("double").alias("progress_percentage"),
            F.col("action").cast("string").alias("action"),
            F.col("quality").cast("string").alias("quality"),
            F.col("location_country").cast("string").alias("region"),
            F.col("is_download").cast("boolean").alias("is_download"),
            F.col("user_rating").cast("double").alias("user_rating"),
        )
        .filter(
            F.col("session_id").isNotNull()
            & F.col("title_id").isNotNull()
            & F.col("watch_duration_mins").isNotNull()
            & (F.col("watch_duration_mins") >= 0)
        )
        .dropDuplicates(["session_id"])
    )

    cleaned.write.mode("overwrite").partitionBy("region").format("parquet").save(output_path)
    job.commit()


if __name__ == "__main__":
    main()
