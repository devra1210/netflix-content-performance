"""AWS Glue job to clean recommendation logs into curated Parquet."""

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
    job.init(arg("JOB_NAME", "clean_recommendation_logs"), {})

    raw_bucket = arg("RAW_BUCKET")
    curated_bucket = arg("CURATED_BUCKET")
    source_path = arg("SOURCE_PATH") or (f"s3://{raw_bucket}/recommendation_logs/" if raw_bucket else None)
    output_path = arg("OUTPUT_PATH") or (f"s3://{curated_bucket}/recommendation_logs/" if curated_bucket else None)

    if not source_path:
        raise ValueError("RAW_BUCKET or SOURCE_PATH is required")
    if not output_path:
        raise ValueError("CURATED_BUCKET or OUTPUT_PATH is required")

    df = spark.read.option("header", True).option("inferSchema", True).option("recursiveFileLookup", True).csv(source_path)
    for column in df.columns:
        df = df.withColumnRenamed(column, snake_case(column))

    required = {"recommendation_id", "user_id", "movie_id", "recommendation_date", "recommendation_score"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"Missing required recommendation columns: {', '.join(missing)}. Found: {df.columns}")

    cleaned = (
        df.select(
            F.col("recommendation_id").cast("string").alias("recommendation_id"),
            F.col("user_id").cast("string").alias("user_id"),
            F.col("movie_id").cast("string").alias("title_id"),
            F.to_timestamp(F.col("recommendation_date")).alias("recommendation_ts"),
            F.col("recommendation_type").cast("string").alias("recommendation_type"),
            F.col("recommendation_score").cast("double").alias("recommendation_score"),
            F.col("was_clicked").cast("boolean").alias("was_clicked"),
            F.col("position_in_list").cast("int").alias("position_in_list"),
            F.col("device_type").cast("string").alias("device_type"),
            F.col("time_of_day").cast("string").alias("time_of_day"),
            F.col("algorithm_version").cast("string").alias("algorithm_version"),
        )
        .filter(F.col("recommendation_id").isNotNull() & F.col("title_id").isNotNull())
        .filter(F.col("recommendation_score").between(0, 1))
        .dropDuplicates(["recommendation_id"])
    )

    cleaned.write.mode("overwrite").format("parquet").save(output_path)
    job.commit()


if __name__ == "__main__":
    main()
