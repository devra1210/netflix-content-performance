"""AWS Glue job to clean search logs into curated Parquet."""

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
    job.init(arg("JOB_NAME", "clean_search_logs"), {})

    raw_bucket = arg("RAW_BUCKET")
    curated_bucket = arg("CURATED_BUCKET")
    source_path = arg("SOURCE_PATH") or (f"s3://{raw_bucket}/search_logs/" if raw_bucket else None)
    output_path = arg("OUTPUT_PATH") or (f"s3://{curated_bucket}/search_logs/" if curated_bucket else None)

    if not source_path:
        raise ValueError("RAW_BUCKET or SOURCE_PATH is required")
    if not output_path:
        raise ValueError("CURATED_BUCKET or OUTPUT_PATH is required")

    df = spark.read.option("header", True).option("inferSchema", True).option("recursiveFileLookup", True).csv(source_path)
    for column in df.columns:
        df = df.withColumnRenamed(column, snake_case(column))

    required = {"search_id", "user_id", "search_query", "search_date"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"Missing required search log columns: {', '.join(missing)}. Found: {df.columns}")

    cleaned = (
        df.select(
            F.col("search_id").cast("string").alias("search_id"),
            F.col("user_id").cast("string").alias("user_id"),
            F.trim(F.col("search_query").cast("string")).alias("search_query"),
            F.to_timestamp(F.col("search_date")).alias("search_ts"),
            F.col("results_returned").cast("int").alias("results_returned"),
            F.col("clicked_result_position").cast("int").alias("clicked_result_position"),
            F.col("device_type").cast("string").alias("device_type"),
            F.col("search_duration_seconds").cast("double").alias("search_duration_seconds"),
            F.col("had_typo").cast("boolean").alias("had_typo"),
            F.col("used_filters").cast("boolean").alias("used_filters"),
            F.col("location_country").cast("string").alias("region"),
        )
        .filter(F.col("search_id").isNotNull() & F.col("user_id").isNotNull())
        .filter(F.length(F.col("search_query")) > 0)
        .dropDuplicates(["search_id"])
    )

    cleaned.write.mode("overwrite").partitionBy("region").format("parquet").save(output_path)
    job.commit()


if __name__ == "__main__":
    main()
