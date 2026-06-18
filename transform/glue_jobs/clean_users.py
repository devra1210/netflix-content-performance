"""AWS Glue job to clean users into curated Parquet."""

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
    job.init(arg("JOB_NAME", "clean_users"), {})

    raw_bucket = arg("RAW_BUCKET")
    curated_bucket = arg("CURATED_BUCKET")
    source_path = arg("SOURCE_PATH") or (f"s3://{raw_bucket}/users/" if raw_bucket else None)
    output_path = arg("OUTPUT_PATH") or (f"s3://{curated_bucket}/users/" if curated_bucket else None)

    if not source_path:
        raise ValueError("RAW_BUCKET or SOURCE_PATH is required")
    if not output_path:
        raise ValueError("CURATED_BUCKET or OUTPUT_PATH is required")

    df = spark.read.option("header", True).option("inferSchema", True).option("recursiveFileLookup", True).csv(source_path)
    for column in df.columns:
        df = df.withColumnRenamed(column, snake_case(column))

    required = {"user_id", "email", "country", "subscription_plan", "subscription_start_date"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"Missing required user columns: {', '.join(missing)}. Found: {df.columns}")

    cleaned = (
        df.select(
            F.col("user_id").cast("string").alias("user_id"),
            F.lower(F.col("email").cast("string")).alias("email"),
            F.col("first_name").cast("string").alias("first_name"),
            F.col("last_name").cast("string").alias("last_name"),
            F.col("age").cast("int").alias("age"),
            F.col("gender").cast("string").alias("gender"),
            F.col("country").cast("string").alias("country"),
            F.col("state_province").cast("string").alias("state_province"),
            F.col("city").cast("string").alias("city"),
            F.col("subscription_plan").cast("string").alias("subscription_plan"),
            F.to_date(F.col("subscription_start_date")).alias("subscription_start_date"),
            F.col("is_active").cast("boolean").alias("is_active"),
            F.col("monthly_spend").cast("double").alias("monthly_spend"),
            F.col("primary_device").cast("string").alias("primary_device"),
            F.col("household_size").cast("int").alias("household_size"),
            F.to_timestamp(F.col("created_at")).alias("created_at"),
        )
        .filter(F.col("user_id").isNotNull() & (F.length(F.trim("user_id")) > 0))
        .dropDuplicates(["user_id"])
    )

    cleaned.write.mode("overwrite").format("parquet").save(output_path)
    job.commit()


if __name__ == "__main__":
    main()
