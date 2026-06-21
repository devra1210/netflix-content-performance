"""AWS Glue job to clean licensing costs into curated Parquet."""

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


def require_columns(columns: list[str], required: set[str]) -> None:
    missing = sorted(required - set(columns))
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}. Found: {columns}")


def main() -> None:
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(arg("JOB_NAME", "clean_licensing"), {})

    raw_bucket = arg("RAW_BUCKET")
    curated_bucket = arg("CURATED_BUCKET")
    source_path = arg("SOURCE_PATH") or (f"s3://{raw_bucket}/licensing/" if raw_bucket else None)
    output_path = arg("OUTPUT_PATH") or (f"s3://{curated_bucket}/licensing/" if curated_bucket else None)

    if not source_path:
        raise ValueError("RAW_BUCKET or SOURCE_PATH is required")
    if not output_path:
        raise ValueError("CURATED_BUCKET or OUTPUT_PATH is required")

    df = spark.read.option("header", True).option("inferSchema", True).option("recursiveFileLookup", True).csv(source_path)
    for column in df.columns:
        df = df.withColumnRenamed(column, snake_case(column))

    require_columns(df.columns, {"title_id", "license_cost_usd", "license_year", "region"})

    cleaned = (
        df.select(
            F.col("title_id").cast("string").alias("title_id"),
            F.col("title_name").cast("string").alias("title_name")
            if "title_name" in df.columns
            else F.lit(None).cast("string").alias("title_name"),

            F.round(
                F.col("license_cost_usd").cast("double") / 1_000_000,
                2
            ).alias("license_cost_usd_millions"),

            F.col("license_year").cast("int").alias("license_year"),
            F.upper(F.col("region").cast("string")).alias("region"),
        )
        .filter(
            F.col("title_id").isNotNull()
            & (F.length(F.trim("title_id")) > 0)
            & F.col("license_cost_usd_millions").isNotNull()
            & (F.col("license_cost_usd_millions") >= 0)
        )
        .dropDuplicates(["title_id", "region", "license_year"])
    )

    cleaned.write.mode("overwrite").format("parquet").save(output_path)
    job.commit()


if __name__ == "__main__":
    main()
