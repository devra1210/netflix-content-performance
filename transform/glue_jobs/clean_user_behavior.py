"""AWS Glue job to clean Netflix user behavior data into curated Parquet."""

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
    value = re.sub(r"[^0-9a-zA-Z]+", "_", name.strip())
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)
    return value.strip("_").lower()


def main() -> None:
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(arg("JOB_NAME", "clean_user_behavior"), {})

    raw_bucket = arg("RAW_BUCKET")
    curated_bucket = arg("CURATED_BUCKET")
    source_path = arg("SOURCE_PATH") or (f"s3://{raw_bucket}/user_behavior/" if raw_bucket else None)
    output_path = arg("OUTPUT_PATH") or (f"s3://{curated_bucket}/user_behavior/" if curated_bucket else None)

    if not source_path:
        raise ValueError("RAW_BUCKET or SOURCE_PATH is required")
    if not output_path:
        raise ValueError("CURATED_BUCKET or OUTPUT_PATH is required")

    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .option("recursiveFileLookup", True)
        .csv(source_path)
    )

    for column in df.columns:
        df = df.withColumnRenamed(column, snake_case(column))

    df = df.dropDuplicates().na.drop("any")

    for column in df.columns:
        lower = column.lower()
        if any(token in lower for token in ("timestamp", "datetime", "watched_at", "churn_date")):
            df = df.withColumn(column, F.to_timestamp(F.col(column)))
        elif any(token in lower for token in ("duration", "mins", "minutes", "watch_time", "age", "rating")):
            df = df.withColumn(column, F.col(column).cast("int"))
        elif lower in {"churned", "is_churned", "cancelled", "canceled"}:
            df = df.withColumn(column, F.col(column).cast("boolean"))

    (
        df.coalesce(8)
        .write.mode("overwrite")
        .format("parquet")
        .save(output_path)
    )
    job.commit()


if __name__ == "__main__":
    main()
