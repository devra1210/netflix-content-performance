"""AWS Glue job to build the final content performance dataset."""

from __future__ import annotations

import re
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window


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


def read_parquet(spark, path: str) -> DataFrame:
    return spark.read.option("recursiveFileLookup", True).parquet(path)


def normalize_columns(df: DataFrame) -> DataFrame:
    for column in df.columns:
        df = df.withColumnRenamed(column, snake_case(column))
    return df


def normalize_region(column: F.Column) -> F.Column:
    normalized = F.upper(F.trim(column.cast("string")))
    return (
        F.when(normalized.isin("USA", "UNITED STATES", "UNITED STATES OF AMERICA"), F.lit("US"))
        .when(normalized.isin("CANADA"), F.lit("CA"))
        .when(normalized.isin("UNITED KINGDOM", "UK"), F.lit("GB"))
        .when(normalized.isin("INDIA"), F.lit("IN"))
        .when(normalized.isin("BRAZIL"), F.lit("BR"))
        .when(normalized.isin("MEXICO"), F.lit("MX"))
        .when(normalized.isin("GERMANY"), F.lit("DE"))
        .when(normalized.isin("JAPAN"), F.lit("JP"))
        .otherwise(F.coalesce(normalized, F.lit("UNKNOWN")))
    )


def main() -> None:
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(arg("JOB_NAME", "join_content_metrics"), {})

    raw_bucket = arg("RAW_BUCKET")
    curated_bucket = arg("CURATED_BUCKET")
    output_path = arg("OUTPUT_PATH") or (f"s3://{curated_bucket}/content_performance/" if curated_bucket else None)

    if not output_path:
        raise ValueError("CURATED_BUCKET or OUTPUT_PATH is required")

    watch_path = arg("USER_BEHAVIOR_PATH") or (f"s3://{curated_bucket}/watch_history/" if curated_bucket else None)
    movies_path = arg("MOVIES_PATH") or (f"s3://{curated_bucket}/movies/" if curated_bucket else None)
    sentiment_path = arg("SENTIMENT_PATH") or (f"s3://{curated_bucket}/sentiment/" if curated_bucket else None)
    if not watch_path or not movies_path or not sentiment_path:
        raise ValueError("CURATED_BUCKET or explicit curated source paths are required")

    watch_df = normalize_columns(read_parquet(spark, watch_path))
    movies_df = normalize_columns(read_parquet(spark, movies_path))
    sentiment_df = normalize_columns(read_parquet(spark, sentiment_path))

    if "title_id" not in movies_df.columns and "id" in movies_df.columns:
        movies_df = movies_df.withColumnRenamed("id", "title_id")
    if "title_name" not in movies_df.columns and "title" in movies_df.columns:
        movies_df = movies_df.withColumnRenamed("title", "title_name")

    licensing_path = arg("LICENSING_PATH") or (f"s3://{curated_bucket}/licensing/" if curated_bucket else None)
    if licensing_path:
        licensing_df = normalize_columns(read_parquet(spark, licensing_path))
    else:
        if not raw_bucket:
            raise ValueError("RAW_BUCKET or LICENSING_PATH is required")
        licensing_df = (
            spark.read.option("header", True)
            .option("inferSchema", True)
            .option("recursiveFileLookup", True)
            .csv(f"s3://{raw_bucket}/licensing/")
        )
        licensing_df = normalize_columns(licensing_df)

    title_col = first_column(watch_df.columns, ("title_id", "movie_id", "imdb_id"))
    user_col = first_column(watch_df.columns, ("user_id", "profile_id", "customer_id"))
    region_col = first_column(watch_df.columns, ("region", "region_code", "country"))
    duration_col = first_column(watch_df.columns, ("watch_duration_mins", "watch_duration_minutes", "duration_mins", "minutes_watched", "watch_time"))
    watch_ts_col = first_column(watch_df.columns, ("timestamp", "watched_at", "event_timestamp", "watch_date", "date"))
    churn_col = first_column(watch_df.columns, ("churned", "is_churned", "cancelled", "canceled"))
    churn_date_col = first_column(watch_df.columns, ("churn_date", "cancel_date", "cancellation_date"))

    if title_col is None or duration_col is None:
        raise ValueError("User behavior needs title_id and watch duration columns to compute ROI")

    prepared = watch_df.withColumn("title_id", F.col(title_col).cast("string")).withColumn(
        "watch_duration_mins", F.col(duration_col).cast("double")
    )
    prepared = prepared.withColumn("region", normalize_region(F.col(region_col)) if region_col else F.lit("UNKNOWN"))
    prepared = prepared.withColumn("user_id", F.col(user_col).cast("string") if user_col else F.monotonically_increasing_id().cast("string"))
    prepared = prepared.withColumn("watched_at", F.to_timestamp(F.col(watch_ts_col)) if watch_ts_col else F.current_timestamp())
    prepared = prepared.withColumn("year", F.year("watched_at"))

    if churn_date_col:
        prepared = prepared.withColumn("churn_date", F.to_timestamp(F.col(churn_date_col)))
        prepared = prepared.withColumn(
            "churned_post_title",
            F.when(F.datediff(F.col("churn_date"), F.col("watched_at")).between(0, 30), F.lit(1.0)).otherwise(F.lit(0.0)),
        )
    elif churn_col:
        prepared = prepared.withColumn("churned_post_title", F.col(churn_col).cast("double"))
    else:
        prepared = prepared.withColumn("churned_post_title", F.lit(0.0))

    usage = (
        prepared.filter(F.col("title_id").isNotNull() & F.col("watch_duration_mins").isNotNull())
        .groupBy("title_id", "region", "year")
        .agg(
            (F.sum("watch_duration_mins") / F.lit(60.0)).alias("total_watch_hours"),
            F.avg("churned_post_title").alias("churn_rate_post_title"),
            F.countDistinct("user_id").alias("unique_viewers"),
        )
    )

    licensing = licensing_df.select(
        F.col("title_id").cast("string").alias("title_id"),
        normalize_region(F.col("region")).alias("license_region"),
        F.col("license_cost_usd_millions").cast("double").alias("license_cost_usd_millions"),
        F.col("license_year").cast("int").alias("license_year"),
    )

    license_window = Window.partitionBy(usage.title_id, usage.region, usage.year).orderBy(
        F.when(licensing.license_year == usage.year, F.lit(0))
        .when(licensing.license_year <= usage.year, F.lit(1))
        .when(licensing.license_year.isNotNull(), F.lit(2))
        .otherwise(F.lit(3)),
        F.when(licensing.license_year.isNull(), F.lit(999999)).otherwise(F.abs(licensing.license_year - usage.year)),
        licensing.license_year.desc_nulls_last(),
    )

    joined = (
        usage.join(
            licensing,
            (usage.title_id == licensing.title_id)
            & ((usage.region == licensing.license_region) | licensing.license_region.isNull()),
            "left",
        )
        .withColumn("license_row_number", F.row_number().over(license_window))
        .filter(F.col("license_row_number") == 1)
        .drop("license_row_number")
        .drop(licensing.title_id)
        .join(movies_df, "title_id", "left")
        .join(sentiment_df.select("title_id", "sentiment_score"), "title_id", "left")
    )

    result = joined.withColumn(
        "cost_per_hour_watched",
        F.when(F.col("total_watch_hours") > 0, F.col("license_cost_usd_millions") / F.col("total_watch_hours")),
    ).withColumn(
        "performance_id",
        F.sha2(F.concat_ws("|", F.col("title_id"), F.col("region"), F.col("year").cast("string")), 256),
    )

    license_match_counts = result.agg(
        F.count("*").alias("row_count"),
        F.count("license_cost_usd_millions").alias("licensed_row_count"),
    ).collect()[0]
    if license_match_counts["row_count"] > 0 and license_match_counts["licensed_row_count"] == 0:
        raise ValueError(
            "No licensing costs matched content usage. "
            "Confirm the curated licensing dataset was regenerated from the same movies.csv as watch_history "
            "and that clean_licensing.py ran after uploading the updated licensing_costs.csv."
        )

    output_columns = [
        ("performance_id", "string"),
        ("title_id", "string"),
        ("region", "string"),
        ("year", "int"),
        ("total_watch_hours", "double"),
        ("churn_rate_post_title", "double"),
        ("unique_viewers", "long"),
        ("license_region", "string"),
        ("license_cost_usd_millions", "double"),
        ("license_year", "int"),
        ("title_name", "string"),
        ("genre", "string"),
        ("secondary_genre", "string"),
        ("release_year", "int"),
        ("content_type", "string"),
        ("popularity", "double"),
        ("duration_minutes", "double"),
        ("rating", "string"),
        ("language", "string"),
        ("country_of_origin", "string"),
        ("imdb_rating", "double"),
        ("is_netflix_original", "boolean"),
        ("added_to_platform", "date"),
        ("sentiment_score", "double"),
        ("cost_per_hour_watched", "double"),
    ]
    for column, data_type in output_columns:
        if column not in result.columns:
            result = result.withColumn(column, F.lit(None).cast(data_type))

    result = result.withColumn("region_partition", F.col("region")).withColumn("year_partition", F.col("year"))
    result.select(
        *[F.col(column).cast(data_type).alias(column) for column, data_type in output_columns],
        "region_partition",
        "year_partition",
    ).write.mode("overwrite").partitionBy("region_partition", "year_partition").format("parquet").save(output_path)
    job.commit()


if __name__ == "__main__":
    main()
