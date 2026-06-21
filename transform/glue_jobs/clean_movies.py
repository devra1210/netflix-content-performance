"""AWS Glue job to clean movie metadata into curated Parquet."""

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
    job.init(arg("JOB_NAME", "clean_movies"), {})

    raw_bucket = arg("RAW_BUCKET")
    curated_bucket = arg("CURATED_BUCKET")
    source_path = arg("SOURCE_PATH") or (f"s3://{raw_bucket}/movies/" if raw_bucket else None)
    output_path = arg("OUTPUT_PATH") or (f"s3://{curated_bucket}/movies/" if curated_bucket else None)

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
        
    title_id = first_column(df.columns, ("movie_id", "title_id", "imdb_id", "id"))
    title = first_column(df.columns, ("title", "title_name", "name", "original_title", "movie_name"))
    genre = first_column(df.columns, ("genre_primary", "genre", "genres", "primary_genre"))
    secondary_genre = first_column(df.columns, ("genre_secondary", "secondary_genre"))
    release_year = first_column(df.columns, ("release_year", "year"))
    release_date = first_column(df.columns, ("release_date", "first_air_date"))
    content_type = first_column(df.columns, ("content_type", "media_type", "type", "title_type"))
    popularity = first_column(df.columns, ("popularity", "vote_count", "score"))
    duration = first_column(df.columns, ("duration_minutes", "duration_mins", "runtime", "runtime_minutes"))
    rating = first_column(df.columns, ("rating", "maturity_rating"))
    language = first_column(df.columns, ("language", "original_language"))
    country = first_column(df.columns, ("country_of_origin", "country", "origin_country"))
    imdb_rating = first_column(df.columns, ("imdb_rating", "average_rating"))
    netflix_original = first_column(df.columns, ("is_netflix_original", "netflix_original"))
    added_to_platform = first_column(df.columns, ("added_to_platform", "date_added"))

    if title_id is None or title is None:
        raise ValueError(f"Movies source needs title id and name columns. Found: {df.columns}")

    selected = df.select(
        F.col(title_id).cast("string").alias("title_id"),
        F.col(title).cast("string").alias("title"),
    
        (F.col(genre).cast("string") if genre else F.lit(None).cast("string")).alias("genre"),
        (F.col(secondary_genre).cast("string") if secondary_genre else F.lit(None).cast("string")).alias("secondary_genre"),
    
        (
            F.col(release_year).cast("int")
            if release_year
            else (
                F.year(F.to_date(F.col(release_date))).cast("int")
                if release_date
                else F.lit(None).cast("int")
            )
        ).alias("release_year"),
    
        (
            F.when(
                F.lower(F.col(content_type)).isin("tv", "show", "series", "tv_series"),
                "series"
            ).otherwise("movie")
            if content_type
            else F.lit("movie")
        ).alias("content_type"),
    
        (F.col(popularity).cast("double") if popularity else F.lit(None).cast("double")).alias("popularity"),
        (F.col(duration).cast("double") if duration else F.lit(None).cast("double")).alias("duration_minutes"),
        (F.col(rating).cast("string") if rating else F.lit(None).cast("string")).alias("rating"),
        (F.col(language).cast("string") if language else F.lit(None).cast("string")).alias("language"),
        (F.col(country).cast("string") if country else F.lit(None).cast("string")).alias("country_of_origin"),
        (F.col(imdb_rating).cast("double") if imdb_rating else F.lit(None).cast("double")).alias("imdb_rating"),
    
        (
            F.when(
                F.lower(F.col(netflix_original)).isin("true", "1", "yes"),
                F.lit(True)
            ).otherwise(F.lit(False))
            if netflix_original
            else F.lit(None).cast("boolean")
        ).alias("is_netflix_original"),
    
        (F.to_date(F.col(added_to_platform)) if added_to_platform else F.lit(None).cast("date")).alias("added_to_platform"),
    )
    

    cleaned = (
        selected.filter(F.col("title_id").isNotNull() & (F.length(F.trim("title_id")) > 0))
        .dropDuplicates(["title_id"])
    )

    cleaned.write.mode("overwrite").format("parquet").save(output_path)
    job.commit()


if __name__ == "__main__":
    main()
