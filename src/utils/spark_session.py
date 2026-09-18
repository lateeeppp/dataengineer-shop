import os

from pyspark.sql import SparkSession


def get_spark_session(app_name: str) -> SparkSession:
    region = os.getenv("AWS_REGION", "ap-southeast-1")
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.hadoop.fs.s3a.input.stream.type", "classic")
    )

    if os.getenv("AWS_PROFILE") or os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("AWS_REGION"):
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.endpoint.region", region)
            .config("spark.hadoop.fs.s3a.endpoint", f"s3.{region}.amazonaws.com")
            .config("spark.hadoop.fs.s3a.path.style.access", "false")
        )

    return builder.getOrCreate()

