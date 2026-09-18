import argparse
import sys
from pathlib import Path

from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.spark_session import get_spark_session


def main(execution_date: str, raw_path: str, silver_path: str):
    spark = get_spark_session("Build_Dim_Geolocation")

    df_geo_raw = spark.read.csv(f"{raw_path}/olist_geolocation_dataset.csv", header=True, inferSchema=True)

    dim_geolocation = (
        df_geo_raw
        .withColumnRenamed("geolocation_zip_code_prefix", "zip_code_prefix")
        .groupBy("zip_code_prefix")
        .agg(
            F.mean("geolocation_lat").alias("lat"),
            F.mean("geolocation_lng").alias("lng"),
            F.first("geolocation_city").alias("city"),
            F.first("geolocation_state").alias("state"),
        )
        .select(
            "zip_code_prefix",
            F.round("lat", 6).alias("lat"),
            F.round("lng", 6).alias("lng"),
            "city",
            "state",
        )
    )

    dim_geolocation.write.mode("overwrite").parquet(f"{silver_path}/dim_geolocation")
    print(f"dim_geolocation saved. Rows: {dim_geolocation.count()}")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--execution-date", required=True)
    parser.add_argument("--raw-path", required=True)
    parser.add_argument("--silver-path", required=True)
    args = parser.parse_args()
    main(args.execution_date, args.raw_path, args.silver_path)
