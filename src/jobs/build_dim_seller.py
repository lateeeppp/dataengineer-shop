import argparse
import sys
from pathlib import Path

from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.spark_session import get_spark_session


def main(execution_date: str, raw_path: str, silver_path: str):
    spark = get_spark_session("Build_Dim_Seller")

    df_sellers_raw = spark.read.csv(f"{raw_path}/olist_sellers_dataset.csv", header=True, inferSchema=True)
    dim_geolocation = spark.read.parquet(f"{silver_path}/dim_geolocation")

    dim_seller = (
        df_sellers_raw
        .join(dim_geolocation.hint("broadcast"), on=df_sellers_raw["seller_zip_code_prefix"] == dim_geolocation["zip_code_prefix"], how="left")
        .select(
            "seller_id",
            F.col("zip_code_prefix").alias("seller_zip_code_prefix"),
            F.col("seller_city").alias("city"),
            F.col("seller_state").alias("state"),
            "lat",
            "lng",
            F.current_timestamp().alias("updated_at"),
        )
        .dropDuplicates(["seller_id"])
    )

    dim_seller.write.mode("overwrite").parquet(f"{silver_path}/dim_seller")
    print(f"dim_seller saved. Rows: {dim_seller.count()}")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--execution-date", required=True)
    parser.add_argument("--raw-path", required=True)
    parser.add_argument("--silver-path", required=True)
    args = parser.parse_args()
    main(args.execution_date, args.raw_path, args.silver_path)
