import argparse
import sys
from pathlib import Path

from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.spark_session import get_spark_session


def main(execution_date: str, raw_path: str, silver_path: str):
    spark = get_spark_session("Build_Dim_Product")

    df_products_raw = spark.read.csv(f"{raw_path}/olist_products_dataset.csv", header=True, inferSchema=True)
    df_categories_raw = spark.read.csv(f"{raw_path}/product_category_name_translation.csv", header=True, inferSchema=True)

    dim_product = (
        df_products_raw
        .join(df_categories_raw, on="product_category_name", how="left")
        .select(
            F.col("product_id"),
            F.coalesce(F.col("product_category_name_english"), F.lit("unknown")).alias("category_name"),
            F.coalesce(F.col("product_weight_g"), F.lit(0)).alias("weight_g"),
            F.coalesce(F.col("product_length_cm"), F.lit(0)).alias("length_cm"),
            F.coalesce(F.col("product_height_cm"), F.lit(0)).alias("height_cm"),
            F.coalesce(F.col("product_width_cm"), F.lit(0)).alias("width_cm"),
            F.current_timestamp().alias("updated_at"),
        )
        .dropDuplicates(["product_id"])
        .cache()
    )

    row_count = dim_product.count()
    dim_product.write.mode("overwrite").parquet(f"{silver_path}/dim_product")
    print(f"dim_product successfully saved. Rows: {row_count}")
    dim_product.unpersist()
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--execution-date", required=True)
    parser.add_argument("--raw-path", required=True)
    parser.add_argument("--silver-path", required=True)
    args = parser.parse_args()
    main(args.execution_date, args.raw_path, args.silver_path)
