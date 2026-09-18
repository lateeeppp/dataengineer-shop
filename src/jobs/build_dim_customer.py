import argparse
import sys
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.window import Window

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.spark_session import get_spark_session


def main(execution_date: str, raw_path: str, silver_path: str):
    spark = get_spark_session("Build_Dim_Customer")

    df_customers_raw = spark.read.csv(f"{raw_path}/olist_customers_dataset.csv", header=True, inferSchema=True)
    df_orders_raw = spark.read.csv(f"{raw_path}/olist_orders_dataset.csv", header=True, inferSchema=True)

    df_cust_orders = (
        df_customers_raw
        .join(df_orders_raw.select("order_id", "customer_id", "order_purchase_timestamp"), on="customer_id", how="inner")
        .withColumn("purchase_date", F.to_date(F.to_timestamp("order_purchase_timestamp")))
        .select(
            "customer_unique_id",
            F.col("customer_zip_code_prefix").alias("zip_code"),
            F.col("customer_city").alias("city"),
            F.col("customer_state").alias("state"),
            "purchase_date",
        )
    )

    df_cust_history = (
        df_cust_orders
        .groupBy("customer_unique_id", "zip_code", "city", "state")
        .agg(F.min("purchase_date").alias("start_date"))
    )

    window_spec = Window.partitionBy("customer_unique_id").orderBy("start_date")

    dim_customer_scd2 = (
        df_cust_history
        .withColumn("next_start_date", F.lead("start_date").over(window_spec))
        .withColumn(
            "end_date",
            F.coalesce(F.date_sub(F.col("next_start_date"), 1), F.to_date(F.lit("9999-12-31"))),
        )
        .withColumn("is_current", F.col("next_start_date").isNull())
        .withColumn(
            "customer_sk",
            F.md5(F.concat_ws("||", "customer_unique_id", "city", "state", "start_date")),
        )
        .select(
            "customer_sk",
            "customer_unique_id",
            "zip_code",
            "city",
            "state",
            "start_date",
            "end_date",
            "is_current",
        )
    )

    dim_customer_scd2.write.mode("overwrite").parquet(f"{silver_path}/dim_customer")
    spark.stop()
    print("dim_customer (SCD Type 2) successfully saved.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--execution-date", required=True)
    parser.add_argument("--raw-path", required=True)
    parser.add_argument("--silver-path", required=True)
    args = parser.parse_args()
    main(args.execution_date, args.raw_path, args.silver_path)
