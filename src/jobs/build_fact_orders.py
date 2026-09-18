import argparse
import sys
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.spark_session import get_spark_session


def main(execution_date: str, raw_path: str, silver_path: str):
    spark = get_spark_session("Build_Fact_Orders")

    df_items_raw = spark.read.csv(f"{raw_path}/olist_order_items_dataset.csv", header=True, inferSchema=True)
    df_orders_raw = spark.read.csv(f"{raw_path}/olist_orders_dataset.csv", header=True, inferSchema=True)
    df_customers_raw = spark.read.csv(f"{raw_path}/olist_customers_dataset.csv", header=True, inferSchema=True)
    dim_customer = spark.read.parquet(f"{silver_path}/dim_customer")
    dim_seller = spark.read.parquet(f"{silver_path}/dim_seller")

    df_transaksi = (
        df_items_raw
        .join(df_orders_raw, on="order_id", how="inner")
        .join(df_customers_raw.select("customer_id", "customer_unique_id"), on="customer_id", how="inner")
        .withColumn("purchase_ts", F.to_timestamp("order_purchase_timestamp"))
        .withColumn("purchase_date", F.to_date("purchase_ts"))
        .withColumn("date_key", F.date_format("purchase_date", "yyyyMMdd").cast(IntegerType()))
        .filter(F.col("purchase_date") == F.to_date(F.lit(execution_date)))
    )

    fact_orders = (
        df_transaksi
        .join(
            dim_customer,
            on=(
                (df_transaksi.customer_unique_id == dim_customer.customer_unique_id)
                & (df_transaksi.purchase_date >= dim_customer.start_date)
                & (df_transaksi.purchase_date <= dim_customer.end_date)
            ),
            how="inner",
        )
        .select(
            df_transaksi["order_id"],
            df_transaksi["order_item_id"],
            df_transaksi["date_key"],
            df_transaksi["product_id"],
            dim_customer["customer_sk"],
            df_transaksi["order_status"],
            df_transaksi["price"].alias("item_price"),
            df_transaksi["freight_value"],
            (df_transaksi["price"] + df_transaksi["freight_value"]).alias("total_item_value"),
            df_transaksi["purchase_date"],
        )
    )

    df_items_seller = df_items_raw.select("order_id", "order_item_id", "seller_id")

    fact_orders_full = (
        fact_orders
        .join(df_items_seller, on=["order_id", "order_item_id"], how="inner")
        .join(dim_seller.select("seller_id", F.col("seller_id").alias("seller_sk")), on="seller_id", how="left")
        .drop("seller_id")
    )

    if fact_orders_full.count() == 0:
        print(f"Warning: Tidak ada data untuk tanggal {execution_date}. Skip menulis.")
    else:
        (
            fact_orders_full
            .write
            .mode("overwrite")
            .partitionBy("purchase_date")
            .parquet(f"{silver_path}/fact_orders")
        )
        print(f"fact_orders for {execution_date} successfully saved with seller_sk.")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--execution-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--raw-path", required=True)
    parser.add_argument("--silver-path", required=True)
    args = parser.parse_args()
    main(args.execution_date, args.raw_path, args.silver_path)
