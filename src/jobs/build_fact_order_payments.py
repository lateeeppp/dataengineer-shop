import argparse
import sys
from pathlib import Path

from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.spark_session import get_spark_session


def main(execution_date: str, raw_path: str, silver_path: str):
    spark = get_spark_session("Build_Fact_Order_Payments")

    df_payments_raw = spark.read.csv(f"{raw_path}/olist_order_payments_dataset.csv", header=True, inferSchema=True)
    df_orders_raw = spark.read.csv(f"{raw_path}/olist_orders_dataset.csv", header=True, inferSchema=True)

    fact_order_payments = (
        df_payments_raw
        .join(df_orders_raw.select("order_id", "order_purchase_timestamp"), on="order_id", how="inner")
        .withColumn("purchase_date", F.to_date(F.to_timestamp("order_purchase_timestamp")))
        .withColumn("date_key", F.date_format("purchase_date", "yyyyMMdd").cast("int"))
        .filter(F.col("purchase_date").isNotNull())
        .select(
            "order_id",
            "payment_sequential",
            "payment_type",
            "payment_installments",
            "payment_value",
            "purchase_date",
            "date_key",
        )
    )

    fact_order_payments.write.mode("overwrite").partitionBy("purchase_date").parquet(f"{silver_path}/fact_order_payments")
    print(f"fact_order_payments saved. Rows: {fact_order_payments.count()}")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--execution-date", required=True)
    parser.add_argument("--raw-path", required=True)
    parser.add_argument("--silver-path", required=True)
    args = parser.parse_args()
    main(args.execution_date, args.raw_path, args.silver_path)
