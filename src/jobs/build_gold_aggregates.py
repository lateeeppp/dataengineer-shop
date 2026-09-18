import argparse
import sys
from pathlib import Path

from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.spark_session import get_spark_session


def build_daily_sales_summary(spark, silver_path, gold_path):
    fact_orders = spark.read.parquet(f"{silver_path}/fact_orders")

    order_totals = (
        fact_orders
        .groupBy("purchase_date", "order_id")
        .agg(
            F.sum("total_item_value").alias("gross_revenue"),
        )
    )

    daily_sales = (
        order_totals
        .groupBy("purchase_date")
        .agg(
            F.count("order_id").alias("order_count"),
            F.sum("gross_revenue").alias("gross_revenue"),
            F.avg("gross_revenue").alias("avg_order_value"),
        )
        .join(
            fact_orders.groupBy("purchase_date").agg(
                F.sum("item_price").alias("product_revenue"),
                F.sum("freight_value").alias("shipping_revenue"),
            ),
            on="purchase_date",
            how="left",
        )
        .orderBy("purchase_date")
        .cache()
    )

    row_count = daily_sales.count()
    daily_sales.write.mode("overwrite").parquet(f"{gold_path}/daily_sales_summary")
    print(f"daily_sales_summary saved. Rows: {row_count}")
    daily_sales.unpersist()


def build_seller_performance_daily(spark, silver_path, gold_path):
    fact_orders = spark.read.parquet(f"{silver_path}/fact_orders")
    dim_seller = spark.read.parquet(f"{silver_path}/dim_seller")

    dim_seller_key = (
        dim_seller
        .select(
            "seller_id",
            F.col("seller_id").alias("seller_sk"),
            "city",
            "state",
        )
    )

    seller_order_totals = (
        fact_orders
        .join(dim_seller_key, on="seller_sk", how="left")
        .groupBy("purchase_date", "order_id", "seller_id", "city", "state")
        .agg(
            F.sum("total_item_value").alias("total_revenue"),
        )
    )

    seller_perf = (
        seller_order_totals
        .groupBy("purchase_date", "seller_id", "city", "state")
        .agg(
            F.count("order_id").alias("order_count"),
            F.sum("total_revenue").alias("total_revenue"),
            F.avg("total_revenue").alias("avg_order_value"),
        )
        .orderBy("purchase_date", F.desc("total_revenue"))
        .cache()
    )

    row_count = seller_perf.count()
    seller_perf.write.mode("overwrite").parquet(f"{gold_path}/seller_performance_daily")
    print(f"seller_performance_daily saved. Rows: {row_count}")
    seller_perf.unpersist()

def build_category_performance(spark, silver_path, gold_path):
    fact_orders = spark.read.parquet(f"{silver_path}/fact_orders")
    dim_product = spark.read.parquet(f"{silver_path}/dim_product")

    category_order_totals = (
        fact_orders
        .join(dim_product, on="product_id", how="left")
        .groupBy("category_name", "order_id")
        .agg(
            F.sum("total_item_value").alias("total_revenue"),
        )
    )

    category_perf = (
        category_order_totals
        .groupBy("category_name")
        .agg(
            F.count("order_id").alias("order_count"),
            F.sum("total_revenue").alias("total_revenue"),
            F.avg("total_revenue").alias("avg_order_value"),
        )
        .orderBy(F.desc("total_revenue"))
        .cache()
    )

    row_count = category_perf.count()
    category_perf.write.mode("overwrite").parquet(f"{gold_path}/category_performance")
    print(f"category_performance saved. Rows: {row_count}")
    category_perf.unpersist()

def build_daily_review_summary(spark, silver_path, gold_path):
    fact_order_reviews = spark.read.parquet(f"{silver_path}/fact_order_reviews")

    review_summary = (
        fact_order_reviews
        .groupBy("purchase_date")
        .agg(
            F.count("review_id").alias("review_count"),
            F.avg("review_score").alias("avg_review_score"),
            F.max("review_score").alias("max_review_score"),
            F.min("review_score").alias("min_review_score"),
        )
        .orderBy("purchase_date")
        .cache()
    )

    row_count = review_summary.count()
    review_summary.write.mode("overwrite").parquet(f"{gold_path}/daily_review_summary")
    print(f"daily_review_summary saved. Rows: {row_count}")
    review_summary.unpersist()


def main(silver_path: str, gold_path: str):
    spark = get_spark_session("Build_Gold_Aggregates")

    build_daily_sales_summary(spark, silver_path, gold_path)
    build_seller_performance_daily(spark, silver_path, gold_path)
    build_category_performance(spark, silver_path, gold_path)
    build_daily_review_summary(spark, silver_path, gold_path)

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver-path", required=True)
    parser.add_argument("--gold-path", required=True)
    args = parser.parse_args()

    main(args.silver_path, args.gold_path)