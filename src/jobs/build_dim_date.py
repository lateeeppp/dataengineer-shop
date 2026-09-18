import argparse
import sys
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.spark_session import get_spark_session

def main(execution_date: str, silver_path: str):
    spark = get_spark_session('Build_Dim_Date')

    df_date_range = spark.sql(
        """
        select
            explode(sequence(
                to_date('2016-01-01'), to_date('2018-12-31'), interval 1 day
            )) as calendar_date
        """
    )

    dim_date = (
        df_date_range
        .withColumn("date_key", F.date_format("calendar_date", "yyyyMMdd").cast(IntegerType()))
        .withColumn("year", F.year("calendar_date"))
        .withColumn("month", F.month("calendar_date"))
        .withColumn("month_name", F.date_format("calendar_date", "MMMM"))
        .withColumn("day", F.dayofmonth("calendar_date"))
        .withColumn("day_of_week", F.dayofweek("calendar_date"))
        .withColumn("day_name", F.date_format("calendar_date", "EEEE"))
        .withColumn("quarter", F.quarter("calendar_date"))
        .withColumn("is_weekend", F.when(F.col("day_of_week").isin(1, 7), True).otherwise(False))
    )

    dim_date.write.mode("overwrite").parquet(f"{silver_path}/dim_date")
    spark.stop()
    print("dim_date successfully saved.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--execution-date", required=True, help="Execution date in YYYY-MM-DD format")
    parser.add_argument("--silver-path", required=True, help="Path to Silver layer directory")
    args = parser.parse_args()
    main(args.execution_date, args.silver_path)