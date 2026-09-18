import os

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from pendulum import datetime


PROJECT_ROOT = os.getenv("PROJECT_ROOT", "/opt/project")
RAW_PATH = os.getenv("RAW_PATH", f"{PROJECT_ROOT}/data/raw/olist")
SILVER_PATH = os.getenv("SILVER_PATH", f"{PROJECT_ROOT}/data/silver")
GOLD_PATH = os.getenv("GOLD_PATH", f"{PROJECT_ROOT}/data/gold")
PYTHON = os.getenv("PYTHON_BIN", "python3")


def run_job(task_id, script, arguments):
    command = " ".join(
        [
            f"cd {PROJECT_ROOT}",
            "&&",
            f"{PYTHON} src/jobs/{script}",
            arguments,
        ]
    )
    return BashOperator(task_id=task_id, bash_command=command)


with DAG(
    dag_id="daily_retail_etl",
    start_date=datetime(2017, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["retail", "spark", "silver", "gold"],
) as dag:
    build_dim_geolocation = run_job(
        "build_dim_geolocation",
        "build_dim_geolocation.py",
        f"--execution-date {{{{ ds }}}} --raw-path {RAW_PATH} --silver-path {SILVER_PATH}",
    )
    build_dim_seller = run_job(
        "build_dim_seller",
        "build_dim_seller.py",
        f"--execution-date {{{{ ds }}}} --raw-path {RAW_PATH} --silver-path {SILVER_PATH}",
    )
    build_dim_date = run_job(
        "build_dim_date",
        "build_dim_date.py",
        f"--execution-date {{{{ ds }}}} --silver-path {SILVER_PATH}",
    )
    build_dim_customer = run_job(
        "build_dim_customer",
        "build_dim_customer.py",
        f"--execution-date {{{{ ds }}}} --raw-path {RAW_PATH} --silver-path {SILVER_PATH}",
    )
    build_dim_product = run_job(
        "build_dim_product",
        "build_dim_product.py",
        f"--execution-date {{{{ ds }}}} --raw-path {RAW_PATH} --silver-path {SILVER_PATH}",
    )
    build_fact_orders = run_job(
        "build_fact_orders",
        "build_fact_orders.py",
        f"--execution-date {{{{ ds }}}} --raw-path {RAW_PATH} --silver-path {SILVER_PATH}",
    )
    build_fact_order_payments = run_job(
        "build_fact_order_payments",
        "build_fact_order_payments.py",
        f"--execution-date {{{{ ds }}}} --raw-path {RAW_PATH} --silver-path {SILVER_PATH}",
    )
    build_fact_order_reviews = run_job(
        "build_fact_order_reviews",
        "build_fact_order_reviews.py",
        f"--execution-date {{{{ ds }}}} --raw-path {RAW_PATH} --silver-path {SILVER_PATH}",
    )
    build_gold = run_job(
        "build_gold_aggregates",
        "build_gold_aggregates.py",
        f"--silver-path {SILVER_PATH} --gold-path {GOLD_PATH}",
    )

    build_dim_geolocation >> build_dim_seller
    [build_dim_date, build_dim_customer, build_dim_product, build_dim_seller] >> build_fact_orders
    [build_dim_date, build_dim_customer, build_dim_product, build_dim_seller] >> build_fact_order_payments
    [build_dim_date, build_dim_customer, build_dim_product, build_dim_seller] >> build_fact_order_reviews
    [build_fact_orders, build_fact_order_payments, build_fact_order_reviews] >> build_gold
