"""
Tests for BI Dashboard Serving Layer (DuckDB & Gold Data Marts)
"""

import unittest
from pathlib import Path

import duckdb
import pandas as pd


class DashboardServingLayerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.project_root = Path(__file__).resolve().parents[1]
        cls.gold_path = cls.project_root / "data" / "gold"
        cls.con = duckdb.connect()

    @classmethod
    def tearDownClass(cls):
        cls.con.close()

    def test_daily_sales_summary_query(self):
        table_path = self.gold_path / "daily_sales_summary"
        if not table_path.exists():
            self.skipTest("Local daily_sales_summary not found")

        df = self.con.execute(
            f"SELECT * FROM read_parquet('{table_path}/*.parquet') ORDER BY purchase_date"
        ).df()

        self.assertFalse(df.empty)
        expected_cols = {"purchase_date", "order_count", "gross_revenue", "avg_order_value", "product_revenue", "shipping_revenue"}
        self.assertTrue(expected_cols.issubset(set(df.columns)))
        self.assertGreater(df["gross_revenue"].sum(), 0)

    def test_category_performance_query(self):
        table_path = self.gold_path / "category_performance"
        if not table_path.exists():
            self.skipTest("Local category_performance not found")

        df = self.con.execute(
            f"SELECT * FROM read_parquet('{table_path}/*.parquet') ORDER BY total_revenue DESC"
        ).df()

        self.assertFalse(df.empty)
        expected_cols = {"category_name", "order_count", "total_revenue", "avg_order_value"}
        self.assertTrue(expected_cols.issubset(set(df.columns)))

    def test_seller_performance_daily_query(self):
        table_path = self.gold_path / "seller_performance_daily"
        if not table_path.exists():
            self.skipTest("Local seller_performance_daily not found")

        df = self.con.execute(
            f"SELECT * FROM read_parquet('{table_path}/*.parquet')"
        ).df()

        self.assertFalse(df.empty)
        expected_cols = {"purchase_date", "seller_id", "city", "state", "order_count", "total_revenue", "avg_order_value"}
        self.assertTrue(expected_cols.issubset(set(df.columns)))

    def test_daily_review_summary_query(self):
        table_path = self.gold_path / "daily_review_summary"
        if not table_path.exists():
            self.skipTest("Local daily_review_summary not found")

        df = self.con.execute(
            f"SELECT * FROM read_parquet('{table_path}/*.parquet')"
        ).df()

        self.assertFalse(df.empty)
        expected_cols = {"purchase_date", "review_count", "avg_review_score", "max_review_score", "min_review_score"}
        self.assertTrue(expected_cols.issubset(set(df.columns)))


if __name__ == "__main__":
    unittest.main()

