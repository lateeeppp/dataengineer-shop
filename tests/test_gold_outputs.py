import unittest
from pathlib import Path

import duckdb


class GoldOutputsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.project_root = Path(__file__).resolve().parents[1]
        cls.gold_path = cls.project_root / "data" / "gold"
        cls.connection = duckdb.connect()

    @classmethod
    def tearDownClass(cls):
        cls.connection.close()

    def assert_metric_consistency(self, table_name, revenue_column):
        query = f"""
            SELECT COUNT(*)
            FROM read_parquet('{self.gold_path / table_name}/*.parquet')
            WHERE order_count <= 0
               OR {revenue_column} < 0
               OR avg_order_value < 0
               OR ABS(avg_order_value - {revenue_column} / order_count) > 0.0001
        """
        invalid_rows = self.connection.execute(query).fetchone()[0]
        self.assertEqual(invalid_rows, 0, table_name)

    def test_gold_outputs_exist_and_are_non_empty(self):
        tables = [
            "daily_sales_summary",
            "seller_performance_daily",
            "category_performance",
            "daily_review_summary",
        ]

        for table_name in tables:
            table_path = self.gold_path / table_name
            self.assertTrue(table_path.exists(), table_name)
            row_count = self.connection.execute(
                f"SELECT COUNT(*) FROM read_parquet('{table_path}/*.parquet')"
            ).fetchone()[0]
            self.assertGreater(row_count, 0, table_name)

    def test_order_level_metrics_are_consistent(self):
        self.assert_metric_consistency("daily_sales_summary", "gross_revenue")
        self.assert_metric_consistency("seller_performance_daily", "total_revenue")
        self.assert_metric_consistency("category_performance", "total_revenue")


if __name__ == "__main__":
    unittest.main()
