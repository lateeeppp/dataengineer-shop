import importlib
import unittest


class PipelineJobsTest(unittest.TestCase):
    def test_missing_silver_jobs_exist(self):
        modules = [
            "src.jobs.build_dim_geolocation",
            "src.jobs.build_dim_seller",
            "src.jobs.build_fact_order_payments",
            "src.jobs.build_fact_order_reviews",
            "src.jobs.build_fact_orders",
        ]

        for module_name in modules:
            with self.subTest(module=module_name):
                module = importlib.import_module(module_name)
                self.assertTrue(hasattr(module, "main"), f"{module_name} missing main()")


if __name__ == "__main__":
    unittest.main()
