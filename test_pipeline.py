import csv
import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class PipelineTests(unittest.TestCase):

    def run_pipeline(self, *args):
        return subprocess.run(
            [sys.executable, str(ROOT / "pipeline.py"), *args],
            cwd=ROOT,
            capture_output=True,
            text=True,
        )

    def test_database_initialization(self):
        result = self.run_pipeline("--init")
        self.assertEqual(result.returncode, 0)
        self.assertIn(
            "Database initialized successfully",
            result.stdout,
        )

    def test_missing_input_file_fails(self):
        result = self.run_pipeline(
            "--input", "data/file_that_does_not_exist.csv"
        )
        self.assertNotEqual(result.returncode, 0)

    def test_negative_lookback_fails(self):
        result = self.run_pipeline(
            "--input", "data/orders.csv",
            "--lookback-minutes", "-1",
        )
        self.assertNotEqual(result.returncode, 0)

    def test_sample_file_exists(self):
        self.assertTrue((ROOT / "data" / "orders.csv").exists())

    def test_csv_has_expected_columns(self):
        with (ROOT / "data" / "orders.csv").open(
            newline="", encoding="utf-8"
        ) as file:
            reader = csv.DictReader(file)
            self.assertEqual(
                set(reader.fieldnames),
                {
                    "order_id",
                    "customer_id",
                    "order_amount",
                    "status",
                    "updated_at",
                    "source_version",
                    "is_deleted",
                },
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)