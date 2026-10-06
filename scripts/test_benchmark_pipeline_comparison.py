"""Check that benchmark validation detects data changes, not just row counts."""

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import duckdb
import pandas as pd

spec = importlib.util.spec_from_file_location(
    "comparison", Path(__file__).with_name("benchmark_pipeline_comparison.py")
)
comparison = importlib.util.module_from_spec(spec)
spec.loader.exec_module(comparison)


class ComparisonValidationTests(unittest.TestCase):
    def test_row_and_column_order_are_irrelevant(self):
        frame = pd.DataFrame({"id": [1, 2], "nested": [{"b": 2, "a": 1}, None]})
        reordered = frame.iloc[::-1][["nested", "id"]]
        self.assertEqual(comparison.fingerprint(frame), comparison.fingerprint(reordered))

    def test_equal_counts_do_not_hide_changed_values(self):
        before = pd.DataFrame({"id": [1, 2], "ingredients": [["a", "b"], ["c"]]})
        after = pd.DataFrame({"id": [1, 2], "ingredients": [["b", "a"], ["c"]]})
        self.assertNotEqual(comparison.fingerprint(before), comparison.fingerprint(after))

    def test_duplicate_multiplicity_is_preserved(self):
        before = pd.DataFrame({"id": [1, 1, 2]})
        after = pd.DataFrame({"id": [1, 2, 2]})
        self.assertNotEqual(comparison.fingerprint(before), comparison.fingerprint(after))

    def test_statistics_use_sample_deviation_and_mad(self):
        result = comparison.stats([1, 2, 3])
        self.assertEqual(result["median"], 2)
        self.assertEqual(result["sample_stdev"], 1)
        self.assertEqual(result["mad"], 1)

    def test_parquet_cache_does_not_add_directory_partition_column(self):
        with tempfile.TemporaryDirectory() as directory, duckdb.connect() as con:
            source = Path(directory) / "source.json"
            source.write_text(json.dumps([{"id": 1, "run_id": "original", "nested": ["a", "b"]}]))
            with patch.object(comparison.S3, "BRONZE_OPTIMIZED_PATH", directory):
                direct, _ = comparison.pipeline.load_bronze_data_from_files(con, [str(source)], "json")
                materialized, _ = comparison.pipeline.load_bronze_data_from_files(con, [str(source)], "parquet")
                cached, _ = comparison.pipeline.load_bronze_data_from_files(con, [str(source)], "parquet", False)
            self.assertNotIn("file_set", cached.columns)
            self.assertEqual(comparison.fingerprint(direct), comparison.fingerprint(materialized))
            self.assertEqual(comparison.fingerprint(direct), comparison.fingerprint(cached))

    def test_settings_support_common_without_optimized_prefix(self):
        from oliveyoung_common import s3_paths

        settings_spec = importlib.util.spec_from_file_location(
            "legacy_common_settings", comparison.ROOT / "config" / "settings.py"
        )
        settings = importlib.util.module_from_spec(settings_spec)
        with patch.dict(s3_paths.__dict__):
            s3_paths.__dict__.pop("BRONZE_OPTIMIZED_PREFIX", None)
            settings_spec.loader.exec_module(settings)
        self.assertEqual(settings.S3.BRONZE_OPTIMIZED_PREFIX,
                         f"{s3_paths.BRONZE_PREFIX}/_optimized/bronze_to_silver")
        self.assertEqual(settings.S3.BRONZE_OPTIMIZED_PATH,
                         f"s3://{s3_paths.BUCKET}/{settings.S3.BRONZE_OPTIMIZED_PREFIX}")


if __name__ == "__main__":
    unittest.main()
