"""백필 누락·부분 수집 카테고리 표시 — 막지 않고 dry-run·리포트·DQ로 보이게."""

import unittest
from unittest.mock import MagicMock, patch

from src.bronze_to_silver import backfill

PREVIEW = {
    "source_run_id": "20260822", "batch_date": "2026-08-25",
    "batch_job": "backfill_20260822", "manifest_status": "interrupted",
    "manifest_integrity_ok": True, "part_count": 120,
    "subcategories": ["스킨케어/크림"], "manifest_missing_parts": [],
}
METRICS = {"bronze_loaded": 3071, "silver_ok": 2128, "silver_error": 1100, "error_rate": 0.34}


class CategoryGapsTest(unittest.TestCase):
    def test_manifest_with_targets_reports_missing_and_partial(self):
        manifest = {
            "target_subcategories": ["스킨케어/크림", "스킨케어/로션", "클렌징/오일/밤"],
            "completed_subcategories": ["스킨케어/크림"],
            "parts": [
                {"key": "k1", "category": "스킨케어", "subcategory": "크림"},
                {"key": "k2", "category": "클렌징", "subcategory": "오일/밤"},
            ],
        }
        missing, partial = backfill._category_gaps(MagicMock(), manifest, [])
        self.assertEqual(missing, ["스킨케어/로션"])
        self.assertEqual(partial, ["클렌징/오일/밤"])

    def test_legacy_manifest_uses_bronze_history_and_cannot_judge_partial(self):
        con = MagicMock()
        with patch.object(backfill, "_known_category_paths",
                          return_value={"스킨케어/크림", "스킨케어/로션", "클렌징/오일-밤"}):
            missing, partial = backfill._category_gaps(con, {"parts": []}, ["스킨케어/크림"])
        self.assertEqual(missing, ["스킨케어/로션", "클렌징/오일-밤"])
        self.assertIsNone(partial)

    def test_report_lists_missing_categories(self):
        preview = {**PREVIEW, "missing_subcategories": ["스킨케어/로션", "클렌징/오일-밤"],
                   "partial_subcategories": None}
        text = backfill._format_backfill_report(preview, METRICS, False)
        self.assertIn("누락 카테고리 2개", text)
        self.assertIn("스킨케어/로션", text)
        self.assertNotIn("부분 수집", text)

    def test_report_truncates_long_missing_list(self):
        preview = {**PREVIEW, "missing_subcategories": [f"a/{i}" for i in range(14)],
                   "partial_subcategories": []}
        text = backfill._format_backfill_report(preview, METRICS, False)
        self.assertIn("누락 카테고리 14개", text)
        self.assertIn("외 4개", text)
        self.assertLessEqual(len(text), 2000)

    def test_dq_records_missing_count(self):
        table = MagicMock()
        table.schema.return_value.as_arrow.return_value = None
        catalog = MagicMock()
        catalog.load_table.return_value = table
        preview = {**PREVIEW, "bronze_rows": 3071, "missing_subcategories": ["스킨케어/로션"]}
        error_df = MagicMock()
        error_df.__len__.return_value = 0
        with patch.object(backfill, "pa", MagicMock()):
            metrics = backfill._replace_dq(catalog, preview, error_df, 2128)
        self.assertEqual(metrics["categories_missing"], 1)


if __name__ == "__main__":
    unittest.main()
