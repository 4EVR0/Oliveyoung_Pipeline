"""백필이 정상 전처리와 같은 선택 규칙(기준 시점 = source_run_id)을 쓰는지, 리포트·DQ 표시."""

import unittest
from unittest.mock import MagicMock, patch

from src.bronze_to_silver import backfill
from tests.test_bronze_gate import BUCKET, CATS, CREAM, ESSENCE, SUN, _manifest, _normal, _world

PREVIEW = {
    "source_run_id": "20260822", "batch_date": "2026-08-25",
    "batch_job": "backfill_20260822", "manifest_status": "interrupted",
    "manifest_integrity_ok": True, "part_count": 120,
    "subcategories": ["스킨케어/크림"], "manifest_missing_parts": [],
}
METRICS = {"bronze_loaded": 3071, "silver_ok": 2128, "silver_error": 1100, "error_rate": 0.34}


class SelectForRunTest(unittest.TestCase):
    def _select(self, runs, source):
        with patch.object(backfill, "load_inputs", return_value=_world(runs)), \
             patch.object(backfill.boto3, "client"):
            return backfill._select_for_run(source)

    def test_missing_category_filled_from_earlier_run(self):
        runs = [_normal("20260819"), _normal("20260822", counts={CREAM: 775, SUN: 66}), _normal("20260825")]
        plans, files, _ = self._select(runs, "20260822")
        by_key = {p.key: p for p in plans.values()}
        self.assertEqual(by_key[ESSENCE].selected_run, "20260819")      # 이전 run으로 채움
        self.assertEqual(by_key[CREAM].selected_run, "20260822")
        self.assertFalse(any("run_id=20260825/" in f for f in files))    # 이후 run은 안 봄

    def test_future_category_excluded(self):
        runs = [_normal("20260822", counts={CREAM: 775}), _normal("20260825")]
        plans, files, _ = self._select(runs, "20260822")
        self.assertEqual({p.key for p in plans.values()}, {CREAM})

    def test_partial_substituted(self):
        runs = [_normal(r) for r in ("20260813", "20260816", "20260819")] + [
            ("20260822", _manifest("20260822", {ESSENCE: 258, CREAM: 775, SUN: 66}, new=False),
             [ESSENCE, CREAM, SUN])]
        plans, _, _ = self._select(runs, "20260822")
        self.assertEqual({p.key: p for p in plans.values()}[ESSENCE].selected_run, "20260819")



class IntegrityTest(unittest.TestCase):
    """무결성은 run별 — 합계가 맞아도 run끼리 상쇄되면 실패해야 한다."""

    def _check(self, rows_by_run):
        import pandas as pd
        runs = [_normal("20260819", counts={ESSENCE: 100, CREAM: 50, SUN: 10}),
                _normal("20260822", counts={CREAM: 50, SUN: 10})]          # 에센스는 0819로 채움
        with patch.object(backfill, "load_inputs", return_value=_world(runs)),              patch.object(backfill.boto3, "client"):
            plans, files, manifests = backfill._select_for_run("20260822")
        df = pd.DataFrame({"source_run_id": [r for r, n in rows_by_run.items() for _ in range(n)]})
        return backfill.check_integrity(plans, manifests, files, df)

    def test_exact_counts_pass(self):
        self.assertTrue(self._check({"20260819": 100, "20260822": 60})["ok"])

    def test_offsetting_counts_fail(self):
        r = self._check({"20260819": 90, "20260822": 70})                  # 합계 160은 같음
        self.assertFalse(r["ok"])
        self.assertEqual(r["count_mismatch"], {"20260819": (100, 90), "20260822": (60, 70)})

    def test_rogue_part_fails(self):
        import pandas as pd
        index, manifests = _world([_normal("20260822")])
        plans, _ = backfill.select_inputs(index, manifests, "20260822")
        rogue = f"{BUCKET}/{CATS[CREAM]}/run_id=20260822/part_9999.json"   # manifest가 모르는 part
        files = backfill.files_for(plans, index) + [rogue]
        df = pd.DataFrame({"source_run_id": ["20260822"] * (900 + 775 + 66)})
        r = backfill.check_integrity(plans, manifests, files, df)
        self.assertFalse(r["ok"])
        self.assertEqual(r["rogue_parts"], [rogue.removeprefix("s3://oliveyoung-crawl-data/")])

class BackfillReportTest(unittest.TestCase):
    def test_report_lists_filled_substituted_and_excluded(self):
        preview = {**PREVIEW, "filled_subcategories": ["스킨케어/로션", "스킨케어/에센스/세럼/앰플"],
                   "partial_subcategories": ["스킨케어/에센스/세럼/앰플"],
                   "missing_subcategories": ["클렌징/오일/밤"]}
        text = backfill._format_backfill_report(preview, METRICS, False)
        self.assertIn("이전 run으로 채움 2개", text)
        self.assertIn("상품 수 미달로 대체 1개", text)
        self.assertIn("쓸 run이 없어 제외 1개", text)

    def test_report_truncates_long_list(self):
        preview = {**PREVIEW, "filled_subcategories": [f"a/{i}" for i in range(14)]}
        text = backfill._format_backfill_report(preview, METRICS, False)
        self.assertIn("외 4개", text)
        self.assertLessEqual(len(text), 2000)

    def test_dq_records_excluded_count(self):
        table = MagicMock()
        catalog = MagicMock()
        catalog.load_table.return_value = table
        preview = {**PREVIEW, "bronze_rows": 3071, "missing_subcategories": ["클렌징/오일/밤"]}
        error_df = MagicMock()
        error_df.__len__.return_value = 0
        with patch.object(backfill, "pa", MagicMock()):
            metrics = backfill._replace_dq(catalog, preview, error_df, 2128)
        self.assertEqual(metrics["categories_missing"], 1)


if __name__ == "__main__":
    unittest.main()
