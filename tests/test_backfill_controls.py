"""Backfill must require a deliberate second run before any write."""

import unittest
from unittest.mock import MagicMock, patch

from src.bronze_to_silver import backfill


class BackfillControlsTest(unittest.TestCase):
    def test_apply_requires_matching_confirmation_before_connecting(self):
        with patch.object(backfill.OliveyoungIceberg, "get_catalog") as catalog:
            with self.assertRaises(ValueError):
                backfill.run("20260725", "apply", None)
            with self.assertRaises(ValueError):
                backfill.run("20260725", "apply", "20260726")
            catalog.assert_not_called()

    def test_dry_run_never_writes_or_sends_report(self):
        preview = {
            "source_run_id": "20260725", "batch_date": "2026-07-25",
            "batch_job": "backfill_20260725", "manifest_status": "completed",
            "manifest_integrity_ok": True, "bronze_rows": 1,
            "existing": {"history_other_jobs": [], "dq_other_runs": [], "dq_normal_runs": []},
        }
        with patch.object(backfill.OliveyoungIceberg, "get_catalog", return_value=MagicMock()), \
             patch.object(backfill.DuckDB, "get_connection", return_value=MagicMock()), \
             patch.object(backfill, "preflight", return_value=(preview, MagicMock())), \
             patch.object(backfill, "_replace_history") as history, \
             patch.object(backfill, "_replace_dq") as dq, \
             patch.object(backfill, "_report") as report:
            backfill.run("20260725", "dry-run", None)
            history.assert_not_called()
            dq.assert_not_called()
            report.assert_not_called()

    def test_manifest_integrity_failure_requires_explicit_override(self):
        preview = {"manifest_status": "interrupted", "manifest_integrity_ok": False,
                   "bronze_rows": 1,
                   "existing": {"history_other_jobs": [], "dq_other_runs": [], "dq_normal_runs": []}}
        with self.assertRaises(ValueError):
            backfill._assert_safe(preview, False)
        backfill._assert_safe(preview, True)

    def test_interrupted_status_with_integrity_ok_does_not_require_override(self):
        # This crawl is chronically interrupted — that alone is normal, not a
        # reason to block. Only a genuine integrity mismatch requires override.
        preview = {"manifest_status": "interrupted", "manifest_integrity_ok": True,
                   "bronze_rows": 1,
                   "existing": {"history_other_jobs": [], "dq_other_runs": [], "dq_normal_runs": []}}
        backfill._assert_safe(preview, False)  # must not raise

    def test_in_progress_manifest_is_hard_rejected_even_with_override(self):
        preview = {"manifest_status": "in_progress", "manifest_integrity_ok": True,
                   "bronze_rows": 1,
                   "existing": {"history_other_jobs": [], "dq_other_runs": [], "dq_normal_runs": []}}
        with self.assertRaises(ValueError):
            backfill._assert_safe(preview, True)

    def test_same_day_other_batch_is_rejected_even_with_override(self):
        preview = {"manifest_status": "completed", "manifest_integrity_ok": True,
                   "bronze_rows": 1,
                   "existing": {"history_other_jobs": ["normal"], "dq_other_runs": [], "dq_normal_runs": []}}
        with self.assertRaises(ValueError):
            backfill._assert_safe(preview, True)

    def test_same_day_normal_dq_is_rejected_even_without_history_rows(self):
        preview = {"manifest_status": "completed", "manifest_integrity_ok": True,
                   "bronze_rows": 1,
                   "existing": {"history_other_jobs": [], "dq_other_runs": [],
                                "dq_normal_runs": ["bronze_to_silver_20260725_010000"]}}
        with self.assertRaises(ValueError):
            backfill._assert_safe(preview, True)


if __name__ == "__main__":
    unittest.main()
