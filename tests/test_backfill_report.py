"""Backfill completion report is distinct, nonfatal, and sent only after verification."""

import json
import os
import unittest
from unittest.mock import MagicMock, patch

from src.bronze_to_silver import backfill


PREVIEW = {
    "source_run_id": "20260731", "batch_date": "2026-08-01",
    "batch_job": "backfill_20260731", "manifest_status": "interrupted",
    "manifest_integrity_ok": True, "part_count": 218,
    "subcategories": ["skin/cream", "skin/lotion"],
    "manifest_missing_parts": [],
}
METRICS = {
    "bronze_loaded": 4142, "silver_ok": 2400, "silver_error": 1600,
    "error_rate": 0.4, "err_UNMAPPED": 1300, "err_INVALID": 300,
}


class BackfillReportTest(unittest.TestCase):
    def test_interrupted_but_integrity_ok_is_informational_not_a_warning(self):
        # This crawl is chronically interrupted — that alone is normal, so the
        # header must be informational (ℹ️), not an override warning (⚠️).
        with patch.dict(os.environ, {
            "DISCORD_DQ_WEBHOOK_URL": "https://example.invalid/secret-token",
            "BACKFILL_DQ_DASHBOARD_URL": "https://grafana.example/d/oliveyoung-dq-table",
        }), patch.object(backfill.urllib.request, "urlopen") as post:
            post.return_value.__enter__.return_value = MagicMock()
            backfill._report(PREVIEW, METRICS, False)
            request = post.call_args.args[0]
            payload = json.loads(request.data.decode("utf-8"))["content"]
            self.assertEqual(request.get_method(), "POST")
            self.assertEqual(request.get_header("User-agent"), "oliveyoung-backfill/1.0")
            self.assertIn("ℹ️", payload)
            self.assertIn("[올리브영 전처리 백필]", payload)
            self.assertNotIn("⚠️ **[올리브영 전처리 백필]", payload)
            self.assertIn("부분 크롤", payload)
            self.assertIn("interrupted", payload)
            self.assertIn("20260731", payload)
            self.assertIn("4,142건", payload)
            self.assertIn("✅ 정상", payload)
            self.assertIn("2,400건", payload)
            self.assertIn("40.0%", payload)
            self.assertIn("silver_current", payload)
            self.assertIn("bronze_to_silver_backfill", payload)
            self.assertLessEqual(len(payload), 2000)

    def test_integrity_override_produces_explicit_warning(self):
        preview = {**PREVIEW, "manifest_integrity_ok": False,
                   "manifest_rogue_parts": ["oliveyoung/스킨케어/로션/run_id=20260731/part_9999.json"]}
        with patch.dict(os.environ, {"DISCORD_DQ_WEBHOOK_URL": "https://example.invalid/secret-token"}), \
             patch.object(backfill.urllib.request, "urlopen") as post:
            post.return_value.__enter__.return_value = MagicMock()
            backfill._report(preview, METRICS, True)
            payload = json.loads(post.call_args.args[0].data.decode("utf-8"))["content"]
            self.assertIn("⚠️", payload)
            self.assertIn("무결성 이상", payload)
            self.assertIn("`True`", payload)  # 무결성 override 사용

    def test_missing_parts_are_surfaced_as_informational(self):
        preview = {**PREVIEW, "manifest_missing_parts": ["oliveyoung/맨즈케어/스킨/run_id=20260731/part_0000.json"]}
        with patch.dict(os.environ, {"DISCORD_DQ_WEBHOOK_URL": "https://example.invalid/secret-token"}), \
             patch.object(backfill.urllib.request, "urlopen") as post:
            post.return_value.__enter__.return_value = MagicMock()
            backfill._report(preview, METRICS, False)
            payload = json.loads(post.call_args.args[0].data.decode("utf-8"))["content"]
            self.assertIn("manifest엔 있으나 S3엔 없는 part 1개", payload)

    def test_report_failure_does_not_expose_webhook_or_raise(self):
        webhook = "https://example.invalid/secret-token"
        with patch.dict(os.environ, {"DISCORD_DQ_WEBHOOK_URL": webhook}), \
             patch.object(backfill.urllib.request, "urlopen",
                          side_effect=RuntimeError(webhook)), \
             self.assertLogs(backfill.logger, level="WARNING") as logs:
            backfill._report(PREVIEW, METRICS, True)
        self.assertNotIn(webhook, " ".join(logs.output))
        self.assertIn("RuntimeError", " ".join(logs.output))

    def test_no_webhook_means_no_send(self):
        with patch.dict(os.environ, {"DISCORD_DQ_WEBHOOK_URL": ""}), \
             patch.object(backfill.urllib.request, "urlopen") as post, \
             self.assertLogs(backfill.logger, level="WARNING"):
            backfill._report(PREVIEW, METRICS, False)
        post.assert_not_called()

    def test_failed_data_verification_never_sends_success_report(self):
        with patch.object(backfill, "_replace_history"), \
             patch.object(backfill, "_replace_dq", return_value=METRICS), \
             patch.object(backfill, "_verify", side_effect=RuntimeError("verify failed")), \
             patch.object(backfill, "_report") as report:
            with self.assertRaisesRegex(RuntimeError, "verify failed"):
                backfill._commit_backfill(MagicMock(), PREVIEW, [], [], True)
            report.assert_not_called()


if __name__ == "__main__":
    unittest.main()
