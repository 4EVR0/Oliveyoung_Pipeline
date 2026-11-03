"""Focused checks for benchmark metric interpretation; no AWS calls."""

from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from scripts.benchmark_athena_compaction import Runner


class MetricTests(unittest.TestCase):
    def runner(self, points):
        runner = Runner.__new__(Runner)
        runner.args = SimpleNamespace(
            metrics_filter="SilverHistoryMetrics", metrics_wait=0,
            metrics_bucket="example", region="ap-northeast-2",
        )
        runner.report = {"metric_windows": {"before": {
            "start": "2026-11-03T00:00:00+00:00", "end": "2026-11-03T00:01:00+00:00",
        }}}
        runner.cloudwatch = Mock()
        runner.cloudwatch.get_metric_statistics.return_value = {"Datapoints": points}
        runner.save = Mock()
        return runner

    def test_missing_is_not_zero(self):
        runner = self.runner([])
        runner.collect_metrics()
        window = runner.report["metric_windows"]["before"]
        self.assertIsNone(window["get_requests_sum"])
        self.assertFalse(window["complete_minutes"])

    def test_observed_zero_is_valid(self):
        runner = self.runner([{"Sum": 0}])
        runner.collect_metrics()
        window = runner.report["metric_windows"]["before"]
        self.assertEqual(window["get_requests_sum"], 0)
        self.assertTrue(window["complete_minutes"])

    def test_partial_minutes_are_marked_incomplete(self):
        runner = self.runner([{"Sum": 150}])
        runner.report["metric_windows"]["before"]["end"] = "2026-11-03T00:02:00+00:00"
        runner.collect_metrics()
        window = runner.report["metric_windows"]["before"]
        self.assertEqual(window["get_requests_sum"], 150)
        self.assertFalse(window["complete_minutes"])

    @patch("scripts.benchmark_athena_compaction.boto3.client")
    def test_wrong_prefix_stops_before_probe(self, client):
        runner = self.runner([])
        client.return_value.get_bucket_metrics_configuration.return_value = {
            "MetricsConfiguration": {"Filter": {"Prefix": "wrong/"}},
        }
        with self.assertRaises(ValueError):
            runner.wait_for_metrics()
        client.return_value.get_object.assert_not_called()


if __name__ == "__main__":
    unittest.main()
