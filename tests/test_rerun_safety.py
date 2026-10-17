"""재실행 안전 보강(독립 검토 지적 대응) — 0건 중단, DQ 요약 실패 시 유형별 생략, CSV 비치명, 백필 사전 검사."""

import importlib
import sys
import types
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from src.bronze_to_silver import pipeline


def _run(silver, error, dq_side_effect=None, csv_side_effect=None):
    """run_pipeline을 외부 I/O만 대체해 실행하고 주요 호출 목을 돌려준다."""
    with patch.object(pipeline, "check_batch_date_required"), \
         patch.object(pipeline.DuckDB, "get_connection"), \
         patch.object(pipeline, "load_bronze_data", return_value=(pd.DataFrame({"x": [1, 2]}), "20261003")), \
         patch.object(pipeline, "load_dictionaries"), \
         patch.object(pipeline, "_resolve_batch_date", return_value="2026-10-04"), \
         patch.object(pipeline, "process_pipeline", return_value=(silver, error)), \
         patch.object(pipeline.OliveyoungIceberg, "get_catalog"), \
         patch.object(pipeline, "write_to_iceberg", return_value=0) as w, \
         patch.object(pipeline, "_verify_silver"), \
         patch.object(pipeline, "write_dq_metrics", side_effect=dq_side_effect) as dq, \
         patch.object(pipeline, "write_csv_to_s3", side_effect=csv_side_effect) as csv:
        pipeline.run_pipeline()
    return w, dq, csv


class PipelineSafetyTest(unittest.TestCase):
    def test_zero_silver_stops_before_any_write(self):
        with patch.object(pipeline, "write_to_iceberg") as w, patch.object(pipeline, "write_dq_metrics") as dq:
            with self.assertRaises(RuntimeError):
                _run(pd.DataFrame(), pd.DataFrame({"error_type": ["X"]}))
        w.assert_not_called()
        dq.assert_not_called()

    def test_error_type_dq_skipped_when_summary_fails(self):
        _, dq, _ = _run(pd.DataFrame({"p": [1]}), pd.DataFrame({"error_type": ["X"]}),
                        dq_side_effect=[RuntimeError("iceberg down"), None])
        self.assertEqual(dq.call_count, 1)          # 요약 실패 → 유형별은 쓰지 않음(이전 완전한 기록 보존)

    def test_error_type_dq_written_when_summary_succeeds(self):
        _, dq, _ = _run(pd.DataFrame({"p": [1]}), pd.DataFrame({"error_type": ["X"]}))
        self.assertEqual(dq.call_count, 2)
        self.assertIn("err_X", dq.call_args_list[1].kwargs)

    def test_csv_failure_does_not_fail_task(self):
        _, _, csv = _run(pd.DataFrame({"p": [1]}), pd.DataFrame(), csv_side_effect=RuntimeError("s3"))
        csv.assert_called_once()                    # 예외가 밖으로 나오지 않음


class _Op:
    def __init__(self, **kw): self.kw = kw
    def __rshift__(self, other): return other


class BackfillGuardTest(unittest.TestCase):
    """dags/oliveyoung_backfill.py의 실제 검사 함수를 최소 Airflow 대역으로 실행."""

    def setUp(self):
        self.active = {}
        state = types.SimpleNamespace(RUNNING="running", QUEUED="queued")
        find = lambda dag_id, state: [types.SimpleNamespace(run_id=r) for r in self.active.get(state, [])]
        dag_cm = MagicMock(); dag_cm.__enter__.return_value = MagicMock(); dag_cm.__exit__.return_value = False

        class _Fail(Exception):
            pass
        self.fail_exc = _Fail
        mods = {
            "airflow": types.SimpleNamespace(DAG=lambda **kw: dag_cm),
            "airflow.exceptions": types.SimpleNamespace(AirflowFailException=_Fail),
            "airflow.models": types.SimpleNamespace(DagRun=types.SimpleNamespace(find=find)),
            "airflow.models.param": types.SimpleNamespace(Param=lambda *a, **k: None),
            "airflow.operators.python": types.SimpleNamespace(PythonOperator=_Op),
            "airflow.providers.docker.operators.docker": types.SimpleNamespace(DockerOperator=_Op),
            "airflow.utils.state": types.SimpleNamespace(DagRunState=state),
        }
        self._patch = patch.dict(sys.modules, mods)
        self._patch.start()
        sys.modules.pop("dags.oliveyoung_backfill", None)
        self.dag_module = importlib.import_module("dags.oliveyoung_backfill")

    def tearDown(self):
        self._patch.stop()
        sys.modules.pop("dags.oliveyoung_backfill", None)

    def _call(self, mode):
        run = types.SimpleNamespace(conf={"mode": mode})
        return self.dag_module._guard_no_normal_run(dag_run=run)

    def test_apply_rejected_while_normal_pipeline_running_or_queued(self):
        for state in ("running", "queued"):
            self.active = {state: ["manual__2026-10-17"]}
            with self.assertRaises(self.fail_exc):
                self._call("apply")

    def test_apply_allowed_when_idle_and_dry_run_never_blocked(self):
        self._call("apply")
        self.active = {"running": ["r1"]}
        self._call("dry-run")


if __name__ == "__main__":
    unittest.main()
