"""입력 선택 규칙(select_inputs)·게이트 판정·실행 종료 코드."""

import unittest
from unittest.mock import patch

import pandas as pd

from src.bronze_gate import decide
from src.bronze_gate.decide import (
    BLOCK, FRESH, OVERRIDDEN, PASS, STALE, SUBSTITUTED, UNRESOLVED, WARN,
    decide_gate, index_files, run_sort_key, select_inputs,
)

BUCKET = "s3://oliveyoung-crawl-data"
# 실제처럼 서브카테고리 이름에 '/'가 있고 S3 경로는 safe_name(-, _)으로 바뀐 경우
CATS = {
    "스킨케어/에센스/세럼/앰플": "oliveyoung/스킨케어/에센스-세럼-앰플",
    "스킨케어/크림": "oliveyoung/스킨케어/크림",
    "더모 코스메틱/선케어": "oliveyoung/더모_코스메틱/선케어",
}
TARGETS = list(CATS)
ESSENCE, CREAM, SUN = TARGETS


def _manifest(run_id, data, done=None, status="interrupted", new=True, expected=None):
    """data: {키: 상품 수}(part 있음), done: 완료 표시 키(기본 = data 전부)."""
    done = list(data) if done is None else done
    parts, categories = [], {}
    for key, count in data.items():
        main, sub = key.split("/", 1)
        parts.append({"key": f"{CATS[key]}/run_id={run_id}/part_0000.json".replace("s3://", ""),
                      "category": main, "subcategory": sub, "product_count": count})
        categories[key] = {"product_count": count}
        if expected and key in expected:
            categories[key]["expected_urls"] = expected[key]
    m = {"run_id": run_id, "status": status, "parts": parts, "categories": categories,
         "completed_subcategories": done}
    if new:
        m["target_subcategories"] = TARGETS
    return m


def _world(runs):
    """runs: [(run_id, manifest 또는 None, 데이터 있는 키 목록)] → (index, manifests)."""
    files, manifests = [], {}
    for rid, m, keys in runs:
        files += [f"{BUCKET}/{CATS[k]}/run_id={rid}/part_0000.json" for k in keys]
        manifests[rid] = m
    return index_files(files), manifests


def _normal(rid, counts=None, **kw):
    counts = counts or {ESSENCE: 900, CREAM: 775, SUN: 66}
    return (rid, _manifest(rid, counts, **kw), list(counts))


HISTORY = [_normal(r) for r in ("20260919", "20260922", "20260925", "20260928")]


class RunIdTest(unittest.TestCase):
    def test_real_date_required(self):
        self.assertIsNotNone(run_sort_key("20261004"))
        self.assertIsNotNone(run_sort_key("20260311_083614"))
        self.assertIsNone(run_sort_key("20261399"))
        self.assertIsNone(run_sort_key("oliveyoung_crawl_20261004_101010"))

    def test_mixed_formats_sort_by_time(self):
        self.assertLess(run_sort_key("20260311_083614"), run_sort_key("20260925"))
        self.assertLess(run_sort_key("20261004"), run_sort_key("20261004_000001"))


class SelectInputsTest(unittest.TestCase):
    def _plans(self, runs, as_of=None):
        index, manifests = _world(runs)
        plans, frontier = select_inputs(index, manifests, as_of)
        return {p.key: p for p in plans.values()}, frontier

    def test_fresh(self):
        plans, frontier = self._plans(HISTORY + [_normal("20261001")])
        self.assertEqual(frontier, "20261001")
        self.assertTrue(all(p.status == FRESH and p.selected_run == "20261001" for p in plans.values()))

    def test_partial_new_run_substituted(self):
        latest = _normal("20261001", done=[CREAM, SUN])          # 에센스는 part만 있고 미완료
        plans, _ = self._plans(HISTORY + [latest])
        self.assertEqual(plans[ESSENCE].status, SUBSTITUTED)
        self.assertEqual(plans[ESSENCE].selected_run, "20260928")
        self.assertEqual(plans[ESSENCE].skipped, [("20261001", "부분 수집")])
        self.assertEqual(plans[CREAM].status, FRESH)

    def test_shrunk_count_rejected_even_if_completed(self):
        latest = _normal("20261001", counts={ESSENCE: 258, CREAM: 775, SUN: 66})   # 9/25 에센스 유형
        plans, _ = self._plans(HISTORY + [latest])
        self.assertEqual(plans[ESSENCE].status, SUBSTITUTED)
        self.assertIn("50%", plans[ESSENCE].skipped[0][1])

    def test_old_manifest_shrunk_rejected(self):
        # 1단계 이전 manifest: 완료 표시가 오염돼 있어도 상품 수로 부분 수집을 잡는다
        old = [(r, _manifest(r, {ESSENCE: 900, CREAM: 775, SUN: 66}, new=False), TARGETS)
               for r in ("20260904", "20260907", "20260910")]
        bad = ("20260913", _manifest("20260913", {ESSENCE: 258, CREAM: 775, SUN: 66}, new=False), TARGETS)
        plans, _ = self._plans(old + [bad])
        self.assertEqual(plans[ESSENCE].selected_run, "20260910")

    def test_too_few_baseline_skips_count_check(self):
        runs = [_normal("20260928"), _normal("20261001", counts={ESSENCE: 10, CREAM: 775, SUN: 66})]
        plans, _ = self._plans(runs)
        self.assertEqual(plans[ESSENCE].selected_run, "20261001")

    def test_baseline_excludes_candidate_and_zero(self):
        # 기준 표본은 후보보다 오래된 run만, 0건 제외 → 표본 3개(900) 기준 50% 미만이면 탈락
        runs = HISTORY[:3] + [_normal("20261001", counts={ESSENCE: 400, CREAM: 775, SUN: 66})]
        plans, _ = self._plans(runs)
        self.assertEqual(plans[ESSENCE].status, SUBSTITUTED)

    def test_missing_falls_back_as_stale(self):
        latest = _normal("20261001", counts={CREAM: 775, SUN: 66})   # 에센스 통째 누락
        plans, _ = self._plans(HISTORY + [latest])
        self.assertEqual(plans[ESSENCE].status, STALE)
        self.assertEqual(plans[ESSENCE].selected_run, "20260928")
        self.assertEqual(plans[ESSENCE].age_days, 3)

    def test_lookback_counts_data_runs_only(self):
        # 에센스가 7회 연속 통째 누락돼도 그 전 데이터로 채운다(전체가 멈추지 않게)
        runs = [_normal("20260901")] + [
            _normal(r, counts={CREAM: 775, SUN: 66})
            for r in ("20260904", "20260907", "20260910", "20260913", "20260916", "20260919", "20260922")]
        plans, _ = self._plans(runs)
        self.assertEqual(plans[ESSENCE].selected_run, "20260901")
        self.assertEqual(plans[ESSENCE].status, STALE)

    def test_all_rejected_unresolved(self):
        runs = [_normal(r, done=[CREAM, SUN]) for r in
                ("20260913", "20260916", "20260919", "20260922", "20260925", "20260928")]
        plans, _ = self._plans(runs)
        self.assertEqual(plans[ESSENCE].status, UNRESOLVED)
        self.assertIsNone(plans[ESSENCE].selected_run)

    def test_as_of_ignores_later_runs(self):
        plans, frontier = self._plans(HISTORY + [_normal("20261001")], as_of="20260925")
        self.assertEqual(frontier, "20260925")
        self.assertTrue(all(p.selected_run == "20260925" for p in plans.values()))

    def test_future_category_not_a_candidate(self):
        runs = [_normal("20260925", counts={CREAM: 775}), _normal("20261001")]
        index, manifests = _world(runs)
        plans, _ = select_inputs(index, manifests, "20260925")
        by_key = {p.key: p for p in plans.values() if p.key}
        self.assertNotIn(ESSENCE, by_key)          # 기준 시점에 데이터가 없던 카테고리

    def test_in_progress_run_excluded(self):
        running = _normal("20261001", status="in_progress")
        plans, frontier = self._plans(HISTORY + [running])
        self.assertEqual(frontier, "20260928")
        self.assertTrue(all(p.selected_run == "20260928" for p in plans.values()))

    def test_invalid_run_id_excluded(self):
        index, manifests = _world(HISTORY)
        index[CATS[ESSENCE]]["oliveyoung_crawl_20261004_101010"] = ["x"]
        plans, _ = select_inputs(index, manifests)
        self.assertEqual(plans[CATS[ESSENCE]].selected_run, "20260928")


class DecideGateTest(unittest.TestCase):
    def test_pass_and_files(self):
        index, manifests = _world(HISTORY + [_normal("20261001")])
        r = decide_gate(index, manifests)
        self.assertEqual(r.status, PASS)
        self.assertTrue(all("run_id=20261001/" in f for f in r.files))
        self.assertEqual(r.metrics["max_source_age_days"], 0)

    def test_plain_missing_is_not_an_alert(self):
        index, manifests = _world(HISTORY + [_normal("20261001", counts={CREAM: 775, SUN: 66})])
        r = decide_gate(index, manifests)
        self.assertEqual(r.status, PASS)
        self.assertEqual(r.metrics["categories_missing"], 1)
        self.assertEqual(r.metrics["max_source_age_days"], 3)

    def test_substitution_warns(self):
        index, manifests = _world(HISTORY + [_normal("20261001", done=[CREAM, SUN])])
        r = decide_gate(index, manifests)
        self.assertEqual(r.status, WARN)
        self.assertEqual(r.metrics["categories_substituted"], 1)
        self.assertEqual(r.metrics["categories_partial_input"], 1)
        self.assertTrue(any("run_id=20260928/" in f and "에센스" in f for f in r.files))
        self.assertIn("🔁", r.reasons[0])

    def test_unresolved_blocks_and_override(self):
        runs = [_normal(r, done=[CREAM, SUN]) for r in
                ("20260913", "20260916", "20260919", "20260922", "20260925", "20260928")]
        index, manifests = _world(runs)
        self.assertEqual(decide_gate(index, manifests).status, BLOCK)
        r = decide_gate(index, manifests, override=True)
        self.assertEqual(r.status, OVERRIDDEN)
        self.assertTrue(all("run_id=20260928/" in f for f in r.files))   # 기존 max(run_id) 입력

    def test_stale_three_runs_warns_including_partial(self):
        runs = HISTORY[:1] + [
            _normal("20260922", counts={CREAM: 775, SUN: 66}),            # 누락
            _normal("20260925", done=[CREAM, SUN]),                        # 부분 수집
            _normal("20260928", counts={CREAM: 775, SUN: 66}),            # 누락
        ]
        index, manifests = _world(runs)
        r = decide_gate(index, manifests)
        self.assertEqual(r.status, WARN)
        self.assertEqual(r.metrics["categories_stale"], 1)

    def test_invalid_latest_run_warns(self):
        index, manifests = _world(HISTORY)
        index[CATS[ESSENCE]]["oliveyoung_crawl_20261004_101010"] = [f"{BUCKET}/x"]
        r = decide_gate(index, manifests)
        self.assertEqual(r.status, WARN)
        self.assertEqual(r.metrics["invalid_run_ids"], 1)

    def test_low_coverage_warns(self):
        latest = _normal("20261001", expected={ESSENCE: 1200})
        index, manifests = _world(HISTORY + [latest])
        self.assertEqual(decide_gate(index, manifests).metrics["categories_low_coverage"], 1)

    def test_untargeted_category_keeps_legacy(self):
        index, manifests = _world(HISTORY + [_normal("20261001")])
        manifests["20261001"]["target_subcategories"] = [CREAM, SUN]     # 에센스가 대상에서 빠진 경우
        r = decide_gate(index, manifests)
        self.assertEqual(r.metrics["categories_untargeted_input"], 1)
        self.assertEqual(r.status, PASS)

    def test_zero_part_latest_run_counts_as_missing(self):
        # part 0개로 끝난 최신 크롤: 파일은 없고 manifest만 있음 → 이전 데이터는 stale, 연속 누락 집계
        empties = [(r, _manifest(r, {}), []) for r in ("20261001", "20261004", "20261007")]
        index, manifests = _world(HISTORY + empties)
        r = decide_gate(index, manifests)
        self.assertEqual(r.status, WARN)
        self.assertEqual(r.metrics["categories_missing"], 3)
        self.assertEqual(r.metrics["categories_stale"], 3)
        self.assertTrue(all(p.status == STALE and p.selected_run == "20260928" for p in r.plans.values()))


class LoadInputsTest(unittest.TestCase):
    """manifest를 먼저 스냅샷 → 그 뒤 생긴 파일의 run은 manifest 없음/진행 중으로 남아 쓰이지 않는다."""

    def test_snapshot_before_files(self):
        from src.bronze_gate import main as gate
        snap = {"20260928": HISTORY[-1][1], "20261001": _manifest("20261001", {}, status="in_progress")}
        files = [f"{BUCKET}/{CATS[k]}/run_id={rid}/part_0000.json"
                 for rid in ("20260928", "20261001", "20261004") for k in TARGETS]
        with patch.object(gate, "list_manifest_runs", return_value=list(snap)),              patch.object(gate, "load_manifest", side_effect=lambda c, rid: snap[rid]),              patch.object(gate, "list_bronze_files", return_value=files):
            index, manifests = gate.load_inputs(None)
        self.assertIsNone(manifests["20261004"])                 # 스냅샷 뒤에 생긴 run
        self.assertEqual(manifests["20261001"]["status"], "in_progress")
        r = decide_gate(index, manifests)
        self.assertTrue(all("run_id=20260928/" in f for f in r.files))


class RunGateTest(unittest.TestCase):
    """BLOCK이면 99, BLOCK인데 DQ 기록 실패면 1(조용한 보류 방지)."""

    def _run(self, recorded):
        from src.bronze_gate import main as gate
        runs = [_normal(r, done=[CREAM, SUN]) for r in
                ("20260913", "20260916", "20260919", "20260922", "20260925", "20260928")]
        with patch.object(gate, "_s3"), \
             patch.object(gate, "load_inputs", return_value=_world(runs)), \
             patch.object(gate, "_record", return_value=recorded), \
             patch.dict("os.environ", {"GATE_MODE": ""}):
            with self.assertRaises(SystemExit) as ctx:
                gate.run_bronze_gate(lambda rid: "2026-10-01")
        return ctx.exception.code

    def test_block_exits_99(self):
        self.assertEqual(self._run(True), 99)

    def test_block_without_dq_record_exits_1(self):
        self.assertEqual(self._run(False), 1)

    def test_attach_source_run_id(self):
        from src.bronze_gate.main import attach_source_run_id
        df = pd.DataFrame({"name": ["a", "b"], "filename": [
            f"{BUCKET}/oliveyoung/스킨케어/크림/run_id=20261001/part_0000.json",
            f"{BUCKET}/oliveyoung/스킨케어/크림/run_id=20260928/part_0003.json"]})
        out = attach_source_run_id(df)
        self.assertEqual(out["source_run_id"].tolist(), ["20261001", "20260928"])
        self.assertNotIn("filename", out.columns)


if __name__ == "__main__":
    unittest.main()
