"""입력 품질 게이트 — 판정(순수 함수)과 실행 시 종료 코드."""

import unittest
from unittest.mock import patch

from src.bronze_gate import decide
from src.bronze_gate.decide import BLOCK, OVERRIDDEN, PASS, WARN, decide_gate

BUCKET = "s3://oliveyoung-crawl-data"
# 실제처럼 서브카테고리 이름에 '/'가 있고 S3 경로는 safe_name(-, _)으로 바뀐 경우
CATS = {
    "스킨케어/에센스/세럼/앰플": "oliveyoung/스킨케어/에센스-세럼-앰플",
    "스킨케어/크림": "oliveyoung/스킨케어/크림",
    "더모 코스메틱/선케어": "oliveyoung/더모_코스메틱/선케어",
}
TARGETS = list(CATS)


def _manifest(run_id, done=(), partial=(), status="interrupted", expected=None, counts=None, targets=TARGETS):
    """done: 완료 카테고리, partial: part만 있고 미완료 카테고리."""
    parts, categories = [], {}
    for key in list(done) + list(partial):
        main, sub = key.split("/", 1)
        parts.append({"key": f"{CATS[key]}/run_id={run_id}/part_0000.json", "category": main, "subcategory": sub})
        entry = {"product_count": (counts or {}).get(key, 20)}
        if expected and key in expected:
            entry["expected_urls"] = expected[key]
        categories[key] = entry
    m = {"run_id": run_id, "status": status, "parts": parts, "categories": categories,
         "completed_subcategories": list(done)}
    if targets is not None:
        m["target_subcategories"] = targets
    return m


def _files(*pairs):
    return [f"{BUCKET}/{CATS[key]}/run_id={rid}/part_0000.json" for key, rid in pairs]


ALL = [(k, "20261004") for k in TARGETS]


class DecideGateTest(unittest.TestCase):
    def test_pass(self):
        m = _manifest("20261004", done=TARGETS)
        r = decide_gate(_files(*ALL), {"20261004": m}, [m])
        self.assertEqual(r.status, PASS)
        self.assertEqual(r.metrics["categories_partial_input"], 0)

    def test_partial_input_blocks(self):
        # 9/25 에센스: part는 있으나 미완료
        m = _manifest("20261004", done=TARGETS[1:], partial=TARGETS[:1])
        r = decide_gate(_files(*ALL), {"20261004": m}, [m])
        self.assertEqual(r.status, BLOCK)
        self.assertEqual(r.metrics["categories_partial_input"], 1)
        self.assertIn("스킨케어/에센스/세럼/앰플", r.reasons[0])

    def test_in_progress_crawl_blocks(self):
        # 크롤 진행 중 수동 실행(9/29): 수집 중 카테고리는 미완료
        running = _manifest("20261004", done=TARGETS[:1], partial=TARGETS[1:2], status="in_progress")
        prev = _manifest("20261001", done=TARGETS)
        files = _files((TARGETS[0], "20261004"), (TARGETS[1], "20261004"), (TARGETS[2], "20261001"))
        r = decide_gate(files, {"20261004": running, "20261001": prev}, [prev])
        self.assertEqual(r.status, BLOCK)

    def test_previous_partial_still_selected_blocks(self):
        # 이전 run 부분 수집 → 이번 run 통째 누락 → max(run_id)가 이전 부분 데이터를 고름
        prev = _manifest("20261001", done=TARGETS[1:], partial=TARGETS[:1])
        cur = _manifest("20261004", done=TARGETS[1:])
        files = _files((TARGETS[0], "20261001"), (TARGETS[1], "20261004"), (TARGETS[2], "20261004"))
        r = decide_gate(files, {"20261001": prev, "20261004": cur}, [cur, prev])
        self.assertEqual(r.status, BLOCK)

    def test_invalid_run_id_blocks(self):
        m = _manifest("20261004", done=TARGETS)
        files = _files(*ALL[1:]) + [f"{BUCKET}/{CATS[TARGETS[0]]}/run_id=oliveyoung_crawl_20261004_101010/part_0000.json"]
        r = decide_gate(files, {"20261004": m}, [m])
        self.assertEqual(r.status, BLOCK)
        self.assertEqual(r.metrics["invalid_run_ids"], 1)

    def test_override_turns_block_into_overridden(self):
        m = _manifest("20261004", done=TARGETS[1:], partial=TARGETS[:1])
        r = decide_gate(_files(*ALL), {"20261004": m}, [m], override=True)
        self.assertEqual(r.status, OVERRIDDEN)

    def test_override_does_not_change_pass(self):
        m = _manifest("20261004", done=TARGETS)
        r = decide_gate(_files(*ALL), {"20261004": m}, [m], override=True)
        self.assertEqual(r.status, PASS)

    def test_stale_three_runs_warns(self):
        runs = [_manifest(rid, done=TARGETS[1:]) for rid in ("20261004", "20261001", "20260928")]
        files = [f for f in _files(*ALL[1:])] + _files((TARGETS[0], "20260925"))
        old = _manifest("20260925", done=TARGETS)
        r = decide_gate(files, {"20261004": runs[0], "20260925": old}, runs)
        self.assertEqual(r.status, WARN)
        self.assertEqual(r.metrics["categories_stale"], 1)

    def test_two_consecutive_misses_pass(self):
        runs = [_manifest(rid, done=TARGETS[1:]) for rid in ("20261004", "20261001")] + [_manifest("20260928", done=TARGETS)]
        files = _files(*ALL[1:]) + _files((TARGETS[0], "20260928"))
        r = decide_gate(files, {"20261004": runs[0], "20260928": runs[2]}, runs)
        self.assertEqual(r.status, PASS)
        self.assertEqual(r.metrics["categories_missing"], 1)

    def test_many_missing_warns(self):
        with patch.object(decide, "MISSING_WARN", 2):
            cur = _manifest("20261004", done=TARGETS[:1])
            prev = _manifest("20261001", done=TARGETS)
            files = _files((TARGETS[0], "20261004"), (TARGETS[1], "20261001"), (TARGETS[2], "20261001"))
            r = decide_gate(files, {"20261004": cur, "20261001": prev}, [cur, prev])
        self.assertEqual(r.status, WARN)
        self.assertEqual(r.metrics["categories_missing"], 2)

    def test_some_missing_without_stale_passes(self):
        # 10/4 형태: 누락은 있지만 연속 누락이 없으면 통과(누락 수는 지표로만 남음)
        cur = _manifest("20261004", done=TARGETS[:1])
        prev = _manifest("20261001", done=TARGETS)
        files = _files((TARGETS[0], "20261004"), (TARGETS[1], "20261001"), (TARGETS[2], "20261001"))
        r = decide_gate(files, {"20261004": cur, "20261001": prev}, [cur, prev])
        self.assertEqual(r.status, PASS)
        self.assertEqual(r.metrics["categories_missing"], 2)

    def test_low_coverage_warns(self):
        m = _manifest("20261004", done=TARGETS, expected={TARGETS[0]: 100}, counts={TARGETS[0]: 50})
        r = decide_gate(_files(*ALL), {"20261004": m}, [m])
        self.assertEqual(r.status, WARN)
        self.assertEqual(r.metrics["categories_low_coverage"], 1)

    def test_missing_manifest_is_unverified_warn(self):
        m = _manifest("20261004", done=TARGETS)
        files = _files(*ALL[1:]) + _files((TARGETS[0], "20260301"))
        r = decide_gate(files, {"20261004": m, "20260301": None}, [m])
        self.assertEqual(r.status, WARN)
        self.assertEqual(r.metrics["categories_unverified"], 1)

    def test_file_unknown_to_manifest_is_unverified(self):
        m = _manifest("20261004", done=TARGETS[1:])   # 에센스 part가 manifest에 없음(rogue)
        r = decide_gate(_files(*ALL), {"20261004": m}, [m])
        self.assertEqual(r.metrics["categories_unverified"], 1)

    def test_no_recent_manifests_skips_missing_checks(self):
        m = _manifest("20261004", done=TARGETS)
        r = decide_gate(_files(*ALL), {"20261004": m}, [])
        self.assertEqual(r.status, PASS)
        self.assertEqual(r.metrics["categories_missing"], 0)

    def test_legacy_manifest_without_targets_or_expected(self):
        m = _manifest("20261004", done=TARGETS, targets=None)
        r = decide_gate(_files(*ALL), {"20261004": m}, [m])
        self.assertEqual(r.status, PASS)
        self.assertEqual(r.metrics["categories_untargeted_input"], 0)

    def test_category_presence(self):
        m = _manifest("20261004", done=TARGETS[:1], partial=TARGETS[1:2])
        self.assertEqual(
            decide.category_presence(m, TARGETS),
            {TARGETS[0]: "completed", TARGETS[1]: "partial", TARGETS[2]: "missing"},
        )


class RunGateExitTest(unittest.TestCase):
    """BLOCK이면 99, BLOCK인데 DQ 기록 실패면 1(조용한 보류 방지)."""

    def _run(self, recorded):
        from src.bronze_gate import main as gate
        m = _manifest("20261004", done=TARGETS[1:], partial=TARGETS[:1])
        with patch.object(gate, "_s3"), \
             patch.object(gate, "_load_manifest", return_value=m), \
             patch.object(gate, "_recent_manifests", return_value=[m]), \
             patch.object(gate, "_record", return_value=recorded), \
             patch.dict("os.environ", {"GATE_MODE": ""}):
            with self.assertRaises(SystemExit) as ctx:
                gate.run_bronze_gate(_files(*ALL), "2026-10-04")
        return ctx.exception.code

    def test_block_exits_99(self):
        self.assertEqual(self._run(True), 99)

    def test_block_without_dq_record_exits_1(self):
        self.assertEqual(self._run(False), 1)


if __name__ == "__main__":
    unittest.main()
