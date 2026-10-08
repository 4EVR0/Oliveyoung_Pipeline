"""전처리 입력 품질 게이트 판정 — 순수 함수(S3·Iceberg 의존 없음, 단위 테스트 대상).

bronze_to_silver가 실제로 로드할 파일(카테고리별 max(run_id))과 크롤 manifest로
"하류를 망가뜨릴 입력"인지 판정한다.
- 부분 수집 카테고리가 입력에 섞이면 CDC REMOVED → Neo4j 삭제 경로라 보류(BLOCK)
- 통째 누락은 이전 run으로 채워져 삭제가 없으므로 오래 지속될 때만 경고(WARN)
"""

import re
from dataclasses import dataclass, field

PASS, WARN, BLOCK, OVERRIDDEN = 0, 1, 2, 3

STALE_RUNS = 3          # 이 횟수 이상 연속 누락이면 경고(약 9일 이상 노후)
# 최신 완료 크롤에서 이 개수 이상 누락이면 경고(19개 중 대부분 실패 = 크롤 전멸 수준).
# 5였을 때 새 코드 첫 2회(누락 11·9)가 모두 경고 → 크롤러를 못 고치는 동안 매번 울려 알림 피로.
# 통째 누락은 이전 run으로 채워져 손상이 없고, 데이터가 실제로 묵는 건 STALE_RUNS가 잡는다.
MISSING_WARN = 15
COVERAGE_WARN = 0.9     # 완료 카테고리 수집률(product_count/expected_urls) 하한
RUN_ID_PATTERN = re.compile(r"^\d{8}(_\d{6})?$")   # 크롤 run_id(ds_nodash, 옛 YYYYMMDD_HHMMSS)
MAX_REASONS = 10        # 리포트에 붙일 사유 줄 상한

_INPUT_RE = re.compile(r"(?:s3://[^/]+/)?(?P<prefix>.+/[^/]+/[^/]+)/run_id=(?P<run_id>[^/]+)/[^/]+$")


@dataclass
class GateResult:
    status: int
    metrics: dict = field(default_factory=dict)
    reasons: list[str] = field(default_factory=list)


def parse_input(path: str) -> tuple[str, str] | None:
    """bronze 파일 경로 → (카테고리 경로 접두사 'oliveyoung/main/sub', run_id)."""
    m = _INPUT_RE.match(path)
    return (m["prefix"], m["run_id"]) if m else None


def group_inputs(files: list[str]) -> dict[tuple[str, str], int]:
    """입력 파일을 (카테고리 경로, run_id)별 파일 수로 묶는다. 형식이 다른 경로는 무시."""
    groups: dict[tuple[str, str], int] = {}
    for f in files:
        parsed = parse_input(f)
        if parsed:
            groups[parsed] = groups.get(parsed, 0) + 1
    return groups


def _cat_key(part: dict) -> str:
    return f"{part.get('category')}/{part.get('subcategory')}"


def category_presence(manifest: dict, targets: list[str] | None = None) -> dict[str, str]:
    """카테고리별 completed / partial / missing. targets가 없으면 manifest에 등장한 카테고리."""
    completed = set(manifest.get("completed_subcategories", []))
    with_parts = {_cat_key(p) for p in manifest.get("parts", [])}
    keys = targets if targets is not None else sorted(with_parts | set(manifest.get("categories", {})))
    status = {}
    for key in keys:
        if key in completed:
            status[key] = "completed"
        elif key in with_parts:
            status[key] = "partial"
        else:
            status[key] = "missing"
    return status


def targets_of(manifest: dict, fallback: list[str]) -> list[str]:
    """manifest의 대상 목록(target_subcategories), 구버전이면 fallback."""
    targets = manifest.get("target_subcategories")
    return list(targets) if targets else list(fallback)


def _input_category(manifest: dict, prefix: str, run_id: str) -> str | None:
    """입력 경로 접두사가 manifest의 어느 카테고리인지(part key로 매칭). 모르면 None."""
    needle = f"{prefix}/run_id={run_id}/"
    for part in manifest.get("parts", []):
        if part.get("key", "").startswith(needle):
            return _cat_key(part)
    return None


def _coverage_low(manifest: dict, key: str) -> bool:
    entry = manifest.get("categories", {}).get(key, {})
    expected = entry.get("expected_urls")
    if not expected:   # 기록 없음(구버전) 또는 0(빈 카테고리) → 판정 제외
        return False
    return entry.get("product_count", 0) / expected < COVERAGE_WARN


def decide_gate(
    files: list[str],
    manifests: dict[str, dict | None],
    recent: list[dict],
    override: bool = False,
) -> GateResult:
    """입력 파일·해당 run manifest·최근 완료 크롤 manifest(최신순)로 판정한다.

    manifests: 입력에 등장한 run_id → manifest(없으면 None)
    recent: in_progress가 아닌 최근 크롤 manifest, 최신순(연속 누락·누락 계산용)
    """
    partial, invalid, unverified, low_cov, untargeted = [], [], [], [], []
    latest_targets: set[str] | None = None
    if recent:
        fallback = sorted({_cat_key(p) for m in recent for p in m.get("parts", [])})
        latest_targets = set(targets_of(recent[0], fallback))

    for (prefix, run_id), _n in sorted(group_inputs(files).items()):
        label = f"{prefix.split('/', 1)[-1]} (run {run_id})"
        if not RUN_ID_PATTERN.match(run_id):
            invalid.append(label)
            continue
        manifest = manifests.get(run_id)
        if manifest is None:
            unverified.append(f"{label} — manifest 없음")
            continue
        key = _input_category(manifest, prefix, run_id)
        if key is None:
            unverified.append(f"{label} — manifest에 없는 파일")
            continue
        state = category_presence(manifest, [key])[key]
        if state != "completed":
            partial.append(f"{key} (run {run_id})")
        elif _coverage_low(manifest, key):
            low_cov.append(f"{key} (run {run_id})")
        if latest_targets is not None and key not in latest_targets:
            untargeted.append(key)

    missing, stale = [], []
    if recent:
        targets = sorted(latest_targets or [])
        latest = category_presence(recent[0], targets)
        missing = [k for k, s in latest.items() if s == "missing"]
        for key in targets:
            streak = 0
            for m in recent:
                if category_presence(m, [key])[key] != "missing":
                    break
                streak += 1
            if streak >= STALE_RUNS:
                stale.append(f"{key} ({streak}회 연속)")

    if partial or invalid:
        status = OVERRIDDEN if override else BLOCK
    elif stale or len(missing) >= MISSING_WARN or low_cov or unverified:
        status = WARN
    else:
        status = PASS

    reasons = (
        [f"🧩 부분 수집 입력: {r}" for r in partial]
        + [f"🚫 비정상 run_id: {r}" for r in invalid]
        + [f"⏳ 연속 누락: {r}" for r in stale]
        + ([f"📭 최신 크롤 누락 {len(missing)}개: {', '.join(missing)}"] if missing else [])
        + [f"📉 수집률 미달: {r}" for r in low_cov]
        + [f"❓ 판별 불가: {r}" for r in unverified]
    )
    metrics = dict(
        gate_status=status,
        categories_partial_input=len(partial),
        invalid_run_ids=len(invalid),
        categories_missing=len(missing),
        categories_stale=len(stale),
        categories_low_coverage=len(low_cov),
        categories_unverified=len(unverified),
        categories_untargeted_input=len(untargeted),
    )
    return GateResult(status=status, metrics=metrics, reasons=reasons)
