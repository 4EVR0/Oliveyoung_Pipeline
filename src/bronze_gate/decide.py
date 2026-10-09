"""전처리 입력 선택·품질 게이트 판정 — 순수 함수(S3·Iceberg 의존 없음, 단위 테스트 대상).

정상 전처리와 백필이 같은 규칙 `select_inputs(as_of)`로 카테고리마다 입력 run을 고른다.
"run `as_of` 시점에 이 규칙으로 정상 전처리가 돌았다면 골랐을 입력"을 만든다.
- 진행 중 run·비정상 run_id는 후보에서 제외
- 부분 수집(새 run의 완료 표시 없음)·상품 수가 평소의 50% 미만이면 버리고 이전 run 사용
- 통째 누락은 이전 run 사용(지금과 같음)
- 데이터 있는 run 6개 안에 쓸 run이 없으면 대체 불가
설계: docs/input_selection_design_review.md §10
"""

import re
import statistics
from dataclasses import dataclass, field
from datetime import date, datetime

PASS, WARN, BLOCK, OVERRIDDEN = 0, 1, 2, 3

LOOKBACK = 6            # 카테고리마다 볼 "데이터 있는 run" 수(통째 누락 run은 세지 않음)
SHRINK_RATIO = 0.5      # 상품 수가 평소(중앙값)의 이 비율 미만이면 버림
BASELINE_RUNS = 6       # 평소 = 후보보다 오래된 데이터 run 최대 이만큼의 중앙값
BASELINE_MIN = 3        # 비교 대상이 이보다 적으면 상품 수 검사 생략
STALE_RUNS = 3          # 이 횟수 이상 연속으로 못 쓰면 경고(데이터 나이 SLO 3크롤 ≈ 9일)
# 최신 완료 크롤에서 이 개수 이상 누락이면 경고(19개 중 대부분 실패 = 크롤 전멸 수준).
# 5였을 때 새 코드 첫 2회(누락 11·9)가 모두 경고 → 크롤러를 못 고치는 동안 매번 울려 알림 피로.
# 통째 누락은 이전 run으로 채워져 손상이 없고, 데이터가 실제로 묵는 건 STALE_RUNS가 잡는다.
MISSING_WARN = 15
COVERAGE_WARN = 0.9     # 선택된 run의 수집률(product_count/expected_urls) 하한
MAX_REASONS = 10        # 리포트에 붙일 사유 줄 상한

FRESH, STALE, SUBSTITUTED, UNRESOLVED = "fresh", "stale", "substituted", "unresolved"

_RUN_ID_RE = re.compile(r"^(\d{8})(?:_(\d{6}))?$")
_FILE_RE = re.compile(r"(?:s3://[^/]+/)?(?P<prefix>[^/]+/[^/]+/[^/]+)/run_id=(?P<run_id>[^/]+)/[^/]+$")


@dataclass
class CategoryPlan:
    prefix: str                         # S3 경로 'oliveyoung/main/sub'
    key: str | None = None              # manifest 키 'main/sub'
    selected_run: str | None = None
    status: str = UNRESOLVED
    skipped: list[tuple[str, str]] = field(default_factory=list)   # (run_id, 사유)
    age_days: int | None = None
    targeted: bool = True


@dataclass
class GateResult:
    status: int
    metrics: dict = field(default_factory=dict)
    reasons: list[str] = field(default_factory=list)
    files: list[str] = field(default_factory=list)          # 실제로 로드할 파일
    plans: dict[str, CategoryPlan] = field(default_factory=dict)


# ── run_id·경로 ────────────────────────────────────────────────────────────

def run_sort_key(run_id: str) -> tuple | None:
    """정상 run_id면 (날짜, 시각) 정렬 키, 아니면 None. 실제 날짜로 파싱되는지까지 본다."""
    m = _RUN_ID_RE.match(run_id or "")
    if not m:
        return None
    try:
        d = datetime.strptime(m[1], "%Y%m%d").date()
        t = datetime.strptime(m[2], "%H%M%S").time() if m[2] else None
    except ValueError:
        return None
    return (d, t.isoformat() if t else "")


def run_date(run_id: str) -> date | None:
    k = run_sort_key(run_id)
    return k[0] if k else None


def parse_file(path: str) -> tuple[str, str] | None:
    """bronze 파일 경로 → (카테고리 경로 'oliveyoung/main/sub', run_id)."""
    m = _FILE_RE.match(path)
    return (m["prefix"], m["run_id"]) if m else None


def index_files(files: list[str]) -> dict[str, dict[str, list[str]]]:
    """bronze 파일 목록 → {카테고리 경로: {run_id: [파일]}}."""
    idx: dict[str, dict[str, list[str]]] = {}
    for f in files:
        parsed = parse_file(f)
        if parsed:
            prefix, rid = parsed
            idx.setdefault(prefix, {}).setdefault(rid, []).append(f)
    return idx


def legacy_select(index: dict[str, dict[str, list[str]]]) -> dict[str, str]:
    """기존 방식(get_latest_bronze_files): 카테고리마다 run_id 문자열 최댓값."""
    return {prefix: max(runs) for prefix, runs in index.items() if runs}


# ── manifest ───────────────────────────────────────────────────────────────

def _cat_key(part: dict) -> str:
    return f"{part.get('category')}/{part.get('subcategory')}"


def category_key(manifest: dict, prefix: str, run_id: str) -> str | None:
    """S3 경로(safe_name)와 manifest 키(main/sub)를 part key 접두사로 매칭."""
    needle = f"{prefix}/run_id={run_id}/"
    for part in manifest.get("parts", []):
        if part.get("key", "").startswith(needle):
            return _cat_key(part)
    return None


def product_count(manifest: dict, key: str) -> int:
    entry = manifest.get("categories", {}).get(key)
    if entry and "product_count" in entry:
        return int(entry["product_count"])
    return sum(int(p.get("product_count", 0)) for p in manifest.get("parts", []) if _cat_key(p) == key)


def is_new_manifest(manifest: dict) -> bool:
    """1단계 이후 manifest(완료 표시가 정확)인지 — target_subcategories 기록 여부."""
    return bool(manifest.get("target_subcategories"))


def category_presence(manifest: dict, targets: list[str] | None = None) -> dict[str, str]:
    """카테고리별 completed / partial / missing. targets가 없으면 manifest에 등장한 카테고리."""
    completed = set(manifest.get("completed_subcategories", []))
    with_parts = {_cat_key(p) for p in manifest.get("parts", [])}
    keys = targets if targets is not None else sorted(with_parts | set(manifest.get("categories", {})))
    return {k: "completed" if k in completed else "partial" if k in with_parts else "missing" for k in keys}


# ── 선택 규칙 ──────────────────────────────────────────────────────────────

def eligible_runs(runs, manifests: dict, as_of: str | None) -> list[str]:
    """정상 형식·as_of 이하·진행 중 아님인 run을 최신순으로."""
    limit = run_sort_key(as_of) if as_of else None
    out = []
    for rid in runs:
        k = run_sort_key(rid)
        if k is None or (limit is not None and k > limit):
            continue
        m = manifests.get(rid)
        if m is not None and m.get("status") == "in_progress":
            continue
        out.append(rid)
    return sorted(out, key=run_sort_key, reverse=True)


def check_candidate(prefix: str, run_id: str, older: list[str], manifests: dict) -> tuple[str | None, str | None]:
    """후보 run 검사 → (manifest 키, 탈락 사유 또는 None). older는 후보보다 오래된 데이터 run(최신순)."""
    m = manifests.get(run_id)
    if m is None:
        return None, "manifest 없음"
    key = category_key(m, prefix, run_id)
    if key is None:
        return None, "manifest에 없는 파일"
    if is_new_manifest(m) and key not in set(m.get("completed_subcategories", [])):
        return key, "부분 수집"
    baseline = []
    for rid in older:
        om = manifests.get(rid)
        ok = om and category_key(om, prefix, rid)
        if ok:
            c = product_count(om, ok)
            if c > 0:
                baseline.append(c)
        if len(baseline) >= BASELINE_RUNS:
            break
    if len(baseline) >= BASELINE_MIN:
        median = statistics.median(baseline)
        count = product_count(m, key)
        if count < SHRINK_RATIO * median:
            return key, f"상품 수 {count} < 평소 {median:g}의 50%"
    return key, None


def select_inputs(
    index: dict[str, dict[str, list[str]]],
    manifests: dict,
    as_of: str | None = None,
    targets: set[str] | None = None,
) -> tuple[dict[str, CategoryPlan], str | None]:
    """카테고리마다 입력 run을 고른다 → ({경로: CategoryPlan}, 기준 run(frontier)).

    targets(manifest 키)가 주어지면 그 밖의 카테고리는 targeted=False(기존 동작 유지용).
    """
    frontier_list = eligible_runs(known_runs(index, manifests), manifests, as_of)
    frontier = frontier_list[0] if frontier_list else None
    plans: dict[str, CategoryPlan] = {}
    for prefix, runs in index.items():
        data_runs = eligible_runs(runs.keys(), manifests, as_of)
        plan = CategoryPlan(prefix=prefix)
        for i, rid in enumerate(data_runs[:LOOKBACK]):
            key, reason = check_candidate(prefix, rid, data_runs[i + 1:], manifests)
            plan.key = plan.key or key
            if reason:
                plan.skipped.append((rid, reason))
                continue
            plan.selected_run = rid
            break
        if plan.selected_run:
            plan.status = SUBSTITUTED if plan.skipped else (FRESH if plan.selected_run == frontier else STALE)
            fd, sd = run_date(frontier) if frontier else None, run_date(plan.selected_run)
            plan.age_days = (fd - sd).days if fd and sd else None
        if targets is not None and plan.key is not None:
            plan.targeted = plan.key in targets
        plans[prefix] = plan
    return plans, frontier


def unusable_streak(prefix: str, index: dict, manifests: dict, frontier_runs: list[str]) -> int:
    """frontier run들(최신순)에서 이 카테고리를 연속으로 못 쓴 횟수(누락·부분 수집·50% 미달 포함)."""
    runs = index.get(prefix, {})
    data_runs = eligible_runs(runs.keys(), manifests, None)
    streak = 0
    for rid in frontier_runs:
        if rid in runs:
            older = [r for r in data_runs if run_sort_key(r) < run_sort_key(rid)]
            _, reason = check_candidate(prefix, rid, older, manifests)
            if reason is None:
                break
        streak += 1
    return streak


def known_runs(index: dict, manifests: dict) -> set[str]:
    """기준 run(frontier) 후보: 파일이 있는 run ∪ manifest가 있는 run(part 0개인 크롤도 놓치지 않게)."""
    return {rid for runs in index.values() for rid in runs} | {rid for rid, m in manifests.items() if m}


def files_for(plans: dict[str, CategoryPlan], index: dict) -> list[str]:
    return sorted(f for p in plans.values() if p.selected_run for f in index[p.prefix][p.selected_run])


# ── 게이트 판정(정상 전처리) ─────────────────────────────────────────────────

def decide_gate(index: dict, manifests: dict, override: bool = False) -> GateResult:
    """정상 전처리용: 최신 기준 선택 + 판정. override면 선택 규칙을 끄고 기존 max(run_id) 입력."""
    frontier_runs = eligible_runs(known_runs(index, manifests), manifests, None)
    latest = manifests.get(frontier_runs[0]) if frontier_runs else None

    targets = None
    if latest:
        targets = set(latest.get("target_subcategories") or []) or None
    plans, frontier = select_inputs(index, manifests, None, targets)
    legacy = legacy_select(index)

    # 대상 밖 카테고리는 기존 동작(max(run_id) 그대로) — 폐지 카테고리 옛 데이터 등
    for prefix, plan in plans.items():
        if not plan.targeted:
            plan.selected_run, plan.status, plan.skipped = legacy[prefix], FRESH, []

    targeted = [p for p in plans.values() if p.targeted]
    substituted = [p for p in targeted if p.status == SUBSTITUTED]
    unresolved = [p for p in targeted if p.status == UNRESOLVED]
    partial = [p for p in targeted if any(r == "부분 수집" for _, r in p.skipped)]
    unverified = [p for p in targeted if any(r.startswith("manifest") for _, r in p.skipped)]
    invalid = sorted({legacy[p.prefix] for p in targeted if run_sort_key(legacy[p.prefix]) is None})
    stale = [(p, n) for p in targeted
             if (n := unusable_streak(p.prefix, index, manifests, frontier_runs)) >= STALE_RUNS]
    low_cov = []
    for p in targeted:
        m = manifests.get(p.selected_run) if p.selected_run else None
        entry = (m or {}).get("categories", {}).get(p.key or "", {})
        expected = entry.get("expected_urls")
        if expected and entry.get("product_count", 0) / expected < COVERAGE_WARN:
            low_cov.append(p)
    missing = []
    if latest:
        keys = sorted(targets or {_cat_key(x) for x in latest.get("parts", [])})
        missing = [k for k, s in category_presence(latest, keys).items() if s == "missing"]

    if override and (unresolved or substituted or invalid):
        status = OVERRIDDEN          # 규칙이 입력을 바꿨을 상황인데 운영자가 기존 입력으로 진행
    elif unresolved:
        status = BLOCK
    elif substituted or invalid or stale or len(missing) >= MISSING_WARN or low_cov:
        status = WARN
    else:
        status = PASS

    def name(p):
        return p.key or p.prefix.split("/", 1)[-1]

    reasons = (
        [f"🔁 이전 run으로 대체: {name(p)} ({', '.join(f'{r} {why}' for r, why in p.skipped)} → {p.selected_run})"
         for p in substituted]
        + [f"⛔ 대체할 run 없음: {name(p)} ({'; '.join(f'{r} {why}' for r, why in p.skipped)})" for p in unresolved]
        + [f"🚫 비정상 run_id 제외: {r}" for r in invalid]
        + [f"⏳ 연속 누락: {name(p)} ({n}회 연속)" for p, n in stale]
        + ([f"📭 최신 크롤 누락 {len(missing)}개: {', '.join(missing)}"] if missing else [])
        + [f"📉 수집률 미달: {name(p)} (run {p.selected_run})" for p in low_cov]
    )
    ages = [p.age_days for p in targeted if p.age_days is not None]
    metrics = dict(
        gate_status=status,
        categories_substituted=len(substituted),
        categories_unresolved=len(unresolved),
        categories_partial_input=len(partial),
        invalid_run_ids=len(invalid),
        categories_missing=len(missing),
        categories_stale=len(stale),
        categories_low_coverage=len(low_cov),
        categories_unverified=len(unverified),
        categories_untargeted_input=len(plans) - len(targeted),
        max_source_age_days=max(ages) if ages else 0,
    )
    if override:
        files = sorted(f for prefix, rid in legacy.items() for f in index[prefix][rid])
    else:
        files = files_for(plans, index)
    return GateResult(status=status, metrics=metrics, reasons=reasons, files=files, plans=plans)
