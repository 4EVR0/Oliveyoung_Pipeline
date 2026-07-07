"""
올리브영 크롤링 데이터 전처리 파이프라인 모듈

담당:
    - 정규표현식 상수 정의
    - 카테고리 추론 (main_category + sub_category + 제품명 → category)
    - 성분 문자열 정제 (무효 문구 제거, 번들 탐지, 괄호 제거 등)
    - 제품명 클리닝
    - 중복 제거
    - 성분 매칭 및 silver/error 라우팅
"""

import re
import uuid

import pandas as pd
import ahocorasick

from src.bronze_to_silver.ac_builder import search_with_ac
from models.pipeline_models import ErrorRecord
from models.batch_metadata import BatchMetadata, add_batch_metadata, create_batch_metadata


# ==========================================
# 정규표현식 상수
# ==========================================

# 무효 데이터 및 안내 문구 제거
REGEX_PREFIX_ALL = re.compile(r'^전성분(?:명)?\s*:?\s*')
REGEX_LEGEND = re.compile(
    r'(?:(?:※|\*|\+)\s*(?:표시\s*:|자연\s*유래|식물\s*유래|유기농|ILN\d+)|'
    r'(?:※|\*|\+)?\s*(?:제공된\s*성분은|ILN\d+\s*성분\s*목록은))'
    r'.*?(?=\[|<|(?:^|\s)\d{1,2}\)\s*[가-힣a-zA-Z]|$)'
)
REGEX_ILN = re.compile(r'(?i)[<\[]?ILN\d+[>\]]?')
REGEX_LEGEND_WITH_BRACKET = re.compile(r'\([^)]*[*※+][^)]*\)')
REGEX_SYMBOLS = re.compile(r'[*※+]')

# 무첨가성분 안내 문구 제거 (말미의 "무첨가 성분: ..." 전체 삭제)
# 예) "토코페롤, 소듐파이테이트 * 무첨가 성분: 페녹시에탄올, PEG ..."
REGEX_NO_INGREDIENT = re.compile(
    r'[*※]?\s*무첨가\s*성분\s*:.*$',
    re.IGNORECASE | re.DOTALL
)

# 이종 결합 번들 탐지
REGEX_BUNDLE = re.compile(
    r'(?:<[^>]+>|'                                                          # <HTML태그> 형식
    r'■\s*[^:■]+:\s*|'                                                      # ■ 섹션명: 형식
    r'[가-힣a-zA-Z0-9]+\s*-\s*\[전성분|'                                     # 한글명-[전성분 형식
    r'\[(?![Ii][Ll][Nn])[^\]]+\]|'                                         # [대괄호] (ILN 제외, 본품/구성품 포함)
    r'(?:^|\s)\d{1,2}\)\s*[가-힣a-zA-Z]+|'                                  # 숫자) 형식
    r'[가-힣a-zA-Z0-9\s]+(?:볼|앰플|세럼|크림|토너|로션)\s*[):]\s*(?=[가-힣])'  # 제품유형 구분자
    r')'
)
REGEX_BONPUM_MULTI = re.compile(r'\[본품\]')
REGEX_PRODUCT_OPTION_BUNDLE = re.compile(
    r'\d+\s*[종가지]\s*(?:(?:택|중|중\s*\d+)\s*(?:1|일|택|선택))?'
    r'|\b(?:택|선택)\s*\d+\b'
)

# 성분명 농도/이명 괄호 제거
REGEX_BRACKET = re.compile(
    r'\([^)]*('
    r'ppm|PPM|ppb|PPB|%|mg|mG|G|g|ml|mL|IU|'
    r'NON\s*GMO|CI\s*\d+|BHA|AHA|'
    r'산\s*|전성분|전성분명|성분|유래|추출물|아쿠아'
    r'|[\d.]+\s*(?:%|ppm|ppb|mg|ml|g)?$'
    r').*?\)'
)

# 제품명 전처리
REGEX_PRODUCT_BRACKET = re.compile(r'\[.*?\]|\(.*?\)')
REGEX_PRODUCT_VOLUME_ANCHOR = re.compile(
    r'('
    r'\d+(?:\.\d+)?\s*[+xX*]\s*\d+.*|'                                           # 3+1, 50+30ml 등 수량/용량 결합
    r'\d+(?:\.\d+)?\s*(?:ml|㎖|l|ℓ|g|매|호|ea|개입|입|p|종)(?=[^a-zA-Z가-힣]|$).*|'  # 150ml, 100g 등
    r'\b\d+(?:\.\d+)?$'                                                           # 문장 끝에 혼자 남은 숫자
    r').*',
    re.IGNORECASE
)
REGEX_PRODUCT_MARKETING = re.compile(
    r'기획|증정|단독|1\+1|본품|추가|대용량|세트|듀오|트리플|한정|리필팩|리필|단품|구성'
    r'|트래블\s*키트|캡슐\s*키트|스틱포'
    r'|더블\s*(기획|세트|구성|\d+입|\d+\s*(ml|g|㎖))',
    re.IGNORECASE
)
REGEX_PRODUCT_TAIL_SYMBOLS = re.compile(r'[\s*/\-.,]+$|(?<=\s)[*/\-,.]+(?=\s|$)')
REGEX_MULTI_SPACE = re.compile(r'\s+')

# 성분명 내 쉼표 마스킹 (영숫자 사이의 쉼표)
REGEX_COMMA_MASK = re.compile(r'(?<=[A-Za-z0-9]),(?=[A-Za-z0-9])')

# 성분 문자열 전처리 (줄바꿈/탭 제거, 구분자 통일)
REGEX_WHITESPACE_CTRL = re.compile(r'[\r\n\t]')
REGEX_ALT_SEPARATOR   = re.compile(r'[@|]')

# typo_map_regex 공통 경계 패턴 (성분 단독 보장)
_TYPO_RE_BOUNDARY = r'(?<![가-힣a-zA-Z0-9\-./]){raw}(?![가-힣a-zA-Z0-9\-./])'


# ==========================================
# 카테고리 추론
# ==========================================

# 후보 목록은 specificity 높은 순서로 정렬
# (더 구체적인 것 먼저 매칭해야 "올인원세럼" 같은 복합명 오분류 방지)
_SKINCARE_TONER_RULES    = {"default": "토너",  "candidates": []}
_SKINCARE_ESSENCE_RULES  = {"default": None,  "candidates": ["세럼", "앰플", "에센스"], "fallback": "에센스"}
_SKINCARE_CREAM_RULES    = {"default": "크림", "candidates": []}
_SKINCARE_LOTION_RULES   = {"default": None,  "candidates": ["올인원", "로션"],         "fallback": "로션"}
_SKINCARE_MISTOIL_RULES  = {"default": None,  "candidates": ["미스트", "페이스오일"],   "fallback": "페이스오일"}

_CLEANSING_FOAMGEL_RULES = {"default": None,         "candidates": ["클렌징젤", "클렌징폼"],   "fallback": "클렌징폼"}
_CLEANSING_OILBALM_RULES = {"default": None,         "candidates": ["클렌징밤", "클렌징오일"], "fallback": "클렌징오일"}
_CLEANSING_WATMILK_RULES = {"default": None,         "candidates": ["클렌징밀크", "클렌징워터"],"fallback": "클렌징워터"}
_CLEANSING_PEEL_RULES    = {"default": "필링스크럽", "candidates": []}

# (main_category, sub_category) → 룰
_SUBCAT_RULES: dict[tuple[str, str], dict] = {
    ("스킨케어",     "스킨/토너"):       _SKINCARE_TONER_RULES,
    ("스킨케어",     "에센스/세럼/앰플"): _SKINCARE_ESSENCE_RULES,
    ("스킨케어",     "크림"):            _SKINCARE_CREAM_RULES,
    ("스킨케어",     "로션"):            _SKINCARE_LOTION_RULES,
    ("스킨케어",     "미스트/오일"):      _SKINCARE_MISTOIL_RULES,
    ("클렌징",       "클렌징폼/젤"):      _CLEANSING_FOAMGEL_RULES,
    ("클렌징",       "오일/밤"):          _CLEANSING_OILBALM_RULES,
    ("클렌징",       "워터/밀크"):        _CLEANSING_WATMILK_RULES,
    ("클렌징",       "필링&스크럽"):      _CLEANSING_PEEL_RULES,
    # 더모 코스메틱 — sub_category가 뭉쳐있어서 전체 후보 포함
    # fallback은 각각 에센스 / 클렌징폼 (가장 범용적)
    ("더모 코스메틱", "스킨케어"): {
        "default": None,
        "candidates": ["세럼", "앰플", "크림", "로션", "올인원", "토너", "에센스", "페이스오일", "미스트"],
        "fallback": "에센스",
    },
    ("더모 코스메틱", "클렌징"): {
        "default": None,
        "candidates": ["클렌징젤", "클렌징오일", "클렌징밤", "클렌징워터", "클렌징밀크", "필링스크럽", "클렌징폼"],
        "fallback": "클렌징폼",
    },
}

_CATEGORY_KEYWORDS: dict[str, re.Pattern] = {
    "토너":       re.compile(r'토너|(?<![가-힣a-zA-Z0-9])토닉(?![가-힣a-zA-Z0-9])|toner|(?<![가-힣a-zA-Z0-9])스킨(?!케어|[가-힣a-zA-Z0-9])|skin(?!care)|패드|pad|소프너', re.I),
    "앰플":       re.compile(r'앰플|원액|스팟|ampoule', re.I),
    "세럼":       re.compile(r'세럼|serum|부스터|샷(?!건)|(?<![가-힣a-zA-Z])젤(?![가-힣a-zA-Z])|(?<![가-힣a-zA-Z])겔(?![가-힣a-zA-Z])', re.I),
    "에센스":     re.compile(r'에센스|essence', re.I),
    "크림":       re.compile(r'크림|cream', re.I),
    "올인원":     re.compile(r'올인원|all.?in.?one|멀티크림|멀티밤', re.I),
    "로션":       re.compile(r'로션|에멀전|에멀젼|유액|lotion|emulsion|모이스처라이저|moisturizer|플루이드|fluid|밀크(?!클렌)', re.I),
    "페이스오일":  re.compile(r'오일|아로마틱 케어|oil', re.I),
    "미스트":     re.compile(r'미스트|스프레이|하이드롤라|워터|에센스|오떼르말|오 떼르말|mist|픽서|픽싱|세팅|fixer|setting', re.I),
    "클렌징젤":   re.compile(r'젤|gel', re.I),
    "클렌징폼":  re.compile(r'폼|foam|워시|wash|비누|솝|soap|바|bar|버블|bubble|무스|mousse|휩|whip|클렌저|클렌져', re.I),
    "클렌징밤":   re.compile(r'밤(?!\s*크림)|발름|balm', re.I),
    "클렌징오일":  re.compile(r'오일|oil', re.I),
    "클렌징밀크":  re.compile(r'밀크|milk|크림\s*클렌|로션\s*클렌|클렌징\s*로션', re.I),
    "클렌징워터":  re.compile(r'워터|water|미셀라|micellar|리무버|remover|H2O', re.I),
    "필링스크럽":  re.compile(r'필링|스크럽|peeling|scrub', re.I),
}
 
 
def infer_category(
    product_name: str,
    main_category: str,
    sub_category: str,
    product_name_raw: str = "",
) -> str:
    """
    (main_category, sub_category) → 후보 목록 → 제품명 키워드 매칭 → category 반환.
 
    매칭 순서:
        1. (main_category, sub_category)로 룰 조회
        2. 단일 확정(candidates 없음)이면 default 바로 반환
        3. clean_product_name 기준 키워드 매칭
        4. 미매칭 시 product_name_raw로 2차 매칭
           (괄호 안 키워드 활용 — 예: "[보습오일] 스쿠알란" → 페이스오일)
        5. 둘 다 미매칭 → candidates[-1] fallback
        6. 룰 자체가 없으면 "기타"
 
    Args:
        product_name:     정제된 제품명 (clean_product_name)
        main_category:    크롤링 원본 main_category
        sub_category:     크롤링 원본 sub_category
        product_name_raw: 크롤링 원본 제품명 (2차 탐색용, 기본값 "")
 
    Returns:
        str: category 값 (예: "세럼", "클렌징폼", "기타")
    """
    rule = _SUBCAT_RULES.get((main_category, sub_category))
    if rule is None:
        return "기타"
 
    # 단일 확정
    if rule["default"] and not rule["candidates"]:
        return rule["default"]
 
    # 1차: clean_product_name 키워드 매칭
    for candidate in rule["candidates"]:
        pattern = _CATEGORY_KEYWORDS.get(candidate)
        if pattern and pattern.search(product_name):
            return candidate
 
    # 2차: product_name_raw 키워드 매칭 (괄호 포함 원본)
    if product_name_raw:
        for candidate in rule["candidates"]:
            pattern = _CATEGORY_KEYWORDS.get(candidate)
            if pattern and pattern.search(product_name_raw):
                return candidate
 
    # fallback: 후보 중 마지막 (가장 범용적인 것을 마지막에 배치)
    return rule.get("fallback") or (rule["candidates"][-1] if rule["candidates"] else "기타")
 

# ==========================================
# 내부 헬퍼
# ==========================================


# 올리브영 도메인 네임스페이스 — 다른 쇼핑몰 데이터 추가 시 별도 네임스페이스로 격리
_OLIVEYOUNG_NS = uuid.uuid5(uuid.NAMESPACE_DNS, "oliveyoung.co.kr")


def _make_product_id(brand: str, clean_name: str) -> str:
    """
    브랜드명 + 정제된 제품명 기반 UUID v5를 product_id로 반환합니다.
 
    UUID v5(SHA-1 + 네임스페이스)를 사용하여:
        - 동일 제품 재처리 시 항상 동일한 ID 생성 (결정적)
        - RFC 4122 표준 포맷으로 타 시스템 연동 호환성 확보
        - 네임스페이스로 도메인(올리브영) 격리
 
    Args:
        brand:      제품 브랜드명
        clean_name: Step 4에서 정제된 제품명
 
    Returns:
        str: UUID v5 문자열 (예: 'a1b2c3d4-e5f6-5789-abcd-ef0123456789')
    """
    return str(uuid.uuid5(_OLIVEYOUNG_NS, f"{brand}||{clean_name}"))
 
 
def _make_error(
    product_id:              str,
    category:                str | None,
    main_category:           str | None,
    sub_category:            str | None,
    brand:                   str,
    product_name_raw:        str,
    product_name:            str,
    raw_text:                str,
    url:                     str,
    crawled_at,
    error_type:              str,
    residual_text:           str,
    goods_no:                str = "",
) -> ErrorRecord:
    """에러 레코드를 생성합니다."""
    return ErrorRecord(
        product_id              = product_id,
        category                = category,
        main_category           = main_category,
        sub_category            = sub_category,
        product_brand           = brand,
        product_name_raw        = product_name_raw,
        product_name            = product_name,
        product_ingredients_raw = raw_text,
        product_url             = url,
        crawled_at              = crawled_at,
        error_type              = error_type,
        residual_text           = residual_text,
        goods_no                = goods_no,
    )


def _is_blank(v: str) -> bool:
    """빈 문자열 또는 'nan' 문자열이면 True를 반환합니다."""
    return not v or v.strip() in ('', 'nan')


def _is_garbage_name(name: str, cfg: dict) -> bool:
    """
    garbage_keywords.json 설정을 기반으로 제품명이 크롤링 오류 텍스트인지 판별합니다.

    판별 순서:
        1. exact   - 완전 일치
        2. contains - 포함 여부

    Args:
        name: 판별할 제품명 (product_name_raw)
        cfg:  garbage_keywords.json을 로드한 dict (None이면 항상 False)

    Returns:
        True면 garbage (INVALID_METADATA_REJECTED 처리 대상)
    """
    if not cfg:
        return False
    if name in cfg.get("exact", []):
        return True
    for kw in cfg.get("contains", []):
        if kw in name:
            return True
    return False


def _apply_typo_maps(
    text: str,
    typo_regex_list: list[dict],
    typo_list: list[dict],
) -> str:
    """
    typo_map_regex.json(정규식 기반) → typo_map.json(단순 치환) 순서로 오타를 보정합니다.

    적용 순서:
        1. typo_map_regex: 부분집합 오염 위험이 있는 케이스. raw 길이 내림차순으로
           미리 정렬된 상태로 전달받으며, 경계 패턴으로 단독 성분만 치환합니다.
        2. typo_map: 단순 문자열 치환. raw 길이 내림차순으로 미리 정렬된 상태로 전달받습니다.

    Args:
        text:             치환 대상 텍스트
        typo_regex_list:  typo_map_regex.json 로드 결과 (list[{"raw", "fix", "pattern"}])
        typo_list:        typo_map.json 로드 결과 (list[{"raw", "fix"}])

    Returns:
        str: 오타 보정된 텍스트
    """
    # 1. 정규식 기반 치환 (우선 적용, 긴 raw부터)
    for entry in typo_regex_list:
        if entry["raw"] in text:
            pattern = _TYPO_RE_BOUNDARY.format(raw=re.escape(entry["raw"]))
            text = re.sub(pattern, entry["fix"], text)

    # 2. 단순 문자열 치환 (긴 raw부터)
    for entry in typo_list:
        if entry["raw"] in text:
            text = text.replace(entry["raw"], entry["fix"])

    return text


# ==========================================
# 전처리 파이프라인 (내부 단계 함수)
# ==========================================

def _compile_product_name_norms(
    norm_list: list[dict],
) -> list[tuple[re.Pattern, str]]:
    """
    product_name_norm_list(Iceberg 로드 결과)를 컴파일된 (패턴, 치환) 튜플로 변환합니다.

    match_type='regex'  → re.compile(raw)
    match_type='simple' → re.compile(re.escape(raw))
    """
    compiled = []
    for entry in norm_list:
        raw        = entry["raw"]
        fix        = entry["fix"]
        match_type = entry.get("match_type", "regex")
        if match_type == "simple":
            compiled.append((re.compile(re.escape(raw)), fix))
        else:  # "regex"
            compiled.append((re.compile(raw), fix))
    return compiled


def _clean_rows(
    df: pd.DataFrame,
    typo_list: list[dict],
    typo_regex_list: list[dict],
    garbage_config: dict,
    product_name_norm_compiled: list[tuple[re.Pattern, str]],
) -> tuple[list[dict], list[dict]]:
    """
    Step 1~9: 행별 정제를 수행하여 interim_list와 error_records를 반환합니다.

    - Step 1: 누락 필드 검사 → INCOMPLETE_DATA_REJECTED
    - Step 2: 제품명 기반 필터링 (옵션 번들, garbage)
    - Step 3: 제품명 클리닝 + product_id 생성
    - Step 4: 특수기호 제거 및 구분자 치환
    - Step 5: 오타 사전 치환
    - Step 6: 무효 문구 소거
    - Step 7: 이종 결합 번들 탐지
    - Step 8: 성분명 농도/이명 괄호 제거
    - Step 9: category 추론
    """
    interim_list  = []
    error_records = []

    for _, row in df.iterrows():
        raw_text         = str(row.get('ingredients', ''))
        product_name_raw = str(row.get('name', ''))
        brand            = str(row.get('brand', ''))
        url              = str(row.get('url', ''))
        main_category    = str(row.get('main_category', ''))
        sub_category     = str(row.get('sub_category', ''))
        goods_no         = str(row.get('goods_no', ''))

        crawled_at_raw = row.get('crawled_at', None)
        try:
            crawled_at = pd.Timestamp(crawled_at_raw, tz="UTC")
        except Exception:
            crawled_at = pd.NaT

        try:
            rating = float(row.get('rating', 0.0))
        except (ValueError, TypeError):
            rating = 0.0

        try:
            review_count = int(float(row.get('review_count', 0)))
        except (ValueError, TypeError):
            review_count = 0

        review_stats = row.get('review_stats', {})

        # [Step 1] 누락 필드 검사
        missing_fields = []
        if _is_blank(raw_text):         missing_fields.append('ingredients')
        if _is_blank(product_name_raw): missing_fields.append('name')
        if _is_blank(brand):            missing_fields.append('brand')
        if _is_blank(url):              missing_fields.append('url')
        if pd.isnull(crawled_at):       missing_fields.append('crawled_at')
        if _is_blank(main_category):    missing_fields.append('main_category')
        if _is_blank(sub_category):     missing_fields.append('sub_category')

        if missing_fields:
            tmp_id = _make_product_id(brand, product_name_raw)
            error_records.append(_make_error(
                tmp_id, None, main_category, sub_category,
                brand, product_name_raw, product_name_raw,
                raw_text, url, crawled_at,
                'INCOMPLETE_DATA_REJECTED',
                f"Missing fields: {', '.join(missing_fields)}",
                goods_no,
            ))
            continue

        # [Step 2a] 옵션 번들 필터링
        if REGEX_PRODUCT_OPTION_BUNDLE.search(product_name_raw):
            tmp_id = _make_product_id(brand, product_name_raw)
            error_records.append(_make_error(
                tmp_id, None, main_category, sub_category,
                brand, product_name_raw, product_name_raw,
                raw_text, url, crawled_at,
                'OPTION_BUNDLE_REJECTED',
                'Multi-option product (n-종) detected in name',
                goods_no,
            ))
            continue

        # [Step 2b] garbage 제품명 필터링
        if _is_garbage_name(product_name_raw, garbage_config):
            tmp_id = _make_product_id(brand, product_name_raw)
            error_records.append(_make_error(
                tmp_id, None, main_category, sub_category,
                brand, product_name_raw, product_name_raw,
                raw_text, url, crawled_at,
                'INVALID_METADATA_REJECTED',
                f"Garbage name detected: {product_name_raw!r}",
                goods_no,
            ))
            continue

        # [Step 3] 제품명 클리닝 + product_id 생성
        product_name = REGEX_PRODUCT_BRACKET.sub(' ', product_name_raw)
        product_name = REGEX_PRODUCT_VOLUME_ANCHOR.sub('', product_name)
        product_name = REGEX_PRODUCT_MARKETING.sub('', product_name)
        product_name = REGEX_PRODUCT_TAIL_SYMBOLS.sub('', product_name)
        clean_product_name = REGEX_MULTI_SPACE.sub(' ', product_name).strip()
        for _pat, _rep in product_name_norm_compiled:
            clean_product_name = _pat.sub(_rep, clean_product_name)
        product_id = _make_product_id(brand, clean_product_name)

        text = raw_text

        # [Step 4] 특수기호 제거 및 구분자 치환
        text = REGEX_WHITESPACE_CTRL.sub('', text)
        text = REGEX_ALT_SEPARATOR.sub(',', text)

        # [Step 5] 오타 사전 치환
        text = _apply_typo_maps(text, typo_regex_list, typo_list)

        # [Step 6] 무효 문구 소거
        text = REGEX_PREFIX_ALL.sub('', text)
        text = REGEX_NO_INGREDIENT.sub('', text)
        text = REGEX_LEGEND.sub('', text)
        text = REGEX_ILN.sub('', text)
        text = REGEX_LEGEND_WITH_BRACKET.sub('', text)
        text = REGEX_SYMBOLS.sub('', text)

        # [Step 7] 이종 결합 번들 탐지
        if len(REGEX_BONPUM_MULTI.findall(text)) >= 2:
            error_records.append(_make_error(
                product_id, None, main_category, sub_category,
                brand, product_name_raw, clean_product_name,
                raw_text, url, crawled_at,
                'HETEROGENEOUS_BUNDLE_REJECTED', text,
                goods_no,
            ))
            continue

        bundle_count = len(REGEX_BUNDLE.findall(text))
        if bundle_count >= 2:
            error_records.append(_make_error(
                product_id, None, main_category, sub_category,
                brand, product_name_raw, clean_product_name,
                raw_text, url, crawled_at,
                'HETEROGENEOUS_BUNDLE_REJECTED', text,
                goods_no,
            ))
            continue
        elif bundle_count == 1:
            text = REGEX_BUNDLE.sub('', text)

        # [Step 8] 성분명 농도/이명 괄호 제거
        text = REGEX_BRACKET.sub('', text)

        # [Step 9] category 추론
        category = infer_category(clean_product_name, main_category, sub_category, product_name_raw)

        interim_list.append({
            'product_id':              product_id,
            'category':                category,
            'main_category':           main_category,
            'sub_category':            sub_category,
            'product_brand':           brand,
            'product_name':            clean_product_name,
            'product_name_raw':        product_name_raw,
            'cleaned_text_str':        text,
            'product_ingredients_raw': raw_text,
            'rating':                  rating,
            'review_count':            review_count,
            'review_stats':            review_stats,
            'product_url':             url,
            'crawled_at':              crawled_at,
            'goods_no':                goods_no,
        })

    return interim_list, error_records


def _dedup_interim(interim_list: list[dict]) -> tuple[pd.DataFrame, list[dict]]:
    """
    Step 10: 브랜드 + 정규화 이름 기준 중복을 제거합니다.
    성분 문자열이 짧은 쪽을 유지하고, 나머지는 DUPLICATE_PRODUCT_REJECTED로 반환합니다.
    """
    interim_df = pd.DataFrame(interim_list)
    interim_df['text_len']  = interim_df['cleaned_text_str'].apply(len)
    interim_df['name_norm'] = interim_df['product_name'].str.replace(" ", "", regex=False)
    interim_df = interim_df.sort_values('text_len', ascending=True)

    duplicate_mask = interim_df.duplicated(subset=['product_brand', 'name_norm'], keep='first')

    duplicate_errors = [
        _make_error(
            r['product_id'], r['category'], r['main_category'], r['sub_category'],
            r['product_brand'], r['product_name_raw'], r['product_name'],
            r['product_ingredients_raw'], r['product_url'], r['crawled_at'],
            'DUPLICATE_PRODUCT_REJECTED',
            f"Duplicate of {r['product_brand']} | {r['product_name']}",
            r['goods_no'],
        )
        for r in interim_df[duplicate_mask].to_dict('records')
    ]

    deduped_df = interim_df[~duplicate_mask].drop(columns=['text_len', 'name_norm'])
    return deduped_df, duplicate_errors


def _match_ingredients(
    deduped_df: pd.DataFrame,
    ac_automaton: ahocorasick.Automaton,
) -> tuple[list[dict], list[dict]]:
    """
    Step 11~13: Aho-Corasick 성분 매칭 후 silver / error 레코드를 반환합니다.

    - Step 11: 성분명 내 쉼표 마스킹
    - Step 12: Aho-Corasick 탐색 + 히든 번들 탐지
    - Step 13: silver / error 라우팅
    """
    silver_records = []
    error_records  = []

    for _, row in deduped_df.iterrows():
        text        = row['cleaned_text_str']
        product_id  = row['product_id']
        url         = row['product_url']
        crawled_at  = row['crawled_at']
        category     = row['category']
        main_category = row['main_category']
        sub_category  = row['sub_category']

        # [Step 11] 성분명 내 쉼표 마스킹
        text = REGEX_COMMA_MASK.sub('_C_', text)

        # [Step 12-1] 공백 제거 후 Aho-Corasick 탐색
        if ',' in text:
            processed_text = ",".join(p.replace(" ", "") for p in text.split(','))
        else:
            processed_text = text.replace(" ", "")

        matches, residual = search_with_ac(processed_text, ac_automaton)

        # [Step 12-2] 히든 번들 탐지
        if matches and (matches.count('정제수') >= 2 or matches.count('글리세린') >= 2):
            error_records.append(_make_error(
                product_id, category, main_category, sub_category,
                row['product_brand'], row['product_name_raw'], row['product_name'],
                row['product_ingredients_raw'], url, crawled_at,
                'HIDDEN_BUNDLE_REJECTED', 'Duplicate Core Ingredients',
                row['goods_no'],
            ))
            continue

        # [Step 12-3] 잔여 텍스트 마스킹 복원
        residual_text = residual.replace("_C_", ",").strip()

        # [Step 13] silver / error 라우팅
        if matches:
            seen = set()
            deduped = [ing for ing in matches if ing not in seen and not seen.add(ing)]

            silver_records.append({
                'product_id':              product_id,
                'category':                category,
                'main_category':           main_category,
                'sub_category':            sub_category,
                'product_brand':           row['product_brand'],
                'product_name':            row['product_name'],
                'product_ingredients':     deduped,
                'product_name_raw':        row['product_name_raw'],
                'product_ingredients_raw': row['product_ingredients_raw'],
                'rating':                  row['rating'],
                'review_count':            row['review_count'],
                'review_stats':            row['review_stats'],
                'product_url':             url,
                'crawled_at':              crawled_at,
                'goods_no':                row['goods_no'],
            })

        if (residual_text
                and len(residual_text.strip()) > 2
                and re.search(r'[가-힣a-zA-Z0-9]', residual_text)):
            error_records.append(_make_error(
                product_id, category, main_category, sub_category,
                row['product_brand'], row['product_name_raw'], row['product_name'],
                row['product_ingredients_raw'], url, crawled_at,
                'UNMAPPED_RESIDUAL', residual_text,
                row['goods_no'],
            ))

    return silver_records, error_records


# ==========================================
# 전처리 파이프라인 (오케스트레이터)
# ==========================================

def process_pipeline(
    df: pd.DataFrame,
    ac_automaton: ahocorasick.Automaton,
    typo_list: list[dict],
    typo_regex_list: list[dict],
    garbage_config: dict = None,
    product_name_norm_list: list[dict] = None,
    batch: BatchMetadata = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Bronze raw DataFrame을 받아 silver / error DataFrame으로 전처리합니다.

    처리 흐름:
        Step 1~9  → _clean_rows()
        Step 10   → _dedup_interim()
        Step 11~13 → _match_ingredients()

    Args:
        df:                     Bronze raw DataFrame
        ac_automaton:           빌드된 Aho-Corasick 오토마타
        typo_list:              typo_map 로드 결과 (list[{"raw", "fix"}], 길이 내림차순)
        typo_regex_list:        typo_map_regex 로드 결과 (list[{"raw", "fix"}], 길이 내림차순)
        garbage_config:         garbage_keywords 로드 결과 (None이면 garbage 필터 미적용)
        product_name_norm_list: 제품명 정규화 규칙 (list[{"raw", "fix", "match_type"}])

    Returns:
        (silver_df, error_df)
    """
    batch = batch or create_batch_metadata("bronze_to_silver")

    # product_name_norm 패턴 컴파일 (한 번만)
    norm_compiled = _compile_product_name_norms(product_name_norm_list or [])

    # [Step 1~9] 행별 정제
    interim_list, error_records = _clean_rows(
        df, typo_list, typo_regex_list, garbage_config, norm_compiled
    )

    if not interim_list:
        error_df = pd.DataFrame([r.to_dict() for r in error_records])
        add_batch_metadata(error_df, batch)
        return pd.DataFrame(), error_df

    # [Step 10] 중복 제거
    deduped_df, duplicate_errors = _dedup_interim(interim_list)
    error_records.extend(duplicate_errors)

    # [Step 11~13] 성분 매칭
    silver_records, match_errors = _match_ingredients(deduped_df, ac_automaton)
    error_records.extend(match_errors)

    silver_df = pd.DataFrame(silver_records)
    error_df  = pd.DataFrame([r.to_dict() for r in error_records])

    for df_ in (silver_df, error_df):
        add_batch_metadata(df_, batch)

    return silver_df, error_df
