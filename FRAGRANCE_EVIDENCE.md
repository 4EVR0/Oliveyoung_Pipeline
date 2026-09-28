# 향료 표기 관찰값과 검토된 무첨가 안내

`CONTAINS`는 INCI 매핑 성공 성분만 담으므로, 간선이 없다는 이유로 무향료라고 판단하지 않는다. Silver 원문 `product_ingredients_raw`를 해석한 관찰값을 Product의 `fragrance_evidence` JSON 문자열로 수출한다. 새 테이블이나 재수집은 필요하지 않다.

## 데이터 계약 v1

- `schema_version`, `parser_version`, `product_id`
- `status`: `present`(향료/Fragrance/Parfum 항목 확인), `not_listed`(파서가 찾지 못함), `unknown`(판독 불충분)
- `present_terms`, `related_terms`: 관찰된 향료 항목 및 리날룰·파네솔·리모넨. 후자는 전체 향 알레르겐 목록이 아니다.
- `label_sha256`: 정규화 전 원문 문자열의 UTF-8 SHA-256
- `source_url`, `observed_at`: 제품 URL과 원문 수집 시각
- `manufacturer_claim`: 선택적인 사람이 검토한 제조사 무첨가 안내

`not_listed`는 전성분 완전성이나 무향료를 보증하지 않는다. 제품 옵션별 혼합 표기, 누락, 자연 유래 향 성분이 있을 수 있다. 파서는 법정 성분표 판정기가 아니다.

## 검토 자료 제공

선택 환경변수 `FRAGRANCE_CLAIMS_PATH`는 실행 컨테이너에서 읽을 수 있는 JSON 파일 경로다. Docker/Airflow 사용 시 검토 파일을 읽기 전용 마운트하고 환경변수를 전달해야 한다. 기본 DAG는 이 파일을 전달하지 않으므로 관찰값만 수출한다. 파일이 없으면 제조사 안내를 자동 생성하지 않는다.

아래는 **실제 검토 데이터가 아닌 구조 예시**다. 빈 필드인 이 예시는 서버 gate를 통과하지 않는다.

```json
{
  "PRODUCT_ID": {
    "claim": "no_added_fragrance",
    "product_id": "PRODUCT_ID",
    "label_sha256": "",
    "label_reviewed_complete": false,
    "source_url": "",
    "reviewed_at": "",
    "quote": "",
    "reviewed_by": ""
  }
}
```

검토자는 제조사의 정확한 제품/옵션에 대한 향료 무첨가 안내, 출처 URL, 원문 인용, 검토 시각(시간대 포함 ISO 8601), 검토자 식별자를 기록한다. 같은 제품의 전성분이 온전히 수집되었는지, 라벨과 제조사 안내가 충돌하지 않는지도 확인하고 원문 해시를 연결한다. 자동 파서의 신호 없음만으로 검토 완료를 표시하면 안 된다.

서버는 제품 ID·원문 해시 일치, 라벨/검토 최신성(기본 180일), 충돌 신호 없음, 검토 완료를 함께 검사한다. 이는 서비스의 보수적 정책이며 법적 인증이나 무취·저자극·알레르기 안전성 보장이 아니다.

## 변경·배포·복구

- 초기 CSV 수출: 기존 `goods_no` 등 컬럼을 유지하면서 메타데이터 추가. CSV에 JSON이 정상적으로 인용된다.
- 증분 NEW/CHANGED: CDC 행에는 검토된 원문 스냅샷이 없으므로 이전 `fragrance_evidence`를 지운다. 오래된 승인 재사용을 방지하며, 재검토·메타데이터 재적재 전에는 서버가 `unknown`으로 처리한다.
- 메타데이터 없는 기존 그래프와도 서버가 호환되지만 무향료 추천은 통과하지 않는다.
- 이 코드 병합 자체는 운영 그래프를 재적재하지 않는다. 실제 적재는 별도 배포 단계다. 전체 그래프를 파괴적으로 재생성할 필요 없이 검증된 Product 메타데이터만 별도 갱신하는 방식을 먼저 검토한다.
- 롤백: 이 변경을 revert하면 신규 수출/증분 메타데이터 처리만 되돌아간다. 이미 적재된 속성은 자동 삭제되지 않는다. 서버는 안전하게 없는/만료된 근거를 거절하므로 원문/관계를 복구하려고 전체 DB를 지우지 않는다.

## GPU 없는 확인

```sh
python -m unittest discover -s tests -p 'test_fragrance_evidence.py' -v
python -m scripts.audit_fragrance_labels
```

감사 명령은 Glue/S3를 읽기만 하며 스냅샷 ID와 집계만 출력한다. S3 업로드·Neo4j 변경·외부 LLM 호출은 하지 않는다.

## 표시 규칙 참고

착향제를 반드시 문자 그대로 ‘향료’로만 표기해야 한다는 전제는 사용하지 않는다. 법령은 ‘향료’로 표시할 **수 있다**고 규정한다. [화장품법 시행규칙 별표 4](https://www.law.go.kr/LSW/flDownload.do?bylClsCd=110201&flSeq=157585847&gubun=), [식약처 알레르기 유발성분 표시 안내](https://www.mfds.go.kr/brd/m_218/view.do?seq=33306).
