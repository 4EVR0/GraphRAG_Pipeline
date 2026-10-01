# 피부 고민별 근거 gate 설계 — v1 shadow pilot

## 목표와 현재 범위

논문 검색 결과나 성분명이 아니라, **출처를 추적할 수 있는 성분–측정 결과 주장**을 검증한다.
검색 모델/최종 응답 평가가 통과해도 잘못 연결한 원문 근거를 보증하지는 못한다.

이번 구현은 독립적인 **오프라인 감사 도구**다. 기존 Gold 생성기, S3,
Neo4j, 서버 검색을 변경하지 않는다. 실제 추천 효과가 검증됐거나 운영 gate가
완성됐다는 의미가 아니다. 보습(HYDRATING / MOISTURE_RETENTION / BARRIER_REPAIR)
세 효능을 첫 파일럿으로 삼고, 다른 효능은 검토 필요로 남긴다.

## 확정 정책

- 사용자 결정: 피부 세포·인공피부·동물 피부 근거는 설명용으로 보존하고 추천 점수에서 제외한다.
  체외 적출 피부도 같은 보수적 정책을 적용한다. 비피부 모델은 피부 기전 설명으로 승격하지 않는다.
- 무관한 논문이나 불명확한 주장도 원본에서 삭제하지 않는다. 판단과 사유를 별도 보관한다.
- 단순 문장 키워드/LLM 자신감/논문 수로 검증 통과를 결정하지 않는다.
- 연구 조건이 없으면 추정하지 않는다. 기존 `human_topical` 라벨도 검증 대상이다.
- 원료/제품 제형 연구, 사람/모델, 바르는/먹는/주사, 효과/내약성/기전 주장을 분리한다.
- 복합제품 전체의 효과는 그 안의 모든 성분에 귀속하지 않는다. 해당 제품 근거로 보존하고,
  성분 기여가 분리되지 않으면 이 파일럿의 성분 추천 후보로는 통과시키지 않는다.
- 성분의 유명세를 가산하지 않는다. 카페인·연어알추출물도 원문이 뒷받침하는 범위에서만 사용한다.
- `reduces`를 일괄 거부하지 않는다. TEWL 감소는 수분 유지에 유리하지만 수분량 감소는 그렇지 않다.
  TEWL 감소 하나를 자동으로 장벽 **복구**와 보습 모두에 중복 배정하지 않는다.
- 승인된 후보도 실제 사용자/제품 적용 가능성을 확인하기 전에는 추천 자격을 보장하지 않는다.

## 단계와 판정

1. 원본 보존: 명시한 단일 CSV 배치, 파일 SHA256, CSV 레코드 번호(물리적 줄 번호 아님),
   PMID, 원문 문장, 전체 행 해시를 기록한다. 서로 다른 배치를 조용히 합치지 않는다.
2. 연구 범위: 대상 조직, 종, 사람 임상 여부, 투여 경로를 본다. 문장 밖 방법/대상 정보도 필요하다.
   제목·원문 비피부 키워드는 경고용이며, 혼합 연구를 자동 삭제하지 않는다.
3. 주장 의미: 성분/제형, 실제 측정 지표, 변화 방향, 대조군, 결과 지지 여부를 각각 기록한다.
   한 문장의 다른 절에서 효능을 빌려오거나 상품 수식어를 결과로 해석하지 않는다.
4. 승인: 원문에 연결된 검토자가 범위·주장 함의를 확인한다. 모델 자동 출력은 승인으로 취급하지 않는다.
5. 후보: 검증된 사람 피부 국소 적용·성분 기여 분리·유의한 긍정 결과만 shadow 후보로 만든다.
   `matched_vehicle` 또는 `ingredient_add_on_control`을 사용한 성분 기여 검증이 필요하다.
   단순 baseline 대비/성별 비교만으로 대조 제형 대비 효과를 추정하지 않는다.

| 상태 | 의미 | 이번 구현에서 추천 점수 기여 |
|---|---|---|
| review_required | 미검토, 맥락 부족, 충돌, 파일럿 외 효능 | 없음 |
| reject | 검토 후 피부 추천 근거로 부적격 | 없음; 원본은 유지 |
| mechanism_only | 검토된 피부 모델 근거 | 없음; 설명 사용도 후속 검증 필요 |
| candidate | 출처·조건을 검토한 사람 피부 근거 | 아직 없음; shadow 후보만 생성 |

**미검토 ≠ 틀린 근거.** 처음 실행 시 승인 파일이 없으면 후보 0건이 정상이다.
그 결과를 논문 부족, 데이터 오류율 또는 근거 고갈로 보고하지 않는다.

## 검토 파일 계약

`--reviews` JSONL은 `tests/test_evidence_gate.py`의 `approved()`가 필드 예시를 제공한다.
그 테스트는 **합성 데이터**이며 실제 승인 레코드로 사용하면 안 된다.
record_id는 반드시 첫 감사의 decisions.jsonl에서 가져온다. CSV는 UTF-8 BOM을 제거해
읽으므로 다른 디코딩 방식으로 별도 해시를 계산하지 않는다. 입력 파일 원본 해시는 별도 보존한다.

- schema_version, record_id(전체 원본 행 해시), decision, reviewer, reviewed_on,
  source_verified, review_note, source_locator, supporting_quote(원문 문장에 존재해야 함)
- subject: human_skin / skin_cells / reconstructed_skin / animal_skin / ex_vivo_skin / non_skin
- route, claim_kind, attribution, comparator, measured_endpoint, change_direction,
  effect_code, result_support, significance, claim_entailment_verified
- population, body_site, concentration, formulation, duration, limitations
- context_excerpt와 context_source_url: 해당 PMID의 초록 방법/결과를 검토한 증거.
  전문이 필요한 경우 자동으로 임의 문단을 채우지 않고 검토 대기한다.
- scope_resolution: 비피부 키워드가 있는 혼합 연구에서 실제 피부 부분을 사용한다면 그 이유.

v1은 자연어 문장의 진실을 형식 검사만으로 증명하지 못한다. 승인 파일은 신뢰된 검토자의
판단이며, 부정확한 사람이 작성한 승인을 자동으로 잡아내는 시스템은 아니다.
측정 대상/개입/대조군을 지원하는 문맥을 사람이 확인하고, 중요 표본은 독립 이중 검토한다.
조건 필드는 자유 텍스트로 보존하지만 다음 단계에서 단위와 대상군을 구조화해야 한다.

## 사용법과 산출물

```bash
python -m scripts.audit_evidence_gate \
  --claims /absolute/path/to/batch/gold_claim_all.csv \
  --output-dir /absolute/path/to/new-shadow-run
# 검토 후 새 디렉터리에 반복: --reviews /absolute/path/to/reviewed_claims.jsonl
```

API, LLM, DB 연결, S3 업로드 없음. 기존 output 디렉터리는 덮어쓰지 않는다.
실험 산출물과 사람 검토 파일은 로컬에만 두고 PR에 넣지 않는다.

- decisions.jsonl: 원본, 판정, 사유, 승인 내용, 파일/행 출처.
- candidate_edges.jsonl: PMID, 원본 근거 ID, 원본/승인 해시, gate 버전, 효능,
  측정 결과, 대상/농도/제형 등 제약. **Neo4j 직접 import 형식이 아니다.**
  INCI 동일성 해결 및 조회 조건 매칭 전에 운영 적재하지 않는다.
- manifest.json: 입력 해시, 코드 SHA/dirty 여부, gate 버전, 판정별 수, 사유별 수,
  효능별 승인된 고유 PMID 수. 기존 graph_score를 임상 효과 크기처럼 재사용하지 않는다.

## 다음 PR 및 운영 전 승인 조건

### 임상 표본의 원문맥 확보

```bash
python -m scripts.fetch_pubmed_review_sources \
  --pmid 18498456 --pmid 11737814 \
  --output-dir /absolute/path/to/new-source-snapshot
```

명시한 고유 PMID 1–10건만 NCBI EFetch 한 요청으로 읽는다. 공개 초록·서지정보만
가져오고 전문 수집/LLM 추출/승인/운영 적재를 수행하지 않는다. 정확한 응답 XML,
Methods/Results 등의 구획, 정정·철회 등 연결 정보, DOI/PMCID, 응답 해시와 수집 시점을
보존한다. MeSH의 Humans·Clinical Trial 표시만으로 임상 적합성을 승인하지 않는다.
초록 없음은 명시하고, 응답 누락/중복 PMID·API 오류는 실패 처리한다. 기존 경로는 덮어쓰지 않는다.

표본 검토는 긍정적인 보습 결과만 고르는 방식이 아니라 같은 연구의 TEWL 무효 결과,
다른 대상군에서의 무효 결과도 함께 확인한다. 초록에 없는 적용 부위·비교군 농도·유의성을
임의로 채우지 않는다. 수집된 papers.jsonl은 Gold claim CSV나 승인 JSONL이 아니다.
관찰 결과와 비교 대상별로 주장을 분리한 다음 승인 검토를 수행해야 한다.

### 주장별 검수 패킷 (승인 전 단계)

```bash
python -m scripts.build_evidence_review_packets \
  --snapshot-dir /absolute/path/to/source-snapshot \
  --annotations /absolute/path/to/provisional-outcomes.jsonl \
  --output-dir /absolute/path/to/new-review-packets
```

이 도구는 자동 추출기가 아니다. 사람이 또는 모델이 작성한 **잠정 해석**을 원본에
연결해 독립 검토할 수 있도록 준비한다. raw XML의 SHA256을 수집 manifest와 비교하고
다시 파싱하므로 수정된 `papers.jsonl`을 신뢰하지 않는다. 기존 Gold나 승인 파일은 변경하지 않는다.

- 입력 한 행은 **성분 × 측정 지표 × 보고된 비교 × 적용 조건**의 한 결과다.
  같은 논문의 수분량 증가와 TEWL 무효 결과를 별도 행으로 보존한다. 같은 원문 문장을
  사용할 수 있지만 다른 측정 지표의 방향/유의성을 가져오면 안 된다.
- `study_control`은 연구 설계의 대조군, `result_comparator`는 해당 결과의 실제 비교 대상이다.
  연구가 RCT여도 결과에 baseline 비교만 보고되었다면 vehicle 대비 우월성으로 승격하지 않는다.
- `fields`의 모든 키는 필수이고 확인 불가 값은 `null`이다. 값이 있으면
  `{"value": "...", "spans": [{"section": 0, "quote": "literal abstract text"}]}`로
  출처를 연결한다. section은 0부터 시작하는 초록 구획 번호다. 임의로 번역한 문장을 quote로 넣지 않는다.
- 필드: ingredient_name, subject, route, claim_kind, attribution, study_control,
  result_comparator, measured_endpoint, change_direction, result_support, significance,
  population, body_site, concentration, formulation, duration.
- 행 메타데이터: schema_version(`atomic-outcome-review-v1`), source_response_sha256,
  pmid, prepared_by, prepared_on, result_span, limitations, note. `prepared_by`는 초안 작성자이지 승인자가 아니다.
  실제 승인 속성을 입력하면 오류로 처리한다. 합성 입력 예시는 `tests/test_evidence_review_packets.py` 참고.
- `no_detected_effect`는 해당 조건에서 효과/차이를 확인하지 못했다는 뜻이다.
  동등성 입증, 모든 조건에서 무효, 악화, 성분 전체 배제를 의미하지 않는다.
- `proposed_effect_code`는 방향에 따른 잠정 매핑일 뿐이다. TEWL 감소는 MOISTURE_RETENTION으로
  매핑하고 자동으로 BARRIER_REPAIR에 중복 배정하지 않는다. 무효·내약성 결과는 긍정 효능으로 매핑하지 않는다.

출력은 `review_packets.jsonl`과 `manifest.json`이다. 원본 초록과 필드별 인용구,
누락/비교/귀속/유의성 문제, 정정·철회 연결 검토 필요 여부, 원본·초안·코드 해시를 남긴다.
전부 `pending_independent_review`, `recommendation_eligible=false`이며 **차단 사유가 없어도 승인이 아니다.**
문자열 존재 검사는 의미 함의나 임상 타당성을 입증하지 않는다. 전체 초록으로 인용한 필드는
검토 시 정확한 절과 맥락을 확인한다. 스냅샷 해시는 로컬 변경 감지용이며 출처의 암호학적 인증은 아니다.

현재 gate의 `--reviews` 계약과 의도적으로 다르며 직접 투입할 수 없다. 기존 CSV 관계와의
계보 연결·독립 검토·승인 변환은 후속 작업이다. 자동 긍정 선별이나 추천 점수 계산을 하지 않는다.
잠정 초안은 편의 표본이며 모든 결과를 빠짐없이 추출했다거나 근거 정확도를 측정했다는 뜻이 아니다.

### 기존 Gold 주장과 출처 대조

```bash
python -m scripts.audit_evidence_lineage \
  --claims /absolute/path/to/one-batch/gold_claim_all.csv \
  --snapshot-dir /absolute/path/to/source-snapshot \
  --annotations /absolute/path/to/provisional-outcomes.jsonl \
  --output-dir /absolute/path/to/new-lineage-audit
```

단일 명시 배치의 전체 행 해시, 원본 근거 ID, 파일 해시와 모든 CSV 레코드 번호를
보존하면서 같은 PMID의 잠정 관찰 결과와 기존 주장들을 나란히 보여준다.
원본 XML 해시와 초안을 재검증하고 혼합 배치·중복 관찰·잘못된 출처는 출력 전에 실패한다.
중복 CSV 행은 한 번만 집계하지만 원본 위치는 모두 남긴다.

- `same_pmid_rows_found`는 논문 식별자가 같다는 뜻이지 주장 의미가 같다는 뜻이 아니다.
- `literal_source_locations`는 기존 원문 문장의 실제 초록 위치다. 해당 구획에 기록된
  `RESULTS: ` 등의 라벨만 제거할 수 있고 문자열을 유사도 기반으로 맞추지 않는다.
- `literal_result_overlap_observation_ids`도 문장 포함 관계만 표시한다. 같은 문장에서
  추출한 극성 반전이나 성분 귀속 오류를 자동 승인하지 않는다.
- `absent_from_selected_batch`는 선택한 로컬 배치에서 찾지 못했다는 뜻이다.
  전체 논문 corpus나 운영 Neo4j에 없다는 의미로 확장하지 않는다.
- 성분 이름 번역/동의어/INCI를 자동으로 통합하지 않는다. 기존 잘못된 행도 수정/삭제하지 않는다.

출력 `lineage_review.jsonl`은 원본/잠정 결과의 대조 자료이고 `manifest.json`은 재현 정보다.
`verified_claim_links=0`, `production_graph_lineage_verified=false`를 명시한다.
실제 의미 검수와 관계 계보 확정은 별도 작업이다. PMID 없는 운영 엣지의 계보를
비슷한 속성만 보고 복원했다고 주장하지 않는다.

### 사용자 분류 확인 이후: 요소 1편의 격리 변환

```bash
python -m scripts.transform_reviewed_urea_pilot \
  --snapshot /absolute/path/to/abstract-snapshot \
  --annotations /absolute/path/to/annotations.jsonl \
  --supplement /absolute/path/to/supplement.json \
  --checklist /absolute/path/to/checklist.json \
  --acceptance /absolute/path/to/classification-acceptance.json \
  --output /absolute/path/to/new-shadow-transform
```

이 도구는 **PMID 35663767 / PMC9060062.1의 합의된 세 분류만** 처리하는
제한된 회귀 파일럿이다. 다른 논문에 같은 결론을 적용하는 범용 추출기나 임상 승인기가 아니다.
실제 사용자 기록/논문 문단은 로컬 입력으로 받고 레포에는 합성 테스트만 둔다.

분류 동의→검토표→보완 문단→원래 초록/주장 해시 연결을 검사한다. 보완 문단은
XML fragment와 텍스트 projection도 대조하고, 기존 초록 인용을 본문 문장으로 바꾸지 않는다.
입력 경로는 명시적으로 받고 기록 안의 경로를 따라 임의 파일을 읽지 않는다.
해시는 변경 감지 및 연결용이지 검토자 신원이나 과학적 진실의 인증은 아니다.

출력 `outcomes.jsonl`은 다음을 별도 레코드로 보존한다.

- 다리: 조건부 긍정 수분량 결과, `HYDRATING`과 연구 대상·부위·농도·제형·기간 유지.
- 팔: 군 간 차이 미확인, 효능 코드 없음. 성분의 보편적 무효나 동등성으로 바꾸지 않음.
- TEWL: `excluded_not_reported`. 긍정도 무효도 아니므로 방향·유의성·효능 코드를 만들지 않음.

세 분류 중 하나라도 미동의/누락/충돌이거나 출처가 바뀌면 출력 전에 실패한다.
본문의 제형 비교 근거는 별도 context_supplement로 남기고 원래 두 관찰을 그대로 포함한다.
초록과 본문의 통계 보고 차이도 보존한다. 부위별 결과를 통합하거나 추천 점수를 계산하지 않는다.

전체 출력은 `recommendation_eligible=false`, `clinical_validity_review_completed=false`이고
`isolated_regression_fixture_not_production_evidence` 용도다. 사용자의 제시된 분류 동의가
독립 원문 전체 검수나 운영 추천 승인으로 확대되지 않는다. v1 gate의 승인 JSONL이나
Neo4j 적재 형식으로 사용할 수 없다. manifest는 입력 해시·코드 SHA·dirty 여부를 남긴다.

### 격리 적용 조건 매칭 검증

`python -m scripts.verify_urea_applicability`에 위 변환 도구와 동일한 여섯 인자를
전달하면 출처 변환과 조건 매칭을 함께 검증한다. 새 출력 디렉터리에만 기록하며
`applicability_report.json`에 입력 해시, 코드 SHA, dirty 여부와 개별 판정을 남긴다.

변환 v2는 부모의 연구 대상·농도·제형·기간 원문 값이 이 파일럿의 고정 조건인지
확인한 뒤 별도 `applicability_scope`를 만든다. 범용 정규화나 자연어 질환 추정은 아니다.
`shadow_applicability.assess`는 명시적으로 정규화된 조건만 정확히 비교한다.
부위·농도·제형·기간·대상·투여 경로·성분·효능 불일치와 누락은 차단한다.
팔의 군 간 차이 미확인과 TEWL 보고 제외를 긍정 효능으로 사용하지 않는다.

실제 출처에서 만든 3개 결과 × 합성 조건 14개 = 42개 검사다. 연구와 일치하는
다리 수분량 1개 조합만 `context_matched_review_pending`이며, 이것도 임상/운영
승인이 아니므로 `evidence_use_allowed=false`다. 전부 차단하는 구현이 통과하지
않도록 해당 조합의 조건 일치도 확인한다. 해시는 변경 감지이지 서명/진실 인증이 아니다.
이 검증은 사용자 응답 품질, 임상 유효성, 범용 추출 정확도를 평가하지 않는다.
운영 검색이나 기존 gate 승인 경로에는 연결하지 않는다.

### 남은 운영 전 검증

1. 보습 표본의 독립 검토표와 승인 가능한 정상 대조 사례 확보. 카페인 조건/연어알 출처 복원.
2. 논문 단위 맥락 추출 + 주장 단위 endpoint/span 구조화. LLM은 초안 생성만 수행하고
   외부 전송이 필요하면 데이터 범위와 비용을 별도 승인받는다.
3. shadow 통과 근거 → Evidence 노드/참조 가능한 근거 파일 → Ingredient–Effect 집계.
   PMID/근거 ID/변환 버전/배치/해시를 보존하고 검토 상태·임상 조건을 조회 시 전달한다.
   동일 PMID·성분·효능은 중복 가산하지 않으며 반대 결과도 별도 보존한다.
4. 별도 Neo4j DB/인스턴스에서 적재. 기존 운영 Gold를 덮어쓰지 않는다.
5. 기존/수정 추천 A/B: 보습 + 비보습 회귀, 출처 역추적, 잘못된 후보 배제,
   정상 근거 보존, 빈 결과/조건 불일치 동작, 인간 concern_fit/grounding/korean_quality 평가.
6. 검토된 골드셋에서 알려진 비피부/극성 반전/내약성 혼입 0건, 승인 후보 출처 추적 100%,
   합의한 정상 대조 보존 및 검토자 확인을 배포 조건으로 삼는다. 테스트 통과를 전체 정확도로 일반화하지 않는다.
7. 별도 배치와 feature flag로 전환, 이전 버전/인덱스를 유지해 롤백 가능하게 한다.
   잘못된 근거를 쓰는 구 버전으로 롤백하는 것은 비상 복구이며 정상 장기 운영안이 아니다.

## 수집 확대 정책

현재 검토와 INCI 동일성 검증 후 효능별 근거 공백을 파악한다. 그다음 PubMed 검색을
성분 동의어 + skin/cutaneous + 실제 endpoint + topical/대조시험 조건으로 설계한다.
PMID 중복, 재검색일, 검색식, 제외 사유를 남기고 부정/무효 결과도 함께 보존한다.
논문 수를 늘리거나 유명 성분을 1위로 만들기 위해 통과 기준을 낮추지 않는다.
기능/사전 근거는 임상 효능 근거와 별도 유형으로 유지하고 이후 랭킹 실험에서 비교한다.
