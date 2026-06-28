# 피지·모공 추천 grounding 저하 진단

## 결론

피지·모공 질의의 추천 품질은 검색 쿼리만으로 해결되지 않는다. WAS 검색
완화로 grounding이 개선됐지만, 그래프에 핵심 성분과 `SEBUM_REGULATION`
효능의 근거 엣지가 부족해 검색 결과의 품질 천장이 남아 있다.

이 문서의 수치와 Neo4j 조회 결과는 WAS 진단에서 관찰한 값이다. 이
저장소만으로 재현한 결과는 아니며, 원본 eval 산출물과 운영 DB 스냅샷을
함께 보존해야 독립 검증할 수 있다.

## 증상에서 코드까지의 추적

1. 모공·피지 질의에 안티에이징 제품과 근거 없는 성분 설명이 반환됐다.
2. C0 eval에서 grounding은 1.90/5였다.
3. 프롬프트 강화는 grounding을 0.37 높였지만 overall을 악화시켜
   주원인으로 보기 어려웠다.
4. 검색 쿼리 완화(D1-A)는 grounding을 2.63으로 0.73 높이고 overall을
   0.27 높였다. 검색이 병목 중 하나라는 근거다.
5. Neo4j 직접 조회에서는 피지 효능에 레티놀 등 안티에이징 성분이
   반환됐고, 나이아신아마이드·BHA·아연은 `SEBUM_REGULATION`에 충분히
   연결되지 않았다.
6. 그래프 생성 코드에는 두 데이터 경로가 있다.
   - COSING 함수는 `_COSING_FUNC_TO_EFFECTS`로 soft edge를 만들며
     `graph_score=0.0`, `paper_count=0`을 기록한다
     (`scripts/build_gold_csvs.py`).
   - PubMed claim은 후보 문장 필터, 성분 문맥 필터, LLM 추출, taxonomy
     매핑을 통과해야 근거 edge가 된다.
7. PubMed 경로의 후보 필터와 `ClaimExtractor`의 공통 피부 문맥에는 피지
   용어가 일관되게 반영되지 않았다. 특히 `ClaimExtractor`의
   `_is_niacinamide_context_valid`는 피부·색소·장벽·여드름 문맥은
   허용하지만 `sebum/oil/oily/pore/seborrhea` 문맥을 허용하지 않았다.

## 이번 변경

피지 문맥을 다음 두 단계에 동일하게 추가한다.

- `claim_filter.is_claim_candidate_sentence`: LLM 호출 전 후보 문장 보존
- `ClaimExtractor`: 공통 피부 문맥 및 니아신아마이드 전용 문맥 보존

허용어는 `sebum`, `oil`, `oily`, `oiliness`, `pore`, `pores`,
`seborrhea`, `seborrheic`이다. `oil`이 `spoiled` 같은 단어 내부에서
매칭되지 않도록 단어 경계를 사용한다. NAD/NMN/대사 문맥 차단 규칙은
그대로 유지한다.

이 변경은 기존 corpus에 관련 문장이 있을 때의 false negative를 줄인다.
관련 논문이나 문장이 corpus에 없다면 새 edge를 만들지 못한다.

## 확정된 사실과 남은 가설

| 판단 | 상태 | 근거 |
|---|---|---|
| 검색이 병목 중 하나다 | 확정 | 쿼리 변경만으로 grounding +0.73 |
| 현재 그래프의 피지 효능 연결이 불완전하다 | 확정 | 운영 Neo4j 직접 조회 |
| COSING soft edge의 점수는 0이다 | 코드로 확정 | `build_cosing_soft_edges` |
| 피지 문맥이 claim 필터 사이에서 일관되지 않았다 | 코드로 확정 | `claim_filter.py`, `claim_extractor.py` |
| 기존 PubMed corpus에 유효 claim이 존재한다 | 미확정 | S3 Bronze/Silver/Gold 조회 필요 |
| 이 필터가 실제 claim을 탈락시켰다 | 미확정 | 배치별 탈락 사유 감사 필요 |

따라서 "필터 편향 때문에 실제 claim이 탈락했다"는 현재 유력한 가설이지,
아직 데이터로 확정된 결론은 아니다.

## 파이프라인 환경 검증 절차

1. S3에서 동일 배치의 Bronze, Silver, Gold 산출물을 확보한다.
2. `sebum|oil|oily|oiliness|pore|seborrh`로 Silver 문장을 찾고,
   니아신아마이드·살리실산·아연 관련 문장 수를 집계한다.
3. 기존 코드와 변경 코드에서 후보 문장 수, 성분 인식 수, LLM claim 수,
   taxonomy 매핑 수를 단계별 비교한다.
4. Gold를 새 batch로 재생성한다. 기존 batch를 덮어쓰지 않는다.
5. Neo4j 적재 전 edge diff를 검토하고, PubMed edge의 PMID와 문장을
   샘플 감사한다.
6. 새 그래프를 별도 DB 또는 namespace에 적재한다.
7. 같은 C0 eval 세트로 grounding와 overall을 재측정한다.

성공 기준은 필터 통과 수가 아니라 다음 세 조건이다.

- 핵심 피지 성분의 근거 있는 `SEBUM_REGULATION` edge 증가
- 비피부·대사 문맥 false positive 비증가
- 동일 eval에서 grounding 개선 및 overall 비악화

## PR 초안

**제목:** `fix(claim): preserve sebum context during claim extraction`

**본문:**

피지 관련 PubMed 문장이 후보 필터 또는 니아신아마이드 전용 문맥 필터에서
탈락할 수 있었다. 두 단계에 같은 피지 문맥 어휘를 추가하고 단어 경계
매칭을 적용했다. 대사/NAD 제외 규칙은 유지하며 회귀 테스트를 추가했다.

이 PR은 추출 recall을 높이는 코드 수정이다. 운영 그래프 개선 효과는 S3
claim audit, Gold 재생성, Neo4j 재적재, C0 eval을 거쳐 별도로 검증해야
한다.

**검증:**

```bash
python -m unittest discover -s tests -v
```
