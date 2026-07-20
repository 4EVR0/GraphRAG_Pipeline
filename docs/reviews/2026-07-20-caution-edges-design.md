# CAUTION 엣지 — 근거 기반 금기 오버레이 (Option A)

## 배경
추천 시스템이 "여드름엔 레티놀 좋음"은 잡지만 "민감성엔 레티놀 주의"를 못 잡는다.
그래프의 `AFFECTS` 엣지는 전부 **긍정 연관**(성분→효능)이라, "성분이 자극을 유발한다"는
부작용 지식이 없기 때문. 이걸 **우리 지식(수기)**이 아니라 **논문 근거**로 채우는 게 목표(정체성 유지).

## 핵심 발견 (Step 0)
추출 파이프라인이 **부작용 claim을 이미 뽑고 있다** — 재마이닝 불필요(A1 한정):
- 스키마: `claim_type=safety`, `relation=causes/does_not_cause`, 타겟 `irritation/erythema/redness`
- `extract_concern_ids`가 이미 `irritation→IRRITATED_SKIN`, `erythema→REDNESS`, `sensitivity→SENSITIVE_SKIN`, `rosacea→ROSACEA_PRONE`, `atopic→ATOPIC_PRONE` 매핑
- **그런데 그래프엔 없음**: Gold 정책이 safety claim을 `recommendation_only` 티어로 강등 → `graph_claim.csv`에서 제외됨 (실측: 후보 13건 중 대부분 `recommendation_only`)

즉 **데이터는 있는데 라우팅에서 버려진다.** A1 = 이 claim들을 CAUTION 엣지로 라우팅.

## 엣지 설계
```
(Ingredient)-[:CAUTION {
    evidence_type: 'pubmed_evidence',   // AFFECTS와 동일 등급 체계 재사용
    type:          'causes',
    graph_score:   float,               // log1p(Σ row_weight per paper)
    paper_count:   int
}]->(Concern)                            // 민감성 계열 concern
```
- **긍정 AFFECTS와 공존**: 같은 성분에 `AFFECTS`(효능) + `CAUTION`(주의)이 동시에 존재 →
  "여드름엔 좋지만 민감성엔 주의"가 자연스러운 그래프 구조로 표현됨.

## A1 선택 규칙 (기존 claim 라우팅)
`gold_claim_all.csv`에서:
- `relation == "causes"` **AND** `evidence_direction == "supports"`
- concern이 **민감성 그룹** 중 하나: `SENSITIVE_SKIN, REDNESS, IRRITATED_SKIN, ATOPIC_PRONE, ROSACEA_PRONE, BARRIER_DAMAGE`
- **오귀인 가드**: `attribution_label ∈ {single_active, single_formulation}` (다성분 제형에서 범인 불명확한 건 제외)
- 집계: `affects`와 동일 — (ingredient, concern) 단위로 논문별 `row_weight` 최대값 합 → `log1p`, distinct pmid 수

> `causes`가 아니라 `prevents/does_not_cause/is_well_tolerated_for`는 **오히려 진정(positive)** →
> CAUTION 아님. 반드시 `causes`만.

## 페이즈
- **P0 설계** (이 문서) ✅
- **P1** `scripts/build_caution_edges.py` — 기존 gold claim → `gold/edges/caution.csv` (오프라인 검증) ← 착수
- **P2** Neo4j 로더에 `CAUTION` 관계 추가 + 그래프 적재
- **P3** 앱(4EVR0-Server) 쿼리: 민감성 요청 시 CAUTION 엣지 있는 성분 감점/제외
- **(b) A2** 부작용-지향 Bronze 수집(레티놀·AHA/BHA "irritation/rosacea" 쿼리)으로 커버리지 조밀화

## A1 커버리지 한계 (정직히)
현 코퍼스는 "긍정 효능"으로 수집돼, 뽑힌 부작용 claim은 부수적(Azelaic/Lactic acid, SLS, phenoxyethanol 등 13건).
**레티놀→자극은 이 코퍼스에 없음** → 데모 핵심은 (b) A2 소규모 타겟 수집으로 채운다.
A1은 "골격 + 근거 파이프라인 검증", A2는 "데이터 충전".
