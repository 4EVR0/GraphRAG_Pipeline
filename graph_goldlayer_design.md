# Gold Layer Refactoring & Evidence Scoring Design

## 🎯 목표

> **추천 점수가 “많이 추출된 claim”이 아니라, “많이 뒷받침된 claim”을 반영하도록 설계**
> 

---

## 🧱 전체 구조

Gold 레이어를 두 단계로 분리한다.

```
Gold Layer
├── evidence_claim (원천 근거 저장)
└── canonical_claim (그래프 및 추천용)
```

---

## 1️⃣ Evidence Claim Layer

### 목적

- 논문 문장 기반 claim을 최대한 보존
- 이후 점수 계산 및 필터링의 원천 데이터

---

### 📌 컬럼 설계

### 기본 정보

- `evidence_id`
- `batch_id`
- `pmid`
- `title`
- `journal`
- `publication_year`
- `section_type`

### 원문 정보

- `source_sentence`

### Claim 구조

- `ingredient_name`
- `relation`
- `target`
- `target_category`
- `normalized_summary`

### 매핑

- `effect_ids`
- `concern_ids`

---

### 🆕 추가 필드 (핵심)

### Canonicalization

- `canonical_claim_key`

### 품질 라벨

- `strength_label` → strong / moderate / weak
- `significance_label` → significant / not_significant / unclear / not_applicable
- `attribution_label` → single / combination / ambiguous
- `modality_label` → efficacy / prevention / safety / mechanism / formulation_support

### Dedup

- `dedup_scope_key`

### Graph 반영 여부

- `is_graph_eligible`

---

## 2️⃣ Canonical Claim Layer

### 목적

- 중복 제거
- 근거 집계
- 추천 점수 계산

---

### 📌 컬럼 설계

- `canonical_claim_key`
- `ingredient_name`
- `relation`
- `target_normalized`
- `target_category`

### 매핑

- `primary_effect_ids`
- `primary_concern_ids`

### Count

- `evidence_count_raw`
- `paper_count_distinct`
- `study_count_distinct`

### Score

- `paper_support_sum`
- `evidence_score`
- `graph_score`
- `recommendation_score_base`

### 기타

- `is_graph_eligible`

---

## 3️⃣ Canonical Claim 정의

### 🔑 Key 생성 규칙

```
canonical_claim_key =
  ingredient_normalized
  + relation_normalized
  + target_normalized
  + target_category
```

예:

```
tranexamic_acid|improves|melasma|concern
```

---

### 🔄 Relation 정규화

사용 권장:

- improves
- reduces
- prevents
- is_well_tolerated_for

---

## 4️⃣ Count 전략

### ❌ 사용 금지

- raw row count 기반 점수

---

### ✅ 사용 권장

### 1. evidence_count_raw

- 단순 row 개수
- 참고용

---

### 2. paper_count_distinct ⭐ (핵심)

```
COUNT(DISTINCT pmid)
```

---

### 3. study_count_distinct

- study_type 기준 distinct count

---

### 4. paper_support_sum ⭐

- weighted evidence 합

---

## 5️⃣ Evidence Quality 라벨링

---

### 5-1. strength_label

| 값 | 기준 |
| --- | --- |
| strong | significantly improved/reduced |
| moderate | improved/reduced |
| weak | may, suggests, promising |

---

### 5-2. significance_label

| 값 | 기준 |
| --- | --- |
| significant | 통계적 유의성 있음 |
| not_significant | 유의성 없음 |
| unclear | 불명 |
| not_applicable | 해당 없음 |

---

### 5-3. attribution_label

| 값 | 의미 |
| --- | --- |
| single | 단일 성분 |
| combination | 다성분 |
| ambiguous | 불명 |

---

### 5-4. modality_label

| 값 | 의미 |
| --- | --- |
| efficacy | 개선 효과 |
| prevention | 예방 |
| safety | 안전성 |
| mechanism | 기전 |
| formulation_support | 제형 기반 |

---

## 6️⃣ Graph 반영 기준

### `is_graph_eligible = true` 조건

- `significance_label != not_significant`
- `attribution_label == single`
- `strength_label ∈ {strong, moderate}`
- effect/concern 매핑 존재

---

## 7️⃣ Dedup 전략

---

### 1단계: exact dedup

기준:

- pmid
- source_sentence
- ingredient
- relation
- target

---

### 2단계: paper-level collapse

같은 pmid 내 동일 claim은:

```
paper_count_distinct += 1
```

---

## 8️⃣ Evidence Weight 설계

### Row weight

```
row_weight =
  strength_weight
× significance_weight
× attribution_weight
× source_type_weight
```

---

### Weight 기준

### strength_weight

- strong = 1.0
- moderate = 0.7
- weak = 0.35

### significance_weight

- significant = 1.0
- unclear = 0.7
- not_applicable = 0.6
- not_significant = 0.0~0.15

### attribution_weight

- single = 1.0
- combination = 0.45
- ambiguous = 0.25

### source_type_weight

- RCT = 1.0
- observational = 0.8
- pilot = 0.6
- review = 0.5
- in vitro = 0.45

---

## 9️⃣ Canonical Score 계산

---

### per-paper weight

같은 pmid 내:

```
paper_weight = max(row_weight)
```

---

### 전체 합

```
paper_support_sum = Σ paper_weight
```

---

### Evidence score

```
evidence_score = log(1 + paper_support_sum)
```

---

## 🔟 추천 점수 구조

```
final_score =
  relevance_score
× evidence_score
× safety_adjustment
```

---

### 구성

### relevance_score

- 사용자 concern/effect 매칭

### evidence_score

- 위에서 계산

### safety_adjustment

- tolerability 반영

---

## 11️⃣ 필수 규칙 (현 데이터 기준)

---

### A. Combination penalty

탐지 패턴:

- and
- with
- combination
- containing
- adjunctive

→ `attribution_label = combination`

---

### B. Weak language 필터

패턴:

- may
- suggests
- promising
- appear
- could

---

### C. Review penalty

- review → 낮은 weight

---

### D. Graph 제한

- strong/moderate + single만 기본 포함

---

## 12️⃣ 운영용 산출물

---

### 1. gold_evidence_audit.csv

- 모든 evidence + 라벨

---

### 2. gold_canonical_claim.csv

- 집계 결과

---

### 3. gold_excluded_claims.csv

- graph 제외 데이터

---

### 4. gold_unmapped_targets.csv

- taxonomy 개선용

---

## 🚀 구현 우선순위

1. canonical_claim_key 생성
2. dedup (exact + paper-level)
3. quality label 추가
4. row_weight 계산
5. canonical aggregation
6. evidence_score 계산
7. graph eligibility 적용
8. audit CSV 생성

---

## ✅ 핵심 원칙

### 1

중복은 제거 대상이 아니라

👉 **근거 집계 대상**

### 2

그래프는 보수적으로

👉 **강한 claim만 반영**

### 3

추천 점수는

👉 **근거의 질 × 다양성 기반**

---

## 🎯 최종 한 줄

> **“많이 나온 claim이 아니라, 강하고 독립적인 근거가 많은 claim이 높은 점수를 받게 만든다.”**
>