# GraphRAG 기반 화장품 성분 추천 파이프라인

화장품 성분별 논문 근거를 수집·정제하고, Graph RAG 추천 시스템에 활용할 수 있는 **Claim Graph 엣지**를 자동으로 생성하는 파이프라인입니다.

---

## 프로젝트 목적

단순 키워드 검색이 아닌 **논문 근거 기반 화장품 성분 추천**을 위해 다음 흐름을 구현합니다.

1. PubMed에서 타겟 성분별 논문 자동 수집 (Bronze)
2. Abstract Chunking 및 구조화 (Silver)
3. LLM 기반 Claim 추출 → 정책 기반 Graph/Recommendation 분류 (Gold)
4. Graph DB 적재 및 GraphRAG Retrieval (향후)

---

## 전체 파이프라인

```
config/target_ingredients.csv   ← 타겟 성분 28개 정의
          ↓
[Bronze]  PubMed 논문 수집
          ↓
[Silver]  Abstract Chunking
          ↓
[Gold]    LLM Claim 추출 + 정책 분류 (strict_graph / soft_graph / recommendation_only)
          ↓
[Graph]   Neo4j 적재 (예정)
          ↓
[Retrieval] GraphRAG 기반 추천 (예정)
```

---

## 디렉토리 구조

```
GraphRAG_Pipeline/
├── config/
│   └── target_ingredients.csv       # 타겟 성분 목록 (28개, 버전 관리)
│
├── db/
│   ├── schema/schema_graph_rag.sql  # PostgreSQL DDL
│   ├── seed/                        # 택소노미 초기 데이터 (effect/concern)
│   └── valid/                       # 검증 쿼리
│
├── pipeline/
│   ├── bronze/pubmed/
│   │   └── run_bronze.py            # PubMed 논문 수집 실행
│   ├── silver/paper/
│   │   └── run_silver.py            # Abstract Chunking 실행
│   ├── gold/claim/
│   │   ├── run_gold.py              # Claim 추출 · Gold 레이어 실행
│   │   └── evidence_scoring.py     # 정책 엔진 (tier / attribution / weight)
│   ├── claim/services/
│   │   ├── claim_extractor.py       # 성분 인식 · 택소노미 매핑
│   │   ├── llm_claim_extractor.py   # OpenAI API 호출 (GPT-4.1-mini)
│   │   ├── claim_validator.py       # Claim 후처리 · 필터
│   │   └── claim_filter.py          # 문장 후보 필터
│   ├── metadata/services/
│   │   ├── pubmed_client.py         # PubMed E-utilities 클라이언트
│   │   ├── pubmed_parser.py         # XML 파싱
│   │   └── query_builder.py         # PubMed 쿼리 생성
│   └── common/
│       ├── config/settings.py       # 환경 변수
│       ├── io/                      # bronze/silver/gold CSV·JSON 출력
│       ├── models/                  # 데이터 모델
│       ├── loaders/                 # 성분 CSV 로더
│       └── repositories/           # PostgreSQL 레포지토리
│
├── bronze/pubmed/batch=<id>/        # Bronze 출력 (논문 원본)
│   ├── paper_raw.csv
│   ├── search_log.csv
│   └── metadata.json
│
├── silver/paper/batch=<id>/         # Silver 출력 (청크)
│   ├── paper_chunk.csv
│   └── metadata.json
│
└── gold/claim/batch=<id>/           # Gold 출력 (클레임)
    ├── gold_claim_all.csv           # 전체 클레임 감사 로그
    ├── graph_claim.csv              # 그래프 엣지 후보 (strict/soft)
    ├── recommendation_claim.csv     # 추천 후보 (graph 포함)
    ├── gold_canonical_claim.csv     # 중복 제거 후 canonical 클레임
    ├── gold_excluded_claims.csv     # 제외된 evidence_only 클레임
    ├── gold_unmapped_targets.csv    # 택소노미 매핑 실패
    ├── claim_effect_map.csv         # 클레임 ↔ effect 매핑
    ├── claim_concern_map.csv        # 클레임 ↔ concern 매핑
    └── metadata.json                # 배치 통계
```

---

## 설치 및 환경 설정

### 1. 가상환경 생성 및 패키지 설치

```bash
python -m venv mvenv
source mvenv/bin/activate
pip install -r requirements.txt
```

### 2. `.env` 파일 생성

```env
DATABASE_URL=postgresql://user:password@localhost:5432/inci_db
NCBI_EMAIL=your_email@example.com
NCBI_API_KEY=                       # 없어도 동작 (초당 3 req 제한)
OPENAI_API_KEY=sk-...
OPENAI_MODEL=gpt-4.1-mini

TARGET_CSV_PATH=config/target_ingredients.csv
SEARCH_LIMIT=50                     # 성분당 PubMed 수집 논문 수

ENABLE_DB_UPSERT=false              # PostgreSQL 적재 여부
```

자세한 전체 항목은 `.env.example`을 참고하세요.

### 3. DB 스키마 초기화 (PostgreSQL 사용 시)

```bash
psql -U user -d inci_db -f db/schema/schema_graph_rag.sql
psql -U user -d inci_db -f db/seed/seed_effect_taxonomy.sql
psql -U user -d inci_db -f db/seed/seed_concern_taxonomy.sql
psql -U user -d inci_db -f db/seed/seed_concern_effect_map.sql
```

---

## 타겟 성분 관리

논문 검색·클레임 추출 대상 성분은 `config/target_ingredients.csv`에서 관리합니다 (`TARGET_CSV_PATH`로 경로 변경 가능).

```csv
ingredient_code,category,canonical_name,query_name,alias_list,concern_keywords,exclude_if_contains,is_target
1,barrier_hydration,Ceramide,Ceramide,Ceramide NP|...,barrier|hydration|TEWL,,true
```

| 컬럼 | 설명 |
|------|------|
| `category` | 성분 군 (barrier_hydration / brightening / acne_keratolytic / soothing / antiaging / antioxidant / peptide) |
| `canonical_name` | 성분 표준명 |
| `query_name` | PubMed 검색어 |
| `alias_list` | 동의어 (파이프 구분) |
| `concern_keywords` | 피부 고민 키워드 (쿼리 보강용) |
| `exclude_if_contains` | 이 단어 포함 시 클레임 제외 |
| `is_target` | `true`만 수집·추출 대상 |

현재 기본 목록: **28개 성분** (Ceramide, Panthenol, Hyaluronic acid, Tranexamic acid, Niacinamide, Ascorbic acid, Salicylic acid, Azelaic acid, Zinc PCA, Centella asiatica, Madecassoside, Retinol, Bakuchiol 등)

---

## 파이프라인 실행

### Step 1. Bronze — PubMed 논문 수집

```bash
python -m pipeline.bronze.pubmed.run_bronze
```

- 성분당 최대 `SEARCH_LIMIT`(기본 50)편 수집
- 출력: `bronze/pubmed/batch=<id>/paper_raw.csv`

### Step 2. Silver — Abstract Chunking

```bash
python -m pipeline.silver.paper.run_silver
```

- 최신 Bronze 배치를 자동 선택 (또는 `--bronze-batch-id` 지정)
- Abstract를 문자 단위 슬라이딩 윈도우로 chunking
- 출력: `silver/paper/batch=<id>/paper_chunk.csv`

### Step 3. Gold — Claim 추출 및 분류

```bash
python -m pipeline.gold.claim.run_gold
```

- 최신 Silver 배치를 자동 선택
- GPT-4.1-mini로 문장 단위 Claim 추출
- 정책 엔진으로 `strict_graph` / `soft_graph` / `recommendation_only` / `evidence_only` 분류
- 출력: `gold/claim/batch=<id>/` 하위 CSV 파일들

---

## Gold 정책 (Eligibility Tier)

| Tier | 설명 | Graph RAG 활용 |
|------|------|----------------|
| `strict_graph` | 단일 성분 · moderate/strong 근거 · 리스트 패턴 없음 | 그래프 엣지 (고신뢰) |
| `soft_graph` | 단일 제형/post-procedure 맥락 · moderate/strong | 그래프 엣지 (중신뢰) |
| `recommendation_only` | 복합 성분 · 병용 시술 · weak 근거 · 리뷰 논문 | 추천 보조 |
| `evidence_only` | 비유의 · 매핑 실패 · 비코스메틱 타겟 | 제외 |

**Attribution 분류 기준**

| Attribution | 의미 |
|-------------|------|
| `single_active` | 단일 성분 단독 근거 |
| `single_formulation` | 단일 성분 제형(크림/마스크 등) |
| `multi_active_combination` | 다성분 혼합 문장 |
| `procedure_adjunct_combination` | 마이크로니들링 등 시술 병용 |
| `post_procedure_recovery_formulation` | 레이저 후 회복 + 제형 맥락 |

---

## 데이터베이스 테이블

| 테이블 | 설명 |
|--------|------|
| `ingredient_master` | 성분 마스터 |
| `paper_metadata` | PubMed 논문 메타데이터 |
| `effect_taxonomy` | 효능 분류 (BARRIER_REPAIR / HYDRATING / DEPIGMENTING 등) |
| `concern_taxonomy` | 피부 고민 분류 (DRY_SKIN / HYPERPIGMENTATION / ACNE 등) |
| `concern_effect_map` | 피부 고민 ↔ 효능 매핑 |
| `extracted_claim` | 추출된 Claim |

---

## 최근 배치 결과 예시

| 단계 | 배치 | 주요 수치 |
|------|------|-----------|
| Bronze | `2026-04-04T09-36-48` | 28개 성분, 논문 539편 수집 |
| Silver | `2026-04-04T09-38-58` | 1,180 chunks / 6,625 문장 |
| Gold | `2026-04-04T09-51-56` | 클레임 64건, graph 엣지 7건 (strict 2 / soft 5), 17개 성분 |

---

## License

MIT License
