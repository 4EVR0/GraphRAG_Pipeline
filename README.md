# 📝 GraphRAG 기반 화장품 성분 논문 수집 파이프라인

본 프로젝트는 **화장품 성분 기반 GraphRAG 추천 시스템 구축을 위한 논문 데이터 파이프라인**입니다.

PubMed에서 피부과 및 화장품 관련 논문을 수집하고, 이를 구조화된 데이터로 저장하여 이후 **Chunking → Claim Extraction → Graph 구축** 단계에 활용합니다.

---

# 📌 프로젝트 목적

화장품 성분 추천 시스템에서 중요한 것은 단순한 키워드 검색이 아니라 **논문 근거 기반 추천**입니다.

이를 위해 다음과 같은 파이프라인을 구축합니다.

* 화장품 성분 기반 논문 수집
* 논문 메타데이터 구조화
* 논문 텍스트 chunking
* LLM 기반 claim 추출
* Neo4j 그래프 구축
* GraphRAG 기반 검색 및 추천

---

# 🛠️ 전체 파이프라인

```text
KCIA 성분 사전
        ↓
CosIng 매핑
        ↓
Target Ingredient 선정
        ↓
PubMed 논문 검색
        ↓
paper_metadata (PostgreSQL)
        ↓
paper_chunk 생성
        ↓
LLM Claim Extraction
        ↓
Graph DB (Neo4j)
        ↓
GraphRAG Retrieval
```

---

# 📁 프로젝트 디렉토리 구조

```text
pipeline/
├─ ingest_pubmed_metadata.py      # 파이프라인 실행 진입점
├─ services/
│  ├─ pubmed_client.py            # PubMed API 호출
│  ├─ pubmed_parser.py            # PubMed XML 파싱
│  └─ query_builder.py            # 논문 검색 쿼리 생성
├─ repositories/
│  └─ paper_repository.py         # PostgreSQL 저장 로직
├─ loaders/
│  └─ ingredient_loader.py        # 대상 성분 CSV 로드
├─ models/
│  └─ paper_record.py             # 논문 데이터 모델
└─ config/
   └─ settings.py                 # 환경변수 설정
```

---

# 데이터베이스 구조

현재 파이프라인에서 사용하는 주요 테이블은 다음과 같습니다.

| 테이블                | 설명                    |
| ------------------ | --------------------- |
| paper_metadata     | PubMed에서 수집한 논문 메타데이터 |
| effect_taxonomy    | 화장품 효능 분류             |
| concern_taxonomy   | 피부 고민 분류              |
| concern_effect_map | 피부 고민 ↔ 효능 매핑         |

---

# 🔐 환경 설정

### 1. 가상환경 생성

```bash
python -m venv mvenv
source mvenv/bin/activate
```

### 2. 패키지 설치

```bash
pip install -r requirements.txt
```

---

# 환경 변수 설정

프로젝트 루트에 `.env` 파일을 생성합니다.

예시:

```env
DATABASE_URL=postgresql://user:password@localhost:5432/inci_db
NCBI_EMAIL=your_email@example.com
NCBI_TOOL=graph_rag_pipeline
SEARCH_LIMIT=20
```

자세한 예시는 `.env.example` 파일을 참고하세요.

---

# 입력 데이터

논문 검색 대상 성분은 다음 CSV 파일에서 관리됩니다.

```text
data/target_ingredients.csv
```

예시:

```csv
ingredient_code,canonical_name,query_name,alias_list,concern_keywords,is_target
1,Niacinamide,Niacinamide,"Nicotinamide","acne|barrier|hyperpigmentation",true
```

| 컬럼               | 설명           |
| ---------------- | ------------ |
| canonical_name   | 성분 표준명       |
| alias_list       | 성분 동의어       |
| concern_keywords | 관련 피부 고민 키워드 |
| is_target        | 논문 수집 대상 여부  |

---

# 파이프라인 실행

논문 메타데이터 수집 실행:

```bash
python -m pipeline.ingest_pubmed_metadata
```

예시 로그:

```text
[INFO] Target: Niacinamide
[INFO] Found 20 PMIDs
[INFO] Upserted 20 records into paper_metadata
```

---

# PubMed API 사용

본 프로젝트는 PubMed E-utilities API를 사용합니다.

* API 식별을 위해 이메일을 전달해야 합니다.
* API key 없이 초당 약 3 request 사용 가능합니다.

---

# 향후 개발 계획

다음 단계로 아래 기능을 구현할 예정입니다.

### 1. 논문 Chunk 생성

논문 abstract를 문장 단위로 분리하여 `paper_chunk` 테이블 생성

### 2. Claim Extraction

LLM을 활용하여 논문에서 성분 효능 claim 추출

### 3. Graph 구축

성분 ↔ 효능 ↔ 논문 근거 관계를 Neo4j에 저장

### 4. GraphRAG Retrieval

사용자 피부 고민 기반 추천 시스템 구축

---

# License

MIT License
