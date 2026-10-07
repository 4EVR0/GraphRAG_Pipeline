#!/usr/bin/env python3
"""
S3 + 로컬 시드 데이터로 Neo4j 임포트용 Gold CSV 파일을 생성합니다.

출력:
  gold/nodes/ingredient.csv  — gold_product_ingredients(parquet) + kcia_cosing(CSV) 조인
  gold/nodes/effect.csv      — db/seed/seed_effect_taxonomy.sql
  gold/nodes/concern.csv     — db/seed/seed_concern_taxonomy.sql
  gold/nodes/product.csv     — 헤더만 (product 데이터 미포함)
  gold/edges/affects.csv     — gold_canonical_claim + claim_effect_map 조인
  gold/edges/relates_to.csv  — db/seed/seed_concern_effect_map.sql
  gold/edges/contains.csv    — 헤더만 (product-ingredient 매핑 미포함)

사용법:
  cd /path/to/GraphRAG_Pipieline
  python scripts/build_gold_csvs.py
  python scripts/build_gold_csvs.py --bucket my-other-bucket
"""

import argparse
import csv
import datetime
import io
import math
import re
import sys
from pathlib import Path

import boto3
import pandas as pd
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from pipeline.claim.services.claim_extractor import extractor
from pipeline.gold.claim.evidence_scoring import compute_eligibility_tier

GOLD_NODES = ROOT / "gold" / "nodes"
GOLD_EDGES = ROOT / "gold" / "edges"
SEED_DIR = ROOT / "db" / "seed"
CLAIM_BATCH_ROOT = ROOT / "gold" / "claim"

S3_BUCKET = "oliveyoung-crawl-data"
S3_PARQUET_PREFIX = "olive_young_gold/gold_product_ingredients/data/"
S3_INCI_PREFIX = "INCI_data_gold/kcia_cosing/"
S3_GOLD_PREFIX = "graph_gold_csvs/"
INCI_FILENAME = "kcia_cosing_gold_ingredients.csv"

# 대한화장품협회 성분사전 1718과 15424는 서로 다른 INCI 명칭이다.
# 원천 CSV가 두 성분을 혼동해도 그래프 노드와 Gold claim 별칭을 오염시키지 않는다.
# https://kcia.or.kr/cid/search/ingd_view.php?no=1718
# https://kcia.or.kr/cid/search/ingd_view.php?no=15424
_VERIFIED_KOREAN_NAMES = {
    "ACETYL HEXAPEPTIDE-8": "아세틸헥사펩타이드-8",
}


def correct_verified_ingredient_names(inci_df: pd.DataFrame) -> pd.DataFrame:
    """공식 INCI-한글명 쌍이 어긋난 알려진 원천 행을 교정한다."""
    corrected = inci_df.copy()
    for inci_name, kor_name in _VERIFIED_KOREAN_NAMES.items():
        mask = corrected["inci_name"].astype(str).str.strip().str.upper() == inci_name
        if mask.any():
            incorrect = mask & (corrected["kor_name"].astype(str).str.strip() != kor_name)
            if incorrect.any():
                print(f"[INCI] {inci_name} 한글명 교정: {int(incorrect.sum())}행")
            corrected.loc[mask, "kor_name"] = kor_name
    return corrected


# ---------------------------------------------------------------------------
# S3 헬퍼
# ---------------------------------------------------------------------------

def _s3_client():
    return boto3.client("s3")


def _latest_inci_prefix(s3, bucket: str) -> str:
    """batch_job= 형식 중 가장 최신 prefix를 반환합니다."""
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=S3_INCI_PREFIX, Delimiter="/")
    prefixes = [p["Prefix"] for p in resp.get("CommonPrefixes", [])]
    batch_job_prefixes = [p for p in prefixes if "/batch_job=" in p]
    if not batch_job_prefixes:
        sys.exit(f"[ERROR] {S3_INCI_PREFIX} 하위에 batch_job= 경로가 없습니다.")
    latest = sorted(batch_job_prefixes)[-1]
    print(f"[S3] INCI 최신 배치: {latest}")
    return latest


def load_parquet_from_s3(bucket: str) -> pd.DataFrame:
    """S3에서 모든 product_ingredients parquet을 내려받아 최신 batch_job만 반환합니다."""
    s3 = _s3_client()
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=S3_PARQUET_PREFIX)
    objects = [o for o in resp.get("Contents", []) if o["Key"].endswith(".parquet")]
    if not objects:
        sys.exit(f"[ERROR] s3://{bucket}/{S3_PARQUET_PREFIX} 에 parquet 파일이 없습니다.")

    print(f"[S3] parquet {len(objects)}개 다운로드 중...")
    frames = []
    for obj in objects:
        buf = io.BytesIO()
        s3.download_fileobj(bucket, obj["Key"], buf)
        buf.seek(0)
        frames.append(pq.read_table(buf).to_pandas())

    df = pd.concat(frames, ignore_index=True)
    latest_batch = df["batch_job"].max()
    df = df[df["batch_job"] == latest_batch].copy()
    df = df.drop_duplicates(subset="inci_name")
    print(f"[S3] parquet 최신 batch={latest_batch}, 고유 inci_name={len(df)}개")
    return df


def load_inci_csv_from_s3(bucket: str) -> pd.DataFrame:
    """S3에서 최신 kcia_cosing CSV를 내려받아 반환합니다."""
    s3 = _s3_client()
    prefix = _latest_inci_prefix(s3, bucket)
    key = f"{prefix}{INCI_FILENAME}"
    buf = io.BytesIO()
    print(f"[S3] INCI CSV 다운로드: {key}")
    s3.download_fileobj(bucket, key, buf)
    buf.seek(0)
    df = pd.read_csv(buf)
    # inci_name 중복 시 ingredient_code 높은 것(최신) 우선
    df = df.sort_values("ingredient_code", ascending=False).drop_duplicates("inci_name")
    print(f"[S3] INCI 고유 inci_name={len(df)}개")
    return correct_verified_ingredient_names(df)


def load_existing_graph_csv(bucket: str, relative_key: str) -> pd.DataFrame:
    """최신 운영 graph batch의 CSV 하나를 로드합니다."""
    s3 = _s3_client()
    response = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=S3_GOLD_PREFIX,
        Delimiter="/",
    )
    prefixes = sorted(
        item["Prefix"]
        for item in response.get("CommonPrefixes", [])
        if "/batch_job=" in item["Prefix"]
    )
    if not prefixes:
        return pd.DataFrame()

    key = f"{prefixes[-1]}{relative_key}"
    buffer = io.BytesIO()
    try:
        s3.download_fileobj(bucket, key, buffer)
    except ClientError:
        return pd.DataFrame()
    buffer.seek(0)
    legacy = pd.read_csv(buffer, encoding="utf-8-sig")
    print(f"[S3] 기존 graph CSV: {key} ({len(legacy)}행)")
    return legacy


# ---------------------------------------------------------------------------
# SQL 시드 파싱
# ---------------------------------------------------------------------------

def parse_effect_taxonomy() -> list[dict]:
    sql = (SEED_DIR / "seed_effect_taxonomy.sql").read_text()
    rows = re.findall(
        r"\('([A-Z_]+)',\s*'([^']+)',\s*'([^']+)'",
        sql,
    )
    return [{"effect_code": r[0], "effect_name_en": r[1]} for r in rows]


def parse_concern_taxonomy() -> list[dict]:
    sql = (SEED_DIR / "seed_concern_taxonomy.sql").read_text()
    rows = re.findall(
        r"\('([A-Z_]+)',\s*'([^']+)',\s*'([^']+)'",
        sql,
    )
    return [{"concern_code": r[0], "concern_name_ko": r[2]} for r in rows]


def parse_concern_effect_map() -> list[tuple[str, str]]:
    """(effect_code, concern_code) 쌍 목록을 반환합니다."""
    sql = (SEED_DIR / "seed_concern_effect_map.sql").read_text()
    pairs: list[tuple[str, str]] = []
    for concern, effects_raw in re.findall(
        r"concern_code='(\w+)' AND e\.effect_code IN \(([^)]+)\)", sql
    ):
        for e in effects_raw.split(","):
            effect_code = e.strip().strip("'")
            pairs.append((effect_code, concern))
    return pairs


# ---------------------------------------------------------------------------
# target_ingredients.csv 생성
# ---------------------------------------------------------------------------

# COSING 함수 → PubMed 키워드 (concern_keywords)
_FUNC_KEYWORDS: dict[str, str] = {
    "SKIN CONDITIONING":  "barrier|hydration|moisturizing|dry skin|skin barrier",
    "HUMECTANT":          "hydration|moisturizing|TEWL|water retention",
    "EMOLLIENT":          "emollient|skin barrier|softening|dry skin",
    "MOISTURISING":       "hydration|moisturizing|dry skin|TEWL",
    "ANTIOXIDANT":        "anti-aging|oxidative stress|photoaging|free radical",
    "ANTIMICROBIAL":      "acne|bacteria|antimicrobial|inflammation",
    "ANTI-SEBUM":         "sebum|oiliness|oily skin|oil control|acne|pore|pores",
    "SKIN PROTECTING":    "barrier|skin protection|soothing|irritation",
    "SOOTHING":           "soothing|calming|irritation|sensitive skin|erythema",
    "TONIC":              "toning|skin tone|pore",
    "ASTRINGENT":         "pore|pores|toning|astringent|sebum|oiliness",
    "EXFOLIANT":          "exfoliation|keratolytic|skin renewal|desquamation|texture|roughness",
    "KERATOLYTIC":        "exfoliation|keratolytic|skin renewal|desquamation|texture|roughness",
    "UV-FILTER":          "UV|photoprotection|sun protection|photoaging",
    "ANTIDANDRUFF":       "dandruff|scalp|seborrheic",
    "HAIR CONDITIONING":  "hair conditioning|hair repair|hair damage",
    "SURFACTANT":         "cleansing|surfactant|foam",
    "CLEANSING":          "cleansing|pore|impurity",
    "SMOOTHING":          "skin texture|smoothing|roughness|pore|skin tone",
    "ANTI-AGEING":        "anti-aging|wrinkle|elasticity|photoaging",
    "SKIN BRIGHTENING":   "brightening|hyperpigmentation|skin tone|melanin|dullness|uneven skin tone",
    "DEPIGMENTING":       "brightening|hyperpigmentation|melasma|pigmentation|melanin|dullness|uneven skin tone",
    "WOUND HEALING":      "wound healing|repair|barrier|recovery",
}

# COSING 함수 → 카테고리
_FUNC_CATEGORY: dict[str, str] = {
    "SKIN CONDITIONING": "barrier_hydration", "HUMECTANT": "barrier_hydration",
    "EMOLLIENT":         "barrier_hydration", "MOISTURISING": "barrier_hydration",
    "SMOOTHING":         "barrier_hydration",
    "ANTIOXIDANT":       "anti_aging",        "UV-FILTER":    "uv_protection",
    "ANTI-AGEING":       "anti_aging",        "WOUND HEALING": "anti_aging",
    "ANTIMICROBIAL":     "acne_control",      "ANTI-SEBUM":   "acne_control",
    "SOOTHING":          "soothing",          "SKIN PROTECTING": "soothing",
    "EXFOLIANT":         "exfoliation",       "KERATOLYTIC":  "exfoliation",
    "ASTRINGENT":        "pore_control",      "TONIC":        "pore_control",
    "ANTIDANDRUFF":      "scalp",             "HAIR CONDITIONING": "hair",
    "SURFACTANT":        "cleansing",         "CLEANSING":    "cleansing",
    "SKIN BRIGHTENING":  "brightening",       "DEPIGMENTING": "brightening",
}

# 피부 활성이 없는 순수 기능성 COSING 함수 (이것만 있으면 제외)
_INACTIVE_FUNCS: frozenset[str] = frozenset({
    "SOLVENT", "BUFFERING", "EMULSIFYING", "VISCOSITY CONTROLLING",
    "PRESERVATIVE", "CHELATING", "COLORANT", "PERFUMING", "MASKING",
    "FILM FORMING", "BINDING", "ABRASIVE", "CARRIER", "FRAGRANCE",
    "DENATURANT", "OPACIFYING", "ORAL CARE", "NAIL CONDITIONING",
    "HAIR DYEING", "OXIDISING",
})


def _normalize_func(raw: str) -> str:
    """'SKIN CONDITIONING - HUMECTANT' → 'SKIN CONDITIONING' 정규화."""
    return raw.split(" - ")[0].strip()


def build_target_ingredients(prod_df: pd.DataFrame, inci_df: pd.DataFrame) -> list[dict]:
    """
    product_ingredients parquet + INCI CSV DataFrame → target_ingredients.csv 행 목록.

    - INCI CSV의 공식 cosing_functions 우선 사용 (parquet은 폴백)
    - COSING 함수명 정규화 (` - ` 접미사 제거)
    - 피부 활성 없는 순수 기능성 성분 필터링
    """
    # INCI CSV에서 inci_name → cosing_functions 조회 테이블 구축
    inci_func_map = (
        inci_df[["inci_name", "cosing_functions"]]
        .dropna(subset=["inci_name"])
        .drop_duplicates("inci_name")
        .set_index("inci_name")["cosing_functions"]
        .to_dict()
    )

    rows: list[dict] = []
    skipped = 0

    for i, row in prod_df.iterrows():
        # INCI CSV 공식 함수 우선, 없으면 parquet 함수 사용
        inci_name = str(row.get("inci_name") or "")
        raw_funcs = str(inci_func_map.get(inci_name) or row.get("cosing_functions") or "")
        funcs = [_normalize_func(f.strip().upper()) for f in raw_funcs.split(";") if f.strip()]

        # 순수 기능성 성분 제외
        if funcs and all(f in _INACTIVE_FUNCS for f in funcs):
            skipped += 1
            continue

        category = next((_FUNC_CATEGORY[f] for f in funcs if f in _FUNC_CATEGORY), "other")

        seen: set[str] = set()
        kws: list[str] = []
        for f in funcs:
            for kw in _FUNC_KEYWORDS.get(f, "").split("|"):
                if kw and kw.lower() not in seen:
                    seen.add(kw.lower())
                    kws.append(kw)

        eng = str(row.get("eng_name") or "").strip()
        kor = str(row.get("kor_name") or "").strip()
        inci = str(row.get("inci_name") or "").strip()
        # Use English name as canonical so attribution matching works on English PubMed text
        canonical_name = eng if eng and eng.lower() != "nan" else inci.title()
        query_name = canonical_name

        alias_parts: list[str] = []
        for v in [inci, kor]:
            v = str(v or "").strip()
            if v and v.lower() != "nan" and v.lower() != canonical_name.lower() and v not in alias_parts:
                alias_parts.append(v)

        rows.append({
            "ingredient_code": len(rows) + 1,
            "category":         category,
            "canonical_name":   canonical_name,
            "query_name":       query_name,
            "alias_list":       "|".join(alias_parts),
            "concern_keywords": "|".join(kws),
            "exclude_if_contains": "",
            "is_target":        "true",
        })

    print(f"[target] 포함: {len(rows)}개, 기능성 제외: {skipped}개")
    return rows


# ---------------------------------------------------------------------------
# COSING 함수 → Effect 매핑 (soft 엣지용)
# ---------------------------------------------------------------------------

# COSING 함수 → (effect_code 목록, relation)
_COSING_FUNC_TO_EFFECTS: dict[str, tuple[list[str], str]] = {
    "SKIN CONDITIONING": (["HYDRATING", "BARRIER_REPAIR"],          "improves"),
    "HUMECTANT":         (["HYDRATING", "MOISTURE_RETENTION"],      "improves"),
    "EMOLLIENT":         (["HYDRATING", "BARRIER_REPAIR"],          "improves"),
    "MOISTURISING":      (["HYDRATING", "MOISTURE_RETENTION"],      "improves"),
    "SMOOTHING":         (["BARRIER_REPAIR"],                       "improves"),
    "SKIN PROTECTING":   (["BARRIER_REPAIR", "SOOTHING"],           "improves"),
    "SOOTHING":          (["SOOTHING", "ANTI_INFLAMMATORY"],        "reduces"),
    "ANTI-INFLAMMATORY": (["ANTI_INFLAMMATORY", "SOOTHING"],        "reduces"),
    "ANTIOXIDANT":       (["ANTIOXIDANT", "ANTI_AGING"],            "improves"),
    "ANTI-AGEING":       (["ANTI_AGING", "ANTIOXIDANT"],            "improves"),
    "WOUND HEALING":     (["WOUND_HEALING", "BARRIER_REPAIR"],      "improves"),
    "ANTI-SEBUM":        (["SEBUM_REGULATION"],                     "reduces"),
    "ASTRINGENT":        (["SEBUM_REGULATION"],                     "reduces"),
    "TONIC":             (["SEBUM_REGULATION"],                     "reduces"),
    "ANTIMICROBIAL":     (["ANTIMICROBIAL"],                        "reduces"),
    "EXFOLIANT":         (["KERATOLYTIC"],                          "improves"),
    "KERATOLYTIC":       (["KERATOLYTIC"],                          "improves"),
    "DEPIGMENTING":      (["DEPIGMENTING", "BRIGHTENING"],          "improves"),
    "SKIN BRIGHTENING":  (["BRIGHTENING", "DEPIGMENTING"],          "improves"),
    "UV-FILTER":         (["PHOTOPROTECTIVE"],                      "improves"),
}


# COSING 함수가 효능을 얼마나 "직접" 지목하는지(구체성) 기반 신뢰도.
# ⚠️ 측정된 근거(논문 수)가 아니라 큐레이션 휴리스틱이다 — CosIng function은 규제/표기 카테고리이지
#    입증된 효능이 아니므로, pubmed 점수보다 낮은 밴드(<= 0.15)를 유지해 논문 근거를 앞지르지 못하게 한다.
#    (evidence tier: regulatory-function < pubmed. 최종 tier 우위는 쿼리의 has_pubmed 정렬로도 보강)
#    값은 초기 휴리스틱이며 eval(grounding) 측정으로 보정한다.
_COSING_FUNC_CONFIDENCE: dict[str, float] = {
    # 직접·1:1 — 함수 용어가 효능을 그대로 지목
    "ANTI-SEBUM": 0.15, "KERATOLYTIC": 0.15, "EXFOLIANT": 0.15,
    "ANTIMICROBIAL": 0.15, "ANTIOXIDANT": 0.15, "DEPIGMENTING": 0.15,
    "ANTI-INFLAMMATORY": 0.15, "WOUND HEALING": 0.15, "UV-FILTER": 0.15,
    "SKIN BRIGHTENING": 0.12,
    # 꽤 직접
    "ASTRINGENT": 0.10, "HUMECTANT": 0.10, "MOISTURISING": 0.10, "SOOTHING": 0.10,
    "TONIC": 0.06,
    # generic·1:다 (거의 모든 성분에 붙는 표기) — 낮음
    "ANTI-AGEING": 0.05, "SKIN CONDITIONING": 0.03, "EMOLLIENT": 0.03,
    "SMOOTHING": 0.03, "SKIN PROTECTING": 0.03,
}
_COSING_DEFAULT_CONFIDENCE = 0.03   # 맵에 없는 함수의 보수적 기본값
_COSING_SECONDARY_FACTOR = 0.6      # 매핑의 2번째 이후(부차) 효능은 감점


def build_cosing_soft_edges(
    prod_df: pd.DataFrame,
    inci_df: pd.DataFrame,
    pubmed_seen: set[tuple],
    valid_effects: set[str],
) -> list[dict]:
    """COSING 함수 기반 soft 엣지 생성. pubmed 엣지와 중복은 추가하지 않음."""
    # inci_name → cosing_functions 조회 (INCI CSV 우선)
    inci_func_map = (
        inci_df[["inci_name", "cosing_functions"]]
        .dropna(subset=["inci_name", "cosing_functions"])
        .drop_duplicates("inci_name")
        .set_index("inci_name")["cosing_functions"]
        .to_dict()
    )
    # parquet에서 보완
    for _, row in prod_df.iterrows():
        iname = str(row.get("inci_name") or "")
        if iname and iname not in inci_func_map and pd.notna(row.get("cosing_functions")):
            inci_func_map[iname] = str(row["cosing_functions"])

    rows: list[dict] = []
    seen: set[tuple] = set()
    skipped_pubmed = 0

    product_ingredients = {
        str(value)
        for value in prod_df["inci_name"].dropna().tolist()
        if str(value)
    }
    for inci_name in sorted(product_ingredients):
        funcs_raw = inci_func_map.get(inci_name)
        if not funcs_raw or pd.isna(funcs_raw):
            continue
        funcs = [_normalize_func(f.strip().upper()) for f in str(funcs_raw).split(";") if f.strip()]
        for func in funcs:
            mapping = _COSING_FUNC_TO_EFFECTS.get(func)
            if not mapping:
                continue
            effect_codes, relation = mapping
            confidence = _COSING_FUNC_CONFIDENCE.get(func, _COSING_DEFAULT_CONFIDENCE)
            for idx, effect_code in enumerate(effect_codes):
                if effect_code not in valid_effects:
                    continue
                key = (inci_name, effect_code, relation)
                if key in pubmed_seen:
                    skipped_pubmed += 1
                    continue
                if key in seen:
                    continue
                seen.add(key)
                # 첫(주) 효능은 full confidence, 2번째 이후(부차)는 감점 → 매핑 구체성 반영
                score = confidence if idx == 0 else round(confidence * _COSING_SECONDARY_FACTOR, 6)
                rows.append({
                    ":START_ID(Ingredient)": inci_name,
                    ":END_ID(Effect)":       effect_code,
                    "type":                  relation,
                    "evidence_type":         "cosing_function",
                    "graph_score:float":     score,
                    "paper_count:int":       0,
                })

    print(f"[cosing] soft 엣지 {len(rows)}개 생성 (pubmed 중복 {skipped_pubmed}개 제외)")
    return rows


# ---------------------------------------------------------------------------
# Gold claim 데이터 로드
# ---------------------------------------------------------------------------

def _all_claim_batches(
    since: str | None = None,
    claim_batch_id: str | None = None,
) -> list[Path]:
    """since: 'YYYY-MM-DD' 형식. 해당 날짜 이후 배치만 반환."""
    if since and claim_batch_id:
        raise ValueError("--since and --claim-batch-id cannot be used together.")
    if claim_batch_id:
        batch = CLAIM_BATCH_ROOT / f"batch={claim_batch_id}"
        if not batch.is_dir():
            sys.exit(f"[ERROR] Gold claim batch not found: {batch}")
        print(f"[filter] claim batch: {claim_batch_id}")
        return [batch]

    batches = sorted(CLAIM_BATCH_ROOT.glob("batch=*"))
    if not batches:
        sys.exit(f"[ERROR] {CLAIM_BATCH_ROOT} 에 batch 디렉토리가 없습니다.")
    if since:
        prefix = f"batch={since}"
        batches = [b for b in batches if b.name >= prefix]
        if not batches:
            sys.exit(f"[ERROR] --since {since} 이후 배치가 없습니다.")
        print(f"[filter] --since {since}: {len(batches)}개 배치 사용")
    return batches


# 내약성·안전성 관찰은 효능 근거가 아니다. "잘 견딘다"가 각질·진정 효능 엣지로
# 승격되지 않게 AFFECTS 집계에서 제외한다(#41, #49).
_NON_EFFICACY_RELATIONS = frozenset({
    "is_well_tolerated_for", "is_safe_for", "does_not_cause", "causes",
})
_NON_EFFICACY_TARGET = re.compile(r"tolera|safety|side effect|adverse", re.IGNORECASE)
# 같은 성분·효능에 관계가 여럿이면 엣지 type은 가장 강한 근거의 관계로 정하고,
# 근거 강도가 같으면 이 순서를 따른다.
_RELATION_PRIORITY = ("improves", "reduces", "prevents", "regulates", "inhibits",
                      "modulates", "stimulates", "increases")


def _target_effect_codes(
    target: str,
    relation: str,
    effect_rows: list[dict],
) -> set[str] | None:
    """claim target만으로 매핑되는 효능. target이 없으면 None(기존 매핑 유지)."""
    if not target:
        return None
    ids = extractor.extract_effect_ids(target, relation, effect_rows)
    by_id = {row["effect_id"]: row["effect_code"] for row in effect_rows}
    return {by_id[i] for i in ids}


# 여드름 결과(target이 acne·병변) claim은 기전 효능 동의어에 걸리지 않는다(#49).
# 사람 대상 여드름 연구만 인정하고, 병변 유형이 명시되면 기전 효능, 아니면
# 결과 효능 BLEMISH_CARE로 연결한다.
_ACNE_TARGET = re.compile(r"\bacne\b|\blesions?\b|blemish|pimple|breakout", re.IGNORECASE)
_ACNE_MENTION = re.compile(r"\bacne\b", re.IGNORECASE)
_NOT_HUMAN_ACNE = re.compile(
    r"rosacea|atopic|dermatitis|\bmice\b|\bmouse\b|\brats?\b|\bmurine\b|sebocyte|"
    r"keratinocyte|in vitro|cell line|ribotype|culture|\bmedium\b",
    re.IGNORECASE,
)
_HUMAN_ACNE_CONTEXTS = frozenset({"unknown", ""})
_LESION_TYPE = re.compile(
    r"\b(non[- ]?)?inflam(?:ed|matory)\s+(?:acne\s+)?lesions?|\b(papules?|pustules?)\b|"
    r"\b(comedon\w*|comedo|blackheads?|whiteheads?)\b",
    re.IGNORECASE,
)


def _acne_outcome_effects(target: str, sentence: str, title: str, study_context: str) -> set[str] | None:
    """여드름 결과 claim의 효능. 여드름 결과가 아니면 None, 인정 범위 밖이면 빈 집합."""
    text = f"{sentence} {title}"
    if not _ACNE_TARGET.search(target) or not (
        _ACNE_MENTION.search(target) or _ACNE_MENTION.search(text)
    ):
        return None
    context = study_context.strip().lower()
    if not (context.startswith("human") or context in _HUMAN_ACNE_CONTEXTS):
        return set()
    if _NOT_HUMAN_ACNE.search(text) or _NOT_HUMAN_ACNE.search(target):
        return set()
    effects: set[str] = set()
    for match in _LESION_TYPE.finditer(text):
        non_inflamed, _, comedonal = match.groups()
        effects.add("COMEDOLYTIC" if comedonal or non_inflamed else "ANTI_INFLAMMATORY")
    return effects or {"BLEMISH_CARE"}


def load_affects_rows(
    effect_id_to_code: dict[int, str],
    inci_lookup: dict[str, str],
    since: str | None = None,
    claim_batch_id: str | None = None,
    evaluated_ingredients: set[str] | None = None,
) -> list[dict]:
    """Graph-eligible evidence를 ingredient/effect 단위로 집계합니다.

    evaluated_ingredients가 주어지면 현재 claim 배치에서 재평가된 INCI를 채운다.
    """
    all_batches = _all_claim_batches(
        since=since,
        claim_batch_id=claim_batch_id,
    )
    evidence_frames = []
    for batch_dir in all_batches:
        f = batch_dir / "gold_claim_all.csv"
        if f.exists() and f.stat().st_size > 5:
            try:
                df = pd.read_csv(f, encoding="utf-8-sig")
                if len(df) > 0:
                    evidence_frames.append(df)
            except Exception:
                pass
    if not evidence_frames:
        sys.exit("[ERROR] gold_claim_all.csv 데이터가 없습니다.")
    evidence = pd.concat(evidence_frames, ignore_index=True)

    # 성분명이 아닌 값 제거 (제형명·일반명 오검출)
    non_ingredient_names = {
        "cream", "water", "lotion", "serum", "gel", "foam", "oil", "emulsion",
        "크림", "빙하수", "멜라닌",
    }
    evidence = evidence[
        ~evidence["ingredient_name"].str.lower().isin(non_ingredient_names)
    ].copy()

    def pipe_values(value: object) -> list[str]:
        if value is None or pd.isna(value):
            return []
        return [
            part.strip()
            for part in str(value).split("|")
            if part.strip() and part.strip().lower() != "nan"
        ]

    def current_tier(row: pd.Series) -> str:
        return compute_eligibility_tier(
            str(row.get("strength_label", "")),
            str(row.get("significance_label", "")),
            str(row.get("attribution_label", "")),
            str(row.get("claim_type", "")),
            [int(float(value)) for value in pipe_values(row.get("effect_ids"))],
            [int(float(value)) for value in pipe_values(row.get("concern_ids"))],
            sentence=str(row.get("source_sentence", "")),
            title=str(row.get("title", "")),
            study_context=str(row.get("study_context", "")),
            detected_labels=pipe_values(row.get("all_detected_ingredients")),
        )

    if evaluated_ingredients is not None:
        evaluated_ingredients.update(
            inci_lookup[name.lower()]
            for name in evidence["ingredient_name"].dropna().astype(str)
            if name.lower() in inci_lookup
        )

    evidence["current_eligibility_tier"] = evidence.apply(current_tier, axis=1)
    eligible = evidence[
        evidence["current_eligibility_tier"].isin(["strict_graph", "soft_graph"])
    ].copy()
    if "ingredient_detection_suspect" in eligible.columns:
        suspect = eligible["ingredient_detection_suspect"].astype(str).str.strip().str.lower().isin({"true", "1"})
        print(f"[claim] 성분 검출 의심 근거 제외: {int(suspect.sum())}행")
        eligible = eligible[~suspect].copy()
    print(
        f"[claim] 전체 배치={len(all_batches)}, evidence={len(evidence)}, "
        f"graph_eligible={len(eligible)}"
    )

    # canonical claim 수준의 effect union을 사용하면 한 논문의 부차 effect가
    # 같은 target을 공유하는 모든 논문의 누적 점수를 받는다. Evidence 행의
    # 실제 effect_ids로 그룹화해 effect 간 점수 누수를 막는다.
    # effect_ids는 원문 문장 전체로 매핑돼 한 문장의 다른 결과(보습·장벽 등)까지
    # 들어 있으므로, claim target으로도 매핑되는 효능만 남긴다.
    effect_names = {r["effect_code"]: r["effect_name_en"] for r in parse_effect_taxonomy()}
    effect_rows = [
        {"effect_id": eid, "effect_code": code, "effect_name_en": effect_names.get(code, code)}
        for eid, code in effect_id_to_code.items()
    ]
    support_by_edge: dict[tuple[str, str], dict[str, float]] = {}
    relation_support: dict[tuple[str, str], dict[str, float]] = {}
    unmapped_ingredients: set[str] = set()
    excluded_inci = {"CREAM", "WATER", "MELANIN"}
    # effect_id_to_code는 claim_effect_map에서 오므로 BLEMISH_CARE처럼 claim 매핑이
    # 없던 효능이 빠져 있다. 여드름 결과 효능은 seed 분류 기준으로 확인한다.
    known_effect_codes = set(effect_names) | set(effect_id_to_code.values())
    dropped = {"non_efficacy": 0, "effect_not_in_target": 0, "acne_outcome": 0, "acne_not_human": 0}

    for _, claim in eligible.iterrows():
        ingredient_name = str(claim["ingredient_name"])
        relation = str(claim["relation"])
        effect_ids_raw = str(claim.get("effect_ids", ""))
        target = "" if pd.isna(claim.get("target")) else str(claim.get("target", "")).strip()
        if (
            relation in _NON_EFFICACY_RELATIONS
            or str(claim.get("claim_type", "")).strip().lower() == "safety"
            or _NON_EFFICACY_TARGET.search(target)
        ):
            dropped["non_efficacy"] += 1
            continue
        target_effects = _target_effect_codes(target, relation, effect_rows)

        inci_name = inci_lookup.get(ingredient_name.lower())
        if not inci_name:
            unmapped_ingredients.add(ingredient_name)
            continue
        if inci_name.upper() in excluded_inci:
            continue

        pmid = str(claim["pmid"])
        row_weight = float(claim.get("row_weight", 0.0) or 0.0)

        acne_effects = _acne_outcome_effects(
            target,
            "" if pd.isna(claim.get("source_sentence")) else str(claim.get("source_sentence")),
            "" if pd.isna(claim.get("title")) else str(claim.get("title")),
            "" if pd.isna(claim.get("study_context")) else str(claim.get("study_context")),
        )
        if acne_effects is not None:
            dropped["acne_outcome"] += 1
            if not acne_effects:
                dropped["acne_not_human"] += 1
            effect_codes = sorted(code for code in acne_effects if code in known_effect_codes)
        else:
            effect_codes = []
            for eid_str in effect_ids_raw.split("|"):
                eid_str = eid_str.strip()
                if not eid_str or eid_str == "nan":
                    continue
                try:
                    eid = int(float(eid_str))
                except ValueError:
                    continue
                effect_code = effect_id_to_code.get(eid)
                if not effect_code:
                    print(f"[WARN] effect_id={eid} 를 effect_code로 변환할 수 없습니다.")
                    continue
                effect_codes.append(effect_code)

        for effect_code in effect_codes:
            # 피지 증가 관찰은 SEBUM_REGULATION 추천 효능으로 노출하지 않는다.
            if effect_code == "SEBUM_REGULATION" and relation == "increases":
                continue
            if (
                acne_effects is None
                and target_effects is not None
                and effect_code not in target_effects
            ):
                dropped["effect_not_in_target"] += 1
                continue
            # 관계(improves/reduces/regulates)별로 엣지를 나누면 같은 논문이
            # 같은 성분·효능에 여러 엣지로 들어가고 점수·논문 수가 쪼개진다.
            key = (inci_name, effect_code)
            by_paper = support_by_edge.setdefault(key, {})
            by_paper[pmid] = max(by_paper.get(pmid, 0.0), row_weight)
            by_relation = relation_support.setdefault(key, {})
            by_relation[relation] = max(by_relation.get(relation, 0.0), row_weight)

    if unmapped_ingredients:
        print(f"[WARN] INCI 매핑 실패 ingredient: {sorted(unmapped_ingredients)}")
    print(
        f"[claim] 효능 근거 아님(내약성·안전성) 제외: {dropped['non_efficacy']}행, "
        f"target과 무관한 효능 매핑 제외: {dropped['effect_not_in_target']}건, "
        f"여드름 결과 claim {dropped['acne_outcome']}행(사람 대상 여드름 연구 아님 "
        f"{dropped['acne_not_human']}행)"
    )

    def edge_type(key: tuple[str, str]) -> str:
        by_relation = relation_support[key]
        return min(
            by_relation,
            key=lambda rel: (
                -by_relation[rel],
                _RELATION_PRIORITY.index(rel) if rel in _RELATION_PRIORITY else len(_RELATION_PRIORITY),
                rel,
            ),
        )

    rows = [
        {
            ":START_ID(Ingredient)": ingredient,
            ":END_ID(Effect)": effect,
            "type": edge_type((ingredient, effect)),
            "evidence_type": "pubmed_evidence",
            "graph_score:float": round(math.log1p(sum(by_paper.values())), 6),
            "paper_count:int": len(by_paper),
        }
        for (ingredient, effect), by_paper in support_by_edge.items()
    ]
    rows.sort(
        key=lambda row: (
            row[":START_ID(Ingredient)"],
            row[":END_ID(Effect)"],
            row["type"],
        )
    )

    print(f"[pubmed] 엣지 {len(rows)}개 생성 (evidence/effect 집계 후)")
    return rows


# ---------------------------------------------------------------------------
# CSV 쓰기
# ---------------------------------------------------------------------------

def upload_gold_to_s3(bucket: str, include_evidence_for: bool = False) -> str:
    """생성된 gold CSV 전체를 S3에 업로드하고 업로드 prefix를 반환합니다."""
    s3 = _s3_client()
    batch_tag = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    prefix = f"{S3_GOLD_PREFIX}batch_job={batch_tag}/"

    upload_targets = [
        (GOLD_NODES / "ingredient.csv",  f"{prefix}nodes/ingredient.csv"),
        (GOLD_NODES / "effect.csv",      f"{prefix}nodes/effect.csv"),
        (GOLD_NODES / "concern.csv",     f"{prefix}nodes/concern.csv"),
        (GOLD_NODES / "product.csv",     f"{prefix}nodes/product.csv"),
        (GOLD_EDGES / "affects.csv",     f"{prefix}edges/affects.csv"),
        (GOLD_EDGES / "relates_to.csv",  f"{prefix}edges/relates_to.csv"),
        (GOLD_EDGES / "contains.csv",    f"{prefix}edges/contains.csv"),
    ]
    # 이번 빌드에서 --review-dir로 만든 경우만 올린다(이전 실행의 파일이 섞이지 않게).
    if include_evidence_for:
        upload_targets.append((GOLD_EDGES / "evidence_for.csv", f"{prefix}edges/evidence_for.csv"))

    print(f"\n[S3] gold CSV 업로드 시작 → s3://{bucket}/{prefix}")
    for local_path, s3_key in upload_targets:
        if not local_path.exists():
            print(f"[S3] 파일 없음 (건너뜀): {local_path.relative_to(ROOT)}")
            continue
        s3.upload_file(str(local_path), bucket, s3_key)
        size_kb = local_path.stat().st_size // 1024
        print(f"[S3] 업로드: {s3_key}  ({size_kb} KB)")

    s3_uri = f"s3://{bucket}/{prefix}"
    print(f"[S3] 업로드 완료: {s3_uri}")
    return s3_uri


def write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(f"[write] {path.relative_to(ROOT)}  ({len(rows)}행)")


def build_inci_lookup(inci_df: pd.DataFrame, valid_ingredient_ids: set[str]) -> dict[str, str]:
    """성분명/동의어를 INCI로 매핑하되 종을 특정할 수 없는 총칭은 제외한다."""
    inci_lookup: dict[str, str] = {}
    ambiguous_ceramide_aliases = {"ceramide", "세라마이드"}
    for _, row in inci_df.iterrows():
        if pd.isna(row["inci_name"]):
            continue
        inci_name = str(row["inci_name"])
        if inci_name not in valid_ingredient_ids:
            continue
        inci_lookup[inci_name.lower()] = inci_name
        for column in ("eng_name", "kor_name"):
            if pd.isna(row.get(column)):
                continue
            alias = str(row[column]).strip().lower()
            if alias and (alias not in ambiguous_ceramide_aliases or alias == inci_name.lower()):
                inci_lookup[alias] = inci_name
    manual_overrides = {
        "alpha arbutin": "ALPHA-ARBUTIN",
        "azelaic acid": "AZELAIC ACID",
        "coenzyme q10": "UBIQUINONE",
    }
    inci_lookup.update({
        alias: inci_name
        for alias, inci_name in manual_overrides.items()
        if inci_name in valid_ingredient_ids
    })
    return inci_lookup


def retain_legacy_affects(
    legacy_rows: list[dict],
    valid_ingredient_ids: set[str],
    valid_effects: set[str],
    current_keys: set[tuple[str, str, str]],
    reevaluated_ingredients: frozenset[str] | set[str] = frozenset(),
) -> list[dict]:
    """기존 관계 중 유효한 것만 보존한다. 출처가 모호한 세라마이드 NP 논문 관계는 제외한다."""
    return [
        row for row in legacy_rows
        if str(row.get(":START_ID(Ingredient)", "")) in valid_ingredient_ids
        and str(row.get(":END_ID(Effect)", "")) in valid_effects
        and (
            str(row.get(":START_ID(Ingredient)", "")),
            str(row.get(":END_ID(Effect)", "")),
            str(row.get("type", "")),
        ) not in current_keys
        # 과거 CSV에는 PMID/원문이 없어 'Ceramide' 총칭에서 온 관계인지
        # 확인할 수 없다. 특정 종의 논문 관계로 재사용하지 않는다.
        and not (
            str(row.get(":START_ID(Ingredient)", "")) == "CERAMIDE NP"
            and str(row.get("evidence_type", "")) == "pubmed_evidence"
        )
        # 현재 claim 배치에서 다시 판정한 성분의 과거 논문 관계는 PMID가 없어
        # 검증할 수 없고, 새 gate가 걸러낸 관계를 되살린다.
        and not (
            str(row.get(":START_ID(Ingredient)", "")) in reevaluated_ingredients
            and str(row.get("evidence_type", "")) == "pubmed_evidence"
        )
    ]


SENSITIVE_CAUTIONS_CSV = ROOT / "config" / "review" / "sensitive_skin_cautions.csv"


def sensitive_caution_actions(path: Path = SENSITIVE_CAUTIONS_CSV) -> dict[str, str]:
    """민감 피부 계열 고민의 자극 우려 성분(#49) INCI → 조치(exclude|caution).

    서버가 민감 계열 고민의 비논문 근거(도서·CosIng) 순위에서도 exclude는 거르고 caution은 표시할 수 있게
    노드에 단다.
    """
    table = pd.read_csv(path, dtype=str)
    return dict(zip(table["inci_name"].str.strip().str.upper(), table["action"]))


def reviewed_ingredient_ids(review_dir: Path) -> set[str]:
    """검수 출력의 review_ingredients.csv: 논문을 찾아 거르기·검수까지 거친 성분."""
    return set(pd.read_csv(review_dir / "review_ingredients.csv", dtype=str)["ingredient_inci"].str.upper())


def load_review_affects_rows(
    review_dir: Path,
    valid_ingredient_ids: set[str],
    valid_effects: set[str],
    min_human_papers: int = 1,
) -> tuple[list[dict], set[str]]:
    """LLM 근거 검수 결과(pipeline/review score 출력) → 논문 AFFECTS 엣지(#49).

    서버는 논문 근거를 다른 근거보다 앞에 두므로, 사람 대상 연구가 min_human_papers편 이상인
    엣지만 넣는다(시험관·동물만 있는 엣지가 참고 도서·CosIng보다 앞서지 않게).
    두 번째 값은 검수한 성분 전체로, 이 성분들의 claim·과거 논문 엣지는 쓰지 않는다.
    """
    edges = pd.read_csv(review_dir / "review_edges.csv", dtype={"ingredient_inci": str, "effect_code": str})
    reviewed = reviewed_ingredient_ids(review_dir)
    rows, skipped = [], {"few_human_papers": 0, "unknown_ingredient": 0, "unknown_effect": 0}
    for edge in edges.itertuples():
        inci, effect = str(edge.ingredient_inci).upper(), str(edge.effect_code)
        if int(edge.human_paper_count) < min_human_papers:
            skipped["few_human_papers"] += 1
            continue
        if inci not in valid_ingredient_ids:
            skipped["unknown_ingredient"] += 1
            continue
        if effect not in valid_effects:
            skipped["unknown_effect"] += 1
            continue
        rows.append({
            ":START_ID(Ingredient)": inci,
            ":END_ID(Effect)": effect,
            "type": "improves",
            "evidence_type": "pubmed_evidence",
            "graph_score:float": round(float(edge.score), 6),
            "paper_count:int": int(edge.paper_count),
        })
    print(f"[review] 검수 성분 {len(reviewed)}개, 논문 엣지 {len(rows)}개 (제외 {skipped})")
    return rows, reviewed & valid_ingredient_ids


def load_review_concern_rows(
    review_dir: Path,
    valid_ingredient_ids: set[str],
    valid_concerns: set[str],
) -> list[dict]:
    """고민별 근거(review_concern_edges.csv) → (Ingredient)-[:EVIDENCE_FOR]->(Concern) 엣지(#49).

    고민마다 그 고민에 맞는 질환의 사람 대상 연구만 센 점수다. 효능 엣지는 질환을 구분하지 않아
    다른 질환의 근거가 섞이므로(예: 건조증의 각질 제거 근거가 여드름 순위에 반영), 고민별 후보는
    이 엣지를 먼저 보도록 서버를 바꾼다.
    """
    path = review_dir / "review_concern_edges.csv"
    if not path.exists():
        return []
    edges = pd.read_csv(path, dtype={"ingredient_inci": str, "concern_code": str, "effects": str})
    rows = [
        {
            ":START_ID(Ingredient)": str(e.ingredient_inci).upper(),
            ":END_ID(Concern)": str(e.concern_code),
            "evidence_type": "pubmed_review",
            "graph_score:float": round(float(e.score), 6),
            "paper_count:int": int(e.paper_count),
            "effects": str(e.effects),
            # 민감 계열 고민에서 계열·구성 성분으로 추정한 자극 우려(예: retinoid:class_inferred).
            "caution": "" if pd.isna(getattr(e, "caution", None)) else str(getattr(e, "caution", "")),
        }
        for e in edges.itertuples()
        if str(e.ingredient_inci).upper() in valid_ingredient_ids and str(e.concern_code) in valid_concerns
    ]
    print(f"[review] 고민별 근거 엣지 {len(rows)}개 (원본 {len(edges)}개)")
    return rows


# ---------------------------------------------------------------------------
# 메인
# ---------------------------------------------------------------------------

def main(
    bucket: str,
    target_only: bool = False,
    since: str | None = None,
    no_upload: bool = False,
    claim_batch_id: str | None = None,
    refresh_targets: bool = False,
    review_dir: Path | None = None,
    review_min_human_papers: int = 1,
) -> None:
    print("=" * 60)
    print("Gold CSV 빌드 시작")
    print(f"  S3 bucket : {bucket}")
    print(f"  출력 경로  : gold/nodes/, gold/edges/")
    if target_only:
        print("  모드: target_ingredients.csv 만 생성")
    elif refresh_targets:
        print("  target_ingredients.csv: 갱신")
    if no_upload:
        print("  S3 업로드: 건너뜀 (--no-upload)")
    if claim_batch_id:
        print(f"  Gold claim batch: {claim_batch_id}")
    print("=" * 60)

    # ── S3에서 원본 데이터 로드 ──────────────────────────────────────────
    prod_df = load_parquet_from_s3(bucket)
    inci_df = load_inci_csv_from_s3(bucket)

    # ── target_ingredients.csv (파이프라인 입력) ──────────────────────────
    if target_only or refresh_targets:
        target_rows = build_target_ingredients(prod_df, inci_df)
        target_fieldnames = [
            "ingredient_code", "category", "canonical_name", "query_name",
            "alias_list", "concern_keywords", "exclude_if_contains", "is_target",
        ]
        write_csv(
            ROOT / "config" / "target_ingredients.csv",
            target_fieldnames,
            target_rows,
        )
    else:
        print("[target] 기존 config/target_ingredients.csv 유지")

    if target_only:
        print()
        print("=" * 60)
        print("완료. target_ingredients.csv 만 생성되었습니다.")
        print("=" * 60)
        return

    # ── ingredient.csv ───────────────────────────────────────────────────
    merged = prod_df.merge(
        inci_df[["inci_name", "kor_name", "cosing_functions"]],
        on="inci_name",
        how="left",
        suffixes=("_prod", "_inci"),
    )
    merged["kor_name_final"] = merged["kor_name_inci"].combine_first(merged["kor_name_prod"])
    merged["cosing_final"] = merged["cosing_functions_inci"].combine_first(merged["cosing_functions_prod"])

    ingredient_rows = [
        {
            "ingredient_id:ID(Ingredient)": row["inci_name"],
            "inci_name": row["inci_name"],
            "kor_name": row["kor_name_final"] if pd.notna(row["kor_name_final"]) else "",
            "cosing_functions:string[]": row["cosing_final"] if pd.notna(row["cosing_final"]) else "",
        }
        for _, row in merged.iterrows()
        if pd.notna(row["inci_name"]) and str(row["inci_name"]).strip()
    ]
    legacy_ingredients = load_existing_graph_csv(
        bucket,
        "nodes/ingredient.csv",
    )
    if not legacy_ingredients.empty:
        ingredient_frame = pd.concat(
            [pd.DataFrame(ingredient_rows), legacy_ingredients],
            ignore_index=True,
        )
        ingredient_frame = ingredient_frame[
            ingredient_frame["ingredient_id:ID(Ingredient)"].notna()
            & (
                ingredient_frame["ingredient_id:ID(Ingredient)"]
                .astype(str)
                .str.strip()
                .ne("")
            )
        ]
        ingredient_rows = (
            ingredient_frame
            .drop_duplicates("ingredient_id:ID(Ingredient)", keep="first")
            .fillna("")
            .to_dict("records")
        )
        print(f"[ingredient] 최신 상품 + 기존 graph union: {len(ingredient_rows)}개")
    ingredient_columns = ["ingredient_id:ID(Ingredient)", "inci_name", "kor_name", "cosing_functions:string[]"]
    if review_dir is not None:
        # 검수한 성분 표시(#49): 서버는 이 성분의 고민 순위를 EVIDENCE_FOR로만 매기고,
        # 질환을 구분하지 않는 논문 AFFECTS 엣지로는 매기지 않는다.
        reviewed_ids = reviewed_ingredient_ids(review_dir)
        caution_actions = sensitive_caution_actions()
        for row in ingredient_rows:
            ing_id = str(row["ingredient_id:ID(Ingredient)"]).upper()
            row["evidence_reviewed:boolean"] = str(ing_id in reviewed_ids).lower()
            row["sensitive_caution"] = caution_actions.get(ing_id, "")
        ingredient_columns += ["evidence_reviewed:boolean", "sensitive_caution"]
    write_csv(GOLD_NODES / "ingredient.csv", ingredient_columns, ingredient_rows)

    # ── inci_name 역방향 lookup (소문자 → inci_name) ─────────────────────
    valid_ingredient_ids = {
        str(row["ingredient_id:ID(Ingredient)"])
        for row in ingredient_rows
        if row.get("ingredient_id:ID(Ingredient)")
    }
    inci_lookup = build_inci_lookup(inci_df, valid_ingredient_ids)

    # ── effect.csv ───────────────────────────────────────────────────────
    effect_rows = parse_effect_taxonomy()
    write_csv(
        GOLD_NODES / "effect.csv",
        ["effect_code:ID(Effect)", "effect_name_en"],
        [{"effect_code:ID(Effect)": r["effect_code"], "effect_name_en": r["effect_name_en"]} for r in effect_rows],
    )

    # ── concern.csv ──────────────────────────────────────────────────────
    concern_rows = parse_concern_taxonomy()
    write_csv(
        GOLD_NODES / "concern.csv",
        ["concern_code:ID(Concern)", "concern_name_ko"],
        [{"concern_code:ID(Concern)": r["concern_code"], "concern_name_ko": r["concern_name_ko"]} for r in concern_rows],
    )

    # ── product.csv (헤더만) ─────────────────────────────────────────────
    write_csv(
        GOLD_NODES / "product.csv",
        ["product_id:ID(Product)", "product_name", "brand"],
        [],
    )
    print("[INFO] product.csv: product 데이터 미제공으로 헤더만 생성")

    # ── affects.csv ──────────────────────────────────────────────────────
    effect_id_to_code: dict[int, str] = {}
    for batch_dir in _all_claim_batches(
        since=since,
        claim_batch_id=claim_batch_id,
    ):
        em_path = batch_dir / "claim_effect_map.csv"
        if em_path.exists() and em_path.stat().st_size > 5:
            try:
                em = pd.read_csv(em_path, encoding="utf-8-sig")
                if len(em) > 0:
                    for _, row in em[["effect_id", "effect_code"]].drop_duplicates().iterrows():
                        effect_id_to_code[int(row["effect_id"])] = row["effect_code"]
            except Exception:
                pass

    evaluated_ingredients: set[str] = set()
    pubmed_rows = load_affects_rows(
        effect_id_to_code,
        inci_lookup,
        since=since,
        claim_batch_id=claim_batch_id,
        evaluated_ingredients=evaluated_ingredients,
    )

    valid_effects = {r["effect_code"] for r in effect_rows}
    if review_dir is not None:
        # 검수한 성분은 claim 기반 논문 엣지를 검수 결과로 바꾼다(#49).
        review_rows, reviewed = load_review_affects_rows(
            review_dir,
            {str(row["ingredient_id:ID(Ingredient)"]) for row in ingredient_rows},
            valid_effects,
            review_min_human_papers,
        )
        kept = [r for r in pubmed_rows if r[":START_ID(Ingredient)"] not in reviewed]
        print(f"[review] claim 기반 논문 엣지 {len(pubmed_rows)}개 중 검수 성분 {len(pubmed_rows) - len(kept)}개 교체")
        pubmed_rows = review_rows + kept
        evaluated_ingredients |= reviewed

    # ── COSING soft 엣지 (pubmed 엣지가 없는 성분·효과 쌍 보완) ──────────
    pubmed_seen: set[tuple] = {
        (r[":START_ID(Ingredient)"], r[":END_ID(Effect)"], r["type"])
        for r in pubmed_rows
    }
    soft_rows = build_cosing_soft_edges(prod_df, inci_df, pubmed_seen, valid_effects)

    affects_rows = pubmed_rows + soft_rows
    legacy_affects = load_existing_graph_csv(bucket, "edges/affects.csv")
    if not legacy_affects.empty:
        valid_ingredient_ids = {
            str(row["ingredient_id:ID(Ingredient)"])
            for row in ingredient_rows
        }
        current_keys = {
            (
                row[":START_ID(Ingredient)"],
                row[":END_ID(Effect)"],
                row["type"],
            )
            for row in affects_rows
        }
        retained_legacy = retain_legacy_affects(
            legacy_affects.to_dict("records"), valid_ingredient_ids,
            valid_effects, current_keys, evaluated_ingredients,
        )
        affects_rows = retained_legacy + affects_rows
        print(
            f"[affects] 기존 운영 유효 edge 보존: {len(retained_legacy)}개 "
            f"(dangling/새 edge 중복 제외)"
        )
    print(
        f"[affects] 합계: 신규 pubmed {len(pubmed_rows)}개 + "
        f"신규 cosing {len(soft_rows)}개 + 보존 legacy "
        f"{len(affects_rows) - len(pubmed_rows) - len(soft_rows)}개 "
        f"= {len(affects_rows)}개"
    )
    write_csv(
        GOLD_EDGES / "affects.csv",
        [":START_ID(Ingredient)", ":END_ID(Effect)", "type",
         "evidence_type", "graph_score:float", "paper_count:int"],
        affects_rows,
    )

    # ── relates_to.csv ───────────────────────────────────────────────────
    valid_concerns = {r["concern_code"] for r in concern_rows}
    relates_rows = [
        {":START_ID(Effect)": ec, ":END_ID(Concern)": cc}
        for ec, cc in parse_concern_effect_map()
        if ec in valid_effects and cc in valid_concerns
    ]
    write_csv(
        GOLD_EDGES / "relates_to.csv",
        [":START_ID(Effect)", ":END_ID(Concern)"],
        relates_rows,
    )

    # ── evidence_for.csv (고민별 논문 근거, #49) ────────────────────────
    # 검수 결과 없이 빌드하면 이전 실행의 파일을 지워, 적재 스크립트가 섞어 올리지 않게 한다.
    if review_dir is None:
        (GOLD_EDGES / "evidence_for.csv").unlink(missing_ok=True)
    else:
        write_csv(
            GOLD_EDGES / "evidence_for.csv",
            [":START_ID(Ingredient)", ":END_ID(Concern)", "evidence_type",
             "graph_score:float", "paper_count:int", "effects", "caution"],
            load_review_concern_rows(
                review_dir,
                {str(row["ingredient_id:ID(Ingredient)"]) for row in ingredient_rows},
                valid_concerns,
            ),
        )

    # ── contains.csv (헤더만) ────────────────────────────────────────────
    write_csv(
        GOLD_EDGES / "contains.csv",
        [":START_ID(Product)", ":END_ID(Ingredient)"],
        [],
    )
    print("[INFO] contains.csv: product 데이터 미제공으로 헤더만 생성")

    if no_upload:
        print()
        print("=" * 60)
        print("완료. S3 업로드 건너뜀 (--no-upload 옵션)")
        print("=" * 60)
        return

    s3_uri = upload_gold_to_s3(bucket, include_evidence_for=review_dir is not None)
    print()
    print("=" * 60)
    print(f"완료. Gold CSV → {s3_uri}")
    print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gold CSV 빌드")
    parser.add_argument("--bucket", default=S3_BUCKET, help="S3 버킷명")
    parser.add_argument("--target-only", action="store_true",
                        help="target_ingredients.csv 만 생성하고 종료")
    parser.add_argument("--refresh-targets", action="store_true",
                        help="Graph CSV 빌드와 함께 target_ingredients.csv 갱신")
    parser.add_argument("--since", default=None,
                        help="이 날짜(YYYY-MM-DD) 이후 gold 배치만 사용. 예: --since 2026-05-10")
    parser.add_argument("--claim-batch-id", default=None,
                        help="정확히 하나의 Gold claim 배치만 사용")
    parser.add_argument("--no-upload", action="store_true",
                        help="S3 업로드를 건너뜀 (로컬 CSV만 생성)")
    parser.add_argument("--review-dir", type=Path, default=None,
                        help="LLM 근거 검수 출력 폴더(review_edges.csv, review_ingredients.csv). 검수한 성분의 논문 엣지를 교체")
    parser.add_argument("--review-min-human-papers", type=int, default=1,
                        help="그래프에 넣을 검수 엣지의 최소 사람 대상 논문 수")
    args = parser.parse_args()
    main(
        args.bucket,
        target_only=args.target_only,
        since=args.since,
        no_upload=args.no_upload,
        claim_batch_id=args.claim_batch_id,
        refresh_targets=args.refresh_targets,
        review_dir=args.review_dir,
        review_min_human_papers=args.review_min_human_papers,
    )
