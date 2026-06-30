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
    return df


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


def load_affects_rows(
    effect_id_to_code: dict[int, str],
    inci_lookup: dict[str, str],
    since: str | None = None,
    claim_batch_id: str | None = None,
) -> list[dict]:
    """Graph-eligible evidence를 ingredient/effect/relation 단위로 집계합니다."""
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

    evidence["current_eligibility_tier"] = evidence.apply(current_tier, axis=1)
    eligible = evidence[
        evidence["current_eligibility_tier"].isin(["strict_graph", "soft_graph"])
    ].copy()
    print(
        f"[claim] 전체 배치={len(all_batches)}, evidence={len(evidence)}, "
        f"graph_eligible={len(eligible)}"
    )

    # canonical claim 수준의 effect union을 사용하면 한 논문의 부차 effect가
    # 같은 target을 공유하는 모든 논문의 누적 점수를 받는다. Evidence 행의
    # 실제 effect_ids로 그룹화해 effect 간 점수 누수를 막는다.
    support_by_edge: dict[tuple[str, str, str], dict[str, float]] = {}
    unmapped_ingredients: set[str] = set()
    excluded_inci = {"CREAM", "WATER", "MELANIN"}

    for _, claim in eligible.iterrows():
        ingredient_name = str(claim["ingredient_name"])
        relation = str(claim["relation"])
        effect_ids_raw = str(claim.get("effect_ids", ""))

        inci_name = inci_lookup.get(ingredient_name.lower())
        if not inci_name:
            unmapped_ingredients.add(ingredient_name)
            continue
        if inci_name.upper() in excluded_inci:
            continue

        pmid = str(claim["pmid"])
        row_weight = float(claim.get("row_weight", 0.0) or 0.0)

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
            # 피지 증가 관찰은 SEBUM_REGULATION 추천 효능으로 노출하지 않는다.
            if effect_code == "SEBUM_REGULATION" and relation == "increases":
                continue
            key = (inci_name, effect_code, relation)
            by_paper = support_by_edge.setdefault(key, {})
            by_paper[pmid] = max(by_paper.get(pmid, 0.0), row_weight)

    if unmapped_ingredients:
        print(f"[WARN] INCI 매핑 실패 ingredient: {sorted(unmapped_ingredients)}")

    rows = [
        {
            ":START_ID(Ingredient)": ingredient,
            ":END_ID(Effect)": effect,
            "type": relation,
            "evidence_type": "pubmed_evidence",
            "graph_score:float": round(math.log1p(sum(by_paper.values())), 6),
            "paper_count:int": len(by_paper),
        }
        for (ingredient, effect, relation), by_paper in support_by_edge.items()
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

def upload_gold_to_s3(bucket: str) -> str:
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
    write_csv(
        GOLD_NODES / "ingredient.csv",
        ["ingredient_id:ID(Ingredient)", "inci_name", "kor_name", "cosing_functions:string[]"],
        ingredient_rows,
    )

    # ── inci_name 역방향 lookup (소문자 → inci_name) ─────────────────────
    valid_ingredient_ids = {
        str(row["ingredient_id:ID(Ingredient)"])
        for row in ingredient_rows
        if row.get("ingredient_id:ID(Ingredient)")
    }
    inci_lookup: dict[str, str] = {}
    for _, row in inci_df.iterrows():
        if pd.isna(row["inci_name"]):
            continue
        inci_name = str(row["inci_name"])
        if inci_name not in valid_ingredient_ids:
            continue
        inci_lookup[inci_name.lower()] = inci_name
        if pd.notna(row.get("eng_name")):
            inci_lookup[str(row["eng_name"]).lower()] = inci_name
        if pd.notna(row.get("kor_name")):
            inci_lookup[str(row["kor_name"]).lower()] = inci_name
    # 수동 보완 (INCI CSV 미등록 or 명칭 불일치)
    manual_overrides: dict[str, str] = {
        "alpha arbutin": "ALPHA-ARBUTIN",
        "azelaic acid": "AZELAIC ACID",
        "ceramide": "CERAMIDE NP",
        "coenzyme q10": "UBIQUINONE",
    }
    inci_lookup.update(
        {
            alias: inci_name
            for alias, inci_name in manual_overrides.items()
            if inci_name in valid_ingredient_ids
        }
    )

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

    pubmed_rows = load_affects_rows(
        effect_id_to_code,
        inci_lookup,
        since=since,
        claim_batch_id=claim_batch_id,
    )

    # ── COSING soft 엣지 (pubmed 엣지가 없는 성분·효과 쌍 보완) ──────────
    valid_effects = {r["effect_code"] for r in effect_rows}
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
        legacy_rows = [
            row
            for row in legacy_affects.to_dict("records")
            if str(row.get(":START_ID(Ingredient)", "")) in valid_ingredient_ids
            and str(row.get(":END_ID(Effect)", "")) in valid_effects
        ]
        current_keys = {
            (
                row[":START_ID(Ingredient)"],
                row[":END_ID(Effect)"],
                row["type"],
            )
            for row in affects_rows
        }
        retained_legacy = [
            row
            for row in legacy_rows
            if (
                row[":START_ID(Ingredient)"],
                row[":END_ID(Effect)"],
                row["type"],
            )
            not in current_keys
        ]
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

    s3_uri = upload_gold_to_s3(bucket)
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
    args = parser.parse_args()
    main(
        args.bucket,
        target_only=args.target_only,
        since=args.since,
        no_upload=args.no_upload,
        claim_batch_id=args.claim_batch_id,
        refresh_targets=args.refresh_targets,
    )
