"""CAUTION 엣지 빌더 (Option A / A1 — 근거 기반 금기 오버레이).

이미 추출된 부작용 claim(gold_claim_all.csv)에서 "성분이 민감성 계열 고민을 유발"하는
근거만 골라 (Ingredient)-[:CAUTION]->(Concern) 엣지 CSV를 만든다. 재마이닝 없이,
기존 affects 파이프라인과 동일한 집계·근거 등급을 재사용한다(새 인식론 없음).

선택 규칙:
  relation == "causes" AND evidence_direction == "supports"
  AND concern ∈ 민감성 그룹(SENSITIVE_SKIN/REDNESS/IRRITATED_SKIN/ATOPIC_PRONE/ROSACEA_PRONE/BARRIER_DAMAGE)
  AND attribution_label ∈ {single_active, single_formulation}   # 다성분 오귀인 방지
집계:
  (ingredient, concern) 단위로 논문별 row_weight 최대값 합 → graph_score=log1p, paper_count=distinct pmid
출력:
  gold/edges/caution.csv  (Neo4j bulk import 포맷; END=Concern)

INCI 매핑은 config/target_ingredients.csv에서 로드(오프라인). 최종 통합 시엔 build_gold_csvs의
S3 inci_lookup(권위 있음)을 쓰는 게 맞다 — 여기선 A1 데이터 경로 검증이 목적.

사용:
    python scripts/build_caution_edges.py [--out gold/edges/caution.csv]
"""

import argparse
import csv
import glob
import math
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# 민감성 계열 concern (이 그룹을 "유발"하면 주의)
SENSITIVITY_CONCERNS = {
    "SENSITIVE_SKIN", "REDNESS", "IRRITATED_SKIN",
    "ATOPIC_PRONE", "ROSACEA_PRONE", "BARRIER_DAMAGE",
}
# 성분이 실제 주체임이 분명한 attribution만(다성분 제형·시술병용 오귀인 배제)
ALLOWED_ATTRIBUTION = {"single_active", "single_formulation"}


def load_concern_id_to_code() -> dict[int, str]:
    """seed_concern_taxonomy.sql의 등장 순서 = concern_id (1-based)."""
    seed = (ROOT / "db" / "seed" / "seed_concern_taxonomy.sql").read_text()
    codes = [m.group(1) for m in re.finditer(r"\('([A-Z_]+)',", seed)]
    return {i + 1: code for i, code in enumerate(codes)}


def load_inci_lookup() -> dict[str, str]:
    """config/target_ingredients.csv → {영문/한글/alias(lower): INCI}.

    gold claim의 ingredient_name(영문 'Azelaic acid' 또는 한글 '글리세릴라우레이트')을
    그래프 Ingredient 노드 id(inci_name, alias_list의 첫 값)로 매핑한다.
    """
    lookup: dict[str, str] = {}
    path = ROOT / "config" / "target_ingredients.csv"
    with open(path, encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            aliases = [a.strip() for a in (row.get("alias_list") or "").split("|") if a.strip()]
            inci = aliases[0] if aliases else (row.get("query_name") or "").strip()
            if not inci:
                continue
            keys = [row.get("canonical_name"), row.get("query_name"), *aliases]
            for k in keys:
                if k and k.strip():
                    lookup[k.strip().lower()] = inci
    return lookup


def pipe_ints(value: str | None) -> set[int]:
    out: set[int] = set()
    for part in (value or "").split("|"):
        part = part.strip()
        if part and part.lower() != "nan":
            try:
                out.add(int(float(part)))
            except ValueError:
                pass
    return out


def build_caution_rows() -> tuple[list[dict], dict]:
    id2code = load_concern_id_to_code()
    sensitivity_ids = {i for i, c in id2code.items() if c in SENSITIVITY_CONCERNS}
    inci_lookup = load_inci_lookup()

    batch_files = sorted(glob.glob(str(ROOT / "gold" / "claim" / "*" / "gold_claim_all.csv")))
    # (inci, concern_code) -> {pmid: max_row_weight}
    support: dict[tuple[str, str], dict[str, float]] = {}
    stats = {"scanned": 0, "causes_supports": 0, "selected": 0,
             "dropped_attribution": 0, "unmapped_inci": 0}
    unmapped: set[str] = set()

    for bf in batch_files:
        with open(bf, encoding="utf-8-sig", newline="") as f:
            for r in csv.DictReader(f):
                stats["scanned"] += 1
                if r.get("relation") != "causes" or r.get("evidence_direction") != "supports":
                    continue
                concern_hit = pipe_ints(r.get("concern_ids")) & sensitivity_ids
                if not concern_hit:
                    continue
                stats["causes_supports"] += 1
                if (r.get("attribution_label") or "") not in ALLOWED_ATTRIBUTION:
                    stats["dropped_attribution"] += 1
                    continue
                name = (r.get("ingredient_name") or "").strip()
                inci = inci_lookup.get(name.lower())
                if not inci:
                    stats["unmapped_inci"] += 1
                    unmapped.add(name)
                    continue
                pmid = str(r.get("pmid") or "")
                weight = float(r.get("row_weight") or 0.0)
                for cid in concern_hit:
                    key = (inci, id2code[cid])
                    by_paper = support.setdefault(key, {})
                    by_paper[pmid] = max(by_paper.get(pmid, 0.0), weight)
                stats["selected"] += 1

    rows = [
        {
            ":START_ID(Ingredient)": inci,
            ":END_ID(Concern)": concern,
            "type": "causes",
            "evidence_type": "pubmed_evidence",
            "graph_score:float": round(math.log1p(sum(by_paper.values())), 6),
            "paper_count:int": len(by_paper),
        }
        for (inci, concern), by_paper in sorted(support.items())
    ]
    stats["edges"] = len(rows)
    stats["unmapped_names"] = sorted(unmapped)
    return rows, stats


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [":START_ID(Ingredient)", ":END_ID(Concern)", "type",
                  "evidence_type", "graph_score:float", "paper_count:int"]
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(ROOT / "gold" / "edges" / "caution.csv"))
    args = ap.parse_args()

    rows, stats = build_caution_rows()
    write_csv(Path(args.out), rows)

    print("=== CAUTION 엣지 빌드 ===")
    print(f"  스캔 claim: {stats['scanned']}")
    print(f"  causes+supports+민감concern: {stats['causes_supports']}")
    print(f"  attribution 컷: {stats['dropped_attribution']}, INCI 매핑실패: {stats['unmapped_inci']}")
    print(f"  최종 선택 evidence: {stats['selected']} → 엣지 {stats['edges']}개")
    if stats["unmapped_names"]:
        print(f"  [매핑실패 성분] {stats['unmapped_names']}")
    print(f"  출력: {args.out}")
    print()
    for r in rows:
        print(f"    {r[':START_ID(Ingredient)']:28s} --CAUTION--> {r[':END_ID(Concern)']:16s} "
              f"score={r['graph_score:float']} papers={r['paper_count:int']}")


if __name__ == "__main__":
    main()
