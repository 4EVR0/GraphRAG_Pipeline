"""#49 근거 검수 결과를 라이브 Neo4j에 증분 적재한다.

`load.sh` 전체 재적재는 S3 스냅샷으로 그래프를 덮어써서, 운영 그래프에 따로 쌓인 데이터
(증분 동기화 제품, 제품 리뷰 속성, 국내 규제 상태, CAUTION, RELATES_TO)를 잃는다.
이 스크립트는 #49 변경분만 기존 그래프에 더한다.

- Ingredient 속성: evidence_reviewed, sensitive_caution, sensitive_caution_with
- (Ingredient)-[:EVIDENCE_FOR]->(Concern): 고민별 논문 근거

기존 AFFECTS·CONTAINS·제품·규제 속성은 건드리지 않는다. 실행할 때마다 위 두 가지를 통째로
지우고 다시 쓰므로 여러 번 실행해도 결과가 같다. 그래프에 없는 성분·고민은 건너뛰고 보고한다.

입력은 build_gold_csvs.py --no-upload --review-dir 로 만든 gold/nodes/ingredient.csv,
gold/edges/evidence_for.csv 이다.

사용:
    NEO4J_URI=bolt://... NEO4J_USER=neo4j NEO4J_PASSWORD=... \\
    python scripts/load_review_evidence_to_neo4j.py              # 기본: dry-run(읽기만)
    python scripts/load_review_evidence_to_neo4j.py --apply      # 적재
    python scripts/load_review_evidence_to_neo4j.py --rollback   # 이 스크립트가 쓴 것만 제거
"""

import argparse
import csv
import os
from pathlib import Path

from neo4j import GraphDatabase

ROOT = Path(__file__).resolve().parent.parent
PROPS = ("evidence_reviewed", "sensitive_caution", "sensitive_caution_with")

# Neo4j 5 버전 차이(CALL () {} 문법)를 피하려고 두 문장으로 나눈다.
CLEAR_EDGES_CYPHER = "MATCH ()-[r:EVIDENCE_FOR]->() DELETE r RETURN count(r) AS n"
CLEAR_PROPS_CYPHER = """
MATCH (i:Ingredient)
WHERE i.evidence_reviewed IS NOT NULL OR i.sensitive_caution IS NOT NULL OR i.sensitive_caution_with IS NOT NULL
REMOVE i.evidence_reviewed, i.sensitive_caution, i.sensitive_caution_with
RETURN count(i) AS n
"""


def _clear(session) -> tuple[int, int]:
    edges = session.run(CLEAR_EDGES_CYPHER).single()["n"]
    nodes = session.run(CLEAR_PROPS_CYPHER).single()["n"]
    return edges, nodes


SET_PROPS_CYPHER = """
UNWIND $rows AS row
MATCH (i:Ingredient {inci_name: row.inci})
SET i.evidence_reviewed = row.evidence_reviewed
FOREACH (_ IN CASE WHEN row.sensitive_caution <> '' THEN [1] ELSE [] END |
    SET i.sensitive_caution = row.sensitive_caution)
FOREACH (_ IN CASE WHEN size(row.sensitive_caution_with) > 0 THEN [1] ELSE [] END |
    SET i.sensitive_caution_with = row.sensitive_caution_with)
RETURN count(i) AS n
"""

CREATE_EDGES_CYPHER = """
UNWIND $rows AS row
MATCH (i:Ingredient {inci_name: row.ingredient})
MATCH (c:Concern {concern_code: row.concern})
CREATE (i)-[r:EVIDENCE_FOR]->(c)
SET r.evidence_type = row.evidence_type,
    r.graph_score   = row.graph_score,
    r.paper_count   = row.paper_count,
    r.effects       = row.effects,
    r.caution       = row.caution
RETURN count(r) AS n
"""


def read_ingredient_props(path: Path) -> list[dict]:
    """ingredient.csv에서 #49 속성이 있는 성분만(검수함 또는 민감 주의)."""
    rows = []
    with open(path, encoding="utf-8-sig", newline="") as f:
        for r in csv.DictReader(f):
            reviewed = (r.get("evidence_reviewed:boolean") or "").strip().lower() == "true"
            caution = (r.get("sensitive_caution") or "").strip()
            caution_with = [c for c in (r.get("sensitive_caution_with:string[]") or "").split(";") if c]
            if reviewed or caution:
                rows.append({"inci": r["inci_name"].strip(), "evidence_reviewed": reviewed,
                             "sensitive_caution": caution, "sensitive_caution_with": caution_with})
    if not rows:
        raise SystemExit(f"{path}: #49 속성이 없습니다. build_gold_csvs.py --review-dir 로 빌드했는지 확인하세요.")
    return rows


def read_evidence_rows(path: Path) -> list[dict]:
    with open(path, encoding="utf-8-sig", newline="") as f:
        return [{"ingredient": r[":START_ID(Ingredient)"], "concern": r[":END_ID(Concern)"],
                 "evidence_type": r["evidence_type"], "graph_score": float(r["graph_score:float"]),
                 "paper_count": int(r["paper_count:int"]), "effects": r.get("effects") or "",
                 "caution": r.get("caution") or ""}
                for r in csv.DictReader(f)]


def _present(session, label: str, key: str, values: list[str]) -> set[str]:
    query = f"UNWIND $x AS v MATCH (n:{label} {{{key}: v}}) RETURN collect(DISTINCT v) AS ok"
    return set(session.run(query, x=values).single()["ok"])


def run(session, ingredients: list[dict], edges: list[dict], mode: str) -> dict:
    """mode: dry-run | apply | rollback. 결과 요약 dict를 돌려준다."""
    if mode == "rollback":
        edges_removed, nodes_cleared = _clear(session)
        return {"removed_edges": edges_removed, "cleared_nodes": nodes_cleared}

    ing_ok = _present(session, "Ingredient", "inci_name",
                      sorted({r["inci"] for r in ingredients} | {e["ingredient"] for e in edges}))
    con_ok = _present(session, "Concern", "concern_code", sorted({e["concern"] for e in edges}))
    props = [r for r in ingredients if r["inci"] in ing_ok]
    joinable = [e for e in edges if e["ingredient"] in ing_ok and e["concern"] in con_ok]
    summary = {
        "props_rows": len(ingredients), "props_matched": len(props),
        "props_missing": sorted({r["inci"] for r in ingredients} - ing_ok),
        "edges_rows": len(edges), "edges_joinable": len(joinable),
        "edges_missing": sorted({(e["ingredient"], e["concern"]) for e in edges if e not in joinable}),
        "existing_edges": session.run("MATCH ()-[r:EVIDENCE_FOR]->() RETURN count(r) AS n").single()["n"],
    }
    if mode == "dry-run":
        return summary
    summary["removed_edges"], summary["cleared_nodes"] = _clear(session)
    summary["set_nodes"] = session.run(SET_PROPS_CYPHER, rows=props).single()["n"]
    summary["created_edges"] = session.run(CREATE_EDGES_CYPHER, rows=joinable).single()["n"]
    return summary


def main() -> None:
    ap = argparse.ArgumentParser(description="#49 근거 검수 결과 증분 적재(기본 dry-run)")
    ap.add_argument("--gold-dir", type=Path, default=ROOT / "gold")
    group = ap.add_mutually_exclusive_group()
    group.add_argument("--apply", action="store_true", help="실제로 적재")
    group.add_argument("--rollback", action="store_true", help="EVIDENCE_FOR와 #49 성분 속성만 제거")
    args = ap.parse_args()
    mode = "apply" if args.apply else "rollback" if args.rollback else "dry-run"

    ingredients = [] if mode == "rollback" else read_ingredient_props(args.gold_dir / "nodes" / "ingredient.csv")
    edges = [] if mode == "rollback" else read_evidence_rows(args.gold_dir / "edges" / "evidence_for.csv")
    uri = os.environ["NEO4J_URI"]
    print(f"대상: {uri.split('@')[-1]}  모드: {mode}")
    with GraphDatabase.driver(uri, auth=(os.environ.get("NEO4J_USER", "neo4j"), os.environ["NEO4J_PASSWORD"])) as d:
        access = "READ" if mode == "dry-run" else "WRITE"
        with d.session(default_access_mode=access) as s:
            summary = s.execute_write(lambda tx: run(tx, ingredients, edges, mode)) if mode != "dry-run" \
                else run(s, ingredients, edges, mode)
    for key, value in summary.items():
        print(f"  {key}: {value}")
    if mode == "dry-run":
        print("dry-run: 쓰지 않음. 적재하려면 --apply")


if __name__ == "__main__":
    main()
