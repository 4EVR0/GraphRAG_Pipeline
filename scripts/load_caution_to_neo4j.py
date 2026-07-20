"""CAUTION 엣지를 라이브 Neo4j에 적재 (Option A / A1 — P2).

gold/edges/caution.csv를 읽어 (Ingredient)-[:CAUTION]->(Concern)를 MERGE한다.
전체 bulk 재임포트가 아니라 **기존 그래프에 새 관계만 추가**(idempotent, 안전, 되돌리기 쉬움).
기존 AFFECTS/CONTAINS는 건드리지 않음.

롤백:  MATCH ()-[r:CAUTION]->() DELETE r

사용:
    NEO4J_URI=bolt://... NEO4J_USER=neo4j NEO4J_PASSWORD=... \
    python scripts/load_caution_to_neo4j.py [--csv gold/edges/caution.csv] [--dry-run]
"""

import argparse
import csv
import os
from pathlib import Path

from neo4j import GraphDatabase

ROOT = Path(__file__).resolve().parent.parent

MERGE_CYPHER = """
UNWIND $rows AS row
MATCH (i:Ingredient {inci_name: row.ingredient})
MATCH (c:Concern {concern_code: row.concern})
MERGE (i)-[r:CAUTION {target_concern: row.concern}]->(c)
SET r.type          = row.type,
    r.evidence_type = row.evidence_type,
    r.graph_score   = toFloat(row.graph_score),
    r.paper_count   = toInteger(row.paper_count)
RETURN count(r) AS merged
"""


def load_rows(csv_path: Path) -> list[dict]:
    rows = []
    with open(csv_path, encoding="utf-8", newline="") as f:
        for r in csv.DictReader(f):
            rows.append({
                "ingredient": r[":START_ID(Ingredient)"],
                "concern": r[":END_ID(Concern)"],
                "type": r["type"],
                "evidence_type": r["evidence_type"],
                "graph_score": r["graph_score:float"],
                "paper_count": r["paper_count:int"],
            })
    return rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default=str(ROOT / "gold" / "edges" / "caution.csv"))
    ap.add_argument("--dry-run", action="store_true", help="적재 없이 조인 가능 엣지만 리포트")
    args = ap.parse_args()

    rows = load_rows(Path(args.csv))
    uri = os.environ["NEO4J_URI"]
    user = os.environ.get("NEO4J_USER", "neo4j")
    pw = os.environ["NEO4J_PASSWORD"]

    driver = GraphDatabase.driver(uri, auth=(user, pw))
    with driver.session() as s:
        # 조인 가능 여부 사전 확인
        ings = sorted({r["ingredient"] for r in rows})
        cons = sorted({r["concern"] for r in rows})
        ing_ok = set(s.run("UNWIND $x AS n MATCH (i:Ingredient {inci_name:n}) RETURN collect(n) AS ok", x=ings).single()["ok"])
        con_ok = set(s.run("UNWIND $x AS c MATCH (n:Concern {concern_code:c}) RETURN collect(c) AS ok", x=cons).single()["ok"])
        joinable = [r for r in rows if r["ingredient"] in ing_ok and r["concern"] in con_ok]
        skipped = [r for r in rows if r not in joinable]

        print(f"CSV 엣지: {len(rows)}, 조인가능: {len(joinable)}, 스킵(노드없음): {len(skipped)}")
        for r in skipped:
            print(f"  [스킵] {r['ingredient']} -> {r['concern']} (노드 없음)")

        if args.dry_run:
            print("dry-run: 적재하지 않음")
            driver.close()
            return

        merged = s.run(MERGE_CYPHER, rows=joinable).single()["merged"]
        total = s.run("MATCH ()-[r:CAUTION]->() RETURN count(r) AS n").single()["n"]
        print(f"MERGE 완료: {merged}건 처리 → 그래프의 CAUTION 엣지 총 {total}개")
    driver.close()


if __name__ == "__main__":
    main()
