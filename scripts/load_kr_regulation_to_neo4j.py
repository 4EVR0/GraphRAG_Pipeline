"""국내 규제 상태(kr_reg_status, kr_limit_note)를 라이브 Neo4j Ingredient에 적재.

입력: INCI_Pipeline `python -m pipeline.mfds_regulation.run` 결과
      data/gold/mfds_regulation/run_id=<id>/ingredient_kr_regulation.csv
      (inci_name, kr_reg_status, kr_limit_note, ...)

- kr_reg_status: banned | conditional | restricted | none
    banned      → 추천 후보에서 제외 (서버 query_ingredients_by_effects 필터)
    conditional → 원료 품질 조건부 금지, 정상 추천
    restricted  → 추천 + kr_limit_note(배합한도) 표시
- CSV에 없는 Ingredient는 'none'으로 두고 kr_limit_note를 지운다(전체 갱신, 멱등).
- kr_reg_run_id 에 입력 run_id를 남겨 어느 배치 기준인지 추적한다.
기존 노드·관계는 만들거나 지우지 않고 위 속성만 SET/REMOVE한다.

롤백:  MATCH (i:Ingredient) REMOVE i.kr_reg_status, i.kr_limit_note, i.kr_reg_run_id

사용:
    NEO4J_URI=bolt://... NEO4J_USER=neo4j NEO4J_PASSWORD=... \
    python scripts/load_kr_regulation_to_neo4j.py --csv <ingredient_kr_regulation.csv> [--dry-run]
"""

from __future__ import annotations

import argparse
import csv
import os
import re
from pathlib import Path

STATUSES = {"banned", "conditional", "restricted"}
_RUN_ID_RE = re.compile(r"run_id=([^/\\]+)")

APPLY_CYPHER = """
MATCH (i:Ingredient)
WHERE NOT i.inci_name IN $names
SET i.kr_reg_status = 'none', i.kr_reg_run_id = $run_id
REMOVE i.kr_limit_note
WITH count(i) AS reset
UNWIND $rows AS row
MATCH (i:Ingredient {inci_name: row.inci_name})
SET i.kr_reg_status = row.kr_reg_status,
    i.kr_limit_note = CASE WHEN row.kr_limit_note = '' THEN null ELSE row.kr_limit_note END,
    i.kr_reg_run_id = $run_id
RETURN reset, count(i) AS updated
"""

SUMMARY_CYPHER = """
MATCH (i:Ingredient)
RETURN coalesce(i.kr_reg_status, '(null)') AS status, count(*) AS n
ORDER BY status
"""


def load_rows(csv_path: Path) -> list[dict]:
    rows = []
    # INCI_Pipeline은 utf-8-sig로 저장한다.
    with open(csv_path, encoding="utf-8-sig", newline="") as f:
        for r in csv.DictReader(f):
            status = r["kr_reg_status"].strip()
            if status not in STATUSES:
                raise ValueError(f"알 수 없는 kr_reg_status: {status!r} ({r['inci_name']})")
            rows.append({
                "inci_name": r["inci_name"].strip(),
                "kr_reg_status": status,
                "kr_limit_note": (r.get("kr_limit_note") or "").strip() if status == "restricted" else "",
            })
    names = [r["inci_name"] for r in rows]
    if not rows:
        raise ValueError(f"빈 CSV: {csv_path}")
    if len(set(names)) != len(names):
        raise ValueError("inci_name 중복")
    return rows


def run_id_of(csv_path: Path) -> str:
    m = _RUN_ID_RE.search(str(csv_path.resolve()))
    return m.group(1) if m else csv_path.stem


def main() -> None:
    from neo4j import GraphDatabase

    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="INCI_Pipeline ingredient_kr_regulation.csv")
    ap.add_argument("--dry-run", action="store_true", help="적재 없이 조인 결과만 리포트")
    args = ap.parse_args()

    csv_path = Path(args.csv)
    rows = load_rows(csv_path)
    run_id = run_id_of(csv_path)

    driver = GraphDatabase.driver(os.environ["NEO4J_URI"],
                                  auth=(os.environ.get("NEO4J_USER", "neo4j"), os.environ["NEO4J_PASSWORD"]))
    with driver.session() as s:
        names = [r["inci_name"] for r in rows]
        ok = set(s.run("UNWIND $x AS n MATCH (i:Ingredient {inci_name:n}) RETURN collect(n) AS ok",
                       x=names).single()["ok"])
        joinable = [r for r in rows if r["inci_name"] in ok]
        total = s.run("MATCH (i:Ingredient) RETURN count(i) AS n").single()["n"]
        by_status = {st: sum(r["kr_reg_status"] == st for r in joinable) for st in sorted(STATUSES)}
        print(f"run_id={run_id}")
        print(f"CSV 성분: {len(rows)}, 그래프 조인: {len(joinable)}, 그래프 Ingredient 총 {total}")
        print(f"조인 성분 상태: {by_status}")
        print("banned(추천 제외): " + ", ".join(r["inci_name"] for r in joinable
                                            if r["kr_reg_status"] == "banned"))

        if args.dry_run:
            print("dry-run: 적재하지 않음")
            driver.close()
            return

        res = s.execute_write(lambda tx: tx.run(
            APPLY_CYPHER, names=[r["inci_name"] for r in joinable], rows=joinable, run_id=run_id).single())
        print(f"적재 완료: none 재설정 {res['reset']}, 상태 SET {res['updated']}")
        for rec in s.run(SUMMARY_CYPHER):
            print(f"  {rec['status']}: {rec['n']}")
    driver.close()


if __name__ == "__main__":
    main()
