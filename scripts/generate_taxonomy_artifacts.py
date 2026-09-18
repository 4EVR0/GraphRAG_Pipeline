#!/usr/bin/env python3
"""taxonomy.yaml(SSOT) → 파생 산출물 생성기.

산출물:
  1. GraphDB-Server/migrations/V002__taxonomy_relates_to.cypher
     - Concern 노드 MERGE(+이름/그룹 속성) 후 RELATES_TO 엣지를 전량 재구축.
       bulk import의 concern.csv(15개)에 없는 concern 노드도 여기서 생성되므로
       26개 concern 전체가 그래프에 보장된다.
  2. GraphDB-Server/migrations/V003__concern_categories.cypher
     - Concern별 appropriate_categories 배열 + TaxonomyConfig(default_categories).
  3. 4EVR0-Server/app/domain/taxonomy_snapshot.json
     - 서버 기동 시 Neo4j 접속 불가 시의 오프라인 폴백.

레포 배치: GraphRAG_Pipeline / GraphDB-Server / 4EVR0-Server 가 형제 디렉토리라고
가정한다(로컬 개발 기준). 다르면 --graphdb-dir / --server-dir 로 지정.

사용법:
  python scripts/generate_taxonomy_artifacts.py [--check]
  --check: 파일을 쓰지 않고 기존 산출물과 diff만 검사 (CI용, 불일치 시 exit 1)
"""

import argparse
import json
import re
import sys
from pathlib import Path

import yaml

PIPELINE_ROOT = Path(__file__).resolve().parents[1]
TAXONOMY_PATH = PIPELINE_ROOT / "db" / "seed" / "taxonomy.yaml"
SEED_MAP_SQL = PIPELINE_ROOT / "db" / "seed" / "seed_concern_effect_map.sql"

GENERATED_HEADER = (
    "// GENERATED FILE — 직접 수정 금지.\n"
    "// 원본: GraphRAG_Pipeline/db/seed/taxonomy.yaml\n"
    "// 재생성: python GraphRAG_Pipeline/scripts/generate_taxonomy_artifacts.py\n"
)


def _q(value: str) -> str:
    """Cypher 문자열 리터럴 이스케이프. 세미콜론은 migrate.py의 문장 분리를 깨므로 금지."""
    if ";" in value:
        raise ValueError(f"taxonomy 값에 세미콜론 사용 불가: {value!r}")
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _str_list(values: list[str]) -> str:
    return "[" + ", ".join(_q(v) for v in values) + "]"


def load_taxonomy() -> dict:
    data = yaml.safe_load(TAXONOMY_PATH.read_text(encoding="utf-8"))
    effects = data["effects"]
    for code, concern in data["concerns"].items():
        unknown = [e for e in concern["effects"] if e not in effects]
        if unknown:
            raise ValueError(f"{code}: effects 목록에 미정의 effect 존재: {unknown}")
        if not concern["effects"]:
            raise ValueError(f"{code}: effects가 비어 있음")
    return data


def build_v002(data: dict) -> str:
    lines = [GENERATED_HEADER]
    lines.append("// concern 노드 보장 + RELATES_TO 전량 재구축 (rank = 우선순위, 0이 최우선)\n")
    for code, c in data["concerns"].items():
        lines.append(
            f"MERGE (c:Concern {{concern_code: {_q(code)}}})\n"
            f"SET c.concern_name_en = {_q(c['name_en'])},\n"
            f"    c.concern_name_ko = {_q(c['name_ko'])},\n"
            f"    c.concern_group = {_q(c['group'])};"
        )
        lines.append(
            f"MATCH (c:Concern {{concern_code: {_q(code)}}})\n"
            f"OPTIONAL MATCH ()-[r:RELATES_TO]->(c) DELETE r;"
        )
        for rank, effect in enumerate(c["effects"]):
            lines.append(
                f"MATCH (e:Effect {{effect_code: {_q(effect)}}}), (c:Concern {{concern_code: {_q(code)}}})\n"
                f"MERGE (e)-[r:RELATES_TO]->(c) SET r.rank = {rank};"
            )
        lines.append("")
    return "\n".join(lines)


def build_v003(data: dict) -> str:
    default = data["default_categories"]
    lines = [GENERATED_HEADER]
    lines.append("// concern별 적합 제품 카테고리 (미지정 concern은 default_categories 적용)\n")
    lines.append(
        f"MERGE (m:TaxonomyConfig {{key: 'default_categories'}})\n"
        f"SET m.values = {_str_list(default)};"
    )
    for code, c in data["concerns"].items():
        categories = c.get("categories", default)
        lines.append(
            f"MATCH (c:Concern {{concern_code: {_q(code)}}})\n"
            f"SET c.appropriate_categories = {_str_list(categories)};"
        )
    lines.append("")
    return "\n".join(lines)


def build_snapshot(data: dict) -> str:
    default = data["default_categories"]
    snapshot = {
        "_generated_from": "GraphRAG_Pipeline/db/seed/taxonomy.yaml",
        "default_categories": default,
        "concerns": {
            code: {
                "effects": c["effects"],
                "categories": c.get("categories", default),
            }
            for code, c in data["concerns"].items()
        },
    }
    return json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n"


def check_seed_sql_drift(data: dict) -> None:
    """구 seed SQL과의 차이를 경고만 한다 (SQL seed는 점진 폐기 대상)."""
    if not SEED_MAP_SQL.exists():
        return
    sql = SEED_MAP_SQL.read_text(encoding="utf-8")
    sql_map: dict[str, set[str]] = {}
    for concern, effects_raw in re.findall(
        r"concern_code='(\w+)' AND e\.effect_code IN \(([^)]+)\)", sql
    ):
        sql_map[concern] = {e.strip().strip("'") for e in effects_raw.split(",")}
    for concern, effects in sql_map.items():
        yaml_effects = set(data["concerns"].get(concern, {}).get("effects", []))
        if yaml_effects != effects:
            print(
                f"[warn] seed_concern_effect_map.sql 불일치: {concern} "
                f"sql={sorted(effects)} yaml={sorted(yaml_effects)} (yaml이 정본)",
                file=sys.stderr,
            )
    missing = set(data["concerns"]) - set(sql_map)
    if missing:
        print(
            f"[info] seed SQL에 없는 concern {len(missing)}개는 yaml에서 신규 정의됨",
            file=sys.stderr,
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--graphdb-dir", type=Path, default=PIPELINE_ROOT.parent / "GraphDB-Server")
    parser.add_argument("--server-dir", type=Path, default=PIPELINE_ROOT.parent / "4EVR0-Server")
    parser.add_argument("--check", action="store_true", help="쓰지 않고 기존 산출물과 diff 검사")
    args = parser.parse_args()

    data = load_taxonomy()
    check_seed_sql_drift(data)

    outputs = {
        args.graphdb_dir / "migrations" / "V002__taxonomy_relates_to.cypher": build_v002(data),
        args.graphdb_dir / "migrations" / "V003__concern_categories.cypher": build_v003(data),
        args.server_dir / "app" / "domain" / "taxonomy_snapshot.json": build_snapshot(data),
    }

    stale = []
    for path, content in outputs.items():
        if args.check:
            current = path.read_text(encoding="utf-8") if path.exists() else None
            if current != content:
                stale.append(path)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")
            print(f"[ok] wrote {path}")

    if args.check and stale:
        for path in stale:
            print(f"[stale] {path} — 생성기를 다시 실행하세요", file=sys.stderr)
        return 1

    n_concern = len(data["concerns"])
    n_edges = sum(len(c["effects"]) for c in data["concerns"].values())
    print(f"[summary] concerns={n_concern} effects={len(data['effects'])} relates_to_edges={n_edges}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
