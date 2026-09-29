#!/usr/bin/env bash
# graph_schema.md — Neo4j bulk import (로컬 neo4j-admin 사용)
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

neo4j-admin database import full \
  --nodes=Product=gold/nodes/product.csv \
  --nodes=Ingredient=gold/nodes/ingredient.csv \
  --nodes=Effect=gold/nodes/effect.csv \
  --nodes=Concern=gold/nodes/concern.csv \
  --relationships=CONTAINS=gold/edges/contains.csv \
  --relationships=AFFECTS=gold/edges/affects.csv \
  --relationships=RELATES_TO=gold/edges/relates_to.csv \
  --overwrite-destination \
  neo4j

echo "벌크 임포트는 그래프를 덮어쓴다. 기동 후 추가 적재 스크립트를 다시 실행할 것:"
echo "  python scripts/load_caution_to_neo4j.py"
echo "  python scripts/load_kr_regulation_to_neo4j.py --csv <INCI_Pipeline ingredient_kr_regulation.csv>"
