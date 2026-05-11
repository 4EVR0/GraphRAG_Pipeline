#!/usr/bin/env bash
# graph_schema.md — Docker neo4j-admin offline import (volume: neo4j_graphrag_data)
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE="${NEO4J_IMAGE:-neo4j:5-community}"

docker volume create neo4j_graphrag_data 2>/dev/null || true

docker run --rm \
  -v "$ROOT/gold:/import:ro" \
  -v neo4j_graphrag_data:/data \
  "$IMAGE" \
  neo4j-admin database import full \
  --nodes=Product=/import/nodes/product.csv \
  --nodes=Ingredient=/import/nodes/ingredient.csv \
  --nodes=Effect=/import/nodes/effect.csv \
  --nodes=Concern=/import/nodes/concern.csv \
  --relationships=CONTAINS=/import/edges/contains.csv \
  --relationships=AFFECTS=/import/edges/affects.csv \
  --relationships=RELATES_TO=/import/edges/relates_to.csv \
  --overwrite-destination \
  neo4j

echo "Import done. Run Neo4j e.g.:"
echo "  docker run -d --name neo4j-graphrag -p 7474:7474 -p 7687:7687 \\"
echo "    -v neo4j_graphrag_data:/data -e NEO4J_AUTH=neo4j/password123 \\"
echo "    $IMAGE"
