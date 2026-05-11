#!/usr/bin/env bash
# Bronze → Silver → Gold 파이프라인 순차 실행
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

LOG_DIR="$ROOT/logs"
mkdir -p "$LOG_DIR"
TIMESTAMP=$(date +%Y%m%dT%H%M%S)
LOG="$LOG_DIR/pipeline_$TIMESTAMP.log"

source "$ROOT/venv/bin/activate"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG"; }

log "========================================"
log "파이프라인 시작 (batch=$TIMESTAMP)"
log "타겟 성분 수: $(tail -n +2 config/target_ingredients.csv | wc -l)"
log "========================================"

log "[1/3] Bronze — PubMed 논문 수집"
python -m pipeline.bronze.pubmed.run_bronze 2>&1 | tee -a "$LOG"

log "[2/3] Silver — Abstract Chunking"
python -m pipeline.silver.paper.run_silver 2>&1 | tee -a "$LOG"

log "[3/3] Gold — Claim 추출"
python -m pipeline.gold.claim.run_gold 2>&1 | tee -a "$LOG"

log "========================================"
log "파이프라인 완료. 로그: $LOG"
log "========================================"
