#!/usr/bin/env bash
# 기존 silver 데이터로 gold layer만 재실행합니다.
# bronze / silver 재실행 없이 gold scoring 수정 후 빠르게 재처리할 때 사용.
# 사용법:
#   bash scripts/rerun_gold_only.sh [FROM_BATCH_IDX]
#   bash scripts/rerun_gold_only.sh 5   # 5번째 silver 배치부터 재시작
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
source "$ROOT/venv/bin/activate"

FROM_IDX="${1:-1}"
LOG_DIR="$ROOT/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/rerun_gold.log"
DETAIL_LOG="$LOG_DIR/rerun_gold_detail.log"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"; }

SILVER_DIR="$ROOT/silver/paper"
BATCH_CSV_DIR="$ROOT/config/batches"
mapfile -t BATCHES < <(ls "$SILVER_DIR" | grep '^batch=' | sort)
mapfile -t BATCH_CSVS < <(ls "$BATCH_CSV_DIR" | grep '^batch_.*\.csv$' | sort)
TOTAL=${#BATCHES[@]}

log "========================================================"
log "Gold 재실행 시작 (배치별 40개 CSV 사용)"
log "  silver 배치: ${TOTAL}개  |  시작 인덱스: ${FROM_IDX}"
log "========================================================"

DONE=0
FAILED=0

for IDX in $(seq 1 $TOTAL); do
    if [ "$IDX" -lt "$FROM_IDX" ]; then
        continue
    fi

    BATCH_DIR_NAME="${BATCHES[$((IDX-1))]}"
    BATCH_ID="${BATCH_DIR_NAME#batch=}"
    BATCH_CSV="$BATCH_CSV_DIR/${BATCH_CSVS[$((IDX-1))]}"

    if [ ! -f "$BATCH_CSV" ]; then
        log "  [경고] batch CSV 없음: $BATCH_CSV — 전체 CSV 사용"
        BATCH_CSV="$ROOT/config/target_ingredients.csv"
    fi

    log "──────────────────────────────────────────────────────"
    log "[$IDX/$TOTAL] Gold 실행: $BATCH_ID  (CSV: $(basename $BATCH_CSV))"

    if TARGET_CSV_PATH="$BATCH_CSV" SILVER_BATCH_ID="$BATCH_ID" \
        python -m pipeline.gold.claim.run_gold \
        >> "$DETAIL_LOG" 2>&1; then
        DONE=$((DONE+1))
        log "  완료 (성공: $DONE, 실패: $FAILED)"
    else
        FAILED=$((FAILED+1))
        log "  실패: $BATCH_ID (계속 진행)"
    fi
done

log "========================================================"
log "전체 완료: 성공 ${DONE}, 실패 ${FAILED} / 총 ${TOTAL} silver 배치"
log "========================================================"
