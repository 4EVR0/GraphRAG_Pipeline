#!/usr/bin/env bash
# 타겟 성분을 BATCH_SIZE 단위로 잘라 Bronze → Silver → Gold 순차 실행
# 사용법:
#   bash scripts/run_pipeline_batched.sh [BATCH_SIZE] [FROM_BATCH]
#   bash scripts/run_pipeline_batched.sh 40 5   # 배치 5번부터 재시작
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
source "$ROOT/venv/bin/activate"

BATCH_SIZE="${1:-40}"
FROM_BATCH="${2:-1}"
TARGET_CSV="$ROOT/config/target_ingredients.csv"
BATCH_DIR="$ROOT/config/batches"
LOG_DIR="$ROOT/logs"
mkdir -p "$BATCH_DIR" "$LOG_DIR"

TOTAL=$(tail -n +2 "$TARGET_CSV" | wc -l)
HEADER=$(head -1 "$TARGET_CSV")
NUM_BATCHES=$(( (TOTAL + BATCH_SIZE - 1) / BATCH_SIZE ))

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_DIR/pipeline_main.log"; }

log "========================================================"
log "배치 파이프라인 시작"
log "  전체 성분: ${TOTAL}개  |  배치 크기: ${BATCH_SIZE}  |  총 배치: ${NUM_BATCHES}  |  시작 배치: ${FROM_BATCH}"
log "========================================================"

# ── 배치 CSV 미리 생성 ─────────────────────────────────────
python3 - <<PYEOF
import csv, math
from pathlib import Path

src = Path("$TARGET_CSV")
batch_dir = Path("$BATCH_DIR")
batch_size = $BATCH_SIZE

with open(src, newline='', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    rows = list(reader)
    fieldnames = reader.fieldnames

for i in range(0, len(rows), batch_size):
    chunk = rows[i:i+batch_size]
    batch_num = i // batch_size + 1
    out = batch_dir / f"batch_{batch_num:04d}.csv"
    with open(out, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(chunk)

print(f"배치 CSV {math.ceil(len(rows)/batch_size)}개 생성 완료 → {batch_dir}")
PYEOF

# ── 배치별 실행 ────────────────────────────────────────────
DONE=0
FAILED=0

for BATCH_FILE in $(ls "$BATCH_DIR"/batch_*.csv | sort); do
    BATCH_NUM=$(basename "$BATCH_FILE" .csv | sed 's/batch_//')
    # FROM_BATCH 이전 배치는 건너뜀
    if [ "$((10#$BATCH_NUM))" -lt "$FROM_BATCH" ]; then
        log "배치 ${BATCH_NUM} 건너뜀 (from-batch=${FROM_BATCH})"
        continue
    fi
    BATCH_LOG="$LOG_DIR/batch_${BATCH_NUM}.log"
    INGREDIENT_COUNT=$(tail -n +2 "$BATCH_FILE" | wc -l)

    log "──────────────────────────────────────────────────────"
    log "배치 ${BATCH_NUM}/${NUM_BATCHES} 시작 (성분 ${INGREDIENT_COUNT}개)"

    export TARGET_CSV_PATH="$BATCH_FILE"

    # Bronze
    log "[${BATCH_NUM}] Bronze 시작"
    if python -m pipeline.bronze.pubmed.run_bronze >> "$BATCH_LOG" 2>&1; then
        log "[${BATCH_NUM}] Bronze 완료"
    else
        log "[${BATCH_NUM}] Bronze 실패 — 스킵"
        FAILED=$((FAILED + 1))
        continue
    fi

    # Silver
    log "[${BATCH_NUM}] Silver 시작"
    if python -m pipeline.silver.paper.run_silver >> "$BATCH_LOG" 2>&1; then
        log "[${BATCH_NUM}] Silver 완료"
    else
        log "[${BATCH_NUM}] Silver 실패 — 스킵"
        FAILED=$((FAILED + 1))
        continue
    fi

    # Gold
    log "[${BATCH_NUM}] Gold 시작"
    if python -m pipeline.gold.claim.run_gold >> "$BATCH_LOG" 2>&1; then
        log "[${BATCH_NUM}] Gold 완료"
    else
        log "[${BATCH_NUM}] Gold 실패 — 스킵"
        FAILED=$((FAILED + 1))
        continue
    fi

    DONE=$((DONE + 1))
    log "배치 ${BATCH_NUM} 완료 (누적: ${DONE}/${NUM_BATCHES}, 실패: ${FAILED})"
done

log "========================================================"
log "전체 완료: 성공 ${DONE}, 실패 ${FAILED} / 총 ${NUM_BATCHES} 배치"
log "========================================================"
