from datetime import datetime, timezone


def build_batch_id() -> str:
    """UTC 기준 배치 ID를 생성한다. 형식: YYYYMMDD_HHMMSS"""
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
