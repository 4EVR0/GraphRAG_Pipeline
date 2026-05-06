"""
Common pipeline logging helpers.

The default unit is a job-level lifecycle log:
    [START] job=graph_sync run_id=20260506_1200
    [END] job=graph_sync status=SUCCESS duration=43s
    [FAIL] job=graph_sync task=upsert_edges error=timeout

Processing, data error, and audit logs use the same key=value shape:
    [PROCESS] job=graph_sync run_id=20260506_1200 pending_events=1200 ...
    [DATA_ERROR] level=ERROR job=ingredient_mapping run_id=20260506_1200 ...
    [AUDIT] run_id=20260506_1200 job_version=v1 git_commit_hash=abc123 ...
"""

from __future__ import annotations

import logging
import time
import json
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator, Optional


def log_job_start(
    logger: logging.Logger,
    job: str,
    run_id: Optional[str] = None,
    **fields: Any,
) -> None:
    """Log the start of a job unit."""
    logger.info(_format_log("START", job=job, run_id=run_id, **fields))


def log_job_end(
    logger: logging.Logger,
    job: str,
    status: str = "SUCCESS",
    duration: Optional[str] = None,
    **fields: Any,
) -> None:
    """Log the successful end of a job unit."""
    logger.info(_format_log("END", job=job, status=status, duration=duration, **fields))


def log_job_fail(
    logger: logging.Logger,
    job: str,
    task: Optional[str] = None,
    error: Optional[Any] = None,
    **fields: Any,
) -> None:
    """Log a failed job unit."""
    logger.error(_format_log("FAIL", job=job, task=task, error=error, **fields))


def log_process_summary(
    logger: logging.Logger,
    job: str,
    run_id: str,
    pending_events: Optional[int] = None,
    processed_events: Optional[int] = None,
    failed_events: Optional[int] = None,
    upserted_nodes: Optional[int] = None,
    upserted_edges: Optional[int] = None,
    deactivated_edges: Optional[int] = None,
    duration: Optional[str] = None,
    **fields: Any,
) -> None:
    """Log a data processing summary for one job run."""
    logger.info(
        _format_log(
            "PROCESS",
            job=job,
            run_id=run_id,
            pending_events=pending_events,
            processed_events=processed_events,
            failed_events=failed_events,
            upserted_nodes=upserted_nodes,
            upserted_edges=upserted_edges,
            deactivated_edges=deactivated_edges,
            duration=duration,
            **fields,
        )
    )


def log_data_error(
    logger: logging.Logger,
    job: str,
    run_id: str,
    table: str,
    row_id: Any,
    column: str,
    value: Any,
    error_type: str,
    message: str,
    **fields: Any,
) -> None:
    """Log a row-level data processing error."""
    logger.error(
        _format_log(
            "DATA_ERROR",
            level="ERROR",
            job=job,
            run_id=run_id,
            table=table,
            row_id=row_id,
            column=column,
            value=_quoted(value),
            error_type=error_type,
            message=_quoted(message),
            **fields,
        )
    )


def log_audit(
    logger: logging.Logger,
    run_id: str,
    job_version: str,
    git_commit_hash: str,
    source_table: str,
    source_snapshot_id: Any,
    target_table: str,
    target_snapshot_id: Any,
    created_at: Optional[str] = None,
    created_by: Optional[str] = None,
    **fields: Any,
) -> None:
    """Log an audit event for table lineage and reproducibility."""
    logger.info(
        _format_log(
            "AUDIT",
            run_id=run_id,
            job_version=job_version,
            git_commit_hash=git_commit_hash,
            source_table=source_table,
            source_snapshot_id=source_snapshot_id,
            target_table=target_table,
            target_snapshot_id=target_snapshot_id,
            created_at=created_at or _utc_now(),
            created_by=created_by,
            **fields,
        )
    )


@contextmanager
def job_unit(
    logger: logging.Logger,
    job: str,
    run_id: Optional[str] = None,
    task: Optional[str] = None,
    **fields: Any,
) -> Iterator[None]:
    """
    Log START/END/FAIL around a job block.

    Exceptions are logged as FAIL and then re-raised.
    """
    started_at = time.monotonic()
    log_job_start(logger, job=job, run_id=run_id, **fields)
    try:
        yield
    except Exception as exc:
        log_job_fail(logger, job=job, task=task, error=exc)
        raise
    else:
        log_job_end(
            logger,
            job=job,
            status="SUCCESS",
            duration=_format_duration(time.monotonic() - started_at),
        )


def _format_log(event: str, **fields: Any) -> str:
    pairs = " ".join(
        f"{key}={_format_value(value)}"
        for key, value in fields.items()
        if value is not None
    )
    if pairs:
        return f"[{event}] {pairs}"
    return f"[{event}]"


def _format_value(value: Any) -> str:
    if isinstance(value, _RawValue):
        return str(value)
    text = str(value)
    if not text:
        return '""'
    if any(char.isspace() for char in text):
        return json.dumps(text, ensure_ascii=False)
    return text


def _format_duration(seconds: float) -> str:
    if seconds < 1:
        return f"{seconds:.3f}s"
    return f"{seconds:.0f}s"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class _RawValue(str):
    pass


def _quoted(value: Any) -> _RawValue:
    return _RawValue(json.dumps(str(value), ensure_ascii=False))
