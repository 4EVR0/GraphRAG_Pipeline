"""검수 결과 검증과 판정 레코드 생성 (#49).

- 응답 거절·잘림·JSON 오류는 사람 검수 큐로 보낸다.
- 허용값이 아니거나 인용 문장이 원문에 없으면 그 판정을 사람 검수 큐로 보낸다.
"""
import json
import re
import unicodedata
from datetime import datetime, timezone

from pipeline.review.batch import ReviewItem
from pipeline.review.schema import (
    ATTRIBUTIONS,
    DIRECTIONS,
    EFFECT_CODES,
    ROUTES,
    SOURCES,
    STUDY_TYPES,
)

_ALLOWED = {
    "effect_code": EFFECT_CODES,
    "attribution": ATTRIBUTIONS,
    "route": ROUTES,
    "study_type": STUDY_TYPES,
    "direction": DIRECTIONS,
}
_PUNCT = str.maketrans({
    "‘": "'", "’": "'", "“": '"', "”": '"',
    "–": "-", "—": "-", "−": "-", " ": " ",
})


def normalize(text: str) -> str:
    """대조용 정규화: 유니코드 호환 형태, 따옴표·대시 통일, 공백 축약."""
    text = unicodedata.normalize("NFKC", text or "").translate(_PUNCT)
    return re.sub(r"\s+", " ", text).strip()


def quote_in_source(quote: str, title: str, source_text: str) -> bool:
    quote = normalize(quote)
    if not quote:
        return False
    haystack = normalize(f"{title}\n{source_text}")
    return quote in haystack or quote.rstrip(".") in haystack


def _human(item: ReviewItem, model: str, sha: str, reason: str, detail: str = "") -> dict:
    return {"pmid": item.pmid, "ingredient_inci": item.ingredient, "model": model,
            "prompt_sha": sha, "reason": reason, "detail": detail}


def judge(
    item: ReviewItem,
    result: dict,
    model: str,
    sha: str,
    reviewed_at: str | None = None,
) -> tuple[list[dict], list[dict], dict]:
    """한 결과를 판정 레코드, 사람 검수 큐, 논문 단위 요약으로 나눈다."""
    reviewed_at = reviewed_at or datetime.now(timezone.utc).isoformat()
    summary = {"pmid": item.pmid, "ingredient_inci": item.ingredient, "model": model, "prompt_sha": sha,
               "status": "ok", "relevant": None, "needs_fulltext": None, "reason": "",
               "judgments": 0, "quote_failures": 0, "invalid_values": 0, "usage": result.get("usage", {})}
    if result.get("result_type") != "succeeded":
        summary["status"] = f"batch_{result.get('result_type')}"
        return [], [_human(item, model, sha, summary["status"], result.get("error", ""))], summary
    if result.get("stop_reason") == "refusal":
        summary["status"] = "refusal"
        return [], [_human(item, model, sha, "refusal", str(result.get("refusal_category")))], summary
    if result.get("stop_reason") == "max_tokens":
        summary["status"] = "max_tokens"
        return [], [_human(item, model, sha, "max_tokens")], summary
    try:
        payload = json.loads(result.get("text") or "")
    except json.JSONDecodeError as exc:
        summary["status"] = "invalid_json"
        return [], [_human(item, model, sha, "invalid_json", str(exc))], summary

    summary.update(relevant=payload.get("relevant"), needs_fulltext=payload.get("needs_fulltext"),
                   reason=payload.get("reason", ""))
    records, queue = [], []
    if payload.get("needs_fulltext"):
        queue.append(_human(item, model, sha, "needs_fulltext", payload.get("reason", "")))
    for judgment in payload.get("judgments", []):
        summary["judgments"] += 1
        bad = [f"{k}={judgment.get(k)!r}" for k, allowed in _ALLOWED.items() if judgment.get(k) not in allowed]
        quote_ok = quote_in_source(judgment.get("evidence_quote", ""), item.title, item.source_text)
        record = {
            "pmid": item.pmid,
            "ingredient_inci": item.ingredient,
            **{k: judgment.get(k) for k in ("effect_code", "attribution", "route", "concentration",
                                             "population", "study_type", "direction", "evidence_quote")},
            "source": item.source if item.source in SOURCES else "abstract",
            "model": model,
            "prompt_sha": sha,
            "reviewed_at": reviewed_at,
            "human_verdict": None,
            "quote_verified": quote_ok,
            "values_valid": not bad,
        }
        records.append(record)
        if bad:
            summary["invalid_values"] += 1
            queue.append(_human(item, model, sha, "invalid_value", "; ".join(bad)))
        if not quote_ok:
            summary["quote_failures"] += 1
            queue.append(_human(item, model, sha, "quote_not_in_source", judgment.get("evidence_quote", "")[:300]))
    return records, queue, summary
