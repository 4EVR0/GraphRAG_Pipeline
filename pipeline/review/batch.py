"""검수 요청 생성, Message Batches 제출과 결과 수집 (#49).

client는 anthropic.Anthropic()과 같은 인터페이스(messages.batches.create/retrieve/results)를 받는다.
테스트에서는 가짜 client를 넣는다.
"""
import re
import time
from dataclasses import dataclass

from pipeline.review.schema import OUTPUT_SCHEMA, SYSTEM_PROMPT, prompt_sha, user_message

# 1M 토큰당 표준 가격(USD). Batches는 이 가격의 50%다.
PRICES_PER_MTOK = {
    "claude-fable-5-1": (10.0, 50.0),
    "claude-opus-5-5": (4.0, 20.0),
    "claude-sonnet-5-5": (2.0, 10.0),
    "claude-haiku-4-5": (1.0, 5.0),
}
BATCH_DISCOUNT = 0.5
# effort를 받지 않는 모델(Haiku 4.5는 effort를 보내면 오류)
NO_EFFORT_MODELS = frozenset({"claude-haiku-4-5"})
DEFAULT_MAX_TOKENS = 16000


@dataclass(frozen=True)
class ReviewItem:
    pmid: str
    ingredient: str
    title: str
    source_text: str
    source: str = "abstract"


def review_key(pmid: str, ingredient: str, model: str, sha: str) -> tuple[str, str, str, str]:
    return (str(pmid), ingredient.upper(), model, sha)


def custom_id(item: ReviewItem, sha: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9]+", "-", item.ingredient).strip("-")[:24]
    return f"{item.pmid}_{slug}_{sha[:8]}"


def request_params(item: ReviewItem, model: str, effort: str | None) -> dict:
    params = {
        "model": model,
        "max_tokens": DEFAULT_MAX_TOKENS,
        "system": SYSTEM_PROMPT,
        "messages": [{"role": "user", "content": user_message(item.ingredient, item.title, item.source_text)}],
        "output_config": {"format": {"type": "json_schema", "schema": OUTPUT_SCHEMA}},
    }
    if effort and model not in NO_EFFORT_MODELS:
        params["output_config"]["effort"] = effort
    return params


def build_requests(
    items: list[ReviewItem],
    model: str,
    effort: str | None,
    done_keys: set[tuple[str, str, str, str]] | None = None,
) -> tuple[list[dict], list[ReviewItem]]:
    """이미 판정한 (PMID, 성분, 모델, prompt_sha)는 건너뛰고 Batch 요청을 만든다."""
    sha = prompt_sha()
    done_keys = done_keys or set()
    requests, skipped = [], []
    seen: set[str] = set()
    for item in items:
        if review_key(item.pmid, item.ingredient, model, sha) in done_keys:
            skipped.append(item)
            continue
        cid = custom_id(item, sha)
        if cid in seen:
            continue
        seen.add(cid)
        requests.append({"custom_id": cid, "params": request_params(item, model, effort)})
    return requests, skipped


def submit(client, requests: list[dict]) -> str:
    """Batch 생성은 멱등이 아니다. 응답을 못 받은 채 재시도하면 같은 Batch가 여러 개 생기므로
    자동 재시도를 끈다. 연결 오류가 나면 batches.list로 생성 여부를 먼저 확인한다."""
    batch = client.with_options(max_retries=0).messages.batches.create(requests=requests)
    return batch.id


def wait(client, batch_id: str, poll_seconds: float = 60.0, timeout_seconds: float = 24 * 3600) -> object:
    deadline = time.monotonic() + timeout_seconds
    while True:
        batch = client.messages.batches.retrieve(batch_id)
        if batch.processing_status == "ended":
            return batch
        if time.monotonic() > deadline:
            raise TimeoutError(f"batch {batch_id} not ended after {timeout_seconds}s")
        time.sleep(poll_seconds)


def _usage_dict(usage) -> dict:
    fields = ("input_tokens", "output_tokens", "cache_creation_input_tokens", "cache_read_input_tokens")
    return {name: int(getattr(usage, name, 0) or 0) for name in fields}


def message_fields(message) -> dict:
    stop_details = getattr(message, "stop_details", None)
    return {
        "model": message.model,
        "stop_reason": message.stop_reason,
        "refusal_category": getattr(stop_details, "category", None) if stop_details else None,
        "text": next((b.text for b in message.content if b.type == "text"), ""),
        "usage": _usage_dict(message.usage),
    }


def run_sync(client, requests: list[dict], on_result=None) -> list[dict]:
    """Batch가 없는 게이트웨이용: 요청을 한 건씩 보내고 Batch 결과와 같은 형식으로 돌려준다."""
    import anthropic

    rows = []
    for request in requests:
        row = {"custom_id": request["custom_id"], "batch_id": None}
        try:
            message = client.messages.create(**request["params"])
            row.update(result_type="succeeded", **message_fields(message))
        except anthropic.APIStatusError as exc:
            row.update(result_type="errored", error=f"{exc.status_code}: {str(exc)[:500]}")
        except anthropic.APIConnectionError as exc:
            row.update(result_type="errored", error=f"connection: {exc}")
        rows.append(row)
        if on_result:
            on_result(row)
    return rows


def collect(client, batch_id: str) -> list[dict]:
    """결과를 custom_id 기준 레코드로 바꾼다. 결과 순서는 보장되지 않는다."""
    rows = []
    for result in client.messages.batches.results(batch_id):
        row = {"custom_id": result.custom_id, "batch_id": batch_id, "result_type": result.result.type}
        if result.result.type == "succeeded":
            row.update(message_fields(result.result.message))
        elif result.result.type == "errored":
            row["error"] = str(getattr(result.result, "error", ""))
        rows.append(row)
    return rows


def cost_usd(model: str, usage: dict, batch: bool = True) -> float:
    input_price, output_price = PRICES_PER_MTOK[model]
    cache_write = usage.get("cache_creation_input_tokens", 0) * input_price * 1.25
    cache_read = usage.get("cache_read_input_tokens", 0) * input_price * 0.1
    total = (usage.get("input_tokens", 0) * input_price + usage.get("output_tokens", 0) * output_price
             + cache_write + cache_read) / 1_000_000
    return total * (BATCH_DISCOUNT if batch else 1.0)
