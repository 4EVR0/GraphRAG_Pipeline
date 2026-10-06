"""검수 전 거르기 (#49).

값싼 모델로 대상 성분의 피부 결과와 무관한 논문만 버린다. 애매하면 남긴다.
거르기에서 버린 논문은 본 검수에 다시 오지 않으므로, 재현율을 따로 측정한다.
client는 openai.OpenAI()와 같은 인터페이스(chat.completions.create)를 받는다.
"""
import hashlib
import json

SCREEN_PROMPT_VERSION = "evidence-screen-v1"
SCREEN_SYSTEM_PROMPT = """You screen PubMed records before a detailed evidence review for a cosmetics recommendation graph.

Keep the record (keep=true) if it might report any outcome of the target ingredient on skin, hair follicles, \
or a skin condition: a clinical study, case report, review summarizing such outcomes, animal or in vitro study \
on skin cells or skin microbes. Also keep it when the target is only one part of a product, peel, regimen, \
or comparator arm.

Drop the record (keep=false) only when it clearly has no such outcome. Examples: analytical or detection methods, \
environmental or food studies, chemical synthesis without biological testing, formulation work without any \
skin or microbial result, or the ingredient appearing only as a background mention.

When unsure, keep it. Give a one-sentence reason."""

SCREEN_SCHEMA = {
    "name": "evidence_screen_result",
    "strict": True,
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {"keep": {"type": "boolean"}, "reason": {"type": "string"}},
        "required": ["keep", "reason"],
    },
}

# 1M 토큰당 표준 가격(USD)
OPENAI_PRICES_PER_MTOK = {
    "gpt-4o-mini": (0.15, 0.60),
    "gpt-4.1-nano": (0.10, 0.40),
    "gpt-4.1-mini": (0.40, 1.60),
    "gpt-5-nano": (0.05, 0.40),
    "gpt-5-mini": (0.25, 2.00),
}


def screen_prompt_sha() -> str:
    payload = json.dumps({"version": SCREEN_PROMPT_VERSION, "system": SCREEN_SYSTEM_PROMPT, "schema": SCREEN_SCHEMA},
                         sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def screen_key(pmid: str, ingredient: str, model: str, sha: str) -> tuple[str, str, str, str]:
    return (str(pmid), ingredient.upper(), model, sha)


def screen_one(client, item, model: str) -> dict:
    """한 (논문, 성분)을 거른다. 응답을 해석할 수 없으면 남긴다(keep=True)."""
    sha = screen_prompt_sha()
    row = {"pmid": item.pmid, "ingredient_inci": item.ingredient, "model": model, "prompt_sha": sha,
           "keep": True, "reason": "", "status": "ok", "usage": {}}
    response = client.chat.completions.create(
        model=model,
        temperature=0.0,
        max_completion_tokens=200,
        response_format={"type": "json_schema", "json_schema": SCREEN_SCHEMA},
        messages=[
            {"role": "system", "content": SCREEN_SYSTEM_PROMPT},
            {"role": "user", "content": f"Target ingredient: {item.ingredient}\n\n<record>\nTitle: {item.title}\n\n{item.source_text}\n</record>"},
        ],
    )
    usage = getattr(response, "usage", None)
    row["usage"] = {"input_tokens": int(getattr(usage, "prompt_tokens", 0) or 0),
                    "output_tokens": int(getattr(usage, "completion_tokens", 0) or 0)}
    message = response.choices[0].message
    if getattr(message, "refusal", None):
        row.update(status="refusal", reason=str(message.refusal))
        return row
    try:
        payload = json.loads(message.content or "")
        row.update(keep=bool(payload["keep"]), reason=str(payload.get("reason", "")))
    except (json.JSONDecodeError, KeyError, TypeError) as exc:
        row.update(status="invalid_json", reason=str(exc))
    return row


def screen_cost_usd(model: str, usage: dict) -> float:
    input_price, output_price = OPENAI_PRICES_PER_MTOK[model]
    return (usage.get("input_tokens", 0) * input_price + usage.get("output_tokens", 0) * output_price) / 1_000_000
