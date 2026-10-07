"""검수 전 거르기 (#49).

값싼 모델로 대상 성분의 피부 결과와 무관한 논문만 버린다. 애매하면 남긴다.
거르기에서 버린 논문은 본 검수에 다시 오지 않으므로, 재현율을 따로 측정한다.
client는 openai.OpenAI()와 같은 인터페이스(chat.completions.create)를 받는다.
"""
import hashlib
import json

# 2026-10-06 비교(500건 표본)로 gpt-5-mini + v2를 기본으로 정했다.
SCREEN_PROMPT_VERSION = "evidence-screen-v2"
DEFAULT_SCREEN_MODEL = "gpt-5-mini"
SCREEN_SYSTEM_PROMPT_V1 = """You screen PubMed records before a detailed evidence review for a cosmetics recommendation graph.

Keep the record (keep=true) if it might report any outcome of the target ingredient on skin, hair follicles, \
or a skin condition: a clinical study, case report, review summarizing such outcomes, animal or in vitro study \
on skin cells or skin microbes. Also keep it when the target is only one part of a product, peel, regimen, \
or comparator arm.

Drop the record (keep=false) only when it clearly has no such outcome. Examples: analytical or detection methods, \
environmental or food studies, chemical synthesis without biological testing, formulation work without any \
skin or microbial result, or the ingredient appearing only as a background mention.

When unsure, keep it. Give a one-sentence reason."""

# v1은 "피부 연구인가"만 보고 남겨, 성분이 측정값·다른 물질 이름·부형제로만 나오는 논문까지 남겼다.
SCREEN_SYSTEM_PROMPT = """You screen PubMed records before a detailed evidence review for a cosmetics recommendation graph.

Keep the record (keep=true) only if the target ingredient itself is used in the study: applied, ingested, \
injected, or tested as a substance, including as one part of a product, peel, or regimen, or as a comparator \
or positive control arm. The study must also report an outcome on skin, skin cells, hair follicles, skin \
microbes, or a skin condition. Clinical studies, case reports, animal and in vitro studies, and reviews \
that summarize such outcomes all count.

Drop the record (keep=false) when any of these holds:
- The target is only measured or discussed as a substance the body makes (for example skin melanin, \
cholesterol in skin lipids, procollagen synthesis by cells, serum glucose), not given as a treatment.
- The target name only appears inside another term (for example "serine protease", "cyclic adenosine \
monophosphate", "poly(lactic-co-glycolic acid)", "lactic acid bacteria").
- The target only serves as a vehicle, solvent, penetration enhancer, or carrier for another active, \
and no effect of the target itself is reported.
- The outcome is not on skin (for example gut, lung, kidney, eye, or blood), or the study is an analytical \
method, environmental, food, or synthesis study without biological testing.

If the record fits none of the drop rules but you are still unsure, keep it. Give a one-sentence reason."""

# gpt-4o-mini + v2가 비교 대조·양성 대조 성분을 버려 문장을 더했으나, gpt-4o-mini에서는 효과가 없었다(비교 기록용).
SCREEN_SYSTEM_PROMPT_V3 = SCREEN_SYSTEM_PROMPT.replace(
    "If the record fits none of the drop rules",
    "Being only a comparator, reference compound, or positive control is not a reason to drop: keep the record "
    "when the target arm or control has a reported result.\n\nIf the record fits none of the drop rules",
)

SCREEN_PROMPTS = {
    "evidence-screen-v1": SCREEN_SYSTEM_PROMPT_V1,
    "evidence-screen-v2": SCREEN_SYSTEM_PROMPT,
    "evidence-screen-v3": SCREEN_SYSTEM_PROMPT_V3,
}

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


def screen_prompt_sha(version: str = SCREEN_PROMPT_VERSION) -> str:
    payload = json.dumps({"version": version, "system": SCREEN_PROMPTS[version], "schema": SCREEN_SCHEMA},
                         sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def screen_key(pmid: str, ingredient: str, model: str, sha: str) -> tuple[str, str, str, str]:
    return (str(pmid), ingredient.upper(), model, sha)


def screen_one(client, item, model: str, version: str = SCREEN_PROMPT_VERSION) -> dict:
    """한 (논문, 성분)을 거른다. 응답을 해석할 수 없으면 남긴다(keep=True)."""
    sha = screen_prompt_sha(version)
    row = {"pmid": item.pmid, "ingredient_inci": item.ingredient, "model": model, "prompt_version": version,
           "prompt_sha": sha, "keep": True, "reason": "", "status": "ok", "usage": {}}
    # gpt-5 계열은 temperature 기본값만 받고, 사고 토큰이 출력 한도에 포함된다.
    reasoning = model.startswith("gpt-5")
    sampling = {"max_completion_tokens": 4000} if reasoning else {"temperature": 0.0, "max_completion_tokens": 200}
    response = client.chat.completions.create(
        model=model,
        **sampling,
        response_format={"type": "json_schema", "json_schema": SCREEN_SCHEMA},
        messages=[
            {"role": "system", "content": SCREEN_PROMPTS[version]},
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
