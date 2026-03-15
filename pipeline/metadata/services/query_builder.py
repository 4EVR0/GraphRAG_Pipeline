from typing import List


DEFAULT_DOMAIN_TERMS = [
    "skin",
    "topical",
    "cosmetic",
    "cosmeceutical",
    "dermatology",
    "dermatologic",
    "epidermis",
    "epidermal",
    "cutaneous",
    "facial",
]

DEFAULT_CLAIM_HINT_TERMS = [
    "barrier",
    "skin barrier",
    "hydration",
    "hydrating",
    "hydrate",
    "moisturizing",
    "moisturization",
    "transepidermal water loss",
    "TEWL",
    "inflammation",
    "inflammatory",
    "anti-inflammatory",
    "hyperpigmentation",
    "pigmentation",
    "melasma",
    "acne",
    "erythema",
    "redness",
    "wrinkle",
    "wrinkles",
    "elasticity",
    "soothing",
    "calming",
    "irritation",
    "sensitive skin",
    "dry skin",
    "photoaging",
    "skin aging",
    "brightening",
    "depigmenting",
    "wound healing",
    "repair",
    "anti-aging",
    "photoprotection",
    "photoprotective",
]


def parse_pipe_list(value: str | None) -> List[str]:
    if not value:
        return []
    return [item.strip() for item in value.split("|") if item.strip()]


def deduplicate_terms(terms: List[str]) -> List[str]:
    unique_terms: List[str] = []
    seen = set()

    for term in terms:
        normalized = term.lower().strip()
        if normalized and normalized not in seen:
            seen.add(normalized)
            unique_terms.append(term.strip())

    return unique_terms


def build_or_part(terms: List[str]) -> str:
    unique_terms = deduplicate_terms(terms)
    return "(" + " OR ".join([f'"{term}"[Title/Abstract]' for term in unique_terms]) + ")"


def build_ingredient_part(query_name: str, alias_list: str | None = None) -> str:
    terms = [query_name.strip()] + parse_pipe_list(alias_list)
    return build_or_part(terms)


def build_context_part(concern_keywords: str | None = None) -> str:
    concern_terms = parse_pipe_list(concern_keywords)
    all_terms = DEFAULT_DOMAIN_TERMS + concern_terms
    return build_or_part(all_terms)


def build_claim_hint_part(concern_keywords: str | None = None) -> str:
    concern_terms = parse_pipe_list(concern_keywords)
    all_terms = DEFAULT_CLAIM_HINT_TERMS + concern_terms
    return build_or_part(all_terms)


def build_pubmed_query(
    query_name: str,
    alias_list: str | None = None,
    concern_keywords: str | None = None,
) -> str:
    ingredient_part = build_ingredient_part(query_name, alias_list)
    context_part = build_context_part(concern_keywords)
    claim_hint_part = build_claim_hint_part(concern_keywords)

    return f"{ingredient_part} AND {context_part} AND {claim_hint_part}"