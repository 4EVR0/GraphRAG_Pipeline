from typing import List


DEFAULT_DOMAIN_TERMS = [
    "skin",
    "topical",
    "cosmetic",
    "dermatology",
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


def build_ingredient_part(query_name: str, alias_list: str | None = None) -> str:
    terms = [query_name.strip()] + parse_pipe_list(alias_list)
    unique_terms = deduplicate_terms(terms)
    return "(" + " OR ".join([f'"{term}"[Title/Abstract]' for term in unique_terms]) + ")"


def build_context_part(concern_keywords: str | None = None) -> str:
    concern_terms = parse_pipe_list(concern_keywords)
    all_terms = deduplicate_terms(DEFAULT_DOMAIN_TERMS + concern_terms)
    return "(" + " OR ".join([f'"{term}"[Title/Abstract]' for term in all_terms]) + ")"


def build_pubmed_query(
    query_name: str,
    alias_list: str | None = None,
    concern_keywords: str | None = None,
) -> str:
    ingredient_part = build_ingredient_part(query_name, alias_list)
    context_part = build_context_part(concern_keywords)
    return f"{ingredient_part} AND {context_part}"