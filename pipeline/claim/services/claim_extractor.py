import csv
import re
from typing import Dict, List, Optional

from pipeline.common.config.settings import settings


class ClaimExtractor:
    def __init__(self) -> None:
        self.ingredient_rules = self._load_ingredient_rules()
        self.allowed_canonical_ingredients = set(self.ingredient_rules.keys())

    def _split_pipe_field(self, value: str) -> List[str]:
        if not value:
            return []
        return [item.strip() for item in value.split("|") if item.strip()]

    def _normalize_unique(self, items: List[str]) -> List[str]:
        unique_items: List[str] = []
        seen = set()

        for item in items:
            key = item.lower().strip()
            if not key:
                continue
            if key in seen:
                continue
            seen.add(key)
            unique_items.append(item.strip())

        return unique_items

    def _load_ingredient_rules(self) -> Dict[str, Dict[str, List[str]]]:
        rules: Dict[str, Dict[str, List[str]]] = {}

        with open(settings.target_csv_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)

            for row in reader:
                if row.get("is_target", "").strip().lower() != "true":
                    continue

                canonical_name = row["canonical_name"].strip()
                query_name = row.get("query_name", "").strip()
                alias_list = row.get("alias_list", "")
                exclude_if_contains = row.get("exclude_if_contains", "")

                aliases = [canonical_name]
                if query_name:
                    aliases.append(query_name)
                aliases.extend(self._split_pipe_field(alias_list))
                aliases = self._normalize_unique(aliases)

                excludes = self._split_pipe_field(exclude_if_contains)
                excludes = self._normalize_unique(excludes)

                rules[canonical_name] = {
                    "aliases": aliases,
                    "excludes": excludes,
                }

        return rules

    def _contains_term(self, text: str, term: str) -> bool:
        pattern = r"\b" + re.escape(term.lower()) + r"\b"
        return re.search(pattern, text.lower()) is not None

    def _contains_exclude_pattern(self, text: str, pattern: str) -> bool:
        return pattern.lower() in text.lower()

    def _has_skin_context(self, text: str) -> bool:
        lower = text.lower()

        skin_terms = [
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
            "barrier",
            "skin barrier",
            "hydration",
            "hydrate",
            "hydrating",
            "moisturizing",
            "moisturization",
            "transepidermal water loss",
            "tewl",
            "pigmentation",
            "hyperpigmentation",
            "melasma",
            "pih",
            "photoaging",
            "photodamaged",
            "photo-damaged",
            "uv",
            "uvb",
            "erythema",
            "redness",
            "acne",
            "wrinkle",
            "wrinkles",
            "elasticity",
            "irritation",
            "sensitive skin",
            "dry skin",
            "brightening",
            "depigmenting",
            "wound healing",
            "repair",
            "anti-aging",
            "laser-induced pih",
            "infraorbital hyperpigmentation",
            "immunosuppression",
        ]
        return any(term in lower for term in skin_terms)

    def _is_blocked_non_cosmetic_domain(self, text: str) -> bool:
        """
        피부/화장품 추천 목적과 명확히 동떨어진 문맥만 차단한다.
        너무 공격적으로 막으면 좋은 피부 논문도 날아가므로 최소 차단만 적용.
        """
        lower = text.lower()

        blocked_terms = [
            "perioperative",
            "cardiac surgery",
            "ostomy",
            "payer",
            "population perspective",
            "cost-effective",
            "cost effective",
            "cost-saving",
            "cost saving",
            "quality-of-life",
            "quality of life",
            "seroma risk",
            "postoperative",
            "surgical drain",
            "breast reconstruction",
        ]

        return any(term in lower for term in blocked_terms)

    def _is_results_or_conclusion_sentence(self, text: str) -> bool:
        lower = text.lower().strip()

        return (
            lower.startswith("results:")
            or lower.startswith("result:")
            or lower.startswith("conclusion:")
            or lower.startswith("conclusions:")
        )

    def _is_study_design_sentence(self, text: str) -> bool:
        """
        목표/배경/방법 문장만 차단한다.
        RESULTS/CONCLUSION 문장은 차단하지 않는다.
        """
        lower = text.lower().strip()

        if self._is_results_or_conclusion_sentence(text):
            return False

        blocked_prefixes = [
            "aim:",
            "aims:",
            "objective:",
            "objectives:",
            "background:",
            "purpose:",
            "purposes:",
            "methods:",
            "method:",
            "materials and methods:",
        ]

        if any(lower.startswith(prefix) for prefix in blocked_prefixes):
            return True

        blocked_phrases = [
            "this study aimed to",
            "this study aims to",
            "this study was designed to",
            "this review aims to",
            "to assess the efficacy",
            "to assess the effectiveness",
            "to evaluate the efficacy",
            "to evaluate the safety",
            "to develop and characterize",
            "to develop a",
            "patients were divided into",
            "were randomized to",
            "in a further approach",
            "we developed",
        ]

        return any(phrase in lower for phrase in blocked_phrases)

    def _has_positive_signal(self, text: str) -> bool:
        lower = text.lower()

        positive_signals = [
            "improved",
            "improves",
            "improvement",
            "reduced",
            "reduces",
            "reduction",
            "increase",
            "increased",
            "increases",
            "enhanced",
            "enhances",
            "effective",
            "efficacy",
            "beneficial",
            "ameliorated",
            "alleviated",
            "prevented",
            "prevents",
            "significantly",
            "associated with",
            "attenuated",
            "restored",
            "promoted",
            "demonstrated",
            "showed",
            "shown",
            "showing",
            "resulted in",
            "led to",
            "improvement in",
            "superior improvements",
            "confer superior improvements",
            "relieves",
            "mitigate",
            "mitigates",
            "stimulates",
            "healing",
            "recovery",
            "well-tolerated",
            "well tolerated",
            "tolerability",
            "promising",
            "therapeutic option",
            "offer greater protection",
            "protective",
            "show promise",
            "appears to be",
            "appear to be",
            "no remarkable side effects",
            "no significant differences in safety outcomes",
            "impressive modalities",
            "lowered",
            "reduced melasma severity",
            "improve hyperpigmentation",
            "improvement in melasma scores",
        ]

        if self._is_results_or_conclusion_sentence(text):
            return True

        return any(term in lower for term in positive_signals)

    def is_claim_like_sentence(self, text: str) -> bool:
        text = text.strip()
        if not text:
            return False

        if self._is_blocked_non_cosmetic_domain(text):
            return False

        if self._is_study_design_sentence(text):
            return False

        has_positive_signal = self._has_positive_signal(text)
        has_skin_context = self._has_skin_context(text)

        return has_positive_signal and has_skin_context

    def _is_niacinamide_context_valid(self, text: str) -> bool:
        lower = text.lower()

        bad_terms = [
            "nad",
            "nad+",
            "nadh",
            "nadp",
            "nadph",
            "nicotinamide adenine dinucleotide",
            "nicotinamide riboside",
            "nicotinamide mononucleotide",
            "nmn",
            "nr",
            "coenzyme",
            "mitochondria",
            "mitochondrial",
            "metabolism",
            "metabolic",
            "bioenergetic",
        ]

        if any(term in lower for term in bad_terms):
            return False

        # nicotinamide/niacinamide는 피부/광손상/색소/장벽 문맥이면 허용
        required_context_terms = [
            "skin",
            "topical",
            "uv",
            "uvb",
            "photoaging",
            "photodamaged",
            "photo-damaged",
            "hyperpigmentation",
            "pigmentation",
            "melasma",
            "barrier",
            "immunosuppression",
            "erythema",
            "acne",
            "facial",
        ]

        has_required_context = any(term in lower for term in required_context_terms)
        return has_required_context

    def _passes_special_context_rule(self, canonical_name: str, text: str) -> bool:
        canonical_lower = canonical_name.lower()

        if canonical_lower == "niacinamide":
            return self._is_niacinamide_context_valid(text)

        return True

    def extract_ingredient_names(self, text: str) -> List[str]:
        text = text.strip()
        if not text:
            return []

        if not self.is_claim_like_sentence(text):
            return []

        found: List[str] = []

        for canonical_name, rule in self.ingredient_rules.items():
            aliases = rule["aliases"]
            excludes = rule["excludes"]

            alias_matched = any(self._contains_term(text, alias) for alias in aliases)
            if not alias_matched:
                continue

            exclude_matched = any(
                self._contains_exclude_pattern(text, exclude_pattern)
                for exclude_pattern in excludes
            )
            if exclude_matched:
                continue

            if not self._passes_special_context_rule(canonical_name, text):
                continue

            found.append(canonical_name)

        return found

    def normalize_ingredient_name(self, ingredient_name: str) -> Optional[str]:
        """
        LLM이 반환한 ingredient를 canonical ingredient로 정규화한다.
        우선순위:
        1) canonical exact match
        2) alias exact match
        """
        raw = ingredient_name.strip()
        if not raw:
            return None

        for canonical_name in self.allowed_canonical_ingredients:
            if raw.lower() == canonical_name.lower():
                return canonical_name

        for canonical_name, rule in self.ingredient_rules.items():
            for alias in rule["aliases"]:
                if raw.lower() == alias.lower():
                    return canonical_name

        return None

    def is_allowed_ingredient(self, ingredient_name: str) -> bool:
        return ingredient_name.strip() in self.allowed_canonical_ingredients

    def get_allowed_ingredient_names(self) -> List[str]:
        return sorted(self.allowed_canonical_ingredients)

    def extract_effect_ids(self, text: str, effect_rows: List[Dict]) -> List[int]:
        lower = text.lower()
        matched_effect_ids: List[int] = []

        synonym_map = {
            "ANTI_INFLAMMATORY": ["anti-inflammatory", "inflammation", "inflammatory"],
            "SOOTHING": ["soothing", "calming", "redness", "irritation"],
            "BARRIER_REPAIR": ["barrier", "barrier repair", "skin barrier"],
            "HYDRATING": ["hydration", "hydrating", "hydrate", "moisturizing"],
            "MOISTURE_RETENTION": ["transepidermal water loss", "tewl", "moisture retention"],
            "SEBUM_REGULATION": ["sebum", "oil control", "oily skin"],
            "KERATOLYTIC": ["keratolytic", "exfoliation", "peeling"],
            "COMEDOLYTIC": ["comedone", "comedones", "blackhead", "whitehead"],
            "ANTIMICROBIAL": ["antimicrobial", "antibacterial", "microbial"],
            "DEPIGMENTING": ["depigment", "melasma", "hyperpigmentation", "pigmentation", "pih"],
            "BRIGHTENING": ["brightening", "skin tone", "dyschromia"],
            "ANTIOXIDANT": ["antioxidant", "oxidative stress"],
            "WOUND_HEALING": ["wound healing", "healing", "repair"],
            "ANTI_AGING": ["anti-aging", "wrinkle", "elasticity", "aging", "photoaging"],
            "PHOTOPROTECTIVE": ["photoprotective", "uv", "uvb", "photoaging", "photodamaged"],
        }

        for row in effect_rows:
            effect_id = row["effect_id"]
            effect_code = row["effect_code"]
            effect_name_en = row["effect_name_en"].lower()

            candidates = [effect_name_en] + synonym_map.get(effect_code, [])
            if any(candidate in lower for candidate in candidates):
                matched_effect_ids.append(effect_id)

        return list(sorted(set(matched_effect_ids)))

    def extract_concern_ids(self, text: str, concern_rows: List[Dict]) -> List[int]:
        lower = text.lower()
        matched_concern_ids: List[int] = []

        synonym_map = {
            "ACNE": ["acne", "acne vulgaris", "pimple"],
            "COMEDONES": ["comedone", "comedones", "blackhead", "whitehead"],
            "OILY_SKIN": ["oily skin", "sebum"],
            "SENSITIVE_SKIN": ["sensitive skin", "sensitivity"],
            "REDNESS": ["redness", "erythema"],
            "IRRITATED_SKIN": ["irritation", "stinging", "burning"],
            "DRY_SKIN": ["dry skin", "xerosis"],
            "DEHYDRATED_SKIN": ["dehydrated skin", "dehydration"],
            "BARRIER_DAMAGE": ["barrier", "skin barrier", "tewl"],
            "HYPERPIGMENTATION": [
                "hyperpigmentation",
                "melasma",
                "pigmentation",
                "pih",
                "infraorbital hyperpigmentation",
                "laser-induced pih",
            ],
            "DULLNESS": ["dullness", "uneven skin tone"],
            "AGING_SIGNS": ["aging", "wrinkle", "elasticity", "photoaging", "photodamaged"],
            "ATOPIC_PRONE": ["atopic", "atopic dermatitis"],
            "ROSACEA_PRONE": ["rosacea"],
            "POST_ACNE_MARKS": ["post-acne", "post inflammatory hyperpigmentation", "pih"],
        }

        for row in concern_rows:
            concern_id = row["concern_id"]
            concern_code = row["concern_code"]
            concern_name_en = row["concern_name_en"].lower()

            candidates = [concern_name_en] + synonym_map.get(concern_code, [])
            if any(candidate in lower for candidate in candidates):
                matched_concern_ids.append(concern_id)

        return list(sorted(set(matched_concern_ids)))

    def build_claim_row(
        self,
        chunk: Dict,
        sentence: str,
        validated_claim: Dict,
        extraction_method: str = "llm",
    ) -> Dict:
        return {
            "paper_id": chunk["paper_id"],
            "chunk_id": chunk["chunk_id"],
            "claim_text": sentence.strip(),
            "normalized_summary": (
                f'{validated_claim["ingredient"]} '
                f'{validated_claim["relation"]} '
                f'{validated_claim["target"]}'
            ),
            "claim_type": validated_claim["claim_type"],
            "evidence_direction": validated_claim["evidence_direction"],
            "confidence_score": validated_claim["confidence"],
            "section_type": chunk["section_type"],
            "extraction_method": extraction_method,
            "source_sentence": sentence.strip(),
            "source_start_offset": chunk["source_start_offset"],
            "source_end_offset": chunk["source_end_offset"],
        }

    def infer_taxonomy_maps(
        self,
        validated_claim: Dict,
        effect_rows: List[Dict],
        concern_rows: List[Dict],
    ) -> Dict[str, List[int]]:
        target_text = validated_claim["target"]

        effect_ids = self.extract_effect_ids(target_text, effect_rows)
        concern_ids = self.extract_concern_ids(target_text, concern_rows)

        return {
            "effect_ids": effect_ids,
            "concern_ids": concern_ids,
        }


extractor = ClaimExtractor()