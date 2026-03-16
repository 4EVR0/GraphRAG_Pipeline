import csv
import re
from typing import Dict, List, Optional, Tuple

from pipeline.common.config.settings import settings


GENERIC_OR_NON_COSMETIC_TARGETS = {
    "benefit",
    "benefits",
    "effect",
    "effects",
    "effectiveness",
    "outcome",
    "outcomes",
    "properties",
    "treatment",
    "treatments",
    "topical treatment",
    "topical treatments",
    "effectiveness of topical treatments",
    "patient outcomes",
    "blood loss",
    "perioperative bleeding",
    "perioperative bleeding treatment",
    "fibrinolysis",
    "systemic txa levels",
    "seroma risk",
    "reparative effects",
    "anti-inflammatory effects",
    "protective metabolites",
    "therapeutic option",
    "clinical improvement",
    "unknown",
    "ven",
    "surgical contexts",
}

NON_COSMETIC_TARGET_PATTERNS = [
    "perioperative",
    "blood loss",
    "fibrinolysis",
    "systemic txa",
    "seroma risk",
    "cardiac surgery",
    "surgery",
    "surgical",
    "ostomy",
    "ven",
    "clinical improvement",
    "topical treatment",
    "topical treatments",
    "therapeutic option",
    "protective metabolite",
    "protective metabolites",
]

CANONICAL_INGREDIENT_MAP = {
    "niacinamide": "Niacinamide",
    "nicotinamide": "Niacinamide",
    "nia": "Niacinamide",
    "panthenol": "Panthenol",
    "d-panthenol": "Panthenol",
    "dexpanthenol": "Panthenol",
    "ceramide": "Ceramide",
    "ceramides": "Ceramide",
    "ceramide np": "Ceramide",
    "ceramide ap": "Ceramide",
    "ceramide eop": "Ceramide",
    "ceramide ns": "Ceramide",
    "ceramide ng": "Ceramide",
    "ceramide as": "Ceramide",
    "ceramide eos": "Ceramide",
    "ceramide np c15": "Ceramide",
    "tranexamic acid": "Tranexamic acid",
    "txa": "Tranexamic acid",
    "salicylic acid": "Salicylic acid",
}

RESULT_PREFIX_RE = re.compile(r"^(results?|conclusions?):\s*", re.IGNORECASE)

NON_TARGET_LEADING_SUBJECT_PATTERNS = [
    r"berberine\b",
    r"β-nicotinamide mononucleotide\b",
    r"nicotinamide mononucleotide\b",
    r"nicotinamide riboside\b",
    r"mdba\b",
    r"pt-liposomes?\b",
    r"basic emollient formulations?\b",
    r"vehicle\b",
    r"formulation\b",
    r"formulations\b",
    r"liposomes?\b",
    r"bacterial-derived ceramides?\b",
    r"cer\d+\b",
]

ALLOWED_PREFIX_PATTERNS = [
    r"topical(?:ly)?\s+",
    r"oral\s+",
    r"application of\s+",
    r"treatment with\s+",
    r"use of\s+",
    r"the use of\s+",
    r"using\s+",
    r"with\s+",
    r"post-treatment with\s+",
]

SKIN_CONTEXT_TERMS = [
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
    "barrier function",
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
    "keratinocyte",
    "epidermal recovery",
]

POSITIVE_SIGNALS = [
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
    "photoprotective",
    "antioxidative",
    "normalized",
    "normalizing",
    "accelerated",
    "accelerates",
    "supports",
    "supported",
]


class ClaimExtractor:
    def __init__(self) -> None:
        self.ingredient_rules = self._load_ingredient_rules()
        self.allowed_canonical_ingredients = set(self.ingredient_rules.keys())
        self.alias_to_canonical = self._build_alias_to_canonical_map()
        self.sorted_aliases = sorted(self.alias_to_canonical.keys(), key=len, reverse=True)

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

                if canonical_name == "Niacinamide":
                    aliases.extend(["nicotinamide", "nia"])
                elif canonical_name == "Panthenol":
                    aliases.extend(["d-panthenol", "dexpanthenol"])
                elif canonical_name == "Ceramide":
                    aliases.extend([
                        "ceramides",
                        "ceramide np",
                        "ceramide ap",
                        "ceramide eop",
                        "ceramide ns",
                        "ceramide ng",
                        "ceramide as",
                        "ceramide eos",
                        "ceramide np c15",
                    ])
                elif canonical_name == "Tranexamic acid":
                    aliases.extend(["txa"])

                aliases = self._normalize_unique(aliases)

                excludes = self._split_pipe_field(exclude_if_contains)

                if canonical_name == "Niacinamide":
                    excludes.extend([
                        "nicotinamide mononucleotide",
                        "β-nicotinamide mononucleotide",
                        "nicotinamide riboside",
                        "(nmn)",
                        "(nr)",
                        " nmn ",
                        " nr ",
                        "nad",
                        "nad+",
                        "nadh",
                        "nadp",
                        "nadph",
                    ])
                elif canonical_name == "Ceramide":
                    excludes.extend([
                        "cer2",
                        "cer14",
                        "bacterial-derived ceramides",
                    ])

                excludes = self._normalize_unique(excludes)

                rules[canonical_name] = {
                    "aliases": aliases,
                    "excludes": excludes,
                }

        return rules

    def _build_alias_to_canonical_map(self) -> Dict[str, str]:
        alias_map: Dict[str, str] = {}

        for canonical_name, rule in self.ingredient_rules.items():
            alias_map[canonical_name.lower()] = canonical_name
            for alias in rule["aliases"]:
                alias_map[alias.lower()] = canonical_name

        for alias, canonical in CANONICAL_INGREDIENT_MAP.items():
            alias_map[alias.lower()] = canonical

        return alias_map

    def _normalize_sentence_for_subject_check(self, text: str) -> str:
        text = text.strip()
        text = RESULT_PREFIX_RE.sub("", text)
        return text.strip()

    def _contains_exclude_pattern(self, text: str, pattern: str) -> bool:
        lower = text.lower()
        pattern_lower = pattern.lower()

        if pattern_lower in {"nad", "nad+", "nadh", "nadp", "nadph", "nmn", "nr"}:
            return re.search(r"\b" + re.escape(pattern_lower) + r"\b", lower) is not None

        return pattern_lower in lower

    def _has_skin_context(self, text: str) -> bool:
        lower = text.lower()
        return any(term in lower for term in SKIN_CONTEXT_TERMS)

    def _is_blocked_non_cosmetic_domain(self, text: str) -> bool:
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
            "traumatic hemorrhage",
            "hemorrhage",
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
            "patients and methods:",
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

        if self._is_results_or_conclusion_sentence(text):
            return True

        return any(term in lower for term in POSITIVE_SIGNALS)

    def _starts_with_non_target_subject(self, text: str) -> bool:
        normalized = self._normalize_sentence_for_subject_check(text).lower()
        return any(re.match(pattern, normalized) for pattern in NON_TARGET_LEADING_SUBJECT_PATTERNS)

    def _extract_front_subject_alias(self, text: str) -> Optional[Tuple[str, str]]:
        normalized = self._normalize_sentence_for_subject_check(text)
        lower = normalized.lower()

        for alias in self.sorted_aliases:
            direct_pattern = r"^" + re.escape(alias) + r"\b"
            if re.match(direct_pattern, lower):
                return alias, self.alias_to_canonical[alias]

            for prefix in ALLOWED_PREFIX_PATTERNS:
                prefixed_pattern = r"^" + prefix + re.escape(alias) + r"\b"
                if re.match(prefixed_pattern, lower):
                    return alias, self.alias_to_canonical[alias]

        return None

    def _extract_any_alias(self, text: str) -> Optional[Tuple[str, str]]:
        lower = text.lower()

        for alias in self.sorted_aliases:
            pattern = r"\b" + re.escape(alias) + r"\b"
            if re.search(pattern, lower):
                return alias, self.alias_to_canonical[alias]

        return None

    def _has_multi_ingredient_enumeration(self, text: str) -> bool:
        lower = text.lower()

        trigger_phrases = [
            "containing",
            "contains",
            "with",
            "along with",
            "combined with",
            "plus",
            "such as",
        ]

        if not any(phrase in lower for phrase in trigger_phrases):
            return False

        matched_aliases = []
        for alias in self.sorted_aliases:
            if re.search(r"\b" + re.escape(alias) + r"\b", lower):
                matched_aliases.append(alias)

        canonicals = {self.alias_to_canonical[a] for a in matched_aliases}
        return len(canonicals) >= 2

    def _is_niacinamide_context_valid(self, text: str) -> bool:
        lower = text.lower()

        if any(
            re.search(r"\b" + re.escape(term) + r"\b", lower)
            for term in ["nmn", "nr", "nad", "nad+", "nadh", "nadp", "nadph"]
        ):
            return False

        if any(term in lower for term in [
            "nicotinamide riboside",
            "nicotinamide mononucleotide",
            "β-nicotinamide mononucleotide",
            "nicotinamide adenine dinucleotide",
            "mitochondria",
            "mitochondrial",
            "metabolism",
            "metabolic",
            "bioenergetic",
            "coenzyme",
        ]):
            return False

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

        return any(term in lower for term in required_context_terms)

    def _passes_special_context_rule(self, canonical_name: str, text: str) -> bool:
        if canonical_name.lower() == "niacinamide":
            return self._is_niacinamide_context_valid(text)
        return True

    def is_claim_like_sentence(self, text: str) -> bool:
        text = text.strip()
        if not text:
            return False

        if self._is_blocked_non_cosmetic_domain(text):
            return False

        if self._is_study_design_sentence(text):
            return False

        if self._starts_with_non_target_subject(text):
            return False

        has_positive_signal = self._has_positive_signal(text)
        has_skin_context = self._has_skin_context(text)

        return has_positive_signal and has_skin_context

    def extract_ingredient_names(self, text: str) -> List[str]:
        text = text.strip()
        if not text:
            return []

        if not self.is_claim_like_sentence(text):
            return []

        if self._has_multi_ingredient_enumeration(text):
            return []

        matched = self._extract_front_subject_alias(text)

        if not matched:
            matched = self._extract_any_alias(text)

        if not matched:
            return []

        _, canonical_name = matched
        rule = self.ingredient_rules.get(canonical_name, {})
        excludes = rule.get("excludes", [])

        if any(self._contains_exclude_pattern(text, exclude_pattern) for exclude_pattern in excludes):
            return []

        if not self._passes_special_context_rule(canonical_name, text):
            return []

        return [canonical_name]

    def normalize_ingredient_name(self, ingredient_name: str) -> Optional[str]:
        raw = ingredient_name.strip()
        if not raw:
            return None

        lower_raw = raw.lower()

        if lower_raw in self.alias_to_canonical:
            return self.alias_to_canonical[lower_raw]

        # ceramide subtype 일반화
        ceramide_subtype_pattern = r"^ceramide(?:\s+[a-z0-9]+)+$"
        if re.match(ceramide_subtype_pattern, lower_raw):
            return "Ceramide"

        for canonical_name in self.allowed_canonical_ingredients:
            if lower_raw == canonical_name.lower():
                return canonical_name

        return None

    def is_allowed_ingredient(self, ingredient_name: str) -> bool:
        return self.normalize_ingredient_name(ingredient_name) is not None

    def get_allowed_ingredient_names(self) -> List[str]:
        return sorted(self.allowed_canonical_ingredients)

    def _is_generic_or_non_cosmetic_target(self, target_text: str) -> bool:
        lower = target_text.strip().lower()

        if lower in GENERIC_OR_NON_COSMETIC_TARGETS:
            return True

        return any(pattern in lower for pattern in NON_COSMETIC_TARGET_PATTERNS)

    def _sentence_mentions_ingredient(
        self,
        normalized_ingredient: str,
        source_sentence: Optional[str] = None,
        claim_text: Optional[str] = None,
    ) -> bool:
        sentence = (source_sentence or claim_text or "").strip()
        if not sentence:
            return True

        if self._starts_with_non_target_subject(sentence):
            return False

        matched = self._extract_front_subject_alias(sentence)
        if matched:
            _, subject_canonical = matched
            return subject_canonical == normalized_ingredient

        matched = self._extract_any_alias(sentence)
        if not matched:
            return False

        _, canonical = matched
        return canonical == normalized_ingredient

    def validate_claim(
        self,
        raw_claim: Dict,
        source_sentence: Optional[str] = None,
    ) -> Optional[Dict]:
        ingredient = raw_claim.get("ingredient", "")
        target = raw_claim.get("target", "")
        relation = raw_claim.get("relation", "")
        claim_type = raw_claim.get("claim_type", "")
        evidence_direction = raw_claim.get("evidence_direction", "")
        confidence = raw_claim.get("confidence", 0.0)

        normalized_ingredient = self.normalize_ingredient_name(ingredient)
        if not normalized_ingredient:
            return None

        if not self._sentence_mentions_ingredient(
            normalized_ingredient=normalized_ingredient,
            source_sentence=source_sentence,
            claim_text=raw_claim.get("claim_text"),
        ):
            return None

        if not target or not isinstance(target, str):
            return None

        cleaned_target = target.strip()
        if not cleaned_target:
            return None

        if self._is_generic_or_non_cosmetic_target(cleaned_target):
            return None

        if not relation or not isinstance(relation, str):
            return None

        if not claim_type or not isinstance(claim_type, str):
            return None

        if not evidence_direction or not isinstance(evidence_direction, str):
            return None

        try:
            confidence = float(confidence)
        except (TypeError, ValueError):
            confidence = 0.0

        return {
            **raw_claim,
            "ingredient": normalized_ingredient,
            "target": cleaned_target,
            "relation": relation.strip(),
            "claim_type": claim_type.strip(),
            "evidence_direction": evidence_direction.strip(),
            "confidence": confidence,
        }

    def extract_effect_ids(self, text: str, effect_rows: List[Dict]) -> List[int]:
        lower = text.lower()
        matched_effect_ids: List[int] = []

        synonym_map = {
            "ANTI_INFLAMMATORY": ["anti-inflammatory", "inflammation", "inflammatory"],
            "SOOTHING": ["soothing", "calming", "redness", "irritation"],
            "BARRIER_REPAIR": ["barrier", "barrier repair", "skin barrier", "barrier function"],
            "HYDRATING": ["hydration", "hydrating", "hydrate", "moisturizing"],
            "MOISTURE_RETENTION": ["transepidermal water loss", "tewl", "moisture retention"],
            "SEBUM_REGULATION": ["sebum", "oil control", "oily skin"],
            "KERATOLYTIC": ["keratolytic", "exfoliation", "peeling"],
            "COMEDOLYTIC": ["comedone", "comedones", "blackhead", "whitehead"],
            "ANTIMICROBIAL": ["antimicrobial", "antibacterial", "microbial"],
            "DEPIGMENTING": ["depigment", "melasma", "hyperpigmentation", "pigmentation", "pih"],
            "BRIGHTENING": ["brightening", "skin tone", "dyschromia"],
            "ANTIOXIDANT": ["antioxidant", "antioxidative", "oxidative stress"],
            "WOUND_HEALING": ["wound healing", "healing", "repair"],
            "ANTI_AGING": ["anti-aging", "wrinkle", "elasticity", "aging", "photoaging"],
            "PHOTOPROTECTIVE": ["photoprotective", "uv", "uvb", "photoaging", "photodamaged", "immunosuppression"],
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
            "BARRIER_DAMAGE": ["barrier", "skin barrier", "tewl", "barrier function"],
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