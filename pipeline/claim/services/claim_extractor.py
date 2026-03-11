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

    def extract_ingredient_names(self, text: str) -> List[str]:
        text = text.strip()
        if not text:
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

            found.append(canonical_name)

        return found

    def normalize_ingredient_name(self, ingredient_name: str) -> Optional[str]:
        """
        LLM이 반환한 ingredient를 canonical ingredient로 정규화한다.
        우선순위:
        1) canonical exact match
        2) alias exact-ish match
        3) canonical/alias substring containment (보수적으로)
        """
        raw = ingredient_name.strip()
        if not raw:
            return None

        # 1) canonical exact match
        for canonical_name in self.allowed_canonical_ingredients:
            if raw.lower() == canonical_name.lower():
                return canonical_name

        # 2) alias exact-ish match
        for canonical_name, rule in self.ingredient_rules.items():
            for alias in rule["aliases"]:
                if raw.lower() == alias.lower():
                    return canonical_name

        # 3) substring containment fallback
        # 예: "Ceramide NP C15" -> "Ceramide"
        # 예: "Dexpanthenol" -> "Panthenol" (alias로 잡히면 2단계에서 먼저 해결됨)
        for canonical_name, rule in self.ingredient_rules.items():
            candidates = [canonical_name] + rule["aliases"]

            for candidate in candidates:
                candidate_lower = candidate.lower()
                raw_lower = raw.lower()

                if candidate_lower and candidate_lower in raw_lower:
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
            "DEPIGMENTING": ["depigment", "melasma", "hyperpigmentation", "pigmentation"],
            "BRIGHTENING": ["brightening", "skin tone", "dyschromia"],
            "ANTIOXIDANT": ["antioxidant", "oxidative stress"],
            "WOUND_HEALING": ["wound healing", "healing", "repair"],
            "ANTI_AGING": ["anti-aging", "wrinkle", "elasticity", "aging"],
            "PHOTOPROTECTIVE": ["photoprotective", "uv", "photoaging"],
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
            "HYPERPIGMENTATION": ["hyperpigmentation", "melasma", "pigmentation"],
            "DULLNESS": ["dullness", "uneven skin tone"],
            "AGING_SIGNS": ["aging", "wrinkle", "elasticity"],
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