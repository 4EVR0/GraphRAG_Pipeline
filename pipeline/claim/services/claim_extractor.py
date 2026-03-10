import csv
from typing import Dict, List

from pipeline.common.config.settings import settings


EFFICACY_KEYWORDS = [
    "improve", "improved", "improves",
    "reduce", "reduced", "reduces",
    "increase", "increased", "increases",
    "effective", "efficacy", "benefit", "beneficial",
]

SAFETY_KEYWORDS = [
    "safe", "safety", "adverse event", "adverse events",
    "tolerable", "tolerability", "irritation", "side effect",
    "side effects", "complication", "complications",
]

MECHANISM_KEYWORDS = [
    "mechanism", "pathway", "inhibit", "inhibition",
    "regulate", "regulation", "stimulate", "stimulation",
    "suppress", "suppression",
]


class ClaimExtractor:
    def __init__(self) -> None:
        self.ingredient_terms = self._load_ingredient_terms()

    def _load_ingredient_terms(self) -> Dict[str, List[str]]:
        terms_map: Dict[str, List[str]] = {}

        with open(settings.target_csv_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("is_target", "").strip().lower() != "true":
                    continue

                canonical_name = row["canonical_name"].strip()
                query_name = row["query_name"].strip()
                alias_list = row.get("alias_list", "")

                terms = [canonical_name, query_name]
                if alias_list:
                    terms.extend([a.strip() for a in alias_list.split("|") if a.strip()])

                unique_terms = []
                seen = set()
                for term in terms:
                    key = term.lower()
                    if key not in seen:
                        seen.add(key)
                        unique_terms.append(term)

                terms_map[canonical_name] = unique_terms

        return terms_map

    def classify_claim_type(self, text: str) -> str:
        lower = text.lower()

        if any(keyword in lower for keyword in SAFETY_KEYWORDS):
            return "safety"
        if any(keyword in lower for keyword in MECHANISM_KEYWORDS):
            return "mechanism"
        if any(keyword in lower for keyword in EFFICACY_KEYWORDS):
            return "efficacy"

        return "efficacy"

    def classify_evidence_direction(self, text: str) -> str:
        lower = text.lower()

        negative_patterns = [
            "no significant difference",
            "not significant",
            "did not improve",
            "did not reduce",
            "no effect",
            "ineffective",
        ]
        if any(pattern in lower for pattern in negative_patterns):
            return "neutral"

        positive_patterns = [
            "significantly reduced",
            "significantly improved",
            "improved",
            "reduced",
            "effective",
            "beneficial",
            "safe",
        ]
        if any(pattern in lower for pattern in positive_patterns):
            return "supports"

        return "neutral"

    def extract_ingredient_names(self, text: str) -> List[str]:
        lower = text.lower()
        found: List[str] = []

        for canonical_name, terms in self.ingredient_terms.items():
            for term in terms:
                if term.lower() in lower:
                    found.append(canonical_name)
                    break

        return found

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

        return matched_effect_ids

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

        return matched_concern_ids

    def extract_claim(
        self,
        chunk_text: str,
        effect_rows: List[Dict],
        concern_rows: List[Dict],
    ) -> Dict:
        return {
            "claim_text": chunk_text,
            "normalized_summary": chunk_text[:500],
            "claim_type": self.classify_claim_type(chunk_text),
            "evidence_direction": self.classify_evidence_direction(chunk_text),
            "confidence_score": 0.6,
            "ingredient_names": self.extract_ingredient_names(chunk_text),
            "effect_ids": self.extract_effect_ids(chunk_text, effect_rows),
            "concern_ids": self.extract_concern_ids(chunk_text, concern_rows),
        }


extractor = ClaimExtractor()