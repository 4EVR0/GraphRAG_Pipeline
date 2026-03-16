BLOCKED_PREFIXES = [
    "BACKGROUND:",
    "OBJECTIVE:",
    "OBJECTIVES:",
    "AIM:",
    "AIMS:",
    "PURPOSE:",
    "METHODS:",
    "PATIENTS AND METHODS:",
    "MATERIALS AND METHODS:",
]

HARD_NON_CLAIM_PATTERNS = [
    "was developed and validated",
    "were developed and validated",
    "limits of detection",
    "limit of detection",
    "percent recoveries",
    "concurrent measurement",
    "measurement of",
    "analytical method",
    "hplc method",
    "population perspective",
    "cost-effective",
    "cost effective",
    "cost-saving",
    "cost saving",
]

HARD_NON_TARGET_SUBJECT_PATTERNS = [
    "berberine",
    "nicotinamide mononucleotide",
    "β-nicotinamide mononucleotide",
    "nicotinamide riboside",
    "mdba",
    "pt-liposomes",
    "pt-liposome",
    "basic emollient formulations",
    "basic emollient formulation",
    "vehicle",
    "formulation",
    "formulations",
    "liposomes",
    "liposome",
]

CLAIM_MARKERS = [
    "improved",
    "improves",
    "improvement",
    "improvements",
    "reduced",
    "reduces",
    "decreased",
    "decreases",
    "enhanced",
    "enhances",
    "promoted",
    "promotes",
    "restored",
    "restores",
    "suppressed",
    "suppresses",
    "effective",
    "efficacious",
    "safe",
    "well tolerated",
    "well-tolerated",
    "tolerated",
    "tolerability",
    "demonstrated",
    "demonstrates",
    "showed",
    "shown",
    "showing",
    "suggest",
    "suggests",
    "indicate",
    "indicates",
    "associated with",
    "resulted in",
    "led to",
    "may reduce",
    "may improve",
    "may enhance",
    "may offer",
    "may prevent",
    "offer greater protection",
    "prevent",
    "prevents",
    "protect",
    "protects",
    "barrier function",
    "skin barrier",
    "hydration",
    "hyperpigmentation",
    "melasma",
    "photoaging",
    "photoprotective",
    "antioxidative",
    "anti-acne",
    "soothing",
    "repairing",
    "lowered",
    "inhibiting",
    "inhibited",
    "stimulated",
    "modulated",
    "mitigate",
    "mitigates",
    "impressive modalities",
    "confer superior improvements",
]

NEGATION_MARKERS = [
    "no significant difference",
    "not significant",
    "did not improve",
    "did not reduce",
    "did not enhance",
    "no effect",
    "ineffective",
    "failed to show",
    "showed no benefit",
    "no benefit",
]


def _normalize_prefix(sentence: str) -> str:
    lower = sentence.lower().strip()

    for prefix in ("results:", "result:", "conclusion:", "conclusions:"):
        if lower.startswith(prefix):
            return lower[len(prefix):].strip()

    return lower


def is_blocked_sentence(sentence: str) -> bool:
    lower = sentence.lower().strip()
    return any(lower.startswith(prefix.lower()) for prefix in BLOCKED_PREFIXES)


def is_claim_candidate_sentence(sentence: str) -> bool:
    lower = sentence.lower().strip()
    normalized = _normalize_prefix(sentence)

    if any(pattern in lower for pattern in HARD_NON_CLAIM_PATTERNS):
        return False

    if any(normalized.startswith(pattern) for pattern in HARD_NON_TARGET_SUBJECT_PATTERNS):
        return False

    # CER2, CER14 같은 ceramide subclass는 현재 프로젝트 스코프에서는 제외
    if normalized.startswith("cer") and len(normalized) >= 4 and normalized[3].isdigit():
        return False

    if lower.startswith("results:") or lower.startswith("result:"):
        return True

    if lower.startswith("conclusion:") or lower.startswith("conclusions:"):
        return True

    if any(marker in lower for marker in CLAIM_MARKERS):
        return True

    if any(marker in lower for marker in NEGATION_MARKERS):
        return True

    return False


def is_claim_worthy_section(section_type: str | None) -> bool:
    if section_type is None:
        return True

    normalized = section_type.strip().lower()
    if not normalized:
        return True

    blocked_sections = {
        "background",
        "objective",
        "objectives",
        "aim",
        "aims",
        "purpose",
        "methods",
        "patients and methods",
        "materials and methods",
    }

    return normalized not in blocked_sections