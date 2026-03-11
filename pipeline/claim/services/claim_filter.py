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
]

CLAIM_MARKERS = [
    "improved",
    "improves",
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
    "tolerated",
    "associated with improvements",
    "associated with improvement",
    "associated with cost savings",
    "cost-effective",
    "cost saving",
    "benefits",
    "quality-of-life benefits",
    "significantly increased",
    "significantly reduced",
    "significantly improved",
    "significantly decreased",
    "increased hydration",
    "improved barrier",
    "barrier repair",
    "anti-acne",
    "soothing",
    "repairing",
    "texture-improving",
    "lowered",
    "inhibiting",
    "inhibited",
    "stimulated",
    "modulated",
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
]


def is_blocked_sentence(sentence: str) -> bool:
    lower = sentence.lower().strip()
    return any(lower.startswith(prefix.lower()) for prefix in BLOCKED_PREFIXES)


def is_claim_candidate_sentence(sentence: str) -> bool:
    lower = sentence.lower().strip()

    if any(pattern in lower for pattern in HARD_NON_CLAIM_PATTERNS):
        return False

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