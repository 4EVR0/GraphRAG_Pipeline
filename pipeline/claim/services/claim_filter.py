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

    # 기존
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

    # 추가 (논문에서 매우 흔함)
    "improvement",
    "improvements",
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

    # 임상 표현
    "may reduce",
    "may improve",
    "may enhance",
    "may offer",
    "offer greater protection",

    # 피부 관련
    "prevent",
    "prevents",
    "protect",
    "protects",

    # 화장품 claim
    "hydration",
    "barrier function",
    "hyperpigmentation",
    "melasma",
    "photoaging",
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