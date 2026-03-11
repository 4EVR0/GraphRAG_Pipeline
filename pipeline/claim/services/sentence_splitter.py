import re


ABBREVIATIONS = [
    "e.g.",
    "i.e.",
    "vs.",
    "Dr.",
    "Mr.",
    "Mrs.",
    "Prof.",
]


def split_sentences(text: str) -> list[str]:
    text = text.strip()
    if not text:
        return []

    protected = text
    for abbr in ABBREVIATIONS:
        protected = protected.replace(abbr, abbr.replace(".", "<DOT>"))

    sentences = re.split(r'(?<=[.!?])\s+', protected)
    restored = [s.replace("<DOT>", ".").strip() for s in sentences if s.strip()]
    return restored