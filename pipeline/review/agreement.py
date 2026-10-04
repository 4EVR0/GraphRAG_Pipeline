"""사람 라벨 대비 판정 일치율 (#49).

논문 단위로 비교한다. 모델 판정이 여러 개면 다수결 값을 논문 값으로 쓴다.
사람 라벨이 비어 있는 칸은 비교에서 뺀다. 값 끝의 *는 접두어 일치를 뜻한다(topical* 등).
"""
import csv
import random
from collections import Counter
from pathlib import Path

FIELDS = ("relevant", "attribution", "route", "direction")
HUMAN_COLUMNS = ("human_relevant", "human_attribution", "human_route", "human_direction", "human_effect_codes", "human_note")
NONE = "none"


def _majority(values: list[str]) -> str:
    values = [v for v in values if v]
    if not values:
        return NONE
    counts = Counter(values)
    top = max(counts.values())
    return sorted(v for v, c in counts.items() if c == top)[0]


def paper_labels(summary: dict, records: list[dict], direction_effects: frozenset[str] | None = None) -> dict:
    """모델 판정을 논문 단위 값으로 줄인다. direction은 direction_effects 효능만 본다(None이면 전체)."""
    direction_records = [r for r in records if direction_effects is None or r["effect_code"] in direction_effects]
    relevant = summary.get("relevant")
    return {
        "relevant": NONE if relevant is None else ("yes" if relevant else "no"),
        "attribution": _majority([r["attribution"] for r in records]),
        "route": _majority([r["route"] for r in records]),
        "direction": _majority([r["direction"] for r in direction_records]),
        "effect_codes": "|".join(sorted({r["effect_code"] for r in records})),
    }


def _match(human: str, model: str) -> bool:
    human, model = human.strip().lower(), model.strip().lower()
    if human.endswith("*"):
        return model.startswith(human[:-1])
    return human == model


def agreement(human_rows: list[dict], model_labels: dict[str, dict]) -> dict:
    """human_rows: pmid + human_* 열. model_labels: pmid → paper_labels 결과."""
    out = {}
    for field in FIELDS:
        pairs = [
            (row[f"human_{field}"], model_labels[row["pmid"]][field])
            for row in human_rows
            if (row.get(f"human_{field}") or "").strip() and row["pmid"] in model_labels
        ]
        matched = sum(_match(h, m) for h, m in pairs)
        out[field] = {"matched": matched, "n": len(pairs), "rate": round(matched / len(pairs), 4) if pairs else None,
                      "mismatches": [{"human": h, "model": m} for h, m in pairs if not _match(h, m)]}
    jaccards = []
    for row in human_rows:
        human = {c.strip() for c in (row.get("human_effect_codes") or "").split("|") if c.strip()}
        if not human or row["pmid"] not in model_labels:
            continue
        model = {c for c in model_labels[row["pmid"]]["effect_codes"].split("|") if c}
        jaccards.append(len(human & model) / len(human | model))
    out["effect_codes_jaccard"] = {"n": len(jaccards),
                                   "mean": round(sum(jaccards) / len(jaccards), 4) if jaccards else None}
    return out


def sample_for_human_review(papers: list[dict], size: int, seed: int, strata_key: str) -> list[dict]:
    """strata_key 값별 비율을 유지해 표본을 뽑는다(시술 연구 여부 등)."""
    rng = random.Random(seed)
    groups: dict[str, list[dict]] = {}
    for paper in sorted(papers, key=lambda p: p["pmid"]):
        groups.setdefault(str(paper.get(strata_key, "")), []).append(paper)
    picked: list[dict] = []
    for key in sorted(groups):
        quota = round(size * len(groups[key]) / len(papers))
        picked += rng.sample(groups[key], min(quota, len(groups[key])))
    leftovers = [p for p in papers if p not in picked]
    rng.shuffle(leftovers)
    picked += leftovers[: max(0, size - len(picked))]
    return sorted(picked[:size], key=lambda p: p["pmid"])


def write_human_sheet(path: Path, papers: list[dict], ingredient: str) -> None:
    """모델 판정을 보여주지 않는 블라인드 검수표."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["pmid", "ingredient_inci", "title", "abstract", *HUMAN_COLUMNS])
        writer.writeheader()
        for paper in papers:
            writer.writerow({"pmid": paper["pmid"], "ingredient_inci": ingredient, "title": paper["title"],
                             "abstract": paper["source_text"], **{c: "" for c in HUMAN_COLUMNS}})


def read_human_sheet(path: Path) -> list[dict]:
    with open(path, encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))
