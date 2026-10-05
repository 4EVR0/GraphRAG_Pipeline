"""성분 × 효능 조합별 좁은 검색으로 PubMed 논문을 수집한다 (#49).

- 조합당 결과가 full_fetch_max 이하면 전부, 넘으면 사람 대상 임상·리뷰로 좁힌 뒤 전부 가져온다.
- 좁힌 뒤에도 pair_cap을 넘으면 pair_cap건만 가져오고 capped로 기록한다.
- PMID 기준으로 중복을 제거하고, existing_pmids에 있는 논문은 다시 받지 않는다.
"""
import csv
import json
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

from pipeline.metadata.services.narrow_query import PairPlan, narrowed_query
from pipeline.metadata.services.pubmed_parser import parse_pubmed_xml

logger = logging.getLogger(__name__)

FETCH_BATCH_SIZE = 200
ESEARCH_MAX_RETMAX = 9999


@dataclass
class NarrowResult:
    search_rows: list[dict] = field(default_factory=list)
    pair_rows: list[dict] = field(default_factory=list)
    paper_rows: list[dict] = field(default_factory=list)
    pmids: set[str] = field(default_factory=set)
    skipped_existing: int = 0
    fetched_without_abstract: int = 0


def search_pair(client, plan: PairPlan, full_fetch_max: int, pair_cap: int) -> dict:
    """한 조합을 건수 규칙에 따라 검색하고 검색 기록을 반환한다(pmids 포함)."""
    row = {
        "inci_name": plan.inci_name,
        "effect_code": plan.effect_code,
        "rule": plan.rule,
        "skip_reason": plan.skip_reason,
        "query": plan.query or "",
        "count": "",
        "narrowed": False,
        "narrowed_count": "",
        "taken": 0,
        "capped": False,
        "pmids": [],
    }
    if plan.query is None:
        return row
    count, pmids = client.search(plan.query, retmax=full_fetch_max)
    row["count"] = count
    if count > full_fetch_max:
        cap = min(pair_cap, ESEARCH_MAX_RETMAX)
        narrowed_count, pmids = client.search(narrowed_query(plan.query), retmax=cap)
        row.update(narrowed=True, narrowed_count=narrowed_count, capped=narrowed_count > cap)
        if row["capped"]:
            logger.warning(
                "상한 도달: %s × %s 좁힌 결과 %s건 중 %s건만 수집",
                plan.inci_name, plan.effect_code, narrowed_count, cap,
            )
    row["pmids"] = list(dict.fromkeys(pmids))
    row["taken"] = len(row["pmids"])
    return row


def collect_narrow(
    client,
    plans: list[PairPlan],
    full_fetch_max: int,
    pair_cap: int,
    existing_pmids: set[str] | None = None,
    search_only: bool = False,
) -> NarrowResult:
    existing_pmids = existing_pmids or set()
    result = NarrowResult()
    pairs_by_pmid: dict[str, set[str]] = defaultdict(set)
    ingredients_by_pmid: dict[str, set[str]] = defaultdict(set)

    for index, plan in enumerate(plans, 1):
        row = search_pair(client, plan, full_fetch_max, pair_cap)
        pmids = row.pop("pmids")
        row["new_pmids"] = sum(1 for p in pmids if p not in existing_pmids and p not in result.pmids)
        result.search_rows.append(row)
        for pmid in pmids:
            result.pmids.add(pmid)
            pairs_by_pmid[pmid].add(f"{plan.inci_name}:{plan.effect_code}")
            ingredients_by_pmid[pmid].add(plan.inci_name)
            result.pair_rows.append(
                {"inci_name": plan.inci_name, "effect_code": plan.effect_code, "pmid": pmid,
                 "already_collected": pmid in existing_pmids}
            )
        if index % 50 == 0:
            logger.info("검색 %s/%s 조합, 고유 PMID %s", index, len(plans), len(result.pmids))

    to_fetch = sorted(result.pmids - existing_pmids)
    result.skipped_existing = len(result.pmids) - len(to_fetch)
    if search_only:
        return result

    for start in range(0, len(to_fetch), FETCH_BATCH_SIZE):
        xml_text = client.fetch_pubmed_xml(to_fetch[start : start + FETCH_BATCH_SIZE])
        if not xml_text:
            continue
        for record in parse_pubmed_xml(xml_text):
            if not record.pmid or record.pmid not in pairs_by_pmid:
                continue
            if not record.abstract_text:
                result.fetched_without_abstract += 1
                continue
            result.paper_rows.append(
                {
                    **record.to_dict(),
                    "searched_ingredients": "|".join(sorted(ingredients_by_pmid[record.pmid])),
                    "searched_pairs": "|".join(sorted(pairs_by_pmid[record.pmid])),
                }
            )
    return result


def load_existing_pmids(paths: list[Path]) -> set[str]:
    """이미 수집한 PMID. 디렉터리면 하위 CSV의 pmid 열을, 파일이면 줄 단위·JSON 목록을 읽는다."""
    pmids: set[str] = set()
    for path in paths:
        files = sorted(path.rglob("*.csv")) if path.is_dir() else [path]
        for file in files:
            if file.suffix == ".csv":
                with open(file, encoding="utf-8-sig", newline="") as handle:
                    reader = csv.DictReader(handle)
                    if reader.fieldnames and "pmid" in reader.fieldnames:
                        pmids.update(r["pmid"].strip() for r in reader if r.get("pmid", "").strip())
            elif file.suffix == ".json":
                pmids.update(str(p) for p in json.loads(file.read_text(encoding="utf-8")))
            else:
                pmids.update(line.strip() for line in file.read_text(encoding="utf-8").splitlines() if line.strip())
    return pmids
