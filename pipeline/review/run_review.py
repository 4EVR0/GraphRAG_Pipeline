"""근거 검수 시범 CLI (#49). 결과는 --out-dir(로컬)에만 쓴다.

python -m pipeline.review.run_review fetch-sources --pmids-tsv T --ingredient "SALICYLIC ACID" --out-dir D
python -m pipeline.review.run_review screen --out-dir D --model gpt-4o-mini [--limit 10]
python -m pipeline.review.run_review submit --out-dir D --model claude-opus-5-5 --effort high [--limit 10]
python -m pipeline.review.run_review collect --out-dir D --batch-id msgbatch_...
python -m pipeline.review.run_review human-sheet --out-dir D --size 30 --strata-tsv T
python -m pipeline.review.run_review agree --out-dir D --human-csv H --model M [--direction-effects acne|all]
python -m pipeline.review.run_review cost --out-dir D [--project-papers 10000]
"""
import argparse
import csv
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from pipeline.review.agreement import (
    agreement,
    paper_labels,
    read_human_sheet,
    sample_for_human_review,
    write_human_sheet,
)
from pipeline.review.batch import (
    ReviewItem,
    build_requests,
    collect,
    cost_usd,
    custom_id,
    review_key,
    submit,
    wait,
)
from pipeline.review.schema import ACNE_EFFECTS, prompt_sha
from pipeline.review.screen import screen_cost_usd, screen_key, screen_one, screen_prompt_sha
from pipeline.review.validate import judge

SOURCES_FILE = "sources.jsonl"
BATCHES_FILE = "batches.jsonl"
JUDGMENTS_FILE = "judgments.jsonl"
SUMMARIES_FILE = "summaries.jsonl"
QUEUE_FILE = "human_queue.jsonl"
SCREEN_FILE = "screen.jsonl"


def read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def append_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def load_items(out_dir: Path) -> list[ReviewItem]:
    return [ReviewItem(r["pmid"], r["ingredient"], r["title"], r["source_text"], r.get("source", "abstract"))
            for r in read_jsonl(out_dir / SOURCES_FILE) if r.get("source_text")]


def fetch_sources(pmids: list[str], ingredient: str, out_dir: Path) -> None:
    from pipeline.metadata.services.pubmed_client import PubMedClient
    from pipeline.metadata.services.pubmed_parser import parse_pubmed_xml

    client = PubMedClient()
    records = {}
    for start in range(0, len(pmids), 100):
        for record in parse_pubmed_xml(client.fetch_pubmed_xml(pmids[start:start + 100]) or "<x/>"):
            records[record.pmid] = record
    existing = {(r["pmid"], r["ingredient"]) for r in read_jsonl(out_dir / SOURCES_FILE)}
    rows = []
    for pmid in pmids:
        if (pmid, ingredient) in existing:
            continue
        record = records.get(pmid)
        rows.append({"pmid": pmid, "ingredient": ingredient, "title": record.title if record else "",
                     "source_text": (record.abstract_text or "") if record else "", "source": "abstract"})
    append_jsonl(out_dir / SOURCES_FILE, rows)
    no_abstract = [r["pmid"] for r in rows if not r["source_text"]]
    append_jsonl(out_dir / QUEUE_FILE, [{"pmid": p, "ingredient_inci": ingredient, "model": None,
                                          "prompt_sha": None, "reason": "no_abstract", "detail": ""}
                                         for p in no_abstract])
    print(f"[sources] {len(rows)} added, no abstract {len(no_abstract)} (human queue)")


def done_keys(out_dir: Path) -> set:
    return {review_key(s["pmid"], s["ingredient_inci"], s["model"], s["prompt_sha"])
            for s in read_jsonl(out_dir / SUMMARIES_FILE)}


def _client():
    import anthropic

    return anthropic.Anthropic()


def cmd_submit(args) -> None:
    items = load_items(args.out_dir)
    if args.pmids:
        wanted = set(args.pmids.split(","))
        items = [i for i in items if i.pmid in wanted]
    requests, skipped = build_requests(items, args.model, args.effort, done_keys(args.out_dir))
    if args.limit:
        requests = requests[: args.limit]
    print(f"[submit] {len(requests)} requests, {len(skipped)} already reviewed, model={args.model}, effort={args.effort}")
    if args.dry_run or not requests:
        return
    batch_id = submit(_client(), requests)
    append_jsonl(args.out_dir / BATCHES_FILE, [{
        "batch_id": batch_id, "model": args.model, "effort": args.effort, "prompt_sha": prompt_sha(),
        "custom_ids": [r["custom_id"] for r in requests],
        "submitted_at": datetime.now(timezone.utc).isoformat(),
    }])
    print(f"[submit] batch_id={batch_id}")


def cmd_collect(args) -> None:
    batches = {b["batch_id"]: b for b in read_jsonl(args.out_dir / BATCHES_FILE)}
    meta = batches[args.batch_id]
    client = _client()
    wait(client, args.batch_id, poll_seconds=args.poll_seconds)
    results = {r["custom_id"]: r for r in collect(client, args.batch_id)}
    append_jsonl(args.out_dir / f"results_{args.batch_id}.jsonl", list(results.values()))
    by_id = {custom_id(item, meta["prompt_sha"]): item for item in load_items(args.out_dir)}
    reviewed_at = datetime.now(timezone.utc).isoformat()
    records, queue, summaries = [], [], []
    for cid in meta["custom_ids"]:
        item = by_id[cid]
        result = results.get(cid, {"result_type": "missing"})
        r, q, s = judge(item, result, meta["model"], meta["prompt_sha"], reviewed_at)
        s.update(batch_id=args.batch_id, effort=meta["effort"])
        records += r
        queue += q
        summaries.append(s)
    append_jsonl(args.out_dir / JUDGMENTS_FILE, records)
    append_jsonl(args.out_dir / QUEUE_FILE, queue)
    append_jsonl(args.out_dir / SUMMARIES_FILE, summaries)
    print(f"[collect] papers={len(summaries)} judgments={len(records)} human_queue={len(queue)}")
    cmd_cost(args)


def cmd_cost(args) -> None:
    by_model: dict[tuple, dict] = defaultdict(lambda: defaultdict(float))
    for s in read_jsonl(args.out_dir / SUMMARIES_FILE):
        acc = by_model[(s["model"], s.get("effort"))]
        acc["papers"] += 1
        for k, v in (s.get("usage") or {}).items():
            acc[k] += v
        acc["usd"] += cost_usd(s["model"], s.get("usage") or {}) if s.get("usage") else 0.0
    for (model, effort), acc in sorted(by_model.items()):
        per_paper = acc["usd"] / acc["papers"] if acc["papers"] else 0.0
        line = (f"[cost] {model} effort={effort} papers={int(acc['papers'])} "
                f"in={int(acc['input_tokens'])} out={int(acc['output_tokens'])} "
                f"usd={acc['usd']:.4f} per_paper={per_paper:.5f}")
        if getattr(args, "project_papers", None):
            line += f" projected_{args.project_papers}={per_paper * args.project_papers:.2f}"
        print(line)


def cmd_screen(args) -> None:
    from openai import OpenAI

    import pipeline.common.config.settings  # noqa: F401  .env의 OPENAI_API_KEY를 읽는다

    sha = screen_prompt_sha()
    done = {screen_key(r["pmid"], r["ingredient_inci"], r["model"], r["prompt_sha"])
            for r in read_jsonl(args.out_dir / SCREEN_FILE) if r.get("status") == "ok"}
    items = [i for i in load_items(args.out_dir) if screen_key(i.pmid, i.ingredient, args.model, sha) not in done]
    if args.limit:
        items = items[: args.limit]
    print(f"[screen] {len(items)} items, model={args.model}")
    client = OpenAI(max_retries=3, timeout=60)
    for item in items:
        append_jsonl(args.out_dir / SCREEN_FILE, [screen_one(client, item, args.model)])
    rows = [r for r in read_jsonl(args.out_dir / SCREEN_FILE) if r["model"] == args.model and r["prompt_sha"] == sha]
    usd = sum(screen_cost_usd(args.model, r["usage"]) for r in rows)
    kept = sum(r["keep"] for r in rows)
    print(f"[screen] total={len(rows)} keep={kept} drop={len(rows) - kept} "
          f"not_ok={sum(r['status'] != 'ok' for r in rows)} usd={usd:.4f}")


def cmd_human_sheet(args) -> None:
    items = load_items(args.out_dir)
    strata = {}
    if args.strata_tsv:
        with open(args.strata_tsv, encoding="utf-8") as handle:
            strata = {r["pmid"]: r.get(args.strata_column, "") for r in csv.DictReader(handle, delimiter="\t")}
    papers = [{"pmid": i.pmid, "title": i.title, "source_text": i.source_text, "stratum": strata.get(i.pmid, "")}
              for i in items if i.ingredient == args.ingredient]
    picked = sample_for_human_review(papers, args.size, args.seed, "stratum")
    path = args.out_dir / f"human_review_{args.size}.csv"
    write_human_sheet(path, picked, args.ingredient)
    print(f"[human-sheet] {len(picked)} papers → {path}")


def cmd_agree(args) -> None:
    summaries = {s["pmid"]: s for s in read_jsonl(args.out_dir / SUMMARIES_FILE)
                 if s["model"] == args.model and s.get("status") == "ok"}
    records = defaultdict(list)
    for r in read_jsonl(args.out_dir / JUDGMENTS_FILE):
        if r["model"] == args.model and r["pmid"] in summaries:
            records[r["pmid"]].append(r)
    effects = ACNE_EFFECTS if args.direction_effects == "acne" else None
    labels = {pmid: paper_labels(s, records[pmid], effects) for pmid, s in summaries.items()}
    report = agreement(read_human_sheet(args.human_csv), labels)
    all_records = [r for rs in records.values() for r in rs]
    report["quote_failure_rate"] = (
        round(sum(not r["quote_verified"] for r in all_records) / len(all_records), 4) if all_records else None
    )
    report["model"] = args.model
    out = args.out_dir / f"agreement_{args.model}_{Path(args.human_csv).stem}.json"
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({k: (v if not isinstance(v, dict) else {kk: vv for kk, vv in v.items() if kk != "mismatches"})
                      for k, v in report.items()}, ensure_ascii=False))


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="LLM evidence review pilot (#49)")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("fetch-sources")
    p.add_argument("--pmids-tsv", type=Path, required=True)
    p.add_argument("--ingredient", required=True)
    p.add_argument("--out-dir", type=Path, required=True)

    p = sub.add_parser("screen")
    p.add_argument("--out-dir", type=Path, required=True)
    p.add_argument("--model", default="gpt-4o-mini")
    p.add_argument("--limit", type=int, default=None)

    p = sub.add_parser("submit")
    p.add_argument("--out-dir", type=Path, required=True)
    p.add_argument("--model", required=True)
    p.add_argument("--effort", default=None)
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--pmids", default=None, help="쉼표로 구분한 PMID만 제출")
    p.add_argument("--dry-run", action="store_true")

    p = sub.add_parser("collect")
    p.add_argument("--out-dir", type=Path, required=True)
    p.add_argument("--batch-id", required=True)
    p.add_argument("--poll-seconds", type=float, default=60.0)

    p = sub.add_parser("cost")
    p.add_argument("--out-dir", type=Path, required=True)
    p.add_argument("--project-papers", type=int, default=None)

    p = sub.add_parser("human-sheet")
    p.add_argument("--out-dir", type=Path, required=True)
    p.add_argument("--ingredient", default="SALICYLIC ACID")
    p.add_argument("--size", type=int, default=30)
    p.add_argument("--seed", type=int, default=49)
    p.add_argument("--strata-tsv", type=Path, default=None)
    p.add_argument("--strata-column", default="peel_or_procedure")

    p = sub.add_parser("agree")
    p.add_argument("--out-dir", type=Path, required=True)
    p.add_argument("--human-csv", type=Path, required=True)
    p.add_argument("--model", required=True)
    p.add_argument("--direction-effects", choices=("acne", "all"), default="acne")

    args = parser.parse_args(argv)
    if args.command == "fetch-sources":
        with open(args.pmids_tsv, encoding="utf-8") as handle:
            pmids = [r["pmid"] for r in csv.DictReader(handle, delimiter="\t")]
        fetch_sources(pmids, args.ingredient, args.out_dir)
    else:
        {"screen": cmd_screen, "submit": cmd_submit, "collect": cmd_collect, "cost": cmd_cost,
         "human-sheet": cmd_human_sheet, "agree": cmd_agree}[args.command](args)


if __name__ == "__main__":
    main()
