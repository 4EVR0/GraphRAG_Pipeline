"""근거 검수 시범 CLI (#49). 결과는 --out-dir(로컬)에만 쓴다.

python -m pipeline.review.run_review fetch-sources --pmids-tsv T --ingredient "SALICYLIC ACID" --out-dir D
python -m pipeline.review.run_review sources-from-bronze --bronze-dir B --out-dir D
python -m pipeline.review.run_review screen --out-dir D [--model gpt-5-mini] [--limit 10]
python -m pipeline.review.run_review screen-submit --out-dir D [--model gpt-5-mini] [--limit N] [--dry-run]
python -m pipeline.review.run_review screen-collect --out-dir D --batch-id batch_...
python -m pipeline.review.run_review submit --out-dir D --model claude-sonnet-5-5 --effort medium --screened-only [--limit 10]
python -m pipeline.review.run_review review-sync --out-dir D --gateway baze --model claude-sonnet-5-5 --effort medium [--limit 10]
python -m pipeline.review.run_review collect --out-dir D --batch-id msgbatch_...
python -m pipeline.review.run_review human-sheet --out-dir D --size 30 --strata-tsv T
python -m pipeline.review.run_review agree --out-dir D --human-csv H --model M [--direction-effects acne|all]
python -m pipeline.review.run_review cost --out-dir D [--project-papers 10000]
python -m pipeline.review.run_review score --out-dir D [--cosing-gold CSV] [--mfds CSV] [--model M]
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
    run_sync,
    submit,
    wait,
)
from pipeline.review.schema import ACNE_EFFECTS, effect_in_quote, prompt_sha
from pipeline.review.scoring import load_cosing_functions, load_mfds_functional, score_records
from pipeline.review.screen import (
    DEFAULT_SCREEN_MODEL,
    SCREEN_PROMPT_VERSION,
    SCREEN_PROMPTS,
    parse_batch_output,
    screen_cost_usd,
    screen_custom_id,
    screen_key,
    screen_one,
    screen_prompt_sha,
    submit_batch,
    write_batch_file,
)
from pipeline.review.validate import judge, quote_in_source

SOURCES_FILE = "sources.jsonl"
BATCHES_FILE = "batches.jsonl"
JUDGMENTS_FILE = "judgments.jsonl"
SUMMARIES_FILE = "summaries.jsonl"
QUEUE_FILE = "human_queue.jsonl"
SCREEN_FILE = "screen.jsonl"
SCREEN_BATCHES_FILE = "screen_batches.jsonl"
# 게이트웨이가 json_schema 출력을 지원하지 않을 때 프롬프트로 형식을 요구한다(검증은 그대로).
NO_SCHEMA_SUFFIX = (
    "\n\nRespond with only one JSON object with keys relevant (boolean), needs_fulltext (boolean), "
    "reason (string), and judgments (array of objects with the fields above). No markdown fences."
)


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
    """판정을 받은 건. 호출 오류(batch_errored 등)는 다시 보낼 수 있게 뺀다."""
    return {review_key(s["pmid"], s["ingredient_inci"], s["model"], s["prompt_sha"])
            for s in read_jsonl(out_dir / SUMMARIES_FILE) if not s["status"].startswith("batch_")}


# Batch가 없는 Anthropic 호환 게이트웨이. 키는 .env에서 읽는다.
GATEWAYS = {
    "anthropic": {"base_url": None, "key_env": "ANTHROPIC_API_KEY"},
    "baze": {"base_url": "https://factchat-cloud.mindlogic.ai/v1/gateway/claude", "key_env": "BAZE_API_KEY"},
}


def _client(gateway: str = "anthropic"):
    import os

    import anthropic

    import pipeline.common.config.settings  # noqa: F401  .env를 읽는다

    # 응답이 brotli로 오면 Brotli<1.2 환경에서 SDK의 압축 해제가 실패해 연결 오류로 보인다
    # (Anthropic API·BAZE 게이트웨이 모두). gzip만 받는다.
    headers = {"Accept-Encoding": "gzip, deflate"}
    config = GATEWAYS[gateway]
    if config["base_url"] is None:
        return anthropic.Anthropic(default_headers=headers)
    key = os.environ.get(config["key_env"])
    if not key:
        raise SystemExit(f"{config['key_env']} is not set in .env")
    return anthropic.Anthropic(api_key=key, base_url=config["base_url"], max_retries=3, default_headers=headers)


def _judge_rows(out_dir: Path, results: list[dict], model: str, effort: str | None, sha: str, run_id: str) -> None:
    by_id = {custom_id(item, sha): item for item in load_items(out_dir)}
    reviewed_at = datetime.now(timezone.utc).isoformat()
    records, queue, summaries = [], [], []
    for result in results:
        item = by_id[result["custom_id"]]
        r, q, s = judge(item, result, model, sha, reviewed_at)
        s.update(batch_id=run_id, effort=effort)
        records += r
        queue += q
        summaries.append(s)
    append_jsonl(out_dir / JUDGMENTS_FILE, records)
    append_jsonl(out_dir / QUEUE_FILE, queue)
    append_jsonl(out_dir / SUMMARIES_FILE, summaries)
    print(f"[judge] papers={len(summaries)} judgments={len(records)} human_queue={len(queue)} "
          f"quote_failures={sum(s['quote_failures'] for s in summaries)} "
          f"not_ok={sum(s['status'] != 'ok' for s in summaries)}")


def cmd_review_sync(args) -> None:
    items = load_items(args.out_dir)
    if args.pmids:
        wanted = set(args.pmids.split(","))
        items = [i for i in items if i.pmid in wanted]
    requests, skipped = build_requests(items, args.model, args.effort, done_keys(args.out_dir))
    if args.limit:
        requests = requests[: args.limit]
    if args.no_schema:
        for request in requests:
            request["params"]["output_config"].pop("format", None)
            if not request["params"]["output_config"]:
                request["params"].pop("output_config")
            request["params"]["system"] += NO_SCHEMA_SUFFIX
    print(f"[review-sync] gateway={args.gateway} {len(requests)} requests, {len(skipped)} already reviewed")
    if not requests:
        return
    run_id = f"sync_{args.gateway}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}"
    results_path = args.out_dir / f"results_{run_id}.jsonl"
    results = run_sync(_client(args.gateway), requests, on_result=lambda row: append_jsonl(results_path, [row]))
    _judge_rows(args.out_dir, results, args.model, args.effort, prompt_sha(), run_id)
    cmd_cost(args)


def cmd_submit(args) -> None:
    items = load_items(args.out_dir)
    if args.screened_only:
        screened = latest_screen(args.out_dir, args.screen_model, args.screen_version)
        # 거르기 결과가 없거나 실패한 건은 보내지 않는다. 남김(keep)만 본 검수로 보낸다.
        items = [i for i in items if screened.get((i.pmid, i.ingredient.upper()), {}).get("keep")
                 and screened[(i.pmid, i.ingredient.upper())]["status"] == "ok"]
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
    ordered = [results.get(cid, {"custom_id": cid, "result_type": "missing"}) for cid in meta["custom_ids"]]
    _judge_rows(args.out_dir, ordered, meta["model"], meta["effort"], meta["prompt_sha"], args.batch_id)
    cmd_cost(args)


def cmd_cost(args) -> None:
    by_model: dict[tuple, dict] = defaultdict(lambda: defaultdict(float))
    for s in read_jsonl(args.out_dir / SUMMARIES_FILE):
        if s["status"].startswith("batch_"):
            continue  # 호출 오류는 과금되지 않는다
        acc = by_model[(s["model"], s.get("effort"))]
        acc["papers"] += 1
        for k, v in (s.get("usage") or {}).items():
            acc[k] += v
        batch = not str(s.get("batch_id") or "").startswith("sync_")  # 게이트웨이 동기 호출은 할인 없음
        acc["usd"] += cost_usd(s["model"], s.get("usage") or {}, batch=batch) if s.get("usage") else 0.0
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

    sha = screen_prompt_sha(args.prompt_version)
    done = {screen_key(r["pmid"], r["ingredient_inci"], r["model"], r["prompt_sha"])
            for r in read_jsonl(args.out_dir / SCREEN_FILE) if r.get("status") == "ok"}
    items = [i for i in load_items(args.out_dir) if screen_key(i.pmid, i.ingredient, args.model, sha) not in done]
    if args.pmids:
        wanted = set(args.pmids.split(","))
        items = [i for i in items if i.pmid in wanted]
    if args.limit:
        items = items[: args.limit]
    print(f"[screen] {len(items)} items, model={args.model}, prompt={args.prompt_version}")
    client = OpenAI(max_retries=3, timeout=60)
    for item in items:
        append_jsonl(args.out_dir / SCREEN_FILE, [screen_one(client, item, args.model, args.prompt_version)])
    rows = [r for r in read_jsonl(args.out_dir / SCREEN_FILE) if r["model"] == args.model and r["prompt_sha"] == sha]
    usd = sum(screen_cost_usd(args.model, r["usage"]) for r in rows)
    kept = sum(r["keep"] for r in rows)
    print(f"[screen] total={len(rows)} keep={kept} drop={len(rows) - kept} "
          f"not_ok={sum(r['status'] != 'ok' for r in rows)} usd={usd:.4f}")


def write_csv(path: Path, rows: list[dict]) -> None:
    fields = list(dict.fromkeys(k for row in rows for k in row))
    with open(path, "w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def cmd_score(args) -> None:
    """판정을 점수 규칙으로 (성분, 효능) 근거 점수로 바꾼다. 그래프에는 쓰지 않는다(shadow)."""
    ok = {review_key(s["pmid"], s["ingredient_inci"], s["model"], s["prompt_sha"])
          for s in read_jsonl(args.out_dir / SUMMARIES_FILE) if s["status"] == "ok"}
    sources = {(r["pmid"], r["ingredient"].upper()): r for r in read_jsonl(args.out_dir / SOURCES_FILE)}
    records = []
    for record in read_jsonl(args.out_dir / JUDGMENTS_FILE):
        if args.model and record["model"] != args.model:
            continue
        if review_key(record["pmid"], record["ingredient_inci"], record["model"], record["prompt_sha"]) not in ok:
            continue
        src = sources[(record["pmid"], record["ingredient_inci"].upper())]
        # 이전 판정도 현재 대조 규칙으로 다시 확인한다.
        record["quote_verified"] = quote_in_source(record["evidence_quote"], src["title"], src["source_text"])
        record["effect_in_quote"] = effect_in_quote(record["effect_code"], record["evidence_quote"])
        records.append(record)
    cosing = load_cosing_functions(args.cosing_gold) if args.cosing_gold else None
    mfds = load_mfds_functional(args.mfds) if args.mfds else None
    scored, edges = score_records(records, cosing, mfds)
    write_csv(args.out_dir / "judgments_scored.csv", scored)
    write_csv(args.out_dir / "review_edges.csv", edges)
    used = sum(1 for r in scored if r["weight"] > 0)
    print(f"[score] judgments={len(scored)} weighted>0={used} edges={len(edges)} "
          f"cosing={'yes' if cosing else 'no'} mfds={'yes' if mfds else 'no'}")


def sources_from_bronze(bronze_dir: Path, out_dir: Path) -> int:
    """narrow 수집 배치(pair_pmids.csv + paper_raw.csv)에서 (논문, 성분) 검수 목록을 만든다."""
    with open(bronze_dir / "paper_raw.csv", encoding="utf-8-sig", newline="") as handle:
        papers = {r["pmid"]: r for r in csv.DictReader(handle) if (r.get("abstract_text") or "").strip()}
    existing = {(r["pmid"], r["ingredient"].upper()) for r in read_jsonl(out_dir / SOURCES_FILE)}
    rows = []
    with open(bronze_dir / "pair_pmids.csv", encoding="utf-8-sig", newline="") as handle:
        for pair in csv.DictReader(handle):
            key = (pair["pmid"], pair["inci_name"].upper())
            paper = papers.get(pair["pmid"])
            if paper is None or key in existing:
                continue
            existing.add(key)
            rows.append({"pmid": pair["pmid"], "ingredient": pair["inci_name"], "title": paper.get("title") or "",
                         "source_text": paper["abstract_text"], "source": "abstract"})
    append_jsonl(out_dir / SOURCES_FILE, rows)
    return len(rows)


def _openai_client():
    from openai import OpenAI

    import pipeline.common.config.settings  # noqa: F401  .env의 OPENAI_API_KEY를 읽는다

    return OpenAI(max_retries=3, timeout=300)


def latest_screen(out_dir: Path, model: str, version: str) -> dict[tuple[str, str], dict]:
    """(PMID, 성분)별 가장 마지막 거르기 결과."""
    sha = screen_prompt_sha(version)
    latest = {}
    for row in read_jsonl(out_dir / SCREEN_FILE):
        if row["model"] == model and row["prompt_sha"] == sha:
            latest[(row["pmid"], row["ingredient_inci"].upper())] = row
    return latest


def cmd_screen_submit(args) -> None:
    done = {k for k, r in latest_screen(args.out_dir, args.model, args.prompt_version).items() if r["status"] == "ok"}
    pending = {cid for b in read_jsonl(args.out_dir / SCREEN_BATCHES_FILE) if not b.get("collected") for cid in b["custom_ids"]}
    items = [i for i in load_items(args.out_dir)
             if (i.pmid, i.ingredient.upper()) not in done and screen_custom_id(i, args.prompt_version) not in pending]
    if args.limit:
        items = items[: args.limit]
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    path = args.out_dir / f"screen_requests_{stamp}.jsonl"
    by_id = write_batch_file(path, items, args.model, args.prompt_version)
    print(f"[screen-submit] {len(by_id)} requests → {path.name}, model={args.model}, prompt={args.prompt_version}")
    if args.dry_run or not by_id:
        return
    batch_id = submit_batch(_openai_client(), path)
    append_jsonl(args.out_dir / SCREEN_BATCHES_FILE, [{
        "batch_id": batch_id, "model": args.model, "prompt_version": args.prompt_version,
        "request_file": path.name, "custom_ids": list(by_id), "submitted_at": datetime.now(timezone.utc).isoformat(),
        "collected": False,
    }])
    print(f"[screen-submit] batch_id={batch_id} (노트북을 꺼도 OpenAI 서버에서 처리, 최대 24시간)")


def cmd_screen_collect(args) -> None:
    batches = read_jsonl(args.out_dir / SCREEN_BATCHES_FILE)
    meta = next(b for b in batches if b["batch_id"] == args.batch_id)
    if meta.get("collected"):
        print(f"[screen-collect] {args.batch_id} 이미 수집함")
        return
    client = _openai_client()
    batch = client.batches.retrieve(args.batch_id)
    counts = getattr(batch, "request_counts", None)
    print(f"[screen-collect] status={batch.status} counts={counts}")
    if batch.status != "completed":
        return
    lines = []
    for file_id in (batch.output_file_id, batch.error_file_id):
        if file_id:
            lines += client.files.content(file_id).text.splitlines()
    by_id = {screen_custom_id(i, meta["prompt_version"]): i for i in load_items(args.out_dir)}
    by_id = {cid: by_id[cid] for cid in meta["custom_ids"] if cid in by_id}
    rows = parse_batch_output(lines, by_id, meta["model"], meta["prompt_version"])
    for row in rows:
        row["batch_id"] = args.batch_id
    append_jsonl(args.out_dir / SCREEN_FILE, rows)
    for b in batches:
        if b["batch_id"] == args.batch_id:
            b["collected"] = True
    (args.out_dir / SCREEN_BATCHES_FILE).write_text(
        "".join(json.dumps(b, ensure_ascii=False) + "\n" for b in batches), encoding="utf-8")
    missing = len(meta["custom_ids"]) - len(rows)
    usd = sum(screen_cost_usd(meta["model"], r["usage"], batch=True) for r in rows if r["usage"])
    print(f"[screen-collect] rows={len(rows)} keep={sum(r['keep'] for r in rows)} "
          f"not_ok={sum(r['status'] != 'ok' for r in rows)} missing={missing} usd={usd:.4f}")


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
    # 판정 형식 버전이 여럿이면 섞이지 않게 prompt_sha 하나만 쓴다(기본: 현재 버전).
    sha = args.prompt_sha or prompt_sha()
    summaries = {s["pmid"]: s for s in read_jsonl(args.out_dir / SUMMARIES_FILE)
                 if s["model"] == args.model and s.get("status") == "ok" and s["prompt_sha"] == sha}
    records = defaultdict(list)
    for r in read_jsonl(args.out_dir / JUDGMENTS_FILE):
        if r["model"] == args.model and r["prompt_sha"] == sha and r["pmid"] in summaries:
            records[r["pmid"]].append(r)
    effects = ACNE_EFFECTS if args.direction_effects == "acne" else None
    labels = {pmid: paper_labels(s, records[pmid], effects) for pmid, s in summaries.items()}
    report = agreement(read_human_sheet(args.human_csv), labels)
    all_records = [r for rs in records.values() for r in rs]
    report["quote_failure_rate"] = (
        round(sum(not r["quote_verified"] for r in all_records) / len(all_records), 4) if all_records else None
    )
    report["model"] = args.model
    report["prompt_sha"] = sha
    report["papers_compared"] = len(summaries)
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
    p.add_argument("--model", default=DEFAULT_SCREEN_MODEL)
    p.add_argument("--prompt-version", default=SCREEN_PROMPT_VERSION, choices=sorted(SCREEN_PROMPTS))
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--pmids", default=None, help="쉼표로 구분한 PMID만 거른다")

    p = sub.add_parser("sources-from-bronze")
    p.add_argument("--bronze-dir", type=Path, required=True)
    p.add_argument("--out-dir", type=Path, required=True)

    p = sub.add_parser("screen-submit")
    p.add_argument("--out-dir", type=Path, required=True)
    p.add_argument("--model", default=DEFAULT_SCREEN_MODEL)
    p.add_argument("--prompt-version", default=SCREEN_PROMPT_VERSION, choices=sorted(SCREEN_PROMPTS))
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--dry-run", action="store_true")

    p = sub.add_parser("screen-collect")
    p.add_argument("--out-dir", type=Path, required=True)
    p.add_argument("--batch-id", required=True)

    p = sub.add_parser("submit")
    p.add_argument("--out-dir", type=Path, required=True)
    p.add_argument("--model", required=True)
    p.add_argument("--effort", default=None)
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--pmids", default=None, help="쉼표로 구분한 PMID만 제출")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--screened-only", action="store_true", help="거르기에서 남긴 (논문, 성분)만 제출")
    p.add_argument("--screen-model", default=DEFAULT_SCREEN_MODEL)
    p.add_argument("--screen-version", default=SCREEN_PROMPT_VERSION, choices=sorted(SCREEN_PROMPTS))

    p = sub.add_parser("review-sync")
    p.add_argument("--out-dir", type=Path, required=True)
    p.add_argument("--gateway", choices=sorted(GATEWAYS), default="baze")
    p.add_argument("--model", default="claude-sonnet-5-5")
    p.add_argument("--effort", default="medium")
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--pmids", default=None)
    p.add_argument("--no-schema", action="store_true", help="json_schema 출력을 끄고 프롬프트로 JSON을 요구한다")

    p = sub.add_parser("collect")
    p.add_argument("--out-dir", type=Path, required=True)
    p.add_argument("--batch-id", required=True)
    p.add_argument("--poll-seconds", type=float, default=60.0)

    p = sub.add_parser("cost")
    p.add_argument("--out-dir", type=Path, required=True)
    p.add_argument("--project-papers", type=int, default=None)

    p = sub.add_parser("score")
    p.add_argument("--out-dir", type=Path, required=True)
    p.add_argument("--cosing-gold", type=Path, default=None, help="KCIA↔CosIng Gold CSV(inci_name, cosing_functions)")
    p.add_argument("--mfds", type=Path, default=None, help="식약처 기능성 고시 원료 CSV(inci_name, function, effect_codes)")
    p.add_argument("--model", default=None)

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
    p.add_argument("--prompt-sha", default=None, help="비교할 판정 형식 버전(기본: 현재 프롬프트)")

    args = parser.parse_args(argv)
    if args.command == "sources-from-bronze":
        print(f"[sources] {sources_from_bronze(args.bronze_dir, args.out_dir)} added")
        return
    if args.command == "fetch-sources":
        with open(args.pmids_tsv, encoding="utf-8") as handle:
            pmids = [r["pmid"] for r in csv.DictReader(handle, delimiter="\t")]
        fetch_sources(pmids, args.ingredient, args.out_dir)
    else:
        {"screen": cmd_screen, "screen-submit": cmd_screen_submit, "screen-collect": cmd_screen_collect,
         "submit": cmd_submit, "review-sync": cmd_review_sync, "collect": cmd_collect, "cost": cmd_cost,
         "human-sheet": cmd_human_sheet, "score": cmd_score, "agree": cmd_agree}[args.command](args)


if __name__ == "__main__":
    main()
