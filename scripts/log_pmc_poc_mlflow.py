#!/usr/bin/env python3
"""Record one exploratory PMC PoC A/B in a separate local MLflow experiment."""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import subprocess
from pathlib import Path

import mlflow
from mlflow import MlflowClient


EXPERIMENT = "graphrag-pmc-fulltext-poc"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tracking-uri", required=True, help="Existing local MLflow backend URI")
    parser.add_argument("--artifact-root", type=Path, required=True)
    parser.add_argument("--responses", type=Path, default=Path("poc_output/pmc_fulltext/ab_responses.json"))
    parser.add_argument("--manifest", type=Path, default=Path("poc_output/pmc_fulltext/manifest.json"))
    parser.add_argument("--db", type=Path, default=Path("poc_output/pmc_fulltext/pmc_poc.sqlite3"))
    args = parser.parse_args()
    if not args.tracking_uri.startswith("sqlite:////"):
        parser.error("This PoC only logs to an absolute local SQLite MLflow backend")
    response_bytes = args.responses.read_bytes()
    report = json.loads(response_bytes)
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    responses = report["responses"]
    if len(manifest) != 3 or len(responses) != 6:
        raise ValueError("Expected exactly three papers and six paired responses")
    if any(p["license"] != "CC BY" for p in manifest):
        raise ValueError("Non-CC-BY paper in PoC manifest")
    with sqlite3.connect(args.db) as db:
        paper_count = db.execute("SELECT COUNT(*) FROM paper").fetchone()[0]
        passage_count = db.execute("SELECT COUNT(*) FROM passage").fetchone()[0]
    digest = hashlib.sha256(response_bytes).hexdigest()
    sha = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    args.artifact_root.mkdir(parents=True, exist_ok=True)
    mlflow.set_tracking_uri(args.tracking_uri)
    client = MlflowClient()
    experiment = client.get_experiment_by_name(EXPERIMENT)
    if experiment is None:
        experiment_id = client.create_experiment(EXPERIMENT, artifact_location=args.artifact_root.resolve().as_uri())
    else:
        experiment_id = experiment.experiment_id
    existing = client.search_runs([experiment_id], filter_string=f"tags.report_sha256 = '{digest}'", max_results=1)
    if existing:
        print(f"Already logged: {existing[0].info.run_id}")
        return 0
    with mlflow.start_run(experiment_id=experiment_id, run_name=f"pmc-3paper-ab-{sha[:7]}", tags={
        "phase": "exploratory", "comparable_to_production": "false",
        "source": "PMC BioC + E-utilities", "source_license": "CC BY",
        "git_sha": sha, "report_sha256": digest,
    }) as run:
        mlflow.log_params({
            "generator_model": responses[0]["model"],
            "prompt_version": report["prompt_version"],
            "paper_versions": ",".join(p["pmcid_version"] for p in manifest),
        })
        mlflow.log_metrics({
            "papers": paper_count, "passages": passage_count,
            "paired_questions": len(responses) // 2, "generated_responses": len(responses),
            "input_tokens": sum(r.get("input_tokens") or 0 for r in responses),
            "output_tokens": sum(r.get("output_tokens") or 0 for r in responses),
        })
        mlflow.log_artifact(str(args.responses), artifact_path="local_reports")
        mlflow.log_artifact(str(args.manifest), artifact_path="local_reports")
        print(f"Logged exploratory MLflow run: {run.info.run_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
