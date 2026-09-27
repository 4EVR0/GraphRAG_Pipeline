#!/usr/bin/env python3
"""Log source-linked card extraction as a separate, local exploratory MLflow run."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from pathlib import Path

import mlflow
from mlflow import MlflowClient


EXPERIMENT = "graphrag-pmc-evidence-card-poc"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tracking-uri", required=True)
    parser.add_argument("--artifact-root", type=Path, required=True)
    parser.add_argument("--report", type=Path, default=Path("poc_output/pmc_fulltext/evidence_cards.json"))
    args = parser.parse_args()
    if not args.tracking_uri.startswith("sqlite:////"):
        parser.error("This PoC only logs to an absolute local SQLite MLflow backend")
    report_bytes = args.report.read_bytes()
    report = json.loads(report_bytes)
    records = report["records"]
    if len(records) != 3 or len({record["pmid"] for record in records}) != 3:
        raise ValueError("Expected exactly three distinct PMID cards")
    if report["prompt_version"] != "pmc_evidence_card_v1":
        raise ValueError("Unexpected prompt version")
    digest = hashlib.sha256(report_bytes).hexdigest()
    sha = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    args.artifact_root.mkdir(parents=True, exist_ok=True)
    mlflow.set_tracking_uri(args.tracking_uri)
    client = MlflowClient()
    experiment = client.get_experiment_by_name(EXPERIMENT)
    experiment_id = (
        experiment.experiment_id if experiment else
        client.create_experiment(EXPERIMENT, artifact_location=args.artifact_root.resolve().as_uri())
    )
    existing = client.search_runs([experiment_id], filter_string=f"tags.report_sha256 = '{digest}'", max_results=1)
    if existing:
        print(f"Already logged: {existing[0].info.run_id}")
        return 0
    with mlflow.start_run(experiment_id=experiment_id, run_name=f"pmc-3paper-card-{sha[:7]}", tags={
        "phase": "exploratory", "comparable_to_production": "false",
        "source": "PMC BioC + E-utilities", "git_sha": sha,
        "report_sha256": digest, "human_reviewed": "false",
    }) as run:
        mlflow.log_params({
            "extractor_model": records[0]["model"],
            "prompt_version": report["prompt_version"],
            "pmids": ",".join(record["pmid"] for record in records),
        })
        mlflow.log_metrics({
            "papers": len(records),
            "validated_cards": sum(record["validation"] == "passed" for record in records),
            "rejected_cards": sum(record["validation"] == "rejected" for record in records),
            "input_tokens": sum(record.get("input_tokens") or 0 for record in records),
            "output_tokens": sum(record.get("output_tokens") or 0 for record in records),
        })
        mlflow.log_artifact(str(args.report), artifact_path="local_reports")
        print(f"Logged exploratory MLflow run: {run.info.run_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
