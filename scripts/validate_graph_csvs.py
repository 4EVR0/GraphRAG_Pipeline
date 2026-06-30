#!/usr/bin/env python3
import argparse
import csv
from pathlib import Path


def read_ids(path: Path, column: str) -> set[str]:
    with path.open(encoding="utf-8-sig", newline="") as file:
        values = [row[column].strip() for row in csv.DictReader(file)]
    if any(not value for value in values):
        raise ValueError(f"{path}: blank {column}")
    if len(values) != len(set(values)):
        raise ValueError(f"{path}: duplicate {column}")
    return set(values)


def validate_relationship(
    path: Path,
    start_column: str,
    start_ids: set[str],
    end_column: str,
    end_ids: set[str],
) -> int:
    with path.open(encoding="utf-8-sig", newline="") as file:
        rows = list(csv.DictReader(file))
    missing_start = {row[start_column] for row in rows} - start_ids
    missing_end = {row[end_column] for row in rows} - end_ids
    if missing_start or missing_end:
        raise ValueError(
            f"{path}: missing start={len(missing_start)}, end={len(missing_end)}"
        )
    return len(rows)


def main(root: Path) -> None:
    nodes = root / "nodes"
    edges = root / "edges"
    products = read_ids(nodes / "product.csv", "product_id:ID(Product)")
    ingredients = read_ids(
        nodes / "ingredient.csv",
        "ingredient_id:ID(Ingredient)",
    )
    effects = read_ids(nodes / "effect.csv", "effect_code:ID(Effect)")
    concerns = read_ids(nodes / "concern.csv", "concern_code:ID(Concern)")

    counts = {
        "CONTAINS": validate_relationship(
            edges / "contains.csv",
            ":START_ID(Product)",
            products,
            ":END_ID(Ingredient)",
            ingredients,
        ),
        "AFFECTS": validate_relationship(
            edges / "affects.csv",
            ":START_ID(Ingredient)",
            ingredients,
            ":END_ID(Effect)",
            effects,
        ),
        "RELATES_TO": validate_relationship(
            edges / "relates_to.csv",
            ":START_ID(Effect)",
            effects,
            ":END_ID(Concern)",
            concerns,
        ),
    }
    print(
        "[OK] "
        f"nodes={len(products) + len(ingredients) + len(effects) + len(concerns)} "
        f"relationships={sum(counts.values())} {counts}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate Neo4j import CSV IDs.")
    parser.add_argument("root", nargs="?", type=Path, default=Path("gold"))
    args = parser.parse_args()
    main(args.root)
