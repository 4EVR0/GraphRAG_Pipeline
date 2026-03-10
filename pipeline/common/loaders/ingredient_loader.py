import csv
from typing import Dict, List


def load_target_ingredients(csv_path: str) -> List[Dict[str, str]]:
    with open(csv_path, "r", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)
        rows: List[Dict[str, str]] = []

        for row in reader:
            if row.get("is_target", "").strip().lower() == "true":
                rows.append(row)

        return rows