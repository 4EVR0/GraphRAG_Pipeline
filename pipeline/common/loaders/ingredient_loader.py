import csv
from pathlib import Path
from typing import Dict, List, Union


def load_target_ingredients(csv_path: Union[str, Path]) -> List[Dict[str, str]]:
    with open(Path(csv_path), "r", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)
        rows: List[Dict[str, str]] = []

        for row in reader:
            if row.get("is_target", "").strip().lower() == "true":
                rows.append(row)

        return rows