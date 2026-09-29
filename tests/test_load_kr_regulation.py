import tempfile
import unittest
from pathlib import Path

from scripts.load_kr_regulation_to_neo4j import load_rows, run_id_of

HEADER = "inci_name,kor_name,kr_reg_status,kr_limit_note,match_basis,reg_ids,notice_names\n"


def _write(tmp: Path, body: str) -> Path:
    path = tmp / "run_id=mfds_regulation_20260929_020512" / "ingredient_kr_regulation.csv"
    path.parent.mkdir(parents=True)
    path.write_text(HEADER + body, encoding="utf-8-sig")
    return path


class LoadKrRegulationTest(unittest.TestCase):
    def test_reads_bom_csv_and_keeps_note_only_for_restricted(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            path = _write(Path(d), "AZELAIC ACID,아젤라익애씨드,banned,,name_inci,kr-1,x\n"
                                   "PHENOXYETHANOL,페녹시에탄올,restricted,1%,name_inci,kr-2,x\n"
                                   "TALC,탤크,conditional,무시,name_inci,kr-3,x\n")
            rows = load_rows(path)
            self.assertEqual("mfds_regulation_20260929_020512", run_id_of(path))
        self.assertEqual([("AZELAIC ACID", "banned", ""), ("PHENOXYETHANOL", "restricted", "1%"),
                          ("TALC", "conditional", "")],
                         [(r["inci_name"], r["kr_reg_status"], r["kr_limit_note"]) for r in rows])

    def test_rejects_unknown_status_and_duplicates(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaises(ValueError):
                load_rows(_write(Path(d) / "a", "X,x,forbidden,,n,kr-1,x\n"))
            with self.assertRaises(ValueError):
                load_rows(_write(Path(d) / "b", "X,x,banned,,n,kr-1,x\nX,x,banned,,n,kr-2,x\n"))


if __name__ == "__main__":
    unittest.main()
