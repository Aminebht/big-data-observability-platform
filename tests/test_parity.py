"""Parité Spark / Polars : les deux moteurs doivent produire exactement les mêmes comptes par règle.

Exécution : .venv\\Scripts\\python.exe -m pytest tests -q   (ou python tests/test_parity.py)
Utilise deux lots mensuels réels (un sain, un abîmé) de data/batches/faulty.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import polars as pl  # noqa: E402

from dq import polars_engine as P  # noqa: E402
from dq.rules import evaluate_spark, load_rules  # noqa: E402
from dq.schema import typed_view  # noqa: E402
from dq.settings import BATCHES, get_spark  # noqa: E402

MONTHS = ["2017-02", "2018-08", "2019-03", "2022-06"]


def compare(month, spark, rules):
    path = BATCHES / "faulty" / f"{month}.parquet"
    _, rs, cs = evaluate_spark(typed_view(spark.read.parquet(str(path))), rules)
    _, rp, cp = P.evaluate(P.typed_view(pl.scan_parquet(str(path))).collect(), rules)
    diffs = []
    for a, b in zip(rs, rp):
        if (a["rows_checked"], a["rows_failed"]) != (b["rows_checked"], b["rows_failed"]):
            diffs.append((month, a["rule_id"], a["rows_checked"], b["rows_checked"], a["rows_failed"], b["rows_failed"]))
    for a, b in zip(cs, cp):
        if a["missing"] != b["missing"]:
            diffs.append((month, "missing:" + a["column"], a["missing"], b["missing"]))
    return diffs


def test_parity():
    spark = get_spark("parity", cores=4, driver_memory="2g")
    rules = load_rules()
    diffs = [d for m in MONTHS for d in compare(m, spark, rules)]
    spark.stop()
    assert not diffs, diffs


if __name__ == "__main__":
    spark = get_spark("parity", cores=4, driver_memory="2g")
    rules = load_rules()
    all_diffs = [d for m in MONTHS for d in compare(m, spark, rules)]
    spark.stop()
    print("PARITÉ OK" if not all_diffs else "\n".join(map(str, all_diffs)))
