"""Rejoue les détecteurs hors ligne à partir de l'état sauvegardé d'un run (data/state/<run>.json).

Sert à calibrer config/detectors.yaml sans relancer Spark : les profils de lot sont déjà calculés.
Usage : python scripts/replay_detectors.py faulty
"""
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from dq import detectors as D  # noqa: E402
from dq.settings import BATCHES, DATA  # noqa: E402


def replay(run: str) -> pd.DataFrame:
    hist = json.loads((DATA / "state" / f"{run}.json").read_text())
    cfg = D.load_detectors()
    state = D.DetectorState()
    rows = []
    for h in hist:
        comp = [{"column": c, "missing_rate": r} for c, r in h["missing"].items()]
        drift = D.detect(state, cfg, h["profile"], comp, h["month"])
        rows += [{"batch_month": h["month"], **d} for d in drift]
        D.remember(state, h["profile"], comp, h["month"])
    return pd.DataFrame(rows)


def main():
    run = sys.argv[1] if len(sys.argv) > 1 else "faulty"
    d = replay(run)
    man = {f["batch_month"]: f["fault_type"] for f in json.loads((BATCHES / "fault_manifest.json").read_text("utf-8"))}
    an = d[d.is_anomaly]
    an = an.assign(name=an.detector + "/" + an.column.fillna("").astype(str))
    print("Défauts :")
    for m, f in man.items():
        print(f"  {m} {f:15s} -> {sorted(an[an.batch_month == m].name)}")
    healthy = an[~an.batch_month.isin(man)]
    print(f"\nAnomalies dans les lots sains : {len(healthy)} sur {healthy.batch_month.nunique()} lots")
    print(healthy.groupby("name").size().sort_values(ascending=False).to_string())
    norm = d[(~d.batch_month.isin(man)) & (d.batch_month >= "2017-01")]
    q = norm.groupby(["detector", "column"], dropna=False)["value"].quantile([.5, .95, 1.0]).unstack().round(3)
    fa = d[d.batch_month.isin(man)].pivot_table(index=["detector", "column"], columns="batch_month",
                                                 values="value", aggfunc="first").round(2)
    print("\nDistribution des valeurs (lots sains) :\n", q.to_string())
    print("\nValeurs dans les lots abîmés :\n", fa.to_string())


if __name__ == "__main__":
    main()
