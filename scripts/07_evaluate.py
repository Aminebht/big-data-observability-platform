"""Section 4 – Efficacité des règles et détecteurs, mesurée sur les défauts injectés.

Méthode différentielle : les runs `clean` et `faulty` traitent les mêmes lots, seuls ~12 lots diffèrent.
Une alerte du run `faulty` est « nouvelle » si la même alerte (source, règle/détecteur, colonne)
n'existe pas pour le même mois dans le run `clean`.
  - défaut détecté      : au moins une alerte nouvelle dans le lot abîmé
  - vrai positif (VP)    : alerte nouvelle dans un lot abîmé
  - faux positif (FP)    : alerte nouvelle dans un lot sain (effet de bord, p. ex. référence polluée)
Sorties : data/metrics/fact_fault_eval.csv, data/metrics/fact_detector_eval.csv, report/evaluation_summary.md
"""
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from dq.settings import BATCHES, METRICS  # noqa: E402


def main():
    alerts = pd.read_csv(METRICS / "fact_alert.csv", dtype=str).fillna("")
    manifest = json.loads((BATCHES / "fault_manifest.json").read_text("utf-8"))
    faulty_months = {f["batch_month"] for f in manifest}
    key = ["batch_month", "source", "detector_or_rule", "column"]
    clean = alerts[alerts.run_id == "clean"][key].drop_duplicates()
    faulty = alerts[alerts.run_id == "faulty"].copy()
    merged = faulty.merge(clean.assign(_in_clean=1), on=key, how="left")
    new = merged[merged._in_clean.isna()].copy()
    new["is_fault_batch"] = new.batch_month.isin(faulty_months)

    rows = []
    for f in manifest:
        a = new[new.batch_month == f["batch_month"]]
        got = sorted(set(a.detector_or_rule))
        expected = f["expected_detection"]
        rows.append({"batch_month": f["batch_month"], "fault_type": f["fault_type"], "description": f["description"],
                     "expected": ";".join(expected), "detected_by": ";".join(got), "detected": bool(len(a)),
                     "expected_hit": bool(set(expected) & {g.replace("SCHEMA", "schema") for g in got}),
                     "new_alerts": len(a)})
    faults = pd.DataFrame(rows)

    det = (new.groupby("detector_or_rule")
           .agg(tp=("is_fault_batch", "sum"), total=("is_fault_batch", "size")).reset_index())
    det["fp"] = det.total - det.tp
    det["precision"] = det.tp / det.total
    det = det.sort_values("detector_or_rule")

    faults.to_csv(METRICS / "fact_fault_eval.csv", index=False)
    det.to_csv(METRICS / "fact_detector_eval.csv", index=False)

    recall = faults.detected.mean()
    lines = ["# Évaluation sur défauts injectés", "",
             f"- Défauts injectés : {len(faults)} ; détectés : {int(faults.detected.sum())} "
             f"(**rappel {recall * 100:.0f} %**)",
             f"- Détectés par le détecteur/règle attendu : {int(faults.expected_hit.sum())}",
             f"- Alertes nouvelles dans les lots abîmés (VP) : {int(new.is_fault_batch.sum())} ; "
             f"dans des lots sains (FP) : {int((~new.is_fault_batch).sum())}", "",
             "| Mois | Défaut | Attendu | Détecté par | OK |", "|---|---|---|---|---|"]
    for r in faults.itertuples():
        lines.append(f"| {r.batch_month} | {r.description} | {r.expected} | {r.detected_by or '—'} | "
                     f"{'✅' if r.detected else '❌'} |")
    lines += ["", "| Règle / détecteur | VP | FP | Précision |", "|---|---|---|---|"]
    for r in det.itertuples():
        lines.append(f"| {r.detector_or_rule} | {r.tp} | {r.fp} | {r.precision * 100:.0f} % |")
    (ROOT / "report" / "evaluation_summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
