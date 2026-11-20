"""Écriture des tables de résultats (schéma en étoile CSV) lues par Power BI.

Chaque table est un fichier CSV dans data/metrics/ ; on AJOUTE des lignes après chaque lot.
Toutes les lignes portent un `run_id` pour filtrer les exécutions dans Power BI.
"""
from __future__ import annotations

import csv
import json
import threading
from datetime import datetime, timezone
from pathlib import Path

from .settings import LOGS, METRICS

TABLES = {
    "dim_batch": ["run_id", "batch_id", "batch_month", "rows", "processed_at", "duration_s",
                  "global_score", "status", "forced_reasons", "score_completeness", "score_uniqueness",
                  "score_validity", "score_consistency", "score_conformity", "schema_changed",
                  "missing_columns", "extra_columns", "planted_faults"],
    "fact_rule_result": ["run_id", "batch_id", "batch_month", "rule_id", "dimension", "severity", "mode",
                         "rows_checked", "rows_failed", "failure_rate", "conformity_rate", "status"],
    "fact_column_completeness": ["run_id", "batch_id", "batch_month", "column", "rows", "missing",
                                 "missing_rate", "completeness_rate"],
    "fact_drift": ["run_id", "batch_id", "batch_month", "detector", "column", "method", "value",
                   "reference", "threshold", "is_anomaly"],
    "fact_alert": ["run_id", "alert_id", "batch_id", "batch_month", "source", "detector_or_rule", "column",
                   "severity", "value", "threshold", "message", "occurrences", "first_batch",
                   "planted_fault", "created_at"],
    "fact_run": ["run_id", "experiment", "engine", "cores", "input_format", "mode", "fraction", "rows",
                 "repeat", "total_s", "load_s", "rules_s", "detectors_s", "write_s", "rows_per_s",
                 "peak_mem_mb", "alerts", "status", "started_at"],
}

_lock = threading.Lock()


def now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def append(table: str, rows: list[dict], base: Path = METRICS) -> None:
    if not rows:
        return
    cols = TABLES[table]
    path = base / f"{table}.csv"
    with _lock:
        base.mkdir(parents=True, exist_ok=True)
        new = not path.exists()
        with path.open("a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
            if new:
                w.writeheader()
            for r in rows:
                w.writerow({k: _fmt(r.get(k)) for k in cols})


def _fmt(v):
    if isinstance(v, float):
        return f"{v:.6g}"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (list, tuple)):
        return ";".join(map(str, v))
    return "" if v is None else v


def reset_run(run_id: str, base: Path = METRICS) -> None:
    """Supprime les lignes d'une exécution précédente portant le même run_id (relance propre)."""
    for table in TABLES:
        path = base / f"{table}.csv"
        if not path.exists():
            continue
        with path.open(encoding="utf-8", newline="") as f:
            rows = list(csv.DictReader(f))
        keep = [r for r in rows if r.get("run_id") != run_id]
        if len(keep) != len(rows):
            with path.open("w", encoding="utf-8", newline="") as f:
                w = csv.DictWriter(f, fieldnames=TABLES[table])
                w.writeheader()
                w.writerows(keep)


class AlertLog:
    """Journal d'alertes : lignes JSON dans logs/alerts.log + affichage console coloré."""

    COLORS = {"CRITICAL": "\033[91m", "WARNING": "\033[93m", "INFO": "\033[96m"}

    def __init__(self, console: bool = True, path: Path = LOGS / "alerts.log"):
        self.console = console
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)

    def emit(self, alert: dict) -> None:
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(alert, ensure_ascii=False, default=str) + "\n")
        if self.console:
            c = self.COLORS.get(alert["severity"], "")
            print(f"{c}[{alert['severity']}] {alert['batch_month']} {alert['detector_or_rule']}"
                  f"{' / ' + alert['column'] if alert.get('column') else ''}: {alert['message']}\033[0m",
                  flush=True)
