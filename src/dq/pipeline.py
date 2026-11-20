"""Traitement d'un micro-batch : schéma -> règles -> score -> détecteurs -> alertes -> tables de résultats.

Utilisé par le flux Spark Structured Streaming (foreachBatch) et par le mode batch.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path

from . import detectors as D
from . import metrics as M
from .rules import Rule, load_rules, rules_table
from .schema import COLUMNS, EXPECTED_TYPES
from .scoring import load_scoring, score_batch
from .settings import BATCHES, DATA

RULE_SEVERITY_TO_ALERT = {"high": "CRITICAL", "medium": "WARNING", "low": "INFO"}
DRIFT_SEVERITY = {"volume": "WARNING", "numeric_drift": "WARNING", "categorical_drift": "WARNING",
                  "outliers": "WARNING", "missing_drift": "WARNING"}


@dataclass
class RunContext:
    run_id: str
    engine: str = "spark"
    write_metrics: bool = True
    console: bool = True
    rules: list[Rule] = field(default_factory=load_rules)
    scoring: dict = field(default_factory=load_scoring)
    detectors: dict = field(default_factory=D.load_detectors)
    manifest: dict = field(default_factory=dict)
    state: D.DetectorState = field(default_factory=D.DetectorState)
    batch_seq: int = 0
    open_alerts: dict = field(default_factory=dict)     # clé -> alerte en cours (regroupement)
    alert_count: int = 0
    timings: dict = field(default_factory=lambda: {"load_s": 0.0, "rules_s": 0.0, "detectors_s": 0.0,
                                                   "write_s": 0.0})
    rows_total: int = 0

    def __post_init__(self):
        self.alert_log = M.AlertLog(console=self.console)
        self.state_path = DATA / "state" / f"{self.run_id}.json"

    @classmethod
    def create(cls, run_id: str, with_manifest: bool = False, **kw) -> "RunContext":
        ctx = cls(run_id=run_id, **kw)
        if with_manifest:
            mf = BATCHES / "fault_manifest.json"
            if mf.exists():
                ctx.manifest = {f["batch_month"]: f["fault_type"] for f in json.loads(mf.read_text("utf-8"))}
        if ctx.write_metrics:
            M.reset_run(run_id)
            _write_dim_rule(ctx)
        ctx.state.save(ctx.state_path)  # repart d'un état vide pour ce run
        return ctx


def _write_dim_rule(ctx: RunContext) -> None:
    path = M.METRICS / "dim_rule.csv"
    M.METRICS.mkdir(parents=True, exist_ok=True)
    rules_table(ctx.rules).to_csv(path, index=False)


def schema_from_names(fields: list[tuple[str, str]]) -> dict:
    received = {n: t for n, t in fields if n not in ("batch_month",)}
    missing = [c for c in COLUMNS if c not in received]
    extra = [c for c in received if c not in COLUMNS]
    type_changes = [f"{c}:{t}" for c, t in received.items()
                    if c in EXPECTED_TYPES and t not in ("string", "large_string")]
    return {"missing_columns": missing, "extra_columns": extra, "type_changes": type_changes,
            "changed": bool(missing or extra or type_changes)}


def process_spark_batch(ctx: RunContext, raw_df, batch_month: str, file_fields: list[tuple[str, str]]) -> dict:
    """Traite un lot Spark brut (colonnes texte). `file_fields` = schéma réel du fichier reçu."""
    from .rules import evaluate_spark
    from .schema import typed_view

    t0 = time.perf_counter()
    schema = schema_from_names(file_fields)
    df = typed_view(raw_df).cache()
    n = df.count()
    t1 = time.perf_counter()
    n, results, completeness = evaluate_spark(df, ctx.rules)
    t2 = time.perf_counter()
    fences = D.fences_for(ctx.state, ctx.detectors)
    profile = D.profile_batch_spark(df, ctx.detectors, fences)
    df.unpersist()
    out = finish_batch(ctx, batch_month, n, schema, results, completeness, profile, t0, t1, t2)
    return out


def finish_batch(ctx: RunContext, batch_month: str, n: int, schema: dict, results: list[dict],
                 completeness: list[dict], profile: dict, t0: float, t1: float, t2: float) -> dict:
    """Partie commune aux moteurs : score, détection, alertes, écriture des résultats."""
    score = score_batch(results, schema["changed"], ctx.scoring)
    drift = D.detect(ctx.state, ctx.detectors, profile, completeness, batch_month)
    D.remember(ctx.state, profile, completeness, batch_month)
    t3 = time.perf_counter()

    ctx.batch_seq += 1
    bid = ctx.batch_seq
    fault = ctx.manifest.get(batch_month, "")
    alerts = _alerts(ctx, bid, batch_month, schema, results, drift, fault)

    if ctx.write_metrics:
        base = {"run_id": ctx.run_id, "batch_id": bid, "batch_month": batch_month}
        M.append("dim_batch", [{**base, "rows": n, "processed_at": M.now(), "duration_s": t3 - t0,
                                **score, "schema_changed": schema["changed"],
                                "missing_columns": schema["missing_columns"],
                                "extra_columns": schema["extra_columns"], "planted_faults": fault}])
        M.append("fact_rule_result", [{**base, **r} for r in results])
        M.append("fact_column_completeness", [{**base, **c} for c in completeness])
        M.append("fact_drift", [{**base, **d} for d in drift])
        M.append("fact_alert", alerts)
        ctx.state.save(ctx.state_path)
    t4 = time.perf_counter()

    ctx.timings["load_s"] += t1 - t0
    ctx.timings["rules_s"] += t2 - t1
    ctx.timings["detectors_s"] += t3 - t2
    ctx.timings["write_s"] += t4 - t3
    ctx.rows_total += n
    ctx.alert_count += len(alerts)
    return {"batch_month": batch_month, "rows": n, "score": score["global_score"], "status": score["status"],
            "alerts": len(alerts), "seconds": t4 - t0}


def _alerts(ctx, bid, month, schema, results, drift, fault) -> list[dict]:
    candidates = []
    if schema["changed"]:
        msg = []
        if schema["missing_columns"]:
            msg.append("colonnes manquantes : " + ", ".join(schema["missing_columns"]))
        if schema["extra_columns"]:
            msg.append("colonnes inattendues : " + ", ".join(schema["extra_columns"]))
        if schema["type_changes"]:
            msg.append("types modifiés : " + ", ".join(schema["type_changes"]))
        candidates.append(("schema", "SCHEMA", "", "CRITICAL", 1.0, 0.0, " ; ".join(msg)))
    for r in results:
        if r["mode"] == "monitor" or r["status"] != "fail":
            continue
        rule = next(x for x in ctx.rules if x.id == r["rule_id"])
        sev = "CRITICAL" if rule.critical else RULE_SEVERITY_TO_ALERT[rule.severity]
        candidates.append(("rule", r["rule_id"], ",".join(rule.columns), sev, r["failure_rate"],
                           rule.max_fail_rate,
                           f"{r['rows_failed']} lignes en échec ({r['failure_rate'] * 100:.2f} %) – {rule.description}"))
    for d in drift:
        if not d["is_anomaly"]:
            continue
        sev = DRIFT_SEVERITY[d["detector"]]
        if d["detector"] == "volume" and abs(d["value"]) > 2 * d["threshold"]:
            sev = "CRITICAL"
        candidates.append(("detector", d["detector"], d["column"], sev, d["value"], d["threshold"],
                           f"{d['method']} = {d['value']:.3f} (seuil {d['threshold']:.3g})"))

    rows, seen = [], set()
    for source, name, column, sev, value, thr, msg in candidates:
        key = (source, name, column)
        seen.add(key)
        prev = ctx.open_alerts.get(key)
        if prev and prev["last_batch"] == bid - 1:
            prev.update(last_batch=bid, occurrences=prev["occurrences"] + 1)
        else:
            prev = {"alert_id": f"{ctx.run_id}-{bid:03d}-{len(rows) + 1:02d}", "first_batch": month,
                    "last_batch": bid, "occurrences": 1}
            ctx.open_alerts[key] = prev
        row = {"run_id": ctx.run_id, "alert_id": prev["alert_id"], "batch_id": bid, "batch_month": month,
               "source": source, "detector_or_rule": name, "column": column, "severity": sev, "value": value,
               "threshold": thr, "message": msg, "occurrences": prev["occurrences"],
               "first_batch": prev["first_batch"], "planted_fault": fault, "created_at": M.now()}
        rows.append(row)
        if prev["occurrences"] == 1 and ctx.write_metrics:
            ctx.alert_log.emit(row)
    return rows
