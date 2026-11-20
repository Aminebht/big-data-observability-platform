"""Score global de qualité (pondéré) – voir config/scoring.yaml."""
from __future__ import annotations

from pathlib import Path

import yaml

from .rules import DIMENSIONS
from .settings import CONFIG


def load_scoring(path: Path | None = None) -> dict:
    return yaml.safe_load(Path(path or CONFIG / "scoring.yaml").read_text(encoding="utf-8"))


def score_batch(results: list[dict], schema_changed: bool, cfg: dict | None = None) -> dict:
    """Calcule les scores par dimension et le score global (0–100) + le statut du lot."""
    cfg = cfg or load_scoring()
    sw, dw = cfg["severity_weights"], cfg["dimension_weights"]
    dims = {}
    for d in DIMENSIONS:
        rs = [r for r in results if r["dimension"] == d and r["mode"] != "monitor"]
        wsum = sum(sw[r["severity"]] for r in rs)
        dims[d] = (sum(sw[r["severity"]] * r["conformity_rate"] * 100 for r in rs) / wsum) if wsum else 100.0
    total_w = sum(dw[d] for d in DIMENSIONS)
    global_score = sum(dw[d] * dims[d] for d in DIMENSIONS) / total_w

    bands = cfg["bands"]
    status = "healthy" if global_score >= bands["healthy"] else "warning" if global_score >= bands["warning"] else "critical"
    reasons = []
    force = cfg.get("force_critical_on", {})
    if force.get("schema_change") and schema_changed:
        reasons.append("schema_change")
    if force.get("critical_rule_failure"):
        reasons += [f"critical_rule:{r['rule_id']}" for r in results
                    if r.get("critical") and r["status"] == "fail"]
    if reasons:
        status = "critical"
    elif status == "healthy":
        sev = set(cfg.get("force_warning_on", {}).get("rule_failure_severities", []))
        failed = [r["rule_id"] for r in results
                  if r["severity"] in sev and r["mode"] != "monitor" and r["status"] == "fail"]
        if failed:
            status = "warning"
            reasons = [f"rule:{f}" for f in failed]
    return {"global_score": global_score, "status": status, "forced_reasons": ";".join(reasons),
            **{f"score_{d}": v for d, v in dims.items()}}
