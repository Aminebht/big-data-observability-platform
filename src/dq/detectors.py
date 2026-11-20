"""Détection d'anomalies et de dérive (Partie 3).

Principe : chaque lot est résumé (« profil de lot ») par le moteur de calcul (Spark ou Pandas),
puis comparé à une fenêtre de référence glissante formée des profils des lots précédents.
La logique de détection ci-dessous est en Python pur : elle est commune aux deux moteurs.
"""
from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import yaml

from .settings import CONFIG

EPS = 1e-6


def load_detectors(path: Path | None = None) -> dict:
    cfg = yaml.safe_load(Path(path or CONFIG / "detectors.yaml").read_text(encoding="utf-8"))
    bins_path = CONFIG / "drift_bins.json"
    cfg["numeric_drift"]["edges"] = json.loads(bins_path.read_text()) if bins_path.exists() else {}
    return cfg


# ------------------------------------------------------------------ profil de lot (Spark)
def profile_batch_spark(df, cfg: dict, fences: dict[str, tuple[float, float]]) -> dict:
    """Résumé d'un lot typé : volume, histogrammes (PSI), catégories (JS), quartiles, aberrants."""
    from pyspark.sql import functions as F

    edges = cfg["numeric_drift"]["edges"]
    aggs = [F.count(F.lit(1)).alias("__n")]
    for c in cfg["numeric_drift"]["columns"]:
        e = edges.get(c, [])
        col = F.col(c)
        for i in range(len(e) + 1):
            lo = e[i - 1] if i > 0 else None
            hi = e[i] if i < len(e) else None
            cond = col.isNotNull()
            if lo is not None:
                cond = cond & (col > lo)
            if hi is not None:
                cond = cond & (col <= hi)
            aggs.append(F.sum(cond.cast("int")).alias(f"h__{c}__{i}"))
    for c, (lo, hi) in fences.items():
        aggs.append(F.sum(((F.col(c) < lo) | (F.col(c) > hi)).cast("int")).alias(f"o__{c}"))
    for c in cfg["outliers"]["columns"]:
        aggs.append(F.sum(F.col(c).isNotNull().cast("int")).alias(f"nn__{c}"))
    row = df.agg(*aggs).collect()[0].asDict()

    n = row["__n"] or 0
    hist = {c: [row[f"h__{c}__{i}"] or 0 for i in range(len(edges.get(c, [])) + 1)]
            for c in cfg["numeric_drift"]["columns"]}
    outl = {c: (row.get(f"o__{c}") or 0) for c in fences}
    nonnull = {c: row[f"nn__{c}"] or 0 for c in cfg["outliers"]["columns"]}

    ocols = cfg["outliers"]["columns"]
    quart = {}
    if n:
        qs = df.approxQuantile(ocols, [0.25, 0.75], 0.01)
        quart = {c: q for c, q in zip(ocols, qs) if len(q) == 2}

    cats = cfg["categorical_drift"]["columns"]
    stack = "stack({}, {}) as (col, value)".format(
        len(cats), ", ".join(f"'{c}', cast(`{c}` as string)" for c in cats))
    top = (df.selectExpr(stack).groupBy("col", "value").count().collect())
    cat = {c: {} for c in cats}
    for r in top:
        cat[r["col"]]["(null)" if r["value"] is None else r["value"]] = r["count"]
    return {"rows": n, "hist": hist, "outliers": outl, "nonnull": nonnull, "quartiles": quart, "cat": cat}


# ------------------------------------------------------------------ mesures statistiques
def psi(ref: list[float], cur: list[float]) -> float:
    r = np.asarray(ref, float); c = np.asarray(cur, float)
    if r.sum() == 0 or c.sum() == 0:
        return 0.0
    r = np.clip(r / r.sum(), EPS, None); c = np.clip(c / c.sum(), EPS, None)
    return float(np.sum((c - r) * np.log(c / r)))


def jensen_shannon(ref: dict, cur: dict, top_k: int) -> float:
    top = [k for k, _ in sorted(ref.items(), key=lambda kv: -kv[1])[:top_k]]
    def vec(d):
        v = [d.get(k, 0) for k in top]
        v.append(sum(val for k, val in d.items() if k not in top))
        return np.asarray(v, float)
    p, q = vec(ref), vec(cur)
    if p.sum() == 0 or q.sum() == 0:
        return 0.0
    p, q = p / p.sum(), q / q.sum()
    m = (p + q) / 2
    def kl(a, b):
        mask = a > 0
        return float(np.sum(a[mask] * np.log2(a[mask] / b[mask])))
    return math.sqrt(max(0.0, (kl(p, m) + kl(q, m)) / 2))


def robust_z(values: list[float], x: float) -> float:
    v = np.asarray(values, float)
    med = np.median(v)
    mad = np.median(np.abs(v - med))
    if mad == 0:
        mad = np.mean(np.abs(v - med)) or 1e-9
    return float(0.6745 * (x - med) / mad)


# ------------------------------------------------------------------ état + détection
@dataclass
class DetectorState:
    history: list[dict] = field(default_factory=list)   # profils + métriques des lots précédents

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.history), encoding="utf-8")

    @classmethod
    def load(cls, path: Path) -> "DetectorState":
        return cls(json.loads(path.read_text())) if path.exists() else cls()


def reference(state: DetectorState, cfg: dict) -> list[dict]:
    return state.history[-cfg["reference"]["window"]:]


def _month_index(month: str) -> int:
    y, m = month.split("-")
    return int(y) * 12 + int(m) - 1


def seasonal_reference(state: DetectorState, cfg: dict, month: str | None) -> list[dict]:
    """Même saison l'année précédente : mois M-11, M-12 et M-13.

    Les distributions (température, jour/nuit, météo...) sont saisonnières : comparer juillet à la
    moyenne des 12 derniers mois signalerait une dérive chaque été. Repli sur la fenêtre glissante
    si l'historique ne couvre pas encore l'année précédente.
    """
    if month and cfg["reference"].get("seasonal", True):
        target = _month_index(month)
        ref = [h for h in state.history if h.get("month") and 11 <= target - _month_index(h["month"]) <= 13]
        if ref:
            return ref
    return reference(state, cfg)


def fences_for(state: DetectorState, cfg: dict) -> dict[str, tuple[float, float]]:
    """Bornes IQR à partir des quartiles médians de la fenêtre de référence."""
    ref = reference(state, cfg)
    k = cfg["outliers"]["k"]
    out = {}
    for c in cfg["outliers"]["columns"]:
        qs = [h["profile"]["quartiles"][c] for h in ref if c in h["profile"]["quartiles"]]
        if not qs:
            continue
        q1 = float(np.median([q[0] for q in qs])); q3 = float(np.median([q[1] for q in qs]))
        iqr = q3 - q1
        if iqr <= 0:
            continue
        out[c] = (q1 - k * iqr, q3 + k * iqr)
    return out


def detect(state: DetectorState, cfg: dict, profile: dict, completeness: list[dict],
           month: str | None = None) -> list[dict]:
    """Compare le lot courant à la référence. Retourne des lignes fact_drift."""
    ref = reference(state, cfg)
    sref = seasonal_reference(state, cfg, month)
    warm = len(state.history) >= cfg["reference"]["warmup"]
    rows = []

    def add(detector, column, method, value, refv, threshold, anomaly):
        rows.append({"detector": detector, "column": column, "method": method, "value": value,
                     "reference": refv, "threshold": threshold, "is_anomaly": bool(anomaly and warm)})

    if not ref:
        return rows

    # 1. volume
    vc = cfg["volume"]
    logs = [math.log1p(h["profile"]["rows"]) for h in ref]
    z = robust_z(logs, math.log1p(profile["rows"]))
    add("volume", "", "robust_z_log", z, float(np.median([h["profile"]["rows"] for h in ref])),
        vc["alert"], abs(z) > vc["alert"])

    # Dérive de distribution = écart à la fois à la même saison l'an passé ET aux derniers mois.
    # -> un changement brusque est signalé ; un effet saisonnier (même saison) ou un changement
    #    durable déjà installé (derniers mois) ne l'est plus après son apparition.
    recent = state.history[-cfg["reference"].get("recent", 3):]
    refs = [sref, recent] if sref is not recent else [sref]

    # 2. dérive numérique (PSI)
    nd = cfg["numeric_drift"]
    for c in nd["columns"]:
        if c not in profile["hist"]:
            continue
        vals = []
        for r in refs:
            hs = [h["profile"]["hist"][c] for h in r if c in h["profile"]["hist"]]
            if hs:
                vals.append(psi(list(np.sum(hs, axis=0)), profile["hist"][c]))
        if not vals:
            continue
        v = min(vals)
        thr = nd.get("alert_by_column", {}).get(c, nd["alert"])
        add("numeric_drift", c, "psi", v, nd["warning"], thr, v > thr)

    # 3. dérive catégorielle (Jensen–Shannon)
    cd = cfg["categorical_drift"]
    for c in cd["columns"]:
        vals = []
        for r in refs:
            agg: dict = {}
            for h in r:
                for k, val in h["profile"]["cat"].get(c, {}).items():
                    agg[k] = agg.get(k, 0) + val
            vals.append(jensen_shannon(agg, profile["cat"].get(c, {}), cd["top_k"]))
        v = min(vals)
        thr = cd.get("alert_by_column", {}).get(c, cd["alert"])
        add("categorical_drift", c, "jensen_shannon", v, None, thr, v > thr)

    # 4. valeurs aberrantes (IQR)
    oc = cfg["outliers"]
    for c, cnt in profile["outliers"].items():
        nn = profile["nonnull"].get(c, 0)
        rate = cnt / nn if nn else 0.0
        ref_rates = [h["outlier_rates"].get(c) for h in ref if h.get("outlier_rates", {}).get(c) is not None]
        ref_rate = float(np.mean(ref_rates)) if ref_rates else None
        thr = max(oc["alert_ratio"] * (ref_rate or 0), oc["min_rate"])
        add("outliers", c, "iqr", rate, ref_rate, thr, ref_rate is not None and rate > thr)

    # 5. dérive du taux de valeurs manquantes
    md = cfg["missing_drift"]
    for comp in completeness:
        c = comp["column"]
        ref_rates = [h["missing"].get(c) for h in ref if c in h.get("missing", {})]
        if not ref_rates:
            continue
        rr = float(np.mean(ref_rates))
        thr = md.get("alert_by_column", {}).get(c, md["alert_points"])
        add("missing_drift", c, "delta_points", comp["missing_rate"] - rr, rr, thr, comp["missing_rate"] - rr > thr)

    return rows


def remember(state: DetectorState, profile: dict, completeness: list[dict],
             month: str | None = None) -> None:
    rates = {c: (cnt / profile["nonnull"][c]) if profile["nonnull"].get(c) else 0.0
             for c, cnt in profile["outliers"].items()}
    state.history.append({"month": month, "profile": profile, "missing": {c["column"]: c["missing_rate"] for c in completeness},
                          "outlier_rates": rates})
