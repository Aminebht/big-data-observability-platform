"""Moteur de règles de qualité piloté par `config/rules.yaml` (Spark).

Toutes les règles « ligne à ligne » sont évaluées en UNE seule agrégation par lot :
pour chaque règle on compte les lignes contrôlées et les lignes en échec.
Les règles d'unicité demandent un regroupement (groupBy) chacune.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml

from .schema import COLUMNS, EXPECTED_TYPES
from .settings import CONFIG

DIMENSIONS = ["completeness", "uniqueness", "validity", "consistency", "conformity"]


@dataclass
class Rule:
    id: str
    dimension: str
    type: str
    severity: str = "medium"
    description: str = ""
    columns: list[str] = field(default_factory=list)
    max_fail_rate: float = 0.0
    mode: str = "score"          # score | monitor
    critical: bool = False
    min: float | None = None
    max: float | None = None
    values: list | None = None
    pattern: str | None = None
    condition: str | None = None
    applies_when: str | None = None

    @property
    def scored(self) -> bool:
        return self.mode != "monitor"


def load_rules(path: Path | None = None) -> list[Rule]:
    raw = yaml.safe_load(Path(path or CONFIG / "rules.yaml").read_text(encoding="utf-8"))["rules"]
    rules = []
    for r in raw:
        r = dict(r)
        if "column" in r:
            r["columns"] = [r.pop("column")]
        rule = Rule(**r)
        if rule.dimension not in DIMENSIONS:
            raise ValueError(f"{rule.id}: dimension inconnue {rule.dimension}")
        rules.append(rule)
    ids = [r.id for r in rules]
    if len(ids) != len(set(ids)):
        raise ValueError("identifiants de règles en double")
    return rules


def rules_table(rules: list[Rule]):
    """Table dim_rule (pandas) générée à partir du YAML."""
    import pandas as pd

    return pd.DataFrame([{
        "rule_id": r.id, "dimension": r.dimension, "type": r.type, "columns": ", ".join(r.columns),
        "severity": r.severity, "critical": r.critical, "mode": r.mode,
        "max_fail_rate": r.max_fail_rate, "description": r.description,
    } for r in rules])


# ---------------------------------------------------------------- Spark
def _raw_missing(df, c):
    from pyspark.sql import functions as F

    if f"{c}__raw" in df.columns:
        raw = F.col(f"{c}__raw")
        return raw.isNull() | (F.trim(raw) == "")
    return F.col(c).isNull()


def _any(conds):
    from functools import reduce

    return reduce(lambda a, b: a | b, conds)


def _compile(rule: Rule, df):
    """Retourne (applies, fails) : expressions booléennes Spark (jamais NULL)."""
    from pyspark.sql import functions as F

    t, cols = rule.type, rule.columns
    if t == "not_null":
        return F.lit(True), _any([_raw_missing(df, c) for c in cols])
    if t == "in_range":
        c = F.col(cols[0])
        return c.isNotNull(), c.isNotNull() & ~c.between(rule.min, rule.max)
    if t == "allowed_values":
        present = [F.col(c).isNotNull() for c in cols]
        bad = [F.col(c).isNotNull() & ~F.col(c).isin(rule.values) for c in cols]
        return _any(present), _any(bad)
    if t == "regex":
        present, bad = [], []
        for c in cols:
            raw = F.col(f"{c}__raw") if f"{c}__raw" in df.columns else F.col(c).cast("string")
            ok = raw.isNotNull() & (F.trim(raw) != "")
            present.append(ok)
            bad.append(ok & ~raw.rlike(rule.pattern))
        return _any(present), _any(bad)
    if t == "expression":
        applies = F.coalesce(F.expr(rule.applies_when), F.lit(False)) if rule.applies_when else F.lit(True)
        return applies, applies & ~F.coalesce(F.expr(rule.condition), F.lit(True))
    raise ValueError(f"{rule.id}: type {t} non géré par le moteur ligne à ligne")


def evaluate_spark(df, rules: list[Rule]) -> tuple[int, list[dict], list[dict]]:
    """Évalue toutes les règles sur un DataFrame typé (sortie de `typed_view`).

    Retourne (nb_lignes, résultats_par_règle, complétude_par_colonne).
    """
    from pyspark.sql import functions as F

    aggs = [F.count(F.lit(1)).alias("__n")]
    row_rules = [r for r in rules if r.type != "unique"]
    for r in row_rules:
        applies, fails = _compile(r, df)
        aggs.append(F.sum(applies.cast("int")).alias(f"chk__{r.id}"))
        aggs.append(F.sum(fails.cast("int")).alias(f"err__{r.id}"))
    present_cols = [c for c in COLUMNS if c in df.columns]
    for c in present_cols:
        aggs.append(F.sum(_raw_missing(df, c).cast("int")).alias(f"miss__{c}"))
    row = df.agg(*aggs).collect()[0].asDict()
    n = row["__n"] or 0

    counts = {r.id: (row[f"chk__{r.id}"] or 0, row[f"err__{r.id}"] or 0) for r in row_rules}
    for r in rules:
        if r.type == "unique":
            key = [F.col(c) for c in r.columns]
            not_all_null = _any([F.col(c).isNotNull() for c in r.columns])
            g = (df.where(not_all_null).groupBy(*key).count()
                 .agg(F.sum("count").alias("chk"),
                      F.sum(F.when(F.col("count") > 1, F.col("count") - 1).otherwise(0)).alias("err"))
                 .collect()[0])
            counts[r.id] = (g["chk"] or 0, g["err"] or 0)

    results = [_result(r, *counts[r.id]) for r in rules]
    completeness = [{"column": c, "rows": n, "missing": row[f"miss__{c}"] or 0,
                     "missing_rate": (row[f"miss__{c}"] or 0) / n if n else 0.0,
                     "completeness_rate": 1 - ((row[f"miss__{c}"] or 0) / n if n else 0.0)}
                    for c in present_cols]
    completeness += [{"column": c, "rows": n, "missing": n, "missing_rate": 1.0, "completeness_rate": 0.0}
                     for c in COLUMNS if c not in df.columns]
    return n, results, completeness


def _result(rule: Rule, checked: int, failed: int) -> dict:
    rate = failed / checked if checked else 0.0
    return {
        "rule_id": rule.id, "dimension": rule.dimension, "severity": rule.severity,
        "mode": rule.mode, "critical": rule.critical,
        "rows_checked": int(checked), "rows_failed": int(failed),
        "failure_rate": rate, "conformity_rate": 1 - rate,
        "status": "pass" if rate <= rule.max_fail_rate else "fail",
    }


def check_schema(df_raw) -> dict:
    """Compare les colonnes et types bruts reçus avec le schéma attendu (données brutes en texte)."""
    received = {f.name: f.dataType.simpleString() for f in df_raw.schema.fields if f.name != "batch_month"}
    missing = [c for c in COLUMNS if c not in received]
    extra = [c for c in received if c not in COLUMNS]
    type_changes = [f"{c}:{t}" for c, t in received.items() if c in EXPECTED_TYPES and t != "string"]
    return {"missing_columns": missing, "extra_columns": extra, "type_changes": type_changes,
            "changed": bool(missing or extra or type_changes)}
