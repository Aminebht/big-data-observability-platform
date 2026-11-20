"""Moteur mono-machine (Polars) : même catalogue YAML, mêmes résultats que le moteur Spark.

Sert à la comparaison de performances (expérience E1) et au test de parité Spark / Polars.
Les conditions SQL des règles `expression` sont exécutées telles quelles via `pl.sql_expr`.
"""
from __future__ import annotations

from functools import reduce

import polars as pl

from .rules import Rule, _result
from .schema import COLUMNS, EXPECTED_TYPES, TIMESTAMP_COLS

_PL_TYPES = {"int": pl.Int32, "double": pl.Float64, "string": pl.Utf8, "boolean": pl.Boolean,
             "timestamp": pl.Datetime("us")}


def typed_view(lf: pl.LazyFrame) -> pl.LazyFrame:
    names = lf.collect_schema().names()
    exprs = []
    for c in COLUMNS:
        t = EXPECTED_TYPES[c]
        if c not in names:
            exprs.append(pl.lit(None, dtype=_PL_TYPES[t]).alias(c))
            continue
        raw = pl.col(c).cast(pl.Utf8)
        if t == "timestamp":
            e = raw.str.slice(0, 19).str.strptime(pl.Datetime("us"), "%Y-%m-%d %H:%M:%S", strict=False)
        elif t == "boolean":
            low = raw.str.strip_chars().str.to_lowercase()
            e = pl.when(low == "true").then(True).when(low == "false").then(False).otherwise(None)
        elif t in ("int", "double"):
            e = raw.cast(_PL_TYPES[t], strict=False)
        else:
            e = pl.when(raw.str.strip_chars() == "").then(None).otherwise(raw)
        exprs.append(e.alias(c))
    exprs += [pl.col(c).cast(pl.Utf8).alias(f"{c}__raw") for c in names if c in COLUMNS]
    out = lf.select(exprs)
    return out.with_columns([pl.col(c).dt.epoch("s").cast(pl.Float64).alias(f"{c}_s") for c in TIMESTAMP_COLS])


# vrai pour chaque ligne (un pl.lit(True) agrégé ne compterait qu'une fois)
_ALL = pl.int_range(pl.len()) >= 0


def _raw_missing(names, c):
    if f"{c}__raw" in names:
        raw = pl.col(f"{c}__raw")
        return raw.is_null() | (raw.str.strip_chars() == "")
    return pl.col(c).is_null()


def _any(conds):
    return reduce(lambda a, b: a | b, conds)


def _compile(rule: Rule, names):
    t, cols = rule.type, rule.columns
    if t == "not_null":
        return _ALL, _any([_raw_missing(names, c) for c in cols])
    if t == "in_range":
        c = pl.col(cols[0])
        return c.is_not_null(), (c.is_not_null() & ~c.is_between(rule.min, rule.max)).fill_null(False)
    if t == "allowed_values":
        present = [pl.col(c).is_not_null() for c in cols]
        bad = [(pl.col(c).is_not_null() & ~pl.col(c).is_in(rule.values)).fill_null(False) for c in cols]
        return _any(present), _any(bad)
    if t == "regex":
        present, bad = [], []
        for c in cols:
            raw = pl.col(f"{c}__raw") if f"{c}__raw" in names else pl.col(c).cast(pl.Utf8)
            ok = (raw.is_not_null() & (raw.str.strip_chars() != "")).fill_null(False)
            present.append(ok)
            bad.append((ok & ~raw.str.contains(rule.pattern)).fill_null(False))
        return _any(present), _any(bad)
    if t == "expression":
        applies = pl.sql_expr(rule.applies_when).fill_null(False) if rule.applies_when else _ALL
        return applies, applies & ~pl.sql_expr(rule.condition).fill_null(True)
    raise ValueError(t)


def evaluate(df: pl.DataFrame, rules: list[Rule]):
    names = df.columns
    aggs = [pl.len().alias("__n")]
    row_rules = [r for r in rules if r.type != "unique"]
    for r in row_rules:
        applies, fails = _compile(r, names)
        aggs.append(applies.cast(pl.Int64).sum().alias(f"chk__{r.id}"))
        aggs.append(fails.cast(pl.Int64).sum().alias(f"err__{r.id}"))
    present = [c for c in COLUMNS if f"{c}__raw" in names]
    for c in present:
        aggs.append(_raw_missing(names, c).cast(pl.Int64).sum().alias(f"miss__{c}"))
    row = df.select(aggs).row(0, named=True)
    n = row["__n"]
    counts = {r.id: (row[f"chk__{r.id}"], row[f"err__{r.id}"]) for r in row_rules}
    for r in rules:
        if r.type == "unique":
            g = (df.filter(_any([pl.col(c).is_not_null() for c in r.columns]))
                 .group_by(r.columns).len())
            counts[r.id] = (int(g["len"].sum()), int((g["len"] - 1).clip(lower_bound=0).sum()))
    results = [_result(r, *counts[r.id]) for r in rules]
    completeness = [{"column": c, "rows": n, "missing": row[f"miss__{c}"],
                     "missing_rate": row[f"miss__{c}"] / n if n else 0.0,
                     "completeness_rate": 1 - (row[f"miss__{c}"] / n if n else 0.0)} for c in present]
    completeness += [{"column": c, "rows": n, "missing": n, "missing_rate": 1.0, "completeness_rate": 0.0}
                     for c in COLUMNS if c not in present]
    return n, results, completeness


def profile_batch(df: pl.DataFrame, cfg: dict, fences: dict) -> dict:
    edges = cfg["numeric_drift"]["edges"]
    aggs = [pl.len().alias("__n")]
    for c in cfg["numeric_drift"]["columns"]:
        e = edges.get(c, [])
        for i in range(len(e) + 1):
            cond = pl.col(c).is_not_null()
            if i > 0:
                cond = cond & (pl.col(c) > e[i - 1])
            if i < len(e):
                cond = cond & (pl.col(c) <= e[i])
            aggs.append(cond.fill_null(False).cast(pl.Int64).sum().alias(f"h__{c}__{i}"))
    for c, (lo, hi) in fences.items():
        aggs.append(((pl.col(c) < lo) | (pl.col(c) > hi)).fill_null(False).cast(pl.Int64).sum().alias(f"o__{c}"))
    for c in cfg["outliers"]["columns"]:
        aggs.append(pl.col(c).is_not_null().cast(pl.Int64).sum().alias(f"nn__{c}"))
        aggs.append(pl.col(c).quantile(0.25).alias(f"q1__{c}"))
        aggs.append(pl.col(c).quantile(0.75).alias(f"q3__{c}"))
    row = df.select(aggs).row(0, named=True)
    hist = {c: [row[f"h__{c}__{i}"] for i in range(len(edges.get(c, [])) + 1)]
            for c in cfg["numeric_drift"]["columns"]}
    quart = {c: [row[f"q1__{c}"], row[f"q3__{c}"]] for c in cfg["outliers"]["columns"]
             if row[f"q1__{c}"] is not None}
    cat = {}
    for c in cfg["categorical_drift"]["columns"]:
        vc = df.group_by(pl.col(c).cast(pl.Utf8)).len()
        cat[c] = {("(null)" if v is None else v): n for v, n in vc.iter_rows()}
    return {"rows": row["__n"], "hist": hist, "outliers": {c: row[f"o__{c}"] for c in fences},
            "nonnull": {c: row[f"nn__{c}"] for c in cfg["outliers"]["columns"]}, "quartiles": quart, "cat": cat}
