"""Partie 1 – Data Profiling du dataset complet (Spark).

Sorties :
  data/metrics/profiling/*.csv   tables de profilage (lisibles par Power BI)
  report/figures/profiling_*.png figures pour le rapport
  report/profiling_summary.md    résumé chiffré
"""
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import matplotlib  # noqa: E402

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from dq.schema import (BOOL_COLS, CATEGORICAL_COLS, COLUMNS, EXPECTED_TYPES,  # noqa: E402
                       NUMERIC_COLS, TIMESTAMP_COLS, typed_view)
from dq.settings import BRONZE, METRICS, ROOT, get_spark  # noqa: E402

OUT = METRICS / "profiling"
FIG = ROOT / "report" / "figures"
QUANTILES = [0.01, 0.25, 0.5, 0.75, 0.99]


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    FIG.mkdir(parents=True, exist_ok=True)
    spark = get_spark("profiling")
    t0 = time.time()

    bronze = spark.read.parquet(str(BRONZE))
    df = typed_view(bronze.drop("batch_month"))  # pas de cache : relire le Parquet est plus rapide que de saturer la mémoire
    n_rows = df.count()

    _log('cache + comptage', t0)
    # ---- valeurs manquantes + échecs de typage (une seule agrégation) ----
    aggs = []
    for c in COLUMNS:
        aggs.append(F.sum(F.col(c).isNull().cast("int")).alias(f"null__{c}"))
        aggs.append(F.sum((F.col(f"{c}__raw").isNull() | (F.trim(F.col(f"{c}__raw")) == "")).cast("int"))
                    .alias(f"rawnull__{c}"))
        aggs.append(F.approx_count_distinct(c).alias(f"distinct__{c}"))
    row = df.agg(*aggs).collect()[0].asDict()
    cols = []
    for c in COLUMNS:
        nulls, raw_nulls = row[f"null__{c}"], row[f"rawnull__{c}"]
        cols.append({
            "column": c, "expected_type": EXPECTED_TYPES[c],
            "missing_count": raw_nulls, "missing_rate": raw_nulls / n_rows,
            "cast_failures": nulls - raw_nulls,
            "distinct_approx": row[f"distinct__{c}"],
        })
    columns_df = pd.DataFrame(cols)

    _log('valeurs manquantes', t0)
    # ---- statistiques numériques + valeurs aberrantes (IQR) ----
    stats_aggs = []
    for c in NUMERIC_COLS:
        stats_aggs += [F.min(c).alias(f"min__{c}"), F.max(c).alias(f"max__{c}"),
                       F.mean(c).alias(f"mean__{c}"), F.stddev(c).alias(f"std__{c}")]
    srow = df.agg(*stats_aggs).collect()[0].asDict()
    qs = df.approxQuantile(NUMERIC_COLS, QUANTILES, 0.001)
    num_rows, outlier_aggs = [], []
    for c, q in zip(NUMERIC_COLS, qs):
        p01, q1, med, q3, p99 = q
        iqr = q3 - q1
        lo, hi = q1 - 3 * iqr, q3 + 3 * iqr
        num_rows.append({"column": c, "min": srow[f"min__{c}"], "p01": p01, "q1": q1, "median": med,
                         "q3": q3, "p99": p99, "max": srow[f"max__{c}"], "mean": srow[f"mean__{c}"],
                         "std": srow[f"std__{c}"], "iqr_low": lo, "iqr_high": hi})
        outlier_aggs.append(F.sum(((F.col(c) < lo) | (F.col(c) > hi)).cast("int")).alias(c))
    orow = df.agg(*outlier_aggs).collect()[0].asDict()
    # déciles -> classes fixes pour le calcul du PSI (définies une fois, au moment de la conception)
    deciles = df.approxQuantile(NUMERIC_COLS, [i / 10 for i in range(1, 10)], 0.001)
    drift_bins = {c: sorted(set(round(v, 6) for v in d)) for c, d in zip(NUMERIC_COLS, deciles) if d}
    (ROOT / "config" / "drift_bins.json").write_text(json.dumps(drift_bins, indent=2), encoding="utf-8")
    for r in num_rows:
        r["outliers_iqr3"] = orow[r["column"]] or 0
        r["outlier_rate"] = r["outliers_iqr3"] / n_rows
    numeric_df = pd.DataFrame(num_rows)

    _log('stats numériques + aberrants + déciles', t0)
    # ---- distributions catégorielles (top 15) ----
    cat_rows = []
    for c in CATEGORICAL_COLS + BOOL_COLS:
        top = (df.groupBy(c).count().orderBy(F.desc("count")).limit(15).collect())
        for rank, r in enumerate(top, 1):
            cat_rows.append({"column": c, "rank": rank, "value": "(null)" if r[0] is None else str(r[0]),
                             "count": r["count"], "share": r["count"] / n_rows})
    categorical_df = pd.DataFrame(cat_rows)

    _log('catégories', t0)
    # ---- histogrammes numériques (20 classes entre p01 et p99) ----
    hist_rows = []
    for r in num_rows:
        c, lo, hi = r["column"], r["p01"], r["p99"]
        if lo is None or hi is None or hi <= lo:
            continue
        width = (hi - lo) / 20
        b = F.least(F.greatest(F.floor((F.col(c) - lo) / width), F.lit(0)), F.lit(19)).cast("int")
        for h in df.where(F.col(c).isNotNull()).groupBy(b.alias("bin")).count().collect():
            hist_rows.append({"column": c, "bin": h["bin"], "bin_start": lo + h["bin"] * width,
                              "bin_end": lo + (h["bin"] + 1) * width, "count": h["count"]})
    hist_df = pd.DataFrame(hist_rows).sort_values(["column", "bin"])

    _log('histogrammes', t0)
    # ---- doublons ----
    non_id = [c for c in COLUMNS if c != "ID"]
    dup_id = df.groupBy("ID").count().where("count > 1").agg(
        F.count("*").alias("k"), F.sum("count").alias("rows")).collect()[0]
    content_hash = F.xxhash64(*[F.col(f"{c}__raw") for c in non_id])
    dup_content = df.groupBy(content_hash.alias("h")).count().where("count > 1").agg(
        F.count("*").alias("k"), F.sum(F.col("count") - 1).alias("extra")).collect()[0]
    event_key = ["Start_Time", "Start_Lat", "Start_Lng", "Description"]
    dup_event = df.groupBy(*event_key).count().where("count > 1").agg(
        F.count("*").alias("k"), F.sum(F.col("count") - 1).alias("extra")).collect()[0]
    duplicates = {
        "duplicate_ids_groups": dup_id["k"], "duplicate_ids_rows": dup_id["rows"] or 0,
        "content_duplicates_extra_rows": dup_content["extra"] or 0,
        "event_duplicates_extra_rows": dup_event["extra"] or 0,
    }

    _log('doublons', t0)
    # ---- volume par mois + incohérences temporelles ----
    monthly = (df.groupBy(F.date_format("Start_Time", "yyyy-MM").alias("month")).count()
               .orderBy("month").toPandas())
    time_checks = df.agg(
        F.sum((F.col("End_Time") <= F.col("Start_Time")).cast("int")).alias("end_before_start"),
        F.sum((F.col("Start_Time__raw").rlike(r"\.\d+$")).cast("int")).alias("start_time_fractional_format"),
        F.min("Start_Time").alias("first"), F.max("Start_Time").alias("last"),
    ).collect()[0].asDict()
    elapsed = time.time() - t0

    # ---- écriture des sorties ----
    columns_df.to_csv(OUT / "profile_columns.csv", index=False)
    numeric_df.to_csv(OUT / "profile_numeric.csv", index=False)
    categorical_df.to_csv(OUT / "profile_categorical.csv", index=False)
    hist_df.to_csv(OUT / "profile_histograms.csv", index=False)
    monthly.to_csv(OUT / "profile_monthly_volume.csv", index=False)
    overview = {"rows": n_rows, "columns": len(COLUMNS), "seconds": round(elapsed, 1),
                **duplicates, **{k: str(v) for k, v in time_checks.items()}}
    (OUT / "profile_overview.json").write_text(json.dumps(overview, indent=2), encoding="utf-8")

    _figures(columns_df, hist_df, categorical_df, monthly)
    _summary(overview, columns_df, numeric_df, categorical_df)
    print(json.dumps(overview, indent=2))
    spark.stop()


def _log(step, t0):
    print(f'[{time.time() - t0:7.1f} s] {step}', flush=True)


def _figures(columns_df, hist_df, categorical_df, monthly):
    m = columns_df.sort_values("missing_rate", ascending=True)
    m = m[m.missing_rate > 0]
    fig, ax = plt.subplots(figsize=(8, max(3, 0.3 * len(m))))
    ax.barh(m.column, m.missing_rate * 100, color="#c0504d")
    ax.set_xlabel("% de valeurs manquantes")
    ax.set_title("Taux de valeurs manquantes par attribut")
    fig.tight_layout(); fig.savefig(FIG / "profiling_missing.png", dpi=130); plt.close(fig)

    cols = list(hist_df.column.unique())
    nrow = (len(cols) + 3) // 4
    fig, axes = plt.subplots(nrow, 4, figsize=(16, 3 * nrow))
    for ax, c in zip(axes.flat, cols):
        h = hist_df[hist_df.column == c]
        ax.bar(h.bin_start, h["count"], width=(h.bin_end - h.bin_start), align="edge", color="#4f81bd")
        ax.set_title(c, fontsize=9)
    for ax in list(axes.flat)[len(cols):]:
        ax.axis("off")
    fig.suptitle("Distribution des variables numériques (p1–p99)")
    fig.tight_layout(); fig.savefig(FIG / "profiling_numeric_hist.png", dpi=110); plt.close(fig)

    show = ["Source", "State", "Weather_Condition", "Sunrise_Sunset", "Timezone", "Wind_Direction"]
    show = [c for c in show if c in set(categorical_df.column)]
    fig, axes = plt.subplots(2, 3, figsize=(16, 8))
    for ax, c in zip(axes.flat, show):
        t = categorical_df[categorical_df.column == c].head(10).iloc[::-1]
        ax.barh(t.value, t["count"], color="#9bbb59")
        ax.set_title(c, fontsize=10)
    for ax in list(axes.flat)[len(show):]:
        ax.axis("off")
    fig.suptitle("Distribution des variables catégorielles (top 10)")
    fig.tight_layout(); fig.savefig(FIG / "profiling_categorical.png", dpi=110); plt.close(fig)

    fig, ax = plt.subplots(figsize=(12, 4))
    mm = monthly.dropna()
    ax.plot(pd.to_datetime(mm.month), mm["count"], marker=".", color="#4f81bd")
    ax.set_title("Nombre d'accidents par mois"); ax.set_ylabel("enregistrements")
    fig.tight_layout(); fig.savefig(FIG / "profiling_monthly_volume.png", dpi=130); plt.close(fig)


def _summary(overview, columns_df, numeric_df, categorical_df):
    lines = ["# Résumé du profilage (Partie 1)", "",
             f"- Enregistrements : **{overview['rows']:,}**".replace(",", " "),
             f"- Attributs : **{overview['columns']}**",
             f"- Période : {overview['first']} → {overview['last']}",
             f"- ID en double : {overview['duplicate_ids_rows']} lignes",
             f"- Doublons de contenu (toutes colonnes sauf ID) : {overview['content_duplicates_extra_rows']} lignes en trop",
             f"- Doublons d'événement (Start_Time, coordonnées, Description) : {overview['event_duplicates_extra_rows']} lignes en trop",
             f"- End_Time ≤ Start_Time : {overview['end_before_start']}",
             f"- Start_Time avec fraction de secondes (format mixte) : {overview['start_time_fractional_format']}",
             f"- Durée du profilage Spark : {overview['seconds']} s", "",
             "## Valeurs manquantes et échecs de typage", "",
             "| Attribut | Type | % manquant | Échecs de typage | Valeurs distinctes (approx.) |",
             "|---|---|---|---|---|"]
    for r in columns_df.sort_values("missing_rate", ascending=False).itertuples():
        lines.append(f"| {r.column} | {r.expected_type} | {r.missing_rate * 100:.2f} | {r.cast_failures} | {r.distinct_approx} |")
    lines += ["", "## Variables numériques", "",
              "| Attribut | min | p1 | médiane | p99 | max | moyenne | écart-type | aberrants (3·IQR) |",
              "|---|---|---|---|---|---|---|---|---|"]
    for r in numeric_df.itertuples():
        lines.append(f"| {r.column} | {r.min:.4g} | {r.p01:.4g} | {r.median:.4g} | {r.p99:.4g} | {r.max:.4g} | "
                     f"{r.mean:.4g} | {r.std:.4g} | {r.outliers_iqr3} ({r.outlier_rate * 100:.2f} %) |")
    (ROOT / "report" / "profiling_summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
