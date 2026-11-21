"""Graphiques des expériences (E1–E4) et de la qualité dans le temps, pour le rapport.

Entrées : data/metrics/fact_run.csv, dim_batch.csv, fact_alert.csv
Sorties : report/figures/perf_*.png, report/figures/quality_*.png, report/benchmark_summary.md
"""
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
METRICS = ROOT / "data" / "metrics"
FIG = ROOT / "report" / "figures"
COLORS = {"spark": "#e8710a", "polars": "#1a73e8"}


def med(df, keys):
    ok = df[df.status == "ok"]
    return ok.groupby(keys, as_index=False).agg(total_s=("total_s", "median"), rows=("rows", "max"),
                                                 rows_per_s=("rows_per_s", "median"),
                                                 peak_mem_mb=("peak_mem_mb", "median"),
                                                 load_s=("load_s", "median"), rules_s=("rules_s", "median"),
                                                 detectors_s=("detectors_s", "median"))


def perf_charts(lines):
    path = METRICS / "fact_run.csv"
    if not path.exists():
        print("fact_run.csv absent : lancer scripts/06_benchmark.py")
        return
    runs = pd.read_csv(path)
    # E3 « parquet » et E4 « batch » = même configuration que E1 Spark local[*], Parquet, 100 % : réutilisées
    ref = runs[(runs.experiment == "E1") & (runs.engine == "spark") & (runs.fraction == 1.0)]
    extra = []
    if not ((runs.experiment == "E3") & (runs.input_format == "parquet")).any():
        extra.append(ref.assign(experiment="E3"))
    if not ((runs.experiment == "E4") & (runs["mode"] == "batch")).any():
        extra.append(ref.assign(experiment="E4"))
    if extra:
        runs = pd.concat([runs, *extra], ignore_index=True)

    e1 = med(runs[runs.experiment == "E1"], ["engine", "fraction"])
    if len(e1):
        fig, axes = plt.subplots(1, 2, figsize=(12, 4))
        for eng, g in e1.groupby("engine"):
            axes[0].plot(g.rows / 1e6, g.total_s, marker="o", label=eng, color=COLORS.get(eng))
            axes[1].plot(g.rows / 1e6, g.peak_mem_mb / 1024, marker="o", label=eng, color=COLORS.get(eng))
        axes[0].set(xlabel="millions de lignes", ylabel="temps (s)", title="E1 – Temps selon le volume")
        axes[1].set(xlabel="millions de lignes", ylabel="mémoire max (Go)", title="E1 – Mémoire selon le volume")
        for ax in axes:
            ax.legend(); ax.grid(alpha=.3)
        fig.tight_layout(); fig.savefig(FIG / "perf_e1_engines.png", dpi=130); plt.close(fig)
        lines += ["## E1 – Polars vs Spark", "", e1.round(2).to_markdown(index=False), ""]
        oom = runs[(runs.experiment == "E1") & (runs.status != "ok")]
        if len(oom):
            lines += [f"Échecs : {len(oom)} mesure(s) ({', '.join(sorted(set(oom.status)))}).", ""]

    e2 = med(runs[runs.experiment == "E2"], ["cores"])
    if len(e2):
        e2["cores"] = e2.cores.astype(int)
        e2 = e2.sort_values("cores")
        base = e2.total_s.iloc[0]
        e2["speedup"] = base / e2.total_s
        fig, axes = plt.subplots(1, 2, figsize=(12, 4))
        axes[0].bar(e2.cores.astype(str), e2.total_s, color=COLORS["spark"])
        axes[0].set(xlabel="cœurs (local[N])", ylabel="temps (s)", title="E2 – Temps selon le nombre de cœurs")
        axes[1].plot(e2.cores, e2.speedup, marker="o", color=COLORS["spark"], label="mesuré")
        axes[1].plot(e2.cores, e2.cores / e2.cores.iloc[0], "--", color="grey", label="idéal")
        axes[1].set(xlabel="cœurs", ylabel="accélération", title="E2 – Accélération (speed-up)")
        axes[1].legend(); axes[1].grid(alpha=.3)
        fig.tight_layout(); fig.savefig(FIG / "perf_e2_scaling.png", dpi=130); plt.close(fig)
        lines += ["## E2 – Passage à l'échelle Spark", "", e2.round(2).to_markdown(index=False), ""]

    for exp, key, title, fname in [("E3", "input_format", "E3 – CSV vs Parquet", "perf_e3_format.png"),
                                   ("E4", "mode", "E4 – Streaming vs batch", "perf_e4_streaming.png")]:
        e = med(runs[runs.experiment == exp], [key])
        if not len(e):
            continue
        fig, ax = plt.subplots(figsize=(6, 4))
        parts = ["load_s", "rules_s", "detectors_s"]
        if e[parts].sum().sum() > 0 and exp == "E3":
            bottom = None
            for p, col in zip(parts, ["#9aa0a6", "#e8710a", "#34a853"]):
                ax.bar(e[key], e[p], bottom=bottom, label={"load_s": "chargement", "rules_s": "règles",
                                                          "detectors_s": "détecteurs"}[p], color=col)
                bottom = e[p] if bottom is None else bottom + e[p]
            ax.legend()
        else:
            ax.bar(e[key], e.total_s, color=COLORS["spark"])
        ax.set(ylabel="temps (s)", title=title)
        fig.tight_layout(); fig.savefig(FIG / fname, dpi=130); plt.close(fig)
        lines += [f"## {title}", "", e.round(2).to_markdown(index=False), ""]


def quality_charts(lines, run="faulty"):
    path = METRICS / "dim_batch.csv"
    if not path.exists():
        return
    b = pd.read_csv(path)
    b = b[b.run_id == run].sort_values("batch_month")
    if not len(b):
        return
    fig, ax = plt.subplots(figsize=(13, 4))
    x = pd.to_datetime(b.batch_month)
    ax.plot(x, b.global_score, marker=".", color="#1a73e8", label="score global")
    ax.axhline(95, color="#34a853", ls="--", lw=1); ax.axhline(85, color="#ea4335", ls="--", lw=1)
    faults = b[b.planted_faults.notna() & (b.planted_faults != "")]
    ax.scatter(pd.to_datetime(faults.batch_month), faults.global_score, color="#ea4335", zorder=3,
               label="lot avec défaut injecté")
    ax.set(ylabel="score (0–100)", title=f"Score global de qualité par lot mensuel (run {run})")
    ax.legend(); ax.grid(alpha=.3)
    fig.tight_layout(); fig.savefig(FIG / f"quality_score_{run}.png", dpi=130); plt.close(fig)


def main():
    FIG.mkdir(parents=True, exist_ok=True)
    lines = ["# Résultats des expériences de performance", ""]
    perf_charts(lines)
    for run in ("clean", "faulty"):
        quality_charts(lines, run)
    (ROOT / "report" / "benchmark_summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))


if __name__ == "__main__":
    sys.exit(main())
