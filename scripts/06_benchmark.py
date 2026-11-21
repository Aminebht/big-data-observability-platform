"""Partie 4 / section 4 – Expériences de performance.

E1  moteur : Polars (mono-machine) vs Spark local[*], volumes 10/25/50/100 %
E2  passage à l'échelle Spark : local[1], [2], [4], [8] sur 25 % (1,9 M lignes ; voir E2_FRACTION)
E3  format de stockage : CSV brut vs Parquet (Spark, 100 %)
E4  streaming (micro-batches mensuels, Structured Streaming) vs traitement batch unique (Spark, 100 %)

Chaque mesure : chargement + règles + profil de dérive ; résultats dans data/metrics/fact_run.csv.
Usage : python scripts/06_benchmark.py --exp E1 E2 E3 E4 --repeats 3
"""
import argparse
import gc
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import psutil  # noqa: E402

from dq import detectors as D  # noqa: E402
from dq import metrics as M  # noqa: E402
from dq.rules import load_rules  # noqa: E402
from dq.schema import RENAME  # noqa: E402
from dq.settings import BATCHES, BRONZE, DATA, RAW_CSV  # noqa: E402

FRACTIONS = [0.10, 0.25, 0.50, 1.00]


class PeakMemory:
    """Mémoire résidente maximale du processus Python + JVM Spark (processus enfants)."""

    def __init__(self):
        self.peak = 0
        self._stop = threading.Event()
        self.proc = psutil.Process()

    def _run(self):
        while not self._stop.is_set():
            try:
                rss = self.proc.memory_info().rss + sum(
                    c.memory_info().rss for c in self.proc.children(recursive=True))
                self.peak = max(self.peak, rss)
            except psutil.Error:
                pass
            time.sleep(0.2)

    def __enter__(self):
        self.t = threading.Thread(target=self._run, daemon=True)
        self.t.start()
        return self

    def __exit__(self, *a):
        self._stop.set()
        self.t.join()


def record(exp, engine, cores, fmt, mode, frac, rows, rep, t, mem, alerts=0, status="ok"):
    total = t.get("total_s", 0)
    row = {"run_id": f"bench-{exp}", "experiment": exp, "engine": engine, "cores": cores, "input_format": fmt,
           "mode": mode, "fraction": frac, "rows": rows, "repeat": rep, **t,
           "rows_per_s": rows / total if total else 0, "peak_mem_mb": mem / 2**20, "alerts": alerts,
           "status": status, "started_at": M.now()}
    M.append("fact_run", [row])
    print(f"{exp} {engine} cores={cores} fmt={fmt} mode={mode} frac={frac} rep={rep}: "
          f"{rows} lignes en {total:.1f} s ({row['rows_per_s']:.0f} l/s, {row['peak_mem_mb']:.0f} Mo) {status}",
          flush=True)


# ------------------------------------------------------------------ Spark, traitement unique
def spark_once(spark, frac, fmt, rules, cfg):
    from pyspark.sql import functions as F

    from dq.rules import evaluate_spark
    from dq.schema import typed_view

    t0 = time.perf_counter()
    if fmt == "csv":
        raw = spark.read.option("header", True).option("quote", '"').option("escape", '"').csv(str(RAW_CSV))
        for old, new in RENAME.items():
            raw = raw.withColumnRenamed(old, new)
    else:
        raw = spark.read.parquet(str(BRONZE)).drop("batch_month")
    if frac < 1:
        raw = raw.where((F.expr("try_cast(substr(ID, 3) as bigint)") % 100) < int(frac * 100))
    # pas de cache : sur 7,7 M lignes (colonnes typées + brutes) il sature la mémoire ;
    # chaque étape relit le stockage, ce qui mesure aussi l'effet du format (E3)
    df = typed_view(raw)
    n = df.count()
    t1 = time.perf_counter()
    _, results, _ = evaluate_spark(df, rules)
    t2 = time.perf_counter()
    D.profile_batch_spark(df, cfg, {})
    t3 = time.perf_counter()
    return n, {"total_s": t3 - t0, "load_s": t1 - t0, "rules_s": t2 - t1, "detectors_s": t3 - t2, "write_s": 0}


def polars_once(frac, rules, cfg):
    import polars as pl

    from dq import polars_engine as P

    t0 = time.perf_counter()
    lf = pl.scan_parquet(str(BRONZE / "**" / "*.parquet"), hive_partitioning=False)
    if frac < 1:
        lf = lf.filter((pl.col("ID").str.slice(2).cast(pl.Int64, strict=False) % 100) < int(frac * 100))
    df = P.typed_view(lf).collect()
    n = df.height
    t1 = time.perf_counter()
    P.evaluate(df, rules)
    t2 = time.perf_counter()
    P.profile_batch(df, cfg, {})
    t3 = time.perf_counter()
    del df
    gc.collect()
    return n, {"total_s": t3 - t0, "load_s": t1 - t0, "rules_s": t2 - t1, "detectors_s": t3 - t2, "write_s": 0}


def run_spark(exp, cores, frac, fmt, reps, rules, cfg):
    from dq.settings import get_spark

    spark = get_spark(f"bench-{exp}", cores=cores)
    spark_once(spark, 0.01, "parquet", rules, cfg)  # préchauffage JVM (non mesuré)
    for rep in range(1, reps + 1):
        with PeakMemory() as pm:
            n, t = spark_once(spark, frac, fmt, rules, cfg)
        record(exp, "spark", cores, fmt, "batch", frac, n, rep, t, pm.peak)
    spark.stop()


MEM_CAP_GB = 6.0         # au-delà, la mesure Polars est arrêtée et notée out_of_memory
MIN_FREE_GB = 0.5        # ... ou si la RAM libre de la machine passe sous ce seuil


def polars_guarded(frac):
    """Mesure Polars dans un processus enfant surveillé : un dépassement mémoire n'emporte pas la machine."""
    import json as _json

    proc = subprocess.Popen([sys.executable, __file__, "--polars-worker", str(frac)],
                            stdout=subprocess.PIPE, text=True)
    ps, peak, killed = psutil.Process(proc.pid), 0, False
    while proc.poll() is None:
        try:
            rss = ps.memory_info().rss + sum(c.memory_info().rss for c in ps.children(recursive=True))
            peak = max(peak, rss)
            if rss > MEM_CAP_GB * 2**30 or psutil.virtual_memory().available < MIN_FREE_GB * 2**30:
                proc.kill(); killed = True
                break
        except psutil.Error:
            pass
        time.sleep(0.2)
    out = proc.communicate()[0]
    if killed or proc.returncode != 0:
        return None, None, peak
    res = _json.loads(out.strip().splitlines()[-1])
    return res["rows"], res["timings"], peak


def e1(reps, rules, cfg, engine="all"):
    for frac in (FRACTIONS if engine in ("all", "polars") else []):
        for rep in range(1, reps + 1):
            n, t, peak = polars_guarded(frac)
            if n is None:
                record("E1", "polars", "*", "parquet", "batch", frac, 0, rep, {}, peak, status="out_of_memory")
                break  # inutile de répéter une mesure qui dépasse la mémoire
            record("E1", "polars", "*", "parquet", "batch", frac, n, rep, t, peak)
    for frac in (FRACTIONS if engine in ("all", "spark") else []):
        run_spark("E1", "*", frac, "parquet", reps, rules, cfg)


E2_FRACTION = 0.25  # 25 % du jeu (1,9 M lignes) : local[1] sur 100 % prendrait ~20 min par mesure


def e2(reps, rules, cfg):
    for cores in (1, 2, 4, 8):
        run_spark("E2", cores, E2_FRACTION, "parquet", reps, rules, cfg)


def e3(reps, rules, cfg, formats=("csv", "parquet")):
    # Parquet à 100 % sur local[*] = même configuration que E1 Spark 100 % (réutilisable par 08_charts.py)
    for fmt in formats:
        run_spark("E3", "*", 1.0, fmt, reps, rules, cfg)


def e4(reps, rules, cfg):
    """Streaming réel : tous les fichiers mensuels sont présents, le flux les consomme un par un."""
    py = sys.executable
    for rep in range(1, reps + 1):
        run = f"bench-e4-stream-{rep}"
        inc = BATCHES / "incoming" / run
        shutil.rmtree(inc, ignore_errors=True)
        inc.mkdir(parents=True)
        for f in sorted((BATCHES / "clean").glob("*.parquet")):
            shutil.copyfile(f, inc / f.name)
        n_files = len(list(inc.glob("*.parquet")))
        t0 = time.perf_counter()
        with PeakMemory() as pm:
            subprocess.run([py, str(ROOT / "scripts" / "05_stream.py"), "--run", run, "--quiet",
                            "--stop-after", str(n_files), "--no-metrics"], check=True)
        total = time.perf_counter() - t0
        record("E4", "spark", "*", "parquet", "streaming", 1.0, 7728394, rep,
               {"total_s": total, "load_s": 0, "rules_s": 0, "detectors_s": 0, "write_s": 0}, pm.peak)
        shutil.rmtree(inc, ignore_errors=True)
        (DATA / "state" / f"{run}.json").unlink(missing_ok=True)
    # le mode batch équivalent (Spark local[*], Parquet, 100 %) est mesuré par E1 : voir 08_charts.py


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--exp", nargs="+", default=["E1", "E2", "E3", "E4"])
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--engine", default="all", choices=["all", "polars", "spark"], help="E1 uniquement")
    ap.add_argument("--no-reset", action="store_true", help="ne pas effacer les mesures existantes du run")
    ap.add_argument("--polars-worker", type=float, default=None, help=argparse.SUPPRESS)
    args = ap.parse_args()
    rules, cfg = load_rules(), D.load_detectors()
    if args.polars_worker is not None:
        import json as _json
        n, t = polars_once(args.polars_worker, rules, cfg)
        print(_json.dumps({"rows": n, "timings": t}))
        return
    for exp in args.exp:
        if not args.no_reset:
            M.reset_run(f"bench-{exp}")
        if exp == "E1":
            e1(args.repeats, rules, cfg, args.engine)
        else:
            {"E2": e2, "E3": e3, "E4": e4}[exp](args.repeats, rules, cfg)


if __name__ == "__main__":
    main()
