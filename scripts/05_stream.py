"""Partie 4 – Surveillance en continu avec Spark Structured Streaming.

Le flux surveille data/batches/incoming/<run_id>/ et traite UN fichier mensuel par déclenchement
(maxFilesPerTrigger=1). Pour chaque micro-batch : contrôle du schéma réel du fichier, règles,
score, détecteurs, alertes et ajout des lignes dans les tables CSV de data/metrics/.

Usage :
  python scripts/05_stream.py --run clean           (à lancer avec scripts/04_producer.py --run clean)
  python scripts/05_stream.py --run faulty --stop-after 86
"""
import argparse
import shutil
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import pyarrow.parquet as pq  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import StringType, StructField, StructType  # noqa: E402

from dq.pipeline import RunContext, process_spark_batch  # noqa: E402
from dq.schema import COLUMNS  # noqa: E402
from dq.settings import BATCHES, DATA, get_spark  # noqa: E402

# schéma attendu du flux : toutes les colonnes en texte (zone d'atterrissage)
STREAM_SCHEMA = StructType([StructField(c, StringType()) for c in COLUMNS])


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", default="clean", help="identifiant du run (clean / faulty / ...)")
    ap.add_argument("--cores", default="*")
    ap.add_argument("--stop-after", type=int, default=0, help="arrêter après N lots (0 = jamais)")
    ap.add_argument("--quiet", action="store_true")
    ap.add_argument("--no-metrics", action="store_true", help="ne pas écrire les tables (mesures de perf)")
    args = ap.parse_args()

    incoming = BATCHES / "incoming" / args.run
    checkpoint = DATA / "checkpoints" / args.run
    shutil.rmtree(checkpoint, ignore_errors=True)
    incoming.mkdir(parents=True, exist_ok=True)

    spark = get_spark(f"stream-{args.run}", cores=args.cores)
    ctx = RunContext.create(args.run, with_manifest=(args.run == "faulty"), console=not args.quiet,
                            write_metrics=not args.no_metrics)

    def on_batch(batch_df, epoch_id):
        files = [r[0] for r in batch_df.select(F.input_file_name()).distinct().collect()]
        for f in sorted(files):
            path = Path(f.replace("file:///", "").replace("file:/", "").replace("%20", " "))
            month = path.stem
            fields = [(fl.name, str(fl.type)) for fl in pq.read_schema(path)]
            one = batch_df.where(F.input_file_name() == f).drop("__file")
            out = process_spark_batch(ctx, one, month, fields)
            print(f"lot {out['batch_month']}: {out['rows']} lignes, score {out['score']:.1f} "
                  f"[{out['status']}], {out['alerts']} alertes, {out['seconds']:.1f} s", flush=True)

    query = (spark.readStream.schema(STREAM_SCHEMA)
             .option("maxFilesPerTrigger", 1)
             .parquet(str(incoming))
             .writeStream.foreachBatch(on_batch)
             .option("checkpointLocation", str(checkpoint))
             .trigger(processingTime="2 seconds")
             .start())
    print(f"flux démarré, en attente de fichiers dans {incoming}", flush=True)
    try:
        while query.isActive:
            time.sleep(1)
            if args.stop_after and ctx.batch_seq >= args.stop_after:
                break
    except KeyboardInterrupt:
        pass
    query.stop()
    spark.stop()
    print(f"terminé : {ctx.batch_seq} lots, {ctx.rows_total} lignes, {ctx.alert_count} alertes", flush=True)


if __name__ == "__main__":
    main()
