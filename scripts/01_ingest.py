"""Étape 1 : CSV brut -> zone bronze (Parquet, colonnes texte, partitionné par mois).

Toutes les colonnes sont lues en texte : c'est la donnée « telle que reçue ».
Le typage et les contrôles de conformité se font ensuite dans le moteur de qualité.
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pyspark.sql import functions as F  # noqa: E402

from dq.schema import RENAME  # noqa: E402
from dq.settings import BRONZE, RAW_CSV, get_spark  # noqa: E402


def main() -> None:
    spark = get_spark("ingest")
    t0 = time.time()
    df = (spark.read
          .option("header", True)
          .option("inferSchema", False)
          .option("quote", '"').option("escape", '"')
          .csv(str(RAW_CSV)))
    for old, new in RENAME.items():
        df = df.withColumnRenamed(old, new)
    df = df.withColumn("batch_month", F.substring("Start_Time", 1, 7))
    (df.repartition("batch_month")
       .write.mode("overwrite")
       .partitionBy("batch_month")
       .parquet(str(BRONZE)))
    elapsed = time.time() - t0

    out = spark.read.parquet(str(BRONZE))
    n = out.count()
    months = out.select("batch_month").distinct().count()
    print(f"rows={n} columns={len(out.columns) - 1} months={months} seconds={elapsed:.1f}")
    spark.stop()


if __name__ == "__main__":
    main()
