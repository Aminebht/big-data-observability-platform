"""Chemins du projet et création de la session Spark (mode local, Windows)."""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data"
RAW_CSV = DATA / "raw" / "US_Accidents_March23.csv"
LAKE = DATA / "lake"
BRONZE = LAKE / "bronze"            # Parquet, toutes colonnes en texte, partitionné par mois
BATCHES = DATA / "batches"          # micro-batches mensuels (source + dossier d'arrivée)
METRICS = DATA / "metrics"          # tables CSV lues par Power BI
CONFIG = ROOT / "config"
LOGS = ROOT / "logs"
TMP = ROOT / "tmp"
HADOOP_HOME = Path(os.environ.get("HADOOP_HOME", r"D:\hadoop"))


def _prepare_env() -> None:
    os.environ["HADOOP_HOME"] = str(HADOOP_HOME)
    bin_dir = str(HADOOP_HOME / "bin")
    if bin_dir not in os.environ.get("PATH", ""):
        os.environ["PATH"] = bin_dir + os.pathsep + os.environ.get("PATH", "")
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    TMP.mkdir(parents=True, exist_ok=True)


def get_spark(app: str = "dq", cores: str | int = "*", driver_memory: str = "8g",
              shuffle_partitions: int | None = None):
    """Session Spark locale. `cores` = '*' ou un entier (local[N])."""
    _prepare_env()
    from pyspark.sql import SparkSession

    n = os.cpu_count() if cores == "*" else int(cores)
    builder = (
        SparkSession.builder.appName(app)
        .master(f"local[{cores}]")
        .config("spark.driver.memory", driver_memory)
        .config("spark.local.dir", str(TMP))
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions or max(4, 2 * n)))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.extraJavaOptions", f"-Djava.io.tmpdir={TMP.as_posix()}")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
