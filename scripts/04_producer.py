"""Simule l'arrivée progressive des données : dépose un fichier mensuel à la fois.

Copie data/batches/<source>/<YYYY-MM>.parquet vers data/batches/incoming/<run>/ avec une pause.
La copie se fait sous un nom caché puis est renommée (dépôt atomique : Spark ne lit jamais un
fichier à moitié écrit).

Usage : python scripts/04_producer.py --run faulty --source faulty --delay 3
"""
import argparse
import os
import shutil
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from dq.settings import BATCHES  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", default="clean")
    ap.add_argument("--source", default=None, help="clean ou faulty (défaut : même nom que --run)")
    ap.add_argument("--delay", type=float, default=3.0, help="secondes entre deux fichiers")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    src = BATCHES / (args.source or args.run)
    dst = BATCHES / "incoming" / args.run
    if dst.exists():
        shutil.rmtree(dst)
    dst.mkdir(parents=True)
    files = sorted(src.glob("*.parquet"))
    if args.limit:
        files = files[:args.limit]
    for i, f in enumerate(files, 1):
        tmp = dst / f".{f.name}.tmp"
        shutil.copyfile(f, tmp)
        os.replace(tmp, dst / f.name)
        print(f"[{i}/{len(files)}] déposé {f.name}", flush=True)
        time.sleep(args.delay)


if __name__ == "__main__":
    main()
