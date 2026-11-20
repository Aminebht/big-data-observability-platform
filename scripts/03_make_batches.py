"""Découpe la zone bronze en micro-batches mensuels et injecte des défauts connus.

Sorties :
  data/batches/clean/<YYYY-MM>.parquet    lots d'origine
  data/batches/faulty/<YYYY-MM>.parquet   mêmes lots, ~12 d'entre eux volontairement abîmés
  data/batches/fault_manifest.json        liste exacte des défauts injectés (vérité terrain)
"""
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from dq.schema import BOOL_COLS  # noqa: E402
from dq.settings import BATCHES, BRONZE  # noqa: E402

SEED = 42

# mois abîmé -> (type de défaut, description, détecteurs/règles attendus)
FAULTS = {
    "2017-03": ("missing_spike", "40 % de Temperature_F vidés", ["CMP_WEATHER_CORE", "missing_drift"]),
    "2017-09": ("duplicates", "5 % des lignes dupliquées avec le même ID", ["UNQ_ID", "UNQ_EVENT"]),
    "2018-02": ("out_of_range", "Temperature_F=500 (2 %), Severity=9 (1 %), coordonnées hors USA (1 %)",
                ["VAL_TEMP", "VAL_SEVERITY", "VAL_LAT_LNG", "outliers"]),
    "2018-08": ("inconsistency", "End_Time avant Start_Time (3 %), Day/Night contradictoires (3 %)",
                ["CNS_TIME_ORDER", "CNS_TWILIGHT"]),
    "2019-03": ("format", "Zipcode 'ABCDE' (3 %), Start_Time au format MM/dd/yyyy HH:mm (3 %)",
                ["CNF_ZIPCODE", "CNF_TIMESTAMP"]),
    "2019-07": ("schema_rename", "Weather_Condition renommée en Weather", ["schema", "CMP_WEATHER_CORE"]),
    "2019-12": ("volume_drop", "lot réduit à 10 % des lignes", ["volume"]),
    "2020-05": ("volume_spike", "lot triplé (nouveaux ID)", ["volume"]),
    "2020-10": ("category_shift", "60 % de Weather_Condition remplacés par 'Snow'", ["categorical_drift"]),
    "2021-04": ("unit_change", "Pressure_in convertie en hPa pour 30 % des lignes", ["VAL_PRESSURE", "numeric_drift"]),
    "2021-11": ("schema_drop", "colonne Humidity_pct supprimée", ["schema", "CMP_WEATHER_CORE"]),
    "2022-06": ("boolean_format", "indicateurs routiers '1'/'0' au lieu de True/False (20 %)", ["CNF_BOOLEANS"]),
}


def pick(df, frac, rng):
    return rng.random(len(df)) < frac


def inject(month: str, df: pd.DataFrame, rng) -> pd.DataFrame:
    kind = FAULTS[month][0]
    df = df.copy()
    if kind == "missing_spike":
        df.loc[pick(df, 0.40, rng), "Temperature_F"] = None
    elif kind == "duplicates":
        df = pd.concat([df, df.sample(frac=0.05, random_state=SEED)], ignore_index=True)
    elif kind == "out_of_range":
        df.loc[pick(df, 0.02, rng), "Temperature_F"] = "500.0"
        df.loc[pick(df, 0.01, rng), "Severity"] = "9"
        m = pick(df, 0.01, rng)
        df.loc[m, "Start_Lat"], df.loc[m, "Start_Lng"] = "48.8566", "2.3522"   # Paris
    elif kind == "inconsistency":
        m = pick(df, 0.03, rng)
        df.loc[m, ["Start_Time", "End_Time"]] = df.loc[m, ["End_Time", "Start_Time"]].values
        m = pick(df, 0.03, rng)
        df.loc[m, "Sunrise_Sunset"], df.loc[m, "Civil_Twilight"] = "Day", "Night"
    elif kind == "format":
        df.loc[pick(df, 0.03, rng), "Zipcode"] = "ABCDE"
        m = pick(df, 0.03, rng)
        ts = pd.to_datetime(df.loc[m, "Start_Time"].str.slice(0, 19), errors="coerce")
        df.loc[m, "Start_Time"] = ts.dt.strftime("%m/%d/%Y %H:%M")
    elif kind == "schema_rename":
        df = df.rename(columns={"Weather_Condition": "Weather"})
    elif kind == "volume_drop":
        df = df.sample(frac=0.10, random_state=SEED)
    elif kind == "volume_spike":
        copies = []
        for k in (1, 2):
            c = df.copy()
            num = pd.to_numeric(c["ID"].str.slice(2), errors="coerce").fillna(0).astype("int64")
            c["ID"] = "A-" + (num + k * 100_000_000).astype(str)
            st = pd.to_datetime(c["Start_Time"].str.slice(0, 19), errors="coerce")
            c["Start_Time"] = (st + pd.to_timedelta(rng.integers(1, 3600, len(c)), unit="s")).dt.strftime(
                "%Y-%m-%d %H:%M:%S")
            copies.append(c)
        df = pd.concat([df, *copies], ignore_index=True)
    elif kind == "category_shift":
        df.loc[pick(df, 0.60, rng), "Weather_Condition"] = "Snow"
    elif kind == "unit_change":
        m = pick(df, 0.30, rng) & df["Pressure_in"].notna()
        df.loc[m, "Pressure_in"] = (df.loc[m, "Pressure_in"].astype(float) * 33.8639).round(1).astype(str)
    elif kind == "schema_drop":
        df = df.drop(columns=["Humidity_pct"])
    elif kind == "boolean_format":
        m = pick(df, 0.20, rng)
        for c in BOOL_COLS:
            df.loc[m, c] = df.loc[m, c].map({"True": "1", "False": "0"})
    return df


def main() -> None:
    rng = np.random.default_rng(SEED)
    clean_dir, faulty_dir = BATCHES / "clean", BATCHES / "faulty"
    clean_dir.mkdir(parents=True, exist_ok=True)
    faulty_dir.mkdir(parents=True, exist_ok=True)
    months = sorted(p.name.split("=", 1)[1] for p in BRONZE.glob("batch_month=*"))
    months = [m for m in months if m and m[:4].isdigit()]
    manifest = []
    for m in months:
        df = pd.read_parquet(BRONZE / f"batch_month={m}")
        df.to_parquet(clean_dir / f"{m}.parquet", index=False)
        if m in FAULTS:
            bad = inject(m, df, rng)
            kind, desc, expected = FAULTS[m]
            manifest.append({"batch_month": m, "fault_type": kind, "description": desc,
                             "expected_detection": expected, "rows_before": len(df), "rows_after": len(bad)})
        else:
            bad = df
        bad.to_parquet(faulty_dir / f"{m}.parquet", index=False)
        print(m, len(df), "FAULT " + FAULTS[m][0] if m in FAULTS else "")
    missing = sorted(set(FAULTS) - set(months))
    if missing:
        raise SystemExit(f"mois de défauts absents du dataset : {missing}")
    (BATCHES / "fault_manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False),
                                                 encoding="utf-8")
    print(f"{len(months)} lots, {len(manifest)} lots abîmés")


if __name__ == "__main__":
    main()
