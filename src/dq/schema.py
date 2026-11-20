"""Schéma attendu du dataset US Accidents et typage des colonnes brutes.

Les données arrivent en texte (zone d'atterrissage / bronze). Le typage est fait ici
avec des conversions tolérantes (try_cast) : une valeur non convertible devient NULL,
et les règles de conformité mesurent ces échecs sur la colonne brute.
"""
from __future__ import annotations

# nom d'origine (CSV Kaggle) -> nom normalisé (sans parenthèses, compatible Parquet / Power BI)
RENAME = {
    "Distance(mi)": "Distance_mi",
    "Temperature(F)": "Temperature_F",
    "Wind_Chill(F)": "Wind_Chill_F",
    "Humidity(%)": "Humidity_pct",
    "Pressure(in)": "Pressure_in",
    "Visibility(mi)": "Visibility_mi",
    "Wind_Speed(mph)": "Wind_Speed_mph",
    "Precipitation(in)": "Precipitation_in",
}

BOOL_COLS = [
    "Amenity", "Bump", "Crossing", "Give_Way", "Junction", "No_Exit", "Railway",
    "Roundabout", "Station", "Stop", "Traffic_Calming", "Traffic_Signal", "Turning_Loop",
]
TWILIGHT_COLS = ["Sunrise_Sunset", "Civil_Twilight", "Nautical_Twilight", "Astronomical_Twilight"]
TIMESTAMP_COLS = ["Start_Time", "End_Time", "Weather_Timestamp"]
INT_COLS = ["Severity"]
DOUBLE_COLS = [
    "Start_Lat", "Start_Lng", "End_Lat", "End_Lng", "Distance_mi", "Temperature_F",
    "Wind_Chill_F", "Humidity_pct", "Pressure_in", "Visibility_mi", "Wind_Speed_mph",
    "Precipitation_in",
]
STRING_COLS = [
    "ID", "Source", "Description", "Street", "City", "County", "State", "Zipcode",
    "Country", "Timezone", "Airport_Code", "Wind_Direction", "Weather_Condition",
] + TWILIGHT_COLS

# ordre des colonnes dans le fichier d'origine (après renommage)
COLUMNS = [
    "ID", "Source", "Severity", "Start_Time", "End_Time", "Start_Lat", "Start_Lng",
    "End_Lat", "End_Lng", "Distance_mi", "Description", "Street", "City", "County",
    "State", "Zipcode", "Country", "Timezone", "Airport_Code", "Weather_Timestamp",
    "Temperature_F", "Wind_Chill_F", "Humidity_pct", "Pressure_in", "Visibility_mi",
    "Wind_Direction", "Wind_Speed_mph", "Precipitation_in", "Weather_Condition",
] + BOOL_COLS + TWILIGHT_COLS

EXPECTED_TYPES = {
    **{c: "string" for c in STRING_COLS},
    **{c: "int" for c in INT_COLS},
    **{c: "double" for c in DOUBLE_COLS},
    **{c: "timestamp" for c in TIMESTAMP_COLS},
    **{c: "boolean" for c in BOOL_COLS},
}
NUMERIC_COLS = INT_COLS + DOUBLE_COLS
CATEGORICAL_COLS = ["Source", "State", "Timezone", "Weather_Condition", "Wind_Direction"] + TWILIGHT_COLS

assert len(COLUMNS) == 46 and set(COLUMNS) == set(EXPECTED_TYPES)


def typed_view(df):
    """Retourne un DataFrame typé à partir des colonnes brutes (texte).

    Les colonnes brutes sont conservées avec le suffixe `__raw` pour les règles de conformité.
    """
    from pyspark.sql import functions as F

    cols = []
    for c in COLUMNS:
        if c not in df.columns:
            cols.append(F.lit(None).cast(EXPECTED_TYPES[c]).alias(c))
            continue
        raw = F.col(f"`{c}`")
        t = EXPECTED_TYPES[c]
        if t == "timestamp":
            # deux formats coexistent : 'yyyy-MM-dd HH:mm:ss' et avec fraction de secondes
            expr = F.expr(f"try_to_timestamp(substr(`{c}`, 1, 19), 'yyyy-MM-dd HH:mm:ss')")
        elif t == "boolean":
            expr = F.when(F.lower(F.trim(raw)) == "true", F.lit(True)).when(
                F.lower(F.trim(raw)) == "false", F.lit(False))
        elif t in ("int", "double"):
            expr = F.expr(f"try_cast(`{c}` AS {t})")
        else:
            expr = F.when(F.trim(raw) == "", F.lit(None)).otherwise(raw)
        cols.append(expr.alias(c))
    extra = [F.col(f"`{c}`").alias(f"{c}__raw") for c in df.columns if c in COLUMNS]
    passthrough = [F.col(c) for c in df.columns if c not in COLUMNS and not c.endswith("__raw")]
    typed = df.select(*cols, *extra, *passthrough)
    # horodatages en secondes epoch : expressions de règles portables Spark / Polars
    return typed.select("*", *[F.col(c).cast("long").cast("double").alias(f"{c}_s") for c in TIMESTAMP_COLS])
