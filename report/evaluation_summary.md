# Évaluation sur défauts injectés

- Défauts injectés : 12 ; détectés : 12 (**rappel 100 %**)
- Détectés par le détecteur/règle attendu : 12
- Alertes nouvelles dans les lots abîmés (VP) : 25 ; dans des lots sains (FP) : 1

| Mois | Défaut | Attendu | Détecté par | OK |
|---|---|---|---|---|
| 2017-03 | 40 % de Temperature_F vidés | CMP_WEATHER_CORE;missing_drift | CMP_WEATHER_CORE;missing_drift | ✅ |
| 2017-09 | 5 % des lignes dupliquées avec le même ID | UNQ_ID;UNQ_EVENT | UNQ_ID | ✅ |
| 2018-02 | Temperature_F=500 (2 %), Severity=9 (1 %), coordonnées hors USA (1 %) | VAL_TEMP;VAL_SEVERITY;VAL_LAT_LNG;outliers | VAL_LAT_LNG;VAL_SEVERITY;VAL_TEMP;outliers | ✅ |
| 2018-08 | End_Time avant Start_Time (3 %), Day/Night contradictoires (3 %) | CNS_TIME_ORDER;CNS_TWILIGHT | CNS_TIME_ORDER;CNS_TWILIGHT | ✅ |
| 2019-03 | Zipcode 'ABCDE' (3 %), Start_Time au format MM/dd/yyyy HH:mm (3 %) | CNF_ZIPCODE;CNF_TIMESTAMP | CNF_TIMESTAMP;CNF_ZIPCODE | ✅ |
| 2019-07 | Weather_Condition renommée en Weather | schema;CMP_WEATHER_CORE | CMP_WEATHER_CORE;SCHEMA;categorical_drift;missing_drift | ✅ |
| 2019-12 | lot réduit à 10 % des lignes | volume | volume | ✅ |
| 2020-05 | lot triplé (nouveaux ID) | volume | CNS_TIME_ORDER;volume | ✅ |
| 2020-10 | 60 % de Weather_Condition remplacés par 'Snow' | categorical_drift | categorical_drift | ✅ |
| 2021-04 | Pressure_in convertie en hPa pour 30 % des lignes | VAL_PRESSURE;numeric_drift | VAL_PRESSURE;numeric_drift | ✅ |
| 2021-11 | colonne Humidity_pct supprimée | schema;CMP_WEATHER_CORE | CMP_WEATHER_CORE;SCHEMA;missing_drift | ✅ |
| 2022-06 | indicateurs routiers '1'/'0' au lieu de True/False (20 %) | CNF_BOOLEANS | CNF_BOOLEANS | ✅ |

| Règle / détecteur | VP | FP | Précision |
|---|---|---|---|
| CMP_WEATHER_CORE | 3 | 0 | 100 % |
| CNF_BOOLEANS | 1 | 0 | 100 % |
| CNF_TIMESTAMP | 1 | 0 | 100 % |
| CNF_ZIPCODE | 1 | 0 | 100 % |
| CNS_TIME_ORDER | 2 | 0 | 100 % |
| CNS_TWILIGHT | 1 | 0 | 100 % |
| SCHEMA | 2 | 0 | 100 % |
| UNQ_ID | 1 | 0 | 100 % |
| VAL_LAT_LNG | 1 | 0 | 100 % |
| VAL_PRESSURE | 1 | 0 | 100 % |
| VAL_SEVERITY | 1 | 0 | 100 % |
| VAL_TEMP | 1 | 0 | 100 % |
| categorical_drift | 2 | 0 | 100 % |
| missing_drift | 3 | 0 | 100 % |
| numeric_drift | 1 | 1 | 50 % |
| outliers | 1 | 0 | 100 % |
| volume | 2 | 0 | 100 % |
