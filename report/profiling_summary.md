# Résumé du profilage (Partie 1)

- Enregistrements : **7 728 394**
- Attributs : **46**
- Période : 2016-01-14 21:18:33 → 2023-04-01 00:30:00
- ID en double : 0 lignes
- Doublons de contenu (toutes colonnes sauf ID) : 102338 lignes en trop
- Doublons d'événement (Start_Time, coordonnées, Description) : 397061 lignes en trop
- End_Time ≤ Start_Time : 0
- Start_Time avec fraction de secondes (format mixte) : 743166
- Durée du profilage Spark : 664.7 s

## Valeurs manquantes et échecs de typage

| Attribut | Type | % manquant | Échecs de typage | Valeurs distinctes (approx.) |
|---|---|---|---|---|
| End_Lng | double | 44.03 | 0 | 1633005 |
| End_Lat | double | 44.03 | 0 | 1449534 |
| Precipitation_in | double | 28.51 | 0 | 304 |
| Wind_Chill_F | double | 25.87 | 0 | 997 |
| Wind_Speed_mph | double | 7.39 | 0 | 173 |
| Visibility_mi | double | 2.29 | 0 | 92 |
| Wind_Direction | string | 2.27 | 0 | 22 |
| Humidity_pct | double | 2.25 | 0 | 97 |
| Weather_Condition | string | 2.24 | 0 | 153 |
| Temperature_F | double | 2.12 | 0 | 862 |
| Pressure_in | double | 1.82 | 0 | 1142 |
| Weather_Timestamp | timestamp | 1.56 | 0 | 946308 |
| Sunrise_Sunset | string | 0.30 | 0 | 2 |
| Civil_Twilight | string | 0.30 | 0 | 2 |
| Astronomical_Twilight | string | 0.30 | 0 | 2 |
| Nautical_Twilight | string | 0.30 | 0 | 2 |
| Airport_Code | string | 0.29 | 0 | 1909 |
| Street | string | 0.14 | 0 | 340065 |
| Timezone | string | 0.10 | 0 | 4 |
| Zipcode | string | 0.02 | 0 | 823900 |
| City | string | 0.00 | 0 | 13769 |
| Description | string | 0.00 | 0 | 3928694 |
| ID | string | 0.00 | 0 | 7390017 |
| Distance_mi | double | 0.00 | 0 | 21805 |
| Start_Lng | double | 0.00 | 0 | 2455263 |
| Source | string | 0.00 | 0 | 3 |
| Severity | int | 0.00 | 0 | 4 |
| Start_Time | timestamp | 0.00 | 0 | 6171824 |
| End_Time | timestamp | 0.00 | 0 | 6483114 |
| Start_Lat | double | 0.00 | 0 | 2514748 |
| County | string | 0.00 | 0 | 1926 |
| Amenity | boolean | 0.00 | 0 | 2 |
| Country | string | 0.00 | 0 | 1 |
| State | string | 0.00 | 0 | 52 |
| Bump | boolean | 0.00 | 0 | 2 |
| Crossing | boolean | 0.00 | 0 | 2 |
| Give_Way | boolean | 0.00 | 0 | 2 |
| Junction | boolean | 0.00 | 0 | 2 |
| Station | boolean | 0.00 | 0 | 2 |
| Roundabout | boolean | 0.00 | 0 | 2 |
| Railway | boolean | 0.00 | 0 | 2 |
| No_Exit | boolean | 0.00 | 0 | 2 |
| Turning_Loop | boolean | 0.00 | 0 | 1 |
| Traffic_Signal | boolean | 0.00 | 0 | 2 |
| Traffic_Calming | boolean | 0.00 | 0 | 2 |
| Stop | boolean | 0.00 | 0 | 2 |

## Variables numériques

| Attribut | min | p1 | médiane | p99 | max | moyenne | écart-type | aberrants (3·IQR) |
|---|---|---|---|---|---|---|---|---|
| Severity | 1 | 2 | 2 | 4 | 4 | 2.212 | 0.4875 | 1571413 (20.33 %) |
| Start_Lat | 24.55 | 25.73 | 35.82 | 47.46 | 49 | 36.2 | 5.076 | 0 (0.00 %) |
| Start_Lng | -124.6 | -123 | -87.77 | -71.5 | -67.11 | -94.7 | 17.39 | 0 (0.00 %) |
| End_Lat | 24.57 | 25.69 | 36.18 | 47.24 | 49.08 | 36.26 | 5.273 | 0 (0.00 %) |
| End_Lng | -124.5 | -123.2 | -88.03 | -72.88 | -67.11 | -95.73 | 18.11 | 0 (0.00 %) |
| Distance_mi | 0 | 0 | 0.03 | 6.982 | 441.8 | 0.5618 | 1.777 | 604682 (7.82 %) |
| Temperature_F | -89 | 12 | 64 | 96 | 207 | 61.66 | 19.01 | 176 (0.00 %) |
| Wind_Chill_F | -89 | -2 | 62 | 96 | 207 | 58.25 | 22.39 | 59 (0.00 %) |
| Humidity_pct | 1 | 12 | 67 | 100 | 100 | 64.83 | 22.82 | 0 (0.00 %) |
| Pressure_in | 0 | 24.54 | 29.86 | 30.44 | 58.63 | 29.54 | 1.006 | 324283 (4.20 %) |
| Visibility_mi | 0 | 0.5 | 10 | 10 | 140 | 9.09 | 2.688 | 1481065 (19.16 %) |
| Wind_Speed_mph | 0 | 0 | 7 | 23 | 1087 | 7.685 | 5.425 | 20191 (0.26 %) |
| Precipitation_in | 0 | 0 | 0 | 0.18 | 36.47 | 0.008407 | 0.1102 | 533090 (6.90 %) |
