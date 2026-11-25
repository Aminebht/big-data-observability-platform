# Plateforme de surveillance de la qualité des données – US Accidents

Projet Big Data ICE5 2026-2027 – Med Amine Belhadj Taher, Zeineb Karoui.
Faculté des Sciences de Tunis – Université de Tunis El Manar.

Plateforme de *Data Observability* sur le jeu de données **US Accidents (2016–2023)** : 7,7 millions
d'enregistrements, 46 attributs. Elle profile les données, contrôle leur qualité avec 26 règles déclaratives,
détecte les anomalies et les dérives, traite les données en micro-batches mensuels avec **Spark Structured
Streaming**, génère des alertes et alimente un tableau de bord **Power BI**.

## Livrables

| Livrable | Fichier |
|---|---|
| Rapport | [report/Rapport_Projet_BigData.pdf](report/Rapport_Projet_BigData.pdf) (version Word : `.docx`) |
| Tableau de bord | [powerbi/DataQuality.pbix](powerbi/DataQuality.pbix) |
| Résultats chiffrés | [report/profiling_summary.md](report/profiling_summary.md), [report/evaluation_summary.md](report/evaluation_summary.md), [report/benchmark_summary.md](report/benchmark_summary.md) |

## Résultats principaux

- **Profilage** : 397 061 événements en double (5,1 %), 743 166 horodatages au format non standard (9,6 %),
  44 % de coordonnées de fin manquantes, valeurs physiquement impossibles (températures de −89 et 207 °F).
- **Évaluation** : sur 12 défauts injectés volontairement, **12 détectés**, chacun par le contrôle prévu ;
  précision des alertes de 96 %.
- **Performances** : Polars 3 à 5 fois plus rapide jusqu'à 1,9 M lignes puis hors mémoire ; Spark traite les
  7,7 M lignes en 44 s ; accélération ×2,8 de 1 à 4 cœurs ; Parquet 1,5 fois plus rapide que CSV ; environ 9 s
  par lot mensuel en streaming.

## Architecture

```
CSV Kaggle ──01_ingest──▶ data/lake/bronze (Parquet, colonnes texte, partitionné par mois)
                                │
                        03_make_batches ──▶ data/batches/clean/*.parquet
                                │           data/batches/faulty/*.parquet + fault_manifest.json
                                ▼
                    04_producer (dépôt d'un fichier par mois)
                                ▼
       data/batches/incoming/<run>/ ──▶ 05_stream (Spark Structured Streaming, foreachBatch)
                                         schéma ▸ règles (rules.yaml) ▸ score (scoring.yaml)
                                         ▸ détecteurs (detectors.yaml) ▸ alertes
                                ▼
                 data/metrics/*.csv (schéma en étoile) ──▶ Power BI (powerbi/DataQuality.pbix)
                 logs/alerts.log (JSON) + console
```

## Installation (Windows)

Prérequis : Java 17, Python 3.10, `winutils.exe` et `hadoop.dll` (Hadoop 3.3) dans `D:\hadoop\bin`
(ou un autre dossier indiqué par la variable d'environnement `HADOOP_HOME`).

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Données : télécharger [US Accidents (2016–2023)](https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents)
et placer `US_Accidents_March23.csv` dans `data/raw/`. Les données brutes et intermédiaires (≈ 5 Go) ne sont pas
versionnées ; seuls les résultats (`data/metrics/`) et le manifeste des défauts injectés le sont.

## Exécution

| Étape | Commande | Durée indicative |
|---|---|---|
| Ingestion CSV → Parquet | `.\.venv\Scripts\python.exe scripts\01_ingest.py` | 4 min |
| Partie 1 – Profilage | `.\.venv\Scripts\python.exe scripts\02_profile.py` | 11 min |
| Lots mensuels + défauts injectés | `.\.venv\Scripts\python.exe scripts\03_make_batches.py` | 3 min |
| Surveillance en continu | `powershell -File scripts\run_monitoring.ps1 -Run faulty` puis `-Run clean` | 15 min chacun |
| Évaluation sur défauts injectés | `.\.venv\Scripts\python.exe scripts\07_evaluate.py` | quelques secondes |
| Expériences E1–E4 | `powershell -File scripts\run_benchmarks.ps1` | ≈ 1 h |
| Graphiques | `.\.venv\Scripts\python.exe scripts\08_charts.py` | quelques secondes |
| Tableau de bord | ouvrir `powerbi\DataQuality.pbix` dans Power BI Desktop, puis Actualiser | 1 min |

### Tableau de bord Power BI

Ouvrir `powerbi\DataQuality.pbix` puis **Accueil > Actualiser** : les tables de résultats sont lues directement
dans ce dépôt GitHub (`data/metrics/`), sans configuration, quel que soit l'emplacement du clone (choisir l'accès
**Anonyme** à la première actualisation). Pour visualiser un run local non poussé, changer le paramètre
`SourceMetriques` (Transformer les données > Modifier les paramètres) en un chemin de dossier. Détails :
[powerbi/GUIDE.md](powerbi/GUIDE.md).

### Surveillance en direct dans deux terminaux

```powershell
.\.venv\Scripts\python.exe scripts\05_stream.py --run faulty
.\.venv\Scripts\python.exe scripts\04_producer.py --run faulty --delay 3
```

Test de parité Spark / Polars : `.\.venv\Scripts\python.exe tests\test_parity.py`.

## Arborescence

| Dossier | Contenu |
|---|---|
| `config/` | `rules.yaml` (26 règles), `scoring.yaml` (score global), `detectors.yaml` (détecteurs), `drift_bins.json` |
| `src/dq/` | schéma et typage, moteur de règles Spark et Polars, score, détecteurs, pipeline par lot, métriques et alertes |
| `scripts/` | étapes numérotées du pipeline et des expériences |
| `tests/` | test de parité Spark / Polars |
| `data/metrics/` | résultats : tables CSV en schéma en étoile lues par Power BI |
| `powerbi/` | tableau de bord (`.pbix`), version projet Power BI (`.pbip`), mesures DAX et guide |
| `report/` | rapport, figures et résumés chiffrés |
