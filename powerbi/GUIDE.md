# Tableau de bord Power BI (`powerbi/DataQuality.pbix`)

Ouvrir `DataQuality.pbix` (ou sa version projet `DataQuality.pbip`) dans Power BI Desktop, puis **Accueil > Actualiser**.

- Les tables sont lues **directement sur GitHub** (`data/metrics/` du dépôt public) : l'actualisation fonctionne
  quel que soit l'endroit où le dépôt est cloné. À la première actualisation, choisir l'accès **Anonyme**.
- Pour lire un dossier local à la place (par exemple un nouveau run non poussé) : **Transformer les données >
  Modifier les paramètres > SourceMetriques**, puis indiquer le chemin du dossier, ex. `D:igdata-dq\data\metrics\`.

Ce document décrit la construction du tableau de bord : chargement des tables, modèle, mesures DAX et pages.
Les exemples de la section 1 utilisent un dossier local (paramètre `MetricsFolder`) ; dans le fichier livré, ce
paramètre s'appelle `SourceMetriques` et pointe vers GitHub.

## 1. Paramètre et chargement des tables (Power Query)

1. **Accueil > Transformer les données > Gérer les paramètres > Nouveau** : `MetricsFolder`, type Texte,
   valeur `D:\bigdata-dq\data\metrics\`.
2. Pour chaque table ci-dessous : **Nouvelle source > Requête vide > Éditeur avancé**, coller le code, renommer la requête.

```m
// dim_batch
let
    Source = Csv.Document(File.Contents(MetricsFolder & "dim_batch.csv"), [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    H = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    T = Table.TransformColumnTypes(H, {{"batch_id", Int64.Type}, {"rows", Int64.Type}, {"duration_s", type number},
        {"global_score", type number}, {"score_completeness", type number}, {"score_uniqueness", type number},
        {"score_validity", type number}, {"score_consistency", type number}, {"score_conformity", type number},
        {"schema_changed", type logical}}, "en-US"),
    D = Table.AddColumn(T, "Date", each Date.FromText([batch_month] & "-01"), type date),
    K = Table.AddColumn(D, "BatchKey", each [run_id] & "|" & [batch_month], type text)
in K
```

Pour les tables de faits, même modèle avec la colonne `BatchKey` (`[run_id] & "|" & [batch_month]`) et les types :

| Requête | Fichier | Colonnes numériques (type number, culture "en-US") |
|---|---|---|
| `fact_rule_result` | `fact_rule_result.csv` | rows_checked, rows_failed (Int64), failure_rate, conformity_rate |
| `fact_column_completeness` | `fact_column_completeness.csv` | rows, missing (Int64), missing_rate, completeness_rate |
| `fact_drift` | `fact_drift.csv` | value, reference, threshold ; is_anomaly (logical) |
| `fact_alert` | `fact_alert.csv` | value, threshold, occurrences (Int64) |
| `dim_rule` | `dim_rule.csv` | max_fail_rate ; *pas de BatchKey* |
| `fact_run` | `fact_run.csv` | rows, total_s, load_s, rules_s, detectors_s, write_s, rows_per_s, peak_mem_mb, fraction ; *pas de BatchKey* |
| `fact_fault_eval` | `fact_fault_eval.csv` | new_alerts ; detected, expected_hit (logical) ; *pas de BatchKey* |
| `fact_detector_eval` | `fact_detector_eval.csv` | tp, fp, total, precision ; *pas de BatchKey* |

Important : toujours utiliser la culture **"en-US"** (point décimal) dans `Table.TransformColumnTypes`,
sinon un Windows en français lit `0.95` comme texte.

3. Créer une table `dim_run` : **Nouvelle source > Requête vide** :
```m
let S = Table.Distinct(Table.SelectColumns(dim_batch, {"run_id"})) in S
```

## 2. Modèle (vue Modèle)

| De (plusieurs) | Vers (un) | Colonne |
|---|---|---|
| fact_rule_result | dim_batch | BatchKey |
| fact_column_completeness | dim_batch | BatchKey |
| fact_drift | dim_batch | BatchKey |
| fact_alert | dim_batch | BatchKey |
| fact_rule_result | dim_rule | rule_id |
| dim_batch | dim_run | run_id |

Filtrage croisé : unidirectionnel (dimension → fait). Puis créer la table `_Mesures` et coller les mesures de
`powerbi/measures.dax`.

## 3. Thème et conventions

- Couleurs : sain `#34A853`, avertissement `#FBBC04`, critique `#EA4335`, neutre `#9AA0A6`, accent `#1A73E8`.
- Mêmes seuils partout : ≥ 95 sain, 85–95 avertissement, < 85 critique.
- Barre de filtres en haut de chaque page : segment **Run** (`dim_run[run_id]`, sélection unique, par défaut `faulty`),
  segment **Période** (`dim_batch[Date]`, entre), segment **Dimension** (`dim_rule[dimension]`).
  Synchroniser les segments sur toutes les pages (Affichage > Synchroniser les segments).
- Info-bulles : ajouter `dim_rule[description]` aux info-bulles des visuels de règles.

## 4. Pages

### Page 1 – Vue d'ensemble
- 4 cartes : `Enregistrements analysés`, `Score global` (couleur = `Couleur score`), `Alertes critiques`, `Statut dernier lot`.
- Jauge : `Score global`, min 0, max 100, cible 95.
- Graphique en courbes : axe `dim_batch[Date]`, valeur `Score global` ; lignes constantes 95 et 85 (volet Analytique).
- Tableau « Top 5 règles en échec » : `fact_rule_result[rule_id]`, `dim_rule[dimension]`, `Erreurs détectées`,
  `Taux de conformité` ; filtre Top N = 5 par `Erreurs détectées`.
- Histogramme empilé : `dim_batch[status]` par mois (nombre de lots).

### Page 2 – Complétude
- Barres horizontales : `fact_column_completeness[column]`, `Taux de complétude` (tri croissant, mise en forme
  conditionnelle : < 95 % rouge).
- Matrice (carte de chaleur) : lignes `column`, colonnes `dim_batch[Date]` (mois), valeurs `Taux de valeurs manquantes`
  avec mise en forme conditionnelle « échelle de couleurs ».
- Courbe : `Taux de valeurs manquantes` dans le temps pour la colonne sélectionnée.

### Page 3 – Règles et erreurs
- Histogramme empilé : `Erreurs (règles notées)` par `dim_rule[dimension]`.
- Tableau : rule_id, dimension, severity, description, `Lignes contrôlées`, `Erreurs détectées`, `Taux de conformité`,
  `Règles en échec`.
- Courbes : `Erreurs détectées` par mois, légende = dimension.
- 5 cartes : `Score complétude`, `Score unicité`, `Score validité`, `Score cohérence`, `Score conformité`.

### Page 4 – Anomalies et alertes
- Courbes : `Enregistrements analysés` par mois (détection de volume) + marqueurs : filtre visuel
  `fact_drift[detector] = volume` et `is_anomaly = vrai` en série séparée.
- Matrice : lignes `fact_drift[column]` (filtre detector = numeric_drift ou categorical_drift), colonnes mois,
  valeur `Valeur de dérive`, échelle de couleurs.
- Courbe : `Z-score volume` par mois, avec les mesures `Seuil volume haut` et `Seuil volume bas` (±5).
- Tableau des alertes : batch_month, severity, source, detector_or_rule, column, message, occurrences, planted_fault ;
  mise en forme conditionnelle de `severity`.
- Panneau « détecté / manqué » : tableau `fact_fault_eval` (batch_month, fault_type, expected, detected_by, detected)
  + cartes `Défauts injectés`, `Défauts détectés`, `Rappel (défauts détectés %)`.

### Page 5 – Performance
- Courbes E1 : `fact_run[rows]` (axe) / `Temps médian (s)`, légende `engine`, filtre experiment = E1.
- Barres E2 : `cores` / `Temps médian (s)`, filtre experiment = E2.
- Barres E3 : `input_format` / `Temps médian (s)` ; E4 : `mode` / `Temps médian (s)`.
- Cartes : `Débit médian (lignes/s)`, `Mémoire max médiane (Mo)`.
- Pour cette page, désactiver l'interaction du segment Run (les benchmarks ont leurs propres run_id).

## 5. Enregistrement

Fichier > Enregistrer sous `D:\bigdata-dq\powerbi\DataQuality.pbix`. Exporter aussi en PDF
(Fichier > Exporter > PDF) pour les captures du rapport.
