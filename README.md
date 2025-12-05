# Pipeline BODACC

Ce dépôt orchestre l'extraction quotidienne des annonces BODACC et leur filtrage à partir d'une liste de SIREN issue de Semarchy MDM. Le traitement complet repose sur trois scripts Python exécutables indépendamment ou via le batch Windows `run_pipeline.bat`.

## 📂 Structure principale

- `01_get_SIREN_from_SEMARCHY_MDM.py` : extrait les SIREN/SIRET depuis Semarchy MDM et génère le CSV source des identifiants.
- `02_get_BODACC_by_day.py` : interroge l'API BODACC jour par jour, écrit les fragments NDJSON par publicationavis, puis fusionne un fichier `YYYYMMDD_bodacc_update.jsonl` par jour dans le répertoire d'output.
- `03_filter_BODACC_by_day.py` : lit les fichiers journaliers produits par `02`, recherche les SIREN présents dans `registre`, applique la logique `topage_DDJC` et écrit un fichier filtré `YYYYMMDD_bodacc_filtered.jsonl` par jour.
- `run_pipeline.bat` : enchaîne les trois scripts dans l'ordre 01 → 02 → 03.
- `base_dir/config/config.ini` : exemple de configuration (chemins et options API/proxy).

## ⚙️ Configuration

Les scripts utilisent le même fichier `config.ini` (passé via `--config`, sinon `base_dir/config/config.ini` par défaut) avec notamment :

- `[directories]`
  - `MAIN_DIR` : racine des répertoires générés.
  - `TMP_DIR` : sous-répertoire pour les fichiers intermédiaires (CSV SIREN, exports consolidés temporaires).
  - `OUTPUT_DIR` : sous-répertoire principal des résultats.
  - `DAILY_OUTPUT_DIR` : sous-répertoire des fichiers journaliers BODACC (défaut : `bodacc_by_day`).
  - `FILTERED_OUTPUT_DIR` : sous-répertoire des fichiers filtrés (défaut : `bodacc_filtered_by_day`).
- `[general]` : paramètres API BODACC (`api_url`, `cert_file`, pagination, profondeur par défaut, etc.).
- `[bodacc_files]` / `[exports_files]` : noms des fichiers (`SIREN_FILENAME`, `TMP_JSON`, `TMP_CSV`, etc.).
- Sections proxy ou bases de données selon l'environnement (utilisées par `01` et `02`).

## ▶️ Exécution

### Lancer chaque script

```bat
python 01_get_SIREN_from_SEMARCHY_MDM.py --config base_dir\config\config.ini
python 02_get_BODACC_by_day.py --config base_dir\config\config.ini --start-date 2025-11-01 --end-date 2025-11-30
python 03_filter_BODACC_by_day.py --config base_dir\config\config.ini
```

### Pipeline complet

`run_pipeline.bat` exécute automatiquement les trois étapes. Un chemin de configuration peut être passé en argument :

```bat
run_pipeline.bat base_dir\config\config.ini
```

## 📦 Sorties attendues

- **01** : un CSV de SIREN/SIRET dans `<MAIN_DIR>/<TMP_DIR>/SIREN_FILENAME.csv`.
- **02** :
  - fichiers journaliers `YYYYMMDD_bodacc_update.jsonl` dans `<MAIN_DIR>/<OUTPUT_DIR>/<DAILY_OUTPUT_DIR>/` ;
  - consolidados temporaires `TMP_resultats_bodacc.json` et `TMP_resume_bodacc.csv` (noms configurables) dans `<MAIN_DIR>/<TMP_DIR>/` quand des annonces sont collectées.
- **03** : un fichier filtré par jour `YYYYMMDD_bodacc_filtered.jsonl` dans `<MAIN_DIR>/<OUTPUT_DIR>/<FILTERED_OUTPUT_DIR>/`, créé vide si aucune annonce n'est retenue pour marquer la journée comme traitée.

