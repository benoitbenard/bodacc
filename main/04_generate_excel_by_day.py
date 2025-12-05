#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Génère des classeurs Excel hebdomadaires à partir des annonces filtrées.

Le script lit les fichiers JSONL produits par ``03_filter_BODACC_by_day.py``
et regroupe les annonces par semaine ISO à partir de la date figurant dans le
nom du fichier (``YYYYMMDD_bodacc_filtered.jsonl``). Pour chaque semaine,
il crée un fichier Excel dans ``output_dir`` dont le nom commence par
``<année>-W<semaine>`` et contenant une feuille ``BODACC`` avec les colonnes
spécifiées dans la demande métier.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import traceback
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Sequence

import pandas as pd

from bodacc.utils.utils_get_directories import get_output_dir
from bodacc.utils.utils_load_config_ini import charger_configuration
from bodacc.utils.utils_logging import initialiser_logging

COLUMN_MAP: List[tuple[str, Sequence[str]]] = [
    ("ID ANNONCE", ["id"]),
    ("NUMERO ANNONCE", ["numeroannonce"]),
    ("DATE PARUTION", ["dateparution"]),
    ("SIREN", ["SIREN"]),
    ("MATRICULE_CCPMA", ["MATRICULE_CCPMA", "MATRICULE_PICRIS_CCPMA"]),
    ("MATRICULE_CPCEA", ["MATRICULE_CPCEA", "MATRICULE_PICRIS_CPCEA"]),
    ("MATRICULE_AGRI", ["MATRICULE_AGRI", "MATRICULE_PICRIS_AGRI"]),
    ("TYPE AVIS", ["typeavis_lib"]),
    ("FAMILLE AVIS", ["familleavis_lib"]),
    ("TYPE JUGEMENT", ["jugement/type"]),
    ("FAMILLE JUGEMENT", ["jugement/famille"]),
    ("NATURE JUGEMENT", ["jugement/nature"]),
    ("DATE JUGEMENT", ["jugement/date"]),
    ("COMPLEMENT JUGEMENT", ["jugement/complementJugement"]),
    ("NOM", ["listepersonnes/personne/nom"]),
    ("PRENOM", ["listepersonnes/personne/prenom"]),
    ("URL", ["url_complete"]),
]


def _load_bodacc_jsonl(path: Path) -> List[Dict]:
    records: List[Dict] = []
    with path.open("r", encoding="utf-8") as stream:
        for line in stream:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                logging.warning("Ligne JSON invalide ignorée dans %s", path)
    return records


def _parse_day_from_filename(path: Path) -> datetime:
    try:
        day_str = path.stem.split("_")[0]
        return datetime.strptime(day_str, "%Y%m%d")
    except Exception as exc:  # noqa: BLE001
        raise ValueError(f"Nom de fichier inattendu : {path.name}") from exc


def _collect_weekly_files(filtered_dir: Path) -> Dict[str, List[Path]]:
    weekly_files: Dict[str, List[Path]] = {}
    for jsonl_path in sorted(filtered_dir.glob("*_bodacc_filtered.jsonl")):
        day = _parse_day_from_filename(jsonl_path)
        iso_year, iso_week, _ = day.isocalendar()
        key = f"{iso_year}-W{iso_week:02d}"
        weekly_files.setdefault(key, []).append(jsonl_path)
    return weekly_files


def _collect_values(obj, parts: Sequence[str]) -> List[str]:
    if not parts:
        if obj is None:
            return []
        if isinstance(obj, str):
            return [obj]
        return [str(obj)]

    head, *tail = parts
    if isinstance(obj, list):
        values: List[str] = []
        for item in obj:
            values.extend(_collect_values(item, parts))
        return values

    if isinstance(obj, dict):
        return _collect_values(obj.get(head), tail)

    if isinstance(obj, str):
        # Certaines clés (ex. "jugement", "listepersonnes") sont stockées sous
        # forme de chaîne JSON. On tente un décodage pour accéder aux champs
        # imbriqués.
        try:
            loaded = json.loads(obj)
        except json.JSONDecodeError:
            return []
        return _collect_values(loaded, parts)

    return []


def _extract_field(record: Dict, candidate_paths: Sequence[str]) -> str:
    for path in candidate_paths:
        parts = path.split("/")
        values = _collect_values(record, parts)
        cleaned = [value for value in values if str(value).strip()]
        if cleaned:
            # On dédoublonne en conservant l'ordre d'apparition
            unique_values = list(dict.fromkeys(cleaned))
            return " ; ".join(unique_values)
    return ""


def _build_row(record: Dict) -> Dict[str, str]:
    return {column: _extract_field(record, paths) for column, paths in COLUMN_MAP}


def _ensure_filtered_dir(config) -> Path:
    output_dir = Path(get_output_dir(config))
    filtered_dirname = config["directories"].get("FILTERED_OUTPUT_DIR", "bodacc_filtered_by_day").strip()
    filtered_dir = output_dir / filtered_dirname
    if not filtered_dir.exists() or not filtered_dir.is_dir():
        raise FileNotFoundError(
            f"Répertoire des fichiers filtrés introuvable : {filtered_dir}. "
            "Exécutez d'abord 03_filter_BODACC_by_day pour générer les JSONL."
        )
    return filtered_dir


def _generate_week_excel(week: str, files: List[Path], target_dir: Path) -> None:
    rows: List[Dict[str, str]] = []
    for file in files:
        for record in _load_bodacc_jsonl(file):
            rows.append(_build_row(record))

    df = pd.DataFrame(rows, columns=[col for col, _ in COLUMN_MAP])
    target_path = target_dir / f"{week}_BODACC_DDJC.xlsx"
    with pd.ExcelWriter(target_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="BODACC")
    logging.info("Classeur généré : %s (%d lignes)", target_path, len(df))


def main():
    parser = argparse.ArgumentParser(description="Génération d'Excel hebdomadaire BODACC")
    parser.add_argument(
        "--key",
        type=str,
        help="Clé Fernet pour déchiffrer le fichier config.ini (optionnelle)",
    )
    parser.add_argument(
        "--config",
        type=str,
        help="Chemin vers le fichier config.ini (optionnel)",
    )
    args = parser.parse_args()

    config = charger_configuration(config=args.config, key=args.key)
    initialiser_logging(config, log_name="generate_excel_by_day")

    logging.info("===== Début de la génération des Excel hebdomadaires BODACC =====")

    try:
        filtered_dir = _ensure_filtered_dir(config)
        output_dir = Path(get_output_dir(config))

        weekly_files = _collect_weekly_files(filtered_dir)
        if not weekly_files:
            raise FileNotFoundError(
                "Aucun fichier *_bodacc_filtered.jsonl trouvé. "
                "Vérifiez le répertoire ou exécutez l'étape 03."
            )

        for week, files in sorted(weekly_files.items()):
            _generate_week_excel(week, files, output_dir)

    except Exception:
        logging.error("Erreur critique pendant la génération des Excel BODACC.")
        logging.error(traceback.format_exc())
        sys.exit(1)

    logging.info("===== Fin de la génération des Excel hebdomadaires BODACC =====")


if __name__ == "__main__":
    main()
