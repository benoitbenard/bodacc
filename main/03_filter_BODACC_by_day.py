#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Filtre les annonces BODACC jour par jour à partir des exports quotidiens.

Ce script lit les fichiers NDJSON générés par ``02_get_BODACC_by_day.py`` et
construit, pour chaque date de parution rencontrée, un fichier NDJSON filtré
contenant uniquement les enregistrements correspondant aux SIREN connus et
marqués ``topage_DDJC = "oui"`` selon la logique du programme ``topage_DDJC``.

Pour chaque journée présente dans les sources :
- si un fichier cible existe déjà dans ``filtered_output_dir``, la journée est ignorée ;
- sinon, le fichier filtré est généré ;
- si aucun enregistrement n'est retenu, un fichier vide est créé pour marquer
  le jour comme traité.
"""

import argparse
import csv
import json
import logging
import os
import sys
import traceback
import unicodedata
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set

from bodacc.utils.utils_get_directories import get_output_dir, get_tmp_dir
from bodacc.utils.utils_load_config_ini import charger_configuration
from bodacc.utils.utils_logging import initialiser_logging


DEFAULT_TOPAGE_KEYWORDS = [
    "sauvegarde",
    "redressement judiciaire",
    "liquidation judiciaire",
    "radiation",
    "cloture pour insuffisance d'actifs",
]

TEXT_COLUMNS_CANDIDATES = [
    "texte",
    "text",
    "objet",
    "description",
    "resume",
]


def _get_section(config, section: str) -> dict:
    if hasattr(config, "__contains__") and section in config:
        return dict(config[section])
    return {}


def _read_keywords(config, option: str) -> List[str]:
    if "keywords" not in config:
        return []
    raw_value = config["keywords"].get(option, "")
    return [line.strip().lower() for line in raw_value.splitlines() if line.strip()]

def _load_sirens_with_info(path: str) -> Dict[str, Dict[str, str]]:
    """
    Charge un dictionnaire {SIREN: {matricules...}} à partir du fichier SIREN CSV.
    """
    if not os.path.exists(path):
        logging.warning("Fichier SIREN introuvable : %s", path)
        return {}

    data = {}
    try:
        with open(path, "r", encoding="utf-8-sig", newline="") as stream:
            reader = csv.DictReader(stream, delimiter=";")
            for row in reader:
                raw_value = (row.get("CODE_SIREN") or "").strip()
                siren = "".join(ch for ch in raw_value if ch.isdigit())

                if len(siren) != 9:
                    continue

                data[siren] = {
                    "MATRICULE_PICRIS_CCPMA": row.get("MATRICULE_PICRIS_CCPMA", "").strip(),
                    "MATRICULE_PICRIS_CPCEA": row.get("MATRICULE_PICRIS_CPCEA", "").strip(),
                    "MATRICULE_PICRIS_AGRI": row.get("MATRICULE_PICRIS_AGRI", "").strip(),
                }

    except Exception as exc:
        logging.error("❌ Erreur lecture fichier SIREN enrichi : %s", exc)
        return {}

    logging.info("%d SIREN enrichis chargés", len(data))
    return data


def _load_sirens(path: str) -> Set[str]:
    if not os.path.exists(path):
        logging.warning("Fichier SIREN introuvable : %s", path)
        return set()

    try:
        sirens: Set[str] = set()
        invalid: Set[str] = set()
        with open(path, "r", encoding="utf-8-sig", newline="") as stream:
            reader = csv.DictReader(stream, delimiter=";")
            if reader.fieldnames and "CODE_SIREN" in reader.fieldnames:
                for row in reader:
                    raw_value = (row.get("CODE_SIREN") or "").strip()
                    digits_only = "".join(ch for ch in raw_value if ch.isdigit())
                    if len(digits_only) == 9:
                        sirens.add(digits_only)
                    elif raw_value:
                        invalid.add(raw_value)
            else:
                # Fallback : première colonne libre interprétation
                stream.seek(0)
                stream_reader = csv.reader(stream, delimiter=";")
                for row in stream_reader:
                    if not row:
                        continue
                    raw_value = row[0].strip()
                    digits_only = "".join(ch for ch in raw_value if ch.isdigit())
                    if len(digits_only) == 9:
                        sirens.add(digits_only)
                    elif raw_value:
                        invalid.add(raw_value)

        logging.info("%d SIREN valides chargés depuis %s", len(sirens), path)
        if invalid:
            logging.warning("%d entrées SIREN invalides ignorées dans %s", len(invalid), path)
        return sirens
    except Exception as exc:
        logging.error("❌ Erreur lecture SIREN : %s", exc)
        return set()


def _load_bodacc_jsonl(path: Path) -> List[Dict]:
    if not path.exists():
        raise FileNotFoundError(f"Fichier BODACC introuvable : {path}")

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
                continue

    return records


def _clean_registre_values(values: Iterable[str]) -> List[str]:
    cleaned: List[str] = []
    for value in values:
        if not isinstance(value, str):
            continue
        digits_only = "".join(ch for ch in value if ch.isdigit())
        if digits_only:
            cleaned.append(digits_only)
    return cleaned


def _matched_sirens(record: Dict[str, object], sirens: Set[str]) -> List[str]:
    registre_values = record.get("registre")
    if not registre_values:
        return []

    if isinstance(registre_values, str):
        registre_list = [registre_values]
    elif isinstance(registre_values, list):
        registre_list = registre_values
    else:
        return []

    cleaned_values = _clean_registre_values(registre_list)
    return [value for value in cleaned_values if value in sirens]


def _normalize_text(text: str) -> str:
    text = text.lower()
    return "".join(c for c in unicodedata.normalize("NFD", text) if unicodedata.category(c) != "Mn")


def _deep_collect_text(obj) -> List[str]:
    texts: List[str] = []
    if isinstance(obj, dict):
        for value in obj.values():
            texts.extend(_deep_collect_text(value))
    elif isinstance(obj, list):
        for value in obj:
            texts.extend(_deep_collect_text(value))
    elif isinstance(obj, str):
        texts.append(obj)
        try:
            parsed = json.loads(obj)
            texts.extend(_deep_collect_text(parsed))
        except Exception:
            pass
    return texts


def _flag_keywords(text: str, keywords: Iterable[str]) -> bool:
    if not text:
        return False
    content = _normalize_text(text)
    return any(keyword in content for keyword in keywords)


def _should_tag_topage(record: Dict[str, object], keywords: List[str]) -> bool:
    if not keywords:
        return False

    text_parts: List[str] = []
    for field in TEXT_COLUMNS_CANDIDATES + ["familleavis_lib", "typeavis_lib", "jugement", "modificationsgenerales", "divers"]:
        if field in record:
            text_parts.extend(_deep_collect_text(record[field]))

    return _flag_keywords(" ".join(text_parts), keywords)


def _parse_day(value: str) -> Optional[str]:
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y%m%d"):
        try:
            return datetime.strptime(value, fmt).strftime("%Y%m%d")
        except Exception:
            continue
    return None


def _existing_days(output_dir: Path) -> Set[str]:
    existing: Set[str] = set()
    for file in output_dir.glob("*_bodacc_filtered.jsonl"):
        name = file.stem
        if len(name) >= 8:
            existing.add(name[:8])
    return existing


def _discover_input_files(base_output_dir: Path, explicit: List[str] | None = None) -> List[Path]:
    if explicit:
        paths = [Path(p) for p in explicit]
    else:
        paths = sorted(base_output_dir.glob("*_bodacc_update.jsonl"))

    return [path for path in paths if path.exists() and path.is_file()]


def _write_day_file(day_str: str, records: List[str], output_dir: Path) -> None:
    target = output_dir / f"{day_str}_bodacc_filtered.jsonl"
    if target.exists():
        logging.info("Fichier déjà présent, aucune régénération : %s", target)
        return

    with target.open("w", encoding="utf-8") as stream:
        if records:
            stream.write("\n".join(records) + "\n")
    logging.info("Écriture %s (%d enregistrements)", target, len(records))


def _filter_records(
    input_files: List[Path],
    sirens: Set[str],
    target_dir: Path,
    keywords: List[str],
    sirens_info: Dict[str, Dict[str, str]],
) -> None:
    if not input_files:
        logging.warning("Aucun fichier source trouvé à filtrer.")
        return

    existing = _existing_days(target_dir)
    logging.info("Jours déjà traités détectés : %s", ", ".join(sorted(existing)) if existing else "aucun")

    total_processed = 0
    total_kept = 0

    for input_file in input_files:
        day_from_name: Optional[str] = None
        name_parts = input_file.stem.split("_")
        if name_parts and name_parts[0].isdigit() and len(name_parts[0]) == 8:
            day_from_name = name_parts[0]

        target_day = day_from_name or None
        if target_day and target_day in existing:
            logging.info(
                "Fichier filtré déjà présent pour %s, passage : %s",
                target_day,
                input_file,
            )
            continue

        logging.info("Traitement de %s", input_file)
        kept_per_day: Dict[str, List[str]] = defaultdict(list)
        seen_per_day: Set[str] = set()

        try:
            for record in _load_bodacc_jsonl(input_file):
                total_processed += 1

                date_parution = record.get("dateparution")
                if not isinstance(date_parution, str):
                    continue

                day_str = _parse_day(date_parution)
                if not day_str:
                    continue

                if day_str in existing:
                    continue

                seen_per_day.add(day_str)

                matches = _matched_sirens(record, sirens)
                if matches:
                    logging.debug("SIREN trouvé : %s", ", ".join(matches))
                else:
                    continue

                if not _should_tag_topage(record, keywords):
                    continue

                record["topage_DDJC"] = "oui"
                # Enrichissement avec les matricules PICRIS
                for siren in matches:
                    info = sirens_info.get(siren)
                    if info:
                        record["MATRICULE_PICRIS_CCPMA"] = info.get("MATRICULE_PICRIS_CCPMA")
                        record["MATRICULE_PICRIS_CPCEA"] = info.get("MATRICULE_PICRIS_CPCEA")
                        record["MATRICULE_PICRIS_AGRI"] = info.get("MATRICULE_PICRIS_AGRI")
                        break  # si plusieurs SIREN matchent, on prend le premier
                                    
                kept_per_day[day_str].append(json.dumps(record, ensure_ascii=False))
                total_kept += 1
        except Exception as exc:
            logging.error("❌ Erreur lors de la lecture de %s : %s", input_file, exc)
            continue

        for day_str in sorted(seen_per_day):
            _write_day_file(day_str, kept_per_day.get(day_str, []), target_dir)
            existing.add(day_str)

    logging.info("%s lignes lues au total, %s enregistrements retenus.", total_processed, total_kept)


def main():
    parser = argparse.ArgumentParser(description="Filtrage BODACC par SIREN et mots-clés")
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
    parser.add_argument(
        "--input-jsonl",
        nargs="*",
        help="Fichiers JSONL en entrée (par défaut : *_bodacc_update.jsonl dans daily_output_dir)",
    )
    args = parser.parse_args()

    config = charger_configuration(config=args.config, key=args.key)
    initialiser_logging(config, log_name="filter_bodacc_by_day")

    logging.info("===== Début du filtrage BODACC =====")

    try:
        bodacc_files = _get_section(config, "bodacc_files")
        exports_files = _get_section(config, "exports_files")

        tmp_dir = get_tmp_dir(config)
        output_dir = get_output_dir(config)

        daily_dir_name = config["directories"].get("DAILY_OUTPUT_DIR", "bodacc_by_day").strip()
        daily_dir = Path(output_dir) / daily_dir_name

        siren_base = exports_files.get("SIREN_FILENAME", bodacc_files.get("SIREN_FILENAME", "siren")).strip()
        siren_path = Path(tmp_dir) / f"{siren_base}.csv"

        input_files = _discover_input_files(daily_dir, args.input_jsonl)
        if not input_files:
            raise FileNotFoundError(
                "Aucun fichier BODACC trouvé. Spécifiez --input-jsonl ou générez un fichier via 02_get_BODACC_by_day."
            )
        logging.info("Fichiers BODACC chargés : %s", ", ".join(str(p) for p in input_files))

        sirens_info = _load_sirens_with_info(str(siren_path))
        sirens = set(sirens_info.keys())
        if not sirens:
            raise ValueError("Aucun SIREN valide chargé : arrêt du programme.")

        topage_keywords = _read_keywords(config, "topage_keywords") or DEFAULT_TOPAGE_KEYWORDS

        target_dir = Path(output_dir) / config["directories"].get("FILTERED_OUTPUT_DIR", "bodacc_filtered_by_day").strip()
        target_dir.mkdir(parents=True, exist_ok=True)

        _filter_records(input_files, sirens, target_dir, topage_keywords, sirens_info)


    except Exception:
        logging.error("Erreur critique pendant le filtrage BODACC.")
        logging.error(traceback.format_exc())
        sys.exit(1)

    logging.info("===== Fin du filtrage BODACC =====")


if __name__ == "__main__":
    main()
