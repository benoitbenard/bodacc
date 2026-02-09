#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Récupération des annonces BODACC jour par jour via l'API officielle.
Le fichier JSON et le CSV temporaires sont définis dans ``config.ini`` via
la section ``[bodacc_files]``.
"""

import argparse
import datetime as dt
import json
import logging
import os
from pathlib import Path
import sys
import time
import traceback
from typing import Dict, Iterable, List, Optional

import pandas as pd
import requests

from utils.utils_get_directories import get_tmp_dir
from utils.utils_load_config_ini import charger_configuration
from utils.utils_logging import initialiser_logging

PUBLICATION_TYPES = ("A", "B", "C")


def _parse_date(date_str: str) -> dt.date:
    return dt.datetime.strptime(date_str, "%Y-%m-%d").date()


def _date_range(start: dt.date, end: dt.date) -> Iterable[dt.date]:
    current = start
    while current <= end:
        yield current
        current += dt.timedelta(days=1)


def _prepare_session(config) -> requests.Session:
    session = requests.Session()

    if "proxy" in config:
        proxy_section = config["proxy"]
        proxy_url = proxy_section.get("url")
        if proxy_url:
            user = proxy_section.get("user")
            password = proxy_section.get("password")
            if user and password:
                session.proxies = {
                    "http": f"http://{user}:{password}@{proxy_url}",
                    "https": f"http://{user}:{password}@{proxy_url}",
                }
            else:
                session.proxies = {
                    "http": f"http://{proxy_url}",
                    "https": f"http://{proxy_url}",
                }
    return session


def _extract_records(payload: Dict) -> List[Dict]:
    if not payload:
        return []

    if "results" in payload and isinstance(payload["results"], list):
        records = payload["results"]
        cleaned = []
        for item in records:
            if isinstance(item, dict):
                if "record" in item and isinstance(item["record"], dict):
                    cleaned.append(item["record"])
                elif "fields" in item and isinstance(item["fields"], dict):
                    cleaned.append(item["fields"])
                else:
                    cleaned.append(item)
        return cleaned

    if "records" in payload and isinstance(payload["records"], list):
        return payload["records"]

    return []


def _write_ndjson_part(
    tmp_dir: str, day: dt.date, publicationavis: str, page_idx: int, records: List[Dict]
) -> None:
    os.makedirs(tmp_dir, exist_ok=True)
    part_path = os.path.join(
        tmp_dir, f"{day:%Y%m%d}_bodacc_update_part_{publicationavis}_{page_idx:03d}.jsonl"
    )
    with open(part_path, "w", encoding="utf-8") as part_file:
        for record in records:
            part_file.write(json.dumps(record, ensure_ascii=False) + "\n")


def _cleanup_day_parts(tmp_dir: str, day: dt.date) -> None:
    prefix = f"{day:%Y%m%d}_bodacc_update_part_"
    for path in Path(tmp_dir).glob(f"{prefix}*.jsonl"):
        try:
            path.unlink()
        except Exception as exc:  # noqa: BLE001
            logging.warning("Impossible de supprimer le fragment %s : %s", path, exc)


def _merge_day_parts(tmp_dir: str, day: dt.date, daily_dir: str) -> Optional[str]:
    os.makedirs(daily_dir, exist_ok=True)
    part_files = sorted(Path(tmp_dir).glob(f"{day:%Y%m%d}_bodacc_update_part_*.jsonl"))
    if not part_files:
        target_path = os.path.join(daily_dir, f"{day:%Y%m%d}_bodacc_update.jsonl")
        Path(target_path).touch()
        logging.info("Aucun fragment pour le %s : fichier NDJSON vide créé (%s)", day, target_path)
        return target_path

    target_path = os.path.join(daily_dir, f"{day:%Y%m%d}_bodacc_update.jsonl")
    with open(target_path, "w", encoding="utf-8") as merged:
        for part in part_files:
            merged.write(part.read_text(encoding="utf-8"))

    for part in part_files:
        try:
            part.unlink()
        except Exception as exc:  # noqa: BLE001
            logging.warning("Impossible de supprimer le fragment %s après fusion : %s", part, exc)

    logging.info("Fichier quotidien fusionné généré : %s", target_path)
    return target_path


def _fetch_day(
    session: requests.Session,
    api_url: str,
    date_value: dt.date,
    publicationavis: str,
    per_page: int,
    max_retries: int,
    backoff_base: float,
    too_many_requests_timeout_sec: int,
    cert_file: Optional[str],
    tmp_dir: str,
) -> List[Dict]:
    all_records: List[Dict] = []
    last_numero = 0
    page_idx = 0

    while True:
        params = {
            "refine": f"dateparution:{date_value:%Y-%m-%d}",
            "where": f"publicationavis = '{publicationavis}' AND numeroannonce > {last_numero}",
            "order_by": "numeroannonce",
            "limit": per_page,
        }

        for attempt in range(max_retries):
            try:
                response = session.get(
                    api_url,
                    params=params,
                    timeout=60,
                    verify=cert_file if cert_file else True,
                )
                if response.status_code == 429:
                    sleep_time = too_many_requests_timeout_sec
                    logging.warning(
                        "429 Too Many Requests reçu, pause de %ss", sleep_time
                    )
                    time.sleep(sleep_time)
                    continue

                response.raise_for_status()
                payload = response.json()
                records = _extract_records(payload)
                all_records.extend(records)

                logging.info(
                    "Date %s — dernier numero %s : %s enregistrements",
                    date_value,
                    last_numero,
                    len(records),
                )

                if not records:
                    return all_records

                page_idx += 1
                _write_ndjson_part(tmp_dir, date_value, publicationavis, page_idx, records)

                numero_values = [
                    r.get("numeroannonce")
                    for r in records
                    if isinstance(r, dict) and r.get("numeroannonce") is not None
                ]
                if numero_values:
                    last_numero = max(max(numero_values), last_numero)

                if len(records) < per_page:
                    return all_records
                break
            except Exception as exc:  # noqa: BLE001
                wait_time = backoff_base * (2**attempt)
                logging.warning(
                    "Erreur API BODACC (tentative %s/%s) : %s — nouvelle tentative dans %.1fs",
                    attempt + 1,
                    max_retries,
                    exc,
                    wait_time,
                )
                time.sleep(wait_time)
        else:
            logging.error(
                "Abandon des appels API pour la date %s après %s tentatives",
                date_value,
                max_retries,
            )
            break

    return all_records


def _write_tmp_outputs(records: List[Dict], tmp_dir: str, bodacc_files) -> None:
    os.makedirs(tmp_dir, exist_ok=True)

    tmp_json_base = bodacc_files.get("TMP_JSON", "TMP_resultats_bodacc").strip()
    tmp_csv_base = bodacc_files.get("TMP_CSV", "TMP_resume_bodacc").strip()

    tmp_json_path = os.path.join(tmp_dir, f"{tmp_json_base}.json")
    tmp_csv_path = os.path.join(tmp_dir, f"{tmp_csv_base}.csv")

    with open(tmp_json_path, "w", encoding="utf-8") as f_json:
        json.dump(records, f_json, ensure_ascii=False, indent=2)

    df = pd.json_normalize(records)
    df.to_csv(tmp_csv_path, sep=";", index=False, encoding="utf-8-sig")
    logging.info("Fichiers temporaires consolidés : %s et %s", tmp_json_path, tmp_csv_path)


def main():
    parser = argparse.ArgumentParser(description="Récupération BODACC par jour")
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
        "--start-date",
        type=str,
        help="Date de début (YYYY-MM-DD). Par défaut : aujourd'hui - DEFAULT_DAYS_DEPTH",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="Date de fin (YYYY-MM-DD). Par défaut : aujourd'hui",
    )
    args = parser.parse_args()

    config = charger_configuration(config=args.config, key=args.key)
    initialiser_logging(config, log_name="get_bodacc_by_day")

    logging.info("===== Début de la récupération BODACC =====")

    try:
        general = config["general"] if "general" in config else {}
        bodacc_files = config["bodacc_files"] if "bodacc_files" in config else {}

        per_page = int(general.get("per_page", 100))
        max_retries = int(general.get("max_retries", 5))
        backoff_base = float(general.get("backoff_base", 1))
        timeout_429 = int(general.get("too_many_requests_timeout_sec", 300))
        default_depth = int(general.get("default_days_depth", 7))
        api_url = general.get("api_url")
        cert_file = general.get("cert_file")

        if not api_url:
            raise ValueError("URL API BODACC manquante dans [general] API_URL")

        today = dt.date.today()
        yesterday = today - dt.timedelta(days=1)

        start_date = (
            _parse_date(args.start_date)
            if args.start_date
            else yesterday - dt.timedelta(days=default_depth - 1)
        )
        end_date = _parse_date(args.end_date) if args.end_date else yesterday
        if end_date >= today:
            logging.info(
                "Date de fin (%s) >= aujourd’hui (%s) → ajustement à J-1",
                end_date,
                today,
            )
            end_date = yesterday        

        if start_date > end_date:
            raise ValueError("La date de début est postérieure à la date de fin")

        main_dir = config["directories"]["MAIN_DIR"].strip()
        output_dir_name = config["directories"]["OUTPUT_DIR"].strip()
        daily_output_dir_name = config["directories"].get("DAILY_OUTPUT_DIR", "bodacc_by_day").strip()

        daily_output_dir = os.path.join(main_dir, output_dir_name, daily_output_dir_name)
        tmp_dir = get_tmp_dir(config)

        session = _prepare_session(config)

        all_records: List[Dict] = []
        for day in _date_range(start_date, end_date):
            logging.info("Récupération des annonces pour %s", day.isoformat())
            combined_records: List[Dict] = []

            target_daily_file = os.path.join(daily_output_dir, f"{day:%Y%m%d}_bodacc_update.jsonl")
            if os.path.exists(target_daily_file):
                logging.info(
                    "Fichier déjà présent pour %s (%s), aucune nouvelle récupération.",
                    day,
                    target_daily_file,
                )
                continue

            _cleanup_day_parts(tmp_dir, day)

            for publicationavis in PUBLICATION_TYPES:
                logging.info("Publicationavis en cours : %s", publicationavis)
                records = _fetch_day(
                    session,
                    api_url,
                    day,
                    publicationavis,
                    per_page,
                    max_retries,
                    backoff_base,
                    timeout_429,
                    cert_file,
                    tmp_dir,
                )
                combined_records.extend(records)

            _merge_day_parts(tmp_dir, day, daily_output_dir)
            all_records.extend(combined_records)

        if all_records:
            _write_tmp_outputs(all_records, tmp_dir, bodacc_files)

    except Exception:
        logging.error("Erreur critique pendant la récupération BODACC.")
        logging.error(traceback.format_exc())
        sys.exit(1)

    logging.info("===== Fin de la récupération BODACC =====")


if __name__ == "__main__":
    main()
