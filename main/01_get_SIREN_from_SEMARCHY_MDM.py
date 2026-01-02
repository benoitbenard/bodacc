#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Extraction des codes SIREN depuis Semarchy MDM.
Le nom du fichier CSV peut être défini dans ``config.ini`` via la clé
``[exports_files] SIREN_FILENAME``.
"""

import os
import sys
import logging
import traceback
import argparse

from utils.utils_cnx_oracle import connecter_a_oracle, lire_oracle_dataframe
from utils.utils_cnx_csv import exporter_dataframe_csv
from utils.utils_get_directories import get_tmp_dir
from utils.utils_load_config_ini import charger_configuration
from utils.utils_logging import initialiser_logging


QUERY_EXPORT = """
    SELECT
        e.id AS EMETTEUR_ID,
        e.code_siren AS CODE_SIREN,
        e.code_siret AS CODE_SIRET,
        e.matricule_picris_ccpma as MATRICULE_PICRIS_CCPMA,
        e.matricule_picris_cpcea as MATRICULE_PICRIS_CPCEA,
        e.matricule_picris_agri as MATRICULE_PICRIS_AGRI
    FROM semarchy_mdm.gd_etablissement e
    inner join semarchy_mdm.USR_ETABLISSEMENT_COUVERTURE couv on couv.F_ETABLISSEMENT = e.ID AND (couv.FCLI_AGRI > 0 OR couv.FCLI_CPCEA > 0 OR couv.FCLI_CCPMA > 0)
    WHERE e.code_siren IS NOT NULL
    AND   e.b_error_status is null
"""


def main():
    parser = argparse.ArgumentParser(description="Programme d'extraction SIREN Semarchy MDM")
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

    # Initialiser logging
    initialiser_logging(config, log_name="get_siren_semarchy_mdm")

    logging.info("===== Début de l'extraction SIREN Semarchy MDM =====")

    try:
        # Connexion Oracle
        conn = connecter_a_oracle(config, section_name="oracle_semarchy_mdm")

        # Exécution requête
        df = lire_oracle_dataframe(conn, QUERY_EXPORT)

        # Répertoire TMP
        tmp_dir = get_tmp_dir(config)

        # Nom de fichier : priorise la clé siren_filename configurée
        # (section ``bodacc_files``), avec repli éventuel sur l'ancienne
        # section ``exports_files`` ou un nom par défaut.
        bodacc_files = config["bodacc_files"] if "bodacc_files" in config else {}
        exports_files = config["exports_files"] if "exports_files" in config else {}

        nom_base = (
            bodacc_files.get("SIREN_FILENAME")
            or exports_files.get("SIREN_FILENAME")
            or "01_get_SIREN_from_SEMARCHY_MDM"
        )
        nom_base = nom_base.strip()
        fichier_csv = f"{nom_base}.csv"

        chemin_sortie = os.path.join(tmp_dir, fichier_csv)
        logging.info(f"Fichier de sortie : {chemin_sortie}")

        exporter_dataframe_csv(df, chemin_sortie)

    except Exception:
        logging.error("Erreur critique pendant l'extraction SIREN.")
        logging.error(traceback.format_exc())
        sys.exit(1)

    finally:
        if "conn" in locals() and conn:
            conn.close()
            logging.info("Connexion Oracle fermée.")

    logging.info("===== Fin de l'extraction SIREN Semarchy MDM =====")


if __name__ == "__main__":
    main()
