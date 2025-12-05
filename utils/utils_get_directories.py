"""afterdata.utils.utils_get_directories

Utilitaires de gestion des répertoires (TMP, OUTPUT) et fonctions d'horodatage
pour les exports AfterData.
"""

import os
from datetime import datetime


def get_tmp_dir(config):
    """Retourne le répertoire temporaire défini dans ``config.ini`` en le créant si besoin."""

    main_dir = config["directories"]["MAIN_DIR"].strip()
    tmp_dir_name = config["directories"]["TMP_DIR"].strip()

    tmp_dir = os.path.join(main_dir, tmp_dir_name)
    os.makedirs(tmp_dir, exist_ok=True)
    return tmp_dir


def get_output_dir(config):
    """Retourne le répertoire OUTPUT défini dans ``config.ini`` en le créant si nécessaire."""

    main_dir = config["directories"]["MAIN_DIR"].strip()
    output_dir_name = config["directories"]["OUTPUT_DIR"].strip()

    output_dir = os.path.join(main_dir, output_dir_name)
    os.makedirs(output_dir, exist_ok=True)
    return output_dir


def horodatage():
    """Horodatage compact utilisé pour préfixer fichiers et journaux."""

    return datetime.now().strftime("%Y%m%d_%H%M%S")


__all__ = ["get_tmp_dir", "get_output_dir", "horodatage"]
