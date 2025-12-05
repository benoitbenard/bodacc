"""afterdata.utils.utils_logging

Configuration centralisée du logging (console + fichier rotatif) pour les scripts
# Initialise le système de logging :
# - Crée le répertoire de logs si nécessaire
# - Initialise le logging avec un fichier horodaté pour chaque exécution.
# - Supprime automatiquement les fichiers .log de plus de 5 jours.
# - Applique un format standard (date - niveau - message)
# - Force la configuration globale du logging
# Retourne le chemin complet du fichier de log créé

"""

import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler

import os
import time
from datetime import datetime, timedelta

def nettoyer_anciens_logs(logs_dir, jours=10):
    """Supprime les fichiers .log plus vieux que X jours."""
    now = time.time()
    limite = now - jours * 24 * 3600

    for fichier in os.listdir(logs_dir):
        if fichier.endswith(".log"):
            chemin = os.path.join(logs_dir, fichier)
            if os.path.isfile(chemin):
                # Vérifier l'âge du fichier
                if os.path.getmtime(chemin) < limite:
                    os.remove(chemin)


def initialiser_logging(config, log_name: str = "script"):
    """Configure le logging avec fichiers horodatés et suppression des logs > 5 jours."""

    main_dir = config["directories"]["MAIN_DIR"].strip()
    log_dir = config["directories"]["LOG_DIR"].strip()

    logs_dir = os.path.join(main_dir, log_dir)
    os.makedirs(logs_dir, exist_ok=True)

    # Nettoyage des anciens logs
    nettoyer_anciens_logs(logs_dir, jours=5)

    # Création du fichier log horodaté
    horo = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(logs_dir, f"{horo}_{log_name}.log")

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logging.basicConfig(level=logging.INFO, handlers=[file_handler, console_handler], force=True)

    logging.info(f"Journalisation initialisée : {log_file}")
    return log_file



__all__ = ["initialiser_logging"]
