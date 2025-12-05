"""afterdata.utils.utils_cnx_marklogic

Connexion MarkLogic (désactivation proxy) et export streaming CSV pour Optic SQL.
"""

import csv
import logging
import os
import time

import requests
from marklogic import Client


def connecter_a_marklogic(config, section_name: str = "marklogic_eco_final"):
    """Connexion MarkLogic en désactivant complètement les proxies système."""

    ml_conf = config[section_name]

    host = ml_conf["ML_HOST"].strip()
    port = ml_conf["ML_PORT"].strip()
    user = ml_conf["ML_USER"].strip()
    pwd = ml_conf["ML_PASSWORD"].strip()

    url = f"{host}:{port}"

    for var in ["HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"]:
        os.environ.pop(var, None)

    requests.sessions.Session.trust_env = False

    def no_proxy_session(*args, **kwargs):
        s = original_session(*args, **kwargs)
        s.proxies = {"http": "", "https": ""}
        s.trust_env = False
        return s

    original_session = requests.Session
    requests.Session = no_proxy_session

    try:
        client = Client(url, auth=(user, pwd))
        logging.info(f"Connexion MarkLogic réussie : {url}")
        logging.info("Proxy désactivé (mode global).")
        return client

    except Exception as e:
        logging.error("Impossible de se connecter à MarkLogic.")
        logging.error(str(e))
        raise


def export_marklogic_sql_to_csv(client, sql, chemin_csv, log_interval: int = 100000):
    """Export Optic SQL → CSV en streaming sans charge mémoire."""

    start_time = time.time()

    logging.info("Exécution SQL MarkLogic (mode streaming)...")
    logging.debug(f"SQL : {sql}")

    resp = client.rows.query(sql=sql, format="csv")

    if isinstance(resp, str):
        logging.info("Réponse reçue (string CSV).")
        csv_stream = resp.splitlines()
    else:
        status = getattr(resp, "status_code", "UNKNOWN")
        logging.error(f"Erreur HTTP MarkLogic : {status}")
        logging.error(resp.text[:500])
        raise RuntimeError(f"Erreur HTTP {status}")

    with open(chemin_csv, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f, delimiter=";", quotechar='"', quoting=csv.QUOTE_ALL)

        reader = csv.reader(csv_stream, delimiter=",")

        total = 0
        for row in reader:
            writer.writerow(row)
            total += 1

            if total % log_interval == 0:
                elapsed = time.time() - start_time
                speed = total / elapsed
                logging.info(f"{total:,} lignes exportées ({speed:,.0f} lignes/s)")

    elapsed = time.time() - start_time
    size_bytes = os.path.getsize(chemin_csv)
    size_mb = size_bytes / (1024 * 1024)

    logging.info("=== STATISTIQUES EXPORT MARKLOGIC ===")
    logging.info(f"Fichier exporté : {chemin_csv}")
    logging.info(f"Lignes totales : {total:,}")
    logging.info(f"Taille finale : {size_mb:,.2f} Mo")
    logging.info(f"Durée totale : {elapsed:,.2f} sec")
    logging.info(f"Débit moyen : {total/elapsed:,.0f} lignes/sec")

    logging.info("Export streaming terminé ✔")


__all__ = ["connecter_a_marklogic", "export_marklogic_sql_to_csv"]
