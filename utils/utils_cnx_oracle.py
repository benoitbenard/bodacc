"""afterdata.utils.utils_cnx_oracle

Connexion Oracle centralisée et helpers DataFrame pour les exports AfterData.
"""

import logging

import oracledb
import pandas as pd


def connecter_a_oracle(config, section_name: str = "oracle_semarchy_mdm"):
    """Connexion Oracle en lisant la section indiquée du ``config.ini``."""

    try:
        oracle_cfg = config[section_name]

        host = oracle_cfg["ORACLE_HOST"].strip()
        port = int(oracle_cfg["ORACLE_PORT"])
        service = oracle_cfg["ORACLE_SERVICE"].strip()
        user = oracle_cfg["ORACLE_USER"].strip()
        pwd = oracle_cfg["ORACLE_PASSWORD"].strip()

        dsn = oracledb.makedsn(host, port, service_name=service)

        logging.info(f"Connexion Oracle vers {host}:{port}/{service} ...")
        conn = oracledb.connect(user=user, password=pwd, dsn=dsn)

        logging.info("Connexion Oracle réussie.")
        return conn

    except Exception as e:
        logging.error("Impossible de se connecter à Oracle.")
        logging.error(str(e))
        raise


def lire_oracle_dataframe(conn, query):
    """Exécute une requête Oracle et retourne un DataFrame pandas typé chaîne."""

    try:
        logging.info("Exécution requête Oracle...")
        df = pd.read_sql(query, conn)
        df = df.astype(str)
        logging.info(f"{len(df)} lignes Oracle récupérées.")
        return df
    except Exception as e:
        logging.error("Erreur lecture Oracle : " + str(e))
        raise


__all__ = ["connecter_a_oracle", "lire_oracle_dataframe"]
