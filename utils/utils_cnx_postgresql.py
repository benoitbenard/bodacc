"""afterdata.utils.utils_cnx_postgresql

Connexion PostgreSQL et export générique SQL → CSV pour les extractions AfterData.
"""

import csv
import logging
import traceback

import psycopg2


def reparer_encodage_corrompu(val):
    """Corrige les chaînes mal encodées en UTF-8/LATIN1 si possible."""

    if isinstance(val, str):
        try:
            return val.encode("latin1").decode("utf-8")
        except Exception:
            return val
    return val


def connecter_a_postgres(config, section_name: str = "postgres_bnc_ods"):
    """Établit une connexion PostgreSQL en lisant la section fournie du ``config.ini``."""

    db_conf = config[section_name]

    try:
        conn = psycopg2.connect(
            host=db_conf["DB_HOST"],
            port=db_conf["DB_PORT"],
            dbname=db_conf["DB_NAME"],
            user=db_conf["DB_USER"],
            password=db_conf["DB_PASSWORD"],
        )
        logging.info("Connexion PostgreSQL réussie.")
        return conn

    except Exception as e:
        logging.error("Connexion PostgreSQL impossible.")
        logging.error(str(e))
        raise


def reparer_texte_corrompu(val):
    """Corrige les chaînes UTF-8 stockées en LATIN1 (cas ODS BNC)."""

    if isinstance(val, str):
        try:
            return val.encode("latin1").decode("utf-8")
        except Exception:
            return val
    return val


def exporter_sql_vers_csv(conn, sql, chemin_csv, taille_lot: int = 1000, reparer_encodage: bool = False):
    """Exécute une requête PostgreSQL et exporte le résultat dans un CSV UTF-8-SIG.

    :param taille_lot: nombre de lignes à lire à la fois depuis PostgreSQL
    :param reparer_encodage: si ``True``, corrige l'encodage de toutes les colonnes
    """

    def clean_row(row):
        """Remplace les ``None`` par des chaînes vides."""

        return ["" if v is None else v for v in row]

    try:
        with conn.cursor() as cur:
            logging.info("Exécution de la requête SQL...")
            cur.execute(sql)

            if cur.description is None:
                logging.warning("La requête n'a retourné aucune donnée.")
                return

            colonnes = [desc[0] for desc in cur.description]

            with open(chemin_csv, "w", encoding="utf-8-sig", newline="") as f:
                writer = csv.writer(f, delimiter=";", quotechar='"', quoting=csv.QUOTE_ALL)
                writer.writerow(colonnes)

                total = 0

                while True:
                    rows = cur.fetchmany(taille_lot)
                    if not rows:
                        break

                    lignes_corrigees = []

                    for row in rows:
                        row = list(clean_row(row))

                        if reparer_encodage:
                            row = [reparer_texte_corrompu(v) for v in row]

                        lignes_corrigees.append(row)

                    writer.writerows(lignes_corrigees)
                    total += len(rows)

                    if total % 100000 == 0:
                        logging.info(f"{total:,} lignes exportées...")

        logging.info(f"Export terminé : {chemin_csv}")

    except Exception as e:
        logging.error("Erreur lors de l'export.")
        logging.error(str(e))
        logging.debug(traceback.format_exc())
        raise


__all__ = [
    "connecter_a_postgres",
    "exporter_sql_vers_csv",
    "reparer_encodage_corrompu",
    "reparer_texte_corrompu",
]
