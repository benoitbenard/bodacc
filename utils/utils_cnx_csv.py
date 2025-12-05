"""afterdata.utils.utils_cnx_csv

Fonctions génériques d'export CSV compatibles avec les fichiers AfterData.
"""

import csv
import logging


def exporter_dataframe_csv(df, fichier_sortie, delim: str = ";", encoding: str = "utf-8-sig"):
    """Export générique d'un DataFrame pandas en CSV (séparateur ``;`` et BOM)."""

    try:
        logging.info(f"Export CSV → {fichier_sortie}")

        df = df.replace("None", "")
        df = df.replace("nan", "")
        df = df.fillna("")

        df.to_csv(
            fichier_sortie,
            sep=delim,
            index=False,
            encoding=encoding,
            quoting=csv.QUOTE_ALL,
            na_rep="",
        )

        logging.info(f"Export CSV terminé ({len(df)} lignes).")

    except Exception as e:
        logging.error(f"Erreur export CSV : {e}")
        raise


__all__ = ["exporter_dataframe_csv"]
