"""afterdata.utils.afterdata_utils

Module agrégateur conservé pour compatibilité. Les briques utilitaires sont
maintenant éclatées dans des modules dédiés :
- utils_load_config_ini : chargement/déchiffrement du config.ini
- utils_logging : logging centralisé
- utils_get_directories : gestion des répertoires TMP/OUTPUT et horodatage
- utils_cnx_postgresql : connexion PostgreSQL et export SQL → CSV
- utils_cnx_marklogic : connexion MarkLogic et export Optic SQL → CSV
- utils_cnx_oracle : connexion Oracle et lecture DataFrame
- utils_cnx_csv : export DataFrame → CSV
"""

import warnings

warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy")

from .utils_cnx_csv import exporter_dataframe_csv
from .utils_cnx_marklogic import connecter_a_marklogic, export_marklogic_sql_to_csv
from .utils_cnx_oracle import connecter_a_oracle, lire_oracle_dataframe
from .utils_cnx_postgresql import (
    connecter_a_postgres,
    exporter_sql_vers_csv,
    reparer_encodage_corrompu,
    reparer_texte_corrompu,
)
from .utils_get_directories import get_output_dir, get_tmp_dir, horodatage
from .utils_load_config_ini import (
    ENC_PREFIX,
    ENC_SUFFIX,
    charger_configuration,
    decrypt_config_if_needed,
    decrypt_value,
    is_encrypted,
)
from .utils_logging import initialiser_logging

__all__ = [
    "ENC_PREFIX",
    "ENC_SUFFIX",
    "charger_configuration",
    "decrypt_config_if_needed",
    "decrypt_value",
    "is_encrypted",
    "initialiser_logging",
    "get_tmp_dir",
    "get_output_dir",
    "horodatage",
    "connecter_a_postgres",
    "exporter_sql_vers_csv",
    "reparer_encodage_corrompu",
    "reparer_texte_corrompu",
    "connecter_a_marklogic",
    "export_marklogic_sql_to_csv",
    "connecter_a_oracle",
    "lire_oracle_dataframe",
    "exporter_dataframe_csv",
]
