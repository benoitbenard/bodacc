"""afterdata.utils.utils_load_config_ini

Gestion centralisée du chargement du fichier ``config.ini`` et du déchiffrement
éventuel des valeurs sensibles entourées de ``ENC(...)``.
"""

from __future__ import annotations

import configparser
import logging
import os
from pathlib import Path
from typing import Optional

from cryptography.fernet import Fernet

ENC_PREFIX = "ENC("
ENC_SUFFIX = ")"


def is_encrypted(value: str) -> bool:
    """Vérifie si une valeur de configuration est chiffrée."""

    return isinstance(value, str) and value.startswith(ENC_PREFIX) and value.endswith(ENC_SUFFIX)


def decrypt_value(value: str, fernet: Fernet) -> str:
    """Déchiffre une valeur entourée de ``ENC(...)`` à l'aide de Fernet."""

    encrypted_part = value[len(ENC_PREFIX) : -len(ENC_SUFFIX)]
    return fernet.decrypt(encrypted_part.encode()).decode()


def decrypt_config_if_needed(config: configparser.ConfigParser, fernet: Optional[Fernet]):
    """Applique le déchiffrement Fernet sur toutes les valeurs marquées ``ENC(...)``."""

    if fernet is None:
        return config

    for section in config.sections():
        for option in config[section]:
            value = config[section][option]
            if is_encrypted(value):
                try:
                    config[section][option] = decrypt_value(value, fernet)
                except Exception:
                    logging.error(f"❌ Impossible de déchiffrer {section}.{option} (valeur invalide ?)")

    return config


def charger_configuration(config: str | None = None, key: str | None = None):
    """Charge le ``config.ini`` AfterData en gérant l'option de chiffrement Fernet.

    Priorité de résolution pour la clé Fernet :
    1️⃣ Clé passée en paramètre à la fonction.
    2️⃣ Variable d'environnement ``AFTERDATA_KEY``.
    3️⃣ ``DEFAULT_KEY`` dans ``[encryption]`` du ``config.ini`` (environnements non prod).

    Priorité pour le fichier de config :
    1️⃣ ``AFTERDATA_CONFIG`` (variable d'env) ou paramètre ``config`` explicite.
    2️⃣ Fichier local ``afterdata/config/config.ini``.
    """

    fernet: Optional[Fernet] = None

    # Résolution de la clé Fernet
    if key:
        try:
            fernet = Fernet(key.encode())
            logging.info("Clé Fernet passée en paramètre.")
        except Exception:
            logging.error("❌ Clé passée en paramètre invalide.")
    else:
        env_key = os.getenv("AFTERDATA_KEY")
        if env_key:
            try:
                fernet = Fernet(env_key.encode())
                logging.info("Clé Fernet chargée depuis AFTERDATA_KEY.")
            except Exception:
                logging.error("❌ Clé Fernet invalide dans AFTERDATA_KEY.")

    # Recherche du fichier config.ini
    config_env = config or os.getenv("AFTERDATA_CONFIG")
    if config_env:
        config_path = Path(config_env)
        if config_path.exists():
            logging.info(f"Configuration chargée via AFTERDATA_CONFIG : {config_path}")
            parsed_config = configparser.ConfigParser()
            parsed_config.read(config_path, encoding="utf-8")

            if fernet is None and "encryption" in parsed_config and "DEFAULT_KEY" in parsed_config["encryption"]:
                default_key = parsed_config["encryption"]["DEFAULT_KEY"].strip()
                if default_key:
                    try:
                        fernet = Fernet(default_key.encode())
                        logging.info("Clé Fernet chargée depuis DEFAULT_KEY du config.ini.")
                    except Exception:
                        logging.error("❌ DEFAULT_KEY invalide dans le fichier config.")

            return decrypt_config_if_needed(parsed_config, fernet)
        logging.warning(f"AFTERDATA_CONFIG défini mais fichier introuvable : {config_path}")

    # Fallback local
    chemin_config_local = Path(__file__).resolve().parent.parent / "config" / "config.ini"
    if not chemin_config_local.exists():
        raise FileNotFoundError(
            f"Fichier configuration introuvable : {chemin_config_local}\n"
            "Ni AFTERDATA_CONFIG, ni configuration locale disponible."
        )

    logging.info(f"Configuration chargée via le répertoire local : {chemin_config_local}")

    parsed_config = configparser.ConfigParser()
    parsed_config.read(chemin_config_local, encoding="utf-8")

    if fernet is None and "encryption" in parsed_config and "DEFAULT_KEY" in parsed_config["encryption"]:
        default_key = parsed_config["encryption"]["DEFAULT_KEY"].strip()
        if default_key:
            try:
                fernet = Fernet(default_key.encode())
                logging.info("Clé Fernet chargée depuis DEFAULT_KEY du config local.")
            except Exception:
                logging.error("❌ DEFAULT_KEY invalide dans le config local.")

    return decrypt_config_if_needed(parsed_config, fernet)


__all__ = [
    "ENC_PREFIX",
    "ENC_SUFFIX",
    "charger_configuration",
    "decrypt_config_if_needed",
    "decrypt_value",
    "is_encrypted",
]
