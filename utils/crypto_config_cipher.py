"""Chiffre les valeurs sensibles d'un fichier ``config.ini`` en place.

Le script est utilisable en CLI :

```
python -m afterdata.utils.crypto_config_cipher <cle_fernet> <chemin_config.ini>
```
"""

import configparser
import os
import sys

from cryptography.fernet import Fernet

ENC_PREFIX = "ENC("
ENC_SUFFIX = ")"


def is_encrypted(value: str) -> bool:
    """Vérifie si une valeur est déjà chiffrée."""
    return value.startswith(ENC_PREFIX) and value.endswith(ENC_SUFFIX)


def encrypt_value(value: str, fernet: Fernet) -> str:
    """Chiffre une valeur et l'encapsule dans ENC(...)."""
    encrypted = fernet.encrypt(value.encode()).decode()
    return f"{ENC_PREFIX}{encrypted}{ENC_SUFFIX}"


def key_should_be_encrypted(option_name: str, keywords: list[str]) -> bool:
    """Vérifie si une clé doit être chiffrée selon les mots-clefs définis dans [encryption]."""
    option_name_upper = option_name.upper()
    return any(keyword.upper() in option_name_upper for keyword in keywords)


def encrypt_config_file(key: str, config_path: str):
    """Procédure principale de cryptage (écrase le fichier d'entrée)."""

    # Charger la clé Fernet
    try:
        fernet = Fernet(key.encode())
    except Exception:
        print("❌ Erreur : clé Fernet invalide.")
        return

    if not os.path.isfile(config_path):
        print(f"❌ Fichier introuvable : {config_path}")
        return

    # Lecture du fichier ini
    config = configparser.ConfigParser()
    config.read(config_path)

    # Vérifier la présence des mots-clefs dans [encryption]
    if "encryption" not in config or "KEYWORDS" not in config["encryption"]:
        print("❌ Erreur : la section [encryption] avec KEYWORDS est manquante dans config.ini.")
        return

    # Extraire et nettoyer les mots-clefs
    raw_keywords = config["encryption"]["KEYWORDS"]
    keywords = [k.strip() for k in raw_keywords.split(",") if k.strip()]

    if not keywords:
        print("❌ Aucun mot-clef valide défini dans KEYWORDS.")
        return

    print(f"🔍 Mots-clefs détectés : {keywords}")

    # Parcours des sections
    for section in config.sections():
        if section == "encryption":
            continue

        for option in config[section]:
            value = config[section][option]

            # Si déjà chiffré → ne rien faire
            if is_encrypted(value):
                continue

            # Si la clé correspond aux mots-clefs → chiffrer
            if key_should_be_encrypted(option, keywords):
                config[section][option] = encrypt_value(value, fernet)

    # ➤ ÉCRITURE DIRECTEMENT DANS LE FICHIER ORIGINAL
    with open(config_path, "w") as f:
        config.write(f)

    print(f"✅ Fichier mis à jour (écrasé) : {config_path}")


# ----------------------------
# Exécution CLI
# ----------------------------
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage : python encrypt_config.py <cle_fernet> <chemin_config.ini>")
        sys.exit(1)

    key = sys.argv[1]
    config_file = sys.argv[2]

    encrypt_config_file(key, config_file)
