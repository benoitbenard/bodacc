"""Générateur simple de clé Fernet pour chiffrer ``config.ini``."""

from cryptography.fernet import Fernet


def generate_and_show_key():
    """Affiche une clé Fernet fraîchement générée sur la sortie standard."""

    key = Fernet.generate_key()
    print("Votre clé générée :")
    print(key.decode())


if __name__ == "__main__":
    generate_and_show_key()
