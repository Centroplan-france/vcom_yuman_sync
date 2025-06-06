"""supabase_setup.py
--------------------
Initialise la base Postgres hébergée sur Supabase (ou locale) pour le projet
VCOM ↔ Yuman.

Usage :
    python supabase_setup.py       # crée toutes les tables si elles n’existent pas

Le script importe :
    • `engine` depuis db.py  – configuré avec env var `DATABASE_URL`
    • les modèles SQLModel définis dans models.py.

Il n’applique aucun DROP : si les tables existent déjà, elles sont laissées
intactes. Ajoute simplement les tables manquantes.
"""

from sqlmodel import SQLModel

from db import engine  # reuse the engine configured via DATABASE_URL
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")


def create_tables() -> None:
    """Create all tables declared in models.py if they don’t already exist."""
    logging.info("Creating tables …")
    SQLModel.metadata.create_all(engine)
    logging.info("✅  All tables are present (created if absent).")


if __name__ == "__main__":
    create_tables()
