"""supabase_setup.py
--------------------
Initialise la base Postgres (Supabase) pour VCOM ↔ Yuman.

• Compatible PgBouncer (pooler 6543) : exécute la DDL en **AUTOCOMMIT**.
• Force le `search_path` sur `public`.
• Importe explicitement `models` pour que SQLModel voie toutes les tables !  
  (sinon `metadata` serait vide et aucune table ne serait créée.)

Usage :
    python supabase_setup.py
"""

import logging
from sqlalchemy import text
from sqlmodel import SQLModel

# 👉 IMPORTANT : enregistre les classes-table auprès de SQLModel.metadata
import models  # noqa: F401 — side‑effect import, keep it!

from db import engine  # engine configuré via env DATABASE_URL

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")


def create_tables() -> None:
    """Crée toutes les tables déclarées dans models.py (AUTOCOMMIT)."""
    logging.info("Creating tables in AUTOCOMMIT mode …")

    # Ouvrir une connexion hors transaction (nécessaire avec PgBouncer)
    with engine.connect() as conn:
        conn = conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(text("set search_path to public"))
        SQLModel.metadata.create_all(conn)

    logging.info("✅ Tables checked/created in schema public.")


if __name__ == "__main__":
    create_tables()
