"""
Initialise or update the Supabase/Postgres schema for VCOM ↔ Yuman Sync.
"""

from __future__ import annotations

import os
from typing import List

from sqlalchemy import text
from sqlmodel import SQLModel

from vysync.db import engine
from vysync.logging import init_logger
from vysync.yuman_client import YumanClient

logger = init_logger(__name__)


# --------------------------------------------------------------------------- #
# Helpers                                                                     #
# --------------------------------------------------------------------------- #
def _constraint_exists(conn, table: str, name: str) -> bool:
    sql = """
    SELECT 1
    FROM pg_constraint c
    JOIN pg_class t ON c.conrelid = t.oid
    WHERE t.relname = :table AND c.conname = :name
    """
    return bool(conn.execute(text(sql), {"table": table, "name": name}).fetchone())


def _add_constraint(conn, table: str, column: str, name: str) -> None:
    if engine.dialect.name != "postgresql":
        return
    if _constraint_exists(conn, table, name):
        logger.debug("Constraint %s already present – skipped", name)
        return
    ddl = f"ALTER TABLE {table} ADD CONSTRAINT {name} UNIQUE ({column})"
    conn.execute(text(ddl))
    logger.info("Constraint %s created", name)


# --------------------------------------------------------------------------- #
# Schema creation                                                             #
# --------------------------------------------------------------------------- #
def ensure_schema() -> None:
    with engine.begin() as conn:
        conn.execute(text("SET search_path TO public"))
        SQLModel.metadata.create_all(conn)
        # ---------------------------------------------------------------
        # Sites : rendre la colonne nullable + colonnes custom + index
        # ---------------------------------------------------------------
        conn.execute(text("""
            ALTER TABLE sites_mapping
                ALTER COLUMN vcom_system_key DROP NOT NULL,
                ALTER COLUMN yuman_site_id DROP NOT NULL;

            ALTER TABLE sites_mapping
                ADD COLUMN IF NOT EXISTS aldi_id text,
                ADD COLUMN IF NOT EXISTS aldi_store_id text,
                ADD COLUMN IF NOT EXISTS project_number_cp text,
                ADD COLUMN IF NOT EXISTS ignore_site boolean
                    DEFAULT false;

            CREATE UNIQUE INDEX IF NOT EXISTS uniq_vcom_system_key
                ON sites_mapping (vcom_system_key)
                WHERE vcom_system_key IS NOT NULL;
                          
            CREATE UNIQUE INDEX IF NOT EXISTS uniq_yuman_site_id
                ON sites_mapping (yuman_site_id)
                WHERE yuman_site_id IS NOT NULL;
        """))


        _add_constraint(conn, "tickets_mapping", "vcom_ticket_id", "uniq_vcom_ticket")

    logger.info("Schema ensured and constraints applied.")


# --------------------------------------------------------------------------- #
# Category import                                                             #
# --------------------------------------------------------------------------- #
def sync_categories(token: str) -> None:
    yc = YumanClient(token)
    cats: List[dict] = yc.get_material_categories()  # [{'id': .., 'name': ..}, ...]

    with engine.begin() as conn:
        for c in cats:
            conn.execute(
                text(
                    """
                    INSERT INTO equipment_categories (id, name, created_at)
                    VALUES (:id, :name, now())
                    ON CONFLICT (id) DO NOTHING
                    """
                ),
                {"id": c["id"], "name": c["name"]},
            )
    logger.info("Equipment categories synced (%s rows)", len(cats))


# --------------------------------------------------------------------------- #
# CLI entry-point                                                             #
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    ensure_schema()
    token = os.getenv("YUMAN_TOKEN")
    if token:
        sync_categories(token)
    else:
        logger.warning("YUMAN_TOKEN not set – skipping category import")
