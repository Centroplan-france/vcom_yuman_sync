"""Initialise the Postgres schema for the sync utilities."""

import logging
import os
from typing import List

import requests
from sqlalchemy import text
from sqlmodel import SQLModel

from src.logging import init_logger

# Register models so SQLModel is aware of them
import models  # noqa: F401 -- register models

from db import engine

logger = init_logger(__name__)


def _fetch_categories(token: str) -> List[dict]:
    url = "https://api.yuman.io/v1/material_categories"
    resp = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    items = data.get("items", data)
    return [{"id": int(it["id"]), "name": it.get("label") or it.get("name", "")} for it in items]


def create_tables() -> None:
    """Create all tables declared in models.py using AUTOCOMMIT."""
    logger.info("Creating tables in AUTOCOMMIT mode ...")

    with engine.connect() as conn:
        conn = conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(text("set search_path to public"))
        SQLModel.metadata.create_all(conn)

        conn.execute(
            text(
                "ALTER TABLE tickets_mapping ADD CONSTRAINT IF NOT EXISTS uniq_vcom_ticket UNIQUE (vcom_ticket_id)"
            )
        )
        conn.execute(
            text(
                "ALTER TABLE sites_mapping ADD CONSTRAINT IF NOT EXISTS uniq_system_key UNIQUE (vcom_system_key)"
            )
        )

        token = os.getenv("YUMAN_TOKEN")
        if token:
            categories = _fetch_categories(token)
            for cat in categories:
                conn.execute(
                    text(
                        "INSERT INTO equipment_categories (id, name) VALUES (:id, :name) ON CONFLICT (id) DO NOTHING"
                    ),
                    cat,
                )

    logger.info("Tables ensured and constraints applied.")


if __name__ == "__main__":
    create_tables()
