#!/usr/bin/env python3
from __future__ import annotations
"""
Entrée principale « snapshot → diff → apply ».

Usage :
    LOG_LEVEL=DEBUG poetry run python -m vysync.cli [--site-key TS9A8]
"""
import argparse
import logging

from vysync.app_logging import init_logger
from vysync.adapters.vcom_adapter import fetch_snapshot
from vysync.vcom_client import VCOMAPIClient
from vysync.adapters.supabase_adapter import SupabaseAdapter
from vysync.adapters.yuman_adapter import YumanAdapter
from vysync.diff import diff_entities

logger = init_logger(__name__)
logger.setLevel(logging.DEBUG)


def main() -> None:
    # ────────────────────────── CLI args ──────────────────────────────
    parser = argparse.ArgumentParser(
        description="Synchronise VCOM ↔ Supabase ↔ Yuman via snapshot/diff"
    )
    parser.add_argument(
        "--site-key",
        help="Traiter exclusivement ce system_key VCOM",
    )
    args = parser.parse_args()
    site_key: str | None = args.site_key  # ← args est défini ici

    # ────────────────────── Initialisation clients ────────────────────
    vc = VCOMAPIClient()
    sb_adapter = SupabaseAdapter()
    y_adapter = YumanAdapter(sb_adapter)

    # ───────────────────────── VCOM ➜ DB phase ────────────────────────
    v_sites, v_equips = fetch_snapshot(vc, vcom_system_key=site_key)
    if site_key:
        v_sites = {k: v for k, v in v_sites.items() if k == site_key}
        v_equips = {k: e for k, e in v_equips.items() if k[0] == site_key}

    db_sites = sb_adapter.fetch_sites()
    # filtrage équipements après snapshot DB
    db_equips = {k: e for k, e in db_equips.items() if e.site_key == site_key}

    if site_key:
        db_sites = {k: v for k, v in db_sites.items() if k == site_key}
        db_equips = {k: e for k, e in db_equips.items() if k[0] == site_key}

    patch_sites = diff_entities(db_sites, v_sites)
    patch_equips = diff_entities(db_equips, v_equips)

    logger.info(
        "[VCOM→DB] Sites Δ +%d ~%d -%d",
        len(patch_sites.add), len(patch_sites.update), len(patch_sites.delete),
    )
    logger.info(
        "[VCOM→DB] Equip Δ +%d ~%d -%d",
        len(patch_equips.add), len(patch_equips.update), len(patch_equips.delete),
    )

    sb_adapter.apply_sites_patch(patch_sites)
    sb_adapter.apply_equips_patch(patch_equips)

    # ───────────────────────── DB ➜ Yuman phase ───────────────────────
    db_sites = sb_adapter.fetch_sites()
    db_equips = sb_adapter.fetch_equipments()
    if site_key:
        db_sites = {k: v for k, v in db_sites.items() if k == site_key}
        db_equips = {k: e for k, e in db_equips.items() if k[0] == site_key}

    y_adapter.apply_sites_patch(db_sites)
    y_adapter.apply_equips_patch(db_equips)

    logger.info("✅ Synchronisation terminée")


if __name__ == "__main__":
    main()
