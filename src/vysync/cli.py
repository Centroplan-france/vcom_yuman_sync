#!/usr/bin/env python3
from __future__ import annotations
"""
vysync.cli
===========

Orchestre la chaîne complète « VCOM → Supabase → Yuman ».

Usage :
    LOG_LEVEL=DEBUG poetry run python -m vysync.cli [--site-key TS9A8]

Étapes :
1.   VCOM → snapshot local
2.   Diff avec la base Supabase     ➜  SupabaseAdapter.apply_*_patch
3.   Relecture Supabase (post-write)
4.   Diff (Supabase ➜ Yuman)        ➜  YumanAdapter.apply_*_patch
"""

import argparse
import logging

from vysync.app_logging import init_logger, _dump
from vysync.adapters.vcom_adapter import fetch_snapshot
from vysync.adapters.supabase_adapter import SupabaseAdapter
from vysync.adapters.yuman_adapter import YumanAdapter
from vysync.vcom_client import VCOMAPIClient
from vysync.diff import diff_entities
from vysync.conflict_resolution import import_yuman_sites_in_mapping, detect_and_resolve_site_conflicts

# ─────────────────────────── Logger ────────────────────────────
logger = init_logger(__name__)
logger.setLevel(logging.DEBUG)

# ──────────────────────────── Main ─────────────────────────────
def main() -> None:
    # -----------------------------------------------------------
    # CLI arguments
    # -----------------------------------------------------------
    parser = argparse.ArgumentParser(
        description="Synchronise VCOM ↔ Supabase ↔ Yuman via snapshot/diff"
    )
    parser.add_argument("--site-key",
        help="Ne traiter qu’un seul system_key VCOM", )
    parser.add_argument("--maj-all",  action="store_true",
                    help="Forcer la mise à jour complète (ignorer cache DB)")
    args = parser.parse_args()
    site_key: str | None = args.site_key
    maj_all  = args.maj_all

    # -----------------------------------------------------------
    # Clients / Adapters
    # -----------------------------------------------------------
    vc          = VCOMAPIClient()
    sb_adapter  = SupabaseAdapter()
    y_adapter   = YumanAdapter(sb_adapter)

    # -----------------------------------------------------------
    # PHASE 1 – VCOM ➜ Supabase
    # -----------------------------------------------------------
    # sites déjà connus (pour filtrage incrémental)
    db_sites  = sb_adapter.fetch_sites()
    known_sys = set(db_sites.keys())

    v_sites, v_equips = fetch_snapshot(vc, vcom_system_key=site_key, skip_keys=None if maj_all or site_key else known_sys,)
    if site_key:
        v_sites  = {k: s for k, s in v_sites.items()  if k == site_key}
        v_equips = {k: e for k, e in v_equips.items()
                    if e.vcom_system_key == site_key}

    db_equips = sb_adapter.fetch_equipments()
    if site_key:
        db_sites  = {k: s for k, s in db_sites.items()  if k == site_key}
        db_equips = {k: e for k, e in db_equips.items()
                     if e.vcom_system_key == site_key}

    patch_sites  = diff_entities(db_sites,  v_sites)
    patch_equips = diff_entities(db_equips, v_equips)

    logger.info("[VCOM→DB] Sites  Δ  +%d  ~%d  -%d",
                len(patch_sites.add), len(patch_sites.update), len(patch_sites.delete))
    logger.info("[VCOM→DB] Equips Δ  +%d  ~%d  -%d",
                len(patch_equips.add), len(patch_equips.update), len(patch_equips.delete))

    sb_adapter.apply_sites_patch(patch_sites)
    sb_adapter.apply_equips_patch(patch_equips)

    # -----------------------------------------------------------
    # PHASE 1½ – Résolution manuelle des conflits de sites
    # -----------------------------------------------------------
    import_yuman_sites_in_mapping(sb_adapter, y_adapter)
    detect_and_resolve_site_conflicts(sb_adapter, y_adapter)

    # Recharger Supabase (sites/équipements) après résolution
    sb_sites  = sb_adapter.fetch_sites()
    sb_equips = sb_adapter.fetch_equipments()

    # Filtrer les sites ignorés
    ignored_keys = {
        r["vcom_system_key"]
        for r in sb_adapter.sb.table("sites_mapping")
                             .select("vcom_system_key")
                             .eq("ignore_site", True).execute().data or []
    }
    sb_sites  = {k: s for k, s in sb_sites.items()  if k not in ignored_keys}
    sb_equips = {k: e for k, e in sb_equips.items()
                 if e.vcom_system_key not in ignored_keys}

    if site_key:
        sb_sites  = {k: s for k, s in sb_sites.items()  if k == site_key}
        sb_equips = {k: e for k, e in sb_equips.items()
                     if e.vcom_system_key == site_key}

    # -----------------------------------------------------------
    # PHASE 2 – Supabase ➜ Yuman
    # -----------------------------------------------------------
    logger.info("[DB→YUMAN] Synchronisation des sites…")
    y_adapter.apply_sites_patch(sb_sites)
    logger.info("[DB→YUMAN] Synchronisation des équipements…")
    y_adapter.apply_equips_patch(sb_equips)
    logger.info("[DB→YUMAN] Fin synchronisation Yuman")

    logger.info("✅ Synchronisation terminée")


if __name__ == "__main__":
    main()