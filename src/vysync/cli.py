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
    parser.add_argument(
        "--site-key",
        help="Ne traiter qu’un seul system_key VCOM",
    )
    args = parser.parse_args()
    site_key: str | None = args.site_key

    # -----------------------------------------------------------
    # Clients / Adapters
    # -----------------------------------------------------------
    vc          = VCOMAPIClient()
    sb_adapter  = SupabaseAdapter()
    y_adapter   = YumanAdapter(sb_adapter)

    # -----------------------------------------------------------
    # PHASE 1 – VCOM ➜ Supabase
    # -----------------------------------------------------------
    v_sites, v_equips = fetch_snapshot(vc, vcom_system_key=site_key)
    _dump("[CLI] VCOM raw sites",  {k: s.to_dict() for k, s in v_sites.items()})
    _dump("[CLI] VCOM raw equips", {k: e.to_dict() for k, e in v_equips.items()})

    if site_key:
        v_sites  = {k: v for k, v in v_sites.items()  if k == site_key}
        v_equips = {k: e for k, e in v_equips.items() if e.vcom_system_key == site_key}

    db_sites  = sb_adapter.fetch_sites()
    db_equips = sb_adapter.fetch_equipments()
    _dump("[CLI] DB raw sites",  {k: s.to_dict() for k, s in db_sites.items()})
    _dump("[CLI] DB raw equips", {k: e.to_dict() for k, e in db_equips.items()})

    if site_key:
        db_sites  = {k: v for k, v in db_sites.items()  if k == site_key}
        db_equips = {k: e for k, e in db_equips.items() if e.vcom_system_key == site_key}

    patch_sites  = diff_entities(db_sites,  v_sites)
    patch_equips = diff_entities(db_equips, v_equips)

    logger.info("[VCOM→DB] Sites  Δ  +%d  ~%d  -%d",
                len(patch_sites.add), len(patch_sites.update), len(patch_sites.delete))
    logger.info("[VCOM→DB] Equips Δ  +%d  ~%d  -%d",
                len(patch_equips.add), len(patch_equips.update), len(patch_equips.delete))

    _dump("[Δ] sites patch",  patch_sites._asdict())
    _dump("[Δ] equips patch", patch_equips._asdict())

    sb_adapter.apply_sites_patch(patch_sites)
    sb_adapter.apply_equips_patch(patch_equips)

    # -----------------------------------------------------------
    # PHASE 2 – Supabase ➜ Yuman
    # -----------------------------------------------------------
    db_sites  = sb_adapter.fetch_sites()
    db_equips = sb_adapter.fetch_equipments()

    if site_key:
        db_sites  = {k: v for k, v in db_sites.items()  if k == site_key}
        db_equips = {k: e for k, e in db_equips.items() if e.vcom_system_key == site_key}

    # --- Sites Yuman ------------------------------------------
    y_adapter.apply_sites_patch(db_sites)
    
    # --- Equipements Yuman ------------------------------------
    y_adapter.apply_equips_patch(db_equips)
    db_equips_post  = sb_adapter.fetch_equipments()
    y_equips_post   = y_adapter.fetch_equips()

    _dump("[CLI] DB equips after YUMAN write",
          {k: e.to_dict() for k, e in db_equips_post.items()})
    _dump("[CLI] YUMAN equips after patch",
          {k: e.to_dict() for k, e in y_equips_post.items()})

    logger.info("✅ Synchronisation terminée")


if __name__ == "__main__":
    main()
