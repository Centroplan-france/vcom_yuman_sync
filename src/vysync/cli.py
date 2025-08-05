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
from vysync.diff import diff_entities, diff_fill_missing, set_parent_map
from vysync.conflict_resolution import detect_and_resolve_site_conflicts, resolve_clients_for_sites

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
    parser.add_argument(
        "--maj-all",
        action="store_true",
        help="Forcer la mise à jour complète (ignorer cache DB)",
    )
    args = parser.parse_args()
    site_key: str | None = args.site_key
    maj_all = args.maj_all

    # -----------------------------------------------------------
    # Clients / Adapters
    # -----------------------------------------------------------
    vc = VCOMAPIClient()
    sb = SupabaseAdapter()
    y  = YumanAdapter(sb)

    # -----------------------------------------------------------
    # PHASE 1 A – VCOM → Supabase
    # -----------------------------------------------------------
    db_sites   = sb.fetch_sites_v(site_key=site_key)
    db_equips  = sb.fetch_equipments_v(site_key=site_key)
    known_sys  = set(db_sites.keys())

    # snapshot VCOM
    v_sites, v_equips = fetch_snapshot(vc, vcom_system_key=site_key, skip_keys=None if maj_all or site_key else known_sys,    )
    if site_key:
        v_sites  = {k: s for k, s in v_sites.items() if k == site_key}
        v_equips = {k: e for k, e in v_equips.items() if e.vcom_system_key == site_key}

    # filtrage incrémental
    if not maj_all and not site_key:
        seen = set(v_sites)
        db_sites  = {k: s for k, s in db_sites.items()  if k in seen}
        db_equips = {k: e for k, e in db_equips.items() if e.vcom_system_key in seen}

    # diff & patch
    patch_sites = diff_entities(db_sites, v_sites, ignore_fields={"yuman_site_id", "client_map_id", "code", "ignore_site"})
    patch_equips = diff_entities(db_equips, v_equips, ignore_fields={"yuman_material_id", "parent_id"})

    logger.info(
        "[VCOM→DB] Sites  Δ  +%d  ~%d  -%d",
        len(patch_sites.add),
        len(patch_sites.update),
        len(patch_sites.delete),
    )
    logger.info(
        "[VCOM→DB] Equips Δ  +%d  ~%d  -%d",
        len(patch_equips.add),
        len(patch_equips.update),
        len(patch_equips.delete),
    )

    sb.apply_sites_patch(patch_sites)
    sb.apply_equips_patch(patch_equips)

    # -----------------------------------------------------------
    # PHASE 1 B – YUMAN → Supabase (mapping)
    # -----------------------------------------------------------
    logger.info("[YUMAN→DB] snapshot & patch fill‑missing …")

    #1) on prend UN SEUL snapshot Yuman
    y_clients = list(y.yc.list_clients())
    y_sites   = y.fetch_sites()
    y_equips  = y.fetch_equips()

    #2) on lit en base les mappings existants
    db_clients = sb.fetch_clients()      # -> Dict[int, Client]
    db_maps_sites  = sb.fetch_sites_y()    # -> Dict[int, SiteMapping]
    db_maps_equips = sb.fetch_equipments_y()   # -> Dict[str, EquipMapping]

    #3) on génère des patchs « fill missing » (pas de supprimer)
    patch_clients = diff_fill_missing(db_clients,     {c["id"]: c for c in y_clients})
    patch_maps_sites  = diff_fill_missing(db_maps_sites,  y_sites, fields=["yuman_site_id","code", "client_map_id", "name",  "aldi_id","aldi_store_id","project_number_cp","commission_date","nominal_power"])
    patch_maps_equips = diff_fill_missing(db_maps_equips, y_equips, fields=["category_id","eq_type", "name", "yuman_material_id",
                                                                              "serial_number","brand","model","count","parent_id", "yuman_site_id"])

    logger.info(
        "[YUMAN→DB] Clients Δ +%d  ~%d  -%d",
        len(patch_clients.add),
        len(patch_clients.update),
        len(patch_clients.delete),
    )
    logger.info(
        "[YUMAN→DB] SitesMapping  Δ +%d  ~%d  -%d",
        len(patch_maps_sites.add),
        len(patch_maps_sites.update),
        len(patch_maps_sites.delete),
    )
    logger.info(
        "[YUMAN→DB] EquipsMapping Δ +%d  ~%d  -%d",
        len(patch_maps_equips.add),
        len(patch_maps_equips.update),
        len(patch_maps_equips.delete),
    )

    #4) on ré‑utilise les mêmes apply_*_patch de SupabaseAdapter
    sb.apply_clients_mapping_patch(patch_clients)
    sb.apply_sites_patch(patch_maps_sites)
    sb.apply_equips_mapping_patch(patch_maps_equips) 

    # -----------------------------------------------------------
    # PHASE 1 C – Résolution manuelle des conflits de sites
    # -----------------------------------------------------------
    logger.info("[CONFLIT] début de la résolution des conflits …")
    detect_and_resolve_site_conflicts(sb, y)
    resolve_clients_for_sites(sb, y)

    # --- re‑charge Supabase et yuman après résolution
    
    y_clients = list(y.yc.list_clients())
    y_sites   = y.fetch_sites()
    y_equips  = y.fetch_equips()
    
    sb_sites  = sb.fetch_sites_y()
    sb_equips = sb.fetch_equipments_y()
    # ➔ (filtrage ignore_site / site_key idem)
    sb_sites = {
                k: s
                for k, s in sb_sites.items()
                if not (getattr(s, "ignore_site", False) and getattr(s, "yuman_site_id", None) is None)
            }

    # -----------------------------------------------------------
    # PHASE 2 – Supabase ➜ Yuman  (diff + patch SANS refetch)
    # -----------------------------------------------------------
    logger.info("[DB→YUMAN] Synchronisation des sites…")
    patch_s = diff_entities(y_sites, sb_sites, ignore_fields={"client_map_id", "id", "ignore_site"})
    logger.info(
        "[DB→YUMAN] Sites Δ  +%d  ~%d  -%d",
        len(patch_s.add),
        len(patch_s.update),
        len(patch_s.delete),
    )
    y.apply_sites_patch(
        db_sites=sb_sites,
        y_sites=y_sites,
        patch=patch_s,
    )


    logger.info("[DB→YUMAN] Synchronisation des équipements…")

    # 1) mapping parent : vcom_device_id → yuman_material_id
    id_by_vcom = {
        e.vcom_device_id: e.yuman_material_id
        for e in y_equips.values()
        if e.yuman_material_id
    }
    set_parent_map(id_by_vcom)
    patch_e = diff_entities(y_equips, sb_equips, ignore_fields={"vcom_system_key", "yuman_site_id", "parent_id"})
    logger.info(
        "[DB→YUMAN] Equips Δ  +%d  ~%d  -%d",
        len(patch_e.add),
        len(patch_e.update),
        len(patch_e.delete),
    )
    y.apply_equips_patch(
        db_equips=sb_equips,
        y_equips=y_equips,
        patch=patch_e,
    )

    logger.info("✅ Synchronisation terminée")


if __name__ == "__main__":
    main()
