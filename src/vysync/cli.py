#!/usr/bin/env python3
from __future__ import annotations
"""
vysync.cli
===========

Orchestre la chaîne complète « VCOM → Supabase → Yuman ».

Usage :
    # Mode QUICK : détection rapide des nouveaux sites (quotidien)
    poetry run python -m vysync.cli --mode quick

    # Mode FULL : vérification complète de tous les sites (hebdomadaire)
    poetry run python -m vysync.cli --mode full

    # Site spécifique
    poetry run python -m vysync.cli --site-key TS9A8

Modes disponibles :
  quick : Détection rapide des nouveaux sites (1 appel VCOM, ~10 secondes)
  full  : Vérification complète de tous les sites (~2274 appels VCOM, ~25 minutes)

Étapes (mode FULL) :
1.   VCOM → snapshot local
2.   Diff avec la base Supabase     ➜  SupabaseAdapter.apply_*_patch
3.   Relecture Supabase (post-write)
4.   Diff (Supabase ➜ Yuman)        ➜  YumanAdapter.apply_*_patch
"""

import argparse
import logging

from vysync.app_logging import _dump
from vysync.adapters.vcom_adapter import fetch_snapshot
from vysync.adapters.supabase_adapter import SupabaseAdapter
from vysync.adapters.yuman_adapter import YumanAdapter
from vysync.vcom_client import VCOMAPIClient
from vysync.diff import diff_entities, diff_fill_missing, set_parent_map, PatchSet
from vysync.conflict_resolution import detect_and_resolve_site_conflicts, resolve_clients_for_sites

# ─────────────────────────── Logger ────────────────────────────
# Logger will be configured by setup_logging() in main()
logger = logging.getLogger(__name__)


# ────────────────────────── sync_quick ─────────────────────────
def sync_quick(vc: VCOMAPIClient, sb: SupabaseAdapter) -> None:
    """
    Mode rapide : détecte uniquement les nouveaux sites.
    Appelé quotidiennement via cron.

    Appels VCOM :
    - 1 appel : GET /systems
    - Si nouveau site détecté : cold start pour CE site uniquement

    Ce mode ne touche PAS à YumanAdapter (uniquement VCOM → DB).
    """
    logger.info("=" * 70)
    logger.info("[MODE QUICK] Detection des nouveaux sites")
    logger.info("=" * 70)

    # 1. Snapshot VCOM minimal (1 seul appel)
    try:
        vcom_systems = vc.get_systems()
        logger.info("[VCOM] %d systemes actifs retournes", len(vcom_systems))
    except Exception as e:
        logger.error("[ERREUR] Lors de GET /systems : %s", e)
        raise

    # 2. Charger cache DB (tous les sites connus)
    try:
        db_sites = sb.fetch_sites_v()
        known_keys = set(db_sites.keys())
        logger.info("[DB] %d sites deja connus", len(known_keys))
    except Exception as e:
        logger.error("[ERREUR] Lors du chargement DB : %s", e)
        raise

    # 3. Identifier les nouveaux sites
    vcom_keys = {s["key"] for s in vcom_systems}
    new_keys = sorted(vcom_keys - known_keys)

    if not new_keys:
        logger.info("[OK] Aucun nouveau site detecte")
        logger.info("     Total VCOM: %d | Total DB: %d | Nouveaux: 0",
                   len(vcom_keys), len(known_keys))
        return

    logger.info("[NOUVEAU] %d nouveau(x) site(s) detecte(s) : %s", len(new_keys), new_keys)

    # 4. Cold start pour chaque nouveau site
    for idx, key in enumerate(new_keys, 1):
        logger.info("-" * 70)
        logger.info("[COLD START] [%d/%d] Site : %s", idx, len(new_keys), key)

        try:
            # Fetch complet UNIQUEMENT pour ce site
            v_sites, v_equips = fetch_snapshot(
                vc,
                vcom_system_key=key,  # limite a CE site
                skip_keys=None
            )

            logger.info("             Recupere : %d site, %d equipements",
                       len(v_sites), len(v_equips))

            # 5. Diff et écriture en DB
            # On compare avec {} car c'est un nouveau site
            patch_sites = diff_entities(
                {}, v_sites,
                ignore_fields={"yuman_site_id", "client_map_id", "code", "ignore_site"}
            )
            patch_equips = diff_entities(
                {}, v_equips,
                ignore_fields={"yuman_material_id", "parent_id"}
            )

            logger.info("             Patch sites  : +%d ~%d -%d",
                       len(patch_sites.add),
                       len(patch_sites.update),
                       len(patch_sites.delete))
            logger.info("             Patch equips : +%d ~%d -%d",
                       len(patch_equips.add),
                       len(patch_equips.update),
                       len(patch_equips.delete))

            # Application en DB
            sb.apply_sites_patch(patch_sites)
            sb.apply_equips_patch(patch_equips)

            logger.info("[OK] Site %s synchronise avec succes", key)

        except Exception as e:
            logger.error("[ERREUR] Lors du traitement du site %s : %s", key, e)
            # On continue avec les autres sites
            continue

    logger.info("=" * 70)
    logger.info("[OK] MODE QUICK termine : %d nouveaux sites traites", len(new_keys))
    logger.info("=" * 70)


# ────────────────────────── sync_full ──────────────────────────
def sync_full(
    vc: VCOMAPIClient, 
    sb: SupabaseAdapter, 
    y: YumanAdapter, 
    site_key: str | None = None,
    maj_all: bool = False
) -> None:
    """
    Mode complet : vérification exhaustive de tous les sites.
    """
    logger.info("=" * 70)
    logger.info("[MODE FULL] Verification complete de tous les sites")
    logger.info("⚠️  Ce mode est LENT (~25 min) - utilisé pour audit hebdomadaire")
    logger.info("=" * 70)
    
    # ─────────────────────────────────────────────────────────────────
    # PHASE 1 A – VCOM → Supabase
    # ─────────────────────────────────────────────────────────────────
    db_sites = sb.fetch_sites_v(site_key=site_key)
    db_equips = sb.fetch_equipments_v(site_key=site_key)

    # ✅ CORRECTION : Le mode FULL ne skip JAMAIS (sauf si --site-key spécifique)
    # On veut TOUJOURS vérifier tous les sites pour détecter les changements
    v_sites, v_equips = fetch_snapshot(
        vc, 
        vcom_system_key=site_key,  # None = tous les sites, ou un site spécifique
        skip_keys=None              # ← JAMAIS skip en mode full
    )
    
    # Si --site-key spécifié, filtrer APRÈS le fetch
    if site_key:
        v_sites = {k: s for k, s in v_sites.items() if k == site_key}
        v_equips = {k: e for k, e in v_equips.items() if e.vcom_system_key == site_key}
        
        # Filtrer aussi la DB pour ne comparer que ce site
        db_sites = {k: s for k, s in db_sites.items() if k == site_key}
        db_equips = {k: e for k, e in db_equips.items() if e.vcom_system_key == site_key}

    # Diff & patch
    patch_sites = diff_entities(
        db_sites, v_sites, 
        ignore_fields={"yuman_site_id", "client_map_id", "code", "ignore_site"}
    )
    patch_equips = diff_entities(
        db_equips, v_equips, 
        ignore_fields={"yuman_material_id", "parent_id"}
    )

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
    
    while input("Écrivez 'oui' pour continuer : ").strip().lower() != "oui":
        pass

    sb.apply_sites_patch(patch_sites)
    sb.apply_equips_patch(patch_equips)

    # ─────────────────────────────────────────────────────────────────
    # PHASE 1 B – YUMAN → Supabase (mapping)
    # ─────────────────────────────────────────────────────────────────
    logger.info("[YUMAN→DB] snapshot & patch fill‑missing …")

    #1) on prend UN SEUL snapshot Yuman
    y_clients = list(y.yc.list_clients())
    y_sites   = y.fetch_sites()
    y_equips  = y.fetch_equips()
    # --- LOG snapshot Yuman
    logger.info("[YUMAN] snapshot: %d clients, %d sites, %d equips", len(y_clients), len(y_sites), len(y_equips))


    #2) on lit en base les mappings existants
    db_clients = sb.fetch_clients()      # -> Dict[int, Client]
    db_maps_sites  = sb.fetch_sites_y()    # -> Dict[int, SiteMapping]
    db_maps_equips = sb.fetch_equipments_y()   # -> Dict[str, EquipMapping]
    logger.info("[SB] snapshot:  %d clients, %d sites, %d equips", len(db_clients), len(db_maps_sites), len(db_maps_equips))


    #3) on génère des patchs « fill missing » (pas de supprimer)
    patch_clients = diff_fill_missing(db_clients,     {c["id"]: c for c in y_clients})
    patch_maps_sites  = diff_fill_missing(db_maps_sites,  y_sites, fields=["yuman_site_id","code", "client_map_id", "name",  "aldi_id","aldi_store_id","project_number_cp","commission_date","nominal_power"])

    patch_maps_equips = diff_fill_missing(db_maps_equips, y_equips, fields=["category_id","eq_type", "name", "yuman_material_id",
                                                                          "serial_number","brand","model","count","parent_id"])

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

    logger.info(
        "[SB] After requalify: ADD=%d, UPDATE=%d, DELETE=%d",
        len(patch_maps_equips.add), len(patch_maps_equips.update), len(patch_maps_equips.delete)
    )

    while input("Écrivez 'oui' pour continuer : ").strip().lower() != "oui":
        pass


    #4) on ré‑utilise les mêmes apply_*_patch de SupabaseAdapter
    sb.apply_clients_mapping_patch(patch_clients)
    sb.apply_sites_patch(patch_maps_sites)
    sb.apply_equips_mapping_patch(patch_maps_equips)

    logger.info("[YUMAN→DB] EquipsMapping patch applied: +%d ~%d -%d",
               len(patch_maps_equips.add),
               len(patch_maps_equips.update),
               len(patch_maps_equips.delete))

    # Log des 5 premiers updates pour debug
    if patch_maps_equips.update:
        logger.info("[DEBUG] Sample of first 5 updates:")
        for i, (old, new) in enumerate(list(patch_maps_equips.update)[:5]):
            logger.info("  [%d] serial=%s: old=%s new=%s",
                       i+1, new.serial_number, old.to_dict(), new.to_dict())

    # # ─────────────────────────────────────────────────────────────────
    # # PHASE 1 C – Résolution manuelle des conflits de sites
    # # ─────────────────────────────────────────────────────────────────
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
                if not getattr(s, "ignore_site", False)
            }

    # ─────────────────────────────────────────────────────────────────
    # PHASE 2 – Supabase ➜ Yuman  (diff + patch SANS refetch)
    # ─────────────────────────────────────────────────────────────────
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

    # 1) mapping parent : vcom_device_id → yuman_material_id
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

    logger.info("[OK] Synchronisation terminee")

    # Résumé des logs
    logger.info("=" * 60)
    logger.info("Execution terminee")
    logger.info("Consultez les fichiers de logs pour plus de details :")
    logger.info("  - logs/debug_*.log : logs complets (DEBUG)")
    logger.info("  - logs/updates_*.log : details des updates d'equipements")
    logger.info("=" * 60)


# ──────────────────────────── Main ─────────────────────────────
def main() -> None:
    """Point d'entrée CLI avec gestion des deux modes."""

    # Configuration du logging
    from vysync.logging_config import setup_logging
    setup_logging()

    # ───────────────────────────────────────────────────────────────
    # CLI arguments
    # ───────────────────────────────────────────────────────────────
    parser = argparse.ArgumentParser(
        description="Synchronise VCOM ↔ Supabase ↔ Yuman",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Modes disponibles :
  quick : Détection rapide des nouveaux sites (quotidien)
          → 1 appel VCOM, ~10 secondes

  full  : Vérification complète de tous les sites (hebdomadaire)
          → ~2274 appels VCOM, ~25 minutes
          → Détecte modifications sur sites existants

Exemples :
  # Détection quotidienne des nouveaux sites
  poetry run python -m vysync.cli --mode quick

  # Vérification hebdomadaire complète
  poetry run python -m vysync.cli --mode full

  # Forcer un site spécifique (mode full automatique)
  poetry run python -m vysync.cli --site-key TS9A8
        """
    )
    parser.add_argument(
        "--site-key",
        help="Traiter un seul site (force le mode FULL)",
    )
    parser.add_argument(
        "--maj-all",
        action="store_true",
        help="Forcer la mise à jour complète (ignorer cache DB)",
    )
    parser.add_argument(
        "--mode",
        choices=["quick", "full"],
        default="full",
        help="Mode de synchronisation (défaut: full)",
    )

    args = parser.parse_args()

    # ───────────────────────────────────────────────────────────────
    # Validation des arguments
    # ───────────────────────────────────────────────────────────────
    if args.mode == "quick" and (args.site_key or args.maj_all):
        logger.error("[ERREUR] --mode quick est incompatible avec --site-key ou --maj-all")
        logger.error("         Le mode quick detecte uniquement les nouveaux sites")
        logger.error("         Utilisez --mode full pour ces options")
        return

    # Si --site-key est spécifié, forcer le mode full
    if args.site_key and args.mode == "quick":
        logger.warning("[ATTENTION] --site-key detecte : passage automatique en mode FULL")
        args.mode = "full"

    # ───────────────────────────────────────────────────────────────
    # Initialisation des clients
    # ───────────────────────────────────────────────────────────────
    try:
        vc = VCOMAPIClient()
        sb = SupabaseAdapter()
        y = YumanAdapter(sb)
    except Exception as e:
        logger.error("[ERREUR] Lors de l'initialisation des clients : %s", e)
        raise

    # ───────────────────────────────────────────────────────────────
    # Exécution selon le mode
    # ───────────────────────────────────────────────────────────────
    try:
        if args.mode == "quick":
            sync_quick(vc, sb)
        else:
            sync_full(vc, sb, y, site_key=args.site_key, maj_all=args.maj_all)
    except KeyboardInterrupt:
        logger.warning("[ATTENTION] Interruption utilisateur (Ctrl+C)")
    except Exception as e:
        logger.error("[ERREUR] Durant la synchronisation : %s", e, exc_info=True)
        raise

    logger.info("=" * 70)
    logger.info("Execution terminee")
    logger.info("Consultez les logs pour plus de details :")
    logger.info("  - logs/debug_*.log : logs complets (DEBUG)")
    logger.info("  - logs/updates_*.log : details des updates")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
