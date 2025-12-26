#!/usr/bin/env python3
"""
vysync.cli
===========

CLI principal pour VYSYNC - Synchronisation VCOM ↔ Supabase ↔ Yuman.

Ce CLI unifie toutes les commandes de synchronisation disponibles,
aligné avec les workflows GitHub Actions.

Usage:
    vysync new-sites       # Détecte et crée les nouveaux sites VCOM
    vysync yuman-to-db     # Sync Yuman → Supabase (clients, sites, équipements)
    vysync auto-merge      # Fusionne automatiquement les paires VCOM/Yuman
    vysync db-to-yuman     # Sync Supabase → Yuman (sites + équipements)
    vysync tickets         # Sync tickets VCOM ↔ workorders Yuman
    vysync ppc             # Sync données PPC (hebdomadaire)
    vysync analytics       # Sync analytics mensuels

    vysync daily           # Exécute la séquence quotidienne complète
    vysync status          # Affiche un résumé de l'état de synchronisation

Workflows GitHub Actions correspondants:
    sync-new-sites.yml       → vysync new-sites      (5h UTC)
    sync_yuman_supabase.yml  → vysync yuman-to-db    (6h UTC)
    auto_merge_sites.yml     → vysync auto-merge     (7h UTC)
    sync_supabase_yuman.yml  → vysync db-to-yuman    (7h15 UTC)
    sync-tickets.yml         → vysync tickets        (5h UTC)
    sync_ppc_weekly.yml      → vysync ppc            (samedi 12h UTC)
    sync_analytics_monthly.yml → vysync analytics    (2 du mois 6h UTC)
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone

from vysync.logging_config import setup_logging

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# COMMANDES INDIVIDUELLES
# ═══════════════════════════════════════════════════════════════════════════════


def cmd_new_sites(args: argparse.Namespace) -> int:
    """
    Détecte et crée les nouveaux sites VCOM dans Supabase.

    Workflow:
    1. Liste tous les sites VCOM actifs
    2. Compare avec les sites existants en DB
    3. Crée les nouveaux sites + équipements
    4. Détecte les changements de nom

    Correspond à: sync-new-sites.yml (quotidien 5h UTC)
    """
    logger.info("=" * 70)
    logger.info("COMMANDE: new-sites (Sync nouveaux sites VCOM)")
    logger.info("=" * 70)

    from vysync.sync_new_sites import sync_new_sites_and_names

    try:
        report = sync_new_sites_and_names()

        if report["summary"]["new_sites_failed"] > 0:
            logger.warning("Certains sites n'ont pas pu être créés")
            return 1

        logger.info("Nouveaux sites créés: %d", report["summary"]["new_sites_created"])
        logger.info("Changements de nom: %d", report["summary"]["name_changes_detected"])
        return 0

    except Exception as e:
        logger.error("Erreur: %s", e, exc_info=True)
        return 1


def cmd_yuman_to_db(args: argparse.Namespace) -> int:
    """
    Synchronise Yuman → Supabase (clients, sites, équipements).

    Workflow:
    1. Sync clients (Yuman = source de vérité)
    2. Sync sites (fill if NULL + détection conflits client_map_id)
    3. Sync équipements (yuman_material_id + SIM brand/model)

    Correspond à: sync_yuman_supabase.yml (quotidien 6h UTC)
    """
    logger.info("=" * 70)
    logger.info("COMMANDE: yuman-to-db (Sync Yuman → Supabase)")
    logger.info("=" * 70)

    from vysync.sync_yuman_to_supabase import main as sync_yuman_main

    try:
        report = sync_yuman_main()
        return 0 if report.get("success", True) else 1
    except Exception as e:
        logger.error("Erreur: %s", e, exc_info=True)
        return 1


def cmd_auto_merge(args: argparse.Namespace) -> int:
    """
    Fusionne automatiquement les paires VCOM/Yuman détectées.

    Workflow:
    1. Détecte les paires potentielles (HIGH + MEDIUM confidence)
    2. Fusionne automatiquement toutes les paires trouvées
    3. Envoie un email d'alerte si sites VCOM sans paire

    Correspond à: auto_merge_sites.yml (quotidien 7h UTC)
    """
    logger.info("=" * 70)
    logger.info("COMMANDE: auto-merge (Fusion automatique VCOM/Yuman)")
    logger.info("=" * 70)

    # Injection des arguments pour auto_merge
    import sys
    original_argv = sys.argv

    sys_args = ["auto_merge_sites"]
    if args.execute:
        sys_args.append("--execute")
    if args.dry_run:
        pass  # dry-run est le défaut
    if hasattr(args, 'test_email') and args.test_email:
        sys_args.append("--test-email")

    sys.argv = sys_args

    try:
        from vysync.auto_merge_sites import main as auto_merge_main
        result = auto_merge_main()
        return result if result is not None else 0
    except Exception as e:
        logger.error("Erreur: %s", e, exc_info=True)
        return 1
    finally:
        sys.argv = original_argv


def cmd_db_to_yuman(args: argparse.Namespace) -> int:
    """
    Synchronise Supabase → Yuman (sites + équipements).

    Workflow:
    1. Lit l'état Supabase (source de vérité)
    2. Lit l'état Yuman (état actuel)
    3. Calcule le diff
    4. Applique les changements dans Yuman

    Correspond à: sync_supabase_yuman.yml (quotidien 7h15 UTC)
    """
    logger.info("=" * 70)
    logger.info("COMMANDE: db-to-yuman (Sync Supabase → Yuman)")
    logger.info("=" * 70)

    from vysync.sync_supabase_to_yuman import sync_supabase_to_yuman

    try:
        report = sync_supabase_to_yuman(
            site_key=args.site_key,
            dry_run=args.dry_run,
            auto_confirm=args.yes,
        )
        return 0 if report.get("success", True) else 1
    except Exception as e:
        logger.error("Erreur: %s", e, exc_info=True)
        return 1


def cmd_tickets(args: argparse.Namespace) -> int:
    """
    Synchronise tickets VCOM ↔ workorders Yuman.

    Workflow:
    1. Récupère les tickets VCOM (open/assigned/inProgress)
    2. Récupère les workorders Yuman
    3. Enrichit les workorders avec les tickets
    4. Crée des workorders pour les sites prioritaires
    5. Ferme les tickets des workorders clôturés

    Correspond à: sync-tickets.yml (quotidien 5h UTC)
    """
    logger.info("=" * 70)
    logger.info("COMMANDE: tickets (Sync tickets VCOM ↔ workorders Yuman)")
    logger.info("=" * 70)

    # Injection des arguments
    import sys
    original_argv = sys.argv

    sys_args = ["sync_tickets_workorders"]
    if args.dry_run:
        sys_args.append("--dry-run")

    sys.argv = sys_args

    try:
        from vysync.sync_tickets_workorders import main as tickets_main
        tickets_main()
        return 0
    except Exception as e:
        logger.error("Erreur: %s", e, exc_info=True)
        return 1
    finally:
        sys.argv = original_argv


def cmd_ppc(args: argparse.Namespace) -> int:
    """
    Synchronise les données PPC (Power Plant Controllers).

    Récupère hebdomadairement la consigne de puissance active depuis VCOM
    et la stocke dans Supabase.

    Correspond à: sync_ppc_weekly.yml (samedi 12h UTC)
    """
    logger.info("=" * 70)
    logger.info("COMMANDE: ppc (Sync données PPC)")
    logger.info("=" * 70)

    from vysync.sync_ppc_data import sync_all_sites

    try:
        sync_all_sites()
        return 0
    except Exception as e:
        logger.error("Erreur: %s", e, exc_info=True)
        return 1


def cmd_analytics(args: argparse.Namespace) -> int:
    """
    Synchronise les analytics mensuels VCOM → Supabase.

    Modes:
    - --historical : Sync depuis commission_date de chaque site
    - --last-month : Sync uniquement le mois dernier complet (défaut)

    Correspond à: sync_analytics_monthly.yml (2 du mois 6h UTC)
    """
    logger.info("=" * 70)
    logger.info("COMMANDE: analytics (Sync analytics mensuels)")
    logger.info("=" * 70)

    from vysync.vcom_client import VCOMAPIClient
    from vysync.adapters.supabase_adapter import SupabaseAdapter
    from vysync.cli_analytics import sync_all_sites_historical, sync_all_sites_last_month

    try:
        vc = VCOMAPIClient()
        sb = SupabaseAdapter()

        if args.historical:
            sync_all_sites_historical(vc, sb, site_key_filter=args.site_key, force=args.force)
        else:
            # --last-month est le défaut
            sync_all_sites_last_month(vc, sb, site_key_filter=args.site_key)

        return 0
    except Exception as e:
        logger.error("Erreur: %s", e, exc_info=True)
        return 1


def cmd_daily(args: argparse.Namespace) -> int:
    """
    Exécute la séquence quotidienne complète.

    Ordre d'exécution (comme les GitHub Actions):
    1. new-sites    (5h UTC)
    2. yuman-to-db  (6h UTC)
    3. auto-merge   (7h UTC)
    4. db-to-yuman  (7h15 UTC)
    """
    logger.info("=" * 70)
    logger.info("COMMANDE: daily (Séquence quotidienne complète)")
    logger.info("Date: %s", datetime.now(timezone.utc).isoformat())
    logger.info("=" * 70)

    steps = [
        ("new-sites", cmd_new_sites),
        ("yuman-to-db", cmd_yuman_to_db),
        ("auto-merge", cmd_auto_merge),
        ("db-to-yuman", cmd_db_to_yuman),
    ]

    results = {}

    for step_name, step_func in steps:
        logger.info("")
        logger.info("=" * 70)
        logger.info("ÉTAPE: %s", step_name)
        logger.info("=" * 70)

        try:
            # Créer un namespace avec les options par défaut
            step_args = argparse.Namespace(
                dry_run=args.dry_run,
                yes=True,  # Auto-confirm pour daily
                execute=not args.dry_run,
                site_key=None,
            )

            result = step_func(step_args)
            results[step_name] = "OK" if result == 0 else "ERREUR"

            if result != 0 and not args.continue_on_error:
                logger.error("Étape %s échouée, arrêt de la séquence", step_name)
                break

        except Exception as e:
            logger.error("Erreur lors de l'étape %s: %s", step_name, e)
            results[step_name] = f"EXCEPTION: {e}"
            if not args.continue_on_error:
                break

    # Résumé
    logger.info("")
    logger.info("=" * 70)
    logger.info("RÉSUMÉ SÉQUENCE QUOTIDIENNE")
    logger.info("=" * 70)
    for step_name, status in results.items():
        logger.info("  %s: %s", step_name.ljust(15), status)
    logger.info("=" * 70)

    # Code de retour
    has_errors = any(v != "OK" for v in results.values())
    return 1 if has_errors else 0


def cmd_status(args: argparse.Namespace) -> int:
    """
    Affiche un résumé de l'état de synchronisation.
    """
    logger.info("=" * 70)
    logger.info("COMMANDE: status (État de synchronisation)")
    logger.info("=" * 70)

    from vysync.adapters.supabase_adapter import SupabaseAdapter

    try:
        sb = SupabaseAdapter()

        # Sites
        sites_result = sb.sb.table("sites_mapping").select("id, vcom_system_key, yuman_site_id, ignore_site").execute()
        sites = sites_result.data or []

        total_sites = len(sites)
        complete_sites = len([s for s in sites if s.get("vcom_system_key") and s.get("yuman_site_id")])
        vcom_only = len([s for s in sites if s.get("vcom_system_key") and not s.get("yuman_site_id")])
        yuman_only = len([s for s in sites if not s.get("vcom_system_key") and s.get("yuman_site_id")])
        ignored = len([s for s in sites if s.get("ignore_site")])

        # Équipements
        equips_result = sb.sb.table("equipments_mapping").select("id, yuman_material_id").execute()
        equips = equips_result.data or []

        total_equips = len(equips)
        linked_equips = len([e for e in equips if e.get("yuman_material_id")])

        # Affichage
        logger.info("")
        logger.info("SITES:")
        logger.info("  Total         : %d", total_sites)
        logger.info("  Complets      : %d (VCOM + Yuman)", complete_sites)
        logger.info("  VCOM seul     : %d", vcom_only)
        logger.info("  Yuman seul    : %d", yuman_only)
        logger.info("  Ignorés       : %d", ignored)
        logger.info("")
        logger.info("ÉQUIPEMENTS:")
        logger.info("  Total         : %d", total_equips)
        logger.info("  Liés Yuman    : %d", linked_equips)
        logger.info("  Non liés      : %d", total_equips - linked_equips)
        logger.info("")
        logger.info("=" * 70)

        return 0

    except Exception as e:
        logger.error("Erreur: %s", e, exc_info=True)
        return 1


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════


def main() -> None:
    """Point d'entrée CLI principal."""

    parser = argparse.ArgumentParser(
        prog="vysync",
        description="VYSYNC - Synchronisation VCOM ↔ Supabase ↔ Yuman",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Commandes disponibles:
  new-sites      Détecte et crée les nouveaux sites VCOM
  yuman-to-db    Sync Yuman → Supabase (clients, sites, équipements)
  auto-merge     Fusionne automatiquement les paires VCOM/Yuman
  db-to-yuman    Sync Supabase → Yuman (sites + équipements)
  tickets        Sync tickets VCOM ↔ workorders Yuman
  ppc            Sync données PPC (hebdomadaire)
  analytics      Sync analytics mensuels
  daily          Exécute la séquence quotidienne complète
  status         Affiche l'état de synchronisation

Exemples:
  # Détecter les nouveaux sites
  vysync new-sites

  # Sync quotidienne complète (avec auto-confirmation)
  vysync daily

  # Sync DB → Yuman pour un site spécifique
  vysync db-to-yuman --site-key 2KC5K

  # Mode dry-run pour analytics
  vysync analytics --last-month --dry-run
        """
    )

    subparsers = parser.add_subparsers(dest="command", help="Commande à exécuter")

    # ─────────────────────────────────────────────────────────────
    # new-sites
    # ─────────────────────────────────────────────────────────────
    p_new = subparsers.add_parser(
        "new-sites",
        help="Détecte et crée les nouveaux sites VCOM"
    )
    p_new.set_defaults(func=cmd_new_sites)

    # ─────────────────────────────────────────────────────────────
    # yuman-to-db
    # ─────────────────────────────────────────────────────────────
    p_yuman = subparsers.add_parser(
        "yuman-to-db",
        help="Sync Yuman → Supabase (clients, sites, équipements)"
    )
    p_yuman.set_defaults(func=cmd_yuman_to_db)

    # ─────────────────────────────────────────────────────────────
    # auto-merge
    # ─────────────────────────────────────────────────────────────
    p_merge = subparsers.add_parser(
        "auto-merge",
        help="Fusionne automatiquement les paires VCOM/Yuman"
    )
    p_merge.add_argument(
        "--execute",
        action="store_true",
        help="Exécuter réellement les fusions (sinon dry-run)"
    )
    p_merge.add_argument(
        "--dry-run",
        action="store_true",
        help="Mode diagnostic uniquement (défaut)"
    )
    p_merge.add_argument(
        "--test-email",
        action="store_true",
        help="Envoyer l'email même en dry-run"
    )
    p_merge.set_defaults(func=cmd_auto_merge)

    # ─────────────────────────────────────────────────────────────
    # db-to-yuman
    # ─────────────────────────────────────────────────────────────
    p_db = subparsers.add_parser(
        "db-to-yuman",
        help="Sync Supabase → Yuman (sites + équipements)"
    )
    p_db.add_argument(
        "--site-key",
        type=str,
        help="Filtrer sur un site spécifique (ex: 2KC5K)"
    )
    p_db.add_argument(
        "--dry-run",
        action="store_true",
        help="Mode diagnostic uniquement"
    )
    p_db.add_argument(
        "--yes", "-y",
        action="store_true",
        help="Confirmer automatiquement"
    )
    p_db.set_defaults(func=cmd_db_to_yuman)

    # ─────────────────────────────────────────────────────────────
    # tickets
    # ─────────────────────────────────────────────────────────────
    p_tickets = subparsers.add_parser(
        "tickets",
        help="Sync tickets VCOM ↔ workorders Yuman"
    )
    p_tickets.add_argument(
        "--dry-run",
        action="store_true",
        help="Mode diagnostic uniquement"
    )
    p_tickets.set_defaults(func=cmd_tickets)

    # ─────────────────────────────────────────────────────────────
    # ppc
    # ─────────────────────────────────────────────────────────────
    p_ppc = subparsers.add_parser(
        "ppc",
        help="Sync données PPC (Power Plant Controllers)"
    )
    p_ppc.set_defaults(func=cmd_ppc)

    # ─────────────────────────────────────────────────────────────
    # analytics
    # ─────────────────────────────────────────────────────────────
    p_analytics = subparsers.add_parser(
        "analytics",
        help="Sync analytics mensuels VCOM → Supabase"
    )
    mode_group = p_analytics.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--historical",
        action="store_true",
        help="Sync depuis commission_date de chaque site"
    )
    mode_group.add_argument(
        "--last-month",
        action="store_true",
        default=True,
        help="Sync uniquement le mois dernier (défaut)"
    )
    p_analytics.add_argument(
        "--site-key",
        type=str,
        help="Limiter à un site spécifique"
    )
    p_analytics.add_argument(
        "--force",
        action="store_true",
        help="Forcer la re-synchronisation"
    )
    p_analytics.set_defaults(func=cmd_analytics)

    # ─────────────────────────────────────────────────────────────
    # daily
    # ─────────────────────────────────────────────────────────────
    p_daily = subparsers.add_parser(
        "daily",
        help="Exécute la séquence quotidienne complète"
    )
    p_daily.add_argument(
        "--dry-run",
        action="store_true",
        help="Mode diagnostic uniquement"
    )
    p_daily.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continuer même si une étape échoue"
    )
    p_daily.set_defaults(func=cmd_daily)

    # ─────────────────────────────────────────────────────────────
    # status
    # ─────────────────────────────────────────────────────────────
    p_status = subparsers.add_parser(
        "status",
        help="Affiche l'état de synchronisation"
    )
    p_status.set_defaults(func=cmd_status)

    # ─────────────────────────────────────────────────────────────
    # Parse et exécution
    # ─────────────────────────────────────────────────────────────
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(0)

    # Configuration du logging
    setup_logging()

    # Exécution de la commande
    try:
        result = args.func(args)
        sys.exit(result if result is not None else 0)
    except KeyboardInterrupt:
        logger.warning("Interruption utilisateur (Ctrl+C)")
        sys.exit(130)
    except Exception as e:
        logger.error("Erreur fatale: %s", e, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
