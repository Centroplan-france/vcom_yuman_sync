#!/usr/bin/env python3
"""
CLI pour synchroniser les analytics mensuels VCOM → Supabase.

Récupère les métriques de production, irradiance, PR, availability et données
meters pour chaque site, puis les insère dans la table monthly_analytics.

Usage:
    # Synchro complète depuis commission_date de chaque site
    poetry run python -m vysync.cli_analytics --historical

    # Synchro uniquement le mois dernier (tous sites)
    poetry run python -m vysync.cli_analytics --last-month

    # Un site spécifique, historique complet
    poetry run python -m vysync.cli_analytics --site-key E3K2L --historical

    # Un site spécifique, dernier mois seulement
    poetry run python -m vysync.cli_analytics --site-key E3K2L --last-month

Modes disponibles:
  --historical : Synchronise depuis commission_date jusqu'au mois dernier
  --last-month : Synchronise uniquement le mois dernier complet

Options:
  --site-key   : Limite la synchro à un site spécifique (ex: E3K2L)
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from typing import List, Tuple

from vysync.vcom_client import VCOMAPIClient
from vysync.adapters.supabase_adapter import SupabaseAdapter
from vysync import vcom_analytics
from vysync.logging_config import setup_logging

logger = logging.getLogger(__name__)


# ────────────────────────── Database Operations ────────────────────────────


def upsert_monthly_analytics(
    sb: SupabaseAdapter,
    site_id: int,
    month: str,
    data: dict
) -> None:
    """
    Insert/update une ligne dans monthly_analytics.

    Args:
        sb: Adapter Supabase
        site_id: ID du site dans sites_mapping
        month: Mois au format "YYYY-MM-01" (ex: "2024-12-01")
        data: Dictionnaire avec les métriques du mois

    Structure data attendue:
        {
            "production_kwh": float | None,
            "irradiance_avg": float | None,
            "performance_ratio": float | None,
            "availability": float | None,
            "grid_export_kwh": float | None,
            "grid_import_kwh": float | None,
            "meter_id": str | None,
            "has_meter_data": bool
        }
    """
    now_iso = datetime.now(timezone.utc).isoformat()

    row = {
        "site_id": site_id,
        "month": month,
        "production_kwh": data.get("production_kwh"),
        "irradiance_avg": data.get("irradiance_avg"),
        "performance_ratio": data.get("performance_ratio"),
        "availability": data.get("availability"),
        "grid_export_kwh": data.get("grid_export_kwh"),
        "grid_import_kwh": data.get("grid_import_kwh"),
        "meter_id": data.get("meter_id"),
        "has_meter_data": data.get("has_meter_data", False),
        "updated_at": now_iso,
    }

    try:
        sb.sb.table("monthly_analytics").upsert(row, on_conflict="site_id,month").execute()
        logger.info("✓ Upsert analytics site_id=%d month=%s", site_id, month)
    except Exception as exc:
        logger.error("✗ Échec upsert analytics site_id=%d month=%s: %s",
                    site_id, month, exc)


# ────────────────────────── Synchronization Logic ────────────────────────────


def sync_site_analytics(
    vc: VCOMAPIClient,
    sb: SupabaseAdapter,
    system_key: str,
    site_id: int,
    months: List[Tuple[int, int]]
) -> None:
    """
    Synchronise les analytics d'un site pour une liste de mois.

    Args:
        vc: Client VCOM API
        sb: Adapter Supabase
        system_key: Clé du système VCOM (ex: "E3K2L")
        site_id: ID du site dans sites_mapping
        months: Liste de tuples (year, month), ex: [(2024, 12), (2025, 1)]
    """
    logger.info("-" * 70)
    logger.info("Synchronisation analytics pour %s (site_id=%d) - %d mois",
               system_key, site_id, len(months))

    success_count = 0
    error_count = 0

    for idx, (year, month) in enumerate(months, 1):
        logger.debug("[%d/%d] Processing %s %d-%02d",
                    idx, len(months), system_key, year, month)

        try:
            # Récupérer les analytics du mois
            analytics = vcom_analytics.fetch_monthly_analytics(
                vc, system_key, year, month
            )

            # Formater la date au format YYYY-MM-01
            month_str = f"{year:04d}-{month:02d}-01"

            # Upsert en DB
            upsert_monthly_analytics(sb, site_id, month_str, analytics)
            success_count += 1

        except Exception as exc:
            logger.error("Erreur lors du traitement de %s %d-%02d: %s",
                        system_key, year, month, exc)
            error_count += 1

    logger.info("Terminé %s: %d succès, %d erreurs",
               system_key, success_count, error_count)


def sync_all_sites_historical(
    vc: VCOMAPIClient,
    sb: SupabaseAdapter,
    site_key_filter: str | None = None
) -> None:
    """
    Mode --historical : tous les sites depuis commission_date.

    Pour chaque site:
    1. Lit commission_date depuis sites_mapping
    2. Génère la liste des mois depuis commission jusqu'au mois dernier
    3. Récupère et upsert les analytics pour chaque mois

    Args:
        vc: Client VCOM API
        sb: Adapter Supabase
        site_key_filter: Si fourni, ne traite que ce site
    """
    logger.info("=" * 70)
    logger.info("[MODE HISTORICAL] Synchronisation complète depuis commission_date")
    if site_key_filter:
        logger.info("Filtre actif: site_key=%s", site_key_filter)
    logger.info("=" * 70)

    # Récupérer tous les sites depuis sites_mapping
    sites = sb.fetch_sites_v(site_key=site_key_filter)

    if not sites:
        logger.warning("Aucun site trouvé en base de données")
        return

    logger.info("Sites à traiter: %d", len(sites))

    processed = 0
    skipped = 0

    for idx, (system_key, site) in enumerate(sorted(sites.items()), 1):
        logger.info("")
        logger.info("=" * 70)
        logger.info("[%d/%d] Site: %s (site_id=%d)", idx, len(sites), system_key, site.id)

        # Vérifier si site marqué à ignorer
        if site.ignore_site:
            logger.info("Site ignoré (ignore_site=True)")
            skipped += 1
            continue

        # Vérifier commission_date
        if not site.commission_date:
            logger.warning("Commission date manquante → skip")
            skipped += 1
            continue

        try:
            # Générer la liste des mois
            months = vcom_analytics.get_month_range(site.commission_date)

            if not months:
                logger.warning("Aucun mois à synchroniser pour %s", system_key)
                skipped += 1
                continue

            logger.info("Mois à synchroniser: %d (depuis %s)",
                       len(months), site.commission_date)

            # Synchroniser tous les mois du site
            sync_site_analytics(vc, sb, system_key, site.id, months)
            processed += 1

        except Exception as exc:
            logger.error("Erreur lors du traitement du site %s: %s", system_key, exc)
            skipped += 1

    logger.info("")
    logger.info("=" * 70)
    logger.info("RÉSUMÉ: %d sites traités, %d sites ignorés/échoués", processed, skipped)
    logger.info("=" * 70)


def sync_all_sites_last_month(
    vc: VCOMAPIClient,
    sb: SupabaseAdapter,
    site_key_filter: str | None = None
) -> None:
    """
    Mode --last-month : tous les sites, uniquement le mois dernier complet.

    Calcule le mois dernier complet et synchronise uniquement ce mois
    pour tous les sites.

    Args:
        vc: Client VCOM API
        sb: Adapter Supabase
        site_key_filter: Si fourni, ne traite que ce site
    """
    logger.info("=" * 70)
    logger.info("[MODE LAST-MONTH] Synchronisation du mois dernier uniquement")
    if site_key_filter:
        logger.info("Filtre actif: site_key=%s", site_key_filter)
    logger.info("=" * 70)

    # Calculer le mois dernier complet
    now = datetime.now(timezone.utc)
    if now.month == 1:
        last_month_year = now.year - 1
        last_month = 12
    else:
        last_month_year = now.year
        last_month = now.month - 1

    logger.info("Mois à synchroniser: %d-%02d", last_month_year, last_month)

    # Récupérer tous les sites
    sites = sb.fetch_sites_v(site_key=site_key_filter)

    if not sites:
        logger.warning("Aucun site trouvé en base de données")
        return

    logger.info("Sites à traiter: %d", len(sites))

    processed = 0
    skipped = 0

    for idx, (system_key, site) in enumerate(sorted(sites.items()), 1):
        logger.info("")
        logger.info("[%d/%d] Site: %s (site_id=%d)", idx, len(sites), system_key, site.id)

        # Vérifier si site marqué à ignorer
        if site.ignore_site:
            logger.info("Site ignoré (ignore_site=True)")
            skipped += 1
            continue

        # Vérifier si le site était déjà en service le mois dernier
        if site.commission_date:
            try:
                commission_dt = datetime.fromisoformat(
                    site.commission_date.replace("Z", "+00:00")
                )
                last_month_dt = datetime(last_month_year, last_month, 1, tzinfo=timezone.utc)

                if commission_dt > last_month_dt:
                    logger.info("Site pas encore en service le mois dernier → skip")
                    skipped += 1
                    continue
            except Exception as exc:
                logger.warning("Erreur parsing commission_date: %s", exc)

        try:
            # Synchroniser uniquement le mois dernier
            months = [(last_month_year, last_month)]
            sync_site_analytics(vc, sb, system_key, site.id, months)
            processed += 1

        except Exception as exc:
            logger.error("Erreur lors du traitement du site %s: %s", system_key, exc)
            skipped += 1

    logger.info("")
    logger.info("=" * 70)
    logger.info("RÉSUMÉ: %d sites traités, %d sites ignorés/échoués", processed, skipped)
    logger.info("=" * 70)


# ────────────────────────── CLI Entry Point ────────────────────────────


def main() -> None:
    """Point d'entrée CLI."""
    parser = argparse.ArgumentParser(
        description="Synchronise les analytics mensuels VCOM → Supabase",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples d'utilisation:
  # Synchro complète (tous sites, tout historique)
  poetry run python -m vysync.cli_analytics --historical

  # Dernier mois uniquement (tous sites)
  poetry run python -m vysync.cli_analytics --last-month

  # Un site spécifique, historique complet
  poetry run python -m vysync.cli_analytics --site-key E3K2L --historical

  # Un site spécifique, dernier mois seulement
  poetry run python -m vysync.cli_analytics --site-key E3K2L --last-month
        """
    )

    # Arguments mutuellement exclusifs pour le mode
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument(
        "--historical",
        action="store_true",
        help="Synchroniser depuis commission_date de chaque site"
    )
    mode_group.add_argument(
        "--last-month",
        action="store_true",
        help="Synchroniser uniquement le mois dernier complet"
    )

    # Filtre optionnel par site
    parser.add_argument(
        "--site-key",
        type=str,
        help="Limiter à un site spécifique (ex: E3K2L)"
    )

    args = parser.parse_args()

    # Initialiser le logging
    setup_logging()
    logger.info("Démarrage CLI Analytics VCOM")

    # Initialiser les clients
    try:
        vc = VCOMAPIClient()
        logger.info("Client VCOM initialisé")

        sb = SupabaseAdapter()
        logger.info("Client Supabase initialisé")

    except Exception as exc:
        logger.error("Erreur lors de l'initialisation des clients: %s", exc)
        sys.exit(1)

    # Lancer la synchronisation selon le mode
    try:
        if args.historical:
            sync_all_sites_historical(vc, sb, site_key_filter=args.site_key)
        elif args.last_month:
            sync_all_sites_last_month(vc, sb, site_key_filter=args.site_key)

        logger.info("")
        logger.info("✓ Synchronisation terminée avec succès")

    except KeyboardInterrupt:
        logger.warning("Interruption utilisateur (Ctrl+C)")
        sys.exit(130)
    except Exception as exc:
        logger.error("Erreur fatale: %s", exc, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
