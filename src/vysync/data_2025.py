#!/usr/bin/env python3
"""
Script de rattrapage des données monthly_analytics manquantes pour 2025.

Récupère les mois manquants (janvier → octobre 2025) pour tous les sites
éligibles et les synchronise dans Supabase.

Usage:
    poetry run python -m vysync.data_2025
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone
from typing import Dict, Set, Any

from vysync.vcom_client import VCOMAPIClient
from vysync.adapters.supabase_adapter import SupabaseAdapter
from vysync.logging_config import setup_logging

logger = logging.getLogger(__name__)

# Constantes
MOIS_2025 = list(range(1, 13))  # Janvier → Décembre (toute l'année)
FROM_DATE = "2025-01-01T00:00:00+01:00"
TO_DATE = "2025-12-31T23:59:59+01:00"


# ────────────────────────── Supabase Queries ────────────────────────────


def fetch_sites_eligibles(sb: SupabaseAdapter) -> Dict[int, Dict[str, Any]]:
    """
    Récupère les sites éligibles depuis Supabase.
    
    Critères:
    - ignore_site = false
    - commission_date IS NOT NULL
    - commission_date <= 2025-12-31

    Returns:
        Dict[site_id, {system_key, commission_date, meter_id}]
    """
    result = sb.sb.table("sites_mapping")\
        .select("id, vcom_system_key, commission_date, vcom_meter_id")\
        .eq("ignore_site", False)\
        .not_.is_("commission_date", "null")\
        .lte("commission_date", "2025-12-31")\
        .not_.is_("vcom_system_key", "null")\
        .execute()
    
    sites = {}
    for row in result.data:
        sites[row["id"]] = {
            "system_key": row["vcom_system_key"],
            "commission_date": row["commission_date"],
            "meter_id": row["vcom_meter_id"]
        }
    
    logger.info("Sites éligibles récupérés: %d", len(sites))
    return sites


def fetch_mois_existants(sb: SupabaseAdapter) -> Dict[int, Set[int]]:
    """
    Récupère les mois déjà présents dans monthly_analytics pour 2025.

    Returns:
        Dict[site_id, set de mois (1-12)]
    """
    result = sb.sb.table("monthly_analytics")\
        .select("site_id, month")\
        .gte("month", "2025-01-01")\
        .lte("month", "2025-12-01")\
        .execute()
    
    existants: Dict[int, Set[int]] = {}
    for row in result.data:
        site_id = row["site_id"]
        mois = int(row["month"].split("-")[1])
        
        if site_id not in existants:
            existants[site_id] = set()
        existants[site_id].add(mois)
    
    logger.info("Mois existants récupérés pour %d sites", len(existants))
    return existants


# ────────────────────────── Calcul des manquants ────────────────────────────


def calculer_mois_attendus(commission_date: str) -> Set[int]:
    """
    Calcule les mois attendus pour un site selon sa commission_date.

    Returns:
        Set de mois (1-12) attendus
    """
    try:
        dt = datetime.fromisoformat(commission_date.replace("Z", "+00:00"))

        # Si commissionné avant 2025, on attend tous les mois
        if dt.year < 2025:
            return set(MOIS_2025)

        # Si commissionné en 2025, on commence au mois de commission
        if dt.year == 2025 and dt.month <= 12:
            return set(range(dt.month, 13))

        # Si commissionné après 2025, aucun mois attendu
        return set()

    except Exception as exc:
        logger.warning("Erreur parsing commission_date %s: %s", commission_date, exc)
        return set()


def calculer_mois_manquants(
    sites: Dict[int, Dict[str, Any]],
    existants: Dict[int, Set[int]]
) -> Dict[int, Set[int]]:
    """
    Calcule les mois manquants pour chaque site.
    
    Returns:
        Dict[site_id, set de mois manquants]
    """
    manquants: Dict[int, Set[int]] = {}
    
    for site_id, site_info in sites.items():
        attendus = calculer_mois_attendus(site_info["commission_date"])
        presents = existants.get(site_id, set())
        delta = attendus - presents
        
        if delta:
            manquants[site_id] = delta
    
    total_mois = sum(len(m) for m in manquants.values())
    logger.info("Mois manquants calculés: %d mois sur %d sites", total_mois, len(manquants))
    
    return manquants


# ────────────────────────── VCOM Bulk Fetch ────────────────────────────


def fetch_bulk_analytics(
    vc: VCOMAPIClient,
    sites: Dict[int, Dict[str, Any]]
) -> Dict[str, Dict[int, Dict[str, float | None]]]:
    """
    Récupère les métriques principales en bulk + fallback pour G_M0.

    - E_Z_EVU, PR, VFG : endpoint bulk cross-sites (3 appels)
    - G_M0 : endpoint par site (N appels, car bulk retourne 404)

    Args:
        vc: Client VCOM API
        sites: Dict[site_id, {system_key, commission_date, meter_id}]

    Returns:
        Dict[system_key, Dict[mois, {production, irradiance, pr, availability}]]
    """
    # Abréviations qui fonctionnent en bulk (G_M0 retourne 404 en bulk)
    bulk_abbreviations = {
        "E_Z_EVU": "production_kwh",
        "PR": "performance_ratio",
        "VFG": "availability"
    }

    # Structure: {system_key: {mois: {metric: value}}}
    data: Dict[str, Dict[int, Dict[str, float | None]]] = {}

    # 1. Fetch bulk pour E_Z_EVU, PR, VFG
    for abbrev, metric_name in bulk_abbreviations.items():
        logger.info("Fetch bulk %s...", abbrev)

        try:
            results = vc.get_bulk_measurements(abbrev, FROM_DATE, TO_DATE, resolution="month")

            for item in results:
                system_key = item.get("systemKey")
                if not system_key:
                    continue

                if system_key not in data:
                    data[system_key] = {}

                measurements = item.get(abbrev, [])
                for measure in measurements:
                    timestamp = measure.get("timestamp", "")
                    value = measure.get("value")

                    # Extraire le mois du timestamp
                    try:
                        mois = int(timestamp.split("-")[1])
                    except (IndexError, ValueError):
                        continue

                    if mois not in data[system_key]:
                        data[system_key][mois] = {}

                    # Convertir en float si possible
                    if value is not None:
                        try:
                            data[system_key][mois][metric_name] = float(value)
                        except (ValueError, TypeError):
                            data[system_key][mois][metric_name] = None
                    else:
                        data[system_key][mois][metric_name] = None

        except Exception as exc:
            logger.error("Erreur fetch bulk %s: %s", abbrev, exc)

    # 2. Fetch G_M0 par site (car bulk retourne 404)
    logger.info("Fetch G_M0 par site (fallback, car bulk retourne 404)...")
    fetched_count = 0
    error_count = 0

    for site_id, site_info in sites.items():
        system_key = site_info["system_key"]
        try:
            response = vc._make_request(
                "GET",
                f"/systems/{system_key}/basics/abbreviations/G_M0/measurements",
                params={"from": FROM_DATE, "to": TO_DATE, "resolution": "month"}
            )
            measurements = response.json().get("data", {}).get("G_M0", [])

            for measure in measurements:
                timestamp = measure.get("timestamp", "")
                value = measure.get("value")
                try:
                    mois = int(timestamp.split("-")[1])
                except (IndexError, ValueError):
                    continue

                if system_key not in data:
                    data[system_key] = {}
                if mois not in data[system_key]:
                    data[system_key][mois] = {}

                if value is not None:
                    try:
                        data[system_key][mois]["irradiance_avg"] = float(value)
                        fetched_count += 1
                    except (ValueError, TypeError):
                        data[system_key][mois]["irradiance_avg"] = None

        except Exception as exc:
            logger.debug("Erreur fetch G_M0 pour %s: %s", system_key, exc)
            error_count += 1

    logger.info("G_M0 récupéré: %d valeurs, %d erreurs", fetched_count, error_count)
    logger.info("Données bulk récupérées pour %d systèmes", len(data))
    return data


# ────────────────────────── Meters Fetch ────────────────────────────


def get_or_fetch_meter_id(
    vc: VCOMAPIClient,
    sb: SupabaseAdapter,
    system_key: str,
    site_id: int
) -> str | None:
    """
    Récupère le meter_id depuis le cache ou l'API VCOM.
    """
    # Vérifier le cache en DB
    result = sb.sb.table("sites_mapping")\
        .select("vcom_meter_id")\
        .eq("id", site_id)\
        .single()\
        .execute()
    
    cached = result.data.get("vcom_meter_id")
    if cached:
        return cached
    
    # Fetch depuis VCOM
    try:
        response = vc._make_request("GET", f"/systems/{system_key}/meters")
        meters = response.json().get("data", [])
        
        if not meters:
            logger.debug("Aucun meter pour %s", system_key)
            return None
        
        meter_id = meters[0].get("id")
        
        # Cache en DB
        sb.sb.table("sites_mapping")\
            .update({"vcom_meter_id": meter_id})\
            .eq("id", site_id)\
            .execute()
        
        logger.info("Meter %s mis en cache pour site_id=%d", meter_id, site_id)
        return meter_id
    
    except Exception as exc:
        logger.warning("Erreur récupération meter pour %s: %s", system_key, exc)
        return None


def fetch_meters_data(
    vc: VCOMAPIClient,
    sb: SupabaseAdapter,
    sites: Dict[int, Dict[str, Any]],
    manquants: Dict[int, Set[int]]
) -> Dict[str, Dict[int, Dict[str, float | None]]]:
    """
    Récupère les données meters pour les sites avec des mois manquants.
    
    Returns:
        Dict[system_key, Dict[mois, {grid_export, grid_import, meter_id}]]
    """
    data: Dict[str, Dict[int, Dict[str, float | None]]] = {}
    
    # Filtrer les sites qui ont des mois manquants
    sites_a_traiter = {
        site_id: sites[site_id]
        for site_id in manquants.keys()
        if site_id in sites
    }
    
    logger.info("Récupération meters pour %d sites...", len(sites_a_traiter))
    
    for idx, (site_id, site_info) in enumerate(sites_a_traiter.items(), 1):
        system_key = site_info["system_key"]
        
        # Récupérer ou fetcher le meter_id
        meter_id = site_info.get("meter_id")
        if not meter_id:
            meter_id = get_or_fetch_meter_id(vc, sb, system_key, site_id)
        
        if not meter_id:
            logger.debug("[%d/%d] %s: pas de meter, skip", idx, len(sites_a_traiter), system_key)
            continue
        
        logger.debug("[%d/%d] Fetch meters pour %s (meter_id=%s)", 
                    idx, len(sites_a_traiter), system_key, meter_id)
        
        try:
            # Fetch les deux abbreviations en une requête
            response = vc._make_request(
                "GET",
                f"/systems/{system_key}/meters/{meter_id}/abbreviations/M_AC_E_EXP,M_AC_E_IMP/measurements",
                params={
                    "from": FROM_DATE,
                    "to": TO_DATE,
                    "resolution": "month"
                }
            )
            
            result = response.json().get("data", {})
            meter_data = result.get(meter_id, {})
            
            if system_key not in data:
                data[system_key] = {}
            
            # Traiter M_AC_E_EXP (export)
            for measure in meter_data.get("M_AC_E_EXP", []):
                timestamp = measure.get("timestamp", "")
                try:
                    mois = int(timestamp.split("-")[1])
                except (IndexError, ValueError):
                    continue
                
                if mois not in data[system_key]:
                    data[system_key][mois] = {"meter_id": meter_id}
                
                value = measure.get("value")
                if value is not None:
                    try:
                        data[system_key][mois]["grid_export_kwh"] = float(value)
                    except (ValueError, TypeError):
                        pass
            
            # Traiter M_AC_E_IMP (import)
            for measure in meter_data.get("M_AC_E_IMP", []):
                timestamp = measure.get("timestamp", "")
                try:
                    mois = int(timestamp.split("-")[1])
                except (IndexError, ValueError):
                    continue
                
                if mois not in data[system_key]:
                    data[system_key][mois] = {"meter_id": meter_id}
                
                value = measure.get("value")
                if value is not None:
                    try:
                        data[system_key][mois]["grid_import_kwh"] = float(value)
                    except (ValueError, TypeError):
                        pass
        
        except Exception as exc:
            logger.warning("Erreur fetch meters pour %s: %s", system_key, exc)
    
    logger.info("Données meters récupérées pour %d systèmes", len(data))
    return data


# ────────────────────────── Fetch Existing Data ────────────────────────────


def fetch_existing_analytics(
    sb: SupabaseAdapter,
    site_id: int,
    months: Set[int]
) -> Dict[int, Dict[str, Any]]:
    """
    Récupère les données existantes en base pour un site et une liste de mois.

    Args:
        sb: Adapter Supabase
        site_id: ID du site
        months: Set de mois (1-12) à récupérer

    Returns:
        Dict[mois, {production_kwh, irradiance_avg, ...}]
    """
    result = sb.sb.table("monthly_analytics")\
        .select("*")\
        .eq("site_id", site_id)\
        .gte("month", "2025-01-01")\
        .lte("month", "2025-12-31")\
        .execute()

    existing: Dict[int, Dict[str, Any]] = {}
    for row in result.data:
        mois = int(row["month"].split("-")[1])
        if mois in months:
            existing[mois] = row

    return existing


# ────────────────────────── Smart Upsert ────────────────────────────


def smart_upsert_monthly_analytics(
    sb: SupabaseAdapter,
    site_id: int,
    mois: int,
    new_data: Dict[str, Any],
    existing_data: Dict[str, Any] | None
) -> None:
    """
    Upsert intelligent : ne jamais écraser une valeur existante avec NULL.

    Règles :
    - Si new_data[field] est non-NULL → utiliser new_data[field]
    - Si new_data[field] est NULL et existing_data[field] est non-NULL → garder existing_data[field]
    - Si les deux sont NULL → NULL

    Args:
        sb: Adapter Supabase
        site_id: ID du site
        mois: Mois (1-12)
        new_data: Nouvelles données à insérer
        existing_data: Données existantes en base (ou None si pas de données)
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    month_str = f"2025-{mois:02d}-01"

    # Champs à fusionner
    fields = [
        "production_kwh",
        "irradiance_avg",
        "performance_ratio",
        "availability",
        "grid_export_kwh",
        "grid_import_kwh",
        "meter_id"
    ]

    row = {
        "site_id": site_id,
        "month": month_str,
        "updated_at": now_iso,
    }

    for field in fields:
        new_value = new_data.get(field)
        existing_value = existing_data.get(field) if existing_data else None

        # Règle : ne jamais écraser une valeur existante avec NULL
        if new_value is not None:
            row[field] = new_value
        elif existing_value is not None:
            row[field] = existing_value
        else:
            row[field] = None

    # Recalculer is_complete et has_meter_data
    row["is_complete"] = all([
        row.get("production_kwh") is not None,
        row.get("irradiance_avg") is not None,
        row.get("performance_ratio") is not None,
        row.get("availability") is not None
    ])

    row["has_meter_data"] = any([
        row.get("grid_export_kwh") is not None,
        row.get("grid_import_kwh") is not None
    ])

    sb.sb.table("monthly_analytics").upsert(row, on_conflict="site_id,month").execute()


# ────────────────────────── Legacy Upsert (unused) ────────────────────────────


def upsert_monthly_analytics(
    sb: SupabaseAdapter,
    site_id: int,
    mois: int,
    analytics: Dict[str, Any],
    meter_data: Dict[str, Any] | None
) -> None:
    """
    Upsert une ligne dans monthly_analytics.
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    month_str = f"2025-{mois:02d}-01"
    
    # Fusionner analytics et meter_data
    production = analytics.get("production_kwh")
    irradiance = analytics.get("irradiance_avg")
    pr = analytics.get("performance_ratio")
    availability = analytics.get("availability")
    
    grid_export = meter_data.get("grid_export_kwh") if meter_data else None
    grid_import = meter_data.get("grid_import_kwh") if meter_data else None
    meter_id = meter_data.get("meter_id") if meter_data else None
    
    is_complete = all([
        production is not None,
        irradiance is not None,
        pr is not None,
        availability is not None
    ])
    
    has_meter_data = any([grid_export is not None, grid_import is not None])
    
    row = {
        "site_id": site_id,
        "month": month_str,
        "production_kwh": production,
        "irradiance_avg": irradiance,
        "performance_ratio": pr,
        "availability": availability,
        "grid_export_kwh": grid_export,
        "grid_import_kwh": grid_import,
        "meter_id": meter_id,
        "has_meter_data": has_meter_data,
        "is_complete": is_complete,
        "updated_at": now_iso,
    }
    
    sb.sb.table("monthly_analytics").upsert(row, on_conflict="site_id,month").execute()


def sync_missing_months(
    sb: SupabaseAdapter,
    sites: Dict[int, Dict[str, Any]],
    manquants: Dict[int, Set[int]],
    bulk_data: Dict[str, Dict[int, Dict[str, float | None]]],
    meters_data: Dict[str, Dict[int, Dict[str, float | None]]]
) -> None:
    """
    Synchronise les mois manquants dans Supabase avec smart_upsert.

    Utilise smart_upsert_monthly_analytics pour ne jamais écraser
    une valeur existante avec NULL.
    """
    total = sum(len(m) for m in manquants.values())
    logger.info("Synchronisation de %d mois manquants (smart_upsert)...", total)

    count = 0
    errors = 0

    for site_id, mois_set in manquants.items():
        site_info = sites.get(site_id)
        if not site_info:
            continue

        system_key = site_info["system_key"]
        site_bulk = bulk_data.get(system_key, {})
        site_meters = meters_data.get(system_key, {})

        # Récupérer les données existantes pour ce site
        existing_data = fetch_existing_analytics(sb, site_id, mois_set)

        for mois in sorted(mois_set):
            analytics = site_bulk.get(mois, {})
            meter = site_meters.get(mois)

            # Fusionner analytics et meter_data
            new_data: Dict[str, Any] = {}
            new_data["production_kwh"] = analytics.get("production_kwh")
            new_data["irradiance_avg"] = analytics.get("irradiance_avg")
            new_data["performance_ratio"] = analytics.get("performance_ratio")
            new_data["availability"] = analytics.get("availability")

            if meter:
                new_data["grid_export_kwh"] = meter.get("grid_export_kwh")
                new_data["grid_import_kwh"] = meter.get("grid_import_kwh")
                new_data["meter_id"] = meter.get("meter_id")

            try:
                smart_upsert_monthly_analytics(
                    sb, site_id, mois, new_data, existing_data.get(mois)
                )
                count += 1

                if count % 50 == 0:
                    logger.info("Progression: %d/%d", count, total)

            except Exception as exc:
                logger.error("Erreur upsert site_id=%d mois=%d: %s", site_id, mois, exc)
                errors += 1

    logger.info("Synchronisation terminée: %d succès, %d erreurs", count, errors)


# ────────────────────────── Main ────────────────────────────


def main() -> None:
    """Point d'entrée principal."""
    setup_logging()
    logger.info("=" * 70)
    logger.info("RATTRAPAGE DONNÉES 2025 (Janvier → Décembre)")
    logger.info("=" * 70)

    # Init clients
    try:
        vc = VCOMAPIClient()
        sb = SupabaseAdapter()
    except Exception as exc:
        logger.error("Erreur initialisation clients: %s", exc)
        sys.exit(1)

    # Étape 1: Récupérer sites éligibles
    logger.info("-" * 70)
    logger.info("ÉTAPE 1: Récupération des sites éligibles")
    sites = fetch_sites_eligibles(sb)

    if not sites:
        logger.warning("Aucun site éligible trouvé")
        sys.exit(0)

    # Étape 2: Récupérer mois existants
    logger.info("-" * 70)
    logger.info("ÉTAPE 2: Récupération des mois existants")
    existants = fetch_mois_existants(sb)

    # Étape 3: Calculer manquants
    logger.info("-" * 70)
    logger.info("ÉTAPE 3: Calcul des mois manquants")
    manquants = calculer_mois_manquants(sites, existants)

    if not manquants:
        logger.info("Aucun mois manquant détecté. Terminé.")
        sys.exit(0)

    # Étape 4: Fetch bulk analytics (3 bulk + G_M0 par site)
    logger.info("-" * 70)
    logger.info("ÉTAPE 4: Récupération VCOM (3 bulk + G_M0 par site)")
    bulk_data = fetch_bulk_analytics(vc, sites)
    
    # Étape 5: Fetch meters
    logger.info("-" * 70)
    logger.info("ÉTAPE 5: Récupération meters VCOM")
    meters_data = fetch_meters_data(vc, sb, sites, manquants)
    
    # Étape 6: Upsert
    logger.info("-" * 70)
    logger.info("ÉTAPE 6: Synchronisation Supabase")
    sync_missing_months(sb, sites, manquants, bulk_data, meters_data)
    
    logger.info("=" * 70)
    logger.info("✓ Rattrapage terminé")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()