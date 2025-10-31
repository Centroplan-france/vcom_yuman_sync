#!/usr/bin/env python3
"""
VCOM Analytics - Module de récupération des métriques mensuelles.

Récupère les données de production, irradiance, performance ratio,
availability et données meters pour chaque site et chaque mois.

Sources de données:
- BASICS: E_Z_EVU (production PV), G_M0 (irradiance)
- CALCULATIONS: PR (performance ratio), VFG (availability)
- METERS: M_AC_E_EXP (injection réseau), M_AC_E_IMP (soutirage réseau)
"""

from __future__ import annotations

import calendar
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple

from vysync.vcom_client import VCOMAPIClient

logger = logging.getLogger(__name__)


# ────────────────────────── Helpers ────────────────────────────


def _build_month_dates(year: int, month: int) -> Tuple[str, str]:
    """
    Construit les dates de début et fin pour un mois donné.

    Args:
        year: Année (ex: 2024)
        month: Mois (1-12)

    Returns:
        Tuple (from_date, to_date) en format ISO avec timezone +01:00

    Exemple:
        >>> _build_month_dates(2024, 1)
        ('2024-01-01T00:00:00+01:00', '2024-01-31T23:59:59+01:00')
    """
    last_day = calendar.monthrange(year, month)[1]
    from_date = f"{year:04d}-{month:02d}-01T00:00:00+01:00"
    to_date = f"{year:04d}-{month:02d}-{last_day:02d}T23:59:59+01:00"
    return from_date, to_date


def _extract_single_value(measurements: List[Dict[str, Any]]) -> float | None:
    """
    Extrait la valeur unique d'une mesure mensuelle.

    Args:
        measurements: Liste de mesures retournée par l'API

    Returns:
        La valeur si elle existe et est valide, None sinon
    """
    if not measurements or len(measurements) == 0:
        return None

    # Pour les mesures mensuelles, on prend la première valeur
    value = measurements[0].get("value")

    if value is None:
        return None

    try:
        return float(value)
    except (ValueError, TypeError):
        logger.warning("Valeur invalide pour conversion float: %s", value)
        return None


def _calculate_delta(measurements: List[Dict[str, Any]]) -> float | None:
    """
    Calcule le delta entre la dernière et la première valeur (pour les meters).

    Args:
        measurements: Liste de mesures journalières

    Returns:
        Delta (dernière - première valeur) ou None si insuffisant de données
    """
    if not measurements or len(measurements) < 2:
        logger.debug("Données insuffisantes pour calculer delta: %d mesures",
                     len(measurements) if measurements else 0)
        return None

    try:
        first_val = float(measurements[0]["value"])
        last_val = float(measurements[-1]["value"])
        delta = last_val - first_val

        # Protection contre les valeurs négatives (reset compteur)
        if delta < 0:
            logger.warning("Delta négatif détecté (reset compteur?): %f - %f = %f",
                          last_val, first_val, delta)
            return None

        return delta
    except (ValueError, TypeError, KeyError) as exc:
        logger.warning("Erreur lors du calcul delta: %s", exc)
        return None


# ────────────────────────── API Fetchers ────────────────────────────


def get_primary_meter(vc: VCOMAPIClient, system_key: str) -> Dict[str, Any] | None:
    """
    Récupère le meter principal d'un site.

    Si plusieurs meters sont présents, retourne le premier.

    Args:
        vc: Client VCOM API
        system_key: Clé du système VCOM (ex: "E3K2L")

    Returns:
        Dictionnaire avec les infos du meter ou None si aucun meter
    """
    try:
        response = vc._make_request("GET", f"/systems/{system_key}/meters")
        meters = response.json().get("data", [])

        if not meters:
            logger.debug("Aucun meter trouvé pour %s", system_key)
            return None

        # Prendre le premier meter
        meter = meters[0]
        logger.debug("Meter principal trouvé pour %s: %s", system_key, meter.get("id"))
        return meter

    except Exception as exc:
        logger.warning("Erreur lors de la récupération des meters pour %s: %s",
                      system_key, exc)
        return None


def fetch_monthly_basics(
    vc: VCOMAPIClient,
    system_key: str,
    year: int,
    month: int
) -> Dict[str, float | None]:
    """
    Récupère les données BASICS (production PV et irradiance) pour un mois.

    Args:
        vc: Client VCOM API
        system_key: Clé du système VCOM
        year: Année
        month: Mois (1-12)

    Returns:
        Dictionnaire avec clés:
        - E_Z_EVU: Production PV en kWh
        - G_M0: Irradiance moyenne en W/m²
    """
    from_date, to_date = _build_month_dates(year, month)
    result = {"E_Z_EVU": None, "G_M0": None}

    for abbrev in ["E_Z_EVU", "G_M0"]:
        try:
            endpoint = f"/systems/{system_key}/basics/abbreviations/{abbrev}/measurements"
            params = {
                "from": from_date,
                "to": to_date,
                "resolution": "month"
            }

            response = vc._make_request("GET", endpoint, params=params)
            data = response.json().get("data", {})

            # Structure: {"<system_key>": {"<abbrev>": [{"timestamp": ..., "value": ...}]}}
            measurements = data.get(abbrev, [])
            result[abbrev] = _extract_single_value(measurements)

            logger.debug("BASICS %s pour %s %d-%02d: %s",
                        abbrev, system_key, year, month, result[abbrev])

        except Exception as exc:
            logger.warning("Erreur récupération BASICS %s pour %s %d-%02d: %s",
                          abbrev, system_key, year, month, exc)

    return result


def fetch_monthly_calculations(
    vc: VCOMAPIClient,
    system_key: str,
    year: int,
    month: int
) -> Dict[str, float | None]:
    """
    Récupère les données CALCULATIONS (PR et VFG) pour un mois.

    Args:
        vc: Client VCOM API
        system_key: Clé du système VCOM
        year: Année
        month: Mois (1-12)

    Returns:
        Dictionnaire avec clés:
        - PR: Performance Ratio en %
        - VFG: Availability (Verfügbarkeit) en %
    """
    from_date, to_date = _build_month_dates(year, month)
    result = {"PR": None, "VFG": None}

    for abbrev in ["PR", "VFG"]:
        try:
            endpoint = f"/systems/{system_key}/calculations/abbreviations/{abbrev}/measurements"
            params = {
                "from": from_date,
                "to": to_date,
                "resolution": "month"
            }

            response = vc._make_request("GET", endpoint, params=params)
            data = response.json().get("data", {})

            measurements = data.get(abbrev, [])
            result[abbrev] = _extract_single_value(measurements)

            logger.debug("CALCULATIONS %s pour %s %d-%02d: %s",
                        abbrev, system_key, year, month, result[abbrev])

        except Exception as exc:
            logger.warning("Erreur récupération CALCULATIONS %s pour %s %d-%02d: %s",
                          abbrev, system_key, year, month, exc)

    return result


def fetch_monthly_meters(
    vc: VCOMAPIClient,
    system_key: str,
    meter_id: str,
    year: int,
    month: int
) -> Dict[str, float | None]:
    """
    Récupère les données METERS (injection et soutirage réseau) pour un mois.

    IMPORTANT: Utilise resolution=day car resolution=month retourne 1 seul point.
    Calcule ensuite le delta entre la dernière et la première valeur.

    Args:
        vc: Client VCOM API
        system_key: Clé du système VCOM
        meter_id: ID du meter (ex: "M0")
        year: Année
        month: Mois (1-12)

    Returns:
        Dictionnaire avec clés:
        - M_AC_E_EXP: Injection réseau en kWh (delta)
        - M_AC_E_IMP: Soutirage réseau en kWh (delta)
    """
    from_date, to_date = _build_month_dates(year, month)
    result = {"M_AC_E_EXP": None, "M_AC_E_IMP": None}

    for abbrev in ["M_AC_E_EXP", "M_AC_E_IMP"]:
        try:
            endpoint = f"/systems/{system_key}/meters/{meter_id}/abbreviations/{abbrev}/measurements"
            params = {
                "from": from_date,
                "to": to_date,
                "resolution": "day"  # IMPORTANT: day car month retourne 1 point
            }

            response = vc._make_request("GET", endpoint, params=params)
            data = response.json().get("data", {})

            # Structure: {"<meter_id>": {"<abbrev>": [{"timestamp": ..., "value": ...}]}}
            measurements = data.get(meter_id, {}).get(abbrev, [])
            result[abbrev] = _calculate_delta(measurements)

            logger.debug("METERS %s pour %s/%s %d-%02d: %s (%d mesures)",
                        abbrev, system_key, meter_id, year, month,
                        result[abbrev], len(measurements) if measurements else 0)

        except Exception as exc:
            logger.warning("Erreur récupération METERS %s pour %s/%s %d-%02d: %s",
                          abbrev, system_key, meter_id, year, month, exc)

    return result


def fetch_monthly_analytics(
    vc: VCOMAPIClient,
    system_key: str,
    year: int,
    month: int
) -> Dict[str, Any]:
    """
    Agrège toutes les données analytics pour un site et un mois.

    Récupère séquentiellement:
    1. Données BASICS (production, irradiance)
    2. Données CALCULATIONS (PR, VFG)
    3. Données METERS si disponible (injection, soutirage)

    Args:
        vc: Client VCOM API
        system_key: Clé du système VCOM
        year: Année
        month: Mois (1-12)

    Returns:
        Dictionnaire avec toutes les métriques du mois:
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
    logger.info("Récupération analytics pour %s %d-%02d", system_key, year, month)

    # Initialiser le résultat
    analytics = {
        "production_kwh": None,
        "irradiance_avg": None,
        "performance_ratio": None,
        "availability": None,
        "grid_export_kwh": None,
        "grid_import_kwh": None,
        "meter_id": None,
        "has_meter_data": False
    }

    # 1. Récupérer BASICS
    basics = fetch_monthly_basics(vc, system_key, year, month)
    analytics["production_kwh"] = basics.get("E_Z_EVU")
    analytics["irradiance_avg"] = basics.get("G_M0")

    # 2. Récupérer CALCULATIONS
    calculations = fetch_monthly_calculations(vc, system_key, year, month)
    analytics["performance_ratio"] = calculations.get("PR")
    analytics["availability"] = calculations.get("VFG")

    # 3. Récupérer METERS si disponible
    meter = get_primary_meter(vc, system_key)
    if meter:
        meter_id = meter.get("id")
        analytics["meter_id"] = meter_id
        analytics["has_meter_data"] = True

        meters_data = fetch_monthly_meters(vc, system_key, meter_id, year, month)
        analytics["grid_export_kwh"] = meters_data.get("M_AC_E_EXP")
        analytics["grid_import_kwh"] = meters_data.get("M_AC_E_IMP")
    else:
        logger.warning("Meter non trouvé pour %s", system_key)

    logger.debug("Analytics complètes pour %s %d-%02d: %s",
                system_key, year, month, analytics)

    return analytics


def get_month_range(
    commission_date: str,
    end_date: str | None = None
) -> List[Tuple[int, int]]:
    """
    Génère la liste des (year, month) entre commission_date et end_date.

    Args:
        commission_date: Date de mise en service au format ISO (YYYY-MM-DD)
        end_date: Date de fin au format ISO ou None pour le mois dernier complet

    Returns:
        Liste de tuples (year, month)

    Exemple:
        >>> get_month_range("2023-06-15", "2023-09-01")
        [(2023, 6), (2023, 7), (2023, 8), (2023, 9)]
    """
    try:
        # Parser commission_date
        start_dt = datetime.fromisoformat(commission_date.replace("Z", "+00:00"))
        start_year, start_month = start_dt.year, start_dt.month

        # Parser end_date ou utiliser mois dernier complet
        if end_date:
            end_dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
        else:
            # Mois dernier complet = mois précédent par rapport à maintenant
            now = datetime.now(timezone.utc)
            end_dt = datetime(now.year, now.month, 1, tzinfo=timezone.utc)
            # Retourner au mois précédent
            if end_dt.month == 1:
                end_dt = datetime(end_dt.year - 1, 12, 1, tzinfo=timezone.utc)
            else:
                end_dt = datetime(end_dt.year, end_dt.month - 1, 1, tzinfo=timezone.utc)

        end_year, end_month = end_dt.year, end_dt.month

        # Générer la liste
        months = []
        current_year, current_month = start_year, start_month

        while (current_year < end_year) or (current_year == end_year and current_month <= end_month):
            months.append((current_year, current_month))

            # Avancer d'un mois
            if current_month == 12:
                current_month = 1
                current_year += 1
            else:
                current_month += 1

        logger.debug("Mois générés de %s à %s: %d mois",
                    commission_date, end_date or "mois dernier", len(months))
        return months

    except Exception as exc:
        logger.error("Erreur lors du calcul de la plage de mois: %s", exc)
        return []

