#!/usr/bin/env python3
"""
Module de synchronisation des données PPC (Power Plant Controllers).

Récupère hebdomadairement la consigne de puissance active (PPC_P_SET_ABS)
depuis VCOM et la stocke dans Supabase.

Fréquence recommandée : Une fois par semaine (suggéré : samedi)
Période de mesure : Veille à 18h-19h UTC (données consolidées)

Usage:
    poetry run python -m vysync.sync_ppc_data
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional

from vysync.vcom_client import VCOMAPIClient
from vysync.adapters.supabase_adapter import SupabaseAdapter

logger = logging.getLogger(__name__)

# Constantes
PPC_ABBREVIATION = "PPC_P_SET_ABS"  # Absolute active power setpoint
MEASUREMENT_HOUR_START = 18  # 18h UTC
MEASUREMENT_DURATION = 1  # 1 heure


def get_measurement_period() -> tuple[datetime, datetime]:
    """
    Retourne la période de mesure : veille à 18h-19h UTC.

    La période de 18h-19h UTC est choisie car c'est le moment où les données
    sont consolidées et fiables.

    Returns:
        Tuple (from_time, to_time) en datetime avec timezone UTC

    Example:
        >>> from_time, to_time = get_measurement_period()
        >>> # Si aujourd'hui est 2025-10-31 14:00:00 UTC
        >>> # from_time = 2025-10-30 18:00:00 UTC
        >>> # to_time = 2025-10-30 19:00:00 UTC
    """
    now = datetime.now(timezone.utc)
    yesterday = now - timedelta(days=1)
    from_time = yesterday.replace(hour=MEASUREMENT_HOUR_START, minute=0, second=0, microsecond=0)
    to_time = from_time + timedelta(hours=MEASUREMENT_DURATION)

    logger.debug("Measurement period: %s to %s", from_time.isoformat(), to_time.isoformat())
    return from_time, to_time


def fetch_ppc_setpoint(
    vc: VCOMAPIClient,
    system_key: str
) -> Dict[str, Any]:
    """
    Récupère la consigne PPC_P_SET_ABS pour un site.

    Gère trois cas distincts :
    - no_ppc : Aucun controller PPC trouvé sur le site
    - no_data : PPC existe mais pas de mesure récente (perte de communication)
    - ok : PPC existe avec mesure valide

    Args:
        vc: Client VCOM API
        system_key: Clé du système VCOM (ex: "WC6HQ")

    Returns:
        Dict avec l'une des structures suivantes :

        - {"status": "no_ppc"} si aucun PPC trouvé
        - {"status": "no_data"} si PPC existe mais pas de mesure (perte de com)
        - {"status": "ok", "data": {
              "controller_id": str,
              "setpoint_kw": float,
              "timestamp": str (ISO format)
          }} si données OK

    Example:
        >>> result = fetch_ppc_setpoint(vc, "WC6HQ")
        >>> if result["status"] == "ok":
        ...     print(f"Setpoint: {result['data']['setpoint_kw']} kW")
    """
    try:
        # 1. Récupérer les power plant controllers du site
        controllers = vc.get_power_plant_controllers(system_key)

        # CAS 1 : Aucun controller trouvé
        if not controllers:
            logger.info("Site %s: No PPC found", system_key)
            return {"status": "no_ppc"}

        # Prendre le premier controller (comme dans le script de test)
        controller = controllers[0]
        controller_id = controller["id"]
        logger.debug("Site %s: Found PPC controller %s", system_key, controller_id)

        # 2. Récupérer la période de mesure
        from_time, to_time = get_measurement_period()

        # 3. Récupérer les mesures pour PPC_P_SET_ABS
        measurements = vc.get_ppc_measurements(
            system_key=system_key,
            device_id=controller_id,
            abbreviation_id=PPC_ABBREVIATION,
            from_time=from_time,
            to_time=to_time,
            resolution="interval"
        )

        # 4. Vérifier si des mesures existent
        recent_measurement = measurements.get("recent_measurement")

        # CAS 2 : PPC existe mais pas de mesure (perte de communication)
        if recent_measurement is None:
            logger.info("Site %s: PPC %s exists but no measurement (communication loss)",
                       system_key, controller_id)
            return {"status": "no_data"}

        # CAS 3 : PPC existe ET mesure disponible
        measurement_value = recent_measurement.get("value")
        measurement_timestamp = recent_measurement.get("timestamp")

        if measurement_value is None:
            logger.warning("Site %s: PPC %s has measurement but value is None",
                          system_key, controller_id)
            return {"status": "no_data"}

        # Conversion W → kW
        setpoint_kw = measurement_value / 1000.0

        logger.info("Site %s: PPC setpoint = %.2f kW (timestamp: %s)",
                   system_key, setpoint_kw, measurement_timestamp)

        return {
            "status": "ok",
            "data": {
                "controller_id": controller_id,
                "setpoint_kw": setpoint_kw,
                "timestamp": measurement_timestamp
            }
        }

    except Exception as e:
        logger.error("Error fetching PPC data for site %s: %s", system_key, e)
        # En cas d'erreur API, on considère que c'est un problème temporaire
        # On ne met pas à jour la DB (comme no_data)
        return {"status": "error", "error": str(e)}


def update_site_ppc_data(
    sb: SupabaseAdapter,
    site_id: int,
    ppc_data: Dict[str, Any]
) -> None:
    """
    Met à jour les colonnes PPC d'un site dans sites_mapping.

    Gère trois cas selon le statut des données PPC :
    - no_ppc : Met tous les champs à NULL
    - no_data : Ne fait RIEN (conserve les valeurs existantes)
    - ok : Met à jour avec les nouvelles valeurs

    Args:
        sb: Adapter Supabase
        site_id: ID du site dans sites_mapping
        ppc_data: Dictionnaire retourné par fetch_ppc_setpoint()

    Raises:
        Exception si l'update échoue
    """
    status = ppc_data.get("status")

    try:
        if status == "no_ppc":
            # CAS 1 : Aucun PPC trouvé → mettre NULL partout
            logger.debug("Site %d: Setting PPC fields to NULL (no PPC)", site_id)
            sb.sb.table("sites_mapping").update({
                "ppc_controller_id": None,
                "ppc_setpoint_kw": None,
                "ppc_last_update": None
            }).eq("id", site_id).execute()
            logger.info("Site %d: No PPC found, set fields to NULL", site_id)

        elif status == "no_data":
            # CAS 2 : PPC existe mais pas de measurement (perte de com)
            # NE RIEN FAIRE : garder les valeurs existantes en DB
            logger.info("Site %d: PPC exists but no measurement (com loss), keeping previous values",
                       site_id)
            return

        elif status == "error":
            # Erreur API : ne rien faire non plus (comme no_data)
            logger.warning("Site %d: API error, keeping previous values: %s",
                          site_id, ppc_data.get("error"))
            return

        elif status == "ok":
            # CAS 3 : Données PPC valides → mise à jour
            data = ppc_data["data"]
            logger.debug("Site %d: Updating PPC data: %s", site_id, data)
            sb.sb.table("sites_mapping").update({
                "ppc_controller_id": data["controller_id"],
                "ppc_setpoint_kw": data["setpoint_kw"],
                "ppc_last_update": data["timestamp"]
            }).eq("id", site_id).execute()
            logger.info("Site %d: PPC setpoint updated to %.2f kW",
                       site_id, data["setpoint_kw"])

        else:
            logger.error("Site %d: Unknown PPC data status: %s", site_id, status)

    except Exception as e:
        logger.error("Failed to update site %d PPC data: %s", site_id, e)
        raise


def sync_all_sites() -> None:
    """
    Synchronise les données PPC pour tous les sites.

    Parcourt tous les sites dans sites_mapping ayant un vcom_system_key
    et met à jour leurs données PPC selon la logique définie :
    - Sites sans PPC : NULL
    - Sites avec PPC mais sans mesure : pas de changement
    - Sites avec PPC et mesure : mise à jour

    Le traitement continue même si des sites individuels échouent.
    Un rapport de synthèse est affiché à la fin.
    """
    logger.info("=" * 70)
    logger.info("Starting PPC data synchronization")
    logger.info("=" * 70)

    try:
        vc = VCOMAPIClient()
        sb = SupabaseAdapter()
    except Exception as e:
        logger.error("Failed to initialize clients: %s", e)
        raise

    # Récupérer tous les sites avec vcom_system_key
    try:
        result = sb.sb.table("sites_mapping")\
            .select("id, vcom_system_key, name")\
            .not_.is_("vcom_system_key", "null")\
            .execute()

        sites = result.data
    except Exception as e:
        logger.error("Failed to fetch sites from database: %s", e)
        raise

    if not sites:
        logger.warning("No sites found with vcom_system_key")
        return

    logger.info("Found %d sites to process", len(sites))
    logger.info("-" * 70)

    # Compteurs pour le rapport final
    success_count = 0
    skip_count = 0
    error_count = 0
    null_count = 0

    for site in sites:
        site_id = site["id"]
        system_key = site["vcom_system_key"]
        site_name = site.get("name") or system_key

        try:
            logger.info("Processing site %d (%s - %s)", site_id, system_key, site_name)

            # Récupérer les données PPC
            ppc_data = fetch_ppc_setpoint(vc, system_key)

            # Mettre à jour la base de données
            update_site_ppc_data(sb, site_id, ppc_data)

            # Comptabiliser selon le statut
            status = ppc_data.get("status")
            if status == "ok":
                success_count += 1
            elif status == "no_data" or status == "error":
                skip_count += 1
            elif status == "no_ppc":
                null_count += 1
                success_count += 1  # Les NULL updates comptent comme succès

            logger.info("-" * 70)

        except Exception as e:
            logger.error("Error processing site %d (%s): %s", site_id, system_key, e)
            error_count += 1
            logger.info("-" * 70)

    # Rapport final
    logger.info("=" * 70)
    logger.info("PPC sync completed")
    logger.info("  - %d sites updated successfully", success_count)
    logger.info("    - %d with PPC data", success_count - null_count)
    logger.info("    - %d set to NULL (no PPC)", null_count)
    logger.info("  - %d sites skipped (com loss or API error)", skip_count)
    logger.info("  - %d sites with errors", error_count)
    logger.info("=" * 70)


def main() -> None:
    """
    Point d'entrée principal du module.

    Configure le logging et lance la synchronisation de tous les sites.
    En cas d'erreur fatale, l'exception est propagée après avoir été loggée.
    """
    # Configuration du logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    logger.info("PPC Data Synchronization Module")
    logger.info("Metric: %s (Absolute active power setpoint)", PPC_ABBREVIATION)
    logger.info("Measurement period: Yesterday %dh-%dh UTC",
               MEASUREMENT_HOUR_START, MEASUREMENT_HOUR_START + MEASUREMENT_DURATION)

    try:
        sync_all_sites()
    except Exception as e:
        logger.error("Fatal error during PPC sync: %s", e)
        raise


if __name__ == "__main__":
    main()
