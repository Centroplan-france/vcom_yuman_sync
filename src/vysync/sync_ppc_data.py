#!/usr/bin/env python3
"""
Module de synchronisation des données PPC (Power Plant Controllers).

Récupère hebdomadairement la consigne de puissance active depuis VCOM et la stocke
dans Supabase. Utilise un système de fallback automatique :
1. PPC_P_SET_ABS (en W) si disponible
2. PPC_P_SET_GRIDOP_REL (en %) sinon
3. PPC_P_SET_REL (en %) en dernier recours

Fréquence recommandée : Une fois par semaine (suggéré : samedi)
Période de mesure : Veille à 12h-13h UTC (midi, production garantie)

Usage:
    poetry run python -m vysync.sync_ppc_data
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional

import requests

from vysync.vcom_client import VCOMAPIClient
from vysync.adapters.supabase_adapter import SupabaseAdapter

logger = logging.getLogger(__name__)

# Constantes
# Ordre de priorité des abréviations PPC (du plus précis au moins précis)
PPC_ABBREVIATIONS_PRIORITY = [
    "PPC_P_SET_ABS",         # Valeur absolue en W (préféré)
    "PPC_P_SET_GRIDOP_REL",  # Valeur relative en % (fallback 1)
    "PPC_P_SET_REL"          # Valeur relative en % (fallback 2)
]
MEASUREMENT_HOUR_START = 12  # 12h UTC (midi, production garantie)
MEASUREMENT_DURATION = 1  # 1 heure


def get_measurement_period() -> tuple[datetime, datetime]:
    """
    Retourne la période de mesure : veille à 12h-13h UTC.

    La période de 12h-13h UTC (midi) est choisie car c'est le moment où la
    production solaire est garantie en France, évitant les valeurs à 0
    dues au mode standby (MODE=201) pendant la nuit.

    Returns:
        Tuple (from_time, to_time) en datetime avec timezone UTC

    Example:
        >>> from_time, to_time = get_measurement_period()
        >>> # Si aujourd'hui est 2025-10-31 14:00:00 UTC
        >>> # from_time = 2025-10-30 12:00:00 UTC
        >>> # to_time = 2025-10-30 13:00:00 UTC
    """
    now = datetime.now(timezone.utc)
    yesterday = now - timedelta(days=1)
    from_time = yesterday.replace(hour=MEASUREMENT_HOUR_START, minute=0, second=0, microsecond=0)
    to_time = from_time + timedelta(hours=MEASUREMENT_DURATION)

    logger.debug("Measurement period: %s to %s", from_time.isoformat(), to_time.isoformat())
    return from_time, to_time


def fetch_ppc_setpoint(
    vc: VCOMAPIClient,
    system_key: str,
    nominal_power: Optional[float]
) -> Dict[str, Any]:
    """
    Récupère la consigne PPC pour un site avec logique basée sur MODE.

    Utilise l'endpoint bulk pour récupérer toutes les mesures en un appel,
    puis sélectionne la source de données selon PPC_P_SET_MODE :
    - MODE = 1 (production active) → utilise PPC_P_SET_ABS (W → kW)
    - MODE = 201 (standby/nuit) → utilise PPC_P_SET_GRIDOP_REL (% → kW)
    - MODE inconnu → fallback sur priorité (ABS > GRIDOP_REL > REL)

    Note: nominal_power dans Supabase est en kW.

    Args:
        vc: Client VCOM API
        system_key: Clé du système VCOM (ex: "WC6HQ")
        nominal_power: Puissance nominale du site en kW

    Returns:
        Dict avec l'une des structures suivantes :
        - {"status": "no_ppc"} si aucun PPC trouvé
        - {"status": "no_data"} si PPC existe mais pas de mesure valide
        - {"status": "ok", "data": {...}} si données OK
    """
    try:
        # 1. Récupérer les power plant controllers du site
        controllers = vc.get_power_plant_controllers(system_key)

        if not controllers:
            logger.info("Site %s: No PPC found", system_key)
            return {"status": "no_ppc"}

        controller = controllers[0]
        controller_id = str(controller["id"])
        logger.debug("Site %s: Found PPC controller %s", system_key, controller_id)

        # 2. Récupérer la période de mesure
        from_time, to_time = get_measurement_period()

        # 3. Récupérer toutes les mesures en bulk (1 seul appel API)
        bulk_data = vc.get_ppc_bulk_measurements(
            system_key, from_time, to_time, resolution="interval"
        )

        if not bulk_data:
            logger.warning("Site %s: Empty bulk response", system_key)
            return {"status": "no_data"}

        # 4. Extraire la dernière mesure
        timestamps = sorted(bulk_data.keys())
        last_ts = timestamps[-1]

        if controller_id not in bulk_data[last_ts]:
            logger.warning("Site %s: Controller %s not in bulk response",
                         system_key, controller_id)
            return {"status": "no_data"}

        measurements = bulk_data[last_ts][controller_id]

        # 5. Lire les valeurs
        mode = measurements.get('PPC_P_SET_MODE')
        abs_value = measurements.get('PPC_P_SET_ABS')
        gridop_rel_value = measurements.get('PPC_P_SET_GRIDOP_REL')
        rel_value = measurements.get('PPC_P_SET_REL')

        logger.debug("Site %s: MODE=%s, ABS=%s, GRIDOP_REL=%s, REL=%s",
                    system_key, mode, abs_value, gridop_rel_value, rel_value)

        # 6. Appliquer la logique basée sur MODE
        setpoint_kw = None
        source = None

        if mode == 1:
            # MODE 1 = Production active → utiliser ABS
            if abs_value is not None:
                setpoint_kw = abs_value / 1000.0  # W → kW
                source = "PPC_P_SET_ABS"
                logger.debug("Site %s: MODE=1, using ABS: %s W → %.2f kW",
                           system_key, abs_value, setpoint_kw)

        elif mode == 201:
            # MODE 201 = Standby → utiliser GRIDOP_REL
            if gridop_rel_value is not None:
                if nominal_power is None or nominal_power <= 0:
                    logger.warning("Site %s: MODE=201 but nominal_power=%s, cannot convert",
                                 system_key, nominal_power)
                    return {"status": "no_data"}
                # nominal_power est en kW, pas besoin de diviser par 1000
                setpoint_kw = nominal_power * gridop_rel_value / 100.0
                source = "PPC_P_SET_GRIDOP_REL"
                logger.debug("Site %s: MODE=201, using GRIDOP_REL: %.2f kW × %.2f%% = %.2f kW",
                           system_key, nominal_power, gridop_rel_value, setpoint_kw)

        else:
            # MODE inconnu ou None → fallback sur priorité
            logger.debug("Site %s: MODE=%s (unknown), using fallback logic", system_key, mode)

            if abs_value is not None and abs_value != 0:
                setpoint_kw = abs_value / 1000.0
                source = "PPC_P_SET_ABS (fallback)"
            elif gridop_rel_value is not None and nominal_power and nominal_power > 0:
                setpoint_kw = nominal_power * gridop_rel_value / 100.0
                source = "PPC_P_SET_GRIDOP_REL (fallback)"
            elif rel_value is not None and nominal_power and nominal_power > 0:
                setpoint_kw = nominal_power * rel_value / 100.0
                source = "PPC_P_SET_REL (fallback)"

        # 7. Vérifier qu'on a une valeur
        if setpoint_kw is None:
            logger.warning("Site %s: No valid setpoint found (MODE=%s)", system_key, mode)
            return {"status": "no_data"}

        logger.info("Site %s: PPC setpoint = %.2f kW (source: %s, timestamp: %s)",
                   system_key, setpoint_kw, source, last_ts)

        return {
            "status": "ok",
            "data": {
                "controller_id": controller_id,
                "setpoint_kw": setpoint_kw,
                "timestamp": last_ts
            }
        }

    except (requests.RequestException, KeyError, ValueError) as e:
        logger.error("Error fetching PPC data for site %s: %s", system_key, e, exc_info=True)
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
        logger.error("Failed to update site %d PPC data: %s", site_id, e, exc_info=True)
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
        logger.error("Failed to initialize clients: %s", e, exc_info=True)
        raise

    # Récupérer tous les sites avec vcom_system_key
    try:
        result = sb.sb.table("sites_mapping")\
            .select("id, vcom_system_key, name, nominal_power")\
            .not_.is_("vcom_system_key", "null")\
            .execute()

        sites = result.data
    except Exception as e:
        logger.error("Failed to fetch sites from database: %s", e, exc_info=True)
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
        nominal_power = site.get("nominal_power")  # Peut être None

        try:
            logger.info("Processing site %d (%s - %s, nominal_power=%.2f kW)",
                      site_id, system_key, site_name,
                      nominal_power if nominal_power else 0)

            # Récupérer les données PPC
            ppc_data = fetch_ppc_setpoint(vc, system_key, nominal_power)

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
            logger.error("Error processing site %d (%s): %s", site_id, system_key, e, exc_info=True)
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
    logger.info("Metrics (priority order): %s", " > ".join(PPC_ABBREVIATIONS_PRIORITY))
    logger.info("Measurement period: Yesterday %dh-%dh UTC",
               MEASUREMENT_HOUR_START, MEASUREMENT_HOUR_START + MEASUREMENT_DURATION)

    try:
        sync_all_sites()
    except Exception as e:
        logger.error("Fatal error during PPC sync: %s", e, exc_info=True)
        raise


if __name__ == "__main__":
    main()
