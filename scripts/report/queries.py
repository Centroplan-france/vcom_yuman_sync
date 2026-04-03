#!/usr/bin/env python3
"""Requêtes pour le rapport hebdomadaire Work Orders.

Utilise supabase-py (.rpc()) pour appeler les fonctions PostgreSQL
rpc_report_* via l'API REST PostgREST.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any

from supabase import create_client, Client as SupabaseClient

logger = logging.getLogger(__name__)


def _get_client() -> SupabaseClient:
    """Crée un client Supabase via SUPABASE_URL + SUPABASE_SERVICE_KEY."""
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_KEY")
    if not (url and key):
        raise EnvironmentError("SUPABASE_URL or SUPABASE_SERVICE_KEY missing")
    return create_client(url, key)


def _rpc(sb: SupabaseClient, fn: str) -> list[dict[str, Any]]:
    """Appelle une fonction RPC et retourne les résultats."""
    result = sb.rpc(fn).execute()
    return result.data or []


@dataclass
class ReportData:
    """Container pour toutes les données du rapport."""
    kpis: list[dict]
    open_wo: list[dict]
    in_progress: list[dict]
    proximity: list[dict]
    trends: list[dict]
    aging: list[dict]
    preventif_lots: list[dict]
    sav_states: list[dict]


def fetch_all() -> ReportData:
    """Exécute toutes les requêtes RPC et retourne les données structurées."""
    logger.info("[REPORT] Connexion à Supabase...")
    sb = _get_client()

    logger.info("[REPORT] Récupération des données via RPC...")

    kpis = _rpc(sb, "rpc_report_kpis")
    logger.info(f"[REPORT] KPIs: {len(kpis)} statuts récupérés")

    open_wo = _rpc(sb, "rpc_report_open_wo")
    logger.info(f"[REPORT] WO Open: {len(open_wo)} lignes")

    in_progress = _rpc(sb, "rpc_report_in_progress")
    logger.info(f"[REPORT] WO In Progress: {len(in_progress)} lignes")

    proximity = _rpc(sb, "rpc_report_proximity")
    logger.info(f"[REPORT] Proximité: {len(proximity)} matchs")

    trends = _rpc(sb, "rpc_report_trends")
    logger.info(f"[REPORT] Tendances: {len(trends)} semaines")

    aging = _rpc(sb, "rpc_report_aging")
    logger.info(f"[REPORT] Vieillissement: {len(aging)} tranches")

    preventif_lots = _rpc(sb, "rpc_report_preventif_lots")
    logger.info(f"[REPORT] Lots préventifs: {len(preventif_lots)} lots")

    sav_states = _rpc(sb, "rpc_report_sav_states")
    logger.info(f"[REPORT] SAV states: {len(sav_states)} états")

    return ReportData(
        kpis=kpis,
        open_wo=open_wo,
        in_progress=in_progress,
        proximity=proximity,
        trends=trends,
        aging=aging,
        preventif_lots=preventif_lots,
        sav_states=sav_states,
    )
