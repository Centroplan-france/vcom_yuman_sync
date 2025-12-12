#!/usr/bin/env python3
"""
Script de diagnostic : compare les données VCOM 2025 avec Supabase.
Génère un rapport JSON détaillé des différences.

Usage:
    poetry run python -m vysync.compare_2025
"""

from __future__ import annotations

import json
import logging
import sys
from collections import defaultdict
from datetime import datetime
from typing import Dict, Any

from vysync.vcom_client import VCOMAPIClient
from vysync.adapters.supabase_adapter import SupabaseAdapter
from vysync.logging_config import setup_logging

logger = logging.getLogger(__name__)

FROM_DATE = "2025-01-01T00:00:00+01:00"
TO_DATE = "2025-10-31T23:59:59+01:00"
MOIS_2025 = list(range(1, 11))  # Janvier → Octobre


def fetch_vcom_data(vc: VCOMAPIClient) -> Dict[str, Dict[int, Dict[str, float | None]]]:
    """
    Récupère E_Z_EVU, PR, VFG en bulk depuis VCOM.
    
    Returns:
        Dict[system_key, Dict[mois, {production_kwh, performance_ratio, availability}]]
    """
    abbreviations = {
        "E_Z_EVU": "production_kwh",
        "PR": "performance_ratio",
        "VFG": "availability"
    }
    
    data: Dict[str, Dict[int, Dict[str, float | None]]] = defaultdict(lambda: defaultdict(dict))
    
    for abbrev, metric_name in abbreviations.items():
        logger.info("Fetch VCOM bulk %s...", abbrev)
        
        try:
            results = vc.get_bulk_measurements(abbrev, FROM_DATE, TO_DATE, resolution="month")
            
            for item in results:
                system_key = item.get("systemKey")
                if not system_key:
                    continue
                
                measurements = item.get(abbrev, [])
                for measure in measurements:
                    timestamp = measure.get("timestamp", "")
                    value = measure.get("value")
                    
                    try:
                        mois = int(timestamp.split("-")[1])
                    except (IndexError, ValueError):
                        continue
                    
                    if value is not None:
                        try:
                            data[system_key][mois][metric_name] = float(value)
                        except (ValueError, TypeError):
                            data[system_key][mois][metric_name] = None
                    else:
                        data[system_key][mois][metric_name] = None
        
        except Exception as exc:
            logger.error("Erreur fetch bulk %s: %s", abbrev, exc)
    
    logger.info("VCOM: %d systèmes récupérés", len(data))
    return dict(data)


def fetch_supabase_data(sb: SupabaseAdapter) -> tuple[Dict[str, Dict[int, Dict[str, Any]]], Dict[str, int]]:
    """
    Récupère les données 2025 depuis Supabase, indexées par system_key.
    
    Returns:
        - Dict[system_key, Dict[mois, {production_kwh, performance_ratio, availability}]]
        - Dict[system_key, site_id] pour référence
    """
    # Récupérer sites éligibles avec leur system_key
    sites_result = sb.sb.table("sites_mapping")\
        .select("id, vcom_system_key, name")\
        .eq("ignore_site", False)\
        .not_.is_("vcom_system_key", "null")\
        .not_.is_("commission_date", "null")\
        .execute()
    
    key_to_site = {}
    key_to_name = {}
    for row in sites_result.data:
        key_to_site[row["vcom_system_key"]] = row["id"]
        key_to_name[row["vcom_system_key"]] = row["name"]
    
    # Récupérer monthly_analytics 2025
    # pagination pour garantir toutes les lignes
    all_analytics = []
    page_size = 1000
    offset = 0

    while True:
        result = sb.sb.table("monthly_analytics")\
            .select("site_id, month, production_kwh, performance_ratio, availability")\
            .gte("month", "2025-01-01")\
            .lte("month", "2025-12-01")\
            .range(offset, offset + page_size - 1)\
            .execute()
        
        all_analytics.extend(result.data)
        
        if len(result.data) < page_size:
            break
        offset += page_size

    
    # Inverser le mapping pour retrouver system_key depuis site_id
    site_to_key = {v: k for k, v in key_to_site.items()}
    
    data: Dict[str, Dict[int, Dict[str, Any]]] = defaultdict(lambda: defaultdict(dict))
    
    for row in all_analytics :
        site_id = row["site_id"]
        system_key = site_to_key.get(site_id)
        
        if not system_key:
            continue
        
        mois = int(row["month"].split("-")[1])
        
        data[system_key][mois] = {
            "production_kwh": float(row["production_kwh"]) if row["production_kwh"] is not None else None,
            "performance_ratio": float(row["performance_ratio"]) if row["performance_ratio"] is not None else None,
            "availability": float(row["availability"]) if row["availability"] is not None else None,
        }
    
    logger.info("Supabase: %d systèmes avec données 2025", len(data))
    return dict(data), key_to_site, key_to_name


def compare_and_report(
    vcom_data: Dict[str, Dict[int, Dict[str, float | None]]],
    supabase_data: Dict[str, Dict[int, Dict[str, Any]]],
    key_to_site: Dict[str, int],
    key_to_name: Dict[str, str]
) -> Dict[str, Any]:
    """
    Compare VCOM vs Supabase et génère un rapport détaillé.
    """
    metrics = ["production_kwh", "performance_ratio", "availability"]
    
    rapport = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "periode": "2025-01 à 2025-10",
        "resume": {
            "systemes_vcom": len(vcom_data),
            "systemes_supabase": len(supabase_data),
            "systemes_eligibles": len(key_to_site),
        },
        "manquants_supabase": [],  # Dans VCOM, pas dans Supabase
        "manquants_vcom": [],      # Dans Supabase, pas dans VCOM
        "ecarts": {
            "production_kwh": [],
            "performance_ratio": [],
            "availability": []
        },
        "statistiques": {
            "total_comparaisons": 0,
            "mois_manquants_supabase": 0,
            "mois_manquants_vcom": 0,
            "ecarts_production": 0,
            "ecarts_pr": 0,
            "ecarts_vfg": 0
        }
    }
    
    # Parcourir les systèmes éligibles uniquement
    for system_key, site_id in key_to_site.items():
        site_name = key_to_name.get(system_key, "")
        
        vcom_mois = vcom_data.get(system_key, {})
        sb_mois = supabase_data.get(system_key, {})
        
        # Mois présents dans VCOM mais pas dans Supabase
        for mois in sorted(vcom_mois.keys()):
            if mois not in sb_mois:
                rapport["manquants_supabase"].append({
                    "system_key": system_key,
                    "site_id": site_id,
                    "site_name": site_name,
                    "mois": mois,
                    "vcom_values": vcom_mois[mois]
                })
                rapport["statistiques"]["mois_manquants_supabase"] += 1
            else:
                # Comparer les valeurs
                rapport["statistiques"]["total_comparaisons"] += 1
                
                for metric in metrics:
                    vcom_val = vcom_mois[mois].get(metric)
                    sb_val = sb_mois[mois].get(metric)
                    
                    # Les deux ont une valeur → vérifier écart
                    if vcom_val is not None and sb_val is not None:
                        diff = abs(vcom_val - sb_val)
                        if diff > 0.01:
                            rapport["ecarts"][metric].append({
                                "system_key": system_key,
                                "site_id": site_id,
                                "site_name": site_name,
                                "mois": mois,
                                "vcom": round(vcom_val, 4),
                                "supabase": round(sb_val, 4),
                                "diff": round(vcom_val - sb_val, 4),
                                "diff_pct": round((vcom_val - sb_val) / sb_val * 100, 2) if sb_val != 0 else None
                            })
                    
                    # VCOM a une valeur, Supabase NULL
                    elif vcom_val is not None and sb_val is None:
                        rapport["ecarts"][metric].append({
                            "system_key": system_key,
                            "site_id": site_id,
                            "site_name": site_name,
                            "mois": mois,
                            "vcom": round(vcom_val, 4),
                            "supabase": None,
                            "diff": None,
                            "diff_pct": None,
                            "note": "Supabase NULL, VCOM a une valeur"
                        })
                    
                    # Supabase a une valeur, VCOM NULL
                    elif vcom_val is None and sb_val is not None:
                        rapport["ecarts"][metric].append({
                            "system_key": system_key,
                            "site_id": site_id,
                            "site_name": site_name,
                            "mois": mois,
                            "vcom": None,
                            "supabase": round(sb_val, 4),
                            "diff": None,
                            "diff_pct": None,
                            "note": "VCOM NULL, Supabase a une valeur"
                        })
        
        # Mois présents dans Supabase mais pas dans VCOM
        for mois in sorted(sb_mois.keys()):
            if mois not in vcom_mois:
                rapport["manquants_vcom"].append({
                    "system_key": system_key,
                    "site_id": site_id,
                    "site_name": site_name,
                    "mois": mois,
                    "supabase_values": sb_mois[mois]
                })
                rapport["statistiques"]["mois_manquants_vcom"] += 1
    
    # Mettre à jour les statistiques d'écarts
    rapport["statistiques"]["ecarts_production"] = len(rapport["ecarts"]["production_kwh"])
    rapport["statistiques"]["ecarts_pr"] = len(rapport["ecarts"]["performance_ratio"])
    rapport["statistiques"]["ecarts_vfg"] = len(rapport["ecarts"]["availability"])
    
    return rapport


def main() -> None:
    setup_logging()
    logger.info("=" * 70)
    logger.info("COMPARAISON VCOM vs SUPABASE 2025")
    logger.info("=" * 70)
    
    try:
        vc = VCOMAPIClient()
        sb = SupabaseAdapter()
    except Exception as exc:
        logger.error("Erreur initialisation: %s", exc)
        sys.exit(1)
    
    # Fetch VCOM (3 appels bulk)
    logger.info("-" * 70)
    vcom_data = fetch_vcom_data(vc)
    
    # Fetch Supabase
    logger.info("-" * 70)
    supabase_data, key_to_site, key_to_name = fetch_supabase_data(sb)
    
    # Comparaison
    logger.info("-" * 70)
    logger.info("Comparaison en cours...")
    rapport = compare_and_report(vcom_data, supabase_data, key_to_site, key_to_name)
    
    # Sauvegarder le rapport JSON
    output_file = "rapport_comparaison_2025.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(rapport, f, ensure_ascii=False, indent=2)
    
    logger.info("Rapport sauvegardé: %s", output_file)
    
    # Afficher le résumé
    print("\n" + "=" * 70)
    print("RÉSUMÉ")
    print("=" * 70)
    print(f"Systèmes VCOM:        {rapport['resume']['systemes_vcom']}")
    print(f"Systèmes Supabase:    {rapport['resume']['systemes_supabase']}")
    print(f"Systèmes éligibles:   {rapport['resume']['systemes_eligibles']}")
    print(f"\nComparaisons:         {rapport['statistiques']['total_comparaisons']}")
    print(f"Mois manquants SB:    {rapport['statistiques']['mois_manquants_supabase']}")
    print(f"Mois manquants VCOM:  {rapport['statistiques']['mois_manquants_vcom']}")
    print(f"\nÉcarts production:    {rapport['statistiques']['ecarts_production']}")
    print(f"Écarts PR:            {rapport['statistiques']['ecarts_pr']}")
    print(f"Écarts VFG:           {rapport['statistiques']['ecarts_vfg']}")
    print("=" * 70)


if __name__ == "__main__":
    main()