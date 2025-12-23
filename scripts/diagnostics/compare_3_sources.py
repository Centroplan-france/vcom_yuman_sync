#!/usr/bin/env python3
"""
Script de diagnostic : compare les données VCOM 2025 entre 3 sources :
1. Export CSV direct de VCOM (fichier Excel exporté)
2. API VCOM bulk
3. Supabase

Génère un rapport JSON détaillé des différences.

Usage:
    poetry run python -m vysync.compare_3_sources --csv /workspaces/vcom_yuman_sync/src/vysync/ExcelExport_2025_01_01.csv
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

from vysync.vcom_client import VCOMAPIClient
from vysync.adapters.supabase_adapter import SupabaseAdapter
from vysync.logging_config import setup_logging

logger = logging.getLogger(__name__)

FROM_DATE = "2025-01-01T00:00:00+01:00"
TO_DATE = "2025-12-31T23:59:59+01:00"

# Mapping des noms de métriques dans le CSV vers nos noms internes
CSV_METRIC_PATTERNS = {
    r"Disponibilité de l'installation \[%\]": "availability",
    r"Ratio de performance \[%\]": "performance_ratio",
    r"Énergie mesurée \[kWh\]": "production_kwh",
}

MOIS_NAMES = ["Janvier", "Février", "Mars", "Avril", "Mai", "Juin", 
              "Juillet", "Août", "Septembre", "Octobre", "Novembre", "Décembre"]


def parse_csv_vcom(csv_path: Path) -> Dict[str, Dict[int, Dict[str, float | None]]]:
    """
    Parse le fichier CSV exporté de VCOM.
    
    Le fichier est encodé en UTF-16 LE avec séparateur tabulation.
    Format: "[Nom site]: [Métrique]" puis 12 colonnes (Jan-Déc)
    
    Returns:
        Dict[site_name, Dict[mois (1-12), {production_kwh, performance_ratio, availability}]]
    """
    data: Dict[str, Dict[int, Dict[str, float | None]]] = defaultdict(lambda: defaultdict(dict))
    
    # Lire le fichier avec le bon encodage
    try:
        content = csv_path.read_text(encoding="utf-16")
    except UnicodeError:
        # Fallback UTF-16 LE explicite
        content = csv_path.read_bytes().decode("utf-16-le")
    
    lines = content.strip().split("\n")
    
    # Trouver la ligne d'en-tête (contient "Date" et les noms de mois)
    header_idx = None
    for i, line in enumerate(lines):
        if "Date" in line and "Janvier" in line:
            header_idx = i
            break
    
    if header_idx is None:
        logger.error("Impossible de trouver la ligne d'en-tête dans le CSV")
        return dict(data)
    
    # Parser les lignes de données
    for line in lines[header_idx + 1:]:
        if not line.strip():
            continue
        
        parts = line.split("\t")
        if len(parts) < 13:  # Nom + 12 mois minimum
            continue
        
        first_col = parts[0].strip()
        
        # Extraire nom du site et métrique
        # Format: "Nom du site: Métrique [unité]"
        match = re.match(r"^(.+?):\s*(.+)$", first_col)
        if not match:
            continue
        
        site_name = match.group(1).strip()
        metric_part = match.group(2).strip()
        
        # Identifier la métrique
        metric_key = None
        for pattern, key in CSV_METRIC_PATTERNS.items():
            if re.search(pattern, metric_part, re.IGNORECASE):
                metric_key = key
                break
        
        if metric_key is None:
            continue  # Métrique non pertinente (ex: énergie simulée)
        
        # Parser les 12 valeurs mensuelles
        for mois_idx in range(12):
            col_idx = mois_idx + 1
            if col_idx >= len(parts):
                break
            
            raw_value = parts[col_idx].strip()
            
            if raw_value.lower() == "x" or raw_value == "":
                value = None
            else:
                try:
                    # Convertir la virgule décimale en point
                    value = float(raw_value.replace(",", "."))
                except ValueError:
                    value = None
            
            mois = mois_idx + 1  # 1-12
            data[site_name][mois][metric_key] = value
    
    logger.info("CSV: %d sites parsés", len(data))
    return dict(data)


def fetch_vcom_api_data(vc: VCOMAPIClient) -> Dict[str, Dict[int, Dict[str, float | None]]]:
    """
    Récupère E_Z_EVU, PR, VFG en bulk depuis l'API VCOM.
    
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
        logger.info("Fetch API VCOM bulk %s...", abbrev)
        
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
    
    logger.info("API VCOM: %d systèmes récupérés", len(data))
    return dict(data)


def fetch_supabase_data(sb: SupabaseAdapter) -> tuple[
    Dict[str, Dict[int, Dict[str, Any]]], 
    Dict[str, int], 
    Dict[str, str],
    Dict[str, str]
]:
    """
    Récupère les données 2025 depuis Supabase et les mappings.
    
    Returns:
        - Dict[system_key, Dict[mois, {production_kwh, performance_ratio, availability}]]
        - Dict[system_key, site_id]
        - Dict[system_key, name]
        - Dict[name, system_key] pour mapper le CSV
    """
    # Récupérer sites éligibles
    sites_result = sb.sb.table("sites_mapping")\
        .select("id, vcom_system_key, name")\
        .eq("ignore_site", False)\
        .not_.is_("vcom_system_key", "null")\
        .execute()
    
    key_to_site = {}
    key_to_name = {}
    name_to_key = {}
    
    for row in sites_result.data:
        system_key = row["vcom_system_key"]
        key_to_site[system_key] = row["id"]
        key_to_name[system_key] = row["name"]
        name_to_key[row["name"]] = system_key
    
    # Récupérer monthly_analytics 2025 avec pagination
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
    
    # Inverser le mapping
    site_to_key = {v: k for k, v in key_to_site.items()}
    
    data: Dict[str, Dict[int, Dict[str, Any]]] = defaultdict(lambda: defaultdict(dict))
    
    for row in all_analytics:
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
    return dict(data), key_to_site, key_to_name, name_to_key


def compare_3_sources(
    csv_data: Dict[str, Dict[int, Dict[str, float | None]]],
    api_data: Dict[str, Dict[int, Dict[str, float | None]]],
    supabase_data: Dict[str, Dict[int, Dict[str, Any]]],
    key_to_site: Dict[str, int],
    key_to_name: Dict[str, str],
    name_to_key: Dict[str, str]
) -> Dict[str, Any]:
    """
    Compare les 3 sources et génère un rapport détaillé.
    """
    metrics = ["production_kwh", "performance_ratio", "availability"]
    
    rapport = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "periode": "2025",
        "resume": {
            "sites_csv": len(csv_data),
            "sites_api": len(api_data),
            "sites_supabase": len(supabase_data),
            "sites_eligibles": len(key_to_site),
            "sites_csv_non_mappes": 0,
        },
        "mapping_csv_echecs": [],
        "comparaisons": [],
        "statistiques": {
            "total_points": 0,
            "ecarts_csv_vs_api": {"production_kwh": 0, "performance_ratio": 0, "availability": 0},
            "ecarts_csv_vs_supabase": {"production_kwh": 0, "performance_ratio": 0, "availability": 0},
            "ecarts_api_vs_supabase": {"production_kwh": 0, "performance_ratio": 0, "availability": 0},
        }
    }
    
    # Identifier les sites CSV non mappés
    for csv_name in csv_data.keys():
        if csv_name not in name_to_key:
            rapport["mapping_csv_echecs"].append(csv_name)
            rapport["resume"]["sites_csv_non_mappes"] += 1
    
    # Parcourir les systèmes éligibles
    for system_key, site_id in key_to_site.items():
        site_name = key_to_name.get(system_key, "")
        
        # Données des 3 sources
        csv_mois = csv_data.get(site_name, {})
        api_mois = api_data.get(system_key, {})
        sb_mois = supabase_data.get(system_key, {})
        
        # Union de tous les mois disponibles
        all_months = set(csv_mois.keys()) | set(api_mois.keys()) | set(sb_mois.keys())
        
        for mois in sorted(all_months):
            csv_vals = csv_mois.get(mois, {})
            api_vals = api_mois.get(mois, {})
            sb_vals = sb_mois.get(mois, {})
            
            rapport["statistiques"]["total_points"] += 1
            
            point = {
                "system_key": system_key,
                "site_id": site_id,
                "site_name": site_name,
                "mois": mois,
                "valeurs": {},
                "ecarts": {}
            }
            
            for metric in metrics:
                csv_val = csv_vals.get(metric)
                api_val = api_vals.get(metric)
                sb_val = sb_vals.get(metric)
                
                point["valeurs"][metric] = {
                    "csv": round(csv_val, 4) if csv_val is not None else None,
                    "api": round(api_val, 4) if api_val is not None else None,
                    "supabase": round(sb_val, 4) if sb_val is not None else None,
                }
                
                # Calculer les écarts
                ecarts = {}
                
                # CSV vs API
                if csv_val is not None and api_val is not None:
                    diff = csv_val - api_val
                    if abs(diff) > 0.01:
                        ecarts["csv_vs_api"] = round(diff, 4)
                        rapport["statistiques"]["ecarts_csv_vs_api"][metric] += 1
                elif csv_val is not None and api_val is None:
                    ecarts["csv_vs_api"] = "API_NULL"
                elif csv_val is None and api_val is not None:
                    ecarts["csv_vs_api"] = "CSV_NULL"
                
                # CSV vs Supabase
                if csv_val is not None and sb_val is not None:
                    diff = csv_val - sb_val
                    if abs(diff) > 0.01:
                        ecarts["csv_vs_supabase"] = round(diff, 4)
                        rapport["statistiques"]["ecarts_csv_vs_supabase"][metric] += 1
                elif csv_val is not None and sb_val is None:
                    ecarts["csv_vs_supabase"] = "SB_NULL"
                elif csv_val is None and sb_val is not None:
                    ecarts["csv_vs_supabase"] = "CSV_NULL"
                
                # API vs Supabase
                if api_val is not None and sb_val is not None:
                    diff = api_val - sb_val
                    if abs(diff) > 0.01:
                        ecarts["api_vs_supabase"] = round(diff, 4)
                        rapport["statistiques"]["ecarts_api_vs_supabase"][metric] += 1
                elif api_val is not None and sb_val is None:
                    ecarts["api_vs_supabase"] = "SB_NULL"
                elif api_val is None and sb_val is not None:
                    ecarts["api_vs_supabase"] = "API_NULL"
                
                if ecarts:
                    point["ecarts"][metric] = ecarts
            
            # N'ajouter au rapport que si des écarts existent
            if point["ecarts"]:
                rapport["comparaisons"].append(point)
    
    return rapport


def print_summary(rapport: Dict[str, Any]) -> None:
    """Affiche un résumé du rapport."""
    print("\n" + "=" * 80)
    print("RÉSUMÉ COMPARAISON 3 SOURCES")
    print("=" * 80)
    
    r = rapport["resume"]
    print(f"\nSites CSV:              {r['sites_csv']}")
    print(f"Sites API VCOM:         {r['sites_api']}")
    print(f"Sites Supabase:         {r['sites_supabase']}")
    print(f"Sites éligibles:        {r['sites_eligibles']}")
    print(f"Sites CSV non mappés:   {r['sites_csv_non_mappes']}")
    
    s = rapport["statistiques"]
    print(f"\nPoints analysés:        {s['total_points']}")
    print(f"Points avec écarts:     {len(rapport['comparaisons'])}")
    
    print("\n--- Écarts CSV vs API ---")
    for metric, count in s["ecarts_csv_vs_api"].items():
        print(f"  {metric}: {count}")
    
    print("\n--- Écarts CSV vs Supabase ---")
    for metric, count in s["ecarts_csv_vs_supabase"].items():
        print(f"  {metric}: {count}")
    
    print("\n--- Écarts API vs Supabase ---")
    for metric, count in s["ecarts_api_vs_supabase"].items():
        print(f"  {metric}: {count}")
    
    if rapport["mapping_csv_echecs"]:
        print(f"\n--- Sites CSV non mappés ({len(rapport['mapping_csv_echecs'])}) ---")
        for name in rapport["mapping_csv_echecs"][:10]:
            print(f"  - {name}")
        if len(rapport["mapping_csv_echecs"]) > 10:
            print(f"  ... et {len(rapport['mapping_csv_echecs']) - 10} autres")
    
    print("=" * 80)


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare 3 sources de données VCOM 2025")
    parser.add_argument("--csv", required=True, type=Path, help="Chemin vers le fichier CSV exporté de VCOM")
    parser.add_argument("--output", default="rapport_3_sources.json", help="Fichier de sortie JSON")
    args = parser.parse_args()
    
    setup_logging()
    logger.info("=" * 70)
    logger.info("COMPARAISON 3 SOURCES : CSV / API VCOM / SUPABASE")
    logger.info("=" * 70)
    
    if not args.csv.exists():
        logger.error("Fichier CSV introuvable: %s", args.csv)
        sys.exit(1)
    
    try:
        vc = VCOMAPIClient()
        sb = SupabaseAdapter()
    except Exception as exc:
        logger.error("Erreur initialisation: %s", exc)
        sys.exit(1)
    
    # 1. Parser le CSV
    logger.info("-" * 70)
    logger.info("Parsing CSV: %s", args.csv)
    csv_data = parse_csv_vcom(args.csv)
    
    # 2. Fetch API VCOM bulk
    logger.info("-" * 70)
    api_data = fetch_vcom_api_data(vc)
    
    # 3. Fetch Supabase + mappings
    logger.info("-" * 70)
    supabase_data, key_to_site, key_to_name, name_to_key = fetch_supabase_data(sb)
    
    # 4. Comparaison
    logger.info("-" * 70)
    logger.info("Comparaison en cours...")
    rapport = compare_3_sources(
        csv_data, api_data, supabase_data,
        key_to_site, key_to_name, name_to_key
    )
    
    # 5. Sauvegarder
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(rapport, f, ensure_ascii=False, indent=2)
    
    logger.info("Rapport sauvegardé: %s", args.output)
    
    # 6. Afficher résumé
    print_summary(rapport)


if __name__ == "__main__":
    main()