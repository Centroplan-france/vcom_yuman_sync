#!/usr/bin/env python3
"""
Script d'exploration pour valider les données analytics VCOM.
Usage: poetry run python -m vysync.test_analytics_exploration
"""

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List
from dotenv import load_dotenv
from vysync.vcom_client import VCOMAPIClient

load_dotenv()

# Couleurs terminal
class C:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    END = '\033[0m'
    BOLD = '\033[1m'

def print_header(text: str):
    print(f"\n{C.HEADER}{C.BOLD}{'='*80}{C.END}")
    print(f"{C.HEADER}{C.BOLD}{text}{C.END}")
    print(f"{C.HEADER}{C.BOLD}{'='*80}{C.END}\n")

def print_section(text: str):
    print(f"\n{C.BLUE}{C.BOLD}{text}{C.END}")
    print(f"{C.BLUE}{'-'*80}{C.END}")

def print_json(data: Any, indent: int = 2):
    print(json.dumps(data, indent=indent, ensure_ascii=False, default=str))


def explore_meters(vc: VCOMAPIClient, system_key: str):
    """Explore les meters disponibles pour un site."""
    print_section(f"1. METERS du site {system_key}")
    
    try:
        # GET /systems/{key}/meters
        response = vc._make_request("GET", f"/systems/{system_key}/meters")
        meters = response.json().get("data", [])
        
        if not meters:
            print(f"{C.YELLOW}⚠️  Aucun meter trouvé pour ce site{C.END}")
            return None
        
        print(f"{C.GREEN}✓ {len(meters)} meter(s) trouvé(s){C.END}")
        print_json(meters)
        
        # Prendre le premier meter (option C validée en Q5)
        primary_meter = meters[0]
        print(f"\n{C.BOLD}Meter principal sélectionné :{C.END}")
        print(f"  ID:   {primary_meter['id']}")
        print(f"  Name: {primary_meter['name']}")
        print(f"  UID:  {primary_meter.get('uid', 'N/A')}")
        
        return primary_meter
        
    except Exception as e:
        print(f"{C.RED}❌ Erreur lors de la récupération des meters : {e}{C.END}")
        return None


def explore_meter_abbreviations(vc: VCOMAPIClient, system_key: str, meter_id: str):
    """Liste les abréviations disponibles pour un meter."""
    print_section(f"2. ABBREVIATIONS du meter {meter_id}")
    
    try:
        # GET /systems/{key}/meters/{meter_id}/abbreviations
        response = vc._make_request(
            "GET", 
            f"/systems/{system_key}/meters/{meter_id}/abbreviations"
        )
        abbreviations = response.json().get("data", [])
        
        print(f"{C.GREEN}✓ {len(abbreviations)} abréviation(s) trouvée(s){C.END}")
        print_json(abbreviations)
        
        # Vérifier la présence des abréviations clés
        print(f"\n{C.BOLD}Vérification des abréviations clés :{C.END}")
        target_abbrevs = ["M_AC_E_EXP", "M_AC_E_IMP"]
        
        for abbrev in target_abbrevs:
            if abbrev in abbreviations:
                print(f"  {C.GREEN}✓{C.END} {abbrev} présent")
                # Récupérer les détails
                detail_response = vc._make_request(
                    "GET",
                    f"/systems/{system_key}/meters/{meter_id}/abbreviations/{abbrev}"
                )
                detail = detail_response.json().get("data", {})
                print(f"    Description: {detail.get('description')}")
                print(f"    Unit: {detail.get('unit')}")
                print(f"    Aggregation: {detail.get('aggregation')}")
            else:
                print(f"  {C.RED}✗{C.END} {abbrev} absent")
        
        return abbreviations
        
    except Exception as e:
        print(f"{C.RED}❌ Erreur lors de la récupération des abréviations : {e}{C.END}")
        return []


def fetch_monthly_basics(vc: VCOMAPIClient, system_key: str, year: int, month: int):
    """Récupère les données basics pour un mois donné."""
    print_section(f"3. BASICS pour {year}-{month:02d}")
    
    # Construire les dates (premier et dernier jour du mois)
    from_date = f"{year}-{month:02d}-01T00:00:00+01:00"
    # Dernier jour du mois
    if month == 12:
        to_date = f"{year}-12-31T23:59:59+01:00"
    else:
        import calendar
        last_day = calendar.monthrange(year, month)[1]
        to_date = f"{year}-{month:02d}-{last_day}T23:59:59+01:00"
    
    abbreviations = ["E_Z_EVU", "G_M0"]
    results = {}
    
    for abbrev in abbreviations:
        try:
            # GET /systems/{key}/basics/abbreviations/{abbrev}/measurements
            response = vc._make_request(
                "GET",
                f"/systems/{system_key}/basics/abbreviations/{abbrev}/measurements",
                params={
                    "from": from_date,
                    "to": to_date,
                    "resolution": "month"
                }
            )
            data = response.json().get("data", {})
            measurements = data.get(abbrev, [])
            
            if measurements:
                value = measurements[0].get("value")
                results[abbrev] = value
                print(f"  {C.GREEN}✓{C.END} {abbrev:10} = {value}")
            else:
                results[abbrev] = None
                print(f"  {C.YELLOW}⚠{C.END} {abbrev:10} = NULL")
                
        except Exception as e:
            print(f"  {C.RED}✗{C.END} {abbrev:10} : {e}")
            results[abbrev] = None
    
    return results


def fetch_monthly_calculations(vc: VCOMAPIClient, system_key: str, year: int, month: int):
    """Récupère les données calculations pour un mois donné."""
    print_section(f"4. CALCULATIONS pour {year}-{month:02d}")
    
    from_date = f"{year}-{month:02d}-01T00:00:00+01:00"
    if month == 12:
        to_date = f"{year}-12-31T23:59:59+01:00"
    else:
        import calendar
        last_day = calendar.monthrange(year, month)[1]
        to_date = f"{year}-{month:02d}-{last_day}T23:59:59+01:00"
    
    abbreviations = ["PR", "VFG"]
    results = {}
    
    for abbrev in abbreviations:
        try:
            response = vc._make_request(
                "GET",
                f"/systems/{system_key}/calculations/abbreviations/{abbrev}/measurements",
                params={
                    "from": from_date,
                    "to": to_date,
                    "resolution": "day"
                }
            )
            data = response.json().get("data", {})
            measurements = data.get(abbrev, [])
            
            if measurements:
                value = measurements[0].get("value")
                results[abbrev] = value
                print(f"  {C.GREEN}✓{C.END} {abbrev:10} = {value}")
            else:
                results[abbrev] = None
                print(f"  {C.YELLOW}⚠{C.END} {abbrev:10} = NULL")
                
        except Exception as e:
            print(f"  {C.RED}✗{C.END} {abbrev:10} : {e}")
            results[abbrev] = None
    
    return results


def fetch_monthly_meters(vc: VCOMAPIClient, system_key: str, meter_id: str, year: int, month: int):
    """Récupère les données meters pour un mois donné."""
    print_section(f"5. METERS pour {year}-{month:02d}")
    
    from_date = f"{year}-{month:02d}-01T00:00:00+01:00"
    if month == 12:
        to_date = f"{year}-12-31T23:59:59+01:00"
    else:
        import calendar
        last_day = calendar.monthrange(year, month)[1]
        to_date = f"{year}-{month:02d}-{last_day}T23:59:59+01:00"
    
    abbreviations = ["M_AC_E_EXP", "M_AC_E_IMP"]
    results = {}
    
    for abbrev in abbreviations:
        try:
            response = vc._make_request(
                "GET",
                f"/systems/{system_key}/meters/{meter_id}/abbreviations/{abbrev}/measurements",
                params={
                    "from": from_date,
                    "to": to_date,
                    "resolution": "day"  # ← garde "day" pour ce test
                }
            )
            
            # ============= AJOUT DEBUG =============
            raw_data = response.json()
            print(f"\n{C.YELLOW}[DEBUG] Réponse brute API pour {abbrev}:{C.END}")
            print_json(raw_data)
            # ========================================
            
            data = raw_data.get("data", {})
            meter_data = data.get(meter_id, {})
            measurements = meter_data.get(abbrev, [])
            
            print(f"\n{C.YELLOW}[DEBUG] Après parsing:{C.END}")
            print(f"  data keys: {list(data.keys())}")
            print(f"  meter_data keys: {list(meter_data.keys()) if isinstance(meter_data, dict) else 'NOT A DICT'}")
            print(f"  measurements type: {type(measurements)}")
            print(f"  measurements length: {len(measurements) if isinstance(measurements, list) else 'NOT A LIST'}")
            
            if measurements and len(measurements) >= 2:
                # Calcul du delta (fin - début)
                start_value = measurements[0].get("value")
                end_value = measurements[-1].get("value")
                delta = end_value - start_value if (end_value and start_value) else None
                
                results[abbrev] = delta
                print(f"  {C.GREEN}✓{C.END} {abbrev:15} = {delta} kWh (delta: {end_value} - {start_value})")
            else:
                results[abbrev] = None
                print(f"  {C.YELLOW}⚠{C.END} {abbrev:15} = NULL (pas assez de mesures)")
                
        except Exception as e:
            print(f"  {C.RED}✗{C.END} {abbrev:15} : {e}")
            import traceback
            traceback.print_exc()  # Afficher la stack trace complète
            results[abbrev] = None
    
    return results


def main():
    """Point d'entrée principal."""
    print_header("🔍 EXPLORATION ANALYTICS VCOM")
    
    # Configuration
    SITE_KEY = "E3K2L"  # Site de test (tu peux changer)
    TEST_YEAR = 2024
    TEST_MONTH = 12  # Janvier 2025
    
    print(f"Site de test : {C.BOLD}{SITE_KEY}{C.END}")
    print(f"Période test : {C.BOLD}{TEST_YEAR}-{TEST_MONTH:02d}{C.END}")
    
    # Initialisation
    print_section("0. INITIALISATION")
    vc = VCOMAPIClient()
    print(f"{C.GREEN}✓ VCOMAPIClient initialisé{C.END}")
    
    # Étape 1 : Meters
    primary_meter = explore_meters(vc, SITE_KEY)
    
    if not primary_meter:
        print(f"\n{C.RED}❌ Impossible de continuer sans meter{C.END}")
        print(f"{C.YELLOW}Conseil : Teste avec un autre site qui a des meters{C.END}")
        return
    
    # Étape 2 : Abréviations meters
    meter_abbrevs = explore_meter_abbreviations(vc, SITE_KEY, primary_meter["id"])
    
    # Étape 3-5 : Récupération données mensuelles
    basics_data = fetch_monthly_basics(vc, SITE_KEY, TEST_YEAR, TEST_MONTH)
    calc_data = fetch_monthly_calculations(vc, SITE_KEY, TEST_YEAR, TEST_MONTH)
    meter_data = fetch_monthly_meters(vc, SITE_KEY, primary_meter["id"], TEST_YEAR, TEST_MONTH)
    
    # Synthèse finale
    print_header("📊 SYNTHÈSE DES DONNÉES RÉCUPÉRÉES")
    
    complete_data = {
        "site_key": SITE_KEY,
        "month": f"{TEST_YEAR}-{TEST_MONTH:02d}",
        "meter_id": primary_meter["id"],
        "basics": basics_data,
        "calculations": calc_data,
        "meters": meter_data,
    }
    
    print_json(complete_data)
    
    # Vérification complétude
    print(f"\n{C.BOLD}Complétude des données :{C.END}")
    all_values = list(basics_data.values()) + list(calc_data.values()) + list(meter_data.values())
    null_count = sum(1 for v in all_values if v is None)
    total_count = len(all_values)
    
    print(f"  Valeurs récupérées : {total_count - null_count}/{total_count}")
    
    if null_count == 0:
        print(f"  {C.GREEN}✓ Toutes les données sont disponibles{C.END}")
    else:
        print(f"  {C.YELLOW}⚠️  {null_count} valeur(s) NULL détectée(s){C.END}")
    
    print_header("✅ EXPLORATION TERMINÉE")
    print(f"\n{C.BOLD}Prochaine étape :{C.END}")
    print("  1. Analyser les résultats ci-dessus")
    print("  2. Valider que les données sont cohérentes")
    print("  3. Si OK → passer à l'étape 2 (module vcom_analytics.py)")


if __name__ == "__main__":
    main()