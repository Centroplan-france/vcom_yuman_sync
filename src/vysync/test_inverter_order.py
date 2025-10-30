#!/usr/bin/env python3
"""
Test diagnostic RAW : Afficher les vraies réponses API VCOM

Objectif : Voir EXACTEMENT ce que retourne chaque endpoint pour comprendre
d'où viennent les différences de modèle

Usage:
    poetry run python test_inverter_api_raw.py
"""

import json
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional
from dotenv import load_dotenv

from vysync.vcom_client import VCOMAPIClient
from vysync.adapters.supabase_adapter import SupabaseAdapter

load_dotenv()

# Couleurs terminal
class C:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    END = '\033[0m'
    BOLD = '\033[1m'

# Prendre juste quelques onduleurs pour analyse détaillée
SAMPLE_SERIALS = [
    "O1V18A01213W1",  # Delta RPI M30A -> M50A
    "27060319",       # Fronius SYMO
    "121703",         # ABB/Power-One
]


def print_header(text: str):
    print(f"\n{C.HEADER}{C.BOLD}{'='*120}{C.END}")
    print(f"{C.HEADER}{C.BOLD}{text.center(120)}{C.END}")
    print(f"{C.HEADER}{C.BOLD}{'='*120}{C.END}\n")


def print_section(text: str):
    print(f"\n{C.CYAN}{C.BOLD}{text}{C.END}")
    print(f"{C.CYAN}{'-'*120}{C.END}")


def print_json(data: Any, indent: int = 2):
    """Pretty print JSON avec couleur"""
    json_str = json.dumps(data, indent=indent, ensure_ascii=False, default=str)
    print(json_str)


def get_db_state(sb: SupabaseAdapter, serial_number: str) -> Optional[Dict[str, Any]]:
    """Récupère l'état actuel en DB pour un onduleur"""
    try:
        result = (
            sb.sb.table("equipments_mapping")
            .select("*")
            .eq("serial_number", serial_number)  # ✅ CORRIGÉ
            .eq("category_id", 11102)  # CAT_INVERTER
            .execute()
        )
        
        if result.data:
            row = result.data[0]
            vcom_key = sb._get_vcom_key_by_site_id(row["site_id"])
            
            return {
                "vcom_device_id": row["vcom_device_id"],
                "serial_number": row.get("serial_number"),
                "brand": row.get("brand"),
                "model": row.get("model"),
                "site_id": row["site_id"],
                "vcom_system_key": vcom_key,
                "yuman_material_id": row.get("yuman_material_id"),
            }
        return None
    except Exception as e:
        print(f"{C.RED}❌ Erreur DB pour {serial_number}: {e}{C.END}")
        return None


def analyze_inverter_raw(vc: VCOMAPIClient, sb: SupabaseAdapter, serial_number: str):
    """Analyse RAW d'un onduleur avec affichage complet des réponses API"""
    
    print_header(f"ANALYSE RAW : {serial_number}")
    
    # ═══════════════════════════════════════════════════════════════
    # ÉTAPE 1 : État DB
    # ═══════════════════════════════════════════════════════════════
    print_section("1️⃣  ÉTAT EN BASE DE DONNÉES")
    
    db_state = get_db_state(sb, serial_number)
    
    if not db_state:
        print(f"{C.RED}❌ Onduleur non trouvé en DB{C.END}")
        return
    
    print(f"\n{C.BOLD}Informations DB :{C.END}")
    print_json(db_state)
    
    system_key = db_state["vcom_system_key"]
    inverter_id = db_state["vcom_device_id"]
    
    if not system_key:
        print(f"{C.RED}❌ vcom_system_key manquant{C.END}")
        return
    
    # ═══════════════════════════════════════════════════════════════
    # ÉTAPE 2 : /systems/{key}/technical-data
    # ═══════════════════════════════════════════════════════════════
    print_section("2️⃣  VCOM API : GET /systems/{key}/technical-data")
    
    print(f"\n{C.YELLOW}Endpoint : GET /v2/systems/{system_key}/technical-data{C.END}")
    
    try:
        tech_data = vc.get_technical_data(system_key)
        
        print(f"\n{C.BOLD}Réponse complète :{C.END}")
        print_json(tech_data)
        
        # Extraire les infos pertinentes
        print(f"\n{C.BOLD}📊 Extraction des informations onduleurs :{C.END}")
        
        if "systemConfigurations" in tech_data:
            for idx, config in enumerate(tech_data["systemConfigurations"], 1):
                print(f"\n  Configuration #{idx} :")
                print_json({
                    "inverterModel": config.get("inverterModel"),
                    "inverterVendor": config.get("inverterVendor"),
                    "inverterPower": config.get("inverterPower"),
                    "mpptInputs": list(config.get("mpptInputs", {}).keys()) if config.get("mpptInputs") else [],
                })
        
    except Exception as e:
        print(f"{C.RED}❌ Erreur : {e}{C.END}")
    
    # ═══════════════════════════════════════════════════════════════
    # ÉTAPE 3 : /systems/{key}/inverters (liste)
    # ═══════════════════════════════════════════════════════════════
    print_section("3️⃣  VCOM API : GET /systems/{key}/inverters")
    
    print(f"\n{C.YELLOW}Endpoint : GET /v2/systems/{system_key}/inverters{C.END}")
    
    try:
        inverters_list = vc.get_inverters(system_key)
        
        print(f"\n{C.BOLD}Réponse complète (liste) :{C.END}")
        print_json(inverters_list)
        
        # Chercher notre onduleur dans la liste
        print(f"\n{C.BOLD}🔍 Recherche de l'onduleur {inverter_id} dans la liste :{C.END}")
        
        target_inv = None
        for idx, inv in enumerate(inverters_list, 1):
            if inv["id"] == inverter_id:
                target_inv = inv
                print(f"\n  ✅ Trouvé à l'index {idx} :")
                print_json({
                    "id": inv.get("id"),
                    "serial": inv.get("serial"),
                    "model": inv.get("model"),
                    "vendor": inv.get("vendor"),
                })
                break
        
        if not target_inv:
            print(f"\n  {C.RED}❌ Onduleur {inverter_id} NON TROUVÉ dans la liste{C.END}")
        
    except Exception as e:
        print(f"{C.RED}❌ Erreur : {e}{C.END}")
    
    # ═══════════════════════════════════════════════════════════════
    # ÉTAPE 4 : /systems/{key}/inverters/{id} (détail)
    # ═══════════════════════════════════════════════════════════════
    print_section("4️⃣  VCOM API : GET /systems/{key}/inverters/{id} ⭐ UTILISÉ ACTUELLEMENT")
    
    print(f"\n{C.YELLOW}Endpoint : GET /v2/systems/{system_key}/inverters/{inverter_id}{C.END}")
    
    try:
        inverter_detail = vc.get_inverter_details(system_key, inverter_id)
        
        print(f"\n{C.BOLD}Réponse complète :{C.END}")
        print_json(inverter_detail)
        
        # Extraire les champs clés
        print(f"\n{C.BOLD}📊 Champs clés extraits :{C.END}")
        print_json({
            "id": inverter_detail.get("id"),
            "serial": inverter_detail.get("serial"),
            "model": inverter_detail.get("model"),  # ⭐ C'EST CE CHAMP QUI EST UTILISÉ
            "vendor": inverter_detail.get("vendor"),
            "power": inverter_detail.get("power"),
        })
        
    except Exception as e:
        print(f"{C.RED}❌ Erreur : {e}{C.END}")
    
    # ═══════════════════════════════════════════════════════════════
    # ÉTAPE 5 : Comparaison finale
    # ═══════════════════════════════════════════════════════════════
    print_section("5️⃣  COMPARAISON FINALE")
    
    print(f"\n{C.BOLD}Modèle actuel en DB :{C.END} {C.CYAN}{db_state['model']}{C.END}")
    
    print(f"\n{C.BOLD}Ce que le code actuel va faire :{C.END}")
    print(f"  1. Appeler : GET /v2/systems/{system_key}/inverters/{inverter_id}")
    print(f"  2. Extraire : det_inv.get('model')")
    print(f"  3. Si différent de '{db_state['model']}' → UPDATE en DB")
    
    print(f"\n{C.YELLOW}Question à résoudre :{C.END}")
    print(f"  • Pourquoi le modèle retourné par /inverters/{{id}} diffère de celui en DB ?")
    print(f"  • Lequel est le VRAI modèle de l'onduleur physique ?")
    print(f"  • Y a-t-il eu un changement matériel ou juste une correction de données ?")


def main():
    """Point d'entrée du test"""
    
    print_header("TEST DIAGNOSTIC RAW : RÉPONSES API COMPLÈTES")
    
    print(f"On va analyser {len(SAMPLE_SERIALS)} onduleurs en détail")
    print(f"pour voir EXACTEMENT ce que retourne chaque endpoint VCOM\n")
    
    # Initialisation
    print_section("INITIALISATION")
    print("Connexion VCOM...")
    vc = VCOMAPIClient()
    print(f"{C.GREEN}✓ VCOM connecté{C.END}")
    
    print("Connexion Supabase...")
    sb = SupabaseAdapter()
    print(f"{C.GREEN}✓ Supabase connecté{C.END}")
    
    # Analyser chaque onduleur
    for serial in SAMPLE_SERIALS:
        analyze_inverter_raw(vc, sb, serial)
        
        # Pause entre chaque onduleur pour lisibilité
        input(f"\n{C.BOLD}Appuyez sur Entrée pour continuer...{C.END}")
    
    print_header("✅ TEST TERMINÉ")
    
    print(f"\n{C.BOLD}Prochaines étapes :{C.END}")
    print(f"  1. Analyser les JSONs pour comprendre d'où vient la différence")
    print(f"  2. Déterminer quelle source est la plus fiable")
    print(f"  3. Décider si on doit corriger le code ou la DB")


if __name__ == "__main__":
    main()