#!/usr/bin/env python3
"""
Vérification exhaustive : tous les custom fields dans le code vs Yuman
"""

import sys
from pathlib import Path
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).parent.parent))
from vysync.logging_config import setup_logging
setup_logging()

from vysync.adapters.supabase_adapter import SupabaseAdapter
from vysync.adapters.yuman_adapter import YumanAdapter
from vysync.models import CAT_INVERTER, CAT_MODULE, CAT_STRING, CAT_SIM, CAT_CENTRALE

# Couleurs
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

def main():
    SITE_KEY = "E3K2L"
    
    print_header("VÉRIFICATION EXHAUSTIVE : CUSTOM FIELDS CODE vs YUMAN")
    
    # ═══════════════════════════════════════════════════════════════
    # ÉTAPE 1 : Extraction des custom fields depuis le CODE
    # ═══════════════════════════════════════════════════════════════
    print_header("ÉTAPE 1 : CUSTOM FIELDS DÉCLARÉS DANS LE CODE")
    
    # Ces dictionnaires sont dans yuman_adapter.py lignes 21-37
    SITE_FIELDS_CODE = {
        "System Key (Vcom ID)": 13583,
        "Nominal Power (kWc)":  13585,
        "Commission Date":      13586,
    }
    
    STRING_FIELDS_CODE = {
        "MPPT index":       16020,
        "nombre de module": 16021,  # ← À vérifier
        "marque du module": 16022,
        "model de module":  16023,  # ← À vérifier
    }
    
    SIM_FIELDS_CODE = {
        "N° carte SIM": 17940,
        "Opérateur":    14653,
    }
    
    # Constante utilisée dans apply_equips_patch ligne 271
    CUSTOM_INVERTER_ID = "Inverter ID (Vcom)"
    
    # Constante BP_MODEL utilisée lignes 270, 330, 338
    BP_MODEL_NAME = "Modèle"
    
    print(f"{C.BOLD}SITE_FIELDS (lignes 21-25) :{C.END}")
    for name, bp_id in SITE_FIELDS_CODE.items():
        print(f"  • {name:30} (blueprint_id={bp_id})")
    
    print(f"\n{C.BOLD}STRING_FIELDS (lignes 27-33) :{C.END}")
    for name, bp_id in STRING_FIELDS_CODE.items():
        print(f"  • {name:30} (blueprint_id={bp_id})")
    
    print(f"\n{C.BOLD}SIM_FIELDS (lignes 34-37) :{C.END}")
    for name, bp_id in SIM_FIELDS_CODE.items():
        print(f"  • {name:30} (blueprint_id={bp_id})")
    
    print(f"\n{C.BOLD}Autres constantes :{C.END}")
    print(f"  • {CUSTOM_INVERTER_ID:30} (utilisé ligne 271, 330)")
    print(f"  • {BP_MODEL_NAME:30} (BP_MODEL=13548, lignes 270, 330, 338)")
    
    # ═══════════════════════════════════════════════════════════════
    # ÉTAPE 2 : Connexion et résolution site
    # ═══════════════════════════════════════════════════════════════
    print_header("ÉTAPE 2 : CONNEXION YUMAN")
    
    print("Initialisation...")
    sb = SupabaseAdapter()
    y = YumanAdapter(sb)
    
    # Résolution E3K2L
    site_result = sb.sb.table("sites_mapping").select("id, yuman_site_id").eq(
        "vcom_system_key", SITE_KEY
    ).execute()
    
    if not site_result.data or not site_result.data[0]['yuman_site_id']:
        print(f"{C.RED}✗ Site {SITE_KEY} non trouvé{C.END}")
        return
    
    supabase_site_id = site_result.data[0]['id']
    yuman_site_id = site_result.data[0]['yuman_site_id']
    
    print(f"✓ Site E3K2L : yuman_site_id={yuman_site_id}")
    
    # ═══════════════════════════════════════════════════════════════
    # ÉTAPE 3 : Récupération des équipements réels Yuman
    # ═══════════════════════════════════════════════════════════════
    print_header("ÉTAPE 3 : FETCH ÉQUIPEMENTS YUMAN (site E3K2L)")
    
    print("Récupération de tous les équipements du site...")
    all_materials = y.yc.list_materials(embed="fields")
    site_materials = [m for m in all_materials if m.get('site_id') == yuman_site_id]
    
    # Grouper par catégorie
    by_category = defaultdict(list)
    for m in site_materials:
        by_category[m['category_id']].append(m)
    
    print(f"✓ {len(site_materials)} équipements récupérés")
    print(f"\nRépartition :")
    for cat_id, materials in sorted(by_category.items()):
        cat_names = {
            CAT_MODULE: "MODULE",
            CAT_INVERTER: "INVERTER",
            CAT_STRING: "STRING",
            CAT_SIM: "SIM",
            CAT_CENTRALE: "CENTRALE"
        }
        print(f"  • {cat_names.get(cat_id, 'UNKNOWN'):15} : {len(materials)}")
    
    # ═══════════════════════════════════════════════════════════════
    # ÉTAPE 4 : Vérification SITE
    # ═══════════════════════════════════════════════════════════════
    print_header("ÉTAPE 4 : VÉRIFICATION CUSTOM FIELDS - SITE")
    
    print("Fetch site Yuman avec custom fields...")
    site_data = y.yc.get_site(yuman_site_id, embed="fields")
    site_fields_actual = {f['name']: f.get('blueprint_id') 
                          for f in site_data.get('_embed', {}).get('fields', [])}
    
    print(f"\n{C.BOLD}Custom fields RÉELS du site :{C.END}")
    for name, bp_id in sorted(site_fields_actual.items()):
        print(f"  • {name:40} (blueprint_id={bp_id})")
    
    print(f"\n{C.BOLD}Comparaison CODE vs YUMAN :{C.END}")
    for name_code, bp_code in SITE_FIELDS_CODE.items():
        if name_code in site_fields_actual:
            bp_actual = site_fields_actual[name_code]
            if bp_code == bp_actual:
                print(f"  {C.GREEN}✓{C.END} {name_code:40} → OK (bp={bp_code})")
            else:
                print(f"  {C.RED}✗{C.END} {name_code:40} → blueprint_id MISMATCH (code={bp_code}, yuman={bp_actual})")
        else:
            print(f"  {C.RED}✗{C.END} {name_code:40} → INTROUVABLE dans Yuman")
            print(f"      {C.YELLOW}Noms proches :{C.END}")
            for actual_name in site_fields_actual.keys():
                if any(word in actual_name.lower() for word in name_code.lower().split()):
                    print(f"        • {actual_name}")
    
    # ═══════════════════════════════════════════════════════════════
    # ÉTAPE 5 : Vérification INVERTER
    # ═══════════════════════════════════════════════════════════════
    if CAT_INVERTER in by_category:
        print_header("ÉTAPE 5 : VÉRIFICATION CUSTOM FIELDS - INVERTER")
        
        inverter = by_category[CAT_INVERTER][0]
        print(f"Analyse de : {inverter['name']} (id={inverter['id']})")
        
        inv_data = y.yc.get_material(inverter['id'], embed="fields")
        inv_fields_actual = {f['name']: f.get('blueprint_id')
                             for f in inv_data.get('_embed', {}).get('fields', [])}
        
        print(f"\n{C.BOLD}Custom fields RÉELS de l'onduleur :{C.END}")
        for name, bp_id in sorted(inv_fields_actual.items()):
            print(f"  • {name:40} (blueprint_id={bp_id})")
        
        print(f"\n{C.BOLD}Vérification des champs utilisés dans le code :{C.END}")
        
        # BP_MODEL (ligne 270)
        if BP_MODEL_NAME in inv_fields_actual:
            print(f"  {C.GREEN}✓{C.END} {BP_MODEL_NAME:40} → OK")
        else:
            print(f"  {C.RED}✗{C.END} {BP_MODEL_NAME:40} → INTROUVABLE")
        
        # CUSTOM_INVERTER_ID (ligne 271, 330)
        if CUSTOM_INVERTER_ID in inv_fields_actual:
            print(f"  {C.GREEN}✓{C.END} {CUSTOM_INVERTER_ID:40} → OK")
        else:
            print(f"  {C.RED}✗{C.END} {CUSTOM_INVERTER_ID:40} → INTROUVABLE")
            print(f"      {C.YELLOW}Noms proches :{C.END}")
            for actual_name in inv_fields_actual.keys():
                if "inverter" in actual_name.lower() or "vcom" in actual_name.lower():
                    print(f"        • {actual_name}")
    
    # ═══════════════════════════════════════════════════════════════
    # ÉTAPE 6 : Vérification STRING (le plus important)
    # ═══════════════════════════════════════════════════════════════
    if CAT_STRING in by_category:
        print_header("ÉTAPE 6 : VÉRIFICATION CUSTOM FIELDS - STRING")
        
        string = by_category[CAT_STRING][0]
        print(f"Analyse de : {string['name']} (id={string['id']})")
        
        str_data = y.yc.get_material(string['id'], embed="fields")
        str_fields_actual = {f['name']: f.get('blueprint_id')
                            for f in str_data.get('_embed', {}).get('fields', [])}
        
        print(f"\n{C.BOLD}Custom fields RÉELS du STRING :{C.END}")
        for name, bp_id in sorted(str_fields_actual.items()):
            value = next((f.get('value') for f in str_data.get('_embed', {}).get('fields', []) 
                         if f['name'] == name), None)
            print(f"  • {name:40} (bp={bp_id}) = {value}")
        
        print(f"\n{C.BOLD}Comparaison CODE vs YUMAN :{C.END}")
        for name_code, bp_code in STRING_FIELDS_CODE.items():
            if name_code in str_fields_actual:
                bp_actual = str_fields_actual[name_code]
                if bp_code == bp_actual:
                    print(f"  {C.GREEN}✓{C.END} {name_code:40} → OK (bp={bp_code})")
                else:
                    print(f"  {C.RED}✗{C.END} {name_code:40} → blueprint_id MISMATCH (code={bp_code}, yuman={bp_actual})")
            else:
                print(f"  {C.RED}✗{C.END} {name_code:40} → INTROUVABLE dans Yuman")
                print(f"      {C.YELLOW}Noms proches dans Yuman :{C.END}")
                for actual_name in str_fields_actual.keys():
                    # Comparaison flexible
                    code_lower = name_code.lower().replace(" ", "")
                    actual_lower = actual_name.lower().replace(" ", "")
                    if code_lower in actual_lower or actual_lower in code_lower:
                        print(f"        • {actual_name} (bp={str_fields_actual[actual_name]})")
    
    # ═══════════════════════════════════════════════════════════════
    # ÉTAPE 7 : Vérification SIM
    # ═══════════════════════════════════════════════════════════════
    if CAT_SIM in by_category:
        print_header("ÉTAPE 7 : VÉRIFICATION CUSTOM FIELDS - SIM")
        
        sim = by_category[CAT_SIM][0]
        print(f"Analyse de : {sim['name']} (id={sim['id']})")
        
        sim_data = y.yc.get_material(sim['id'], embed="fields")
        sim_fields_actual = {f['name']: f.get('blueprint_id')
                            for f in sim_data.get('_embed', {}).get('fields', [])}
        
        print(f"\n{C.BOLD}Custom fields RÉELS de la SIM :{C.END}")
        for name, bp_id in sorted(sim_fields_actual.items()):
            value = next((f.get('value') for f in sim_data.get('_embed', {}).get('fields', []) 
                         if f['name'] == name), None)
            print(f"  • {name:40} (bp={bp_id}) = {value}")
        
        print(f"\n{C.BOLD}Comparaison CODE vs YUMAN :{C.END}")
        for name_code, bp_code in SIM_FIELDS_CODE.items():
            if name_code in sim_fields_actual:
                bp_actual = sim_fields_actual[name_code]
                if bp_code == bp_actual:
                    print(f"  {C.GREEN}✓{C.END} {name_code:40} → OK (bp={bp_code})")
                else:
                    print(f"  {C.RED}✗{C.END} {name_code:40} → blueprint_id MISMATCH (code={bp_code}, yuman={bp_actual})")
            else:
                print(f"  {C.RED}✗{C.END} {name_code:40} → INTROUVABLE dans Yuman")
                print(f"      {C.YELLOW}Noms proches :{C.END}")
                for actual_name in sim_fields_actual.keys():
                    if any(word in actual_name.lower() for word in name_code.lower().split()):
                        print(f"        • {actual_name}")
    
    # ═══════════════════════════════════════════════════════════════
    # ÉTAPE 8 : Vérification MODULE
    # ═══════════════════════════════════════════════════════════════
    if CAT_MODULE in by_category:
        print_header("ÉTAPE 8 : VÉRIFICATION CUSTOM FIELDS - MODULE")
        
        module = by_category[CAT_MODULE][0]
        print(f"Analyse de : {module['name']} (id={module['id']})")
        
        mod_data = y.yc.get_material(module['id'], embed="fields")
        mod_fields_actual = {f['name']: f.get('blueprint_id')
                            for f in mod_data.get('_embed', {}).get('fields', [])}
        
        print(f"\n{C.BOLD}Custom fields RÉELS du MODULE :{C.END}")
        for name, bp_id in sorted(mod_fields_actual.items()):
            value = next((f.get('value') for f in mod_data.get('_embed', {}).get('fields', []) 
                         if f['name'] == name), None)
            print(f"  • {name:40} (bp={bp_id}) = {value}")
        
        print(f"\n{C.BOLD}Vérification du champ 'Modèle' (BP_MODEL=13548) :{C.END}")
        if BP_MODEL_NAME in mod_fields_actual:
            print(f"  {C.GREEN}✓{C.END} {BP_MODEL_NAME:40} → OK")
        else:
            print(f"  {C.RED}✗{C.END} {BP_MODEL_NAME:40} → INTROUVABLE")
    
    # ═══════════════════════════════════════════════════════════════
    # SYNTHÈSE
    # ═══════════════════════════════════════════════════════════════
    print_header("✅ VÉRIFICATION TERMINÉE")
    
    print(f"{C.BOLD}Résumé :{C.END}")
    print(f"  • Ce script a comparé tous les custom fields déclarés dans yuman_adapter.py")
    print(f"  • avec les custom fields RÉELS retournés par l'API Yuman")
    print(f"  • pour le site E3K2L")
    print(f"\n{C.YELLOW}Prochaine étape :{C.END}")
    print(f"  → Corriger les noms de champs erronés dans le code")
    print(f"  → Vérifier que les blueprint_id correspondent")


if __name__ == "__main__":
    main()