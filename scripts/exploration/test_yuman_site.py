#!/usr/bin/env python3
"""
Script de test #4 - Analyse complète de tous les équipements d'un site.

Usage:
    python test_yuman_site_analysis.py

Ce script analyse TOUS les équipements du site 583841 pour comprendre :
- Quels types d'équipements existent
- Quels champs sont utilisés pour chaque catégorie
- Comment les données sont mappées entre DB et Yuman
"""

import os
import json
from typing import Dict, Any, List
from collections import defaultdict
from dotenv import load_dotenv
from supabase import create_client
from vysync.yuman_client import YumanClient

load_dotenv()

# Couleurs pour le terminal
class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def print_header(text: str):
    print(f"\n{Colors.HEADER}{Colors.BOLD}{'='*100}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{text}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{'='*100}{Colors.ENDC}\n")

def print_section(text: str):
    print(f"\n{Colors.OKCYAN}{Colors.BOLD}{text}{Colors.ENDC}")
    print(f"{Colors.OKCYAN}{'-'*100}{Colors.ENDC}")

def print_field(label: str, value: Any, indent: int = 0):
    spacing = "  " * indent
    print(f"{spacing}{Colors.OKBLUE}{label}:{Colors.ENDC} {value}")

def print_success(text: str):
    print(f"{Colors.OKGREEN}✓ {text}{Colors.ENDC}")

def print_warning(text: str):
    print(f"{Colors.WARNING}⚠️  {text}{Colors.ENDC}")

def print_error(text: str):
    print(f"{Colors.FAIL}❌ {text}{Colors.ENDC}")


# Mapping des catégories
CATEGORIES = {
    11102: "INVERTER (Onduleur)",
    11103: "MODULE",
    12404: "STRING_PV",
    11441: "PLANT (Centrale)",
    11382: "SIM",
}


def get_equipment_details(yc: YumanClient, material_id: int) -> Dict[str, Any]:
    """Récupère les détails complets d'un équipement."""
    try:
        material = yc.get_material(material_id, embed="fields,category,parent")
        
        # Extraction des champs custom
        fields = material.get("_embed", {}).get("fields", [])
        fields_dict = {f["name"]: f.get("value") for f in fields}
        
        return {
            "id": material['id'],
            "category_id": material.get('category_id'),
            "category_name": CATEGORIES.get(material.get('category_id'), "UNKNOWN"),
            "name": material.get('name'),
            "serial_number": material.get('serial_number'),
            "brand": material.get('brand'),
            "model": material.get('model'),
            "description": material.get('description'),
            "count": material.get('count'),
            "site_id": material.get('site_id'),
            "parent_id": material.get('parent_id'),
            "family_name": material.get('family_name'),
            "material_type": material.get('material_type'),
            "in_service_date": material.get('in_service_date'),
            "warranty_date": material.get('warranty_date'),
            "external_reference": material.get('external_reference'),
            "fields": fields_dict,
            "raw_fields": fields
        }
    except Exception as e:
        print_error(f"Erreur lors de la récupération de l'équipement {material_id}: {e}")
        return None


def analyze_field_usage(equipments_by_category: Dict[int, List[Dict]]):
    """Analyse l'utilisation des champs standard et custom par catégorie."""
    print_header("📊 ANALYSE DE L'UTILISATION DES CHAMPS PAR CATÉGORIE")
    
    for category_id, equipments in sorted(equipments_by_category.items()):
        category_name = CATEGORIES.get(category_id, f"UNKNOWN_{category_id}")
        print_section(f"Catégorie : {category_name} (ID: {category_id}) - {len(equipments)} équipement(s)")
        
        # Analyse des champs standard
        standard_fields = [
            'name', 'serial_number', 'brand', 'model', 'description', 
            'count', 'parent_id', 'family_name', 'in_service_date', 
            'warranty_date', 'external_reference'
        ]
        
        print(f"\n  {Colors.BOLD}Champs STANDARD utilisés :{Colors.ENDC}")
        for field in standard_fields:
            # Compter combien d'équipements utilisent ce champ (valeur non-None)
            used_count = sum(1 for eq in equipments if eq.get(field) not in (None, "", []))
            percentage = (used_count / len(equipments)) * 100
            
            if used_count > 0:
                example_value = next((eq.get(field) for eq in equipments if eq.get(field)), None)
                print(f"    • {field:25} : {used_count:2}/{len(equipments)} ({percentage:5.1f}%) | Ex: {str(example_value)[:50]}")
        
        # Analyse des champs custom
        all_custom_fields = set()
        for eq in equipments:
            all_custom_fields.update(eq.get('fields', {}).keys())
        
        if all_custom_fields:
            print(f"\n  {Colors.BOLD}Champs CUSTOM (fields) utilisés :{Colors.ENDC}")
            for field_name in sorted(all_custom_fields):
                used_count = sum(1 for eq in equipments if eq.get('fields', {}).get(field_name) not in (None, "", []))
                percentage = (used_count / len(equipments)) * 100
                example_value = next((eq.get('fields', {}).get(field_name) for eq in equipments if eq.get('fields', {}).get(field_name)), None)
                
                # Récupérer le blueprint_id
                blueprint_id = None
                for eq in equipments:
                    for f in eq.get('raw_fields', []):
                        if f['name'] == field_name:
                            blueprint_id = f.get('blueprint_id')
                            break
                    if blueprint_id:
                        break
                
                bp_info = f"(bp:{blueprint_id})" if blueprint_id else ""
                print(f"    • {field_name:25} {bp_info:12} : {used_count:2}/{len(equipments)} ({percentage:5.1f}%) | Ex: {str(example_value)[:40]}")


def compare_db_vs_yuman(sb, yc, yuman_site_id: int):
    """Compare les données DB vs Yuman pour tous les équipements du site."""
    print_header("🔍 COMPARAISON DB vs YUMAN")
    
    # Récupérer le site_id Supabase
    site_result = sb.table("sites_mapping").select("id, vcom_system_key").eq(
        "yuman_site_id", yuman_site_id
    ).execute()
    
    if not site_result.data:
        print_error(f"Site Yuman {yuman_site_id} non trouvé dans Supabase")
        return
    
    supabase_site_id = site_result.data[0]['id']
    vcom_system_key = site_result.data[0]['vcom_system_key']
    
    print_field("Yuman Site ID", yuman_site_id)
    print_field("Supabase Site ID", supabase_site_id)
    print_field("VCOM System Key", vcom_system_key)
    
    # Récupérer les équipements DB
    db_equips = sb.table("equipments_mapping").select("*").eq(
        "site_id", supabase_site_id
    ).execute()
    
    print(f"\n{Colors.OKGREEN}✓ {len(db_equips.data)} équipements trouvés dans Supabase{Colors.ENDC}")
    
    # Analyser par catégorie
    discrepancies_by_category = defaultdict(list)
    
    for db_eq in db_equips.data:
        yuman_mat_id = db_eq.get('yuman_material_id')
        if not yuman_mat_id:
            print_warning(f"Équipement DB {db_eq['serial_number']} sans yuman_material_id")
            continue
        
        # Récupérer depuis Yuman
        yuman_eq = get_equipment_details(yc, yuman_mat_id)
        if not yuman_eq:
            continue
        
        # Comparer les champs
        category_id = db_eq['category_id']
        category_name = CATEGORIES.get(category_id, f"UNKNOWN_{category_id}")
        
        discrepancies = []
        
        # Champs standard à comparer
        fields_to_compare = {
            'name': (db_eq.get('name'), yuman_eq.get('name')),
            'serial_number': (db_eq.get('serial_number'), yuman_eq.get('serial_number')),
            'brand': (db_eq.get('brand'), yuman_eq.get('brand')),
            'model': (db_eq.get('model'), yuman_eq.get('model')),
            'count': (db_eq.get('count'), yuman_eq.get('count')),
        }
        
        for field_name, (db_val, yuman_val) in fields_to_compare.items():
            if db_val != yuman_val and not (db_val in (None, "") and yuman_val in (None, "")):
                discrepancies.append({
                    'field': field_name,
                    'db_value': db_val,
                    'yuman_value': yuman_val,
                    'in_custom_fields': field_name in yuman_eq.get('fields', {})
                })
        
        # Vérifier si des champs DB sont dans les fields custom Yuman
        if category_id == 12404:  # STRING
            # count -> nombre de modules
            db_count = db_eq.get('count')
            yuman_nb_modules = yuman_eq.get('fields', {}).get('nombre de modules')
            if str(db_count) != str(yuman_nb_modules):
                discrepancies.append({
                    'field': 'count (DB) vs nombre de modules (Yuman custom)',
                    'db_value': db_count,
                    'yuman_value': yuman_nb_modules,
                    'in_custom_fields': True
                })
            
            # model -> modèle de module
            db_model = db_eq.get('model')
            yuman_model_custom = yuman_eq.get('fields', {}).get('modèle de module')
            if db_model != yuman_model_custom:
                discrepancies.append({
                    'field': 'model (DB) vs modèle de module (Yuman custom)',
                    'db_value': db_model,
                    'yuman_value': yuman_model_custom,
                    'in_custom_fields': True
                })
        
        if discrepancies:
            discrepancies_by_category[category_name].append({
                'serial': db_eq.get('serial_number'),
                'yuman_id': yuman_mat_id,
                'discrepancies': discrepancies
            })
    
    # Afficher les différences par catégorie
    print_section("📋 DIFFÉRENCES DÉTECTÉES PAR CATÉGORIE")
    
    if not any(discrepancies_by_category.values()):
        print_success("Aucune différence détectée ! DB et Yuman sont synchronisés.")
        return
    
    for category_name, items in sorted(discrepancies_by_category.items()):
        print(f"\n  {Colors.BOLD}{category_name} : {len(items)} équipement(s) avec différences{Colors.ENDC}")
        
        for item in items[:3]:  # Limiter à 3 exemples par catégorie
            print(f"\n    Serial: {item['serial']} (Yuman ID: {item['yuman_id']})")
            for disc in item['discrepancies']:
                in_custom = " [DANS CUSTOM FIELDS]" if disc['in_custom_fields'] else ""
                print(f"      • {disc['field']:40}{in_custom}")
                print(f"        DB:    {str(disc['db_value'])[:60]}")
                print(f"        Yuman: {str(disc['yuman_value'])[:60]}")


def main():
    print_header("🧪 TEST #4 - ANALYSE COMPLÈTE D'UN SITE")
    
    # Initialisation
    print("Initialisation des connexions...")
    sb = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_KEY"))
    yc = YumanClient()
    print_success("Connexions établies")
    
    yuman_site_id = 583841
    
    # Récupérer tous les équipements du site depuis Yuman
    print_section(f"📊 RÉCUPÉRATION DES ÉQUIPEMENTS DU SITE {yuman_site_id}")
    
    all_materials = yc.list_materials(embed="fields,category")
    site_materials = [m for m in all_materials if m.get('site_id') == yuman_site_id]
    
    print_success(f"{len(site_materials)} équipements trouvés sur le site {yuman_site_id}")
    
    # Récupérer les détails de chaque équipement
    print("\nRécupération des détails complets...")
    equipments_by_category = defaultdict(list)
    
    for material in site_materials:
        details = get_equipment_details(yc, material['id'])
        if details:
            equipments_by_category[details['category_id']].append(details)
    
    # Afficher la répartition par catégorie
    print_section("📦 RÉPARTITION PAR CATÉGORIE")
    for category_id, equipments in sorted(equipments_by_category.items()):
        category_name = CATEGORIES.get(category_id, f"UNKNOWN_{category_id}")
        print(f"  • {category_name:30} : {len(equipments)} équipement(s)")
    
    # Analyser l'utilisation des champs
    analyze_field_usage(equipments_by_category)
    
    # Comparer avec la DB
    compare_db_vs_yuman(sb, yc, yuman_site_id)
    
    # Recommandations finales
    print_header("💡 RECOMMANDATIONS POUR LA SYNCHRONISATION")
    
    print(f"\n{Colors.BOLD}1. CHAMPS STANDARD À SYNCHRONISER :{Colors.ENDC}")
    print(f"{Colors.OKGREEN}   Ces champs devraient être dans le payload standard :{Colors.ENDC}")
    print("   • brand")
    print("   • description")
    print("   • in_service_date")
    print("   • warranty_date")
    print("   • serial_number (avec précaution)")
    
    print(f"\n{Colors.BOLD}2. CHAMPS À IGNORER (non-modifiables) :{Colors.ENDC}")
    print(f"{Colors.FAIL}   Ne pas essayer de synchroniser ces champs :{Colors.ENDC}")
    print("   • name")
    print("   • model (champ standard)")
    print("   • count (champ standard)")
    print("   • site_id")
    print("   • parent_id")
    print("   • category_id")
    
    print(f"\n{Colors.BOLD}3. MAPPING CHAMPS DB → FIELDS CUSTOM :{Colors.ENDC}")
    print(f"{Colors.WARNING}   Ces données DB doivent aller dans les fields custom :{Colors.ENDC}")
    print("   • Equipment.count → fields[nombre de modules] (STRING)")
    print("   • Equipment.model → fields[modèle de module] (STRING)")
    print("   • Equipment.model → fields[Modèle] (INVERTER)")
    
    print(f"\n{Colors.BOLD}4. LOGIQUE DE COMPARAISON :{Colors.ENDC}")
    print(f"{Colors.OKBLUE}   _equip_equals() devrait comparer :{Colors.ENDC}")
    print("   • Les champs standard modifiables (brand, serial_number)")
    print("   • NE PAS comparer count/model standard (ils sont dans fields)")
    print("   • Les fields custom seront détectés dans apply_equips_patch()")
    
    print_header("✅ ANALYSE TERMINÉE")


if __name__ == "__main__":
    main()