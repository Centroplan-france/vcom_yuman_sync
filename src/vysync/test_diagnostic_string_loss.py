#!/usr/bin/env python3
"""
Test du flux Yuman → Supabase pour le site E3K2L
Réplique exactement le code de cli.py (PHASE 1 B) avec diagnostics
"""

import sys
from pathlib import Path
from collections import defaultdict
from typing import Dict, Any

# Setup logging
sys.path.insert(0, str(Path(__file__).parent.parent))
from vysync.logging_config import setup_logging
setup_logging()

from vysync.adapters.supabase_adapter import SupabaseAdapter
from vysync.adapters.yuman_adapter import YumanAdapter
from vysync.diff import diff_fill_missing
from vysync.models import Equipment, Site, CAT_INVERTER, CAT_MODULE, CAT_STRING, CAT_SIM, CAT_CENTRALE

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

def print_section(text: str):
    print(f"\n{C.BLUE}{C.BOLD}{text}{C.END}")
    print(f"{C.BLUE}{'-'*80}{C.END}")

def print_equipment_detail(eq: Equipment, sb: SupabaseAdapter, label: str):
    """Affiche TOUS les champs d'un équipement"""
    cat_names = {
        CAT_MODULE: "MODULE",
        CAT_INVERTER: "INVERTER",
        CAT_STRING: "STRING",
        CAT_SIM: "SIM",
        CAT_CENTRALE: "CENTRALE"
    }
    
    print(f"\n{C.BOLD}[{cat_names.get(eq.category_id, 'UNKNOWN')}] {label}{C.END}")
    print(f"  site_id:            {eq.site_id}")
    print(f"  category_id:        {eq.category_id}")
    print(f"  eq_type:            {eq.eq_type}")
    print(f"  vcom_device_id:     {eq.vcom_device_id}")
    print(f"  name:               {eq.name}")
    print(f"  brand:              {eq.brand}")
    print(f"  model:              {eq.model}")
    print(f"  serial_number:      {eq.serial_number}")
    print(f"  count:              {eq.count}")
    print(f"  parent_id:          {eq.parent_id}")
    print(f"  yuman_material_id:  {eq.yuman_material_id}")

def compare_equipment(old: Equipment, new: Equipment, sb: SupabaseAdapter):
    """Compare 2 équipements et affiche les différences"""
    cat_names = {
        CAT_MODULE: "MODULE",
        CAT_INVERTER: "INVERTER",
        CAT_STRING: "STRING",
        CAT_SIM: "SIM",
        CAT_CENTRALE: "CENTRALE"
    }
    
    print(f"\n{C.BOLD}[{cat_names.get(new.category_id, 'UNKNOWN')}] {new.name}{C.END}")
    
    # Comparer tous les champs
    fields = ['site_id', 'category_id', 'eq_type', 'vcom_device_id', 'name', 
              'brand', 'model', 'serial_number', 'count', 'parent_id', 'yuman_material_id']
    
    has_changes = False
    for field in fields:
        old_val = getattr(old, field, None) if old else None
        new_val = getattr(new, field, None)
        
        if old_val != new_val:
            has_changes = True
            print(f"  {field:20} : {C.RED}{old_val}{C.END} → {C.GREEN}{new_val}{C.END}")
        else:
            print(f"  {field:20} : {old_val}")
    
    if not has_changes:
        print(f"  {C.YELLOW}(aucun changement){C.END}")

def main():
    SITE_KEY = "E3K2L"
    
    print_header(f"TEST YUMAN → SUPABASE pour {SITE_KEY}")
    print(f"{C.YELLOW}Ce script reproduit EXACTEMENT le code de cli.py (PHASE 1 B){C.END}")
    
    # ═══════════════════════════════════════════════════════════════
    # ÉTAPE 1 : INITIALISATION
    # ═══════════════════════════════════════════════════════════════
    print_header("ÉTAPE 1 : INITIALISATION")
    
    print("Connexion Supabase...")
    sb = SupabaseAdapter()
    print(f"{C.GREEN}✓ Supabase connecté{C.END}")
    
    print("Connexion Yuman...")
    y = YumanAdapter(sb)
    print(f"{C.GREEN}✓ Yuman connecté{C.END}")
    
    # Résolution E3K2L → yuman_site_id
    print(f"\nRésolution {SITE_KEY} → yuman_site_id...")
    site_result = sb.sb.table("sites_mapping").select("id, yuman_site_id").eq(
        "vcom_system_key", SITE_KEY
    ).execute()
    
    if not site_result.data:
        print(f"{C.RED}✗ Site {SITE_KEY} non trouvé en DB{C.END}")
        return
    
    supabase_site_id = site_result.data[0]['id']
    yuman_site_id = site_result.data[0]['yuman_site_id']
    
    print(f"  • Supabase site_id:  {supabase_site_id}")
    print(f"  • Yuman site_id:     {yuman_site_id}")
    
    if not yuman_site_id:
        print(f"{C.RED}✗ Pas de yuman_site_id pour {SITE_KEY}{C.END}")
        return
    
    # ═══════════════════════════════════════════════════════════════
    # ÉTAPE 2 : SNAPSHOT YUMAN (CODE EXACT DE cli.py)
    # ═══════════════════════════════════════════════════════════════
    print_header("ÉTAPE 2 : SNAPSHOT YUMAN (fetch complet puis filtrage)")
    
    print("Appel y.yc.list_clients()...")
    y_clients = list(y.yc.list_clients())
    print(f"{C.GREEN}✓ {len(y_clients)} clients récupérés{C.END}")
    
    print("\nAppel y.fetch_sites()...")
    y_sites = y.fetch_sites()
    print(f"{C.GREEN}✓ {len(y_sites)} sites récupérés (TOUS){C.END}")
    
    print("\nAppel y.fetch_equips()...")
    y_equips = y.fetch_equips()
    print(f"{C.GREEN}✓ {len(y_equips)} équipements récupérés (TOUS){C.END}")
    
    # Filtrage sur notre site
    print(f"\n{C.YELLOW}Filtrage sur yuman_site_id={yuman_site_id}...{C.END}")
    
    y_sites_filtered = {k: s for k, s in y_sites.items() if k == yuman_site_id}
    y_equips_filtered = {k: e for k, e in y_equips.items() 
                         if e.site_id == supabase_site_id}
    
    print(f"  • Sites filtrés:       {len(y_sites_filtered)}")
    print(f"  • Équipements filtrés: {len(y_equips_filtered)}")
    
    # Statistiques par catégorie
    by_cat = defaultdict(int)
    for eq in y_equips_filtered.values():
        by_cat[eq.category_id] += 1
    
    print(f"\n{C.BOLD}Répartition Yuman par catégorie :{C.END}")
    for cat_id, count in sorted(by_cat.items()):
        cat_names = {
            CAT_MODULE: "MODULE",
            CAT_INVERTER: "INVERTER",
            CAT_STRING: "STRING",
            CAT_SIM: "SIM",
            CAT_CENTRALE: "CENTRALE"
        }
        print(f"  • {cat_names.get(cat_id, 'UNKNOWN'):15} : {count}")
    

    # ═══════════════════════════════════════════════════════════════
    # DEBUG : Données brutes Yuman pour 1 STRING
    # ═══════════════════════════════════════════════════════════════
    print_header("DEBUG : DONNÉES BRUTES YUMAN pour STRING")

    # Trouver le STRING problématique
    target_string = None
    for eq in y_equips_filtered.values():
        if eq.category_id == CAT_STRING and "MPPT-4.1" in eq.vcom_device_id:
            target_string = eq
            break

    if target_string:
        print(f"STRING trouvé : {target_string.name}")
        print(f"yuman_material_id : {target_string.yuman_material_id}")
        
        # Fetch les détails complets depuis Yuman
        print("\nAppel y.yc.get_material() avec embed=fields...")
        raw_mat = y.yc.get_material(target_string.yuman_material_id, embed="fields")
        
        print(f"\n{C.BOLD}Champs standard Yuman :{C.END}")
        print(f"  name:          {raw_mat.get('name')}")
        print(f"  brand:         {raw_mat.get('brand')}")
        print(f"  model:         {raw_mat.get('model')}")
        print(f"  count:         {raw_mat.get('count')}")
        print(f"  parent_id:     {raw_mat.get('parent_id')}")
        print(f"  serial_number: {raw_mat.get('serial_number')}")
        
        print(f"\n{C.BOLD}Custom fields (_embed.fields) :{C.END}")
        for field in raw_mat.get("_embed", {}).get("fields", []):
            print(f"  • {field['name']:30} = {field.get('value')}")


    # ═══════════════════════════════════════════════════════════════
    # ÉTAPE 3 : SNAPSHOT DB ACTUEL (CODE EXACT DE cli.py)
    # ═══════════════════════════════════════════════════════════════
    print_header("ÉTAPE 3 : SNAPSHOT DB ACTUEL")
    
    print("Appel sb.fetch_sites_y()...")
    db_maps_sites = sb.fetch_sites_y()
    print(f"{C.GREEN}✓ {len(db_maps_sites)} sites en DB (TOUS){C.END}")
    
    print("\nAppel sb.fetch_equipments_y()...")
    db_maps_equips = sb.fetch_equipments_y()
    print(f"{C.GREEN}✓ {len(db_maps_equips)} équipements en DB (TOUS){C.END}")
    
    # Filtrage sur notre site
    print(f"\n{C.YELLOW}Filtrage sur yuman_site_id={yuman_site_id}...{C.END}")
    
    db_maps_sites_filtered = {k: s for k, s in db_maps_sites.items() 
                              if k == yuman_site_id}
    db_maps_equips_filtered = {k: e for k, e in db_maps_equips.items() 
                               if e.site_id == supabase_site_id}
    
    print(f"  • Sites filtrés:       {len(db_maps_sites_filtered)}")
    print(f"  • Équipements filtrés: {len(db_maps_equips_filtered)}")
    
    # Statistiques par catégorie
    by_cat_db = defaultdict(int)
    for eq in db_maps_equips_filtered.values():
        by_cat_db[eq.category_id] += 1
    
    print(f"\n{C.BOLD}Répartition DB par catégorie :{C.END}")
    for cat_id, count in sorted(by_cat_db.items()):
        cat_names = {
            CAT_MODULE: "MODULE",
            CAT_INVERTER: "INVERTER",
            CAT_STRING: "STRING",
            CAT_SIM: "SIM",
            CAT_CENTRALE: "CENTRALE"
        }
        print(f"  • {cat_names.get(cat_id, 'UNKNOWN'):15} : {count}")
    
    # ═══════════════════════════════════════════════════════════════
    # ÉTAPE 4 : DIFF (CODE EXACT DE cli.py)
    # ═══════════════════════════════════════════════════════════════
    print_header("ÉTAPE 4 : DIFF (diff_fill_missing)")
    
    print("Calcul patch_sites avec diff_fill_missing...")
    patch_maps_sites = diff_fill_missing(
        db_maps_sites_filtered,
        y_sites_filtered,
        fields=["yuman_site_id", "code", "client_map_id", "name", "aldi_id",
                "aldi_store_id", "project_number_cp", "commission_date", "nominal_power"]
    )
    
    print(f"{C.GREEN}✓ Patch sites calculé{C.END}")
    print(f"  • ADD:    {len(patch_maps_sites.add)}")
    print(f"  • UPDATE: {len(patch_maps_sites.update)}")
    print(f"  • DELETE: {len(patch_maps_sites.delete)}")
    
    print("\nCalcul patch_equips avec diff_fill_missing...")
    patch_maps_equips = diff_fill_missing(
        db_maps_equips_filtered,
        y_equips_filtered,
        fields=["category_id", "eq_type", "name", "yuman_material_id",
                "serial_number", "brand", "model", "count", "parent_id"]
    )
    
    print(f"{C.GREEN}✓ Patch équipements calculé{C.END}")
    print(f"  • ADD:    {len(patch_maps_equips.add)}")
    print(f"  • UPDATE: {len(patch_maps_equips.update)}")
    print(f"  • DELETE: {len(patch_maps_equips.delete)}")
    
    # ═══════════════════════════════════════════════════════════════
    # ÉTAPE 5 : DÉTAILS DES CHANGEMENTS (1 par catégorie)
    # ═══════════════════════════════════════════════════════════════
    print_header("ÉTAPE 5 : DÉTAILS DES CHANGEMENTS")
    
    if patch_maps_equips.add:
        print_section("AJOUTS")
        by_cat_add = defaultdict(list)
        for eq in patch_maps_equips.add:
            by_cat_add[eq.category_id].append(eq)
        
        for cat_id in sorted(by_cat_add.keys()):
            eq = by_cat_add[cat_id][0]  # Premier de chaque catégorie
            print_equipment_detail(eq, sb, f"{eq.name} (NOUVEAU)")
    
    if patch_maps_equips.update:
        print_section("MISES À JOUR")
        by_cat_upd = defaultdict(list)
        for old, new in patch_maps_equips.update:
            by_cat_upd[new.category_id].append((old, new))
        
        for cat_id in sorted(by_cat_upd.keys()):
            old, new = by_cat_upd[cat_id][0]  # Premier de chaque catégorie
            compare_equipment(old, new, sb)
    
    # ═══════════════════════════════════════════════════════════════
    # ÉTAPE 6 : CONFIRMATION
    # ═══════════════════════════════════════════════════════════════
    print_header("ÉTAPE 6 : CONFIRMATION")
    
    print(f"{C.YELLOW}⚠️  Cette étape va MODIFIER la base de données{C.END}")
    print(f"\nChangements à appliquer :")
    print(f"  Sites:       +{len(patch_maps_sites.add)} ~{len(patch_maps_sites.update)} -{len(patch_maps_sites.delete)}")
    print(f"  Équipements: +{len(patch_maps_equips.add)} ~{len(patch_maps_equips.update)} -{len(patch_maps_equips.delete)}")
    
    response = input(f"\n{C.BOLD}Taper 'oui' pour appliquer : {C.END}")
    
    if response.lower() != "oui":
        print(f"{C.RED}✗ Application annulée{C.END}")
        return
    
    # ═══════════════════════════════════════════════════════════════
    # ÉTAPE 7 : APPLICATION (CODE EXACT DE cli.py)
    # ═══════════════════════════════════════════════════════════════
    print_header("ÉTAPE 7 : APPLICATION")
    
    print("Application sb.apply_sites_patch()...")
    sb.apply_sites_patch(patch_maps_sites)
    print(f"{C.GREEN}✓ Sites appliqués{C.END}")
    
    print("\nApplication sb.apply_equips_mapping_patch()...")
    sb.apply_equips_mapping_patch(patch_maps_equips)
    print(f"{C.GREEN}✓ Équipements appliqués{C.END}")
    
    # ═══════════════════════════════════════════════════════════════
    # ÉTAPE 8 : VÉRIFICATION FINALE
    # ═══════════════════════════════════════════════════════════════
    print_header("ÉTAPE 8 : VÉRIFICATION FINALE")
    
    print("Re-fetch DB après application...")
    db_maps_equips_after = sb.fetch_equipments_y()
    db_maps_equips_after_filtered = {k: e for k, e in db_maps_equips_after.items()
                                     if e.site_id == supabase_site_id}
    
    print(f"{C.GREEN}✓ {len(db_maps_equips_after_filtered)} équipements en DB{C.END}")
    
    # Comparaison avant/après
    by_cat_after = defaultdict(int)
    for eq in db_maps_equips_after_filtered.values():
        by_cat_after[eq.category_id] += 1
    
    print(f"\n{C.BOLD}Comparaison AVANT → APRÈS :{C.END}")
    all_cats = set(by_cat_db.keys()) | set(by_cat_after.keys())
    for cat_id in sorted(all_cats):
        cat_names = {
            CAT_MODULE: "MODULE",
            CAT_INVERTER: "INVERTER",
            CAT_STRING: "STRING",
            CAT_SIM: "SIM",
            CAT_CENTRALE: "CENTRALE"
        }
        before = by_cat_db.get(cat_id, 0)
        after = by_cat_after.get(cat_id, 0)
        diff = after - before
        color = C.GREEN if diff >= 0 else C.RED
        print(f"  • {cat_names.get(cat_id, 'UNKNOWN'):15} : {before} → {after} {color}({diff:+d}){C.END}")
    
    print_header("✅ TEST TERMINÉ")


if __name__ == "__main__":
    main()