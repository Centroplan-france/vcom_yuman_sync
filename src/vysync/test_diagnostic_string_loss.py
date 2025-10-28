#!/usr/bin/env python3
"""
Test détaillé du flux VCOM → Supabase pour le site E3K2L

Ce script analyse chaque étape du processus de synchronisation :
1. État initial DB
2. Snapshot VCOM
3. Diff (détection des changements)
4. Application des patches
"""

import sys
import os
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Any

# Ajouter le chemin src au PYTHONPATH
sys.path.insert(0, str(Path(__file__).parent / "src"))

from vysync.logging_config import setup_logging
from vysync.vcom_client import VCOMAPIClient
from vysync.adapters.supabase_adapter import SupabaseAdapter
from vysync.adapters.vcom_adapter import fetch_snapshot
from vysync.diff import diff_entities
from vysync.models import Equipment, Site, CAT_INVERTER, CAT_MODULE, CAT_STRING, CAT_SIM, CAT_CENTRALE

# Configuration du logging
setup_logging()

# Couleurs pour terminal
class C:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    END = '\033[0m'
    BOLD = '\033[1m'

def print_header(text: str):
    """Affiche un header coloré"""
    print(f"\n{C.HEADER}{C.BOLD}{'='*80}{C.END}")
    print(f"{C.HEADER}{C.BOLD}{text}{C.END}")
    print(f"{C.HEADER}{C.BOLD}{'='*80}{C.END}\n")

def print_section(text: str):
    """Affiche une section"""
    print(f"\n{C.BLUE}{C.BOLD}{text}{C.END}")
    print(f"{C.BLUE}{'-'*80}{C.END}")

def print_equipment_detail(eq: Equipment, sb: SupabaseAdapter, prefix: str = ""):
    """Affiche les détails d'un équipement"""
    cat_names = {
        CAT_MODULE: "MODULE",
        CAT_INVERTER: "INVERTER",
        CAT_STRING: "STRING",
        CAT_SIM: "SIM",
        CAT_CENTRALE: "CENTRALE"
    }
    
    print(f"{prefix}{C.BOLD}[{cat_names.get(eq.category_id, 'UNKNOWN')}] {eq.name}{C.END}")
    print(f"{prefix}  • vcom_device_id:     {eq.vcom_device_id}")
    print(f"{prefix}  • serial_number:      {eq.serial_number}")
    print(f"{prefix}  • brand:              {eq.brand}")
    print(f"{prefix}  • model:              {eq.model}")
    print(f"{prefix}  • count:              {eq.count}")
    print(f"{prefix}  • parent_id:          {eq.parent_id}")
    print(f"{prefix}  • yuman_material_id:  {eq.yuman_material_id}")
    print(f"{prefix}  • site_id:            {eq.site_id}")
    print(f"{prefix}  • vcom_system_key:    {eq.get_vcom_system_key(sb)}")

def print_site_detail(site: Site, sb: SupabaseAdapter, prefix: str = ""):
    """Affiche les détails d'un site"""
    print(f"{prefix}{C.BOLD}{site.name}{C.END}")
    print(f"{prefix}  • id:                 {site.id}")
    print(f"{prefix}  • vcom_system_key:    {site.get_vcom_system_key(sb)}")
    print(f"{prefix}  • yuman_site_id:      {site.get_yuman_site_id(sb)}")
    print(f"{prefix}  • address:            {site.address}")
    print(f"{prefix}  • latitude:           {site.latitude}")
    print(f"{prefix}  • longitude:          {site.longitude}")
    print(f"{prefix}  • nominal_power:      {site.nominal_power}")
    print(f"{prefix}  • commission_date:    {site.commission_date}")
    print(f"{prefix}  • client_map_id:      {site.client_map_id}")

def group_by_category(equips: Dict[str, Equipment]) -> Dict[int, List[Equipment]]:
    """Regroupe les équipements par catégorie"""
    groups = defaultdict(list)
    for eq in equips.values():
        groups[eq.category_id].append(eq)
    return dict(groups)

def print_patch_details(patch, data_type: str, sb: SupabaseAdapter):
    """Affiche les détails d'un patch"""
    print(f"\n{C.YELLOW}Patch {data_type}:{C.END}")
    print(f"  • ADD:    {len(patch.add)}")
    print(f"  • UPDATE: {len(patch.update)}")
    print(f"  • DELETE: {len(patch.delete)}")
    
    if data_type == "Equipment":
        # Grouper par catégorie
        if patch.add:
            print(f"\n{C.GREEN}  Ajouts par catégorie :{C.END}")
            groups = group_by_category({e.key(): e for e in patch.add})
            for cat_id, items in groups.items():
                cat_names = {
                    CAT_MODULE: "MODULE",
                    CAT_INVERTER: "INVERTER",
                    CAT_STRING: "STRING",
                    CAT_SIM: "SIM",
                    CAT_CENTRALE: "CENTRALE"
                }
                print(f"    - {cat_names.get(cat_id, 'UNKNOWN')}: {len(items)}")
                # Afficher le premier de chaque catégorie
                print_equipment_detail(items[0], sb, prefix="      ")
        
        if patch.update:
            print(f"\n{C.YELLOW}  Mises à jour par catégorie :{C.END}")
            groups = group_by_category({e[1].key(): e[1] for e in patch.update})
            for cat_id, items in groups.items():
                cat_names = {
                    CAT_MODULE: "MODULE",
                    CAT_INVERTER: "INVERTER",
                    CAT_STRING: "STRING",
                    CAT_SIM: "SIM",
                    CAT_CENTRALE: "CENTRALE"
                }
                print(f"    - {cat_names.get(cat_id, 'UNKNOWN')}: {len(items)}")
                # Afficher le premier changement de chaque catégorie
                old, new = patch.update[0]
                print(f"      {C.RED}AVANT:{C.END}")
                print_equipment_detail(old, sb, prefix="        ")
                print(f"      {C.GREEN}APRÈS:{C.END}")
                print_equipment_detail(new, sb, prefix="        ")
    
    elif data_type == "Site":
        if patch.add:
            print(f"\n{C.GREEN}  Ajouts :{C.END}")
            for site in patch.add:
                print_site_detail(site, sb, prefix="    ")
        
        if patch.update:
            print(f"\n{C.YELLOW}  Mises à jour :{C.END}")
            for old, new in patch.update:
                print(f"    {C.RED}AVANT:{C.END}")
                print_site_detail(old, sb, prefix="      ")
                print(f"    {C.GREEN}APRÈS:{C.END}")
                print_site_detail(new, sb, prefix="      ")

def main():
    """Point d'entrée du test"""
    SITE_KEY = "E3K2L"
    
    print_header(f"TEST VCOM → SUPABASE pour le site {SITE_KEY}")
    
    # ══════════════════════════════════════════════════════════════════
    # INITIALISATION
    # ══════════════════════════════════════════════════════════════════
    print_section("INITIALISATION")
    print("Connexion à VCOM...")
    vc = VCOMAPIClient()
    print(f"{C.GREEN}✓ VCOM connecté{C.END}")
    
    print("Connexion à Supabase...")
    sb = SupabaseAdapter()
    print(f"{C.GREEN}✓ Supabase connecté{C.END}")
    
    # ══════════════════════════════════════════════════════════════════
    # ÉTAPE 1 : ÉTAT INITIAL DB
    # ══════════════════════════════════════════════════════════════════
    print_header(f"ÉTAPE 1 : ÉTAT INITIAL DB pour {SITE_KEY}")
    
    print("Récupération des données DB...")
    db_sites = sb.fetch_sites_v(site_key=SITE_KEY)
    db_equips = sb.fetch_equipments_v(site_key=SITE_KEY)
    
    print(f"\n{C.BOLD}Résultats :{C.END}")
    print(f"  • Sites:       {len(db_sites)}")
    print(f"  • Équipements: {len(db_equips)}")
    
    if db_sites:
        print(f"\n{C.BOLD}Détail du site :{C.END}")
        for site in db_sites.values():
            print_site_detail(site, sb, prefix="  ")
    
    if db_equips:
        print(f"\n{C.BOLD}Équipements par catégorie :{C.END}")
        groups = group_by_category(db_equips)
        for cat_id, items in groups.items():
            cat_names = {
                CAT_MODULE: "MODULE",
                CAT_INVERTER: "INVERTER",
                CAT_STRING: "STRING",
                CAT_SIM: "SIM",
                CAT_CENTRALE: "CENTRALE"
            }
            print(f"\n  [{cat_names.get(cat_id, 'UNKNOWN')}] : {len(items)} équipement(s)")
            # Afficher le premier de chaque type
            print_equipment_detail(items[0], sb, prefix="    ")
    
    # ══════════════════════════════════════════════════════════════════
    # ÉTAPE 2 : SNAPSHOT VCOM
    # ══════════════════════════════════════════════════════════════════
    print_header(f"ÉTAPE 2 : SNAPSHOT VCOM pour {SITE_KEY}")
    
    print("Appel de fetch_snapshot()...")
    print(f"{C.YELLOW}⏳ Ceci va faire plusieurs appels API VCOM...{C.END}")
    
    v_sites, v_equips = fetch_snapshot(
        vc,
        vcom_system_key=SITE_KEY,
        skip_keys=None,
        sb_adapter=sb
    )
    
    print(f"\n{C.BOLD}Résultats :{C.END}")
    print(f"  • Sites:       {len(v_sites)}")
    print(f"  • Équipements: {len(v_equips)}")
    
    if v_sites:
        print(f"\n{C.BOLD}Détail du site :{C.END}")
        for site in v_sites.values():
            print_site_detail(site, sb, prefix="  ")
    
    if v_equips:
        print(f"\n{C.BOLD}Équipements par catégorie :{C.END}")
        groups = group_by_category(v_equips)
        for cat_id, items in groups.items():
            cat_names = {
                CAT_MODULE: "MODULE",
                CAT_INVERTER: "INVERTER",
                CAT_STRING: "STRING",
                CAT_SIM: "SIM",
                CAT_CENTRALE: "CENTRALE"
            }
            print(f"\n  [{cat_names.get(cat_id, 'UNKNOWN')}] : {len(items)} équipement(s)")
            # Afficher le premier de chaque type
            print_equipment_detail(items[0], sb, prefix="    ")
    
    # ══════════════════════════════════════════════════════════════════
    # ÉTAPE 3 : DIFF
    # ══════════════════════════════════════════════════════════════════
    print_header("ÉTAPE 3 : DIFF (DB → VCOM)")
    
    print("Calcul des différences pour les sites...")
    patch_sites = diff_entities(
        db_sites, v_sites,
        ignore_fields={"yuman_site_id", "client_map_id", "code", "ignore_site"}
    )
    
    print("Calcul des différences pour les équipements...")
    patch_equips = diff_entities(
        db_equips, v_equips,
        ignore_fields={"yuman_material_id", "parent_id"}
    )
    
    print_patch_details(patch_sites, "Site", sb)
    print_patch_details(patch_equips, "Equipment", sb)
    
    # ══════════════════════════════════════════════════════════════════
    # ÉTAPE 4 : APPLICATION (avec confirmation)
    # ══════════════════════════════════════════════════════════════════
    print_header("ÉTAPE 4 : APPLICATION DES PATCHES")
    
    print(f"{C.YELLOW}⚠️  Cette étape va MODIFIER la base de données{C.END}")
    print(f"\nChangements à appliquer :")
    print(f"  Sites:       +{len(patch_sites.add)} ~{len(patch_sites.update)} -{len(patch_sites.delete)}")
    print(f"  Équipements: +{len(patch_equips.add)} ~{len(patch_equips.update)} -{len(patch_equips.delete)}")
    
    response = input(f"\n{C.BOLD}Confirmer l'application ? (oui/non) :{C.END} ")
    
    if response.lower() != "oui":
        print(f"{C.RED}✗ Application annulée{C.END}")
        return
    
    print(f"\n{C.GREEN}Application des patches...{C.END}")
    
    print("  → Application patch sites...")
    sb.apply_sites_patch(patch_sites)
    print(f"    {C.GREEN}✓ Sites mis à jour{C.END}")
    
    print("  → Application patch équipements...")
    sb.apply_equips_patch(patch_equips)
    print(f"    {C.GREEN}✓ Équipements mis à jour{C.END}")
    
    # ══════════════════════════════════════════════════════════════════
    # ÉTAPE 5 : VÉRIFICATION FINALE
    # ══════════════════════════════════════════════════════════════════
    print_header("ÉTAPE 5 : VÉRIFICATION FINALE")
    
    print("Re-lecture de la DB après application...")
    db_sites_after = sb.fetch_sites_v(site_key=SITE_KEY)
    db_equips_after = sb.fetch_equipments_v(site_key=SITE_KEY)
    
    print(f"\n{C.BOLD}État final DB :{C.END}")
    print(f"  • Sites:       {len(db_sites_after)}")
    print(f"  • Équipements: {len(db_equips_after)}")
    
    print(f"\n{C.BOLD}Comparaison avant/après :{C.END}")
    print(f"  Sites:       {len(db_sites)} → {len(db_sites_after)} ({len(db_sites_after) - len(db_sites):+d})")
    print(f"  Équipements: {len(db_equips)} → {len(db_equips_after)} ({len(db_equips_after) - len(db_equips):+d})")
    
    # Grouper par catégorie pour comparaison
    if db_equips_after:
        print(f"\n{C.BOLD}Équipements finaux par catégorie :{C.END}")
        groups_after = group_by_category(db_equips_after)
        groups_before = group_by_category(db_equips)
        
        for cat_id in sorted(set(groups_after.keys()) | set(groups_before.keys())):
            cat_names = {
                CAT_MODULE: "MODULE",
                CAT_INVERTER: "INVERTER",
                CAT_STRING: "STRING",
                CAT_SIM: "SIM",
                CAT_CENTRALE: "CENTRALE"
            }
            before = len(groups_before.get(cat_id, []))
            after = len(groups_after.get(cat_id, []))
            diff = after - before
            print(f"  [{cat_names.get(cat_id, 'UNKNOWN')}]: {before} → {after} ({diff:+d})")
    
    print_header("✅ TEST TERMINÉ")


if __name__ == "__main__":
    main()