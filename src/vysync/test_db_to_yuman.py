#!/usr/bin/env python3
"""
Test du flux Supabase → Yuman pour le site E3K2L

Ce script teste la synchronisation complète :
1. État initial DB (sites_mapping + equipments_mapping)
2. État initial Yuman (via YumanAdapter)
3. Diff (détection des changements)
4. Analyse détaillée des écarts (standard + custom fields)
5. Confirmation utilisateur
6. Application des patches
7. Vérification finale
"""

import sys
import os
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Any

# Ajouter le chemin src au PYTHONPATH
sys.path.insert(0, str(Path(__file__).parent.parent))

from vysync.logging_config import setup_logging
from vysync.adapters.supabase_adapter import SupabaseAdapter
from vysync.adapters.yuman_adapter import YumanAdapter
from vysync.diff import diff_entities, set_parent_map
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
    print(f"\n{C.HEADER}{C.BOLD}{'='*100}{C.END}")
    print(f"{C.HEADER}{C.BOLD}{text}{C.END}")
    print(f"{C.HEADER}{C.BOLD}{'='*100}{C.END}\n")

def print_section(text: str):
    """Affiche une section"""
    print(f"\n{C.BLUE}{C.BOLD}{text}{C.END}")
    print(f"{C.BLUE}{'-'*100}{C.END}")

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
    
    # Custom fields selon la catégorie
    if eq.category_id == CAT_STRING:
        print(f"{prefix}  • CUSTOM mppt_idx:     {getattr(eq, 'mppt_idx', 'N/A')}")
        print(f"{prefix}  • CUSTOM nb_modules:   {getattr(eq, 'nb_modules', 'N/A')}")
        print(f"{prefix}  • CUSTOM module_brand: {getattr(eq, 'module_brand', 'N/A')}")
        print(f"{prefix}  • CUSTOM module_model: {getattr(eq, 'module_model', 'N/A')}")

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

def print_patch_summary(patch, data_type: str):
    """Affiche un résumé du patch"""
    print(f"\n{C.YELLOW}Patch {data_type}:{C.END}")
    print(f"  • ADD:    {len(patch.add)}")
    print(f"  • UPDATE: {len(patch.update)}")
    print(f"  • DELETE: {len(patch.delete)}")

def print_site_diff(old: Site, new: Site, sb: SupabaseAdapter):
    """Affiche les différences entre deux sites"""
    print(f"\n{C.YELLOW}Site UPDATE : {new.name}{C.END}")
    
    # Champs standard
    changes = []
    for field in ['name', 'address', 'latitude', 'longitude', 'nominal_power', 'commission_date']:
        old_val = getattr(old, field, None)
        new_val = getattr(new, field, None)
        if old_val != new_val:
            changes.append((field, old_val, new_val))
    
    if changes:
        print(f"  {C.BOLD}Champs standard modifiés :{C.END}")
        for field, old_val, new_val in changes:
            print(f"    • {field:20} : {C.RED}{old_val}{C.END} → {C.GREEN}{new_val}{C.END}")
    
    # Custom fields (simulés - on les récupérera via l'API)
    print(f"  {C.BOLD}Custom fields (à vérifier via API Yuman) :{C.END}")
    print(f"    • System Key (Vcom ID)")
    print(f"    • Nominal Power (kWc)")
    print(f"    • Commission Date")

def print_equipment_diff(old: Equipment, new: Equipment, sb: SupabaseAdapter):
    """Affiche les différences entre deux équipements"""
    cat_names = {
        CAT_MODULE: "MODULE",
        CAT_INVERTER: "INVERTER",
        CAT_STRING: "STRING",
        CAT_SIM: "SIM",
        CAT_CENTRALE: "CENTRALE"
    }
    
    print(f"\n{C.YELLOW}Equipment UPDATE [{cat_names.get(new.category_id)}] : {new.name}{C.END}")
    
    # Champs standard modifiables
    changes = []
    for field in ['serial_number', 'brand', 'model', 'count', 'parent_id']:
        old_val = getattr(old, field, None)
        new_val = getattr(new, field, None)
        if old_val != new_val:
            changes.append((field, old_val, new_val))
    
    if changes:
        print(f"  {C.BOLD}Champs modifiés :{C.END}")
        for field, old_val, new_val in changes:
            print(f"    • {field:20} : {C.RED}{old_val}{C.END} → {C.GREEN}{new_val}{C.END}")
    
    # Custom fields selon la catégorie
    if new.category_id == CAT_STRING:
        print(f"  {C.BOLD}Custom fields STRING :{C.END}")
        for field in ['mppt_idx', 'nb_modules', 'module_brand', 'module_model']:
            old_val = getattr(old, field, None)
            new_val = getattr(new, field, None)
            if old_val != new_val:
                print(f"    • {field:20} : {C.RED}{old_val}{C.END} → {C.GREEN}{new_val}{C.END}")
    elif new.category_id == CAT_INVERTER:
        print(f"  {C.BOLD}Custom fields INVERTER :{C.END}")
        print(f"    • Modèle (custom field)")
        print(f"    • Inverter ID (Vcom)")
    elif new.category_id == CAT_MODULE:
        print(f"  {C.BOLD}Custom fields MODULE :{C.END}")
        print(f"    • Modèle (custom field)")
    elif new.category_id == CAT_SIM:
        print(f"  {C.BOLD}Custom fields SIM :{C.END}")
        print(f"    • Opérateur (custom field)")
        print(f"    • N° carte SIM (custom field)")

def main():
    """Point d'entrée du test"""
    SITE_KEY = "E3K2L"
    
    print_header(f"TEST SUPABASE → YUMAN pour le site {SITE_KEY}")
    
    # ══════════════════════════════════════════════════════════════════
    # INITIALISATION
    # ══════════════════════════════════════════════════════════════════
    print_section("INITIALISATION")
    print("Connexion à Supabase...")
    sb = SupabaseAdapter()
    print(f"{C.GREEN}✓ Supabase connecté{C.END}")
    
    print("Connexion à Yuman...")
    y = YumanAdapter(sb)
    print(f"{C.GREEN}✓ Yuman connecté{C.END}")
    
    # ══════════════════════════════════════════════════════════════════
    # ÉTAPE 1 : ÉTAT DB (source de vérité)
    # ══════════════════════════════════════════════════════════════════
    print_header(f"ÉTAPE 1 : ÉTAT DB (source de vérité) pour {SITE_KEY}")
    
    print("Récupération des données DB...")
    db_sites = sb.fetch_sites_y()  # fetch avec yuman_site_id comme clé
    db_equips = sb.fetch_equipments_y()  # fetch avec serial_number comme clé
    
    # Filtrer pour E3K2L uniquement
    db_sites = {k: s for k, s in db_sites.items() if s.get_vcom_system_key(sb) == SITE_KEY}
    db_equips = {k: e for k, e in db_equips.items() if e.get_vcom_system_key(sb) == SITE_KEY}
    
    print(f"\n{C.BOLD}Résultats DB :{C.END}")
    print(f"  • Sites:       {len(db_sites)}")
    print(f"  • Équipements: {len(db_equips)}")
    
    if db_sites:
        print(f"\n{C.BOLD}Détail du site DB :{C.END}")
        for site in db_sites.values():
            print_site_detail(site, sb, prefix="  ")
    
    if db_equips:
        print(f"\n{C.BOLD}Équipements DB par catégorie :{C.END}")
        groups = group_by_category(db_equips)
        for cat_id, items in sorted(groups.items()):
            cat_names = {
                CAT_MODULE: "MODULE",
                CAT_INVERTER: "INVERTER",
                CAT_STRING: "STRING",
                CAT_SIM: "SIM",
                CAT_CENTRALE: "CENTRALE"
            }
            print(f"\n  [{cat_names.get(cat_id, 'UNKNOWN')}] : {len(items)} équipement(s)")
    
    # ══════════════════════════════════════════════════════════════════
    # ÉTAPE 2 : ÉTAT YUMAN (état actuel)
    # ══════════════════════════════════════════════════════════════════
    print_header(f"ÉTAPE 2 : ÉTAT YUMAN (état actuel) pour {SITE_KEY}")
    
    print("Fetch Yuman sites...")
    y_sites_all = y.fetch_sites()
    print("Fetch Yuman équipements...")
    y_equips_all = y.fetch_equips()
    
    # Filtrer pour E3K2L
    y_sites = {k: s for k, s in y_sites_all.items() if s.get_vcom_system_key(sb) == SITE_KEY}
    y_equips = {k: e for k, e in y_equips_all.items() if e.get_vcom_system_key(sb) == SITE_KEY}
    
    print(f"\n{C.BOLD}Résultats Yuman :{C.END}")
    print(f"  • Sites:       {len(y_sites)}")
    print(f"  • Équipements: {len(y_equips)}")
    
    if y_sites:
        print(f"\n{C.BOLD}Détail du site Yuman :{C.END}")
        for site in y_sites.values():
            print_site_detail(site, sb, prefix="  ")
    
    if y_equips:
        print(f"\n{C.BOLD}Équipements Yuman par catégorie :{C.END}")
        groups = group_by_category(y_equips)
        for cat_id, items in sorted(groups.items()):
            cat_names = {
                CAT_MODULE: "MODULE",
                CAT_INVERTER: "INVERTER",
                CAT_STRING: "STRING",
                CAT_SIM: "SIM",
                CAT_CENTRALE: "CENTRALE"
            }
            print(f"\n  [{cat_names.get(cat_id, 'UNKNOWN')}] : {len(items)} équipement(s)")
    
    # ══════════════════════════════════════════════════════════════════
    # ÉTAPE 3 : DIFF SITES
    # ══════════════════════════════════════════════════════════════════
    print_header("ÉTAPE 3 : DIFF SITES (DB → Yuman)")
    
    print("Calcul des différences pour les sites...")
    patch_sites = diff_entities(
        y_sites, db_sites,
        ignore_fields={"client_map_id", "id", "ignore_site"}
    )
    
    print_patch_summary(patch_sites, "Sites")
    
    # Détail des changements
    if patch_sites.add:
        print(f"\n{C.GREEN}Sites à CRÉER dans Yuman :{C.END}")
        for site in patch_sites.add:
            print_site_detail(site, sb, prefix="  ")
    
    if patch_sites.update:
        print(f"\n{C.YELLOW}Sites à METTRE À JOUR dans Yuman :{C.END}")
        for old, new in patch_sites.update:
            print_site_diff(old, new, sb)
    
    if patch_sites.delete:
        print(f"\n{C.RED}Sites à SUPPRIMER dans Yuman :{C.END}")
        for site in patch_sites.delete:
            print_site_detail(site, sb, prefix="  ")
    
    # ══════════════════════════════════════════════════════════════════
    # ÉTAPE 4 : DIFF ÉQUIPEMENTS
    # ══════════════════════════════════════════════════════════════════
    print_header("ÉTAPE 4 : DIFF ÉQUIPEMENTS (DB → Yuman)")
    
    print("Calcul des différences pour les équipements...")
    
    # Préparer le mapping parent_id pour les STRING
    id_by_vcom = {
        e.vcom_device_id: e.yuman_material_id
        for e in y_equips.values()
        if e.yuman_material_id
    }
    set_parent_map(id_by_vcom)
    
    patch_equips = diff_entities(
        y_equips, db_equips,
        ignore_fields={"vcom_system_key", "parent_id"}
    )
    
    print_patch_summary(patch_equips, "Équipements")
    
    # Détail des changements par catégorie
    if patch_equips.add:
        print(f"\n{C.GREEN}Équipements à CRÉER dans Yuman :{C.END}")
        groups = group_by_category({e.key(): e for e in patch_equips.add})
        for cat_id, items in sorted(groups.items()):
            cat_names = {
                CAT_MODULE: "MODULE",
                CAT_INVERTER: "INVERTER",
                CAT_STRING: "STRING",
                CAT_SIM: "SIM",
                CAT_CENTRALE: "CENTRALE"
            }
            print(f"\n  [{cat_names.get(cat_id)}] : {len(items)} équipement(s)")
            for eq in items[:3]:  # Limiter à 3 par catégorie
                print_equipment_detail(eq, sb, prefix="    ")
    
    if patch_equips.update:
        print(f"\n{C.YELLOW}Équipements à METTRE À JOUR dans Yuman :{C.END}")
        groups = defaultdict(list)
        for old, new in patch_equips.update:
            groups[new.category_id].append((old, new))
        
        for cat_id, items in sorted(groups.items()):
            cat_names = {
                CAT_MODULE: "MODULE",
                CAT_INVERTER: "INVERTER",
                CAT_STRING: "STRING",
                CAT_SIM: "SIM",
                CAT_CENTRALE: "CENTRALE"
            }
            print(f"\n  [{cat_names.get(cat_id)}] : {len(items)} équipement(s)")
            for old, new in items[:3]:  # Limiter à 3 par catégorie
                print_equipment_diff(old, new, sb)
    
    if patch_equips.delete:
        print(f"\n{C.RED}Équipements à SUPPRIMER dans Yuman :{C.END}")
        groups = group_by_category({e.key(): e for e in patch_equips.delete})
        for cat_id, items in sorted(groups.items()):
            cat_names = {
                CAT_MODULE: "MODULE",
                CAT_INVERTER: "INVERTER",
                CAT_STRING: "STRING",
                CAT_SIM: "SIM",
                CAT_CENTRALE: "CENTRALE"
            }
            print(f"\n  [{cat_names.get(cat_id)}] : {len(items)} équipement(s)")
    
    # ══════════════════════════════════════════════════════════════════
    # ÉTAPE 5 : RÉCAPITULATIF
    # ══════════════════════════════════════════════════════════════════
    print_header("RÉCAPITULATIF DES CHANGEMENTS")
    
    print(f"\n{C.BOLD}Sites :{C.END}")
    print(f"  • Créations:     {len(patch_sites.add)}")
    print(f"  • Mises à jour:  {len(patch_sites.update)}")
    print(f"  • Suppressions:  {len(patch_sites.delete)}")
    
    print(f"\n{C.BOLD}Équipements :{C.END}")
    print(f"  • Créations:     {len(patch_equips.add)}")
    print(f"  • Mises à jour:  {len(patch_equips.update)}")
    print(f"  • Suppressions:  {len(patch_equips.delete)}")
    
    # Détail par catégorie pour les updates
    if patch_equips.update:
        print(f"\n{C.BOLD}Mises à jour par catégorie :{C.END}")
        groups = defaultdict(int)
        for old, new in patch_equips.update:
            groups[new.category_id] += 1
        
        for cat_id, count in sorted(groups.items()):
            cat_names = {
                CAT_MODULE: "MODULE",
                CAT_INVERTER: "INVERTER",
                CAT_STRING: "STRING",
                CAT_SIM: "SIM",
                CAT_CENTRALE: "CENTRALE"
            }
            print(f"  • {cat_names.get(cat_id, 'UNKNOWN'):15} : {count}")
    
    # ══════════════════════════════════════════════════════════════════
    # ÉTAPE 5B : DIAGNOSTIC APPROFONDI
    # ══════════════════════════════════════════════════════════════════
    print_header("DIAGNOSTIC APPROFONDI DES ÉQUIPEMENTS PROBLÉMATIQUES")
    
    # 1. MODULE à CRÉER avec yuman_material_id déjà renseigné
    if patch_equips.add:
        print(f"\n{C.YELLOW}⚠️  ANALYSE DES CRÉATIONS{C.END}")
        for eq in patch_equips.add:
            if eq.yuman_material_id:
                print(f"\n{C.RED}PROBLÈME : Équipement à créer mais yuman_material_id déjà renseigné !{C.END}")
                print(f"\n{C.BOLD}Détails DB :{C.END}")
                print(f"  • serial_number:      {eq.serial_number}")
                print(f"  • vcom_device_id:     {eq.vcom_device_id}")
                print(f"  • yuman_material_id:  {eq.yuman_material_id}")
                print(f"  • category_id:        {eq.category_id}")
                print(f"  • site_id:            {eq.site_id}")
                
                # Vérifier si ce yuman_material_id existe dans le snapshot Yuman
                print(f"\n{C.BOLD}Recherche dans le snapshot Yuman :{C.END}")
                found_in_yuman = False
                for y_eq in y_equips_all.values():
                    if y_eq.yuman_material_id == eq.yuman_material_id:
                        found_in_yuman = True
                        print(f"  {C.GREEN}✓ Trouvé dans Yuman snapshot (serial: {y_eq.serial_number}){C.END}")
                        print(f"    • vcom_system_key (Yuman): {y_eq.get_vcom_system_key(sb)}")
                        print(f"    • site_id (Yuman):         {y_eq.site_id}")
                        break
                
                if not found_in_yuman:
                    print(f"  {C.RED}✗ NON trouvé dans Yuman snapshot{C.END}")
                    print(f"  {C.YELLOW}→ Appel direct API Yuman...{C.END}")
                    try:
                        direct_fetch = y.yc.get_material(eq.yuman_material_id, embed="fields,site")
                        print(f"  {C.GREEN}✓ Existe bien dans Yuman API{C.END}")
                        print(f"    • name:    {direct_fetch.get('name')}")
                        print(f"    • serial:  {direct_fetch.get('serial_number')}")
                        print(f"    • site_id: {direct_fetch.get('site_id')}")
                        print(f"    • category: {direct_fetch.get('category_id')}")
                    except Exception as e:
                        print(f"  {C.RED}✗ N'existe PAS dans Yuman API : {e}{C.END}")
    
    # 2. MODULE à SUPPRIMER
    if patch_equips.delete:
        print(f"\n{C.YELLOW}⚠️  ANALYSE DES SUPPRESSIONS{C.END}")
        for eq in patch_equips.delete:
            print(f"\n{C.BOLD}Équipement à SUPPRIMER :{C.END}")
            print(f"  • serial_number:      {eq.serial_number}")
            print(f"  • vcom_device_id:     {eq.vcom_device_id}")
            print(f"  • yuman_material_id:  {eq.yuman_material_id}")
            print(f"  • category_id:        {eq.category_id}")
            print(f"  • name:               {eq.name}")
            print(f"  • brand:              {eq.brand}")
            print(f"  • model:              {eq.model}")
            print(f"  • site_id:            {eq.site_id}")
            
            # Vérifier s'il existe en DB
            print(f"\n{C.BOLD}Recherche dans la DB :{C.END}")
            found_in_db = False
            for db_eq in db_equips.values():
                if db_eq.serial_number == eq.serial_number:
                    found_in_db = True
                    print(f"  {C.GREEN}✓ Trouvé dans DB{C.END}")
                    break
            
            if not found_in_db:
                print(f"  {C.RED}✗ NON trouvé dans DB (normal si c'est un équipement Yuman-only){C.END}")
    
    # 3. STRING avec custom fields None
    if patch_equips.update:
        print(f"\n{C.YELLOW}⚠️  ANALYSE DES CUSTOM FIELDS{C.END}")
        for old, new in patch_equips.update:
            if new.category_id == CAT_STRING:
                print(f"\n{C.BOLD}STRING : {new.name}{C.END}")
                print(f"  • serial_number: {new.serial_number}")
                print(f"  • yuman_material_id: {new.yuman_material_id}")
                
                # Afficher les custom fields BRUTS de Yuman
                print(f"\n{C.BOLD}Custom fields BRUTS (depuis Yuman API) :{C.END}")
                if new.yuman_material_id:
                    try:
                        direct_fetch = y.yc.get_material(new.yuman_material_id, embed="fields")
                        raw_fields = direct_fetch.get("_embed", {}).get("fields", [])
                        
                        if raw_fields:
                            for f in raw_fields:
                                print(f"    • {f['name']:30} (bp:{f.get('blueprint_id'):5}) = {f.get('value')}")
                        else:
                            print(f"  {C.RED}✗ Aucun custom field trouvé dans Yuman{C.END}")
                    except Exception as e:
                        print(f"  {C.RED}✗ Erreur lors de la récupération : {e}{C.END}")
                
                # Comparer avec les valeurs DB
                print(f"\n{C.BOLD}Valeurs DB (ce qu'on veut pousser) :{C.END}")
                print(f"  • count:        {new.count}")
                print(f"  • brand:        {new.brand}")
                print(f"  • model:        {new.model}")
                print(f"  • parent_id:    {new.parent_id}")
                
                # Comparer avec les valeurs actuelles Yuman (via l'objet old)
                print(f"\n{C.BOLD}Valeurs actuelles Yuman (via snapshot) :{C.END}")
                print(f"  • count:        {old.count}")
                print(f"  • brand:        {old.brand}")
                print(f"  • model:        {old.model}")
                print(f"  • parent_id:    {old.parent_id}")
                print(f"  • mppt_idx:     {getattr(old, 'mppt_idx', 'N/A')}")
                print(f"  • nb_modules:   {getattr(old, 'nb_modules', 'N/A')}")
                print(f"  • module_brand: {getattr(old, 'module_brand', 'N/A')}")
                print(f"  • module_model: {getattr(old, 'module_model', 'N/A')}")
    
    # ══════════════════════════════════════════════════════════════════
    # ÉTAPE 6 : CONFIRMATION
    # ══════════════════════════════════════════════════════════════════
    print_header("CONFIRMATION")
    
    total_changes = (
        len(patch_sites.add) + len(patch_sites.update) + len(patch_sites.delete) +
        len(patch_equips.add) + len(patch_equips.update) + len(patch_equips.delete)
    )
    
    if total_changes == 0:
        print(f"{C.GREEN}✓ Aucun changement à appliquer. DB et Yuman sont déjà synchronisés.{C.END}")
        return
    
    print(f"{C.YELLOW}⚠️  Cette étape va MODIFIER Yuman API{C.END}")
    print(f"\nTotal de changements à appliquer : {total_changes}")
    print(f"  • Sites:       {len(patch_sites.add) + len(patch_sites.update) + len(patch_sites.delete)}")
    print(f"  • Équipements: {len(patch_equips.add) + len(patch_equips.update) + len(patch_equips.delete)}")
    
    response = input(f"\n{C.BOLD}Confirmer l'application sur Yuman ? (oui/non) :{C.END} ")
    
    if response.lower() != "oui":
        print(f"{C.RED}✗ Application annulée{C.END}")
        return
    
    # ══════════════════════════════════════════════════════════════════
    # ÉTAPE 7 : APPLICATION
    # ══════════════════════════════════════════════════════════════════
    print_header("APPLICATION DES PATCHES")
    
    print(f"\n{C.GREEN}Application des patches sites...{C.END}")
    try:
        y.apply_sites_patch(
            db_sites=db_sites,
            y_sites=y_sites,
            patch=patch_sites,
        )
        print(f"  {C.GREEN}✓ Sites mis à jour{C.END}")
    except Exception as e:
        print(f"  {C.RED}✗ Erreur lors de la mise à jour des sites : {e}{C.END}")
        import traceback
        traceback.print_exc()
        return
    
    print(f"\n{C.GREEN}Application des patches équipements...{C.END}")
    try:
        y.apply_equips_patch(
            db_equips=db_equips,
            y_equips=y_equips,
            patch=patch_equips,
        )
        print(f"  {C.GREEN}✓ Équipements mis à jour{C.END}")
    except Exception as e:
        print(f"  {C.RED}✗ Erreur lors de la mise à jour des équipements : {e}{C.END}")
        import traceback
        traceback.print_exc()
        return
    
    # ══════════════════════════════════════════════════════════════════
    # ÉTAPE 8 : VÉRIFICATION FINALE
    # ══════════════════════════════════════════════════════════════════
    print_header("VÉRIFICATION FINALE")
    
    print("Refetch Yuman après application...")
    y_sites_after = y.fetch_sites()
    y_equips_after = y.fetch_equips()
    
    # Filtrer pour E3K2L
    y_sites_after = {k: s for k, s in y_sites_after.items() if s.get_vcom_system_key(sb) == SITE_KEY}
    y_equips_after = {k: e for k, e in y_equips_after.items() if e.get_vcom_system_key(sb) == SITE_KEY}
    
    print(f"\n{C.BOLD}État final Yuman :{C.END}")
    print(f"  • Sites:       {len(y_sites_after)}")
    print(f"  • Équipements: {len(y_equips_after)}")
    
    # Nouveau diff pour vérifier
    print("\nNouvel diff DB vs Yuman...")
    patch_sites_final = diff_entities(
        y_sites_after, db_sites,
        ignore_fields={"client_map_id", "id", "ignore_site"}
    )
    patch_equips_final = diff_entities(
        y_equips_after, db_equips,
        ignore_fields={"vcom_system_key", "parent_id"}
    )
    
    total_remaining = (
        len(patch_sites_final.add) + len(patch_sites_final.update) + len(patch_sites_final.delete) +
        len(patch_equips_final.add) + len(patch_equips_final.update) + len(patch_equips_final.delete)
    )
    
    if total_remaining == 0:
        print(f"\n{C.GREEN}✓✓✓ SUCCÈS : DB et Yuman sont maintenant parfaitement synchronisés !{C.END}")
    else:
        print(f"\n{C.YELLOW}⚠️  Il reste {total_remaining} différence(s) après synchronisation{C.END}")
        print_patch_summary(patch_sites_final, "Sites (final)")
        print_patch_summary(patch_equips_final, "Équipements (final)")
    
    print_header("✅ TEST TERMINÉ")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Interruption utilisateur")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ ERREUR : {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)