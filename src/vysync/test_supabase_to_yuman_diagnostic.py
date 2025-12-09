#!/usr/bin/env python3
"""
test_supabase_to_yuman_diagnostic.py
=====================================

Script de diagnostic pour tester le flux Supabase → Yuman en 4 étapes :
1. Lecture des données Supabase (source de vérité après fusion)
2. Lecture de l'état actuel Yuman
3. Calcul du diff (sites + équipements)
4. Affichage détaillé des changements prévus

⚠️ CE SCRIPT N'APPLIQUE RIEN - il est purement diagnostique.

Usage:
    poetry run python -m vysync.test_supabase_to_yuman_diagnostic
    poetry run python -m vysync.test_supabase_to_yuman_diagnostic --site-key 2KC5K
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections import defaultdict
from dataclasses import asdict
from datetime import datetime
from typing import Any, Dict, List

# Configuration logging AVANT imports vysync
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s | %(message)s'
)
logger = logging.getLogger(__name__)

# Imports vysync
from vysync.adapters.supabase_adapter import SupabaseAdapter
from vysync.adapters.yuman_adapter import YumanAdapter
from vysync.diff import diff_entities, set_parent_map
from vysync.models import (
    Site, Equipment,
    CAT_MODULE, CAT_INVERTER, CAT_STRING, CAT_SIM, CAT_CENTRALE
)


# ═══════════════════════════════════════════════════════════════════════════════
# COULEURS CONSOLE
# ═══════════════════════════════════════════════════════════════════════════════

class C:
    """Codes ANSI pour colorer la sortie console."""
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    END = '\033[0m'


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS D'AFFICHAGE
# ═══════════════════════════════════════════════════════════════════════════════

CAT_NAMES = {
    CAT_MODULE: "MODULE",
    CAT_INVERTER: "INVERTER",
    CAT_STRING: "STRING",
    CAT_SIM: "SIM",
    CAT_CENTRALE: "CENTRALE",
}


def print_header(title: str) -> None:
    """Affiche un header de section."""
    print(f"\n{'═' * 70}")
    print(f"{C.BOLD}{C.HEADER}{title}{C.END}")
    print(f"{'═' * 70}")


def print_section(title: str) -> None:
    """Affiche un sous-titre de section."""
    print(f"\n{C.BOLD}── {title} ──{C.END}")


def print_stats(label: str, count: int, color: str = C.BLUE) -> None:
    """Affiche une statistique."""
    print(f"  {color}•{C.END} {label}: {C.BOLD}{count}{C.END}")


def print_patch_summary(patch, label: str) -> None:
    """Affiche le résumé d'un patch."""
    print(f"\n{C.YELLOW}Patch {label}:{C.END}")
    print(f"  {C.GREEN}+ ADD:    {len(patch.add)}{C.END}")
    print(f"  {C.YELLOW}~ UPDATE: {len(patch.update)}{C.END}")
    print(f"  {C.RED}- DELETE: {len(patch.delete)}{C.END}")


def format_value(val: Any) -> str:
    """Formate une valeur pour l'affichage."""
    if val is None:
        return f"{C.RED}None{C.END}"
    if isinstance(val, str) and val.strip() == "":
        return f"{C.RED}''{C.END}"
    return str(val)


# ═══════════════════════════════════════════════════════════════════════════════
# FONCTIONS D'AFFICHAGE DÉTAILLÉ
# ═══════════════════════════════════════════════════════════════════════════════

def print_site_detail(site: Site, sb: SupabaseAdapter, prefix: str = "  ") -> None:
    """Affiche les détails d'un site."""
    print(f"{prefix}{C.BOLD}{site.name}{C.END}")
    print(f"{prefix}  • id:              {site.id}")
    print(f"{prefix}  • vcom_system_key: {site.vcom_system_key}")
    print(f"{prefix}  • yuman_site_id:   {site.yuman_site_id}")
    print(f"{prefix}  • address:         {site.address}")
    print(f"{prefix}  • latitude:        {site.latitude}")
    print(f"{prefix}  • longitude:       {site.longitude}")
    print(f"{prefix}  • nominal_power:   {site.nominal_power}")
    print(f"{prefix}  • commission_date: {site.commission_date}")
    print(f"{prefix}  • client_map_id:   {site.client_map_id}")


def print_site_diff(old: Site, new: Site) -> None:
    """Affiche les différences entre deux sites."""
    print(f"\n  {C.YELLOW}Site: {new.name}{C.END}")
    print(f"    vcom_system_key: {new.vcom_system_key}")
    print(f"    yuman_site_id:   {new.yuman_site_id}")
    
    fields = ['name', 'address', 'latitude', 'longitude', 'nominal_power', 'commission_date']
    changes = []
    
    for field in fields:
        old_val = getattr(old, field, None)
        new_val = getattr(new, field, None)
        if old_val != new_val:
            changes.append((field, old_val, new_val))
    
    if changes:
        print(f"    {C.BOLD}Changements:{C.END}")
        for field, old_val, new_val in changes:
            print(f"      • {field:18}: {C.RED}{format_value(old_val)}{C.END} → {C.GREEN}{format_value(new_val)}{C.END}")
    else:
        print(f"    {C.YELLOW}(aucun changement de champ standard détecté){C.END}")


def print_equipment_detail(eq: Equipment, prefix: str = "  ") -> None:
    """Affiche les détails d'un équipement."""
    cat_name = CAT_NAMES.get(eq.category_id, "UNKNOWN")
    print(f"{prefix}{C.BOLD}[{cat_name}] {eq.name}{C.END}")
    print(f"{prefix}  • serial_number:     {eq.serial_number}")
    print(f"{prefix}  • vcom_device_id:    {eq.vcom_device_id}")
    print(f"{prefix}  • yuman_material_id: {eq.yuman_material_id}")
    print(f"{prefix}  • brand:             {eq.brand}")
    print(f"{prefix}  • model:             {eq.model}")
    print(f"{prefix}  • count:             {eq.count}")
    print(f"{prefix}  • site_id:           {eq.site_id}")


def print_equipment_diff(old: Equipment, new: Equipment) -> None:
    """Affiche les différences entre deux équipements."""
    cat_name = CAT_NAMES.get(new.category_id, "UNKNOWN")
    print(f"\n  {C.YELLOW}[{cat_name}] {new.name}{C.END}")
    print(f"    serial_number:     {new.serial_number}")
    print(f"    yuman_material_id: {new.yuman_material_id}")
    
    fields = ['name', 'brand', 'model', 'count', 'serial_number']
    changes = []
    
    for field in fields:
        old_val = getattr(old, field, None)
        new_val = getattr(new, field, None)
        if old_val != new_val:
            changes.append((field, old_val, new_val))
    
    if changes:
        print(f"    {C.BOLD}Changements:{C.END}")
        for field, old_val, new_val in changes:
            print(f"      • {field:18}: {C.RED}{format_value(old_val)}{C.END} → {C.GREEN}{format_value(new_val)}{C.END}")


def group_by_category(equips: List[Equipment]) -> Dict[int, List[Equipment]]:
    """Regroupe les équipements par catégorie."""
    groups = defaultdict(list)
    for eq in equips:
        groups[eq.category_id].append(eq)
    return dict(groups)


# ═══════════════════════════════════════════════════════════════════════════════
# DIAGNOSTIC PRINCIPAL
# ═══════════════════════════════════════════════════════════════════════════════

def run_diagnostic(site_key: str | None = None) -> Dict[str, Any]:
    """
    Exécute le diagnostic complet en 4 phases.
    
    Retourne un dict avec toutes les données collectées pour analyse.
    """
    report = {
        "timestamp": datetime.now().isoformat(),
        "site_key_filter": site_key,
        "phase1_supabase": {},
        "phase2_yuman": {},
        "phase3_diff": {},
        "phase4_details": {},
        "errors": [],
    }
    
    # ═══════════════════════════════════════════════════════════════════════════
    # INITIALISATION
    # ═══════════════════════════════════════════════════════════════════════════
    print_header("DIAGNOSTIC SUPABASE → YUMAN")
    print(f"Date: {report['timestamp']}")
    if site_key:
        print(f"Filtre site_key: {C.BOLD}{site_key}{C.END}")
    else:
        print(f"Filtre: {C.YELLOW}TOUS LES SITES{C.END}")
    
    print("\nInitialisation des adaptateurs...")
    try:
        sb = SupabaseAdapter()
        y = YumanAdapter(sb)
        print(f"  {C.GREEN}✓ SupabaseAdapter initialisé{C.END}")
        print(f"  {C.GREEN}✓ YumanAdapter initialisé{C.END}")
    except Exception as e:
        print(f"  {C.RED}✗ Erreur initialisation: {e}{C.END}")
        report["errors"].append({"phase": "init", "error": str(e)})
        return report
    
    # ═══════════════════════════════════════════════════════════════════════════
    # PHASE 1 : LECTURE SUPABASE
    # ═══════════════════════════════════════════════════════════════════════════
    print_header("PHASE 1 : LECTURE SUPABASE (source de vérité)")
    
    # Variable pour stocker les yuman_site_ids à filtrer (pour Phase 2)
    target_yuman_site_ids: set = set()
    target_supabase_site_ids: set = set()
    
    try:
        print("Chargement des sites depuis Supabase...")
        sb_sites_all = sb.fetch_sites_y()  # Indexé par yuman_site_id
        
        # Filtrer les sites ignorés
        sb_sites = {
            k: s for k, s in sb_sites_all.items()
            if not getattr(s, "ignore_site", False)
        }
        
        # Filtrer par site_key si spécifié
        if site_key:
            sb_sites = {k: s for k, s in sb_sites.items() if s.vcom_system_key == site_key}
        
        # Collecter les yuman_site_id et site_id pour filtrer Yuman ensuite
        for yuman_site_id, site in sb_sites.items():
            target_yuman_site_ids.add(yuman_site_id)
            if site.id:
                target_supabase_site_ids.add(site.id)
        
        print(f"  {C.GREEN}✓ {len(sb_sites)} sites chargés{C.END}")
        if len(sb_sites_all) != len(sb_sites):
            ignored_count = len(sb_sites_all) - len(sb_sites)
            print(f"    (ignorés/filtrés: {ignored_count})")
        
        # Afficher les yuman_site_ids qu'on va chercher
        if site_key and target_yuman_site_ids:
            print(f"    yuman_site_ids à synchroniser: {target_yuman_site_ids}")
        
        print("\nChargement des équipements depuis Supabase...")
        sb_equips_all = sb.fetch_equipments_y()
        
        # Filtrer par site_id Supabase
        if site_key:
            sb_equips = {k: e for k, e in sb_equips_all.items() if e.site_id in target_supabase_site_ids}
        else:
            sb_equips = sb_equips_all
        
        print(f"  {C.GREEN}✓ {len(sb_equips)} équipements chargés{C.END}")
        
        # Stats par catégorie
        print_section("Répartition équipements Supabase")
        by_cat_sb = defaultdict(int)
        for eq in sb_equips.values():
            by_cat_sb[eq.category_id] += 1
        for cat_id, count in sorted(by_cat_sb.items()):
            print_stats(CAT_NAMES.get(cat_id, f"CAT_{cat_id}"), count)
        
        report["phase1_supabase"] = {
            "sites_count": len(sb_sites),
            "equips_count": len(sb_equips),
            "equips_by_category": dict(by_cat_sb),
            "yuman_site_ids": list(target_yuman_site_ids),
        }
        
    except Exception as e:
        print(f"  {C.RED}✗ Erreur Phase 1: {e}{C.END}")
        report["errors"].append({"phase": "phase1_supabase", "error": str(e)})
        import traceback
        traceback.print_exc()
        return report
    
    # ═══════════════════════════════════════════════════════════════════════════
    # PHASE 2 : LECTURE YUMAN
    # ═══════════════════════════════════════════════════════════════════════════
    print_header("PHASE 2 : LECTURE YUMAN (état actuel)")
    
    try:
        print("Chargement des sites depuis Yuman...")
        y_sites_all = y.fetch_sites()  # Indexé par yuman_site_id
        
        # Filtrer par yuman_site_id (PAS par vcom_system_key car il peut être vide côté Yuman)
        if site_key:
            # On utilise les yuman_site_ids récupérés depuis Supabase
            y_sites = {k: s for k, s in y_sites_all.items() if k in target_yuman_site_ids}
            print(f"    Filtrage par yuman_site_id: {target_yuman_site_ids}")
        else:
            y_sites = y_sites_all
        
        print(f"  {C.GREEN}✓ {len(y_sites)} sites chargés{C.END}")
        
        # Afficher l'état du vcom_system_key côté Yuman
        for yid, site in y_sites.items():
            vcom_key_yuman = site.vcom_system_key
            if vcom_key_yuman:
                print(f"    Site {yid}: vcom_system_key = {C.GREEN}{vcom_key_yuman}{C.END}")
            else:
                print(f"    Site {yid}: vcom_system_key = {C.RED}NON RENSEIGNÉ{C.END} (à synchroniser)")
        
        print("\nChargement des équipements depuis Yuman...")
        y_equips_all = y.fetch_equips()
        
        # Filtrer par site_id Supabase (les équipements Yuman ont le site_id Supabase via le mapping)
        if site_key:
            y_equips = {k: e for k, e in y_equips_all.items() if e.site_id in target_supabase_site_ids}
        else:
            y_equips = y_equips_all
        
        print(f"  {C.GREEN}✓ {len(y_equips)} équipements chargés{C.END}")
        
        # Stats par catégorie
        print_section("Répartition équipements Yuman")
        by_cat_y = defaultdict(int)
        for eq in y_equips.values():
            by_cat_y[eq.category_id] += 1
        for cat_id, count in sorted(by_cat_y.items()):
            print_stats(CAT_NAMES.get(cat_id, f"CAT_{cat_id}"), count)
        
        report["phase2_yuman"] = {
            "sites_count": len(y_sites),
            "equips_count": len(y_equips),
            "equips_by_category": dict(by_cat_y),
        }
        
    except Exception as e:
        print(f"  {C.RED}✗ Erreur Phase 2: {e}{C.END}")
        report["errors"].append({"phase": "phase2_yuman", "error": str(e)})
        import traceback
        traceback.print_exc()
        return report
    
    # ═══════════════════════════════════════════════════════════════════════════
    # PHASE 3 : CALCUL DU DIFF
    # ═══════════════════════════════════════════════════════════════════════════
    print_header("PHASE 3 : CALCUL DU DIFF")
    
    try:
        # DIFF SITES
        print("Calcul diff sites (Yuman vs Supabase)...")
        patch_sites = diff_entities(
            y_sites,      # current (Yuman)
            sb_sites,     # target (Supabase = vérité)
            ignore_fields={"client_map_id", "id", "ignore_site"}
        )
        print_patch_summary(patch_sites, "Sites")
        
        # DIFF EQUIPMENTS
        print("\nPréparation mapping parent_id pour STRING...")
        id_by_vcom = {
            e.vcom_device_id: e.yuman_material_id
            for e in y_equips.values()
            if e.yuman_material_id
        }
        set_parent_map(id_by_vcom)
        print(f"  {C.GREEN}✓ {len(id_by_vcom)} mappings parent configurés{C.END}")
        
        print("\nCalcul diff équipements (Yuman vs Supabase)...")
        patch_equips = diff_entities(
            y_equips,     # current (Yuman)
            sb_equips,    # target (Supabase = vérité)
            ignore_fields={"vcom_system_key", "parent_id"}
        )
        print_patch_summary(patch_equips, "Équipements")
        
        report["phase3_diff"] = {
            "sites": {
                "add": len(patch_sites.add),
                "update": len(patch_sites.update),
                "delete": len(patch_sites.delete),
            },
            "equips": {
                "add": len(patch_equips.add),
                "update": len(patch_equips.update),
                "delete": len(patch_equips.delete),
            },
        }
        
    except Exception as e:
        print(f"  {C.RED}✗ Erreur Phase 3: {e}{C.END}")
        report["errors"].append({"phase": "phase3_diff", "error": str(e)})
        import traceback
        traceback.print_exc()
        return report
    
    # ═══════════════════════════════════════════════════════════════════════════
    # PHASE 4 : AFFICHAGE DÉTAILLÉ
    # ═══════════════════════════════════════════════════════════════════════════
    print_header("PHASE 4 : DÉTAIL DES CHANGEMENTS")
    
    # ─── SITES ───
    print_section("SITES")
    
    if patch_sites.add:
        print(f"\n{C.GREEN}Sites à CRÉER dans Yuman ({len(patch_sites.add)}):{C.END}")
        for site in patch_sites.add[:5]:  # Max 5
            print_site_detail(site, sb)
        if len(patch_sites.add) > 5:
            print(f"  ... et {len(patch_sites.add) - 5} autres")
    
    if patch_sites.update:
        print(f"\n{C.YELLOW}Sites à METTRE À JOUR dans Yuman ({len(patch_sites.update)}):{C.END}")
        for old, new in patch_sites.update[:5]:  # Max 5
            print_site_diff(old, new)
        if len(patch_sites.update) > 5:
            print(f"  ... et {len(patch_sites.update) - 5} autres")
    
    if patch_sites.delete:
        print(f"\n{C.RED}Sites à SUPPRIMER de Yuman ({len(patch_sites.delete)}):{C.END}")
        for site in patch_sites.delete[:5]:  # Max 5
            print(f"  • {site.name} (yuman_site_id={site.yuman_site_id})")
        if len(patch_sites.delete) > 5:
            print(f"  ... et {len(patch_sites.delete) - 5} autres")
    
    if patch_sites.is_empty():
        print(f"\n{C.GREEN}✓ Sites déjà synchronisés - aucun changement{C.END}")
    
    # ─── ÉQUIPEMENTS ───
    print_section("ÉQUIPEMENTS")
    
    if patch_equips.add:
        print(f"\n{C.GREEN}Équipements à CRÉER dans Yuman ({len(patch_equips.add)}):{C.END}")
        groups_add = group_by_category(patch_equips.add)
        for cat_id, items in sorted(groups_add.items()):
            print(f"\n  {C.BOLD}[{CAT_NAMES.get(cat_id, 'UNKNOWN')}] : {len(items)} équipement(s){C.END}")
            for eq in items[:2]:  # Max 2 par catégorie
                print_equipment_detail(eq, prefix="    ")
            if len(items) > 2:
                print(f"    ... et {len(items) - 2} autres")
    
    if patch_equips.update:
        print(f"\n{C.YELLOW}Équipements à METTRE À JOUR dans Yuman ({len(patch_equips.update)}):{C.END}")
        groups_upd = defaultdict(list)
        for old, new in patch_equips.update:
            groups_upd[new.category_id].append((old, new))
        
        for cat_id, items in sorted(groups_upd.items()):
            print(f"\n  {C.BOLD}[{CAT_NAMES.get(cat_id, 'UNKNOWN')}] : {len(items)} équipement(s){C.END}")
            for old, new in items[:2]:  # Max 2 par catégorie
                print_equipment_diff(old, new)
            if len(items) > 2:
                print(f"    ... et {len(items) - 2} autres")
    
    if patch_equips.delete:
        print(f"\n{C.RED}Équipements à SUPPRIMER de Yuman ({len(patch_equips.delete)}):{C.END}")
        groups_del = group_by_category(patch_equips.delete)
        for cat_id, items in sorted(groups_del.items()):
            print(f"\n  {C.BOLD}[{CAT_NAMES.get(cat_id, 'UNKNOWN')}] : {len(items)} équipement(s){C.END}")
            for eq in items[:2]:
                print(f"    • {eq.name} (serial={eq.serial_number})")
            if len(items) > 2:
                print(f"    ... et {len(items) - 2} autres")
    
    if patch_equips.is_empty():
        print(f"\n{C.GREEN}✓ Équipements déjà synchronisés - aucun changement{C.END}")
    
    # ═══════════════════════════════════════════════════════════════════════════
    # RÉSUMÉ FINAL
    # ═══════════════════════════════════════════════════════════════════════════
    print_header("RÉSUMÉ FINAL")
    
    total_changes = (
        len(patch_sites.add) + len(patch_sites.update) + len(patch_sites.delete) +
        len(patch_equips.add) + len(patch_equips.update) + len(patch_equips.delete)
    )
    
    print(f"\n{C.BOLD}Total des changements prévus : {total_changes}{C.END}")
    print(f"\n  Sites:")
    print(f"    {C.GREEN}+ Créations:    {len(patch_sites.add)}{C.END}")
    print(f"    {C.YELLOW}~ Mises à jour: {len(patch_sites.update)}{C.END}")
    print(f"    {C.RED}- Suppressions: {len(patch_sites.delete)}{C.END}")
    print(f"\n  Équipements:")
    print(f"    {C.GREEN}+ Créations:    {len(patch_equips.add)}{C.END}")
    print(f"    {C.YELLOW}~ Mises à jour: {len(patch_equips.update)}{C.END}")
    print(f"    {C.RED}- Suppressions: {len(patch_equips.delete)}{C.END}")
    
    if total_changes == 0:
        print(f"\n{C.GREEN}✓✓✓ AUCUN CHANGEMENT NÉCESSAIRE - Supabase et Yuman sont synchronisés !{C.END}")
    else:
        print(f"\n{C.YELLOW}⚠️  {total_changes} changement(s) seraient appliqués si on exécute la synchronisation.{C.END}")
        print(f"{C.YELLOW}    Ce script est DIAGNOSTIC UNIQUEMENT - rien n'a été modifié.{C.END}")
    
    report["phase4_details"] = {
        "total_changes": total_changes,
    }
    
    return report


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Diagnostic Supabase → Yuman (sans application)"
    )
    parser.add_argument(
        "--site-key",
        type=str,
        default=None,
        help="Filtrer sur un site spécifique (ex: E3K2L)"
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Exporter le rapport en JSON"
    )
    args = parser.parse_args()
    
    report = run_diagnostic(site_key=args.site_key)
    
    if args.json:
        filename = f"diagnostic_sb_to_yuman_{datetime.now():%Y%m%d_%H%M%S}.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False, default=str)
        print(f"\n{C.BLUE}Rapport JSON exporté: {filename}{C.END}")
    
    if report["errors"]:
        print(f"\n{C.RED}⚠️  Des erreurs ont été détectées:{C.END}")
        for err in report["errors"]:
            print(f"  • Phase {err['phase']}: {err['error']}")
        sys.exit(1)


if __name__ == "__main__":
    main()