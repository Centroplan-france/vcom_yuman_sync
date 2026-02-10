#!/usr/bin/env python3
"""
sync_supabase_to_yuman.py
==========================

Synchronise les données Supabase → Yuman (sites + équipements).

Ce script :
1. Lit l'état Supabase (source de vérité)
2. Lit l'état Yuman (état actuel)
3. Calcule le diff
4. Applique les changements dans Yuman

Usage:
    # Mode dry-run (diagnostic uniquement)
    poetry run python -m vysync.sync_supabase_to_yuman --dry-run

    # Site spécifique
    poetry run python -m vysync.sync_supabase_to_yuman --site-key 2KC5K

    # Tous les sites (avec confirmation)
    poetry run python -m vysync.sync_supabase_to_yuman

    # Tous les sites (sans confirmation - pour GitHub Actions)
    poetry run python -m vysync.sync_supabase_to_yuman --yes
"""

from __future__ import annotations
from dataclasses import replace

import argparse
import json
import logging
import os
import re
import sys
from collections import defaultdict
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

# Configuration logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Imports vysync
from vysync.adapters.supabase_adapter import SupabaseAdapter
from vysync.adapters.yuman_adapter import YumanAdapter
from vysync.diff import diff_entities, set_parent_map, PatchSet
from vysync.models import (
    Site, Equipment,
    CAT_MODULE, CAT_INVERTER, CAT_STRING, CAT_SIM, CAT_CENTRALE
)


# ═══════════════════════════════════════════════════════════════════════════════
# UTILITAIRES
# ═══════════════════════════════════════════════════════════════════════════════

def normalize_site_name(name: str) -> str:
    """Normalise un nom de site en enlevant le préfixe numérique, 'France' et le suffixe entre parenthèses."""
    if not name:
        return ""
    return re.sub(r'^\d+\s+|\s*\(.*?\)| France', '', name).strip()


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
    
    @classmethod
    def disable(cls):
        """Désactive les couleurs (pour CI/logs)."""
        cls.HEADER = cls.BLUE = cls.GREEN = cls.YELLOW = cls.RED = cls.BOLD = cls.END = ''


CAT_NAMES = {
    CAT_MODULE: "MODULE",
    CAT_INVERTER: "INVERTER",
    CAT_STRING: "STRING",
    CAT_SIM: "SIM",
    CAT_CENTRALE: "CENTRALE",
}


def print_header(title: str) -> None:
    print(f"\n{'═' * 70}")
    print(f"{C.BOLD}{C.HEADER}{title}{C.END}")
    print(f"{'═' * 70}")


def print_section(title: str) -> None:
    print(f"\n{C.BOLD}── {title} ──{C.END}")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ═══════════════════════════════════════════════════════════════════════════════
# FONCTION PRINCIPALE
# ═══════════════════════════════════════════════════════════════════════════════

def sync_supabase_to_yuman(
    site_key: Optional[str] = None,
    dry_run: bool = False,
    auto_confirm: bool = False,
) -> Dict[str, Any]:
    """
    Synchronise Supabase → Yuman.
    
    Args:
        site_key: Filtrer sur un site spécifique (optionnel)
        dry_run: Si True, ne fait que le diagnostic sans appliquer
        auto_confirm: Si True, ne demande pas de confirmation
    
    Returns:
        Rapport d'exécution
    """
    report = {
        "execution_date": _now_iso(),
        "site_key_filter": site_key,
        "dry_run": dry_run,
        "sites": {"before": 0, "add": 0, "update": 0, "delete": 0, "after": 0},
        "equipments": {"before": 0, "add": 0, "update": 0, "delete": 0, "after": 0},
        "success": True,
        "errors": [],
        # Detailed logs for JSON report (BUG 3 fix)
        "details": {
            "sites_created": [],
            "sites_updated": [],
            "sites_deleted": [],
            "equipments_created": [],
            "equipments_updated": [],
            "equipments_deleted": [],
            "ignored_sites": [],
        },
    }
    
    # ═══════════════════════════════════════════════════════════════════════════
    # INITIALISATION
    # ═══════════════════════════════════════════════════════════════════════════
    print_header("SYNC SUPABASE → YUMAN")
    print(f"Date: {report['execution_date']}")
    print(f"Mode: {C.YELLOW}DRY-RUN{C.END}" if dry_run else f"Mode: {C.GREEN}APPLICATION RÉELLE{C.END}")
    if site_key:
        print(f"Filtre: site_key = {C.BOLD}{site_key}{C.END}")
    else:
        print(f"Filtre: {C.YELLOW}TOUS LES SITES{C.END}")
    
    print("\nInitialisation...")
    try:
        sb = SupabaseAdapter()
        y = YumanAdapter(sb)
        logger.info("Adaptateurs initialisés")
    except Exception as e:
        logger.error("Erreur initialisation: %s", e, exc_info=True)
        report["errors"].append({"phase": "init", "error": str(e)})
        report["success"] = False
        return report
    
    # ═══════════════════════════════════════════════════════════════════════════
    # PHASE 1 : LECTURE SUPABASE
    # ═══════════════════════════════════════════════════════════════════════════
    print_header("PHASE 1 : LECTURE SUPABASE")

    target_yuman_site_ids: set = set()
    target_supabase_site_ids: set = set()
    # BUG 1 & 2 FIX: Track ignored sites to exclude them from both sides
    ignored_yuman_site_ids: set = set()
    ignored_supabase_site_ids: set = set()

    try:
        # Sites - Load ALL sites first (including ignored ones)
        sb_sites_all = sb.fetch_sites_y()

        # BUG 1 FIX: Collect ignored site IDs BEFORE filtering
        for yuman_site_id, site in sb_sites_all.items():
            if getattr(site, "ignore_site", False):
                ignored_yuman_site_ids.add(yuman_site_id)
                if site.id:
                    ignored_supabase_site_ids.add(site.id)
                # Log ignored sites for report
                report["details"]["ignored_sites"].append({
                    "yuman_site_id": yuman_site_id,
                    "site_id": site.id,
                    "name": site.name,
                    "vcom_system_key": site.vcom_system_key,
                })

        if ignored_yuman_site_ids:
            logger.info("Sites ignorés (ignore_site=true): %d", len(ignored_yuman_site_ids))
            print(f"  {C.YELLOW}⚠ {len(ignored_yuman_site_ids)} sites ignorés (ignore_site=true){C.END}")

        # Now filter out ignored sites for the diff
        sb_sites = {
            k: s for k, s in sb_sites_all.items()
            if not getattr(s, "ignore_site", False)
        }

        if site_key:
            sb_sites = {k: s for k, s in sb_sites.items() if s.vcom_system_key == site_key}

        for yuman_site_id, site in sb_sites.items():
            target_yuman_site_ids.add(yuman_site_id)
            if site.id:
                target_supabase_site_ids.add(site.id)

        logger.info("Supabase: %d sites chargés", len(sb_sites))
        print(f"  {C.GREEN}✓ {len(sb_sites)} sites{C.END}")

        # Équipements - Load all, then filter
        sb_equips_all = sb.fetch_equipments_y()

        # BUG 2 FIX: Filter out equipments from ignored sites
        sb_equips = {
            k: e for k, e in sb_equips_all.items()
            if e.site_id not in ignored_supabase_site_ids
        }

        # Filtrer les équipements dont le site n'a pas de yuman_site_id
        # Ces équipements ne peuvent pas être créés dans Yuman
        sites_with_yuman_id = {s.id for s in sb_sites.values() if s.yuman_site_id}
        sb_equips = {
            k: e for k, e in sb_equips.items()
            if e.site_id in sites_with_yuman_id
        }

        if site_key:
            sb_equips = {k: e for k, e in sb_equips.items() if e.site_id in target_supabase_site_ids}

        logger.info("Supabase: %d équipements chargés", len(sb_equips))
        print(f"  {C.GREEN}✓ {len(sb_equips)} équipements{C.END}")

        report["sites"]["before"] = len(sb_sites)
        report["equipments"]["before"] = len(sb_equips)
        
    except Exception as e:
        logger.error("Erreur Phase 1: %s", e, exc_info=True)
        report["errors"].append({"phase": "phase1", "error": str(e)})
        report["success"] = False
        return report
    
    # ═══════════════════════════════════════════════════════════════════════════
    # PHASE 2 : LECTURE YUMAN
    # ═══════════════════════════════════════════════════════════════════════════
    print_header("PHASE 2 : LECTURE YUMAN")

    try:
        # Sites
        y_sites_all = y.fetch_sites()

        # BUG 1 FIX: Exclude ignored sites from Yuman side too
        # This prevents them from appearing in DELETE
        y_sites = {
            k: s for k, s in y_sites_all.items()
            if k not in ignored_yuman_site_ids
        }

        if site_key:
            y_sites = {k: s for k, s in y_sites.items() if k in target_yuman_site_ids}

        logger.info("Yuman: %d sites chargés", len(y_sites))
        print(f"  {C.GREEN}✓ {len(y_sites)} sites{C.END}")

        # Équipements
        y_equips_all = y.fetch_equips()

        # BUG 2 FIX: Exclude equipments from ignored sites on Yuman side too
        y_equips = {
            k: e for k, e in y_equips_all.items()
            if e.site_id not in ignored_supabase_site_ids
        }

        if site_key:
            y_equips = {k: e for k, e in y_equips.items() if e.site_id in target_supabase_site_ids}

        logger.info("Yuman: %d équipements chargés", len(y_equips))
        print(f"  {C.GREEN}✓ {len(y_equips)} équipements{C.END}")
        
    except Exception as e:
        logger.error("Erreur Phase 2: %s", e, exc_info=True)
        report["errors"].append({"phase": "phase2", "error": str(e)})
        report["success"] = False
        return report
    
    # ═══════════════════════════════════════════════════════════════════════════
    # PHASE 3 : CALCUL DU DIFF
    # ═══════════════════════════════════════════════════════════════════════════
    print_header("PHASE 3 : CALCUL DU DIFF")

    try:
        # Normaliser les noms des sites Supabase pour éviter les faux positifs
        # Le nom dans Yuman est déjà normalisé (sans "01", "France", "(Cestas)")
        # Note: Site est un frozen dataclass, on doit créer des copies
        sb_sites = {
            k: replace(s, name=normalize_site_name(s.name))
            for k, s in sb_sites.items()
        }

        # Diff sites
        # ignore_fields: latitude/longitude ne peuvent pas être mis à jour via l'API Yuman
        patch_sites = diff_entities(
            y_sites,
            sb_sites,
            ignore_fields={"client_map_id", "id", "ignore_site", "latitude", "longitude"}
        )
        
        logger.info("Diff sites: +%d ~%d -%d", 
                   len(patch_sites.add), len(patch_sites.update), len(patch_sites.delete))
        print(f"  Sites:       {C.GREEN}+{len(patch_sites.add)}{C.END}  "
              f"{C.YELLOW}~{len(patch_sites.update)}{C.END}  "
              f"{C.RED}-{len(patch_sites.delete)}{C.END}")
        
        # Mapping parent pour équipements
        id_by_vcom = {
            e.vcom_device_id: e.yuman_material_id
            for e in y_equips.values()
            if e.yuman_material_id
        }
        set_parent_map(id_by_vcom)

        # Diff équipements (inclut les SIM pour permettre leur création)
        # ignore_fields: name et parent_id ne peuvent pas être modifiés via l'API Yuman
        patch_equips_raw = diff_entities(
            y_equips,
            sb_equips,
            ignore_fields={"vcom_system_key", "parent_id", "name"}
        )

        # RÈGLE MÉTIER : Pour les SIM, Yuman est source de vérité
        # → On permet la CRÉATION de SIM, mais pas la mise à jour ni la suppression
        patch_equips = PatchSet(
            add=patch_equips_raw.add,  # Garder toutes les créations (y compris SIM)
            update=[(old, new) for old, new in patch_equips_raw.update if new.category_id != CAT_SIM],
            delete=[e for e in patch_equips_raw.delete if e.category_id != CAT_SIM],
        )
        
        logger.info("Diff équipements: +%d ~%d -%d",
                   len(patch_equips.add), len(patch_equips.update), len(patch_equips.delete))
        print(f"  Équipements: {C.GREEN}+{len(patch_equips.add)}{C.END}  "
              f"{C.YELLOW}~{len(patch_equips.update)}{C.END}  "
              f"{C.RED}-{len(patch_equips.delete)}{C.END}")
        
        report["sites"]["add"] = len(patch_sites.add)
        report["sites"]["update"] = len(patch_sites.update)
        report["sites"]["delete"] = len(patch_sites.delete)
        report["equipments"]["add"] = len(patch_equips.add)
        report["equipments"]["update"] = len(patch_equips.update)
        report["equipments"]["delete"] = len(patch_equips.delete)

        # BUG 3 FIX: Add detailed logs to report
        # Sites created
        for site in patch_sites.add:
            report["details"]["sites_created"].append({
                "name": site.name,
                "vcom_system_key": site.vcom_system_key,
                "yuman_site_id": site.yuman_site_id,
                "address": site.address,
            })

        # Sites updated with field changes
        site_fields = ['name', 'address', 'latitude', 'longitude', 'nominal_power', 'commission_date', 'vcom_system_key']
        for old, new in patch_sites.update:
            changes = {}
            for field in site_fields:
                old_val = getattr(old, field, None)
                new_val = getattr(new, field, None)
                if old_val != new_val:
                    changes[field] = {"old": old_val, "new": new_val}
            report["details"]["sites_updated"].append({
                "name": new.name,
                "vcom_system_key": new.vcom_system_key,
                "yuman_site_id": new.yuman_site_id,
                "changes": changes,
            })

        # Sites deleted
        for site in patch_sites.delete:
            report["details"]["sites_deleted"].append({
                "name": site.name,
                "vcom_system_key": site.vcom_system_key,
                "yuman_site_id": site.yuman_site_id,
            })

        # Equipments created
        for eq in patch_equips.add:
            report["details"]["equipments_created"].append({
                "name": eq.name,
                "serial_number": eq.serial_number,
                "category": CAT_NAMES.get(eq.category_id, f"CAT_{eq.category_id}"),
                "site_id": eq.site_id,
                "vcom_device_id": eq.vcom_device_id,
            })

        # Equipments updated with field changes
        equip_fields = ['name', 'brand', 'model', 'count', 'serial_number']
        for old, new in patch_equips.update:
            changes = {}
            for field in equip_fields:
                old_val = getattr(old, field, None)
                new_val = getattr(new, field, None)
                if old_val != new_val:
                    changes[field] = {"old": old_val, "new": new_val}
            report["details"]["equipments_updated"].append({
                "name": new.name,
                "serial_number": new.serial_number,
                "category": CAT_NAMES.get(new.category_id, f"CAT_{new.category_id}"),
                "yuman_material_id": new.yuman_material_id,
                "changes": changes,
            })

        # Equipments deleted
        for eq in patch_equips.delete:
            report["details"]["equipments_deleted"].append({
                "name": eq.name,
                "serial_number": eq.serial_number,
                "category": CAT_NAMES.get(eq.category_id, f"CAT_{eq.category_id}"),
                "yuman_material_id": eq.yuman_material_id,
            })

    except Exception as e:
        logger.error("Erreur Phase 3: %s", e, exc_info=True)
        report["errors"].append({"phase": "phase3", "error": str(e)})
        report["success"] = False
        return report
    
    # ═══════════════════════════════════════════════════════════════════════════
    # RÉSUMÉ
    # ═══════════════════════════════════════════════════════════════════════════
    print_header("RÉSUMÉ DES CHANGEMENTS")
    
    total_changes = (
        len(patch_sites.add) + len(patch_sites.update) + len(patch_sites.delete) +
        len(patch_equips.add) + len(patch_equips.update) + len(patch_equips.delete)
    )
    
    print(f"\n{C.BOLD}Total: {total_changes} changement(s){C.END}")
    
    # Détail sites
    if patch_sites.add:
        print(f"\n{C.GREEN}Sites à créer ({len(patch_sites.add)}):{C.END}")
        for s in patch_sites.add[:5]:
            print(f"  • {s.name} (vcom_key={s.vcom_system_key})")
        if len(patch_sites.add) > 5:
            print(f"  ... et {len(patch_sites.add) - 5} autres")
    
    if patch_sites.update:
        print(f"\n{C.YELLOW}Sites à mettre à jour ({len(patch_sites.update)}):{C.END}")
        for old, new in patch_sites.update[:5]:
            print(f"  • {new.name} (yuman_id={new.yuman_site_id})")
        if len(patch_sites.update) > 5:
            print(f"  ... et {len(patch_sites.update) - 5} autres")
    
    # Détail équipements par catégorie
    if patch_equips.add:
        print(f"\n{C.GREEN}Équipements à créer ({len(patch_equips.add)}):{C.END}")
        by_cat = defaultdict(int)
        for e in patch_equips.add:
            by_cat[e.category_id] += 1
        for cat_id, count in sorted(by_cat.items()):
            print(f"  • {CAT_NAMES.get(cat_id, 'UNKNOWN')}: {count}")
    
    if patch_equips.update:
        print(f"\n{C.YELLOW}Équipements à mettre à jour ({len(patch_equips.update)}):{C.END}")
        by_cat = defaultdict(int)
        for old, new in patch_equips.update:
            by_cat[new.category_id] += 1
        for cat_id, count in sorted(by_cat.items()):
            print(f"  • {CAT_NAMES.get(cat_id, 'UNKNOWN')}: {count}")
    
    # ═══════════════════════════════════════════════════════════════════════════
    # DRY-RUN CHECK
    # ═══════════════════════════════════════════════════════════════════════════
    if dry_run:
        print(f"\n{C.YELLOW}═══ MODE DRY-RUN : Aucun changement appliqué ═══{C.END}")
        return report
    
    if total_changes == 0:
        print(f"\n{C.GREEN}✓ Aucun changement nécessaire - déjà synchronisé{C.END}")
        return report
    
    # ═══════════════════════════════════════════════════════════════════════════
    # CONFIRMATION
    # ═══════════════════════════════════════════════════════════════════════════
    if not auto_confirm:
        print(f"\n{C.YELLOW}⚠️  Ces changements vont être appliqués dans Yuman.{C.END}")
        response = input(f"{C.BOLD}Confirmer ? (oui/non): {C.END}").strip().lower()
        if response != "oui":
            print(f"{C.RED}✗ Annulé par l'utilisateur{C.END}")
            report["success"] = False
            report["errors"].append({"phase": "confirmation", "error": "Annulé par l'utilisateur"})
            return report
    
    # ═══════════════════════════════════════════════════════════════════════════
    # PHASE 4 : APPLICATION
    # ═══════════════════════════════════════════════════════════════════════════
    print_header("PHASE 4 : APPLICATION DES CHANGEMENTS")
    
    # 4.1 Sites
    if not patch_sites.is_empty():
        print(f"\n{C.BLUE}Application des changements sites...{C.END}")
        try:
            y.apply_sites_patch(
                db_sites=sb_sites,
                y_sites=y_sites,
                patch=patch_sites,
            )
            logger.info("Sites patch appliqué avec succès")
            print(f"  {C.GREEN}✓ Sites mis à jour{C.END}")
        except Exception as e:
            logger.error("Erreur application sites: %s", e, exc_info=True)
            print(f"  {C.RED}✗ Erreur: {e}{C.END}")
            report["errors"].append({"phase": "apply_sites", "error": str(e)})
            report["success"] = False
    
    # 4.2 Équipements
    if not patch_equips.is_empty():
        print(f"\n{C.BLUE}Application des changements équipements...{C.END}")
        try:
            y.apply_equips_patch(
                db_equips=sb_equips,
                y_equips=y_equips,
                patch=patch_equips,
            )
            logger.info("Équipements patch appliqué avec succès")
            print(f"  {C.GREEN}✓ Équipements mis à jour{C.END}")
        except Exception as e:
            logger.error("Erreur application équipements: %s", e, exc_info=True)
            print(f"  {C.RED}✗ Erreur: {e}{C.END}")
            report["errors"].append({"phase": "apply_equips", "error": str(e)})
            report["success"] = False
    
    # ═══════════════════════════════════════════════════════════════════════════
    # PHASE 5 : VÉRIFICATION
    # ═══════════════════════════════════════════════════════════════════════════
    print_header("PHASE 5 : VÉRIFICATION")

    try:
        print("Re-lecture Yuman après application...")

        # Re-fetch Yuman
        y_sites_after_all = y.fetch_sites()
        y_equips_after_all = y.fetch_equips()

        # APPLIQUER LES MÊMES FILTRES QUE PHASES 1-2 :
        # 1. Exclure les sites ignorés
        y_sites_after = {
            k: s for k, s in y_sites_after_all.items()
            if k not in ignored_yuman_site_ids
        }

        # 2. Exclure les équipements des sites ignorés
        y_equips_after = {
            k: e for k, e in y_equips_after_all.items()
            if e.site_id not in ignored_supabase_site_ids
        }

        # 3. Filtrer par site_key si spécifié
        if site_key:
            y_sites_after = {k: s for k, s in y_sites_after.items() if k in target_yuman_site_ids}
            y_equips_after = {k: e for k, e in y_equips_after.items() if e.site_id in target_supabase_site_ids}

        # Nouveau diff pour vérifier (sites)
        patch_sites_after = diff_entities(
            y_sites_after, sb_sites,
            ignore_fields={"client_map_id", "id", "ignore_site", "latitude", "longitude"}
        )

        # 4. Diff équipements pour vérification
        patch_equips_after_raw = diff_entities(
            y_equips_after, sb_equips,
            ignore_fields={"vcom_system_key", "parent_id", "name"}
        )

        # Appliquer la même règle SIM : ignorer UPDATE et DELETE pour les SIM
        patch_equips_after = PatchSet(
            add=patch_equips_after_raw.add,
            update=[(old, new) for old, new in patch_equips_after_raw.update if new.category_id != CAT_SIM],
            delete=[e for e in patch_equips_after_raw.delete if e.category_id != CAT_SIM],
        )

        remaining = (
            len(patch_sites_after.add) + len(patch_sites_after.update) + len(patch_sites_after.delete) +
            len(patch_equips_after.add) + len(patch_equips_after.update) + len(patch_equips_after.delete)
        )

        report["sites"]["after"] = len(y_sites_after)
        report["equipments"]["after"] = len(y_equips_after)

        if remaining == 0:
            print(f"\n{C.GREEN}✓✓✓ SUCCÈS : Supabase et Yuman sont parfaitement synchronisés !{C.END}")
        else:
            print(f"\n{C.YELLOW}⚠️  {remaining} différence(s) restante(s) après application{C.END}")
            print(f"    Sites: +{len(patch_sites_after.add)} ~{len(patch_sites_after.update)} -{len(patch_sites_after.delete)}")
            print(f"    Équipements: +{len(patch_equips_after.add)} ~{len(patch_equips_after.update)} -{len(patch_equips_after.delete)}")

    except Exception as e:
        logger.error("Erreur vérification: %s", e, exc_info=True)
        print(f"  {C.YELLOW}⚠️  Vérification échouée: {e}{C.END}")
        report["errors"].append({"phase": "verification", "error": str(e)})
    
    # ═══════════════════════════════════════════════════════════════════════════
    # FIN
    # ═══════════════════════════════════════════════════════════════════════════
    print_header("TERMINÉ")
    
    if report["success"]:
        print(f"{C.GREEN}✓ Synchronisation terminée avec succès{C.END}")
    else:
        print(f"{C.RED}✗ Synchronisation terminée avec des erreurs{C.END}")
        for err in report["errors"]:
            print(f"  • {err['phase']}: {err['error']}")

    # Auto-génération du rapport JSON
    os.makedirs("logs", exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    auto_json_path = f"logs/sync_sb_to_yuman_{timestamp}.json"
    with open(auto_json_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False, default=str)
    logger.info("Rapport JSON auto-généré: %s", auto_json_path)
    print(f"\n📄 Rapport JSON: {auto_json_path}")

    return report


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Synchronise Supabase → Yuman (sites + équipements)"
    )
    parser.add_argument(
        "--site-key",
        type=str,
        default=None,
        help="Filtrer sur un site spécifique (ex: 2KC5K)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Mode diagnostic uniquement (aucune modification)"
    )
    parser.add_argument(
        "--yes", "-y",
        action="store_true",
        help="Confirmer automatiquement (pour GitHub Actions)"
    )
    parser.add_argument(
        "--no-color",
        action="store_true",
        help="Désactiver les couleurs (pour logs CI)"
    )
    parser.add_argument(
        "--json",
        type=str,
        default=None,
        help="Exporter le rapport en JSON (chemin du fichier)"
    )
    
    args = parser.parse_args()
    
    # Désactiver couleurs si demandé ou si pas de TTY
    if args.no_color or not sys.stdout.isatty():
        C.disable()
    
    # Exécution
    report = sync_supabase_to_yuman(
        site_key=args.site_key,
        dry_run=args.dry_run,
        auto_confirm=args.yes,
    )
    
    # Export JSON si demandé
    if args.json:
        with open(args.json, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False, default=str)
        print(f"\nRapport JSON: {args.json}")
    
    # Exit code
    sys.exit(0 if report["success"] else 1)


if __name__ == "__main__":
    main()