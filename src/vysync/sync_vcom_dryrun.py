#!/usr/bin/env python3
"""
Dry-run de synchronisation VCOM → Supabase.

Ce script génère un JSON avec tous les changements détectés SANS modifier la DB.
Utile pour vérifier les changements avant d'exécuter le vrai sync.

Usage:
    poetry run python -m vysync.sync_vcom_dryrun
    poetry run python -m vysync.sync_vcom_dryrun --site JG9Q3
    poetry run python -m vysync.sync_vcom_dryrun --output changes.json
"""

import argparse
import json
import logging
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Set

from vysync.adapters.supabase_adapter import SupabaseAdapter
from vysync.adapters.vcom_adapter import fetch_snapshot
from vysync.diff import PatchSet, diff_entities
from vysync.logging_config import setup_logging
from vysync.models import CAT_CENTRALE, CAT_SIM, CAT_INVERTER, CAT_MODULE, CAT_STRING
from vysync.vcom_client import VCOMAPIClient

logger = logging.getLogger(__name__)


CATEGORY_NAMES = {
    CAT_INVERTER: "INVERTER",
    CAT_MODULE: "MODULE",
    CAT_STRING: "STRING",
    CAT_CENTRALE: "CENTRALE",
    CAT_SIM: "SIM",
}


def get_changed_fields(old: Any, new: Any) -> Dict[str, Dict[str, Any]]:
    """
    Compare deux objets et retourne uniquement les champs qui ont changé.

    Returns:
        Dict avec les champs modifiés: {"field": {"old": value, "new": value}}
    """
    old_dict = asdict(old)
    new_dict = asdict(new)

    changes = {}
    for key in old_dict.keys():
        old_val = old_dict.get(key)
        new_val = new_dict.get(key)

        # Normaliser pour comparaison
        if isinstance(old_val, str) and isinstance(new_val, str):
            if old_val.strip().lower() != new_val.strip().lower():
                changes[key] = {"old": old_val, "new": new_val}
        elif old_val != new_val:
            changes[key] = {"old": old_val, "new": new_val}

    return changes


def format_equipment_summary(eq: Any) -> Dict[str, Any]:
    """Formate un équipement pour le JSON de sortie."""
    return {
        "serial_number": eq.serial_number,
        "vcom_device_id": eq.vcom_device_id,
        "name": eq.name,
        "category": CATEGORY_NAMES.get(eq.category_id, str(eq.category_id)),
        "brand": eq.brand,
        "model": eq.model,
        "site_id": eq.site_id,
        "name_inverter": getattr(eq, "name_inverter", None),
        "carport": getattr(eq, "carport", False),
    }


def sync_vcom_dryrun(site_key: Optional[str] = None) -> dict:
    """
    Effectue un dry-run de synchronisation VCOM → Supabase.

    Args:
        site_key: Optionnel - limiter à un site spécifique

    Returns:
        Dictionnaire avec tous les changements détectés
    """
    logger.info("═" * 60)
    logger.info("DRY-RUN : Synchronisation VCOM → Supabase")
    logger.info("═" * 60)
    if site_key:
        logger.info(f"🎯 Site filtré : {site_key}")

    vc = VCOMAPIClient()
    sb = SupabaseAdapter()

    # ═══════════════════════════════════════════════════════════════
    # RÉCUPÉRATION DES DONNÉES
    # ═══════════════════════════════════════════════════════════════
    logger.info("\n📥 RÉCUPÉRATION DES DONNÉES")
    logger.info("─" * 60)

    logger.info("Récupération snapshot VCOM...")
    v_sites, v_equips = fetch_snapshot(vc, vcom_system_key=site_key, sb_adapter=sb)
    logger.info("  ✓ Sites VCOM : %d", len(v_sites))
    logger.info("  ✓ Équipements VCOM : %d", len(v_equips))

    logger.info("\nRécupération données Supabase (y compris obsolètes pour comparaison)...")
    db_sites = sb.fetch_sites_v(site_key=site_key)
    db_equips = sb.fetch_equipments_v(site_key=site_key, include_obsolete=True)
    logger.info("  ✓ Sites Supabase : %d", len(db_sites))
    logger.info("  ✓ Équipements Supabase : %d", len(db_equips))

    # ═══════════════════════════════════════════════════════════════
    # DÉTECTION DES CHANGEMENTS
    # ═══════════════════════════════════════════════════════════════
    logger.info("\n🔍 DÉTECTION DES CHANGEMENTS")
    logger.info("─" * 60)

    # Diff des équipements
    patch_equips = diff_entities(
        db_equips, v_equips, ignore_fields={"yuman_material_id", "parent_id"}
    )

    logger.info("  • Équipements à ajouter : %d", len(patch_equips.add))
    logger.info("  • Équipements à modifier : %d", len(patch_equips.update))
    logger.info("  • Équipements à supprimer : %d", len(patch_equips.delete))

    # ═══════════════════════════════════════════════════════════════
    # CONSTRUCTION DU RAPPORT
    # ═══════════════════════════════════════════════════════════════

    report = {
        "execution_date": datetime.now(timezone.utc).isoformat(),
        "mode": "DRY-RUN",
        "site_filter": site_key,
        "summary": {
            "vcom_sites": len(v_sites),
            "vcom_equipments": len(v_equips),
            "db_sites": len(db_sites),
            "db_equipments": len(db_equips),
            "equipments_to_add": len(patch_equips.add),
            "equipments_to_update": len(patch_equips.update),
            "equipments_to_delete": len(patch_equips.delete),
        },
        "changes": {
            "add": [],
            "update": [],
            "delete": [],
        }
    }

    # Détailler les ajouts
    for eq in patch_equips.add:
        report["changes"]["add"].append(format_equipment_summary(eq))

    # Détailler les mises à jour (avec les champs modifiés)
    for old, new in patch_equips.update:
        changed_fields = get_changed_fields(old, new)

        # Filtrer les champs ignorés
        for field in ["yuman_material_id", "parent_id"]:
            changed_fields.pop(field, None)

        if changed_fields:  # Seulement si des champs pertinents ont changé
            update_entry = {
                "serial_number": new.serial_number,
                "vcom_device_id": new.vcom_device_id,
                "category": CATEGORY_NAMES.get(new.category_id, str(new.category_id)),
                "site_id": new.site_id,
                "changes": changed_fields,
            }
            report["changes"]["update"].append(update_entry)

    # Détailler les suppressions (onduleurs orphelins)
    for eq in patch_equips.delete:
        delete_entry = format_equipment_summary(eq)
        delete_entry["reason"] = "orphan" if eq.category_id == CAT_INVERTER else "missing"
        report["changes"]["delete"].append(delete_entry)

    # Statistiques par catégorie
    stats_by_category = {}
    for eq in patch_equips.add:
        cat_name = CATEGORY_NAMES.get(eq.category_id, "OTHER")
        stats_by_category.setdefault(cat_name, {"add": 0, "update": 0, "delete": 0})
        stats_by_category[cat_name]["add"] += 1

    for old, new in patch_equips.update:
        cat_name = CATEGORY_NAMES.get(new.category_id, "OTHER")
        stats_by_category.setdefault(cat_name, {"add": 0, "update": 0, "delete": 0})
        stats_by_category[cat_name]["update"] += 1

    for eq in patch_equips.delete:
        cat_name = CATEGORY_NAMES.get(eq.category_id, "OTHER")
        stats_by_category.setdefault(cat_name, {"add": 0, "update": 0, "delete": 0})
        stats_by_category[cat_name]["delete"] += 1

    report["stats_by_category"] = stats_by_category

    # ═══════════════════════════════════════════════════════════════
    # RÉSUMÉ
    # ═══════════════════════════════════════════════════════════════
    logger.info("\n" + "═" * 60)
    logger.info("📊 RÉSUMÉ DRY-RUN")
    logger.info("═" * 60)

    total_changes = (
        len(report["changes"]["add"]) +
        len(report["changes"]["update"]) +
        len(report["changes"]["delete"])
    )

    logger.info(f"Total changements détectés : {total_changes}")
    logger.info("")
    logger.info("Par catégorie :")
    for cat, stats in stats_by_category.items():
        logger.info(f"  {cat}:")
        if stats["add"]:
            logger.info(f"    • À ajouter : {stats['add']}")
        if stats["update"]:
            logger.info(f"    • À modifier : {stats['update']}")
        if stats["delete"]:
            logger.info(f"    • À supprimer : {stats['delete']}")

    if report["changes"]["update"]:
        logger.info("")
        logger.info("Détail des modifications d'onduleurs :")
        for upd in report["changes"]["update"]:
            if upd["category"] == "INVERTER":
                logger.info(f"  • {upd['serial_number']} ({upd['vcom_device_id']}):")
                for field, change in upd["changes"].items():
                    logger.info(f"      {field}: {change['old']!r} → {change['new']!r}")

    logger.info("")
    logger.info("⚠️  MODE DRY-RUN : Aucune modification n'a été appliquée à la DB")
    logger.info("═" * 60)

    return report


def main():
    """Point d'entrée CLI."""
    parser = argparse.ArgumentParser(
        description="Dry-run de synchronisation VCOM → Supabase"
    )
    parser.add_argument(
        "--site", "-s",
        help="Limiter à un site spécifique (ex: JG9Q3)",
        default=None
    )
    parser.add_argument(
        "--output", "-o",
        help="Fichier JSON de sortie (défaut: sync_dryrun_<timestamp>.json)",
        default=None
    )
    args = parser.parse_args()

    setup_logging()

    try:
        report = sync_vcom_dryrun(site_key=args.site)

        # Déterminer le nom du fichier de sortie
        if args.output:
            output_file = args.output
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            site_suffix = f"_{args.site}" if args.site else ""
            output_file = f"sync_dryrun{site_suffix}_{timestamp}.json"

        # Sauvegarder le rapport JSON
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False, default=str)

        logger.info(f"\n📄 Rapport sauvegardé : {output_file}")

        return 0

    except Exception as e:
        logger.error("❌ Erreur fatale : %s", e, exc_info=True)
        return 1


if __name__ == "__main__":
    exit(main())
