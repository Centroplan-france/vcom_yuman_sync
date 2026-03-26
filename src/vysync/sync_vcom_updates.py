#!/usr/bin/env python3
"""
Workflow de synchronisation complète VCOM → Supabase.

Ce script synchronise tous les sites existants en détectant et appliquant
les changements de données (coordonnées, puissance, équipements, etc.)

Usage:
    poetry run python -m vysync.sync_vcom_updates
"""

import json
import logging
from collections import defaultdict
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Set

from vysync.adapters.supabase_adapter import SupabaseAdapter
from vysync.adapters.vcom_adapter import fetch_snapshot
from vysync.diff import PatchSet, diff_entities
from vysync.logging_config import setup_logging, get_reports_dir
from vysync.models import CAT_CENTRALE, CAT_SIM, CAT_INVERTER, CAT_STRING
from vysync.vcom_client import VCOMAPIClient

logger = logging.getLogger(__name__)


def format_changes_summary(
    patch: PatchSet, entity_type: str, ignore_fields: Optional[Set[str]] = None
) -> Dict[str, Any]:
    """
    Formate un résumé des changements pour un type d'entité.

    Args:
        patch: PatchSet contenant add, update, delete
        entity_type: "sites" ou "equipments"
        ignore_fields: Ensemble de champs à exclure du rapport (optionnel)

    Returns:
        Dictionnaire avec statistiques et détails des changements
    """
    if ignore_fields is None:
        ignore_fields = set()
    changes: Dict[str, Any] = {
        "entity_type": entity_type,
        "summary": {
            "added": len(patch.add),
            "updated": len(patch.update),
            "deleted": len(patch.delete),
        },
        "added_items": [],
        "updated_items": [],
        "deleted_items": [],
    }

    # Détailler les ajouts
    for item in patch.add:
        changes["added_items"].append(
            {
                "key": item.vcom_system_key
                if hasattr(item, "vcom_system_key")
                else item.serial_number,
                "name": item.name if hasattr(item, "name") else None,
            }
        )

    # Détailler les mises à jour (avec les champs modifiés)
    for old, new in patch.update:
        old_dict = asdict(old)
        new_dict = asdict(new)

        # Détecter les champs qui ont changé (en filtrant les champs ignorés)
        changed_fields = {}
        for key in old_dict.keys():
            # Ignorer les champs spécifiés dans ignore_fields
            if key in ignore_fields:
                continue

            old_val = old_dict.get(key)
            new_val = new_dict.get(key)
            if old_val != new_val:
                changed_fields[key] = {"old": old_val, "new": new_val}

        # N'ajouter l'item que si au moins un champ pertinent a changé
        if changed_fields:
            changes["updated_items"].append(
                {
                    "key": new.vcom_system_key
                    if hasattr(new, "vcom_system_key")
                    else new.serial_number,
                    "name": new.name if hasattr(new, "name") else None,
                    "changed_fields": changed_fields,
                }
            )

    # Détailler les suppressions
    for item in patch.delete:
        changes["deleted_items"].append(
            {
                "key": item.vcom_system_key
                if hasattr(item, "vcom_system_key")
                else item.serial_number,
                "name": item.name if hasattr(item, "name") else None,
            }
        )

    return changes


def sync_vcom_to_supabase() -> dict:
    """
    Synchronise tous les sites VCOM vers Supabase.

    Returns:
        Dictionnaire du rapport (aussi sauvegardé en JSON)
    """

    # ═══════════════════════════════════════════════════════════════
    # INITIALISATION
    # ═══════════════════════════════════════════════════════════════
    logger.info("═" * 60)
    logger.info("DÉMARRAGE : Synchronisation VCOM → Supabase")
    logger.info("═" * 60)

    vc = VCOMAPIClient()
    sb = SupabaseAdapter()

    # ═══════════════════════════════════════════════════════════════
    # PHASE 1 : RÉCUPÉRATION DES DONNÉES
    # ═══════════════════════════════════════════════════════════════
    logger.info("\n📥 RÉCUPÉRATION DES DONNÉES")
    logger.info("─" * 60)

    logger.info("Récupération snapshot VCOM (tous les sites)...")
    v_sites, v_equips = fetch_snapshot(vc, sb_adapter=sb)
    logger.info("  ✓ Sites VCOM : %d", len(v_sites))
    logger.info("  ✓ Équipements VCOM : %d", len(v_equips))

    logger.info("\nRécupération données Supabase (y compris obsolètes pour comparaison)...")
    db_sites = sb.fetch_sites_v()
    db_equips = sb.fetch_equipments_v(include_obsolete=True)
    logger.info("  ✓ Sites Supabase : %d", len(db_sites))
    logger.info("  ✓ Équipements Supabase : %d", len(db_equips))

    # ═══════════════════════════════════════════════════════════════
    # PHASE 2 : DÉTECTION DES CHANGEMENTS
    # ═══════════════════════════════════════════════════════════════
    logger.info("\n🔍 DÉTECTION DES CHANGEMENTS")
    logger.info("─" * 60)

    # Diff des sites (en ignorant les champs non-VCOM)
    logger.info("Comparaison des sites...")
    patch_sites = diff_entities(
        db_sites,
        v_sites,
        ignore_fields={
            "yuman_site_id",  # Clé Yuman
            "id",  # Clé interne Supabase
            "ignore_site",  # Flag manuel
            "client_map_id",  # Géré par sync_new_sites
            "name",  # Géré par sync_new_sites (changements de nom)
            "code",  # Code Yuman (pas dans VCOM)
            "aldi_id",  # ALDI ID (pas dans VCOM)
            "aldi_store_id",  # ID magasin Aldi (pas dans VCOM)
            "project_number_cp",  # Project number Centroplan (pas dans VCOM)
        },
    )

    logger.info("  • Sites à ajouter : %d", len(patch_sites.add))
    logger.info("  • Sites à modifier : %d", len(patch_sites.update))
    logger.info("  • Sites à supprimer : %d", len(patch_sites.delete))

    # FILTRAGE : Nouveaux sites (gérés par sync_new_sites.py)
    if patch_sites.add:
        logger.warning(
            "⚠️  %d nouveaux sites détectés → ignorés (utiliser sync_new_sites.py)",
            len(patch_sites.add),
        )

    # FILTRAGE : Suppressions (sécurité)
    if patch_sites.delete:
        logger.warning(
            "⚠️  %d sites absents de VCOM → pas de suppression automatique",
            len(patch_sites.delete),
        )

    # Créer un PatchSet filtré
    patch_sites = PatchSet(
        add=[],  # Pas de création (sync_new_sites.py)
        update=patch_sites.update,  # Garder les mises à jour
        delete=[],  # Pas de suppression (sécurité)
    )

    logger.info("\nAprès filtrage :")
    logger.info("  • Sites à modifier : %d", len(patch_sites.update))

    # Diff des équipements
    logger.info("\nComparaison des équipements...")
    patch_equips = diff_entities(
        db_equips, v_equips, ignore_fields={"yuman_material_id", "parent_id"}
    )

    logger.info("  • Équipements à ajouter : %d", len(patch_equips.add))
    logger.info("  • Équipements à modifier : %d", len(patch_equips.update))
    logger.info("  • Équipements à supprimer : %d", len(patch_equips.delete))

    # FILTRAGE : SIM et CENTRALE ne doivent JAMAIS être mis à jour
    if patch_equips.update:
        filtered_updates = []
        skipped_sim_centrale = 0

        for old, new in patch_equips.update:
            if new.category_id in (CAT_SIM, CAT_CENTRALE):
                skipped_sim_centrale += 1
                continue
            filtered_updates.append((old, new))

        if skipped_sim_centrale > 0:
            logger.info(
                "  • SIM/CENTRALE ignorés (équipements synthétiques) : %d",
                skipped_sim_centrale,
            )

        patch_equips = PatchSet(
            add=patch_equips.add,
            update=filtered_updates,
            delete=[],
        )

    # FILTRAGE : Suppressions (marquage obsolète pour onduleurs uniquement)
    if patch_equips.delete:
        # Séparer les onduleurs (peuvent être marqués obsolètes) des autres équipements
        inverters_to_delete = [e for e in patch_equips.delete if e.category_id == CAT_INVERTER]
        other_to_delete = [e for e in patch_equips.delete if e.category_id != CAT_INVERTER]

        if other_to_delete:
            logger.warning(
                "⚠️  %d équipements non-onduleurs absents de VCOM → pas de suppression automatique",
                len(other_to_delete),
            )

        if inverters_to_delete:
            logger.info(
                "🗑️  %d onduleurs orphelins détectés → marquage is_obsolete=True",
                len(inverters_to_delete),
            )
            for inv in inverters_to_delete:
                logger.info(
                    "   • Onduleur orphelin: serial=%s, vcom_device_id=%s",
                    inv.serial_number, inv.vcom_device_id
                )

        patch_equips = PatchSet(
            add=patch_equips.add,
            update=patch_equips.update,
            delete=inverters_to_delete,  # Seuls les onduleurs peuvent être marqués obsolètes
        )

    logger.info("\nAprès filtrage :")
    logger.info("  • Équipements à ajouter : %d", len(patch_equips.add))
    logger.info("  • Équipements à modifier : %d", len(patch_equips.update))
    logger.info("  • Onduleurs à marquer obsolètes : %d", len(patch_equips.delete))

    # RÉACTIVATION : onduleurs marqués obsolètes mais toujours présents dans VCOM
    logger.info("\nVérification des onduleurs obsolètes à réactiver...")
    obsolete_inv_serials = sb.fetch_obsolete_inverter_serials()
    to_restore = sorted(sn for sn in obsolete_inv_serials if sn in v_equips)
    if to_restore:
        logger.info(
            "♻️  %d onduleur(s) obsolètes détectés dans VCOM → réactivation :",
            len(to_restore),
        )
        for sn in to_restore:
            inv = v_equips[sn]
            logger.info("   • serial=%s | site=%s", sn, inv.site_id)
    else:
        logger.info("  ✓ Aucun onduleur obsolète à réactiver")

    # ORPHELINS : strings actives dont le parent (onduleur) est absent de VCOM
    # On utilise v_equips (source VCOM) pour déterminer quels onduleurs sont actifs,
    # ce qui est plus fiable que db_equips (qui inclut des entrées obsolètes).
    logger.info("\nDétection des strings orphelines...")
    vcom_inv_serials = {
        e.serial_number
        for e in v_equips.values()
        if e.category_id == CAT_INVERTER and e.serial_number
    }
    orphan_strings = []
    for e in db_equips.values():
        if e.category_id != CAT_STRING:
            continue
        if not e.parent_id:
            continue
        if e.parent_id not in vcom_inv_serials:
            orphan_strings.append(e)
    if orphan_strings:
        logger.warning(
            "⚠️  %d string(s) orpheline(s) (parent absent ou obsolète) :", len(orphan_strings)
        )
        # Grouper par parent_id pour un log plus lisible
        by_parent: dict = defaultdict(list)
        for s in orphan_strings:
            by_parent[s.parent_id].append(s.name)
        for parent_sn, names in sorted(by_parent.items()):
            logger.warning("   • parent_serial=%s → %d string(s): %s", parent_sn, len(names), names[:3])
    else:
        logger.info("  ✓ Aucune string orpheline détectée")

    # ═══════════════════════════════════════════════════════════════
    # PHASE 3 : APPLICATION DES CHANGEMENTS
    # ═══════════════════════════════════════════════════════════════
    total_changes = (
        len(patch_sites.add)
        + len(patch_sites.update)
        + len(patch_sites.delete)
        + len(patch_equips.add)
        + len(patch_equips.update)
        + len(patch_equips.delete)
        + len(to_restore)
    )

    if total_changes == 0:
        logger.info("\n✅ Aucun changement détecté - Base de données à jour")
        changes_applied = False
    else:
        logger.info("\n💾 APPLICATION DES CHANGEMENTS")
        logger.info("─" * 60)

        # Appliquer les modifications de sites
        if (
            len(patch_sites.add) + len(patch_sites.update) + len(patch_sites.delete)
            > 0
        ):
            logger.info("Application des modifications de sites...")
            sb.apply_sites_patch(patch_sites)
            logger.info("  ✓ Sites synchronisés")

        # Appliquer les modifications d'équipements
        if (
            len(patch_equips.add) + len(patch_equips.update) + len(patch_equips.delete)
            > 0
        ):
            logger.info("\nApplication des modifications d'équipements...")
            sb.apply_equips_patch(patch_equips)
            logger.info("  ✓ Équipements synchronisés")

        # Réactiver les onduleurs obsolètes qui sont revenus dans VCOM
        if to_restore:
            logger.info("\nRéactivation des onduleurs obsolètes présents dans VCOM...")
            restored_count = sb.restore_inverters(to_restore)
            logger.info("  ✓ %d onduleur(s) réactivé(s)", restored_count)

        changes_applied = True
        logger.info("\n✅ Tous les changements ont été appliqués")

    # ═══════════════════════════════════════════════════════════════
    # PHASE 4 : GÉNÉRATION DU RAPPORT
    # ═══════════════════════════════════════════════════════════════
    logger.info("\n📊 GÉNÉRATION DU RAPPORT")
    logger.info("─" * 60)

    report = {
        "execution_date": datetime.now(timezone.utc).isoformat(),
        "changes_applied": changes_applied,
        "summary": {
            "total_changes": total_changes,
            "sites": {
                "added": len(patch_sites.add),
                "updated": len(patch_sites.update),
                "deleted": len(patch_sites.delete),
            },
            "equipments": {
                "added": len(patch_equips.add),
                "updated": len(patch_equips.update),
                "deleted": len(patch_equips.delete),
                "restored": len(to_restore),
            },
        },
        "restored_inverters": [
            {"serial_number": sn, "name": v_equips[sn].name, "site_id": v_equips[sn].site_id}
            for sn in to_restore
        ],
        "orphan_strings": [
            {"serial_number": s.serial_number, "name": s.name, "parent_id": s.parent_id, "site_id": s.site_id}
            for s in orphan_strings
        ],
        "sites_changes": format_changes_summary(
            patch_sites,
            "sites",
            ignore_fields={
                "yuman_site_id",  # Clé Yuman (pas dans VCOM)
                "id",  # Clé interne Supabase
                "ignore_site",  # Flag manuel
                "client_map_id",  # Géré par sync_new_sites
                "name",  # Géré par sync_new_sites
                "code",  # Code Yuman (pas dans VCOM)
                "aldi_id",  # ALDI ID (pas dans VCOM)
                "aldi_store_id",  # ID magasin Aldi (pas dans VCOM)
                "project_number_cp",  # Project number Centroplan (pas dans VCOM)
            },
        ),
        "equipments_changes": format_changes_summary(
            patch_equips,
            "equipments",
            ignore_fields={
                "yuman_material_id",  # Clé Yuman (pas dans VCOM)
                "parent_id",  # Contrainte FK complexe
            },
        ),
    }

    # Sauvegarder en JSON
    report_path = get_reports_dir() / f"sync_vcom_updates_{datetime.now():%Y%m%d_%H%M%S}.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    logger.info("\n" + "═" * 60)
    logger.info("📈 RÉSUMÉ FINAL")
    logger.info("═" * 60)
    logger.info("Total changements : %d", total_changes)
    logger.info("")
    logger.info("Sites :")
    logger.info("  • Ajoutés : %d", len(patch_sites.add))
    logger.info("  • Modifiés : %d", len(patch_sites.update))
    logger.info("  • Supprimés : %d", len(patch_sites.delete))
    logger.info("")
    logger.info("Équipements :")
    logger.info("  • Ajoutés : %d", len(patch_equips.add))
    logger.info("  • Modifiés : %d", len(patch_equips.update))
    logger.info("  • Supprimés : %d", len(patch_equips.delete))
    logger.info("  • Réactivés (obsolète → actif) : %d", len(to_restore))
    logger.info("")
    if orphan_strings:
        logger.info("Strings orphelines (parent absent/obsolète) : %d", len(orphan_strings))
        logger.info("")
    logger.info("Rapport sauvegardé : %s", report_path.name)
    logger.info("═" * 60)

    return report


def main():
    """Point d'entrée CLI."""
    setup_logging()

    try:
        report = sync_vcom_to_supabase()

        # Code de sortie basé sur les changements
        total_changes = report["summary"]["total_changes"]

        if total_changes == 0:
            logger.info("✅ Synchronisation terminée - Aucun changement nécessaire")
        else:
            logger.info(
                "✅ Synchronisation terminée - %d changements appliqués", total_changes
            )

        return 0

    except Exception as e:
        logger.error("❌ Erreur fatale : %s", e, exc_info=True)
        return 1


if __name__ == "__main__":
    exit(main())
