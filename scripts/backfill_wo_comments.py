#!/usr/bin/env python3
"""
backfill_wo_comments.py - Script de rattrapage one-shot pour poster les
commentaires VCOM manquants sur les tickets lies a un workorder.

Cible : tous les tickets avec yuman_workorder_id IS NOT NULL
        et vcom_comment_id IS NULL.

Usage:
  poetry run python scripts/backfill_wo_comments.py           # dry-run (defaut)
  poetry run python scripts/backfill_wo_comments.py --execute  # execution reelle
"""
from __future__ import annotations

import argparse
import logging
import os
import sys

# Ajouter le dossier src/ au path pour pouvoir importer vysync
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from supabase import create_client
from vysync.vcom_client import VCOMAPIClient
from vysync.yuman_client import YumanClient
from vysync.sync_tickets_workorders import (
    _update_vcom_comments_for_wo,
    init_users_cache,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("backfill_wo_comments")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Rattrapage des commentaires VCOM manquants"
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Executer pour de vrai (sans ce flag = dry-run)",
    )
    args = parser.parse_args()
    dry_run = not args.execute

    if dry_run:
        logger.info("=== MODE DRY-RUN — aucune ecriture ne sera faite ===")
    else:
        logger.info("=== MODE EXECUTION REELLE ===")

    # --- Connexions --------------------------------------------------------
    sb = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_KEY"))
    vc = VCOMAPIClient()
    yc = YumanClient(os.getenv("YUMAN_TOKEN"))

    # Initialiser le cache des techniciens (necessaire pour le formatage)
    init_users_cache(yc)

    # --- 1. Recuperer les tickets concernes --------------------------------
    logger.info("Recuperation des tickets avec yuman_workorder_id NOT NULL et vcom_comment_id NULL...")
    result = (
        sb.table("tickets")
        .select("vcom_ticket_id, yuman_workorder_id, vcom_comment_id")
        .not_.is_("yuman_workorder_id", "null")
        .is_("vcom_comment_id", "null")
        .limit(10000)
        .execute()
    )
    tickets_to_fix = result.data or []
    logger.info("Tickets concernes : %d", len(tickets_to_fix))

    if not tickets_to_fix:
        logger.info("Rien a faire, tous les commentaires sont deja en place.")
        return 0

    # --- 2. Grouper par workorder_id ---------------------------------------
    tickets_by_wo: dict[int, list[dict]] = {}
    for t in tickets_to_fix:
        wo_id = t["yuman_workorder_id"]
        tickets_by_wo.setdefault(wo_id, []).append(t)

    logger.info("Workorders concernes : %d", len(tickets_by_wo))

    # --- 3. Pour chaque WO, recuperer wo_history et traiter ----------------
    success_count = 0
    skip_count = 0
    error_count = 0

    for wo_id, tickets in tickets_by_wo.items():
        ticket_ids = [t["vcom_ticket_id"] for t in tickets]
        logger.info(
            "WO %s → %d ticket(s) sans commentaire : %s",
            wo_id, len(tickets), ticket_ids,
        )

        # Recuperer wo_history depuis la table work_orders
        # Note: la colonne "number" n'existe pas en base, on utilise workorder_id
        try:
            wo_db = (
                sb.table("work_orders")
                .select("workorder_id, wo_history")
                .eq("workorder_id", wo_id)
                .execute()
            )
        except Exception as exc:
            logger.error("  Erreur lecture work_orders pour WO %s : %s", wo_id, exc)
            error_count += 1
            continue

        if not wo_db.data:
            logger.warning("  WO %s introuvable dans work_orders, skip", wo_id)
            skip_count += 1
            continue

        wo_row = wo_db.data[0]
        wo_history = wo_row.get("wo_history") or []

        if not wo_history:
            logger.warning("  WO %s : wo_history vide, skip", wo_id)
            skip_count += 1
            continue

        # _update_vcom_comments_for_wo utilise wo.get("number", wo_id)
        # On met number = workorder_id comme fallback lisible
        wo_row["number"] = wo_id
        wo_number = wo_id
        logger.info(
            "  WO #%s : %d entrees dans wo_history, %d ticket(s) a traiter",
            wo_number, len(wo_history), len(tickets),
        )

        if dry_run:
            # En dry-run, on affiche ce qui serait fait sans rien ecrire
            from vysync.sync_tickets_workorders import _format_wo_history_as_comment
            comment_preview = _format_wo_history_as_comment(wo_number, wo_history)
            for t in tickets:
                logger.info(
                    "  [DRY-RUN] Posterait un commentaire sur ticket %s :",
                    t["vcom_ticket_id"],
                )
                # Afficher les 5 premieres lignes du commentaire
                preview_lines = comment_preview.split("\n")[:6]
                for line in preview_lines:
                    logger.info("    | %s", line)
                if len(comment_preview.split("\n")) > 6:
                    logger.info("    | ... (%d lignes au total)", len(comment_preview.split("\n")))
            success_count += len(tickets)
        else:
            # Execution reelle
            try:
                _update_vcom_comments_for_wo(sb, vc, wo_id, wo_row, wo_history, tickets)
                success_count += len(tickets)
            except Exception as exc:
                logger.error("  Erreur traitement WO %s : %s", wo_id, exc)
                error_count += 1

    # --- Resume ------------------------------------------------------------
    logger.info("=== Resume ===")
    logger.info("Tickets traites avec succes : %d", success_count)
    logger.info("WO ignores (introuvables ou historique vide) : %d", skip_count)
    logger.info("Erreurs : %d", error_count)

    if dry_run:
        logger.info("Mode dry-run : aucune modification effectuee.")
        logger.info("Relancez avec --execute pour appliquer les changements.")

    return 0 if error_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
