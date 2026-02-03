#!/usr/bin/env python3
"""
backfill_kpi_dates.py - Remplit les colonnes KPI pour l'historique.

Usage: /workspaces/vcom_yuman_sync/scripts/exploration/backfill_kpi_dates.py
  poetry run python -m scripts.exploration.backfill_kpi_dates [--dry-run]

Ce script :
  1. Récupère tous les tickets depuis VCOM et met à jour vcom_created_at/vcom_rectified_at
  2. Récupère tous les WO depuis Yuman et met à jour yuman_created_at/date_done
"""
from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime, timezone

from supabase import create_client
from vysync.vcom_client import VCOMAPIClient
from vysync.yuman_client import YumanClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="Backfill des dates KPI")
    parser.add_argument("--dry-run", action="store_true", help="Pas d'écriture en base")
    parser.add_argument("--tickets-only", action="store_true", help="Backfill uniquement les tickets")
    parser.add_argument("--wo-only", action="store_true", help="Backfill uniquement les work orders")
    return parser.parse_args()


def backfill_tickets(sb, vc, *, dry: bool = False) -> int:
    """
    Backfill vcom_created_at et vcom_rectified_at pour tous les tickets.
    
    Stratégie : Récupérer tous les tickets VCOM (open + assigned + inProgress + closed)
    et mettre à jour en base.
    
    Returns:
        Nombre de tickets mis à jour
    """
    logger.info("=== BACKFILL TICKETS ===")
    
    # Récupérer tous les tickets VCOM
    all_tickets = []
    for status in ["open", "assigned", "inProgress", "closed"]:
        try:
            tickets = vc.get_tickets(status=status)
            all_tickets.extend(tickets)
            logger.info("VCOM: %d tickets récupérés (status=%s)", len(tickets), status)
        except Exception as exc:
            logger.error("Erreur récupération tickets VCOM (%s): %s", status, exc)
    
    logger.info("Total tickets VCOM: %d", len(all_tickets))
    
    # Mettre à jour en base
    updated = 0
    for t in all_tickets:
        vcom_id = t.get("id")
        created_at = t.get("createdAt")
        rectified_at = t.get("rectifiedAt")
        
        if not vcom_id:
            continue
        
        update_data = {}
        if created_at:
            update_data["vcom_created_at"] = created_at
        if rectified_at:
            update_data["vcom_rectified_at"] = rectified_at
        
        if not update_data:
            continue
        
        if dry:
            logger.debug("[DRY] Ticket %s: %s", vcom_id, update_data)
        else:
            try:
                sb.table("tickets").update(update_data).eq("vcom_ticket_id", str(vcom_id)).execute()
                updated += 1
            except Exception as exc:
                logger.error("Erreur update ticket %s: %s", vcom_id, exc)
    
    logger.info("Tickets mis à jour: %d", updated)
    return updated


def backfill_workorders(sb, yc, *, dry: bool = False) -> int:
    """
    Backfill yuman_created_at et date_done pour tous les work orders.
    
    Stratégie : Récupérer tous les WO Yuman (paginé) et mettre à jour en base.
    Les champs sont disponibles dans la liste, pas besoin de requêtes individuelles.
    
    Returns:
        Nombre de WO mis à jour
    """
    logger.info("=== BACKFILL WORK ORDERS ===")
    
    # Récupérer tous les WO Yuman
    try:
        all_wo = yc.list_workorders()
        logger.info("Yuman: %d workorders récupérés", len(all_wo))
    except Exception as exc:
        logger.error("Erreur récupération workorders Yuman: %s", exc)
        return 0
    
    # Mettre à jour en base
    updated = 0
    for wo in all_wo:
        wo_id = wo.get("id")
        created_at = wo.get("created_at")
        date_done = wo.get("date_done")
        
        if not wo_id:
            continue
        
        update_data = {}
        if created_at:
            update_data["yuman_created_at"] = created_at
        if date_done:
            update_data["date_done"] = date_done
        
        if not update_data:
            continue
        
        if dry:
            logger.debug("[DRY] WO %s: %s", wo_id, update_data)
        else:
            try:
                sb.table("work_orders").update(update_data).eq("workorder_id", wo_id).execute()
                updated += 1
            except Exception as exc:
                logger.error("Erreur update WO %s: %s", wo_id, exc)
    
    logger.info("Work orders mis à jour: %d", updated)
    return updated


def main():
    args = parse_args()
    
    # Clients
    sb = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_KEY"))
    vc = VCOMAPIClient()
    yc = YumanClient()
    
    logger.info("Backfill KPI dates - dry_run=%s", args.dry_run)
    
    total_updated = 0
    
    if not args.wo_only:
        total_updated += backfill_tickets(sb, vc, dry=args.dry_run)
    
    if not args.tickets_only:
        total_updated += backfill_workorders(sb, yc, dry=args.dry_run)
    
    logger.info("=== BACKFILL TERMINÉ ===")
    logger.info("Total enregistrements mis à jour: %d", total_updated)


if __name__ == "__main__":
    main()