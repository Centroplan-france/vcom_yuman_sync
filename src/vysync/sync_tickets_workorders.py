#!/usr/bin/env python3
"""
sync_tickets_workorders.py – Synchronise les tickets VCOM et les workorders Yuman.

Usage:
  poetry run python -m vysync.sync_tickets_workorders [--dry-run]

Ce script exécute le flux suivant :
  1. Récupère les tickets VCOM (open/assigned/inProgress) puis les upsert dans Supabase.
  2. Récupère les workorders Yuman puis les upsert dans Supabase.
  3. Pour chaque workorder Yuman actif : enrichit sa description avec les tickets VCOM du même site non assignés et passe ces tickets à « assigned ».
  4. Si un site possède des tickets VCOM urgent/high mais aucun workorder actif : crée une workorder_demand Yuman et assigne les tickets.
  5. Ferme les tickets VCOM liés aux workorders Yuman clôturés.
"""
from __future__ import annotations

import argparse
import os
import logging
import textwrap
from datetime import datetime, timezone
from typing import Any, Dict, List

from supabase import create_client
from vysync.vcom_client import VCOMAPIClient
from vysync.yuman_client import YumanClient

# ---------------------------------------------------------------------------
# Logger global
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CLI

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Synchronise tickets VCOM et workorders Yuman"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Pas d'écriture ni update sur les APIs/BD",
    )
    return parser.parse_args()

# ---------------------------------------------------------------------------
# Helpers d'upsert (DB ⇆ Supabase)

def upsert_tickets(sb, tickets: List[Dict[str, Any]], *, dry: bool = False) -> None:
    # Récupérer l'ensemble des vcom_system_key valides dans sites_mapping
    valid_system_keys_result = sb.table("sites_mapping").select("vcom_system_key").execute()
    valid_system_keys = {row["vcom_system_key"] for row in valid_system_keys_result.data if row["vcom_system_key"] is not None}

    # Filtrer les tickets pour ne garder que ceux avec un systemKey valide
    valid_tickets = []
    ignored_tickets = []

    for t in tickets:
        system_key = t.get("systemKey")
        if system_key in valid_system_keys:
            valid_tickets.append(t)
        else:
            ignored_tickets.append(t)

    # Logger les tickets ignorés
    if ignored_tickets:
        ignored_ids = [t.get("id") for t in ignored_tickets]
        logger.warning(
            "%d tickets ignorés (system_key non présent dans sites_mapping): %s",
            len(ignored_tickets),
            ignored_ids
        )

    rows = [
        {
            "vcom_ticket_id": t["id"],
            "system_key": t.get("systemKey"),
            "title": t.get("designation"),
            "description": t.get("description"),
            "status": t.get("status"),
            "priority": t.get("priority"),
            "last_changed_at": t.get("lastChangedAt"),
        }
        for t in valid_tickets
    ]

    if not rows:
        return

    if dry:
        logger.info("[DRY] %d tickets à upsert", len(rows))
    else:
        sb.table("tickets").upsert(rows, on_conflict="vcom_ticket_id").execute()
        logger.info("%d tickets upsertés", len(rows))

def upsert_workorders(sb, orders: List[Dict[str, Any]], *, dry: bool = False) -> None:
    # Récupérer l'ensemble des yuman_site_id valides dans sites_mapping
    valid_site_ids_result = sb.table("sites_mapping").select("yuman_site_id").execute()
    valid_site_ids = {row["yuman_site_id"] for row in valid_site_ids_result.data if row["yuman_site_id"] is not None}

    # Filtrer les workorders pour ne garder que ceux avec un site_id valide
    valid_orders = []
    ignored_orders = []

    for w in orders:
        site_id = w.get("site_id")
        if site_id in valid_site_ids:
            valid_orders.append(w)
        else:
            ignored_orders.append(w)

    # Logger les workorders ignorés
    if ignored_orders:
        ignored_ids = [w.get("id") for w in ignored_orders]
        logger.warning(
            "%d work_orders ignorés (site_id non présent dans sites_mapping): %s",
            len(ignored_orders),
            ignored_ids
        )

    rows = [
        {
            "workorder_id": w["id"],
            "status": w.get("status"),
            "client_id": w.get("client_id"),
            "site_id": w.get("site_id"),
            "scheduled_date": w.get("date_planned"),  # garder pour compatibilité
            "date_planned": w.get("date_planned"),    # nouvelle colonne avec timezone
            "description": w.get("description"),
            "title": w.get("title"),
            "category_id": w.get("category_id"),
            "workorder_type": w.get("workorder_type"),
            "technician_id": w.get("technician_id"),
            "manager_id": w.get("manager_id"),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        for w in valid_orders
    ]

    if not rows:
        return

    if dry:
        logger.info("[DRY] %d workorders à upsert", len(rows))
    else:
        sb.table("work_orders").upsert(rows, on_conflict="workorder_id").execute()
        logger.info("%d workorders upsertés", len(rows))

# ---------------------------------------------------------------------------
# Étape 1 : collecte des tickets VCOM

def collect_vcom_tickets(vc, statuses: List[str] | None = None) -> List[Dict[str, Any]]:
    statuses = statuses or ["open", "assigned", "inProgress", "closed"]
    tickets: List[Dict[str, Any]] = []
    for st in statuses:
        try:
            chunk = vc.get_tickets(status=st)
            tickets.extend(chunk)
            logger.info("VCOM: %d tickets récupérés (status=%s)", len(chunk), st)
        except Exception as exc:
            logger.error("Erreur récupération tickets VCOM (%s): %s", st, exc)
    return tickets

# ---------------------------------------------------------------------------
# Étape 2 : collecte des workorders Yuman

def collect_yuman_workorders(yc) -> List[Dict[str, Any]]:
    try:
        data = yc.list_workorders()
        logger.info("YUMAN: %d workorders récupérés", len(data))
        return data
    except Exception as exc:
        logger.error("Erreur récupération workorders Yuman: %s", exc)
        return []

# ---------------------------------------------------------------------------
# Étapes 1‑bis & 2‑bis : synchronisation DB

def sync_tickets_to_db(sb, tickets, *, dry: bool = False) -> None:
    upsert_tickets(sb, tickets, dry=dry)

def sync_workorders_to_db(sb, workorders, *, dry: bool = False) -> None:
    upsert_workorders(sb, workorders, dry=dry)

# ---------------------------------------------------------------------------
# Étape 3 : enrichir les workorders actifs et assigner les tickets

def assign_tickets_to_active_workorders(
    sb, vc, yc, workorders: List[Dict[str, Any]], *, dry: bool = False
) -> None:
    # Regroupement des workorders actifs par site
    active_by_site: Dict[int, List[Dict[str, Any]]] = {}
    for w in workorders:
        if w.get("status", "").lower() != "closed":
            active_by_site.setdefault(w.get("site_id"), []).append(w)

    for site_id, orders in active_by_site.items():
        # VCOM system_key ↔ Yuman site_id
        res = (
            sb.table("sites_mapping")
            .select("vcom_system_key")
            .eq("yuman_site_id", site_id)
            .execute()
        )
        if not res.data:
            continue
        system_key = res.data[0]["vcom_system_key"]
        if not system_key:
            continue

        # Tickets non assignés sur ce site
        rows = (
            sb.table("tickets")
            .select("*")
            .eq("system_key", system_key)
            .not_.in_("status", ["assigned", "inProgress", "closed", "deleted"])
            .not_.in_("priority", ["low"])
            .is_("yuman_workorder_id", None)
            .execute()
        )
        new_tickets = rows.data or []
        if not new_tickets:
            continue

        # Enrichissement de la description du premier workorder actif
        order = orders[0]
        additional = "".join(
            f"\n\n{t['title'] or t['vcom_ticket_id']}:\n{textwrap.fill(t.get('description', ''), width=80)}"
            for t in new_tickets
        )
        new_desc = (order.get("description") or "") + additional

        if not dry:
            try:
                yc.update_workorder(order["id"], {"description": new_desc})
                logger.info("Workorder %s mis à jour", order["id"])
            except Exception as exc:
                logger.error("Échec update workorder %s: %s", order["id"], exc)
                continue

        # Passer les tickets à « assigned »
        for t in new_tickets:
            tid = t["vcom_ticket_id"]
            if dry:
                continue
            try:
                vc.update_ticket(tid, status="assigned")
                sb.table("tickets").update(
                    {"status": "assigned", "yuman_workorder_id": order["id"]}
                ).eq("vcom_ticket_id", tid).execute()
            except Exception as exc:
                logger.error("Échec update ticket %s: %s", tid, exc)

# ---------------------------------------------------------------------------
# Étape 4 : créer des workorders « Open » pour les sites prioritaires sans WO actif
def create_workorders_for_priority_sites(
    sb,
    vc,
    yc,
    tickets: List[Dict[str, Any]],
    workorders: List[Dict[str, Any]],
    *,
    dry: bool = False,
) -> None:
    # Récupérer l'ensemble des yuman_site_id valides dans sites_mapping
    valid_site_ids_result = sb.table("sites_mapping").select("yuman_site_id").execute()
    valid_site_ids = {row["yuman_site_id"] for row in valid_site_ids_result.data if row["yuman_site_id"] is not None}

    active_sites = {
        w["site_id"] for w in workorders if w.get("status", "").lower() != "closed"
    }

    # Tickets urgent/high regroupés par site
    by_site: Dict[int, List[Dict[str, Any]]] = {}
    for t in tickets:
        if t.get("status") == "open" and t.get("priority") in ("high", "urgent"):
            row = (
                sb.table("sites_mapping")
                .select("yuman_site_id")
                .eq("vcom_system_key", t.get("systemKey"))
                .execute()
            ).data
            if row:
                site_id = row[0]["yuman_site_id"]
                if site_id is None:
                    logger.debug(
                        "Ignoré ticket %s sans yuman_site_id dans sites_mapping",
                        t.get("id") or t.get("vcom_ticket_id"),
                    )
                    continue
                by_site.setdefault(site_id, []).append(t)

    for site_id, ts in by_site.items():
        # Ignorer les sites non présents dans sites_mapping
        if site_id not in valid_site_ids:
            logger.warning(
                "Site %s ignoré lors de création WO (yuman_site_id non présent dans sites_mapping)",
                site_id
            )
            continue

        if site_id in active_sites:
            continue  # déjà un WO actif

        # ---------------- Construction du WO -----------------
        ts.sort(key=lambda x: {"urgent": 0, "high": 1}.get(x["priority"], 2))
        title = ts[0].get("designation") or ts[0]["id"]
        description = "\n".join(
            f"{t.get('designation') or t['id']}:\n{t.get('description', '')}"
            for t in ts
        )

        # ► Récupérer client_id + address depuis les tables de mapping
        map_row = (
            sb.table("sites_mapping")
            .select("client_map_id", "address")
            .eq("yuman_site_id", site_id)
            .execute()
        ).data
        if not map_row:
            logger.error("❌ Pas de mapping trouvé pour site %s – WO non créé", site_id)
            continue

        client_map_id, address = map_row[0]["client_map_id"], map_row[0]["address"]
        if address in (None, ""):
            logger.error("❌ Address manquante pour site %s – WO non créé", site_id)
            continue

        cli_row = (
            sb.table("clients_mapping")
            .select("yuman_client_id")
            .eq("id", client_map_id)
            .execute()
        ).data
        if not cli_row or cli_row[0]["yuman_client_id"] is None:
            logger.error(
                "❌ yuman_client_id manquant pour client_map_id %s – WO non créé",
                client_map_id,
            )
            continue
        yuman_client_id = cli_row[0]["yuman_client_id"]

        payload = {
            "workorder_type": "Reactive",
            "title": title,
            "description": description,
            "client_id": yuman_client_id,
            "site_id": site_id,
            "address": address,
            "manager_id": 10338,  # Anthony - responsable des WO
            # date_planned absent → Yuman mettra « Current DateTime »
        }

        if dry:
            logger.info("[DRY] Création WO site=%s client=%s", site_id, yuman_client_id)
            continue

        # ---------------- Appel API Yuman --------------------
        try:
            res = yc.create_workorder(payload)
            wo_id = res["id"]

            # Insérer le WO en base
            sb.table("work_orders").insert(
                {
                    "workorder_id": wo_id,
                    "status": res.get("status"),
                    "client_id": res.get("client_id"),
                    "site_id": site_id,
                    "scheduled_date": res.get("date_planned"),
                    "description": res.get("description"),
                    "title": res.get("title"),
                }
            ).execute()

            # Assigner les tickets VCOM à ce WO
            for t in ts:
                vc.update_ticket(t["id"], status="assigned")
                sb.table("tickets").update(
                    {"status": "assigned", "yuman_workorder_id": wo_id}
                ).eq("vcom_ticket_id", t["id"]).execute()

            logger.info("✅ Workorder %s créé pour site %s", wo_id, site_id)
        except Exception as exc:
            logger.error("Création WO site %s KO : %s", site_id, exc)


# ---------------------------------------------------------------------------
# Étape 5 : fermer les tickets VCOM des workorders clos

def close_tickets_of_closed_workorders(
    sb, vc, workorders: List[Dict[str, Any]], *, dry: bool = False
) -> None:
    """
    Ferme les tickets VCOM dont le workorder Yuman est clôturé.

    Args:
        sb: Client Supabase
        vc: Client VCOM
        workorders: Liste des workorders Yuman (déjà récupérés)
        dry: Mode dry-run
    """
    # Filtrer les WO clôturés (comparaison case-insensitive)
    closed_wo_ids = [
        w["id"] for w in workorders
        if w.get("status", "").lower() == "closed"
    ]

    if not closed_wo_ids:
        logger.info("Aucun workorder clôturé à traiter")
        return

    logger.info("%d workorders clôturés à vérifier", len(closed_wo_ids))

    for wo_id in closed_wo_ids:
        # Vérifier le status actuel en DB
        res = (
            sb.table("work_orders")
            .select("status")
            .eq("workorder_id", wo_id)
            .execute()
        )

        # Si pas en DB ou déjà marqué closed, skip
        if not res.data:
            continue

        db_status = res.data[0].get("status", "")
        if db_status.lower() == "closed":
            continue  # déjà synchronisé

        # Récupérer les tickets liés à ce WO
        t_rows = (
            sb.table("tickets")
            .select("vcom_ticket_id, status")
            .eq("yuman_workorder_id", wo_id)
            .execute()
        )

        tickets_to_close = [
            row for row in (t_rows.data or [])
            if row.get("status", "").lower() not in ("closed", "deleted")
        ]

        if dry:
            logger.info(
                "[DRY] Clôture WO %s (status DB: %s) + %d tickets",
                wo_id, db_status, len(tickets_to_close)
            )
            continue

        # Mettre à jour le WO en DB
        sb.table("work_orders").update({
            "status": "Closed",
            "updated_at": datetime.now(timezone.utc).isoformat()
        }).eq("workorder_id", wo_id).execute()

        # Fermer chaque ticket VCOM
        for row in tickets_to_close:
            tid = row["vcom_ticket_id"]
            try:
                vc.close_ticket(tid)
                sb.table("tickets").update({
                    "status": "closed",
                    "yuman_wo_status": "Closed",
                    "last_sync_at": datetime.now(timezone.utc).isoformat()
                }).eq("vcom_ticket_id", tid).execute()
                logger.info("✅ Ticket %s fermé (WO %s clôturé)", tid, wo_id)
            except Exception as exc:
                logger.error("❌ Échec fermeture ticket %s: %s", tid, exc)

        logger.info("✅ WO %s marqué Closed + %d tickets fermés", wo_id, len(tickets_to_close))

# ---------------------------------------------------------------------------
# Orchestrateur principal

def run_tickets_sync(dry_run: bool = False) -> int:
    """
    Logique métier de synchronisation tickets VCOM ↔ workorders Yuman.

    Args:
        dry_run: Si True, pas d'écriture ni update sur les APIs/BD

    Returns:
        0 en cas de succès, 1 en cas d'erreur
    """
    # Connexions externes
    sb = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_KEY"))
    vc = VCOMAPIClient()
    yc = YumanClient(os.getenv("YUMAN_TOKEN"))

    # 1. Collecte
    tickets = collect_vcom_tickets(vc)
    workorders = collect_yuman_workorders(yc)

    # 2. DB sync
    sync_tickets_to_db(sb, tickets, dry=dry_run)
    sync_workorders_to_db(sb, workorders, dry=dry_run)

    # 3‑5. Règles métier
    assign_tickets_to_active_workorders(sb, vc, yc, workorders, dry=dry_run)
    create_workorders_for_priority_sites(sb, vc, yc, tickets, workorders, dry=dry_run)
    close_tickets_of_closed_workorders(sb, vc, workorders, dry=dry_run)

    logger.info("✅ Synchronisation terminée")
    return 0


def main() -> int:
    """Point d'entrée CLI pour le script standalone."""
    args = parse_args()
    return run_tickets_sync(dry_run=args.dry_run)


if __name__ == "__main__":
    import sys
    sys.exit(main())
