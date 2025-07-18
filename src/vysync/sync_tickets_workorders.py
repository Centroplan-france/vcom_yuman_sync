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
import textwrap
from typing import Any, Dict, List

from supabase import create_client
from vysync.app_logging import init_logger
from vysync.vcom_client import VCOMAPIClient
from vysync.yuman_client import YumanClient

# ---------------------------------------------------------------------------
# Logger global
logger = init_logger(__name__)

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
        for t in tickets
    ]

    if not rows:
        return

    if dry:
        logger.info("[DRY] %d tickets à upsert", len(rows))
    else:
        sb.table("tickets").upsert(rows, on_conflict="vcom_ticket_id").execute()
        logger.info("%d tickets upsertés", len(rows))

def upsert_workorders(sb, orders: List[Dict[str, Any]], *, dry: bool = False) -> None:
    rows = [
        {
            "workorder_id": w["id"],
            "status": w.get("status"),
            "client_id": w.get("client_id"),
            "site_id": w.get("site_id"),
            "scheduled_date": w.get("date_planned"),
            "description": w.get("description"),
            "title": w.get("title"),
        }
        for w in orders
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
    statuses = statuses or ["open", "assigned", "inProgress"]
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
            .not_.in_("status", ["assigned", "inProgress"])
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
                by_site.setdefault(site_id, []).append(t)

    for site_id, ts in by_site.items():
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
            "manager_id": 10339,  # manager fixe fourni
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

def close_tickets_of_closed_workorders(sb, vc, yc, *, dry: bool = False) -> None:
    closed_ids = [
        w["id"] for w in yc.list_workorders() if w.get("status", "").lower() == "closed"
    ]

    for wo_id in closed_ids:
        res = (
            sb.table("work_orders")
            .select("status")
            .eq("workorder_id", wo_id)
            .execute()
        )
        if not res.data or res.data[0]["status"].lower() == "closed":
            continue  # déjà synchronisé

        t_rows = (
            sb.table("tickets")
            .select("vcom_ticket_id")
            .eq("yuman_workorder_id", wo_id)
            .execute()
        )

        if dry:
            logger.info("[DRY] Clôture WO %s + %d tickets", wo_id, len(t_rows.data or []))
            continue

        sb.table("work_orders").update({"status": "closed"}).eq(
            "workorder_id", wo_id
        ).execute()

        for row in t_rows.data or []:
            tid = row["vcom_ticket_id"]
            try:
                vc.close_ticket(tid)
                sb.table("tickets").update({"status": "closed"}).eq(
                    "vcom_ticket_id", tid
                ).execute()
            except Exception as exc:
                logger.error("Échec fermeture ticket %s: %s", tid, exc)

# ---------------------------------------------------------------------------
# Orchestrateur principal

def main() -> None:
    args = parse_args()
    dry = args.dry_run

    # Connexions externes
    sb = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_KEY"))
    vc = VCOMAPIClient()
    yc = YumanClient(os.getenv("YUMAN_TOKEN"))

    # 1. Collecte
    tickets = collect_vcom_tickets(vc)
    workorders = collect_yuman_workorders(yc)

    # 2. DB sync
    sync_tickets_to_db(sb, tickets, dry=dry)
    sync_workorders_to_db(sb, workorders, dry=dry)

    # 3‑5. Règles métier
    assign_tickets_to_active_workorders(sb, vc, yc, workorders, dry=dry)
    create_workorders_for_priority_sites(sb, vc, yc, tickets, workorders, dry=dry)
    close_tickets_of_closed_workorders(sb, vc, yc, dry=dry)

    logger.info("✅ Synchronisation terminée")


if __name__ == "__main__":
    main()
