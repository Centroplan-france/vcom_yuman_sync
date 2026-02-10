#!/usr/bin/env python3
"""
sync_tickets_workorders.py - Synchronise les tickets VCOM et les workorders Yuman.

Usage:
  poetry run python -m vysync.sync_tickets_workorders [--dry-run]

Ce script execute le flux suivant :
  1. Recupere les tickets VCOM (open/assigned/inProgress) puis les upsert dans Supabase.
  2. Recupere les workorders Yuman puis les upsert dans Supabase.
     La detection des changements (status, date, technicien) et la mise a jour
     des commentaires VCOM se font directement dans l'upsert des workorders.
  3. Assigne les tickets urgent/high aux WO SAV Reactive existants ou cree de nouveaux WO.
  4. Assigne les tickets normal aux WO actifs existants (sans creation).
  5. Ferme les tickets VCOM lies aux workorders Yuman clotures.
"""
from __future__ import annotations

import argparse
import os
import logging
import textwrap
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import requests
from supabase import create_client
from vysync.vcom_client import VCOMAPIClient
from vysync.yuman_client import YumanClient

# ---------------------------------------------------------------------------
# Logger global
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constantes metier
MANAGER_ID_ANTHONY = 10338
CATEGORY_SAV_CURATIVE = 11138  # "SAV Maintenance curative"
WO_TYPE_REACTIVE = "Reactive"
WO_MAX_AGE_DAYS = 30  # Un WO avec date_planned > 30 jours dans le passe est considere obsolete

# Cache global pour les utilisateurs Yuman (initialise au debut de sync)
_users_cache: Dict[int, str] = {}

# ---------------------------------------------------------------------------
# CLI

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Synchronise tickets VCOM et workorders Yuman"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Pas d'ecriture ni update sur les APIs/BD",
    )
    return parser.parse_args()

# ---------------------------------------------------------------------------
# Helpers pour les nouvelles regles metier

def init_users_cache(yc) -> None:
    """Initialise le cache des utilisateurs Yuman."""
    global _users_cache
    try:
        _users_cache = yc.get_users_dict()
        logger.info("Cache utilisateurs Yuman initialise (%d entrees)", len(_users_cache))
    except Exception as exc:
        logger.warning("Impossible de charger les utilisateurs Yuman: %s", exc)
        _users_cache = {}


def get_technician_name(yc, tech_id: Optional[int]) -> str:
    """Recupere le nom du technicien depuis le cache ou l'API."""
    if tech_id is None:
        return "Non assigne"
    if tech_id in _users_cache:
        return _users_cache[tech_id]
    # Fallback: essayer de recuperer depuis l'API
    try:
        users = yc.get_users_dict()
        _users_cache.update(users)
        return _users_cache.get(tech_id, f"Technicien #{tech_id}")
    except Exception:
        return f"Technicien #{tech_id}"


def format_date(date_str: Optional[str]) -> str:
    """Formate une date ISO en format francais."""
    if not date_str:
        return "N/A"
    try:
        if isinstance(date_str, str):
            # Parse ISO format
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        else:
            dt = date_str
        return dt.strftime("%d/%m/%Y")
    except (ValueError, TypeError):
        return str(date_str)[:10]


def parse_date(date_str: Optional[str]) -> Optional[datetime]:
    """Parse une date ISO en datetime."""
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def _dates_equal(date1, date2) -> bool:
    """
    Compare deux dates en les normalisant en UTC.

    Gere les formats :
    - String ISO : "2026-02-11T10:00:00.000+01:00"
    - String PostgreSQL : "2026-02-11 09:00:00+00"
    - datetime object
    - None

    Returns:
        True si les deux dates representent le meme instant (ou les deux sont None)
    """
    if date1 is None and date2 is None:
        return True
    if date1 is None or date2 is None:
        return False

    def to_datetime(d):
        if isinstance(d, datetime):
            return d
        if isinstance(d, str):
            # Normaliser le format PostgreSQL (espace -> T)
            normalized = d.replace(" ", "T")
            # Gerer le +00 court
            if normalized.endswith("+00"):
                normalized += ":00"
            try:
                return datetime.fromisoformat(normalized)
            except ValueError:
                return None
        return None

    dt1 = to_datetime(date1)
    dt2 = to_datetime(date2)

    if dt1 is None or dt2 is None:
        # Fallback: comparaison string
        return str(date1) == str(date2)

    return dt1 == dt2


# ---------------------------------------------------------------------------
# Helpers pour l'historique des workorders


def _get_technician_name_from_cache(tech_id: Optional[int]) -> str:
    """Recupere le nom du technicien depuis le cache global."""
    if tech_id is None:
        return "Non assigné"
    return _users_cache.get(tech_id, f"Technicien #{tech_id}")




def find_best_workorder(
    workorders: List[Dict[str, Any]],
    site_id: int,
    *,
    require_sav_reactive: bool = False,
) -> Optional[Dict[str, Any]]:
    """
    Trouve le meilleur workorder sur un site selon les regles metier.

    Args:
        workorders: Liste des workorders Yuman
        site_id: ID du site Yuman
        require_sav_reactive: Si True, filtre sur category_id=11138 et workorder_type=Reactive

    Returns:
        Le workorder le plus approprie ou None
    """
    now = datetime.now(timezone.utc)
    cutoff_date = now - timedelta(days=WO_MAX_AGE_DAYS)

    # Filtrer les WO du site
    candidates = []
    for w in workorders:
        if w.get("site_id") != site_id:
            continue

        # Exclure les WO clotures
        if w.get("status", "").lower() == "closed":
            continue

        # Si on cherche un WO SAV Reactive
        if require_sav_reactive:
            if w.get("category_id") != CATEGORY_SAV_CURATIVE:
                continue
            if w.get("workorder_type") != WO_TYPE_REACTIVE:
                continue

            # Verifier que le WO n'est pas trop vieux
            date_planned = parse_date(w.get("date_planned"))
            if date_planned and date_planned < cutoff_date:
                continue

        candidates.append(w)

    if not candidates:
        return None

    # Trier : date_planned la plus proche dans le futur, sinon la plus recente dans le passe
    def sort_key(w: Dict[str, Any]) -> tuple:
        date_planned = parse_date(w.get("date_planned"))
        if not date_planned:
            return (2, datetime.max)  # Pas de date -> en dernier
        if date_planned >= now:
            return (0, date_planned)  # Dans le futur -> priorite 0, trie par date croissante
        else:
            return (1, -date_planned.timestamp())  # Dans le passe -> priorite 1, trie par date decroissante (plus recent d'abord)

    candidates.sort(key=sort_key)
    return candidates[0]


def enrich_workorder_description(
    yc,
    wo: Dict[str, Any],
    tickets: List[Dict[str, Any]],
    *,
    dry: bool = False,
) -> bool:
    """
    Enrichit la description d'un workorder avec les informations des tickets.

    Returns:
        True si l'update a reussi, False sinon
    """
    if not tickets:
        return True

    additional = "".join(
        f"\n\n--- Ticket VCOM ---\n{t.get('title') or t.get('designation') or t.get('vcom_ticket_id')}:\n{textwrap.fill(t.get('description', '') or '', width=80)}"
        for t in tickets
    )
    new_desc = (wo.get("description") or "") + additional

    if dry:
        logger.info("[DRY] Enrichissement WO %s avec %d ticket(s)", wo["id"], len(tickets))
        return True

    try:
        yc.update_workorder(wo["id"], {"description": new_desc})
        logger.info("Workorder %s enrichi avec %d ticket(s)", wo["id"], len(tickets))
        return True
    except Exception as exc:
        logger.error("Echec enrichissement workorder %s: %s", wo["id"], exc, exc_info=True)
        return False


# ---------------------------------------------------------------------------
# Helpers pour les commentaires VYSYNC



def post_report_comment(vc, yc, ticket: Dict[str, Any], wo: Dict[str, Any], *, dry: bool = False) -> None:
    """Poste le rapport d'intervention a la cloture."""
    ticket_id = ticket["vcom_ticket_id"]

    report = wo.get("report", "")
    if not report:
        return  # Pas de rapport a poster

    tech_name = get_technician_name(yc, wo.get("technician_id"))
    time_taken = wo.get("time_taken", 0)
    date_done = format_date(wo.get("date_done"))

    content = f"""[VYSYNC] Rapport d'intervention du {date_done}

Technicien : {tech_name}
Duree : {time_taken} minutes

{report}"""

    if not dry:
        try:
            vc.create_ticket_comment(ticket_id, content)
            logger.info("Rapport poste pour ticket %s", ticket_id)
        except requests.RequestException as exc:
            logger.error("Echec post rapport ticket %s: %s", ticket_id, exc, exc_info=True)


# ---------------------------------------------------------------------------
# Helpers d'upsert (DB - Supabase)

def upsert_tickets(sb, tickets: List[Dict[str, Any]], *, dry: bool = False) -> None:
    # Recuperer l'ensemble des vcom_system_key valides dans sites_mapping
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

    # Logger les tickets ignores
    if ignored_tickets:
        ignored_ids = [t.get("id") for t in ignored_tickets]
        logger.warning(
            "%d tickets ignores (system_key non present dans sites_mapping): %s",
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
            "vcom_created_at": t.get("createdAt"),
            "vcom_rectified_at": t.get("rectifiedAt"),
        }
        for t in valid_tickets
    ]

    if not rows:
        return

    if dry:
        logger.info("[DRY] %d tickets a upsert", len(rows))
    else:
        sb.table("tickets").upsert(rows, on_conflict="vcom_ticket_id").execute()
        logger.info("%d tickets upsertes", len(rows))


def upsert_workorders(sb, yc, vc, orders: List[Dict[str, Any]], *, dry: bool = False) -> None:
    """
    Met a jour les workorders en base et gere l'historique.

    Pour chaque WO de l'API Yuman :
    1. Si nouveau -> INSERT avec wo_history initialise
    2. Si existant -> comparer et detecter les changements
       - Si changement -> UPDATE + append wo_history + update commentaire VCOM
       - Sinon -> ne rien faire
    """
    # Recuperer l'ensemble des yuman_site_id valides dans sites_mapping
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

    # Logger les workorders ignores
    if ignored_orders:
        ignored_ids = [w.get("id") for w in ignored_orders]
        logger.warning(
            "%d work_orders ignores (site_id non present dans sites_mapping): %s",
            len(ignored_orders),
            ignored_ids
        )

    if not valid_orders:
        return

    # Recuperer les WO existants avec leurs valeurs actuelles
    existing_wo_result = sb.table("work_orders").select(
        "workorder_id, status, date_planned, technician_id, wo_history"
    ).execute()

    existing_wo_map = {
        row["workorder_id"]: row
        for row in existing_wo_result.data
    }

    # Recuperer les tickets lies aux WO (pour poster les commentaires VCOM)
    tickets_result = sb.table("tickets").select(
        "vcom_ticket_id, yuman_workorder_id, vcom_comment_id"
    ).not_.is_("yuman_workorder_id", "null").execute()

    tickets_by_wo = {}
    for t in tickets_result.data:
        wo_id = t["yuman_workorder_id"]
        tickets_by_wo.setdefault(wo_id, []).append(t)

    rows_to_upsert = []

    for w in valid_orders:
        wo_id = w["id"]
        existing = existing_wo_map.get(wo_id)

        # Valeurs de l'API
        new_status = w.get("status") or "Open"
        new_date_planned = w.get("date_planned")
        new_technician = w.get("technician_id")

        # Convertir predicted_duration en int (peut etre float dans l'API)
        predicted_duration = w.get("predicted_duration")
        if predicted_duration is not None:
            predicted_duration = int(predicted_duration)

        # Construire la row de base
        row = {
            "workorder_id": wo_id,
            "status": new_status,
            "client_id": w.get("client_id"),
            "site_id": w.get("site_id"),
            "date_planned": new_date_planned,
            "description": w.get("description"),
            "title": w.get("title"),
            "category_id": w.get("category_id"),
            "workorder_type": w.get("workorder_type"),
            "technician_id": new_technician,
            "manager_id": w.get("manager_id"),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "yuman_created_at": w.get("created_at"),
            "date_done": w.get("date_done"),
            "predicted_duration": predicted_duration,
            "time_taken": w.get("time_taken"),
        }

        if existing is None:
            # ===============================================================
            # NOUVEAU WO -> Initialiser wo_history
            # ===============================================================
            yuman_created_at = w.get("created_at") or datetime.now(timezone.utc).isoformat()

            # Regle metier : si status = "Open", planned_at = null
            initial_planned_at = None if new_status == "Open" else new_date_planned

            row["source"] = "yuman_manual"
            row["wo_history"] = [{
                "status": new_status,
                "planned_at": initial_planned_at,
                "technician_id": new_technician,
                "changed_at": yuman_created_at
            }]

            logger.info("Nouveau WO %s detecte (status=%s)", wo_id, new_status)

        else:
            # ===============================================================
            # WO EXISTANT -> Detecter les changements
            # ===============================================================
            old_status = existing.get("status") or ""
            old_date_planned = existing.get("date_planned")
            old_technician = existing.get("technician_id")

            # Comparer les valeurs (normalisation)
            status_changed = old_status.lower() != new_status.lower()
            date_changed = not _dates_equal(old_date_planned, new_date_planned)
            tech_changed = old_technician != new_technician

            if status_changed or date_changed or tech_changed:
                # -----------------------------------------------------------
                # CHANGEMENT DETECTE
                # -----------------------------------------------------------

                # Regle metier : si status = "Open", planned_at = null dans l'historique
                history_planned_at = None if new_status == "Open" else new_date_planned

                # Recuperer l'historique existant et ajouter la nouvelle entree
                wo_history = existing.get("wo_history") or []
                wo_history.append({
                    "status": new_status,
                    "planned_at": history_planned_at,
                    "technician_id": new_technician,
                    "changed_at": datetime.now(timezone.utc).isoformat()
                })
                row["wo_history"] = wo_history

                logger.info(
                    "Changement WO %s: status=%s->%s, date=%s->%s, tech=%s->%s",
                    wo_id, old_status, new_status, old_date_planned, new_date_planned, old_technician, new_technician
                )

                # -----------------------------------------------------------
                # METTRE A JOUR LE COMMENTAIRE VCOM (si ticket associe)
                # -----------------------------------------------------------
                if wo_id in tickets_by_wo and not dry:
                    _update_vcom_comments_for_wo(sb, vc, wo_id, w, wo_history, tickets_by_wo[wo_id])

                    # Poster le rapport d'intervention si cloture
                    if status_changed and new_status.lower() == "closed":
                        for ticket in tickets_by_wo[wo_id]:
                            post_report_comment(vc, yc, ticket, w)

            else:
                # Aucun changement detecte -> ne pas upsert ce WO
                # (sinon le batch upsert ecraserait wo_history avec NULL)
                continue

        rows_to_upsert.append(row)

    # Upsert en batch
    if rows_to_upsert:
        if dry:
            new_count = sum(1 for w in valid_orders if w["id"] not in existing_wo_map)
            logger.info("[DRY] %d workorders a upsert (%d nouveaux)", len(rows_to_upsert), new_count)
        else:
            sb.table("work_orders").upsert(rows_to_upsert, on_conflict="workorder_id").execute()
            new_count = sum(1 for w in valid_orders if w["id"] not in existing_wo_map)
            logger.info("%d workorders upsertes (%d nouveaux)", len(rows_to_upsert), new_count)


# ---------------------------------------------------------------------------
# Helpers pour la mise a jour des commentaires VCOM depuis wo_history

def _update_vcom_comments_for_wo(sb, vc, wo_id: int, wo: Dict, wo_history: list, tickets: List[Dict]) -> None:
    """
    Met a jour ou cree le commentaire VYSYNC pour tous les tickets lies a un WO.

    Le commentaire est genere depuis wo_history et remplace l'ancien commentaire.
    """
    wo_number = wo.get("number", wo_id)

    # Generer le contenu du commentaire depuis wo_history
    comment_content = _format_wo_history_as_comment(wo_number, wo_history)

    for ticket in tickets:
        ticket_id = ticket["vcom_ticket_id"]
        existing_comment_id = ticket.get("vcom_comment_id")

        try:
            if existing_comment_id:
                # Mettre a jour le commentaire existant
                vc.update_ticket_comment(ticket_id, existing_comment_id, comment_content)
                logger.info("Commentaire VYSYNC mis a jour pour ticket %s", ticket_id)
            else:
                # Creer un nouveau commentaire
                comment_id = vc.create_ticket_comment(ticket_id, comment_content)
                if comment_id:
                    sb.table("tickets").update({
                        "vcom_comment_id": comment_id,
                        "last_sync_at": datetime.now(timezone.utc).isoformat()
                    }).eq("vcom_ticket_id", ticket_id).execute()
                logger.info("Commentaire VYSYNC cree pour ticket %s", ticket_id)
        except requests.RequestException as exc:
            logger.error("Echec commentaire VCOM ticket %s: %s", ticket_id, exc, exc_info=True)


def _format_wo_history_as_comment(wo_number, history: list) -> str:
    """
    Formate l'historique complet du WO pour un commentaire VCOM lisible.

    Format de sortie :
    [VYSYNC] Historique du WO #01216

    04/02/2026 :
      * Intervention demarree

    30/01/2026 :
      * Intervention planifiee : 05/02/2026

    29/01/2026 :
      * WO attribue a : Hadj
      * Intervention planifiee : 03/02/2026

    18/01/2026 :
      * WO ouvert
    """
    if not history:
        return f"[VYSYNC] Historique du WO #{wo_number}\n\n(Aucun historique)"

    lines = [f"[VYSYNC] Historique du WO #{wo_number}", ""]

    # Grouper les entrees par date (jour)
    entries_by_date = {}
    prev_entry = None

    for entry in history:
        changed_at = entry.get("changed_at")
        if not changed_at:
            continue

        # Extraire la date (jour uniquement)
        try:
            dt = datetime.fromisoformat(changed_at.replace("Z", "+00:00").replace(" ", "T"))
            date_key = dt.strftime("%d/%m/%Y")
        except (ValueError, TypeError):
            date_key = changed_at[:10]

        # Determiner les changements par rapport a l'entree precedente
        changes = []
        status = entry.get("status")
        planned_at = entry.get("planned_at")
        technician_id = entry.get("technician_id")

        if prev_entry is None:
            # Premiere entree = creation
            changes.append(f"WO ouvert ({status})")
            if technician_id is not None:
                tech_name = _get_technician_name_from_cache(technician_id)
                changes.append(f"WO attribue a : {tech_name}")
            if planned_at:
                changes.append(f"Intervention planifiee : {_format_date(planned_at)}")
        else:
            # Entrees suivantes = changements
            if status != prev_entry.get("status"):
                if status and status.lower() == "closed":
                    changes.append("WO cloture")
                elif status and status.lower() == "in progress":
                    changes.append("Intervention demarree")
                elif status and status.lower() == "scheduled":
                    changes.append("WO planifie")
                else:
                    changes.append(f"Statut : {status}")

            if technician_id != prev_entry.get("technician_id") and technician_id is not None:
                tech_name = _get_technician_name_from_cache(technician_id)
                changes.append(f"WO attribue a : {tech_name}")

            if planned_at != prev_entry.get("planned_at") and planned_at:
                if prev_entry.get("planned_at"):
                    changes.append(f"Reporte au : {_format_date(planned_at)}")
                else:
                    changes.append(f"Intervention planifiee : {_format_date(planned_at)}")

        if changes:
            entries_by_date.setdefault(date_key, []).extend(changes)

        prev_entry = entry

    # Formater les entrees (du plus recent au plus ancien)
    sorted_dates = sorted(entries_by_date.keys(), key=lambda d: datetime.strptime(d, "%d/%m/%Y"), reverse=True)

    for date_key in sorted_dates:
        lines.append(f"{date_key} :")
        for change in entries_by_date[date_key]:
            lines.append(f"  * {change}")
        lines.append("")

    return "\n".join(lines).strip()


def _format_date(date_str) -> str:
    """Formate une date ISO en format francais (DD/MM/YYYY)."""
    if not date_str:
        return "N/A"
    try:
        if isinstance(date_str, str):
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00").replace(" ", "T"))
        else:
            dt = date_str
        return dt.strftime("%d/%m/%Y")
    except (ValueError, TypeError):
        return str(date_str)[:10]


# ---------------------------------------------------------------------------
# Etape 1 : collecte des tickets VCOM

def collect_vcom_tickets(vc, statuses: List[str] | None = None) -> List[Dict[str, Any]]:
    statuses = statuses or ["open", "assigned", "inProgress", "closed"]
    tickets: List[Dict[str, Any]] = []
    for st in statuses:
        try:
            chunk = vc.get_tickets(status=st)
            tickets.extend(chunk)
            logger.info("VCOM: %d tickets recuperes (status=%s)", len(chunk), st)
        except requests.RequestException as exc:
            logger.error("Erreur recuperation tickets VCOM (%s): %s", st, exc, exc_info=True)
    return tickets


# ---------------------------------------------------------------------------
# Etape 2 : collecte des workorders Yuman

def collect_yuman_workorders(yc) -> List[Dict[str, Any]]:
    try:
        data = yc.list_workorders()
        logger.info("YUMAN: %d workorders recuperes", len(data))
        return data
    except requests.RequestException as exc:
        logger.error("Erreur recuperation workorders Yuman: %s", exc, exc_info=True)
        return []


# ---------------------------------------------------------------------------
# Etapes 1-bis & 2-bis : synchronisation DB

def sync_tickets_to_db(sb, tickets, *, dry: bool = False) -> None:
    upsert_tickets(sb, tickets, dry=dry)


def sync_workorders_to_db(sb, yc, vc, workorders, *, dry: bool = False) -> None:
    upsert_workorders(sb, yc, vc, workorders, dry=dry)


# ---------------------------------------------------------------------------
# Etape 3a : Assigner les tickets urgent/high

def assign_urgent_high_tickets(
    sb, vc, yc, tickets: List[Dict[str, Any]], workorders: List[Dict[str, Any]], *, dry: bool = False
) -> None:
    """
    Assigne les tickets urgent/high aux WO SAV Reactive existants ou cree de nouveaux WO.

    Regle 1 : Tickets urgent/high
    - Cherche un WO SAV Reactive sur le site (category_id=11138, workorder_type=Reactive)
    - Si trouve : enrichit et assigne
    - Sinon : cree un nouveau WO Reactive
    """
    # Filtrer les tickets urgent/high open
    priority_tickets = [
        t for t in tickets
        if t.get("status") == "open" and t.get("priority") in ("urgent", "high")
    ]

    if not priority_tickets:
        logger.info("Aucun ticket urgent/high a traiter")
        return

    logger.info("%d tickets urgent/high a traiter", len(priority_tickets))

    # Regrouper par site
    by_site: Dict[int, List[Dict[str, Any]]] = {}
    for t in priority_tickets:
        system_key = t.get("systemKey")
        if not system_key:
            continue

        # Recuperer le site_id depuis le mapping
        row = (
            sb.table("sites_mapping")
            .select("yuman_site_id")
            .eq("vcom_system_key", system_key)
            .execute()
        ).data
        if not row or row[0]["yuman_site_id"] is None:
            logger.debug("Ticket %s ignore - pas de mapping site", t.get("id"))
            continue

        site_id = row[0]["yuman_site_id"]
        by_site.setdefault(site_id, []).append(t)

    for site_id, site_tickets in by_site.items():
        # Chercher un WO SAV Reactive eligible
        wo = find_best_workorder(workorders, site_id, require_sav_reactive=True)

        if wo:
            # WO trouve -> enrichir et assigner
            logger.info("WO SAV Reactive trouve pour site %s: WO #%s", site_id, wo["id"])

            # Enrichir la description du WO
            enrich_workorder_description(yc, wo, site_tickets, dry=dry)

            # Marquer les tickets comme assigned
            for t in site_tickets:
                tid = t.get("id") or t.get("vcom_ticket_id")
                if dry:
                    logger.info("[DRY] Ticket %s -> assigned (WO %s)", tid, wo["id"])
                    continue
                try:
                    vc.update_ticket(tid, status="assigned")
                    sb.table("tickets").update({
                        "status": "assigned",
                        "yuman_workorder_id": wo["id"],
                        "last_sync_at": datetime.now(timezone.utc).isoformat()
                    }).eq("vcom_ticket_id", tid).execute()
                    logger.info("Ticket %s assigne au WO %s", tid, wo["id"])
                except Exception as exc:
                    logger.error("Echec assignation ticket %s: %s", tid, exc, exc_info=True)
        else:
            # Aucun WO SAV Reactive eligible -> creer un nouveau WO
            _create_new_workorder_for_tickets(sb, vc, yc, site_id, site_tickets, dry=dry)


def _create_new_workorder_for_tickets(
    sb, vc, yc, site_id: int, tickets: List[Dict[str, Any]], *, dry: bool = False
) -> None:
    """Cree un nouveau WO Reactive pour un site avec des tickets prioritaires."""
    # Trier par priorite (urgent d'abord)
    tickets.sort(key=lambda x: {"urgent": 0, "high": 1}.get(x.get("priority", ""), 2))

    # Recuperer les infos du site depuis le mapping
    map_row = (
        sb.table("sites_mapping")
        .select("client_map_id, address, yuman_site_id")
        .eq("yuman_site_id", site_id)
        .execute()
    ).data
    if not map_row:
        logger.error("Pas de mapping trouve pour site %s - WO non cree", site_id)
        return

    client_map_id = map_row[0]["client_map_id"]
    address = map_row[0]["address"]

    if not address:
        logger.error("Address manquante pour site %s - WO non cree", site_id)
        return

    # Recuperer le yuman_client_id
    cli_row = (
        sb.table("clients_mapping")
        .select("yuman_client_id")
        .eq("id", client_map_id)
        .execute()
    ).data
    if not cli_row or cli_row[0]["yuman_client_id"] is None:
        logger.error("yuman_client_id manquant pour client_map_id %s - WO non cree", client_map_id)
        return

    yuman_client_id = cli_row[0]["yuman_client_id"]

    # Construire le payload
    title = tickets[0].get("designation") or tickets[0].get("id") or "Ticket VCOM"
    description = "\n".join(
        f"{t.get('designation') or t.get('id')}:\n{t.get('description', '')}"
        for t in tickets
    )

    payload = {
        "workorder_type": WO_TYPE_REACTIVE,
        "category_id": CATEGORY_SAV_CURATIVE,
        "title": title,
        "description": description,
        "client_id": yuman_client_id,
        "site_id": site_id,
        "address": address,
        "manager_id": MANAGER_ID_ANTHONY,
        # technician_id absent -> non assigne
    }

    if dry:
        logger.info("[DRY] Creation WO Reactive pour site %s (client %s)", site_id, yuman_client_id)
        return

    try:
        res = yc.create_workorder(payload)
        wo_id = res["id"]
        wo_number = res.get("number", wo_id)

        # Initialiser wo_history avec l'entree de creation
        initial_status = res.get("status") or "Open"
        yuman_created_at = res.get("created_at") or datetime.now(timezone.utc).isoformat()

        initial_wo_history = [{
            "status": initial_status,
            "planned_at": None,  # Toujours null pour un WO Open cree par vysync
            "technician_id": None,
            "changed_at": yuman_created_at
        }]

        # Inserer le WO en base avec wo_history initialise
        sb.table("work_orders").insert({
            "workorder_id": wo_id,
            "status": initial_status,
            "client_id": res.get("client_id"),
            "site_id": site_id,
            "date_planned": res.get("date_planned"),
            "description": res.get("description"),
            "title": res.get("title"),
            "category_id": CATEGORY_SAV_CURATIVE,
            "workorder_type": WO_TYPE_REACTIVE,
            "manager_id": MANAGER_ID_ANTHONY,
            "yuman_created_at": yuman_created_at,
            "wo_history": initial_wo_history,
            "source": "vysync",
        }).execute()

        # Assigner les tickets a ce WO
        for t in tickets:
            tid = t.get("id") or t.get("vcom_ticket_id")
            try:
                vc.update_ticket(tid, status="assigned")
                sb.table("tickets").update({
                    "status": "assigned",
                    "yuman_workorder_id": wo_id,
                    "last_sync_at": datetime.now(timezone.utc).isoformat()
                }).eq("vcom_ticket_id", tid).execute()
            except Exception as exc:
                logger.error("Echec assignation ticket %s: %s", tid, exc, exc_info=True)

        logger.info("Workorder #%s cree pour site %s (%d tickets)", wo_number, site_id, len(tickets))

    except Exception as exc:
        logger.error("Creation WO site %s KO: %s", site_id, exc, exc_info=True)


# ---------------------------------------------------------------------------
# Etape 3b : Assigner les tickets normal

def assign_normal_tickets(
    sb, vc, yc, tickets: List[Dict[str, Any]], workorders: List[Dict[str, Any]], *, dry: bool = False
) -> None:
    """
    Assigne les tickets normal aux WO actifs existants.

    Regle 2 : Tickets normal
    - Cherche n'importe quel WO actif sur le site (peu importe category_id ou workorder_type)
    - Si trouve : enrichit et assigne
    - Sinon : ignore le ticket (reste open)
    """
    # Filtrer les tickets normal open
    normal_tickets = [
        t for t in tickets
        if t.get("status") == "open" and t.get("priority") == "normal"
    ]

    if not normal_tickets:
        logger.info("Aucun ticket normal a traiter")
        return

    logger.info("%d tickets normal a traiter", len(normal_tickets))

    # Regrouper par site
    by_site: Dict[int, List[Dict[str, Any]]] = {}
    for t in normal_tickets:
        system_key = t.get("systemKey")
        if not system_key:
            continue

        # Recuperer le site_id depuis le mapping
        row = (
            sb.table("sites_mapping")
            .select("yuman_site_id")
            .eq("vcom_system_key", system_key)
            .execute()
        ).data
        if not row or row[0]["yuman_site_id"] is None:
            logger.debug("Ticket %s ignore - pas de mapping site", t.get("id"))
            continue

        site_id = row[0]["yuman_site_id"]
        by_site.setdefault(site_id, []).append(t)

    for site_id, site_tickets in by_site.items():
        # Chercher n'importe quel WO actif sur le site
        wo = find_best_workorder(workorders, site_id, require_sav_reactive=False)

        if wo:
            # WO trouve -> enrichir et assigner
            logger.info("WO actif trouve pour site %s: WO #%s", site_id, wo["id"])

            # Enrichir la description du WO
            enrich_workorder_description(yc, wo, site_tickets, dry=dry)

            # Marquer les tickets comme assigned
            for t in site_tickets:
                tid = t.get("id") or t.get("vcom_ticket_id")
                if dry:
                    logger.info("[DRY] Ticket %s (normal) -> assigned (WO %s)", tid, wo["id"])
                    continue
                try:
                    vc.update_ticket(tid, status="assigned")
                    sb.table("tickets").update({
                        "status": "assigned",
                        "yuman_workorder_id": wo["id"],
                        "last_sync_at": datetime.now(timezone.utc).isoformat()
                    }).eq("vcom_ticket_id", tid).execute()
                    logger.info("Ticket %s (normal) assigne au WO %s", tid, wo["id"])
                except Exception as exc:
                    logger.error("Echec assignation ticket %s: %s", tid, exc, exc_info=True)
        else:
            # Aucun WO actif -> ignorer les tickets
            for t in site_tickets:
                tid = t.get("id") or t.get("vcom_ticket_id")
                logger.info("Ticket %s (normal) ignore - aucun WO actif sur site %s", tid, site_id)


# ---------------------------------------------------------------------------
# Etape 4 : fermer les tickets VCOM des workorders clos

def close_tickets_of_closed_workorders(
    sb, vc, workorders: List[Dict[str, Any]], *, dry: bool = False
) -> None:
    """
    Ferme les tickets VCOM dont le workorder Yuman est cloture.

    Args:
        sb: Client Supabase
        vc: Client VCOM
        workorders: Liste des workorders Yuman (deja recuperes)
        dry: Mode dry-run
    """
    # Filtrer les WO clotures (comparaison case-insensitive)
    closed_wo_ids = [
        w["id"] for w in workorders
        if w.get("status", "").lower() == "closed"
    ]

    if not closed_wo_ids:
        logger.info("Aucun workorder cloture a traiter")
        return

    logger.info("%d workorders clotures a verifier", len(closed_wo_ids))

    for wo_id in closed_wo_ids:
        # Verifier le status actuel en DB
        res = (
            sb.table("work_orders")
            .select("status")
            .eq("workorder_id", wo_id)
            .execute()
        )

        # Si pas en DB ou deja marque closed, skip
        if not res.data:
            continue

        db_status = res.data[0].get("status", "")
        if db_status.lower() == "closed":
            continue  # deja synchronise

        # Recuperer les tickets lies a ce WO
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
                "[DRY] Cloture WO %s (status DB: %s) + %d tickets",
                wo_id, db_status, len(tickets_to_close)
            )
            continue

        # Mettre a jour le WO en DB
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
                    "last_sync_at": datetime.now(timezone.utc).isoformat()
                }).eq("vcom_ticket_id", tid).execute()
                logger.info("Ticket %s ferme (WO %s cloture)", tid, wo_id)
            except Exception as exc:
                logger.error("Echec fermeture ticket %s: %s", tid, exc, exc_info=True)

        logger.info("WO %s marque Closed + %d tickets fermes", wo_id, len(tickets_to_close))


# ---------------------------------------------------------------------------
# Orchestrateur principal

def run_tickets_sync(dry_run: bool = False) -> int:
    """
    Logique metier de synchronisation tickets VCOM - workorders Yuman.

    Args:
        dry_run: Si True, pas d'ecriture ni update sur les APIs/BD

    Returns:
        0 en cas de succes, 1 en cas d'erreur
    """
    # Connexions externes
    sb = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_KEY"))
    vc = VCOMAPIClient()
    yc = YumanClient(os.getenv("YUMAN_TOKEN"))

    # 0. Initialiser le cache des utilisateurs Yuman
    logger.info("=== Etape 0 : Initialisation cache utilisateurs ===")
    init_users_cache(yc)

    # 1. Collecte des donnees
    logger.info("=== Etape 1 : Collecte des donnees ===")
    tickets = collect_vcom_tickets(vc)
    workorders = collect_yuman_workorders(yc)

    # 2. Sync vers DB (inclut detection des changements et mise a jour commentaires VCOM)
    logger.info("=== Etape 2 : Synchronisation DB ===")
    sync_tickets_to_db(sb, tickets, dry=dry_run)
    sync_workorders_to_db(sb, yc, vc, workorders, dry=dry_run)

    # 3. Assignation des tickets selon nouvelles regles
    logger.info("=== Etape 3 : Assignation des tickets ===")
    assign_urgent_high_tickets(sb, vc, yc, tickets, workorders, dry=dry_run)
    assign_normal_tickets(sb, vc, yc, tickets, workorders, dry=dry_run)
    # Note: tickets "low" sont ignores

    # 4. Fermeture des tickets dont le WO est cloture
    logger.info("=== Etape 4 : Fermeture des tickets ===")
    close_tickets_of_closed_workorders(sb, vc, workorders, dry=dry_run)

    logger.info("Synchronisation terminee")
    return 0


def main() -> int:
    """Point d'entree CLI pour le script standalone."""
    args = parse_args()
    return run_tickets_sync(dry_run=args.dry_run)


if __name__ == "__main__":
    import sys
    sys.exit(main())
