#!/usr/bin/env python3
"""
sync_tickets_workorders.py - Synchronise les tickets VCOM et les workorders Yuman.

Usage:
  poetry run python -m vysync.sync_tickets_workorders [--dry-run]

Ce script execute le flux suivant :
  1. Recupere les tickets VCOM (open/assigned/inProgress) puis les upsert dans Supabase.
  2. Recupere les workorders Yuman puis les upsert dans Supabase.
  3. Assigne les tickets urgent/high aux WO SAV Reactive existants ou cree de nouveaux WO.
  4. Assigne les tickets normal aux WO actifs existants (sans creation).
  5. Synchronise les changements WO vers les tickets (commentaires VYSYNC).
  6. Ferme les tickets VCOM lies aux workorders Yuman clotures.
"""
from __future__ import annotations

import argparse
import os
import logging
import textwrap
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

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
    except Exception:
        return str(date_str)[:10]


def parse_date(date_str: Optional[str]) -> Optional[datetime]:
    """Parse une date ISO en datetime."""
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except Exception:
        return None


def dates_are_equal(date1: Optional[str], date2: Optional[str]) -> bool:
    """
    Compare deux dates ISO en les normalisant en UTC.

    Gere les cas ou les dates sont dans des fuseaux horaires differents
    (ex: UTC +00:00 vs Paris +01:00).

    Returns:
        True si les deux dates representent le meme instant, ou si les deux sont None.
        False sinon.
    """
    if date1 is None and date2 is None:
        return True
    if date1 is None or date2 is None:
        return False

    dt1 = parse_date(date1)
    dt2 = parse_date(date2)

    if dt1 is None or dt2 is None:
        # Fallback: comparaison string si parse echoue
        return date1 == date2

    # Comparer les timestamps UTC
    return dt1 == dt2


# ---------------------------------------------------------------------------
# Helpers pour l'historique des workorders

def append_wo_history(sb, workorder_id: int, status: str, planned_at: Optional[str], technician_id: Optional[int] = None) -> None:
    """
    Ajoute une entrée dans l'historique du workorder.

    Format JSON: [{status, planned_at, technician_id, changed_at}, ...]
    - status: statut du WO (Open, Scheduled, In progress, Closed)
    - planned_at: date planifiée (null si pas encore planifié)
    - technician_id: ID du technicien assigné (null si non assigné)
    - changed_at: timestamp du changement détecté
    """
    try:
        # Récupérer l'historique actuel
        wo_record = sb.table("work_orders").select("wo_history").eq("workorder_id", workorder_id).single().execute()

        history = wo_record.data.get("wo_history") or []

        # Ajouter la nouvelle entrée
        history.append({
            "status": status,
            "planned_at": planned_at,
            "technician_id": technician_id,
            "changed_at": datetime.now(timezone.utc).isoformat()
        })

        # Mettre à jour
        sb.table("work_orders").update({
            "wo_history": history
        }).eq("workorder_id", workorder_id).execute()

        logger.info("Historique WO mis à jour pour WO %s: status=%s, planned_at=%s, technician_id=%s", workorder_id, status, planned_at, technician_id)

    except Exception as exc:
        logger.error("Echec mise à jour historique WO %s: %s", workorder_id, exc)


def _get_technician_name_from_cache(tech_id: Optional[int]) -> str:
    """Recupere le nom du technicien depuis le cache global."""
    if tech_id is None:
        return "Non assigné"
    return _users_cache.get(tech_id, f"Technicien #{tech_id}")


def format_wo_history_for_comment(history: list) -> str:
    """
    Formate l'historique du workorder pour un commentaire VCOM lisible.

    Gère à la fois la nouvelle structure (status, planned_at, technician_id, changed_at)
    et l'ancienne structure (from, to, changed_at) pour compatibilité.

    Exemple de sortie:
    📅 Historique du workorder :
      • 15/01/2026 : Créé (Open)
      • 15/01/2026 : Planifié pour le 11/02/2026, assigné à Hadj
      • 10/02/2026 : Réassigné à Anthony
      • 18/02/2026 : Intervention démarrée
      • 18/02/2026 : Clôturé
    """
    if not history:
        return ""

    lines = ["", "📅 Historique du workorder :"]

    prev_status = None
    prev_planned_at = None
    prev_technician_id = None

    for i, entry in enumerate(history):
        changed_at = format_date(entry.get("changed_at"))

        # Détecter si c'est l'ancien format (from/to) ou le nouveau (status/planned_at)
        if "status" in entry:
            # Nouveau format
            status = entry.get("status")
            planned_at = entry.get("planned_at")
            technician_id = entry.get("technician_id")

            if i == 0:
                # Première entrée = création
                parts = [f"Créé ({status})"]
                if planned_at:
                    parts.append(f"planifié pour le {format_date(planned_at)}")
                if technician_id is not None:
                    tech_name = _get_technician_name_from_cache(technician_id)
                    parts.append(f"assigné à {tech_name}")
                lines.append(f"  • {changed_at} : {', '.join(parts)}")
            else:
                # Entrées suivantes = changements
                status_changed = status != prev_status
                date_changed = planned_at != prev_planned_at
                tech_changed = technician_id != prev_technician_id

                if status_changed and status.lower() == "closed":
                    lines.append(f"  • {changed_at} : Clôturé")
                elif status_changed and status.lower() == "in progress":
                    lines.append(f"  • {changed_at} : Intervention démarrée")
                elif date_changed and planned_at:
                    date_part = f"Reporté au {format_date(planned_at)}" if prev_planned_at else f"Planifié pour le {format_date(planned_at)}"
                    if tech_changed and technician_id is not None:
                        tech_name = _get_technician_name_from_cache(technician_id)
                        lines.append(f"  • {changed_at} : {date_part}, assigné à {tech_name}")
                    else:
                        lines.append(f"  • {changed_at} : {date_part}")
                elif tech_changed and technician_id is not None:
                    tech_name = _get_technician_name_from_cache(technician_id)
                    lines.append(f"  • {changed_at} : Réassigné à {tech_name}")
                elif status_changed:
                    lines.append(f"  • {changed_at} : Statut changé en {status}")

            prev_status = status
            prev_planned_at = planned_at
            prev_technician_id = technician_id
        else:
            # Ancien format (compatibilité)
            to_date = format_date(entry.get("to"))
            from_date = entry.get("from")

            if from_date is None:
                lines.append(f"  • {changed_at} : Planifié pour le {to_date}")
            else:
                from_date_str = format_date(from_date)
                lines.append(f"  • {changed_at} : Déplacé du {from_date_str} au {to_date}")

    return "\n".join(lines)


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
        logger.error("Echec enrichissement workorder %s: %s", wo["id"], exc)
        return False


# ---------------------------------------------------------------------------
# Helpers pour les commentaires VYSYNC

def get_vysync_comment(vc, ticket_id: str) -> Optional[Dict[str, Any]]:
    """Recupere le commentaire VYSYNC existant s'il existe."""
    try:
        comments = vc.get_ticket_comments(ticket_id)
        for c in comments:
            if c.get("comment", "").startswith("[VYSYNC] Historique"):
                return c
        return None
    except Exception as exc:
        logger.warning("Impossible de recuperer les commentaires du ticket %s: %s", ticket_id, exc)
        return None


def update_vysync_comment(sb, vc, ticket: Dict[str, Any], wo: Dict[str, Any], changes: List[str], *, wo_history: list = None, dry: bool = False) -> None:
    """Met a jour ou cree le commentaire VYSYNC avec l'historique."""
    ticket_id = ticket["vcom_ticket_id"]
    wo_number = wo.get("number", wo["id"])
    today = datetime.now().strftime("%d/%m/%Y")

    # Nouvelle entree
    new_entry = f"{today} :\n" + "\n".join(f"* {c}" for c in changes)

    # Ajouter l'historique du WO si plusieurs changements
    if wo_history and len(wo_history) > 1:
        new_entry += "\n" + format_wo_history_for_comment(wo_history)

    # Recuperer le commentaire existant
    existing = get_vysync_comment(vc, ticket_id)

    if existing:
        # Ajouter la nouvelle entree en haut (apres le header)
        old_content = existing["comment"]
        header = f"[VYSYNC] Historique du WO #{wo_number}\n\n"
        body = old_content.replace(header, "")
        new_content = header + new_entry + "\n\n" + body

        if not dry:
            try:
                vc.update_ticket_comment(ticket_id, existing["commentId"], new_content)
                logger.info("Commentaire VYSYNC mis a jour pour ticket %s", ticket_id)
            except Exception as exc:
                logger.error("Echec mise a jour commentaire VYSYNC ticket %s: %s", ticket_id, exc)
    else:
        # Creer le commentaire
        new_content = f"[VYSYNC] Historique du WO #{wo_number}\n\n{new_entry}"

        if not dry:
            try:
                comment_id = vc.create_ticket_comment(ticket_id, new_content)
                # Stocker l'ID du commentaire
                if comment_id:
                    sb.table("tickets").update({
                        "vcom_comment_id": comment_id
                    }).eq("vcom_ticket_id", ticket_id).execute()
                logger.info("Commentaire VYSYNC cree pour ticket %s", ticket_id)
            except Exception as exc:
                logger.error("Echec creation commentaire VYSYNC ticket %s: %s", ticket_id, exc)


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
        except Exception as exc:
            logger.error("Echec post rapport ticket %s: %s", ticket_id, exc)


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


def upsert_workorders(sb, orders: List[Dict[str, Any]], *, dry: bool = False) -> None:
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

    # Recuperer les workorder_id existants en base pour distinguer nouveaux vs existants
    existing_wo_ids_result = sb.table("work_orders").select("workorder_id").execute()
    existing_wo_ids = {row["workorder_id"] for row in existing_wo_ids_result.data}

    rows = []
    for w in valid_orders:
        wo_id = w["id"]
        is_new = wo_id not in existing_wo_ids

        # Convertir predicted_duration en int (peut etre float dans l'API)
        predicted_duration = w.get("predicted_duration")
        if predicted_duration is not None:
            predicted_duration = int(predicted_duration)

        row = {
            "workorder_id": wo_id,
            "status": w.get("status"),
            "client_id": w.get("client_id"),
            "site_id": w.get("site_id"),
            "scheduled_date": w.get("date_planned"),  # garder pour compatibilite
            "date_planned": w.get("date_planned"),    # nouvelle colonne avec timezone
            "description": w.get("description"),
            "title": w.get("title"),
            "category_id": w.get("category_id"),
            "workorder_type": w.get("workorder_type"),
            "technician_id": w.get("technician_id"),
            "manager_id": w.get("manager_id"),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "yuman_created_at": w.get("created_at"),
            "date_done": w.get("date_done"),
            "predicted_duration": predicted_duration,
            "time_taken": w.get("time_taken"),
        }

        # Pour les nouveaux WO, ajouter source et wo_history
        if is_new:
            initial_status = w.get("status") or "Open"
            initial_date_planned = w.get("date_planned")
            yuman_created_at = w.get("created_at") or datetime.now(timezone.utc).isoformat()

            row["source"] = "yuman_manual"
            row["wo_history"] = [{
                "status": initial_status,
                "planned_at": initial_date_planned,
                "technician_id": w.get("technician_id"),
                "changed_at": yuman_created_at
            }]

        rows.append(row)

    if not rows:
        return

    if dry:
        new_count = sum(1 for w in valid_orders if w["id"] not in existing_wo_ids)
        logger.info("[DRY] %d workorders a upsert (%d nouveaux)", len(rows), new_count)
    else:
        sb.table("work_orders").upsert(rows, on_conflict="workorder_id").execute()
        new_count = sum(1 for w in valid_orders if w["id"] not in existing_wo_ids)
        logger.info("%d workorders upsertes (%d nouveaux avec source='yuman_manual')", len(rows), new_count)


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
        except Exception as exc:
            logger.error("Erreur recuperation tickets VCOM (%s): %s", st, exc)
    return tickets


# ---------------------------------------------------------------------------
# Etape 2 : collecte des workorders Yuman

def collect_yuman_workorders(yc) -> List[Dict[str, Any]]:
    try:
        data = yc.list_workorders()
        logger.info("YUMAN: %d workorders recuperes", len(data))
        return data
    except Exception as exc:
        logger.error("Erreur recuperation workorders Yuman: %s", exc)
        return []


# ---------------------------------------------------------------------------
# Etapes 1-bis & 2-bis : synchronisation DB

def sync_tickets_to_db(sb, tickets, *, dry: bool = False) -> None:
    upsert_tickets(sb, tickets, dry=dry)


def sync_workorders_to_db(sb, workorders, *, dry: bool = False) -> None:
    upsert_workorders(sb, workorders, dry=dry)


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
                    logger.error("Echec assignation ticket %s: %s", tid, exc)
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

        # Initialiser wo_history avec l'entrée de création
        initial_status = res.get("status") or "Open"
        initial_date_planned = res.get("date_planned")
        yuman_created_at = res.get("created_at") or datetime.now(timezone.utc).isoformat()

        initial_wo_history = [{
            "status": initial_status,
            "planned_at": initial_date_planned,
            "technician_id": res.get("technician_id"),
            "changed_at": yuman_created_at
        }]

        # Inserer le WO en base avec wo_history initialisé
        sb.table("work_orders").insert({
            "workorder_id": wo_id,
            "status": initial_status,
            "client_id": res.get("client_id"),
            "site_id": site_id,
            "scheduled_date": res.get("date_planned"),
            "date_planned": initial_date_planned,
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
                logger.error("Echec assignation ticket %s: %s", tid, exc)

        logger.info("Workorder #%s cree pour site %s (%d tickets)", wo_number, site_id, len(tickets))

    except Exception as exc:
        logger.error("Creation WO site %s KO: %s", site_id, exc)


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
                    logger.error("Echec assignation ticket %s: %s", tid, exc)
        else:
            # Aucun WO actif -> ignorer les tickets
            for t in site_tickets:
                tid = t.get("id") or t.get("vcom_ticket_id")
                logger.info("Ticket %s (normal) ignore - aucun WO actif sur site %s", tid, site_id)


# ---------------------------------------------------------------------------
# Etape 4 : Sync des changements WO -> Tickets (commentaires VYSYNC)

def sync_wo_changes_to_tickets(
    sb, vc, yc, workorders: List[Dict[str, Any]], *, dry: bool = False
) -> None:
    """
    Synchronise les changements WO vers les tickets VCOM (commentaires).
    Enregistre aussi l'historique des changements de date dans work_orders.
    """
    # Recuperer tous les tickets avec un WO assigne (exclure les tickets fermes)
    tickets_with_wo = sb.table("tickets").select("*").not_.is_("yuman_workorder_id", "null").neq("status", "closed").execute()

    if not tickets_with_wo.data:
        logger.info("Aucun ticket avec WO assigne a synchroniser")
        return

    logger.info("%d tickets avec WO a verifier pour changements", len(tickets_with_wo.data))

    # Creer un dict des WO pour lookup rapide
    wo_by_id = {w["id"]: w for w in workorders}

    # Tracker les WO déjà mis à jour (pour éviter doublons si plusieurs tickets sur même WO)
    wo_history_updated = set()

    for ticket in tickets_with_wo.data:
        wo_id = ticket["yuman_workorder_id"]
        wo = wo_by_id.get(wo_id)

        if not wo:
            continue  # WO n'existe plus ou pas dans la liste

        # Detecter les changements
        changes = []

        # Changement de technicien
        old_tech = ticket.get("yuman_technician_id")
        new_tech = wo.get("technician_id")
        if old_tech != new_tech and new_tech is not None:
            tech_name = get_technician_name(yc, new_tech)
            changes.append(f"WO attribue a : {tech_name}")

        # Changement de date planifiee
        old_date = ticket.get("yuman_date_planned")
        new_date = wo.get("date_planned")
        date_changed = not dates_are_equal(old_date, new_date) and new_date is not None

        # Changement de status
        old_status = ticket.get("yuman_wo_status") or ""
        new_status = wo.get("status") or ""
        status_changed = old_status.lower() != new_status.lower()

        if date_changed:
            date_str = format_date(new_date)
            changes.append(f"Intervention planifiee : {date_str}")

        if status_changed:
            if new_status.lower() == "closed":
                date_done = wo.get("date_done") or datetime.now().isoformat()
                changes.append(f"WO cloture le {format_date(date_done)}")
            elif new_status.lower() == "in progress":
                changes.append("Intervention demarree")
            else:
                changes.append(f"Statut WO : {new_status}")

        # Detecter changement de technicien
        tech_changed = old_tech != new_tech

        # Enregistrer dans l'historique du WO si statut, date ou technicien a changé (une seule fois par WO)
        if (status_changed or date_changed or tech_changed) and wo_id not in wo_history_updated:
            if not dry:
                append_wo_history(sb, wo_id, new_status, new_date, new_tech)
                wo_history_updated.add(wo_id)
            else:
                logger.info("[DRY] Historique WO %s: status=%s, planned_at=%s, technician_id=%s", wo_id, new_status, new_date, new_tech)
                wo_history_updated.add(wo_id)

        if not changes:
            continue  # Rien n'a change

        # Recuperer l'historique du WO pour l'inclure dans le commentaire
        wo_history = []
        if not dry:
            try:
                wo_record = sb.table("work_orders").select("wo_history").eq("workorder_id", wo_id).single().execute()
                wo_history = wo_record.data.get("wo_history") or []
            except Exception:
                pass  # Pas grave si on n'arrive pas à récupérer l'historique

        # Mettre a jour le commentaire VYSYNC (avec historique si disponible)
        if dry:
            logger.info("[DRY] Changements detectes pour ticket %s: %s", ticket["vcom_ticket_id"], changes)
            if wo_history and len(wo_history) > 1:
                logger.info("[DRY] Historique WO: %s", wo_history)
        else:
            update_vysync_comment(sb, vc, ticket, wo, changes, wo_history=wo_history, dry=dry)

        # Si cloture, poster aussi le report
        if new_status.lower() == "closed" and old_status.lower() != new_status.lower():
            post_report_comment(vc, yc, ticket, wo, dry=dry)

        # Mettre a jour la DB tickets
        if not dry:
            try:
                sb.table("tickets").update({
                    "yuman_technician_id": new_tech,
                    "yuman_date_planned": new_date,
                    "yuman_wo_status": new_status,
                    "last_sync_at": datetime.now(timezone.utc).isoformat()
                }).eq("vcom_ticket_id", ticket["vcom_ticket_id"]).execute()
            except Exception as exc:
                logger.error("Echec mise a jour DB ticket %s: %s", ticket["vcom_ticket_id"], exc)


# ---------------------------------------------------------------------------
# Etape 5 : fermer les tickets VCOM des workorders clos

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
                    "yuman_wo_status": "Closed",
                    "last_sync_at": datetime.now(timezone.utc).isoformat()
                }).eq("vcom_ticket_id", tid).execute()
                logger.info("Ticket %s ferme (WO %s cloture)", tid, wo_id)
            except Exception as exc:
                logger.error("Echec fermeture ticket %s: %s", tid, exc)

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

    # 2. Sync vers DB
    logger.info("=== Etape 2 : Synchronisation DB ===")
    sync_tickets_to_db(sb, tickets, dry=dry_run)
    sync_workorders_to_db(sb, workorders, dry=dry_run)

    # 3. Assignation des tickets selon nouvelles regles
    logger.info("=== Etape 3 : Assignation des tickets ===")
    assign_urgent_high_tickets(sb, vc, yc, tickets, workorders, dry=dry_run)
    assign_normal_tickets(sb, vc, yc, tickets, workorders, dry=dry_run)
    # Note: tickets "low" sont ignores

    # 4. Sync des changements WO -> Tickets (commentaires VYSYNC)
    logger.info("=== Etape 4 : Sync WO -> Tickets (commentaires) ===")
    sync_wo_changes_to_tickets(sb, vc, yc, workorders, dry=dry_run)

    # 5. Fermeture des tickets dont le WO est cloture
    logger.info("=== Etape 5 : Fermeture des tickets ===")
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
