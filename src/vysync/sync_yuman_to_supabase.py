#!/usr/bin/env python3
"""
sync_yuman_to_supabase.py
=========================

Synchronise les données Yuman → Supabase selon les règles de source de vérité :

1. clients_mapping : Yuman = source de vérité (insert + update)
   - Clé: yuman_client_id
   
2. sites_mapping :
   - Clé: yuman_site_id
   - INSERT si yuman_site_id pas en DB (nouveaux sites Yuman)
   - UPDATE (fill if NULL) pour: code, aldi_id, aldi_store_id, project_number_cp
   - client_map_id → détection conflit + mail (pas d'update auto)
   - Ne pas toucher les sites avec ignore_site = true
   
3. equipments_mapping :
   - yuman_material_id → UPDATE toujours (jointure par serial_number)
   - SIM : brand, model → UPDATE (Yuman = vérité)
   - Autres catégories : ne pas toucher (sauf yuman_material_id)

Usage:
    poetry run python -m vysync.sync_yuman_to_supabase
"""

from __future__ import annotations

import json
import logging
import os
import smtplib
from dataclasses import asdict
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Dict, List, Optional

from vysync.adapters.supabase_adapter import SupabaseAdapter
from vysync.adapters.yuman_adapter import YumanAdapter
from vysync.models import Client, Site, Equipment, CAT_SIM

# ─────────────────────────── Logger ────────────────────────────
logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

ALERT_EMAIL = "t.roquefeuil@centroplan.fr"

# Champs sites à synchroniser (fill if NULL uniquement)
SITE_FILL_FIELDS = [
    "code",
    "aldi_id",
    "aldi_store_id",
    "project_number_cp",
]


# ═══════════════════════════════════════════════════════════════════════════════
# FONCTIONS UTILITAIRES
# ═══════════════════════════════════════════════════════════════════════════════

def _now_iso() -> str:
    """Retourne l'horodatage UTC actuel en ISO 8601."""
    return datetime.now(timezone.utc).isoformat()


def _is_empty(value: Any) -> bool:
    """Vérifie si une valeur est 'vide' (None, "", 0)."""
    if value is None:
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def _norm_serial(s: str | None) -> str:
    """Normalise un serial_number (strip + upper)."""
    return (s or "").strip().upper()


# ═══════════════════════════════════════════════════════════════════════════════
# ENVOI DE MAIL
# ═══════════════════════════════════════════════════════════════════════════════

def send_conflict_email(conflicts: List[Dict[str, Any]]) -> bool:
    """
    Envoie un email de notification pour les conflits client_map_id.
    
    Utilise les variables d'environnement :
    - SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD
    
    Retourne True si l'envoi a réussi, False sinon.
    """
    if not conflicts:
        return True
    
    # Construction du contenu
    subject = f"[vysync] {len(conflicts)} conflit(s) client_map_id détecté(s)"
    
    body_lines = [
        "Bonjour,",
        "",
        f"{len(conflicts)} conflit(s) de client_map_id ont été détectés lors de la synchronisation Yuman → Supabase.",
        "",
        "Ces conflits nécessitent une intervention manuelle pour déterminer la source de vérité.",
        "",
        "=" * 60,
        ""
    ]
    
    for i, c in enumerate(conflicts, 1):
        body_lines.extend([
            f"[Conflit {i}]",
            f"  Site ID (Supabase) : {c['site_id']}",
            f"  Site name          : {c['site_name']}",
            f"  vcom_system_key    : {c['vcom_system_key']}",
            f"  yuman_site_id      : {c['yuman_site_id']}",
            f"  client_map_id DB   : {c['db_client_map_id']}",
            f"  client_map_id Yuman: {c['yuman_client_map_id']}",
            ""
        ])
    
    body_lines.extend([
        "=" * 60,
        "",
        "Merci de vérifier dans Supabase et Yuman quelle est la bonne valeur.",
        "",
        "— vysync"
    ])
    
    body = "\n".join(body_lines)
    
    # Envoi via SMTP
    smtp_host = os.getenv("SMTP_HOST")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER")
    smtp_password = os.getenv("SMTP_PASSWORD")
    
    if not all([smtp_host, smtp_user, smtp_password]):
        logger.warning("[MAIL] Variables SMTP non configurées, email non envoyé")
        logger.info("[MAIL] Contenu du mail qui aurait été envoyé:")
        for line in body_lines:
            logger.info("[MAIL]   %s", line)
        return False
    
    try:
        msg = MIMEMultipart()
        msg["From"] = smtp_user
        msg["To"] = ALERT_EMAIL
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))
        
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.send_message(msg)
        
        logger.info("[MAIL] Email envoyé à %s (%d conflits)", ALERT_EMAIL, len(conflicts))
        return True
        
    except Exception as e:
        logger.error("[MAIL] Erreur envoi email: %s", e)
        logger.info("[MAIL] Contenu du mail:")
        for line in body_lines:
            logger.info("[MAIL]   %s", line)
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# SYNC CLIENTS (logique custom - clé: yuman_client_id)
# ═══════════════════════════════════════════════════════════════════════════════

def sync_clients(
    sb: SupabaseAdapter,
    y: YumanAdapter,
) -> Dict[str, Any]:
    """
    Synchronise les clients : Yuman → Supabase.
    Yuman est la source de vérité.
    
    Clé de jointure : yuman_client_id
    
    Retourne un dict avec les compteurs.
    """
    logger.info("[CLIENTS] Démarrage synchronisation...")
    
    # 1) Snapshot Yuman
    y_clients_raw = list(y.yc.list_clients())
    logger.info("[CLIENTS] %d clients Yuman", len(y_clients_raw))
    
    # 2) Snapshot DB (indexé par yuman_client_id)
    db_clients = sb.fetch_clients()  # Dict[yuman_client_id, Client]
    db_client_ids = set(db_clients.keys())
    logger.info("[CLIENTS] %d clients en DB", len(db_clients))
    
    # 3) Comparaison
    to_insert = []
    to_update = []
    
    for y_row in y_clients_raw:
        yid = y_row["id"]  # yuman_client_id
        y_name = y_row.get("name", "")
        y_code = y_row.get("code")
        y_address = y_row.get("address") or y_row.get("billing_address")
        
        if yid not in db_client_ids:
            # Nouveau client → INSERT
            to_insert.append({
                "yuman_client_id": yid,
                "name": y_name,
                "code": y_code,
                "address": y_address,
                "created_at": _now_iso(),
            })
        else:
            # Client existant → vérifier si UPDATE nécessaire
            db_client = db_clients[yid]
            changes = {}
            
            if y_name and y_name != db_client.name:
                changes["name"] = y_name
            if y_code and y_code != db_client.code:
                changes["code"] = y_code
            if y_address and y_address != db_client.address:
                changes["address"] = y_address
            
            if changes:
                to_update.append({
                    "yuman_client_id": yid,
                    "changes": changes,
                })
    
    logger.info("[CLIENTS] À insérer: %d, À mettre à jour: %d", len(to_insert), len(to_update))
    
    # 4) Apply INSERTs
    for row in to_insert:
        try:
            sb.sb.table("clients_mapping").insert(row).execute()
            logger.debug("[CLIENTS] INSERT yuman_client_id=%d name=%s", 
                        row["yuman_client_id"], row["name"])
        except Exception as e:
            logger.error("[CLIENTS] ERREUR INSERT yuman_client_id=%d: %s", 
                        row["yuman_client_id"], e)
    
    # 5) Apply UPDATEs
    for upd in to_update:
        try:
            sb.sb.table("clients_mapping").update(upd["changes"]).eq(
                "yuman_client_id", upd["yuman_client_id"]
            ).execute()
            logger.debug("[CLIENTS] UPDATE yuman_client_id=%d: %s", 
                        upd["yuman_client_id"], upd["changes"])
        except Exception as e:
            logger.error("[CLIENTS] ERREUR UPDATE yuman_client_id=%d: %s", 
                        upd["yuman_client_id"], e)
    
    return {
        "yuman_count": len(y_clients_raw),
        "db_count": len(db_clients),
        "inserted": len(to_insert),
        "updated": len(to_update),
        "insert_details": to_insert,
        "update_details": [{"yuman_client_id": u["yuman_client_id"], **u["changes"]} for u in to_update],
    }


# ═══════════════════════════════════════════════════════════════════════════════
# SYNC SITES (logique custom - clé: yuman_site_id)
# ═══════════════════════════════════════════════════════════════════════════════

def sync_sites(
    sb: SupabaseAdapter,
    y: YumanAdapter,
) -> Dict[str, Any]:
    """
    Synchronise les sites : Yuman → Supabase.
    
    Clé de jointure : yuman_site_id
    
    Règles :
    - INSERT si yuman_site_id pas en DB (nouveaux sites Yuman, vcom_system_key = NULL)
    - UPDATE (fill if NULL) pour: code, aldi_id, aldi_store_id, project_number_cp
    - Détection conflit client_map_id → mail
    - Ne pas toucher les sites avec ignore_site = true
    
    Retourne un dict avec les compteurs et conflits.
    """
    logger.info("[SITES] Démarrage synchronisation...")

    # 0) Pré-charger le mapping yuman_client_id → client_map_id
    # Les clients sont synchronisés AVANT les sites dans le workflow
    clients_data = sb.sb.table("clients_mapping").select("id,yuman_client_id").execute().data or []
    yuman_to_client_map = {
        c["yuman_client_id"]: c["id"]
        for c in clients_data
        if c.get("yuman_client_id")
    }
    logger.info("[SITES] %d clients mappés (yuman_client_id → client_map_id)", len(yuman_to_client_map))

    # 1) Snapshot Yuman (indexé par yuman_site_id)
    y_sites = y.fetch_sites()  # Dict[yuman_site_id, Site]
    logger.info("[SITES] %d sites Yuman", len(y_sites))
    
    # 2) Snapshot DB - besoin d'un index par yuman_site_id
    # fetch_sites_y retourne Dict[id, Site] mais on a besoin de yuman_site_id
    db_sites_raw = sb.sb.table("sites_mapping").select("*").execute().data or []
    
    # Index par yuman_site_id (pour les sites qui en ont un)
    db_by_yuman_id: Dict[int, dict] = {}
    db_ignored_yuman_ids: set = set()
    
    for row in db_sites_raw:
        yid = row.get("yuman_site_id")
        if yid is not None:
            db_by_yuman_id[yid] = row
            if row.get("ignore_site"):
                db_ignored_yuman_ids.add(yid)
    
    logger.info("[SITES] %d sites en DB (dont %d avec yuman_site_id)", 
                len(db_sites_raw), len(db_by_yuman_id))
    logger.info("[SITES] %d sites avec ignore_site=true", len(db_ignored_yuman_ids))
    
    # 3) Comparaison
    to_insert = []
    to_update = []
    conflicts = []
    skipped_ignored = 0
    
    for yid, y_site in y_sites.items():
        # Skip si ignore_site = true
        if yid in db_ignored_yuman_ids:
            skipped_ignored += 1
            continue

        # Résoudre le client_map_id depuis le yuman_client_id du site
        yuman_client_id = getattr(y_site, "yuman_client_id", None)
        y_client_map_id = yuman_to_client_map.get(yuman_client_id) if yuman_client_id else None

        if yid not in db_by_yuman_id:
            # Nouveau site Yuman → INSERT (avec vcom_system_key = NULL)
            to_insert.append({
                "yuman_site_id": yid,
                "name": y_site.name,
                "code": getattr(y_site, "code", None),
                "latitude": y_site.latitude,
                "longitude": y_site.longitude,
                "address": y_site.address,
                "aldi_id": y_site.aldi_id,
                "aldi_store_id": y_site.aldi_store_id,
                "project_number_cp": y_site.project_number_cp,
                "nominal_power": y_site.nominal_power,
                "commission_date": y_site.commission_date,
                "client_map_id": y_client_map_id,  # Client récupéré depuis l'API Yuman
                "vcom_system_key": None,  # Explicitement NULL
                "created_at": _now_iso(),
            })
        else:
            # Site existant → vérifier fill-if-NULL et conflits
            db_row = db_by_yuman_id[yid]
            
            # A) Fill if NULL pour les champs spécifiés
            changes = {}
            for field in SITE_FILL_FIELDS:
                db_val = db_row.get(field)
                y_val = getattr(y_site, field, None)
                
                if _is_empty(db_val) and not _is_empty(y_val):
                    changes[field] = y_val
            
            if changes:
                to_update.append({
                    "site_id": db_row["id"],
                    "yuman_site_id": yid,
                    "changes": changes,
                })
            
            # B) Détecter conflit client_map_id
            # y_client_map_id est déjà résolu plus haut depuis yuman_client_id
            db_client_map_id = db_row.get("client_map_id")

            if (
                not _is_empty(db_client_map_id)
                and not _is_empty(y_client_map_id)
                and db_client_map_id != y_client_map_id
            ):
                conflicts.append({
                    "site_id": db_row["id"],
                    "site_name": db_row.get("name"),
                    "vcom_system_key": db_row.get("vcom_system_key"),
                    "yuman_site_id": yid,
                    "db_client_map_id": db_client_map_id,
                    "yuman_client_map_id": y_client_map_id,
                })
    
    logger.info("[SITES] À insérer: %d, À mettre à jour: %d, Conflits: %d, Ignorés: %d",
                len(to_insert), len(to_update), len(conflicts), skipped_ignored)
    
    # 4) Apply INSERTs
    for row in to_insert:
        try:
            # Nettoyer les valeurs None pour éviter les erreurs
            clean_row = {k: v for k, v in row.items() if v is not None or k == "vcom_system_key"}
            sb.sb.table("sites_mapping").insert(clean_row).execute()
            logger.info("[SITES] INSERT yuman_site_id=%d name=%s", 
                       row["yuman_site_id"], row["name"])
        except Exception as e:
            logger.error("[SITES] ERREUR INSERT yuman_site_id=%d: %s", 
                        row["yuman_site_id"], e)
    
    # 5) Apply UPDATEs
    for upd in to_update:
        try:
            sb.sb.table("sites_mapping").update(upd["changes"]).eq(
                "id", upd["site_id"]
            ).execute()
            logger.debug("[SITES] UPDATE site_id=%d yuman_site_id=%d: %s", 
                        upd["site_id"], upd["yuman_site_id"], upd["changes"])
        except Exception as e:
            logger.error("[SITES] ERREUR UPDATE site_id=%d: %s", 
                        upd["site_id"], e)
    
    # 6) Envoyer mail si conflits
    if conflicts:
        logger.warning("[SITES] %d conflit(s) client_map_id détectés", len(conflicts))
        send_conflict_email(conflicts)
    
    return {
        "yuman_count": len(y_sites),
        "db_count": len(db_sites_raw),
        "inserted": len(to_insert),
        "updated": len(to_update),
        "conflicts": conflicts,
        "skipped_ignored": skipped_ignored,
        "insert_details": to_insert,
        "update_details": to_update,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# SYNC EQUIPMENTS
# ═══════════════════════════════════════════════════════════════════════════════

def sync_equipments(
    sb: SupabaseAdapter,
    y: YumanAdapter,
) -> Dict[str, Any]:
    """
    Synchronise les équipements : Yuman → Supabase.
    
    Règles :
    - yuman_material_id → UPDATE toujours (jointure par serial_number)
    - SIM : brand, model → UPDATE (Yuman = vérité)
    - Autres catégories : ne pas toucher (sauf yuman_material_id)
    
    Retourne un dict avec les compteurs.
    """
    logger.info("[EQUIPS] Démarrage synchronisation...")
    
    # 1) Snapshot Yuman
    y_equips = y.fetch_equips()
    logger.info("[EQUIPS] %d équipements Yuman", len(y_equips))
    
    # 2) Snapshot DB
    db_equips = sb.fetch_equipments_y()
    logger.info("[EQUIPS] %d équipements en DB", len(db_equips))
    
    # 3) Index DB par serial_number (pour lookup rapide)
    db_by_serial: Dict[str, Equipment] = {
        _norm_serial(e.serial_number): e
        for e in db_equips.values()
        if e.serial_number
    }
    
    # 4) Traitement équipement par équipement
    updates_yuman_material_id = []
    updates_sim = []
    skipped_no_serial = 0
    skipped_not_in_db = 0
    
    for y_serial, y_equip in y_equips.items():
        serial_norm = _norm_serial(y_serial)
        
        # Skip si pas de serial
        if not serial_norm:
            skipped_no_serial += 1
            continue
        
        # Trouver l'équipement DB correspondant
        db_equip = db_by_serial.get(serial_norm)
        if db_equip is None:
            skipped_not_in_db += 1
            continue
        
        # A) Toujours mettre à jour yuman_material_id si différent
        if (
            y_equip.yuman_material_id is not None
            and y_equip.yuman_material_id != db_equip.yuman_material_id
        ):
            updates_yuman_material_id.append({
                "serial_number": serial_norm,
                "old_yuman_material_id": db_equip.yuman_material_id,
                "new_yuman_material_id": y_equip.yuman_material_id,
                "category_id": y_equip.category_id,
            })
        
        # B) Pour les SIM : mettre à jour brand et model
        if y_equip.category_id == CAT_SIM:
            changes = {}
            if y_equip.brand and y_equip.brand != db_equip.brand:
                changes["brand"] = y_equip.brand
            if y_equip.model and y_equip.model != db_equip.model:
                changes["model"] = y_equip.model
            
            if changes:
                updates_sim.append({
                    "serial_number": serial_norm,
                    "changes": changes,
                })
    
    logger.info("[EQUIPS] yuman_material_id à mettre à jour: %d", len(updates_yuman_material_id))
    logger.info("[EQUIPS] SIM (brand/model) à mettre à jour: %d", len(updates_sim))
    logger.info("[EQUIPS] Skipped (pas de serial): %d", skipped_no_serial)
    logger.info("[EQUIPS] Skipped (pas en DB): %d", skipped_not_in_db)
    
    # 5) Appliquer les updates yuman_material_id
    for upd in updates_yuman_material_id:
        try:
            sb.sb.table("equipments_mapping").update({
                "yuman_material_id": upd["new_yuman_material_id"]
            }).eq("serial_number", upd["serial_number"]).execute()
            
            logger.debug(
                "[EQUIPS] UPDATE yuman_material_id serial=%s: %s → %s",
                upd["serial_number"],
                upd["old_yuman_material_id"],
                upd["new_yuman_material_id"],
            )
        except Exception as e:
            logger.error(
                "[EQUIPS] ERREUR update yuman_material_id serial=%s: %s",
                upd["serial_number"], e
            )
    
    # 6) Appliquer les updates SIM (brand/model)
    for upd in updates_sim:
        try:
            sb.sb.table("equipments_mapping").update(
                upd["changes"]
            ).eq("serial_number", upd["serial_number"]).execute()
            
            logger.debug(
                "[EQUIPS] UPDATE SIM serial=%s: %s",
                upd["serial_number"],
                upd["changes"],
            )
        except Exception as e:
            logger.error(
                "[EQUIPS] ERREUR update SIM serial=%s: %s",
                upd["serial_number"], e
            )
    
    return {
        "yuman_count": len(y_equips),
        "db_count": len(db_equips),
        "updates_yuman_material_id": len(updates_yuman_material_id),
        "updates_sim": len(updates_sim),
        "skipped_no_serial": skipped_no_serial,
        "skipped_not_in_db": skipped_not_in_db,
        "details_yuman_material_id": updates_yuman_material_id,
        "details_sim": updates_sim,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main() -> Dict[str, Any]:
    """
    Point d'entrée principal.
    
    Exécute la synchronisation complète Yuman → Supabase et génère un rapport.
    """
    # Configuration logging
    from vysync.logging_config import setup_logging
    setup_logging()
    
    logger.info("=" * 70)
    logger.info("SYNC YUMAN → SUPABASE")
    logger.info("=" * 70)
    
    # Initialisation
    sb = SupabaseAdapter()
    y = YumanAdapter(sb)
    
    # Rapport
    report = {
        "execution_date": _now_iso(),
        "clients": {},
        "sites": {},
        "equipments": {},
        "success": True,
        "errors": [],
    }
    
    # 1) Sync clients
    try:
        report["clients"] = sync_clients(sb, y)
    except Exception as e:
        logger.error("[CLIENTS] Erreur: %s", e, exc_info=True)
        report["errors"].append({"step": "clients", "error": str(e)})
        report["success"] = False
    
    # 2) Sync sites
    try:
        report["sites"] = sync_sites(sb, y)
    except Exception as e:
        logger.error("[SITES] Erreur: %s", e, exc_info=True)
        report["errors"].append({"step": "sites", "error": str(e)})
        report["success"] = False
    
    # 3) Sync equipments
    try:
        report["equipments"] = sync_equipments(sb, y)
    except Exception as e:
        logger.error("[EQUIPS] Erreur: %s", e, exc_info=True)
        report["errors"].append({"step": "equipments", "error": str(e)})
        report["success"] = False
    
    # Sauvegarde rapport JSON
    report_filename = f"sync_yuman_supabase_{datetime.now():%Y%m%d_%H%M%S}.json"
    with open(report_filename, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False, default=str)
    
    # Résumé console
    logger.info("=" * 70)
    logger.info("RÉSUMÉ")
    logger.info("=" * 70)
    logger.info("  Clients  : +%d ~%d", 
                report["clients"].get("inserted", 0),
                report["clients"].get("updated", 0))
    logger.info("  Sites    : +%d ~%d (conflits: %d, ignorés: %d)",
                report["sites"].get("inserted", 0),
                report["sites"].get("updated", 0),
                len(report["sites"].get("conflicts", [])),
                report["sites"].get("skipped_ignored", 0))
    logger.info("  Équipements : yuman_material_id=%d, SIM=%d",
                report["equipments"].get("updates_yuman_material_id", 0),
                report["equipments"].get("updates_sim", 0))
    logger.info("  Rapport  : %s", report_filename)
    logger.info("=" * 70)
    
    return report


if __name__ == "__main__":
    main()