#!/usr/bin/env python3
"""Synchronise la base locale (Supabase) avec VCOM et Yuman.

Étapes :
1. Importer tous les systèmes VCOM non présents → `sites_mapping` et `equipments_mapping`.
2. Importer tous les clients, sites et équipements pertinents (modules & onduleurs) de Yuman → mapping.
3. Détecter les conflits de sites (ligne VCOM sans `yuman_site_id`, ligne Yuman sans `vcom_system_key`).
4. Interaction CLI pour résoudre les conflits et/ou créer manuellement des mappages.
5. Fermer les conflits : fusionner les lignes et ré‑affecter les FK équipements.
6. Résoudre les sites sans client → auto‑match ou saisie interactive + éventuelle création d’un client Yuman.
7. Créer immédiatement sur Yuman les sites restants (issus de VCOM) ainsi que leurs équipements standard.

Le script est idempotent : chaque upsert s’appuie sur des clefs uniques.
"""
from __future__ import annotations

import os
import re
import sys

from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple, Any, Set
from itertools import chain

from supabase import create_client, Client as SupabaseClient

# Clients internes
from vysync.yuman_client import YumanClient
from vysync.vcom_client import VCOMAPIClient
from vysync.app_logging import init_logger

# ───────────────────────── Constantes
CAT_CENTRALE = 11441
CAT_MODULE = 11103
CAT_INVERTER = 11102

# Alias used for panel equipments
CAT_PANEL = CAT_MODULE


ALLOWED_EQUIP_CATEGORIES: Set[int] = {CAT_INVERTER, CAT_MODULE}
CUSTOM_FIELD_INVERTER_ID = "Inverter ID (Vcom)"

SITE_CUSTOM_FIELDS = {          # champ Yuman  →  colonne DB
    "System Key (Vcom ID)": "vcom_system_key",
    "ALDI ID": "aldi_id",
    "ID magasin (n° interne Aldi)": "aldi_store_id",
    "Project number (Centroplan ID)": "project_number_cp",
}

SITE_TABLE        = "sites_mapping"
EQUIP_TABLE       = "equipments_mapping"
CLIENT_TABLE      = "clients_mapping"
FIELD_VALUES_TABLE = "equipment_field_values"
CONFLICT_TABLE     = "conflicts"

LOCK_FILE = Path("/tmp/sync_db_vcom_yuman.lock")

logger = init_logger(__name__)

# ───────────────────────── Utilitaires

@contextmanager
def execution_lock():
    """Verrou grossier basé sur un fichier. Évite deux exécutions simultanées."""
    if LOCK_FILE.exists():
        logger.error("Another sync is already running (lockfile %s).", LOCK_FILE)
        sys.exit(1)
    try:
        LOCK_FILE.touch(exist_ok=False)
        yield
    finally:
        try:
            LOCK_FILE.unlink()
        except FileNotFoundError:
            pass

def build_address(addr: Dict[str, Any]) -> str | None:
    if not addr:
        return None
    parts = [addr.get("street"), f"{addr.get('postalCode', '')} {addr.get('city', '')}".strip()]
    return ", ".join(filter(None, parts)) or None

def sb_upsert(table: str, rows: List[Dict[str, Any]], sb: SupabaseClient,
              pk: str, dry: bool = False) -> None:
    """Upsert générique s’appuyant sur la(les) colonne(s) *pk* comme clé unique."""
    if not rows:
        return
    key_cols = [c.strip() for c in pk.split(",")]
    uniq: Dict[Tuple[Any, ...], Dict[str, Any]] = {
        tuple(r.get(c) for c in key_cols): r for r in rows
    }
    rows = list(uniq.values())
    if dry:
        logger.debug("[DRY] %s : upsert %d lignes sur %s", table, len(rows), pk)
        return
    sb.table(table).upsert(rows, on_conflict=pk).execute()



def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def check_vcom_updates(
    sb: "SupabaseClient",
    vc: "VCOMAPIClient",
    *,
    dry: bool = True,
) -> List[Dict[str, Any]]:
    """Detecte les écarts entre Supabase et VCOM sans rien écrire."""

    # --- A. Snapshot DB -------------------------------------------------
    sites = (
        sb.table(SITE_TABLE)
          .select(
              "id,vcom_system_key,nominal_power,site_area,"
              "commission_date,name"
          )
          .not_.is_("vcom_system_key", None)
          .execute()
          .data
    )

    inv_rows = (
        sb.table(EQUIP_TABLE)
          .select(
              "id,site_id,vcom_device_id,serial_number,name,brand,model"
          )
          .eq("category_id", CAT_INVERTER)
          .eq("is_obsolete", False)
          .execute()
          .data
    )

    panel_rows = (
        sb.table(EQUIP_TABLE)
          .select("id,site_id,brand,model,count")
          .eq("category_id", CAT_PANEL)
          .execute()
          .data
    )

    inv_by_site_dev: Dict[Tuple[int, str], Dict[str, Any]] = {
        (r["site_id"], r["vcom_device_id"]): r for r in inv_rows
    }
    panel_by_site_md: Dict[Tuple[int, str, str], Dict[str, Any]] = {
        (r["site_id"], r["brand"], r["model"]): r for r in panel_rows
    }

    # --- B. Noms des sites : 1 seul appel global -----------------------
    try:
        systems = vc.get_systems()
    except Exception as exc:
        logger.error("VCOM get_systems() failed: %s", exc)
        raise

    sys_by_key = {sys["key"]: sys for sys in systems}

    # --- C. Boucle sites ------------------------------------------------
    changes: List[Dict[str, Any]] = []
    details_to_fetch: Set[Tuple[str, str]] = set()
    placeholder_inserts: Dict[Tuple[int, str], Dict[str, Any]] = {}

    for s in sites:
        sys_key = s["vcom_system_key"]
        vcom_sys = sys_by_key.get(sys_key)
        if not vcom_sys:
            logger.warning("Site %s (id=%s) absent de VCOM", sys_key, s["id"])
            continue

        try:
            tech = vc.get_technical_data(sys_key)
            invs = vc.get_inverters(sys_key)
        except Exception as exc:
            logger.error("[VCOM] %s – erreur API : %s", sys_key, exc)
            continue

        site_patch: Dict[str, Any] = {}
        obsolete_ids: List[int] = []
        equip_inserts: List[Dict[str, Any]] = []
        equip_patches: List[Dict[str, Any]] = []

        # 1. Métadonnées site -------------------------------------------
        if tech.get("nominalPower") != s.get("nominal_power"):
            site_patch["nominal_power"] = tech.get("nominalPower")

        if tech.get("siteArea") != s.get("site_area"):
            site_patch["site_area"] = tech.get("siteArea")

        if tech.get("commissionDate") and tech["commissionDate"] != s.get(
            "commission_date"
        ):
            site_patch["commission_date"] = tech["commissionDate"]

        if vcom_sys.get("name") and vcom_sys["name"] != s.get("name"):
            site_patch["name"] = vcom_sys["name"]

        # 2. Onduleurs (liste) -----------------------------------------
        for iv in invs:
            key_db = (s["id"], iv["id"])
            db_row = inv_by_site_dev.get(key_db)

            needs_detail = False

            if not db_row:
                needs_detail = True
            else:
                if iv.get("serial") and iv["serial"] != db_row["serial_number"]:
                    needs_detail = True
                if iv.get("name") and iv["name"] != db_row["name"]:
                    needs_detail = True

            if not needs_detail:
                continue

            if db_row:
                obsolete_ids.append(db_row["id"])

            placeholder = {
                "site_id": s["id"],
                "category_id": CAT_INVERTER,
                "eq_type": "inverter",
                "vcom_device_id": iv["id"],
                "name": iv.get("name") or iv["id"],
                "serial_number": iv.get("serial"),
                "brand": None,
                "model": None,
                "created_at": now_iso(),
            }
            equip_inserts.append(placeholder)
            placeholder_inserts[(s["id"], iv["id"])] = placeholder
            details_to_fetch.add((sys_key, iv["id"]))

        # 3. Panneaux (update sur place) -------------------------------
        for p in tech.get("panels", []):
            key_md = (s["id"], p.get("vendor"), p["model"])
            db_row = panel_by_site_md.get(key_md)

            if not db_row:
                equip_inserts.append({
                    "site_id": s["id"],
                    "category_id": CAT_PANEL,
                    "eq_type": "panel",
                    "brand": p.get("vendor"),
                    "model": p["model"],
                    "count": (p["count"] or None),
                    "created_at": now_iso(),
                })
                continue

            patch: Dict[str, Any] = {}
            if (
                p.get("count") is not None
                and p["count"] != db_row.get("count")
                and not (db_row.get("count") is None and p["count"] == 0)
            ):
                patch["count"] = p["count"]

            if patch:
                patch["id"] = db_row["id"]
                equip_patches.append(patch)

        if site_patch or obsolete_ids or equip_inserts or equip_patches:
            changes.append({
                "site_id": s["id"],
                "vcom_system_key": sys_key,
                "site_patch": site_patch,
                "obsolete_ids": obsolete_ids,
                "equip_inserts": equip_inserts,
                "equip_patches": equip_patches,
            })

    # --- D. Appels unitaires pour détails onduleurs --------------------
    for sys_key, dev_id in details_to_fetch:
        try:
            det = vc.get_inverter_details(sys_key, dev_id)
        except Exception as exc:
            logger.warning(
                "VCOM inverter detail %s/%s failed: %s", sys_key, dev_id, exc
            )
            continue

        for chg in changes:
            ph = placeholder_inserts.get((chg["site_id"], dev_id))
            if ph and ph in chg["equip_inserts"]:
                ph["brand"] = det.get("vendor")
                ph["model"] = det.get("model")
                break

    if dry:
        logger.info(
            "[DRY] %d site(s) présentent des écarts VCOM → DB", len(changes)
        )

    return changes


def _apply_vcom_changes_to_db(
    sb: "SupabaseClient",
    changes: List[Dict[str, Any]],
    *,
    dry: bool = True,
) -> None:
    """Applique dans Supabase le résultat de `check_vcom_updates()`."""

    # ──────────────── 1. Agrégation des listes ──────────────────────────
    site_updates: List[Dict[str, Any]] = []
    obsolete_ids: List[int] = []
    equip_inserts: List[Dict[str, Any]] = []
    panel_patches: List[Dict[str, Any]] = []

    for chg in changes:
        if chg["site_patch"]:
            site_updates.append({"id": chg["site_id"], **chg["site_patch"]})
        obsolete_ids.extend(chg["obsolete_ids"])
        equip_inserts.extend(chg["equip_inserts"])
        panel_patches.extend(chg["equip_patches"])

    now = now_iso()

    logger.info(
        "Applying VCOM → DB: %d site update(s), %d obsolete, %d insert(s), %d panel patch(es)",
        len(site_updates), len(obsolete_ids), len(equip_inserts), len(panel_patches)
    )

    if dry:
        logger.info("[DRY] Aucun écrit en base effectué.")
        return

    # ──────────────── 2. Sites : patch partiel --------------------------
    if site_updates:
        sb.table(SITE_TABLE).upsert(site_updates, on_conflict="id").execute()

    # ──────────────── 3. Obsolescence onduleurs -------------------------
    if obsolete_ids:
        sb.table(EQUIP_TABLE)\
          .update({"is_obsolete": True, "obsolete_at": now})\
          .in_("id", obsolete_ids)\
          .execute()

    # ──────────────── 4. Inserts (ond./panneaux) ------------------------
    if equip_inserts:
        sb.table(EQUIP_TABLE).insert(equip_inserts).execute()

    # ──────────────── 5. Patch compte panneaux -------------------------
    for patch in panel_patches:
        equip_id = patch.pop("id")
        sb.table(EQUIP_TABLE).update(patch).eq("id", equip_id).execute()


# ───────────────────────── Phase 1 : Import VCOM → DB

def sync_vcom_to_db(sb: SupabaseClient, vc: VCOMAPIClient) -> None:
    logger.info("[VCOM] Début import VCOM → DB")
    # 1. Récupérer tous les systèmes VCOM
    try:
        vcom_systems = vc.get_systems()
    except Exception as err:
        logger.exception("Unable to fetch systems from VCOM: %s", err)
        return

    # 2. Récupérer les vcom_system_key déjà connus
    existing_keys_res = (
        sb.table("sites_mapping").select("vcom_system_key").execute()
    )
    existing_keys = {row["vcom_system_key"] for row in (existing_keys_res.data or []) if row["vcom_system_key"]}

    new_sites = [s for s in vcom_systems if s["key"] not in existing_keys]
    logger.info("[VCOM] %d nouveau(x) site(s) à insérer", len(new_sites))

    # 3. Insérer chaque nouveau site + équipements modules/onduleurs
    for sys_data in new_sites:
        try:
            _insert_vcom_site(sb, vc, sys_data)
        except Exception:
            logger.exception("[VCOM] Failure inserting VCOM system %s", sys_data.get("key"))

    logger.info("[VCOM] Fin import VCOM → DB")


def _insert_vcom_site(sb: SupabaseClient, vc: VCOMAPIClient, sys_data: Dict) -> None:
    """Insère un site VCOM + équipements de base dans Supabase."""
    vcom_key = sys_data["key"]
    tech = vc.get_technical_data(vcom_key)
    detail_sysd = vc.get_system_details(vcom_key)
    logger.debug("[VCOM] Insertion site %s", vcom_key)
    addr = build_address(detail_sysd.get("address", {}))

    site_payload = {
        "vcom_system_key": vcom_key,
        "name": sys_data.get("name") or vcom_key,
        "latitude": detail_sysd.get("coordinates", {}).get("latitude"),
        "longitude": detail_sysd.get("coordinates", {}).get("longitude"),
        "address": addr,
        "commission_date": detail_sysd.get("commissionDate"),
        "nominal_power": tech.get("nominalPower"),
        "site_area": tech.get("siteArea"),
        "created_at": now_iso(),
    }
    res = sb.table("sites_mapping")\
            .upsert(site_payload, on_conflict="vcom_system_key")\
            .execute()
    site_db_id = res.data[0]["id"]

    # Equipement Modules
    panels = tech.get("panels") or []
    if panels:
        p = panels[0]  # assume single entry (vendor/model/count)
        module_row = {
            "vcom_device_id":    f"MODULES-{vcom_key}",
            "yuman_material_id": None,           # sera lié plus tard
            "category_id":       CAT_MODULE,
            "eq_type": "module",
            "brand":             p.get("vendor"),    # colonne brand
            "model":             p.get("model"),
            "name":              p.get("model"),     # nom obligatoire
            "serial_number":     None,
            "count":             p.get("count"),
            "vcom_system_key":   vcom_key,
            "site_id":           site_db_id,
            "created_at":        now_iso(),
        }
    # upsert au lieu d'insert brut
        sb_upsert(EQUIP_TABLE, [module_row], sb, "vcom_device_id")

    # Equipements Onduleurs
    inv_rows: List[Dict[str, Any]] = []
    try:
        invs = vc.get_inverters(vcom_key)
    except Exception:
        logger.warning("[VCOM] No inverter list for %s", vcom_key)
        invs = []

    for inv in invs:
        details = vc.get_inverter_details(vcom_key, inv["id"])
        inv_rows.append({
            "vcom_device_id":    inv["id"],
            "yuman_material_id": None,
            "category_id":       CAT_INVERTER,
            "eq_type": "inverter", 
            "brand":             details.get("vendor"),
            "model":             details.get("model"),
            "name":              inv.get("name"),     # nom fourni par VCOM
            "serial_number":     inv.get("serial"),
            "vcom_system_key":   vcom_key,
            "site_id":           site_db_id, 
            "created_at":        now_iso(),
        })
    if inv_rows:
        sb_upsert(EQUIP_TABLE, inv_rows, sb, "vcom_device_id")


    # Log
    sb.table("sync_logs").insert({
        "source": "vcom",
        "action": "vcom_import",
        "payload": {"vcom_system_key": vcom_key},
        "created_at": now_iso(),
    }).execute()


# ───────────────────────── Phase 2 : Import Yuman → DB

def sync_yuman_to_db(sb: SupabaseClient, yc: YumanClient) -> None:
    """Import complet Yuman V2 → DB (clients, sites, équipements)."""
    logger.info("[YUMAN] Début import Yuman V2 → DB")
    stats = {
        "clients":         sync_clients(yc, sb),
        "sites":           None,
        "site_conflicts":  None,
        "equipments":      None,
    }
    stats["sites"], stats["site_conflicts"] = sync_sites(yc, sb)
    stats["equipments"] = sync_equipments(yc, sb)

    sb.table("sync_logs").insert({
        "source": "yuman",
        "action": "import_v2",
        "payload": stats,
        "created_at": now_iso(),
    }).execute()
    logger.info("[YUMAN] Fin import Yuman V2 – %s", stats)


# ---------------------------------------------------------------------------
# Sync Yuman – V2 (clients, sites, équipements)
# ---------------------------------------------------------------------------

def sync_clients(yc: YumanClient, sb: SupabaseClient, dry: bool = False) -> int:
    rows = [{
        "yuman_client_id": c["id"],
        "code": c.get("code"),
        "name": c["name"],
        "created_at": c.get("created_at") or now_iso(),
    } for c in yc.list_clients()]
    sb_upsert(CLIENT_TABLE, rows, sb, "yuman_client_id", dry)
    return len(rows)


def sync_sites(yc: YumanClient, sb: SupabaseClient, dry: bool = False) -> Tuple[int, int]:
    existing = sb.table(SITE_TABLE).select("*").execute().data
    by_vcom = {s["vcom_system_key"]: s for s in existing if s.get("vcom_system_key")}
    by_yuman_id  = {s["yuman_site_id"]:   s for s in existing if s.get("yuman_site_id")}
    clients = sb.table(CLIENT_TABLE).select("id,yuman_client_id").execute().data
    client_map = {c["yuman_client_id"]: c["id"] for c in clients}

    inserted, upsert_yid, conflicts = [], [], []
    for det in yc.list_sites(embed="fields,client,category"):
        cvals = {f["name"]: f.get("value") for f in det.get("_embed", {}).get("fields", [])}
        row = {
            "yuman_site_id": det["id"],
            "client_map_id": client_map.get(det.get("client_id")),
            "code": det.get("code"),
            "name": det.get("name"),
            "address": det.get("address"),
            "latitude": det.get("latitude"),
            "longitude": det.get("longitude"),
            "created_at": det.get("created_at") or now_iso(),
        }
        # champs custom → colonnes
        for src, col in SITE_CUSTOM_FIELDS.items():
            v = (cvals.get(src) or "").strip() or None
            row[col] = v

        if row.get("vcom_system_key") and (db := by_vcom.get(row["vcom_system_key"])):
            patch = {k: v for k, v in row.items() if v and not db.get(k)}
            if patch:
                sb.table(SITE_TABLE).update(patch).eq("vcom_system_key", row["vcom_system_key"]).execute()
        else:
            db_row = by_yuman_id.get(row["yuman_site_id"])
            if db_row:
                # Le site existe déjà : on ne crée pas de doublon,
                # on ne génère pas de conflit ; on complète juste les colonnes manquantes.
                patch = {k: v for k, v in row.items() if v and not db_row.get(k)}
                if patch:
                    sb.table(SITE_TABLE).update(patch)\
                                        .eq("yuman_site_id", row["yuman_site_id"]).execute()
            else:
                (inserted if row.get("vcom_system_key") else upsert_yid).append(row)
                if not row.get("vcom_system_key"):
                    conflicts.append({...})

    sb_upsert(SITE_TABLE, inserted, sb, "vcom_system_key", dry)
    sb_upsert(SITE_TABLE, upsert_yid, sb, "yuman_site_id", dry)
    sb_upsert(CONFLICT_TABLE, conflicts, sb, "yuman_site_id,issue", dry)
    return len(inserted) + len(upsert_yid), len(conflicts)


def sync_equipments(yc: YumanClient, sb: SupabaseClient, dry: bool = False) -> int:
    # --- 0. Pré‐requis ----------------------------------------------------------------
    sites = sb.table(SITE_TABLE).select("id,yuman_site_id,vcom_system_key").execute().data
    yid_to_pk = {s["yuman_site_id"]: s["id"] for s in sites}
    with_vcom    = {s["yuman_site_id"] for s in sites if s.get("vcom_system_key")}
    without_vcom = {s["yuman_site_id"] for s in sites if not s.get("vcom_system_key")}

    existing = (
        sb.table(EQUIP_TABLE)
          .select("id,site_id,category_id,vcom_device_id,yuman_material_id")
          .in_("category_id", list(ALLOWED_EQUIP_CATEGORIES))
          .execute().data
    )
    inv_lookup = {(r["site_id"], r.get("vcom_device_id")): r for r in existing if r["category_id"] == CAT_INVERTER}
    mod_lookup = {r["site_id"]: r for r in existing if r["category_id"] == CAT_MODULE}

    inserts, patches, pending_fv = [], [], {}

    # --- 1. Parcours Yuman --------------------------------------------------------------
    for cat in ALLOWED_EQUIP_CATEGORIES:
        for eq in yc.list_materials(category_id=cat, embed="fields,site,category"):
            y_site   = eq.get("site_id")
            pk_site  = yid_to_pk.get(y_site)
            if pk_site is None:
                continue

            fields = eq.get("_embed", {}).get("fields", [])

            # 1-A. Site déjà mappé VCOM → patch éventuel
            if y_site in with_vcom:
                key = (pk_site, next((f["value"] for f in fields if f["name"] == CUSTOM_FIELD_INVERTER_ID), None)) \
                      if cat == CAT_INVERTER else pk_site
                row = inv_lookup.get(key) if cat == CAT_INVERTER else mod_lookup.get(key)
                if row and not row.get("yuman_material_id"):
                    patches.append({"id": row["id"], "yuman_material_id": eq["id"]})
                continue

            # 1-B. Site uniquement Yuman → insert (mapping provisoire sans vcom)
            if y_site not in without_vcom:
                continue
            inserts.append({
                "yuman_material_id": eq["id"],
                "site_id":          pk_site,
                "category_id":      cat,
                "name":             eq.get("name"),
                "brand":            eq.get("brand"),
                "model":            eq.get("model"),
                "serial_number":    eq.get("serial_number"),
                "created_at":       eq.get("created_at") or now_iso(),
                "vcom_device_id":   next((f["value"] for f in fields if f["name"] == CUSTOM_FIELD_INVERTER_ID), None),
            })

            # Stocke en attente les field values (sera remappé après insert)
            for f in fields:
                pending_fv[(eq["id"], f["name"])] = {
                    "equipment_id": eq["id"],          # provisoire, sera remplacé par PK DB
                    "field_name":   f["name"],
                    "value":        f.get("value"),
                    "created_at":   now_iso(),
                }

    # --- 2. Upsert équipements (insert + patch) ----------------------------------------
    sb_upsert(EQUIP_TABLE, inserts, sb, "yuman_material_id", dry)

    if not dry:
        existing_ids = {r["yuman_material_id"] for r in existing if r.get("yuman_material_id")}
        for p in patches:
            if p["yuman_material_id"] in existing_ids:
                continue
            sb.table(EQUIP_TABLE)\
              .update({"yuman_material_id": p["yuman_material_id"]})\
              .eq("id", p["id"]).execute()
            existing_ids.add(p["yuman_material_id"])

    # --- 3. Remap field_values vers la PK DB -------------------------------------------
    if not dry and pending_fv:
        yuman_ids = [ymid for (ymid, _) in pending_fv.keys()]
        rows = (
            sb.table(EQUIP_TABLE)
              .select("id,yuman_material_id")
              .in_("yuman_material_id", yuman_ids)
              .execute().data or []
        )
        y2db = {r["yuman_material_id"]: r["id"] for r in rows}

        fv_rows = []
        for (ym_id, fname), payload in pending_fv.items():
            db_id = y2db.get(ym_id)
            if db_id:
                payload["equipment_id"] = db_id
                fv_rows.append(payload)

        sb_upsert(FIELD_VALUES_TABLE, fv_rows, sb, "equipment_id,field_name", dry)

    return len(inserts) + len(patches)



# ───────────────────────── Phase 3 : Détecter conflits de site

def detect_site_conflicts(sb: SupabaseClient) -> Dict[str, List[Dict]]:
    """Retourne deux listes : vcom_only et yuman_only."""
    vcom_only = (
        sb.table("sites_mapping")
        .select("id,name,vcom_system_key")
        .is_("yuman_site_id", None)
        .eq("ignore_site", False)
        .execute()
        .data
        or []
    )
    yuman_only = (
        sb.table("sites_mapping")
        .select("id,name,yuman_site_id")
        .is_("vcom_system_key", None)
        .eq("ignore_site", False)
        .execute()
        .data
        or []
    )
    return {"vcom_only": vcom_only, "yuman_only": yuman_only}


# ───────────────────────── Phase 4 : Interaction utilisateur

def prompt_user_mapping(conflicts: Dict[str, List[Dict]]) -> Dict[str, str]:
    if not conflicts["vcom_only"] and not conflicts["yuman_only"]:
        logger.info("[CONFLICT] Aucun conflit de site détecté.")
        return {}

    print("\n================== CONFLITS DE SITES ==================")
    print("Sites VCOM sans Yuman :")
    v_map = {}
    for idx, row in enumerate(conflicts["vcom_only"], 1):
        v_map[str(idx)] = row
        print(f"vcom {idx}: {row['name']}")

    print("\nSites Yuman sans VCOM :")
    y_map = {}
    for idx, row in enumerate(conflicts["yuman_only"], 1):
        y_map[str(idx)] = row
        print(f"yuman {idx}: {row['name']}")

    print("\nVeuillez associer les sites, syntaxe exemple :")
    print("vcom 1 = yuman 2; vcom 2 = yuman 1; yuman 3 ignorer; vcom 4 creer")
    user_in = input("> ")

    mapping: Dict[str, str] = {}
    token_re = re.compile(r"(vcom|yuman)\s+(\d+)\s*(=|ignorer|creer)?\s*(?:yuman|vcom)?\s*(\d+)?", re.I)
    for part in user_in.split(";"):
        part = part.strip()
        if not part:
            continue
        m = token_re.match(part)
        if not m:
            print(f"⛔️ Syntaxe invalide : '{part}' – ignoré")
            continue
        side, idx1, op, idx2 = m.groups()
        side = side.lower()
        op = (op or "").lower()
        if op == "=" and idx2 is None:
            print(f"⛔️ Syntaxe '=' mais pas de seconde référence : '{part}' – ignoré")
            continue
        key1 = ("v" if side == "vcom" else "y") + idx1
        if op == "=":
            key2 = ("y" if side == "vcom" else "v") + idx2
            mapping[key1] = key2
        elif op == "ignorer":
            mapping[key1] = "IGNORE"
        elif op == "creer":
            mapping[key1] = "CREATE"
        else:
            print(f"⛔️ Opération inconnue dans '{part}' – ignoré")
    return mapping


# ───────────────────────── Phase 5 : Résolution des conflits de site

def resolve_site_conflicts(sb: SupabaseClient, yc: YumanClient, mapping: Dict[str, str]) -> None:
    if not mapping:
        logger.info("[CONFLICT] Pas de conflit à résoudre.")
        return

    logger.info("[CONFLICT] Début résolution de %d mappage(s)", len(mapping))

    # On fige les listes avant de commencer
    conf_snapshot = detect_site_conflicts(sb)
    vcom_only  = conf_snapshot["vcom_only"]
    yuman_only = conf_snapshot["yuman_only"]

    for key1, action in mapping.items():
        prefix, idx = key1[0], key1[1:]
        if prefix == "v":
            row_v = vcom_only[int(idx) - 1]
            row_y = None
        else:
            row_y = yuman_only[int(idx) - 1]
            row_v = None

        if action == "IGNORE":
            _ignore_site(sb, row_v or row_y)
            continue
        if action == "CREATE":
            # sera géré plus tard par create_missing_yuman_sites
            continue

        # action est une autre clef
        row_other_prefix, row_other_idx = action[0], action[1:]
        if row_other_prefix == "v":
            row_v = vcom_only[int(row_other_idx) - 1]
        else:
            row_y = yuman_only[int(row_other_idx) - 1]

        if not row_v or not row_y:
            logger.warning("[CONFLICT] Impossible de récupérer lignes pour mapping %s = %s", key1, action)
            continue
        _merge_sites(sb, yc, row_v, row_y)

    logger.info("[CONFLICT] Résolution terminée")


def _ignore_site(sb: SupabaseClient, row: Dict) -> None:
    # 1️ Marque la ligne mapping comme ignorée
    sb.table("sites_mapping")\
      .update({"ignore_site": True})\
      .eq("id", row["id"]).execute()

    # 2️ Résout le(s) conflit(s) correspondant(s)
    if row.get("yuman_site_id"):
        sb.table(CONFLICT_TABLE)\
          .update({"resolved": True, "resolved_at": now_iso()})\
          .eq("entity", "site")\
          .eq("yuman_site_id", row["yuman_site_id"]).execute()
    else:  # cas VCOM-only éventuel
        sb.table(CONFLICT_TABLE)\
          .update({"resolved": True, "resolved_at": now_iso()})\
          .eq("entity", "site")\
          .is_("yuman_site_id", None)\
          .eq("vcom_system_key", row.get("vcom_system_key")).execute()

    # 3️ Log
    sb.table("sync_logs").insert({
        "source": "user",
        "action": "ignore_site",
        "payload": {"site_id": row["id"]},
        "created_at": now_iso(),
    }).execute()



def _merge_sites(sb: SupabaseClient, yc, v_row: Dict, y_row: Dict) -> None:
    """Master = VCOM line (v_row). Copy info from y_row then delete y_row."""
    logger.info("[MERGE] Fusion site VCOM id=%d ⇐ Yuman id=%d", v_row["id"], y_row["id"])

    # 1. Prépare les champs à déporter (on retire yuman_site_id pour l'instant)
    update_fields, pending_yuman_id = {}, None
    for col in ("yuman_site_id", "client_map_id", "aldi_id", "aldi_store_id", "project_number_cp"):
        if not v_row.get(col) and y_row.get(col):
            if col == "yuman_site_id":
                pending_yuman_id = y_row[col]
            else:
                update_fields[col] = y_row[col]

    # 2. Réaffecter les équipements — indispensable avant la suppression
    sb.table("equipments_mapping").update({"site_id": v_row["id"]}).eq("site_id", y_row["id"]).execute()

    # 3. Supprimer la ligne Yuman
    sb.table("sites_mapping").delete().eq("id", y_row["id"]).execute()

    # 4. Mettre à jour v_row : d’abord les champs non-uniques, puis yuman_site_id
    if update_fields:
        sb.table("sites_mapping").update(update_fields).eq("id", v_row["id"]).execute()
    if pending_yuman_id:
        sb.table("sites_mapping").update({"yuman_site_id": pending_yuman_id}).eq("id", v_row["id"]).execute()
    
    # stocke la clé VCOM dans les custom fields du site Yuman
    try:
        yc.update_site(
            pending_yuman_id,
            {
                "fields": [
                    {
                        "blueprint_id": 4,
                        "name":         "System Key (Vcom ID)",
                        "value":        v_row["vcom_system_key"],
                    }
                ]
            },
        )
    except Exception as e:
        logger.warning(
            "[YUMAN] Impossible de poser la VCOM key %s sur site %s : %s",
            v_row["vcom_system_key"], pending_yuman_id, e,
        )
    #  Marquer le conflit « missing_vcom_system_key » comme résolu
    if y_row.get("yuman_site_id"):
        sb.table(CONFLICT_TABLE)\
          .update({"resolved": True, "resolved_at": now_iso()})\
          .eq("entity", "site")\
          .eq("yuman_site_id", y_row["yuman_site_id"]).execute()

    # 5. Log
    sb.table("sync_logs").insert({
        "source": "user",
        "action": "merge_site",
        "payload": {"from": y_row["id"], "into": v_row["id"]},
        "created_at": now_iso(),
    }).execute()

    # 6️ Nettoyage équipements (modules + log onduleurs)
    _cleanup_equipment_after_merge(sb, v_row["id"])

# ---------------------------------------------------------------------------
# Post-merge : nettoyage des doublons d'équipements
# ---------------------------------------------------------------------------

def _cleanup_equipment_after_merge(sb: SupabaseClient, site_id: int) -> None:
    """Fusionne les modules doublons et logue les onduleurs en double."""
    # ---- Modules ----------------------------------------------------------
    mods = (
        sb.table(EQUIP_TABLE)
          .select("*")
          .eq("site_id", site_id)
          .eq("category_id", CAT_MODULE)
          .execute()
          .data
          or []
    )
    if len(mods) > 1:
        keep = next((m for m in mods if m.get("vcom_device_id")), mods[0])
        drop = next((m for m in mods if m["id"] != keep["id"]), None)
        if drop:
            # 1️ Libère la clé unique avant de copier
            #    mais d'abord on déplace les field_values
            sb.table(FIELD_VALUES_TABLE)\
              .update({"equipment_id": keep["id"]})\
              .eq("equipment_id", drop["id"]).execute()
            if drop.get("yuman_material_id"):
                sb.table(EQUIP_TABLE)\
                .update({"yuman_material_id": None})\
                .eq("id", drop["id"]).execute()

            # 2️ Copie dans keep si absent
            if not keep.get("yuman_material_id") and drop.get("yuman_material_id"):
                sb.table(EQUIP_TABLE)\
                .update({"yuman_material_id": drop["yuman_material_id"]})\
                .eq("id", keep["id"]).execute()

            # 3️ Supprime la ligne redondante
            sb.table(EQUIP_TABLE).delete().eq("id", drop["id"]).execute()

            sb.table("sync_logs").insert({
                "source": "auto",
                "action": "module_merge",
                "payload": {"site_id": site_id, "kept": keep["id"], "deleted": drop["id"]},
                "created_at": now_iso(),
            }).execute()

    # ---- Onduleurs : détection de doublons -------------------------------
    invs = (
        sb.table(EQUIP_TABLE)
          .select("id,vcom_device_id,serial_number")
          .eq("site_id", site_id)
          .eq("category_id", CAT_INVERTER)
          .execute()
          .data
          or []
    )
    seen, duplicates = {}, set()
    for inv in invs:
        key = inv.get("vcom_device_id") or inv.get("serial_number")
        if key and key in seen:
            duplicates.add(key)
        elif key:
            seen[key] = inv["id"]

    if duplicates:
        rows = [{
            "entity": "equipment",
            "site_id": site_id,
            "issue": f"duplicate_inverter_{key}",
            "created_at": now_iso(),
        } for key in duplicates]
        sb_upsert(CONFLICT_TABLE, rows, sb, "site_id,issue")

# ───────────────────────── Phase 6 : Résolution clients résiduels

def resolve_clients_for_sites(sb: SupabaseClient, yc: YumanClient) -> None:
    """Pour chaque site sans client_map_id : tentative automatique puis prompt."""
    sites_no_client = (
        sb.table("sites_mapping")
        .select("id,name,client_map_id")
        .is_("client_map_id", None)
        .eq("ignore_site", False)
        .execute()
        .data
        or []
    )
    if not sites_no_client:
        logger.info("[CLIENT] Tous les sites ont un client.")
        return

    # Précharge mapping name_addition → client_map_id
    name_add_rows = sb.table("clients_mapping").select("id,name_addition").execute().data or []
    add_map = {r["name_addition"]: r["id"] for r in name_add_rows if r.get("name_addition")}

    for site in sites_no_client:
        region_match = re.search(r"\(([^)]+)\)", site["name"] or "")
        region = region_match.group(1).strip() if region_match else None
        chosen_client_id = None
        if region and region in add_map:
            chosen_client_id = add_map[region]
        else:
            print(f"Site '{site['name']}' sans client.")
            choice = input("Entrez ID client existant ou 0 pour créer : ")
            if choice.strip() == "0":
                nom = input("Nom du client : ")
                adresse = input("Adresse : ")
                cli_created = yc.create_client({"name": nom, "address": adresse})

                # 1️⃣ Upsert immédiat dans `clients_mapping`
                sb_upsert(
                    CLIENT_TABLE,
                    [{
                        "yuman_client_id": cli_created["id"],
                        "code": cli_created.get("code"),
                        "name": cli_created["name"],
                        "created_at": now_iso(),
                    }],
                    sb,
                    "yuman_client_id",
                )

                # 2️⃣ Récupération de la PK pour l’associer au site
                client_row = (
                    sb.table(CLIENT_TABLE)
                      .select("id")
                      .eq("yuman_client_id", cli_created["id"])
                      .single()
                      .execute()
                      .data
                )
                chosen_client_id = client_row["id"]
            else:
                chosen_client_id = int(choice)
        if chosen_client_id:
            sb.table("sites_mapping").update({"client_map_id": chosen_client_id}).eq("id", site["id"]).execute()
            sb.table("sync_logs").insert({
                "source": "user",
                "action": "client_resolved",
                "payload": {"site_id": site["id"], "client_map_id": chosen_client_id},
                "created_at": now_iso(),
            }).execute()


# ───────────────────────── Phase 7 : Création des sites Yuman manquants
def _clean_site_name(raw: str) -> str:
    # supprime préfixe "00/01 ", "France", et la parenthèse finale
    name = re.sub(r"^\d{2}\s+", "", raw)           # 01 (...)
    name = re.sub(r"\s+France\s+", " ", name, flags=re.I)
    name = re.sub(r"\s*\([^)]*\)\s*$", "", name)   # (Cestas)
    return name.strip()


def create_missing_yuman_sites(sb: SupabaseClient, yc: YumanClient) -> None:
    """Crée sur Yuman les sites VCOM encore sans yuman_site_id."""
    sites_to_create = (
        sb.table("sites_mapping")
        .select("*")
        .is_("yuman_site_id", None)
        .eq("ignore_site", False)
        .execute()
        .data
        or []
    )
    if not sites_to_create:
        logger.info("[YUMAN] Aucun site VCOM à créer sur Yuman.")
        return

    for site in sites_to_create:
        if not site.get("client_map_id"):
            logger.warning("[YUMAN] Site id=%d sans client, ignoré.", site["id"])
            continue
        client_row = sb.table("clients_mapping").select("yuman_client_id").eq("id", site["client_map_id"]).single().execute().data
        if not client_row:
            logger.warning("[YUMAN] Client map id=%d introuvable.", site["client_map_id"])
            continue
        # --------- prépare les custom fields sous forme de tableau --------
        fields_list: list[dict[str, Any]] = []
        # 4 = System Key (Vcom ID)
        fields_list.append({
            "blueprint_id": 4,
            "name":         "System Key (Vcom ID)",
            "value":        site["vcom_system_key"],
        })
        # 5 = Nominal Power (kWc) si présent
        if site.get("nominal_power") not in (None, ""):
            fields_list.append({
                "blueprint_id": 5,
                "name":         "Nominal Power (kWc)",
                "value":        site["nominal_power"],
            })
        # 6 = Commission Date si présent
        if site.get("commission_date"):
            fields_list.append({
                "blueprint_id": 6,
                "name":         "Commission Date",
                "value":        site["commission_date"],
            })

        payload = {
            "client_id": client_row["yuman_client_id"],
            "name":      _clean_site_name(site["name"]),
            "address":   site.get("address") or "",
            "fields":    fields_list,
        }



        created_site = yc.create_site(payload)
        yuman_site_id = created_site["id"]
        sb.table("sites_mapping").update({"yuman_site_id": yuman_site_id}).eq("id", site["id"]).execute()
        # Equipements standards à créer (centrale, modules, onduleurs)
        _create_yuman_equipments(sb, yc, site, yuman_site_id)
        sb.table("sync_logs").insert({
            "source": "yuman",
            "action": "site_create",
            "payload": {"site_id_db": site["id"], "yuman_site_id": yuman_site_id},
            "created_at": now_iso(),
        }).execute()


# ---------------------------------------------------------------------------
# Création des équipements Yuman pour un site nouvellement créé
# ---------------------------------------------------------------------------
def _create_yuman_equipments(sb: SupabaseClient, yc: YumanClient,
                             site_row: Dict, yuman_site_id: int) -> None:
    """
    • Crée la centrale & les équipements côté Yuman uniquement.
    • Met à jour (mais ne crée pas) la ligne Modules dans equipments_mapping.
    • Met à jour les lignes onduleurs existantes avec le yuman_material_id.
    """
    # ─── Centrale (pure création Yuman, rien dans la DB) ───────────────────
    yc.create_material({
        "site_id": yuman_site_id,
        "name": "Centrale",
        "category_id": CAT_CENTRALE,
    })

    # ─── Modules : MAJ de la ligne VCOM, création uniquement si absente ───
    module_ym = yc.create_material({
        "site_id": yuman_site_id,
        "name": "Modules",
        "category_id": CAT_MODULE,
    })

    mod_row = (
        sb.table(EQUIP_TABLE)
          .select("id,yuman_material_id")
          .eq("site_id", site_row["id"])
          .eq("category_id", CAT_MODULE)
          .eq("vcom_device_id", f"MODULES-{site_row['vcom_system_key']}")
          .maybe_single()
          .execute()
          .data               # -> dict   ou None s’il n’y a pas de ligne
    )

    if mod_row:
        if not mod_row["yuman_material_id"]:
            sb.table(EQUIP_TABLE)\
              .update({"yuman_material_id": module_ym["id"]})\
              .eq("id", mod_row["id"]).execute()
    else:
        # Cas exceptionnel : la ligne Modules n’existait pas encore
        sb.table(EQUIP_TABLE).insert({
            "yuman_material_id": module_ym["id"],
            "category_id": CAT_MODULE,
            "eq_type": "module",
            "vcom_system_key": site_row["vcom_system_key"],
            "vcom_device_id": f"MODULES-{site_row['vcom_system_key']}",
            "site_id": site_row["id"],
            "name": "Modules",
            "created_at": now_iso(),
        }).execute()

    # ─── Onduleurs : mise à jour des lignes existantes ────────────────────
    inv_rows = (
        sb.table(EQUIP_TABLE)
          .select("*")
          .eq("site_id", site_row["id"])
          .eq("category_id", CAT_INVERTER)   # ← catégorie plutôt qu’eq_type
          .execute()
          .data
          or []
    )
    for inv in inv_rows:
        y_inv = yc.create_material({
            "site_id": yuman_site_id,
            "name": inv["name"],
            "category_id": CAT_INVERTER,
            "brand": inv.get("brand"),
            "model": inv.get("model"),
            "serial_number": inv.get("serial_number"),
        })
        if not inv.get("yuman_material_id"):
            sb.table(EQUIP_TABLE)\
              .update({"yuman_material_id": y_inv["id"]})\
              .eq("id", inv["id"]).execute()


# ───────────────────────── Entrée principale

def main():
    # Variables d'environnement
    sb_url = os.getenv("SUPABASE_URL")
    sb_key = os.getenv("SUPABASE_SERVICE_KEY")
    yuman_token = os.getenv("YUMAN_TOKEN")
    if not all((sb_url, sb_key, yuman_token)):
        logger.error("Missing required env vars SUPABASE_URL, SUPABASE_SERVICE_KEY, YUMAN_TOKEN.")
        sys.exit(1)

    sb = create_client(sb_url, sb_key)
    vc = VCOMAPIClient()
    yc = YumanClient(yuman_token)

    with execution_lock():
        sync_vcom_to_db(sb, vc)
        sync_yuman_to_db(sb, yc)
        conflicts = detect_site_conflicts(sb)
        mapping = prompt_user_mapping(conflicts)
        resolve_site_conflicts(sb, yc, mapping)
        resolve_clients_for_sites(sb, yc)
        create_missing_yuman_sites(sb, yc)

    logger.info("✅ Synchronisation terminée avec succès.")


if __name__ == "__main__":
    main()
