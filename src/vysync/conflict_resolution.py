#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Résolution MANUELLE des conflits de sites VCOM ↔ Yuman.

Usage (appelé par cli.py) :
    import_yuman_sites_in_mapping(sb_adapter, y_adapter)
    detect_and_resolve_site_conflicts(sb_adapter, y_adapter)

Fonctions :
    • import_yuman_sites_in_mapping …  ↳ alimente `sites_mapping` + inscrit les
      conflits « missing_vcom_system_key ».
    • detect_and_resolve_site_conflicts …  ↳ interaction console et application
      des actions (merge / ignore).
    – helpers internes _ignore_site, _merge_sites, _cleanup_equipment_after_merge
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional

from vysync.app_logging import init_logger
from vysync.models import Site
from vysync.yuman_client import YumanClient
from vysync.adapters.supabase_adapter import SupabaseAdapter

logger = init_logger(__name__)

SITE_TABLE = "sites_mapping"
EQUIP_TABLE = "equipments_mapping"
CONFLICT_TABLE = "conflicts"
SYNC_LOGS_TABLE = "sync_logs"
FIELD_VALUES_TABLE = "equipment_field_values"

# ──────────────────────────────  PUBLIC API  ────────────────────────────
def import_yuman_sites_in_mapping(sb_adapter: SupabaseAdapter,
                                  y_adapter) -> None:
    """
    Insère/complète dans `sites_mapping` les sites Yuman dépourvus de clé VCOM
    et crée une ligne `conflicts` (issue : missing_vcom_system_key).
    """
    sb = sb_adapter.sb
    yc: YumanClient = y_adapter.yc

    existing = sb.table(SITE_TABLE).select("*").execute().data or []
    by_yid = {r["yuman_site_id"]: r for r in existing if r.get("yuman_site_id")}
    by_vkey = {r["vcom_system_key"]: r for r in existing if r.get("vcom_system_key")}

    # map client→mapping id pour stocker le client sur les nouveaux sites
    clients = sb.table("clients_mapping").select("id,yuman_client_id") \
                                         .execute().data or []
    client_map = {c["yuman_client_id"]: c["id"] for c in clients}

    upsert_rows, conflict_rows = [], []
    for det in yc.list_sites(embed="fields,client"):
        cvals = {f["name"]: f.get("value") for f in det.get("_embed", {}).get("fields", [])}
        vkey = (cvals.get("System Key (Vcom ID)") or "").strip() or None
        row = {
            "name": det.get("name"),
            "address": det.get("address"),
            "code": det.get("code"),
            "latitude": det.get("latitude"),
            "longitude": det.get("longitude"),
            "client_map_id": client_map.get(det.get("client_id")),
            "yuman_site_id": det["id"],
            "vcom_system_key": vkey,
            "aldi_id": (cvals.get("ALDI ID") or "").strip() or None,
            "aldi_store_id": (cvals.get("ID magasin (n° interne Aldi)") or "").strip() or None,
            "project_number_cp": (cvals.get("Project number (Centroplan ID)") or "").strip() or None,
            "created_at": det.get("created_at") or _now(),
        }
        if vkey:
            # Si déjà présent côté VCOM : mise à jour éventuelle
            if vkey in by_vkey:
                patch = {k: v for k, v in row.items()
                         if v and not by_vkey[vkey].get(k)}
                if patch:
                    sb.table(SITE_TABLE).update(patch) \
                                        .eq("vcom_system_key", vkey).execute()
            else:
                upsert_rows.append(row)
        
        else:      # ----------- pas de clé VCOM  → potentiel conflit ----------
            existing_row = by_yid.get(det["id"])
            ignore_flag  = existing_row and existing_row.get("ignore_site")

            if existing_row:
                # Mise à jour éventuelle des champs manquants
                patch = {k: v for k, v in row.items()
                         if v and not existing_row.get(k)}
                if patch:
                    sb.table(SITE_TABLE).update(patch) \
                                        .eq("yuman_site_id", det["id"]).execute()
            else:
                # nouvelle ligne mapping (par défaut ignore_site = False)
                row["ignore_site"] = False
                upsert_rows.append(row)

            # Créer un conflit **uniquement si le site n’est PAS ignoré**
            if not ignore_flag:
                conflict_rows.append({
                    "entity": "site",
                    "yuman_site_id": det["id"],
                    "issue": "missing_vcom_system_key",
                    "created_at": _now(),
                })

    if upsert_rows:
        sb.table(SITE_TABLE).upsert(upsert_rows,
                                    on_conflict="yuman_site_id").execute()
    if conflict_rows:
        sb.table(CONFLICT_TABLE).upsert(conflict_rows,
                                        on_conflict="yuman_site_id,issue").execute()


def detect_and_resolve_site_conflicts(sb_adapter: SupabaseAdapter,
                                      y_adapter) -> None:
    """
    • Cherche les lignes mapping sans pendant VCOM ou Yuman (ignore_site=FALSE).
    • Affiche les conflits, interroge l’utilisateur (« vcom 1 = yuman 2 », …).
    • Applique les résolutions (merge / ignore) puis journalise.
    """
    vcom_only, yuman_only = _detect_site_conflicts(sb_adapter.sb)
    if not vcom_only and not yuman_only:
        logger.info("[CONFLICT] aucun conflit détecté.")
        return

    mapping = _prompt_user_mapping(vcom_only, yuman_only)
    _resolve_site_conflicts(sb_adapter, y_adapter, mapping)


# ─────────────────────────────  INTERNAL  ───────────────────────────────
def _detect_site_conflicts(sb, ) -> Tuple[List[Dict], List[Dict]]:
    vcom_only = (
        sb.table(SITE_TABLE).select("id,name,vcom_system_key")
          .is_("yuman_site_id", None).eq("ignore_site", False).execute().data or []
    )
    yuman_only = (
        sb.table(SITE_TABLE).select("id,name,yuman_site_id")
          .is_("vcom_system_key", None).eq("ignore_site", False).execute().data or []
    )
    return vcom_only, yuman_only


def _prompt_user_mapping(vcom_only: List[Dict],
                         yuman_only: List[Dict]) -> Dict[str, str]:
    print("\n==============================================")
    print("CONFLITS DE SITES (résolution manuelle)")
    print("----------------------------------------------")
    if vcom_only:
        print("Sites VCOM sans Yuman :")
        for i, row in enumerate(vcom_only, 1):
            print(f"  vcom {i}: {row['name']}")
    if yuman_only:
        print("\nSites Yuman sans VCOM :")
        for i, row in enumerate(yuman_only, 1):
            print(f"  yuman {i}: {row['name']}")
    print("\nSyntaxes autorisées (séparées par ';') :")
    print("  vcom 1 = yuman 3      (fusionner)")
    print("  vcom 2 ignorer        (ignorer site VCOM)")
    print("  yuman 4 ignorer       (ignorer site Yuman)")
    print("  vcom 5 creer          (laisser créer plus tard)")
    cmd = input("\n> ").strip()

    mapping: Dict[str, str] = {}
    tok = re.compile(r"(vcom|yuman)\s+(\d+)\s*(=|ignorer|creer)?\s*(?:yuman|vcom)?\s*(\d+)?", re.I)
    for part in cmd.split(";"):
        part = part.strip()
        if not part:
            continue
        m = tok.match(part)
        if not m:
            print(f"⛔️ Syntaxe invalide : {part!r}")
            continue
        side, idx, op, idx2 = m.groups()
        key1 = ("v" if side.lower() == "vcom" else "y") + idx
        if (op or "").lower() in ("ignorer", "creer"):
            mapping[key1] = op.upper()
        elif op == "=" and idx2:
            key2 = ("y" if side.lower() == "vcom" else "v") + idx2
            mapping[key1] = key2
        else:
            print(f"⛔️ Incomplet : {part!r}")
    return mapping


def _resolve_site_conflicts(sb_adapter: SupabaseAdapter,
                            y_adapter,
                            mapping: Dict[str, str]) -> None:
    if not mapping:
        logger.info("[CONFLICT] aucune action choisie.")
        return

    sb = sb_adapter.sb
    yc: YumanClient = y_adapter.yc
    vcom_only, yuman_only = _detect_site_conflicts(sb)

    def _row(prefix: str, idx: int) -> Dict:
        lst = vcom_only if prefix == "v" else yuman_only
        return lst[idx - 1] if 0 < idx <= len(lst) else {}

    for src, action in mapping.items():
        p, i = src[0], int(src[1:])
        row_src = _row(p, i)
        if not row_src:
            logger.warning("Index %s introuvable – ignoré", src)
            continue

        if action in ("IGNORER", "CREER"):
            _ignore_site(sb, row_src) if action == "IGNORER" else None
            continue

        # action = 'vX' ou 'yY' → fusion
        p2, i2 = action[0], int(action[1:])
        row_dst = _row(p2, i2)
        if p == "v":   # src = VCOM, dst = YUMAN
            v_row, y_row = row_src, row_dst
        else:          # src = YUMAN
            v_row, y_row = row_dst, row_src
        _merge_sites(sb, yc, v_row, y_row)


def _ignore_site(sb, row: Dict) -> None:
    sb.table(SITE_TABLE).update({"ignore_site": True}) \
                        .eq("id", row["id"]).execute()
    sb.table(CONFLICT_TABLE).update({"resolved": True, "resolved_at": _now()}) \
        .eq("entity", "site") \
        .eq("yuman_site_id", row.get("yuman_site_id")).execute()
    sb.table(SYNC_LOGS_TABLE).insert({
        "source": "user",
        "action": "ignore_site",
        "payload": {"site_id": row["id"]},
        "created_at": _now(),
    }).execute()
    logger.info("[CONFLICT] site id=%s ignoré", row["id"])


def _merge_sites(sb, yc: YumanClient,
                 v_row: Dict, y_row: Dict) -> None:
    logger.info("[MERGE] VCOM id=%d  ⇐  Yuman id=%d", v_row["id"], y_row["id"])

    # transférer équipements Yuman → VCOM
    sb.table(EQUIP_TABLE).update({"site_id": v_row["id"]}) \
                         .eq("site_id", y_row["id"]).execute()

    # mettre à jour champs manquants côté VCOM
    patch = {}
    for col in ("aldi_id", "aldi_store_id", "project_number_cp", "client_map_id"):
        if not v_row.get(col) and y_row.get(col):
            patch[col] = y_row[col]
    if y_row.get("yuman_site_id"):
        patch["yuman_site_id"] = y_row["yuman_site_id"]
    if patch:
        sb.table(SITE_TABLE).update(patch).eq("id", v_row["id"]).execute()

    # supprimer la ligne Yuman
    sb.table(SITE_TABLE).delete().eq("id", y_row["id"]).execute()

    # mettre à jour champ custom côté Yuman
    try:
        yc.update_site(y_row["yuman_site_id"], {
            "fields": [{
                "blueprint_id": 13583,   # System Key (Vcom ID)
                "name": "System Key (Vcom ID)",
                "value": v_row["vcom_system_key"],
            }]
        })
    except Exception as exc:
        logger.warning("Yuman update_site failed : %s", exc)

    # marquer le conflit résolu
    sb.table(CONFLICT_TABLE).update({"resolved": True, "resolved_at": _now()}) \
        .eq("entity", "site") \
        .eq("yuman_site_id", y_row["yuman_site_id"]).execute()

    # log
    sb.table(SYNC_LOGS_TABLE).insert({
        "source": "user",
        "action": "merge_site",
        "payload": {"from": y_row["id"], "into": v_row["id"]},
        "created_at": _now(),
    }).execute()

    _cleanup_equipment_after_merge(sb, v_row["id"])


def _cleanup_equipment_after_merge(sb, site_id: int) -> None:
    # fusion éventuelle de doublons de modules
    mods = sb.table(EQUIP_TABLE).select("id,vcom_device_id,yuman_material_id") \
                                .eq("site_id", site_id) \
                                .eq("category_id", 11103).execute().data or []
    if len(mods) > 1:
        keep = next((m for m in mods if m["vcom_device_id"]), mods[0])
        for m in mods:
            if m["id"] == keep["id"]:
                continue
            # transférer les field_values
            sb.table(FIELD_VALUES_TABLE).update({"equipment_id": keep["id"]}) \
                                        .eq("equipment_id", m["id"]).execute()
            # transférer éventuel yuman_material_id
            if not keep["yuman_material_id"] and m["yuman_material_id"]:
                sb.table(EQUIP_TABLE).update({"yuman_material_id": m["yuman_material_id"]}) \
                                     .eq("id", keep["id"]).execute()
            sb.table(EQUIP_TABLE).delete().eq("id", m["id"]).execute()
    # détection de doublons onduleurs (enregistre un conflit)
    invs = sb.table(EQUIP_TABLE).select("id,vcom_device_id,serial_number") \
                                .eq("site_id", site_id) \
                                .eq("category_id", 11102).execute().data or []
    seen: Dict[str, int] = {}
    dups = set()
    for inv in invs:
        key = inv["vcom_device_id"] or inv["serial_number"]
        if key in seen:
            dups.add(key)
        else:
            seen[key] = inv["id"]
    if dups:
        sb.table(CONFLICT_TABLE).upsert(
            [{"entity": "equipment",
              "site_id": site_id,
              "issue": f"duplicate_inverter_{k}",
              "created_at": _now()} for k in dups],
            on_conflict="site_id,issue").execute()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()

# ────────────────────────────────────────────────────────────────────────
