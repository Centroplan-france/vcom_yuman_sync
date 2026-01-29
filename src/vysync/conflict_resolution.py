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
import logging
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional

from vysync.models import Site
from vysync.yuman_client import YumanClient
from vysync.adapters.supabase_adapter import SupabaseAdapter

logger = logging.getLogger(__name__)

SITE_TABLE = "sites_mapping"
EQUIP_TABLE = "equipments_mapping"
CONFLICT_TABLE = "conflicts"
SYNC_LOGS_TABLE = "sync_logs"
FIELD_VALUES_TABLE = "equipment_field_values"



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
        .execute()
    sb.table(SYNC_LOGS_TABLE).insert({
        "source": "user",
        "action": "ignore_site",
        "payload": {"site_id": row["id"]},
        "created_at": _now(),
    }).execute()
    logger.info("[CONFLICT] site id=%s ignoré", row["id"])


def _merge_sites(sb, yc: YumanClient,
                 v_row: Dict, y_row: Dict) -> None:
    """
    Fusionne la ligne « site » Yuman (y_row) dans la ligne VCOM (v_row).

    • Les équipements sont ré-affectés.
    • La ligne Yuman N’EST PLUS supprimée : on la neutralise pour
      conserver l’historique et éviter la casse de requêtes tierces.
    • La contrainte UNIQUE (yuman_site_id) est libérée avant d’être
      ré-attribuée à la ligne VCOM.
    """
    logger.info("[MERGE] VCOM id=%d  ⇐  Yuman id=%d", v_row["id"], y_row["id"])

    sb.rpc("merge_sites", {
        "vcom_id": v_row["id"],
        "yuman_id": y_row["id"],
    }).execute()

    # 1) récupérer l'ID Yuman externe (pour l'API Yuman)
    yuman_site_id = y_row["yuman_site_id"]  # ID externe Yuman (pas l'ID Supabase interne)
    vcom_key = v_row["vcom_system_key"]

    # 2) appeler l'API Yuman pour renseigner le champ personnalisé
    yc.update_site(yuman_site_id, {"fields": [
    {"blueprint_id": 13583, "name": "System Key (Vcom ID)", "value": vcom_key}]})

    # 6) Marquer le conflit résolu
    sb.table(CONFLICT_TABLE).update({"resolved": True, "resolved_at": _now()}) \
        .eq("entity", "site") \
        .eq("yuman_site_id", y_row["yuman_site_id"]).execute()

    # 7) Log fusion
    sb.table(SYNC_LOGS_TABLE).insert({
        "source":  "user",
        "action":  "merge_site",
        "payload": {"from": y_row["id"], "into": v_row["id"],
                    "yuman_site_id": y_row["yuman_site_id"]},
        "created_at": _now(),
    }).execute()

    # 8) Nettoyage des doublons d'équipements
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

# ───────────────────────── CLIENTS ────────────────────────────
def resolve_clients_for_sites(sb_adapter: SupabaseAdapter,
                              y_adapter) -> None:
    sb = sb_adapter.sb
    yc = y_adapter.yc
    sites = sb.table("sites_mapping").select("id,name,client_map_id") \
                .is_("client_map_id", None).eq("ignore_site", False).execute().data
    if not sites:
        logger.info("[CLIENT] aucun site orphelin")
        return

    # Pré-chargement région → client (name_addition prioritaire, name en fallback)
    clients_data = sb.table("clients_mapping") \
                     .select("id,name,name_addition").execute().data or []
    name_addition_map = {r["name_addition"]: r["id"]
                         for r in clients_data
                         if r.get("name_addition")}
    name_map = {r["name"]: r["id"]
                for r in clients_data
                if r.get("name")}

    for s in sites:
        region = re.search(r"\(([^)]+)\)", s["name"] or "")
        region = region.group(1).strip() if region else None
        # Priorité à name_addition, fallback sur name
        client_id = name_addition_map.get(region) if region else None
        if not client_id and region:
            client_id = name_map.get(region)
        if not client_id:
            print(f"\nSite « {s['name']} » sans client.")
            choice = input("ID client existant ou 0 pour créer : ").strip()
            if choice == "0":
                nom = input("Nom client : ")
                adr = input("Adresse : ")
                new_cli = yc.create_client({"name": nom, "address": adr})
                sb.table("clients_mapping").insert({
                    "yuman_client_id": new_cli["id"],
                    "name":  new_cli["name"],
                    "created_at": _now(),
                }).execute()
                client_id = sb.table("clients_mapping") \
                              .select("id").eq("yuman_client_id", new_cli["id"]) \
                              .single().execute().data["id"]
            else:
                client_id = int(choice)
        # mise à jour site
        sb.table("sites_mapping").update({"client_map_id": client_id}) \
                                 .eq("id", s["id"]).execute()
        sb.table("sync_logs").insert({
            "source": "user",
            "action": "client_resolved",
            "payload": {"site_id": s["id"], "client_map_id": client_id},
            "created_at": _now(),
        }).execute()

# ────────────────────────────────────────────────────────────────────────
