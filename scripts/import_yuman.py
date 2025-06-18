#!/usr/bin/env python3
"""import_yuman.py – Incremental sync Yuman → Supabase/Postgres (V2, 2025‑06‑17)

Usage
-----
python import_yuman.py [--since YYYY-MM-DD] [--limit-sites N] [--dry-run]
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import UTC, datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from supabase import Client as SupabaseClient, create_client
from vysync.yuman_client import YumanClient

# ---------------------------------------------------------------------------
# Constants & mappings
# ---------------------------------------------------------------------------
CAT_INVERTER = 11102
CAT_MODULE = 11103
ALLOWED_EQUIP_CATEGORIES: Set[int] = {CAT_INVERTER, CAT_MODULE}
CUSTOM_FIELD_INVERTER_ID = "Inverter ID (Vcom)"

SITE_CUSTOM_FIELDS = {
    "System Key (Vcom ID)": "vcom_system_key",
    "ALDI ID": "aldi_id",
    "ID magasin (n° interne Aldi)": "aldi_store_id",
    "Project number (Centroplan ID)": "project_number_cp",
}

SITE_TABLE = "sites_mapping"
EQUIP_TABLE = "equipments_mapping"
CLIENT_TABLE = "clients_mapping"
FIELD_VALUES_TABLE = "equipment_field_values"
CONFLICT_TABLE = "conflicts"

now_iso = datetime.now(UTC).isoformat()

# ---------------------------------------------------------------------------
# CLI & helper
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser("Import Yuman into Supabase")
    p.add_argument("--since", help="Objects updated after YYYY-MM-DD")
    p.add_argument("--limit-sites", type=int, help="Process at most N sites (debug)")
    p.add_argument("--dry-run", action="store_true", help="No DB writes")
    return p.parse_args()


def sb_upsert(table: str, rows: List[Dict[str, Any]], sb: SupabaseClient, pk: str, dry: bool = False):
    if not rows:
        return
    key_cols = [c.strip() for c in pk.split(",")]
    uniq: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
    for r in rows:
        uniq[tuple(r.get(c) for c in key_cols)] = r
    rows = list(uniq.values())
    if dry:
        print(f"[DRY] {table}: upsert {len(rows)} rows on {pk}")
        return
    sb.table(table).upsert(rows, on_conflict=pk).execute()

# ---------------------------------------------------------------------------
# Sync Clients
# ---------------------------------------------------------------------------

def sync_clients(yc: YumanClient, sb: SupabaseClient, since: Optional[str], dry: bool) -> int:
    clients = yc._get("/clients", params={"per_page": 100})
    rows = [{
        "yuman_client_id": c["id"],
        "code": c.get("code"),
        "name": c["name"],
        "created_at": c.get("created_at") or now_iso,
    } for c in clients]
    sb_upsert(CLIENT_TABLE, rows, sb, "yuman_client_id", dry)
    return len(rows)

# ---------------------------------------------------------------------------
# Sync Sites (embed optimisation)
# ---------------------------------------------------------------------------

def sync_sites(
    yc: YumanClient,
    sb: SupabaseClient,
    since: Optional[str],
    limit_sites: Optional[int],
    dry: bool,
) -> Tuple[int, int]:
    existing = sb.table(SITE_TABLE).select("*").execute().data
    by_vcom = {s["vcom_system_key"]: s for s in existing if s.get("vcom_system_key")}
    clients = sb.table(CLIENT_TABLE).select("id,yuman_client_id").execute().data
    client_map = {c["yuman_client_id"]: c["id"] for c in clients}

    inserted, upsert_yid, conflicts = [], [], []
    for idx, det in enumerate(yc.list_sites(per_page=100, embed="fields,client,category", since=since)):
        if limit_sites and idx >= limit_sites:
            break
        cvals = {f["name"]: f.get("value") for f in det.get("_embed", {}).get("fields", [])}
        row = {
            "yuman_site_id": det["id"],
            "client_map_id": client_map.get(det.get("client_id")),
            "code": det.get("code"),
            "name": det.get("name"),
            "address": det.get("address"),
            "latitude": det.get("latitude"),
            "longitude": det.get("longitude"),
            "created_at": det.get("created_at") or now_iso,
        }
        for src, col in SITE_CUSTOM_FIELDS.items():
            v = cvals.get(src)
            if isinstance(v, str):
                v = v.strip() or None
            row[col] = v
        has_vcom = bool(row.get("vcom_system_key"))
        if has_vcom and (db := by_vcom.get(row["vcom_system_key"])):
            patch = {k: v for k, v in row.items() if v not in (None, "") and db.get(k) in (None, "")}
            if patch and not dry:
                sb.table(SITE_TABLE).update(patch).eq("vcom_system_key", row["vcom_system_key"]).execute()
        else:
            (inserted if has_vcom else upsert_yid).append(row)
            if not has_vcom:
                conflicts.append({"entity": "site", "yuman_site_id": row["yuman_site_id"], "issue": "missing_vcom_system_key", "created_at": now_iso})

    sb_upsert(SITE_TABLE, inserted, sb, "vcom_system_key", dry)
    sb_upsert(SITE_TABLE, upsert_yid, sb, "yuman_site_id", dry)
    sb_upsert(CONFLICT_TABLE, conflicts, sb, "yuman_site_id,issue", dry)
    return len(inserted) + len(upsert_yid), len(conflicts)

# ---------------------------------------------------------------------------
# Sync Equipments (embed optimisation)
# ---------------------------------------------------------------------------

def sync_equipments(
    yc: YumanClient,
    sb: SupabaseClient,
    since: Optional[str],
    dry: bool,
) -> int:
    sites = sb.table(SITE_TABLE).select("id,yuman_site_id,vcom_system_key").execute().data
    yid_to_pk = {s["yuman_site_id"]: s["id"] for s in sites}
    with_vcom = {s["yuman_site_id"] for s in sites if s.get("vcom_system_key")}
    without_vcom = {s["yuman_site_id"] for s in sites if not s.get("vcom_system_key")}

    existing = sb.table(EQUIP_TABLE).select("id,site_id,category_id,vcom_device_id,yuman_material_id").in_("category_id", list(ALLOWED_EQUIP_CATEGORIES)).execute().data
    inv_lookup = {(r["site_id"], r.get("vcom_device_id")): r for r in existing if r["category_id"] == CAT_INVERTER}
    mod_lookup = {r["site_id"]: r for r in existing if r["category_id"] == CAT_MODULE}

    inserts, patches, field_rows = [], [], {}
    for cat in ALLOWED_EQUIP_CATEGORIES:
        for eq in yc.list_materials(category_id=cat, embed="fields,site,category", per_page=100, since=since):
            y_site = eq.get("site_id")
            pk_site = yid_to_pk.get(y_site)
            if pk_site is None:
                continue
            fields = eq.get("_embed", {}).get("fields", [])
            if y_site in with_vcom:
                key = (pk_site, next((f["value"] for f in fields if f["name"] == CUSTOM_FIELD_INVERTER_ID), None)) if cat == CAT_INVERTER else pk_site
                row = inv_lookup.get(key) if cat == CAT_INVERTER else mod_lookup.get(key)
                if row and row.get("yuman_material_id") in (None, ""):
                    patches.append({"id": row["id"], "yuman_material_id": eq["id"]})
                continue
            if y_site not in without_vcom:
                continue
            inserts.append({
                "yuman_material_id": eq["id"],
                "site_id": pk_site,
                "category_id": cat,
                "name": eq.get("name"),
                "brand": eq.get("brand"),
                "model": eq.get("model"),
                "serial_number": eq.get("serial_number"),
                "created_at": eq.get("created_at") or now_iso,
                "vcom_device_id": next((f["value"] for f in fields if f["name"] == CUSTOM_FIELD_INVERTER_ID), None),
            })
            for f in fields:
                field_rows[(eq["id"], f["name"])] = {
                    "equipment_id": eq["id"],
                    "field_name": f["name"],
                    "value": f.get("value"),
                    "created_at": now_iso,
                }

    # --- Write to DB ---
    sb_upsert(EQUIP_TABLE, inserts, sb, "yuman_material_id", dry)
    existing_ids = {r["yuman_material_id"] for r in existing if r.get("yuman_material_id")}
    if not dry:
        for p in patches:
            if p["yuman_material_id"] in existing_ids:
                # Duplicate yuman_material_id already present elsewhere → ignore silently
                continue
            sb.table(EQUIP_TABLE).update({"yuman_material_id": p["yuman_material_id"]}).eq("id", p["id"]).execute()
            existing_ids.add(p["yuman_material_id"])
    sb_upsert(FIELD_VALUES_TABLE, list(field_rows.values()), sb, "equipment_id,field_name", dry)

    return len(inserts) + len(patches)

# ---------------------------------------------------------------------------
# Main entry
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    token = os.getenv("YUMAN_TOKEN")
    sb_url = os.getenv("SUPABASE_URL")
    sb_key = os.getenv("SUPABASE_SERVICE_KEY")
    if not all([token, sb_url, sb_key]):
        sys.exit("Missing YUMAN_TOKEN / SUPABASE_URL / SUPABASE_SERVICE_KEY env vars")

    yc = YumanClient(token)
    sb = create_client(sb_url, sb_key)

    stats = {}
    stats["clients"] = sync_clients(yc, sb, args.since, args.dry_run)
    stats["sites"], stats["site_conflicts"] = sync_sites(yc, sb, args.since, args.limit_sites, args.dry_run)
    stats["equipments"] = sync_equipments(yc, sb, args.since, args.dry_run)

    if not args.dry_run:
        sb.table("sync_logs").insert({
            "source": "yuman",
            "action": "import",
            "payload": stats,
            "created_at": now_iso,
        }).execute()

    print("Sync finished:", stats)


if __name__ == "__main__":
    main()
