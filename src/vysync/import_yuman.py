#!/usr/bin/env python3
"""import_yuman.py
Incremental sync from Yuman Cloud into Supabase/Postgres.

This version relies on the shared **yuman_client.YumanClient** wrapper and the
SQLModel schema in **models.py**.  Sites receive custom‑field columns directly;
equipments are limited to category *Onduleur*.

Run with:
  python import_yuman.py [--since YYYY-MM-DD] [--dry-run]
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
from typing import Any, Dict, List, Tuple

from supabase import Client as SupabaseClient, create_client

from yuman_client import YumanClient  # ← shared wrapper
from models import Site  # type: ignore – SQLModel ORM

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
ALLOWED_EQUIP_CATEGORIES = {"Onduleur"}
CUSTOM_FIELD_INVERTER_ID = "Inverter ID (Vcom)"

SITE_CUSTOM_FIELDS: dict[str, str] = {
    "System Key (Vcom ID)": "vcom_system_key",
    "ALDI ID": "aldi_id",
    "ID magasin (n° interne Aldi)": "aldi_store_id",
    "Project number (Centroplan ID)": "project_number_cp",
}

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Import Yuman into Supabase")
    p.add_argument("--since", type=str, help="Import records updated after date YYYY-MM-DD", default=None)
    p.add_argument("--dry-run", action="store_true", help="Do not write to database")
    return p.parse_args()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def sb_upsert(table: str, rows: List[Dict[str, Any]], sb: SupabaseClient, pk: str, dry: bool = False):
    if not rows:
        return
    if dry:
        print(f"[DRY] {table}: would upsert {len(rows)} rows")
        return
    sb.table(table).upsert(rows, on_conflict=pk).execute()

# ---------------------------------------------------------------------------
# Sync Sites
# ---------------------------------------------------------------------------

def sync_sites(yc: YumanClient, sb: SupabaseClient, since: str | None, dry: bool) -> Tuple[int, int]:
    """Return tuple (imported, conflicts)"""
    rows: List[Dict[str, Any]] = []
    conflicts: List[Dict[str, Any]] = []

    for site in yc.list_sites(per_page=100, since=since):
        detail = yc.get_site_detailed(site["id"], embed="client,category,fields")
        custom_map = {f["name"]: f.get("value") for f in detail.get("fields", [])}

        row: Dict[str, Any] = {
            "yuman_site_id": detail["id"],
            "yuman_client_id": detail.get("client_id"),
            "code": detail.get("code"),
            "name": detail.get("name"),
            "address": detail.get("address"),
            "latitude": detail.get("latitude"),
            "longitude": detail.get("longitude"),
            "updated_at": detail.get("updated_at"),
        }
        for src, col in SITE_CUSTOM_FIELDS.items():
            row[col] = custom_map.get(src)

        # conflict detection (missing vcom_system_key while ignore_site is false)
        ignore_flag = detail.get("ignore_site", False)
        if not row.get("vcom_system_key") and not ignore_flag:
            conflicts.append({
                "entity": "site",
                "yuman_site_id": row["yuman_site_id"],
                "issue": "missing_vcom_system_key",
                "created_at": dt.datetime.utcnow().isoformat(),
            })
        rows.append(row)

    sb_upsert("sites", rows, sb, "yuman_site_id", dry)
    if conflicts:
        sb_upsert("conflicts", conflicts, sb, "yuman_site_id,issue", dry)
    return len(rows), len(conflicts)

# ---------------------------------------------------------------------------
# Sync Equipments (Onduleurs)
# ---------------------------------------------------------------------------

def sync_equipments(yc: YumanClient, sb: SupabaseClient, since: str | None, dry: bool) -> int:
    cat_id = yc.get_category_id("Onduleur")
    if cat_id is None:
        print("[WARN] Category 'Onduleur' not found; skip equip import")
        return 0

    equip_rows: List[Dict[str, Any]] = []
    field_rows: List[Dict[str, Any]] = []
    valid_pairs: set[Tuple[int, str]] = set()

    for equip in yc.list_materials(category_id=cat_id, embed="site,category", since=since):
        detail = yc.get_material_detailed(equip["id"])  # wrapper method
        fields = detail.get("fields", [])
        equip_rows.append({
            "yuman_material_id": equip["id"],
            "yuman_site_id": equip.get("site", {}).get("id"),
            "category_id": cat_id,
            "name": equip.get("name"),
            "brand": equip.get("brand"),
            "model": equip.get("model"),
            "serial_number": equip.get("serial_number"),
            "updated_at": equip.get("updated_at"),
            "vcom_device_id": next((f["value"] for f in fields if f["name"] == CUSTOM_FIELD_INVERTER_ID), None),
        })
        for f in fields:
            key = (equip["id"], f["name"])
            valid_pairs.add(key)
            field_rows.append({
                "yuman_material_id": equip["id"],
                "field_name": f["name"],
                "field_value": f.get("value"),
                "active": True,
            })

    sb_upsert("equipments", equip_rows, sb, "yuman_material_id", dry)
    sb_upsert("equipment_field_values", field_rows, sb, "yuman_material_id,field_name", dry)

    if valid_pairs and not dry:
        sb.rpc("mark_missing_field_values_inactive", {"valid_pairs": list(valid_pairs)}).execute()

    return len(equip_rows)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args = parse_args()
    token = os.getenv("YUMAN_TOKEN")
    sb_url = os.getenv("SUPABASE_URL")
    sb_key = os.getenv("SUPABASE_SERVICE_KEY")
    if not all([token, sb_url, sb_key]):
        sys.exit("Missing env vars YUMAN_TOKEN / SUPABASE_URL / SUPABASE_SERVICE_KEY")

    yc = YumanClient(token)
    sb = create_client(sb_url, sb_key)

    stats: Dict[str, Any] = {}
    stats["sites"], stats["site_conflicts"] = sync_sites(yc, sb, args.since, args.dry_run)
    stats["equipments"] = sync_equipments(yc, sb, args.since, args.dry_run)

    if not args.dry_run:
        sb.table("sync_logs").insert({
            "source": "yuman",
            "action": "import",
            "payload": stats,
            "created_at": dt.datetime.utcnow().isoformat(),
        }).execute()
    print("Sync finished:", stats)


if __name__ == "__main__":
    main()
