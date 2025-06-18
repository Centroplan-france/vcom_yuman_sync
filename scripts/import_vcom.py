#!/usr/bin/env python3
"""import_vcom.py

Sync VCOM Cloud data → Supabase/Postgres.
• Systems → sites_mapping (lat/long, address, nominal_power, site_area, commission_date).  
• Inverters → equipments (category 'Onduleur', vcom_device_id, vendor, model, serial).  
• Modules (panel aggregate) → equipments (category 'Module', one per site, fields vendor, model, count).

Usage
-----
python import_vcom.py [--since YYYY-MM-DD] [--dry-run]
"""
from __future__ import annotations

import argparse
from datetime import datetime, UTC
import sys
from typing import Any, Dict, List
from vysync.vcom_client import VCOMAPIClient
from vysync.db import sb_upsert, supabase

now_iso = datetime.now(UTC).isoformat()

# ------------------------------------------------------------------
# Préparer le dictionnaire name -> id (lookup rapide)
# ------------------------------------------------------------------
cat_lookup = {
    c["name"]: c["id"]
    for c in supabase.table("equipment_categories")
               .select("id,name")
               .execute()
               .data
}
INVERTER_CAT_ID = cat_lookup.get("Onduleur")    # ou int fixe si connu
MODULES_CAT_ID  = 11103    


# ---------------------------------------------------------------------------
# Constants & mappings
# ---------------------------------------------------------------------------
CATEGORY_SITE_MODULE = "Module"  # category name in equipment_categories
CATEGORY_INVERTER = "Onduleur"

SITE_TABLE = "sites_mapping"
EQUIP_TABLE = "equipments_mapping"

# ---------------------------------------------------------------------------
# CLI parsing
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Import VCOM → Supabase")
    p.add_argument("--since", type=str, default=None, help="Only import systems commissioned after this date (YYYY-MM-DD)")
    p.add_argument("--dry-run", action="store_true", help="No DB writes, just log")
    return p.parse_args()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def build_address(addr: Dict[str, Any]) -> str | None:
    if not addr:
        return None
    parts = [addr.get("street"), f"{addr.get('postalCode', '')} {addr.get('city', '')}".strip()]
    return ", ".join(filter(None, parts)) or None


# ---------------------------------------------------------------------------
# Main sync routine
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    dry = args.dry_run

    if supabase is None:
        sys.exit("Supabase client not initialised (env vars missing)")

    vc = VCOMAPIClient()
    systems = vc.get_systems()

    site_rows: List[Dict[str, Any]] = []
    equip_rows: List[Dict[str, Any]] = []

    for sysd in systems:
        # Filter by commissionDate if --since
        if args.since and sysd.get("commissionDate"):
            if sysd["commissionDate"] < args.since:
                continue

        key = sysd["key"]
        tech = vc.get_technical_data(key)
        detail_sysd = vc.get_system_details(key)

        # ---------------- sites
        addr = build_address(detail_sysd.get("address", {}))
        site_rows.append({
            "vcom_system_key": key,
            "name": sysd.get("name"),
            "latitude": detail_sysd.get("coordinates", {}).get("latitude"),
            "longitude": detail_sysd.get("coordinates", {}).get("longitude"),
            "address": addr,
            "commission_date": detail_sysd.get("commissionDate"),
            "nominal_power": tech.get("nominalPower"),
            "site_area": tech.get("siteArea"),
            "created_at": now_iso,
        })

        # ---------------- modules (one aggregate row per site)
        panels = tech.get("panels") or []
        if panels:
            p = panels[0]  # assume single entry (vendor/model/count)
            equip_rows.append({
                "vcom_device_id":    f"MODULES-{key}",
                "yuman_material_id": None,           # sera lié plus tard
                "category_id":       MODULES_CAT_ID,
                "brand":             p.get("vendor"),    # colonne brand
                "model":             p.get("model"),
                "name":              p.get("model"),     # nom obligatoire
                "serial_number":     None,
                "count":             p.get("count"),
                "vcom_system_key":   key,
                "site_id":           None,               # complété juste après
                "created_at":        now_iso,
            })



        # ---------------- inverters
        invs = vc.get_inverters(key)
        for inv in invs:
            details = vc.get_inverter_details(key, inv["id"])
            equip_rows.append({
                "vcom_device_id":    inv["id"],
                "yuman_material_id": None,
                "category_id":       INVERTER_CAT_ID,
                "brand":             details.get("vendor"),
                "model":             details.get("model"),
                "name":              inv.get("name"),     # nom fourni par VCOM
                "serial_number":     inv.get("serial"),
                "vcom_system_key":   key,
                "site_id":           None,                # complété plus bas
                "created_at":        now_iso,
            })



    # ---------------- DB writes
    # -- upsert sites
    if not dry:
        sb_upsert(SITE_TABLE, site_rows, on_conflict="vcom_system_key", ignore_duplicates=True)
    else:
        print(f"[DRY] sites to upsert: {len(site_rows)}")
    if not dry:
        sb_upsert(SITE_TABLE, site_rows, on_conflict="vcom_system_key", ignore_duplicates=False)
    else:
        print(f"[DRY] sites to upsert: {len(site_rows)}")
    # ------------------------------------------------------------------
    # Compléter site_id dans equip_rows à partir de vcom_system_key
    # ------------------------------------------------------------------
    if equip_rows:
        # récupère mapping vcom_system_key -> site PK
        site_map = {
            s["vcom_system_key"]: s["id"]
            for s in supabase.table("sites_mapping")
                            .select("id,vcom_system_key")
                            .in_("vcom_system_key", [e["vcom_system_key"] for e in equip_rows])
                            .execute()
                            .data
        }
        for eq in equip_rows:
            eq["site_id"] = site_map.get(eq["vcom_system_key"])  # peut rester None si le site n’est pas encore là

    # -- upsert equipments
    if not dry:
        sb_upsert(EQUIP_TABLE, equip_rows, on_conflict="vcom_device_id", ignore_duplicates=True)
    else:
        print(f"[DRY] equipments to upsert: {len(equip_rows)}")

        print(f"Imported {len(site_rows)} sites, {len(equip_rows)} equipments from VCOM")

    if not dry:
        sb_upsert(EQUIP_TABLE, equip_rows, on_conflict="vcom_device_id", ignore_duplicates=False)
    else:
        print(f"[DRY] equipments to upsert: {len(equip_rows)}")

        print(f"Imported {len(site_rows)} sites, {len(equip_rows)} equipments from VCOM")


if __name__ == "__main__":
    main()
