#!/usr/bin/env python3
"""
Script d'investigation : onduleurs WR1 manquants sur plusieurs sites.

Vérifie :
1. Onduleurs VCOM pour Bourbon-Lancy (114QU)
2. Onduleurs VCOM pour Lapalisse, Sika Show Roof, La Primaube
3. Orphelins Supabase : strings WRx sans onduleur WRx actif
"""

import os
import re
import sys
from pathlib import Path

# Ajouter le src au path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from vysync.vcom_client import VCOMAPIClient
from vysync.adapters.supabase_adapter import SupabaseAdapter


def check_vcom_inverters(vc: VCOMAPIClient, system_key: str, site_label: str):
    print(f"\n{'='*60}")
    print(f"VCOM : {site_label} (system_key={system_key})")
    print('='*60)
    try:
        inverters = vc.get_inverters(system_key)
        print(f"  Nombre d'onduleurs VCOM : {len(inverters)}")
        for inv in inverters:
            inv_id = inv.get("id") or inv.get("inverterId") or inv.get("deviceId") or "???"
            try:
                details = vc.get_inverter_details(system_key, inv_id)
                serial = details.get("serialNumber") or details.get("serial") or "N/A"
                name   = details.get("name") or inv.get("name") or "N/A"
                model  = details.get("type", {}).get("name") if isinstance(details.get("type"), dict) else details.get("model", "N/A")
                print(f"  - id={inv_id} | serial={serial} | name={name} | model={model}")
                print(f"    raw_details={details}")
            except Exception as e:
                print(f"  - id={inv_id} | erreur détails : {e}")
                print(f"    raw_inv={inv}")
    except Exception as e:
        print(f"  ERREUR : {e}")


def check_supabase_orphans(sb: SupabaseAdapter):
    print(f"\n{'='*60}")
    print("SUPABASE : Strings WRx sans onduleur WRx actif (orphelins)")
    print('='*60)

    # Récupérer tous les équipements non-obsolètes
    rows = (
        sb.sb.table("equipments_mapping")
        .select("id, site_id, category_id, name, serial_number, parent_id, is_obsolete")
        .eq("is_obsolete", False)
        .execute()
        .data or []
    )

    # Récupérer les sites
    sites = (
        sb.sb.table("sites_mapping")
        .select("id, code, name, vcom_system_key, ignore_site, merged_into")
        .execute()
        .data or []
    )
    site_map = {s["id"]: s for s in sites}

    # CAT_INVERTER = 11102, CAT_STRING = 12404
    CAT_INVERTER = 11102
    CAT_STRING   = 12404

    # Grouper par site
    from collections import defaultdict
    site_inverters = defaultdict(dict)   # site_id -> {wr_num -> serial}
    site_strings   = defaultdict(list)   # site_id -> [row]

    for row in rows:
        site_id = row["site_id"]
        cat = row["category_id"]
        name = row.get("name", "") or ""

        if cat == CAT_INVERTER:
            # Extraire WR number du nom (ex: "WR 1 - Onduleur", "WR 2 - Onduleur")
            m = re.search(r"WR\s*(\d+)", name, re.IGNORECASE)
            wr_num = int(m.group(1)) if m else None
            site_inverters[site_id][wr_num] = row["serial_number"]

        elif cat == CAT_STRING:
            # Extraire WR number du nom (ex: "STRING-01-WR1", "STRING-02-WR2")
            m = re.search(r"WR(\d+)", name, re.IGNORECASE)
            if m:
                row["_wr_num"] = int(m.group(1))
                site_strings[site_id].append(row)

    # Trouver les orphelins
    orphan_sites = {}
    all_site_ids = set(site_inverters.keys()) | set(site_strings.keys())

    for site_id in all_site_ids:
        site_info = site_map.get(site_id, {})
        if site_info.get("ignore_site") or site_info.get("merged_into"):
            continue

        invs = site_inverters.get(site_id, {})
        strs = site_strings.get(site_id, [])

        # WR numbers référencés par des strings
        wr_in_strings = set(s["_wr_num"] for s in strs)
        # WR numbers avec un onduleur actif
        wr_with_inverter = set(k for k in invs.keys() if k is not None)

        # WR orphelins : strings WRx sans onduleur WRx
        orphan_wrs = wr_in_strings - wr_with_inverter
        if orphan_wrs:
            orphan_sites[site_id] = {
                "site": site_info,
                "orphan_wrs": sorted(orphan_wrs),
                "inverters": invs,
                "strings_by_wr": {
                    wr: [s for s in strs if s["_wr_num"] == wr]
                    for wr in orphan_wrs
                }
            }

    if not orphan_sites:
        print("  Aucun orphelin trouvé.")
    else:
        print(f"  {len(orphan_sites)} site(s) avec strings WRx sans onduleur WRx actif :\n")
        for site_id, info in sorted(orphan_sites.items(), key=lambda x: (x[1]["site"].get("code") or "?")):
            site = info["site"]
            print(f"  Site : {site.get('code','?')} | {site.get('name','?')} | vcom={site.get('vcom_system_key','?')}")
            print(f"    Onduleurs actifs : WR{sorted(info['inverters'].keys())}")
            print(f"    WR orphelins (strings présents, onduleur absent) : {info['orphan_wrs']}")
            for wr, strs in info["strings_by_wr"].items():
                print(f"    WR{wr} : {len(strs)} strings → ex: {strs[0]['name'] if strs else '?'}")


def check_supabase_site(sb: SupabaseAdapter, site_code: str):
    print(f"\n{'='*60}")
    print(f"SUPABASE : Site {site_code} - onduleurs (actifs + obsolètes)")
    print('='*60)
    # Trouver le site
    site_rows = (
        sb.sb.table("sites_mapping")
        .select("id, code, name, vcom_system_key")
        .eq("code", site_code)
        .execute()
        .data or []
    )
    if not site_rows:
        print(f"  Site {site_code} introuvable.")
        return None
    site = site_rows[0]
    print(f"  {site['code']} | {site['name']} | vcom_system_key={site['vcom_system_key']}")

    equips = (
        sb.sb.table("equipments_mapping")
        .select("id, category_id, name, serial_number, is_obsolete, vcom_device_id, parent_id")
        .eq("site_id", site["id"])
        .execute()
        .data or []
    )

    CAT_INVERTER = 11102
    CAT_STRING   = 12404
    inverters = [e for e in equips if e["category_id"] == CAT_INVERTER]
    strings   = [e for e in equips if e["category_id"] == CAT_STRING]

    print(f"\n  Onduleurs ({len(inverters)}) :")
    for inv in sorted(inverters, key=lambda x: x["name"]):
        obs = "OBSOLETE" if inv["is_obsolete"] else "ACTIF"
        print(f"    [{obs}] {inv['name']} | serial={inv['serial_number']} | vcom_id={inv['vcom_device_id']}")

    active_inv_serials = {inv["serial_number"] for inv in inverters if not inv["is_obsolete"]}
    active_inv_vcom_ids = {inv["vcom_device_id"] for inv in inverters if not inv["is_obsolete"]}

    # Compter les strings actives par WR
    from collections import Counter
    wr_counts = Counter()
    orphan_strings = []
    for s in strings:
        if s["is_obsolete"]:
            continue
        m = re.search(r"WR(\d+)", s.get("name",""), re.IGNORECASE)
        if m:
            wr_num = int(m.group(1))
            wr_counts[wr_num] += 1
            # Vérifier si le parent existe (parent_id = vcom_device_id de l'onduleur)
            if s.get("parent_id") and s["parent_id"] not in active_inv_vcom_ids:
                orphan_strings.append(s)

    print(f"\n  Strings actives par WR : {dict(sorted(wr_counts.items()))}")
    if orphan_strings:
        print(f"  Strings orphelines (parent_id sans onduleur actif) : {len(orphan_strings)}")
        for s in orphan_strings[:5]:
            print(f"    {s['name']} | parent_id={s['parent_id']}")

    return site.get("vcom_system_key")


def main():
    print("Initialisation clients...")
    vc = VCOMAPIClient()
    sb = SupabaseAdapter()

    # 1. Bourbon-Lancy
    vk_bourbon = check_supabase_site(sb, "00053")
    if vk_bourbon:
        check_vcom_inverters(vc, vk_bourbon, "Bourbon-Lancy")

    # 2. Lapalisse
    vk_lapalisse = check_supabase_site(sb, "00152")
    if vk_lapalisse:
        check_vcom_inverters(vc, vk_lapalisse, "Lapalisse")

    # 3. Sika Show Roof
    vk_sika = check_supabase_site(sb, "00328")
    if vk_sika:
        check_vcom_inverters(vc, vk_sika, "Sika Show Roof")

    # 4. La Primaube
    vk_primaube = check_supabase_site(sb, "00146")
    if vk_primaube:
        check_vcom_inverters(vc, vk_primaube, "La Primaube")

    # 5. Détection globale des orphelins dans Supabase
    check_supabase_orphans(sb)

    print("\n\nInvestigation terminée.")


if __name__ == "__main__":
    main()
