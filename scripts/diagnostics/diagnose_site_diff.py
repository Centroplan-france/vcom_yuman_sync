#!/usr/bin/env python3
"""
Diagnostic : Identifier pourquoi certains sites apparaissent en UPDATE
alors qu'ils n'ont pas de changements visibles.
"""

import sys
import re
from dataclasses import fields, replace

sys.path.insert(0, "/workspaces/vcom_yuman_sync/src")

from vysync.adapters.supabase_adapter import SupabaseAdapter
from vysync.adapters.yuman_adapter import YumanAdapter
from vysync.models import Site

def normalize_site_name(name: str) -> str:
    if not name:
        return ""
    return re.sub(r'^\d+\s+|\s*\(.*?\)| France', '', name).strip()

# Sites ALDI suspects (avec changes: {} dans le rapport)
SUSPECT_SITES = [
    ("2KC5K", "ALDI Pau Daurat", 747924),
    ("PPWN2", "ALDI Puttelange aux Lacs", 655338),
    ("D4WQL", "ALDI Frignicourt", 655342),
]

def main():
    print("=" * 70)
    print("DIAGNOSTIC : Pourquoi ces sites sont en UPDATE ?")
    print("=" * 70)
    
    sb = SupabaseAdapter()
    y = YumanAdapter(sb)
    
    print("\nChargement Supabase...")
    sb_sites_all = sb.fetch_sites_y()
    
    print("Chargement Yuman (sites seulement)...")
    y_sites = y.fetch_sites()
    
    # Normaliser les noms Supabase
    sb_sites = {
        k: replace(s, name=normalize_site_name(s.name))
        for k, s in sb_sites_all.items()
        if not getattr(s, "ignore_site", False)
    }
    
    site_field_names = [f.name for f in fields(Site)]
    ignore_fields = {"client_map_id", "id", "ignore_site", "latitude", "longitude"}
    
    print(f"\nChamps du dataclass Site: {site_field_names}")
    print(f"Champs ignorés par diff: {ignore_fields}")
    
    for vcom_key, name, yuman_id in SUSPECT_SITES:
        print(f"\n{'─' * 70}")
        print(f"Site: {name} (yuman_id={yuman_id})")
        print(f"{'─' * 70}")
        
        sb_site = sb_sites.get(yuman_id)
        y_site = y_sites.get(yuman_id)
        
        if not sb_site or not y_site:
            print(f"  ❌ Site non trouvé")
            continue
        
        print("\n  Différences détectées:")
        for field_name in site_field_names:
            sb_val = getattr(sb_site, field_name, None)
            y_val = getattr(y_site, field_name, None)
            
            if sb_val != y_val:
                ignored = "(IGNORÉ)" if field_name in ignore_fields else "→ CAUSE UPDATE"
                print(f"    {field_name}: {ignored}")
                print(f"      SB: {repr(sb_val)}")
                print(f"      YU: {repr(y_val)}")

    # Sites sans yuman_site_id
    print(f"\n{'=' * 70}")
    print("Sites SANS yuman_site_id (causent équipements orphelins):")
    print(f"{'=' * 70}")
    
    for k, s in sb_sites_all.items():
        if not s.yuman_site_id and not getattr(s, "ignore_site", False):
            print(f"  • {s.name} (vcom={s.vcom_system_key}, sb_id={s.id})")

if __name__ == "__main__":
    main()