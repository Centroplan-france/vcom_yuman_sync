#!/usr/bin/env python3
"""
Synchronisation du champ "Project number (Centroplan ID)" entre DB et Yuman.

Logique :
  - Si égal → skip
  - Si différent (les deux ont une valeur) → DB écrase Yuman
  - Si Yuman a une valeur mais DB est null/vide → Yuman écrase DB

Usage :
   poetry run python -m vysync.sync_project_number_cp          # Dry-run (affiche les changements)
   poetry run python -m vysync.sync_project_number_cp --apply  # Applique les changements
"""

import sys
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from vysync.logging_config import setup_logging
setup_logging()

from vysync.adapters.supabase_adapter import SupabaseAdapter
from vysync.adapters.yuman_adapter import YumanAdapter


# Blueprint ID du champ "Project number (Centroplan ID)" dans Yuman
BP_PROJECT_NUMBER = 13582


class C:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    CYAN = '\033[96m'
    END = '\033[0m'
    BOLD = '\033[1m'


def normalize(val) -> str:
    """Normalise une valeur pour comparaison (strip, None → '')"""
    if val is None:
        return ""
    return str(val).strip()


def main():
    parser = argparse.ArgumentParser(description="Sync project_number_cp DB ↔ Yuman")
    parser.add_argument("--apply", action="store_true", help="Appliquer les changements (sinon dry-run)")
    args = parser.parse_args()

    mode = "APPLY" if args.apply else "DRY-RUN"
    
    print(f"\n{C.BOLD}═══════════════════════════════════════════════════════════════{C.END}")
    print(f"{C.BOLD}  SYNC project_number_cp  [{mode}]{C.END}")
    print(f"{C.BOLD}═══════════════════════════════════════════════════════════════{C.END}\n")

    # Connexion
    print("Connexion à Supabase...")
    sb = SupabaseAdapter()
    print(f"{C.GREEN}✓ Supabase connecté{C.END}")

    print("Connexion à Yuman...")
    y = YumanAdapter(sb)
    print(f"{C.GREEN}✓ Yuman connecté{C.END}")

    # ═══════════════════════════════════════════════════════════════
    # ÉTAPE 1 : Récupérer tous les sites avec yuman_site_id
    # ═══════════════════════════════════════════════════════════════
    print(f"\n{C.BOLD}ÉTAPE 1 : Récupération des sites DB avec yuman_site_id{C.END}")
    
    result = sb.sb.table("sites_mapping") \
        .select("id, yuman_site_id, vcom_system_key, name, project_number_cp") \
        .not_.is_("yuman_site_id", "null") \
        .execute()
    
    db_sites = {row["yuman_site_id"]: row for row in result.data}
    print(f"  → {len(db_sites)} sites trouvés en DB avec yuman_site_id")

    # ═══════════════════════════════════════════════════════════════
    # ÉTAPE 2 : Récupérer les valeurs Yuman
    # ═══════════════════════════════════════════════════════════════
    print(f"\n{C.BOLD}ÉTAPE 2 : Récupération des valeurs Yuman{C.END}")
    
    yuman_values = {}  # yuman_site_id → project_number_cp
    
    for yuman_site_id in db_sites.keys():
        try:
            site_data = y.yc.get_site(yuman_site_id, embed="fields")
            raw_fields = site_data.get("_embed", {}).get("fields", [])
            
            # Chercher le champ Project number
            project_val = None
            for f in raw_fields:
                if f.get("blueprint_id") == BP_PROJECT_NUMBER:
                    project_val = f.get("value")
                    break
            
            yuman_values[yuman_site_id] = normalize(project_val)
        except Exception as e:
            print(f"{C.RED}  ✗ Erreur fetch yuman_site_id={yuman_site_id}: {e}{C.END}")
            yuman_values[yuman_site_id] = ""
    
    print(f"  → {len(yuman_values)} sites récupérés depuis Yuman")

    # ═══════════════════════════════════════════════════════════════
    # ÉTAPE 3 : Comparer et classifier
    # ═══════════════════════════════════════════════════════════════
    print(f"\n{C.BOLD}ÉTAPE 3 : Comparaison DB vs Yuman{C.END}")
    
    to_update_yuman = []   # (site_info, db_val, yuman_val) → DB écrase Yuman
    to_update_db = []      # (site_info, db_val, yuman_val) → Yuman écrase DB
    identical = []         # Pas de changement
    both_empty = []        # Les deux vides
    
    for yuman_site_id, site_info in db_sites.items():
        db_val = normalize(site_info.get("project_number_cp"))
        yuman_val = yuman_values.get(yuman_site_id, "")
        
        if db_val == yuman_val:
            if db_val == "":
                both_empty.append(site_info)
            else:
                identical.append(site_info)
        elif db_val and yuman_val:
            # Les deux ont une valeur différente → DB gagne
            to_update_yuman.append((site_info, db_val, yuman_val))
        elif yuman_val and not db_val:
            # Yuman a une valeur, DB vide → Yuman écrase DB
            to_update_db.append((site_info, db_val, yuman_val))
        else:
            # DB a une valeur, Yuman vide → DB écrase Yuman
            to_update_yuman.append((site_info, db_val, yuman_val))

    # ═══════════════════════════════════════════════════════════════
    # ÉTAPE 4 : Afficher le résumé
    # ═══════════════════════════════════════════════════════════════
    print(f"\n{C.BOLD}═══════════════════════════════════════════════════════════════{C.END}")
    print(f"{C.BOLD}  RÉSUMÉ{C.END}")
    print(f"{C.BOLD}═══════════════════════════════════════════════════════════════{C.END}")
    print(f"  {C.GREEN}✓ Identiques         : {len(identical)}{C.END}")
    print(f"  {C.CYAN}○ Les deux vides     : {len(both_empty)}{C.END}")
    print(f"  {C.YELLOW}→ DB écrase Yuman    : {len(to_update_yuman)}{C.END}")
    print(f"  {C.YELLOW}← Yuman écrase DB    : {len(to_update_db)}{C.END}")

    # ═══════════════════════════════════════════════════════════════
    # DÉTAIL : DB → Yuman
    # ═══════════════════════════════════════════════════════════════
    if to_update_yuman:
        print(f"\n{C.BOLD}DÉTAIL : DB → Yuman ({len(to_update_yuman)} sites){C.END}")
        print(f"{'─'*90}")
        print(f"{'Site':<40} {'VCOM':<8} {'Yuman (actuel)':<18} {'DB (nouveau)':<18}")
        print(f"{'─'*90}")
        for site_info, db_val, yuman_val in to_update_yuman:
            name = (site_info['name'] or '')[:38]
            vcom = (site_info.get('vcom_system_key') or '')[:6]
            yuman_display = yuman_val if yuman_val else "(vide)"
            print(f"{name:<40} {vcom:<8} {C.RED}{yuman_display:<18}{C.END} {C.GREEN}{db_val:<18}{C.END}")
        print(f"{'─'*90}")

    # ═══════════════════════════════════════════════════════════════
    # DÉTAIL : Yuman → DB
    # ═══════════════════════════════════════════════════════════════
    if to_update_db:
        print(f"\n{C.BOLD}DÉTAIL : Yuman → DB ({len(to_update_db)} sites){C.END}")
        print(f"{'─'*90}")
        print(f"{'Site':<40} {'VCOM':<8} {'DB (actuel)':<18} {'Yuman (nouveau)':<18}")
        print(f"{'─'*90}")
        for site_info, db_val, yuman_val in to_update_db:
            name = (site_info['name'] or '')[:38]
            vcom = (site_info.get('vcom_system_key') or '')[:6]
            db_display = db_val if db_val else "(vide)"
            print(f"{name:<40} {vcom:<8} {C.RED}{db_display:<18}{C.END} {C.GREEN}{yuman_val:<18}{C.END}")
        print(f"{'─'*90}")

    # ═══════════════════════════════════════════════════════════════
    # ÉTAPE 5 : Appliquer si --apply
    # ═══════════════════════════════════════════════════════════════
    if not args.apply:
        print(f"\n{C.YELLOW}Mode DRY-RUN : aucune modification appliquée.{C.END}")
        print(f"Relancer avec {C.BOLD}--apply{C.END} pour appliquer les changements.\n")
        return

    print(f"\n{C.BOLD}ÉTAPE 5 : Application des changements{C.END}")
    
    # 5.1 - DB → Yuman
    if to_update_yuman:
        print(f"\n  {C.BOLD}DB → Yuman :{C.END}")
        success_yuman = 0
        for site_info, db_val, yuman_val in to_update_yuman:
            yuman_site_id = site_info["yuman_site_id"]
            try:
                y.yc.update_site(yuman_site_id, {
                    "fields": [{
                        "blueprint_id": BP_PROJECT_NUMBER,
                        "name": "Project number (Centroplan ID)",
                        "value": db_val
                    }]
                })
                success_yuman += 1
                print(f"    {C.GREEN}✓{C.END} {site_info['name'][:40]}")
            except Exception as e:
                print(f"    {C.RED}✗{C.END} {site_info['name'][:40]} : {e}")
        print(f"  → {success_yuman}/{len(to_update_yuman)} mis à jour dans Yuman")

    # 5.2 - Yuman → DB
    if to_update_db:
        print(f"\n  {C.BOLD}Yuman → DB :{C.END}")
        success_db = 0
        for site_info, db_val, yuman_val in to_update_db:
            site_id = site_info["id"]
            try:
                sb.sb.table("sites_mapping") \
                    .update({"project_number_cp": yuman_val}) \
                    .eq("id", site_id) \
                    .execute()
                success_db += 1
                print(f"    {C.GREEN}✓{C.END} {site_info['name'][:40]}")
            except Exception as e:
                print(f"    {C.RED}✗{C.END} {site_info['name'][:40]} : {e}")
        print(f"  → {success_db}/{len(to_update_db)} mis à jour en DB")

    print(f"\n{C.GREEN}{C.BOLD}✓ Synchronisation terminée.{C.END}\n")


if __name__ == "__main__":
    main()