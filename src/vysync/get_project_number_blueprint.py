#!/usr/bin/env python3
"""
Diagnostic : récupérer le blueprint_id du champ "Project number (Centroplan ID)"
depuis l'API Yuman.
"""

import sys
from pathlib import Path

# Ajouter le chemin du projet vysync
sys.path.insert(0, str(Path(__file__).parent / "vysync-main" / "src"))

from vysync.logging_config import setup_logging
setup_logging()

from vysync.adapters.supabase_adapter import SupabaseAdapter
from vysync.adapters.yuman_adapter import YumanAdapter


class C:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    END = '\033[0m'
    BOLD = '\033[1m'


def main():
    print(f"\n{C.BOLD}═══════════════════════════════════════════════════════════════{C.END}")
    print(f"{C.BOLD}  DIAGNOSTIC : Custom fields SITE dans Yuman{C.END}")
    print(f"{C.BOLD}═══════════════════════════════════════════════════════════════{C.END}\n")

    # Connexion
    print("Connexion à Supabase...")
    sb = SupabaseAdapter()
    print(f"{C.GREEN}✓ Supabase connecté{C.END}")

    print("Connexion à Yuman...")
    y = YumanAdapter(sb)
    print(f"{C.GREEN}✓ Yuman connecté{C.END}")

    # Récupérer un site qui a un project_number_cp renseigné en DB
    print("\nRecherche d'un site avec project_number_cp en DB...")
    result = sb.sb.table("sites_mapping") \
        .select("id, yuman_site_id, vcom_system_key, name, project_number_cp") \
        .not_.is_("project_number_cp", "null") \
        .not_.is_("yuman_site_id", "null") \
        .limit(1) \
        .execute()

    if not result.data:
        print(f"{C.RED}✗ Aucun site trouvé avec project_number_cp ET yuman_site_id{C.END}")
        return

    site_info = result.data[0]
    yuman_site_id = site_info["yuman_site_id"]
    
    print(f"\n{C.BOLD}Site sélectionné :{C.END}")
    print(f"  • ID Supabase:      {site_info['id']}")
    print(f"  • Yuman site ID:    {yuman_site_id}")
    print(f"  • VCOM key:         {site_info['vcom_system_key']}")
    print(f"  • Nom:              {site_info['name']}")
    print(f"  • project_number_cp (DB): {site_info['project_number_cp']}")

    # Fetch les custom fields depuis Yuman
    print(f"\n{C.BOLD}Récupération des custom fields depuis Yuman...{C.END}")
    site_data = y.yc.get_site(yuman_site_id, embed="fields")
    
    raw_fields = site_data.get("_embed", {}).get("fields", [])
    
    if not raw_fields:
        print(f"{C.RED}✗ Aucun custom field trouvé pour ce site{C.END}")
        return

    print(f"\n{C.BOLD}Tous les custom fields du site :{C.END}")
    print(f"{'─'*80}")
    print(f"{'Nom du champ':<45} {'blueprint_id':>12}  {'Valeur':<20}")
    print(f"{'─'*80}")
    
    project_number_bp = None
    
    for f in sorted(raw_fields, key=lambda x: x.get("name", "")):
        name = f.get("name", "")
        bp_id = f.get("blueprint_id", "?")
        value = f.get("value", "")
        
        # Highlight le champ recherché
        if "project" in name.lower() or "centroplan" in name.lower():
            print(f"{C.GREEN}{name:<45} {bp_id:>12}  {str(value):<20}{C.END} ← TROUVÉ")
            project_number_bp = bp_id
        else:
            print(f"{name:<45} {bp_id:>12}  {str(value):<20}")
    
    print(f"{'─'*80}")

    # Résumé
    print(f"\n{C.BOLD}═══════════════════════════════════════════════════════════════{C.END}")
    if project_number_bp:
        print(f"{C.GREEN}✓ RÉSULTAT : blueprint_id = {project_number_bp}{C.END}")
        print(f"\nÀ ajouter dans SITE_FIELDS (yuman_adapter.py) :")
        print(f'    "Project number (Centroplan ID)": {project_number_bp},')
    else:
        print(f"{C.YELLOW}⚠ Champ 'Project number (Centroplan ID)' non trouvé{C.END}")
        print("Vérifier le nom exact du champ dans la liste ci-dessus.")
    print(f"{C.BOLD}═══════════════════════════════════════════════════════════════{C.END}\n")


if __name__ == "__main__":
    main()