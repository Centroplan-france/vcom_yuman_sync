#!/usr/bin/env python3
"""
Script d'exploration des Power Plant Controllers (PPC) VCOM
Teste 4 sites : WC6HQ, 4FXLS, 4QW9N, S3KCJ

Pour chaque site :
1. Récupère tous les PPC
2. Pour chaque PPC, liste toutes les abréviations
3. Pour chaque abréviation, récupère métadonnées + valeur récente (dernière heure)

Sortie : ppc_exploration_results.json
"""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

# Ajouter le chemin du projet pour importer les modules
sys.path.insert(0, str(Path(__file__).parent / "src"))

from vysync.vcom_client import VCOMAPIClient


def explore_site_ppc(client: VCOMAPIClient, site_key: str) -> dict:
    """
    Explore tous les PPC d'un site et leurs abréviations.
    
    Returns:
        dict: Structure avec controllers et leurs abréviations
    """
    print(f"\n{'='*60}")
    print(f"Exploration du site: {site_key}")
    print(f"{'='*60}")
    
    site_data = {
        "controllers": [],
        "error": None
    }
    
    try:
        # 1. Récupérer tous les PPC du site
        print(f"[{site_key}] Récupération des power plant controllers...")
        controllers = client.get_power_plant_controllers(site_key)
        
        if not controllers:
            print(f"[{site_key}] Aucun power plant controller trouvé")
            site_data["error"] = "no_controllers"
            return site_data
        
        print(f"[{site_key}] {len(controllers)} controller(s) trouvé(s)")
        
        # 2. Pour chaque PPC, explorer les abréviations
        for controller in controllers:
            controller_id = controller.get('id')
            controller_name = controller.get('name', 'N/A')
            print(f"\n[{site_key}] Controller: {controller_name} (ID: {controller_id})")
            
            controller_data = {
                "id": controller_id,
                "name": controller_name,
                "uid": controller.get('uid'),
                "abbreviations": [],
                "error": None
            }
            
            try:
                # 2a. Récupérer la liste des abréviations
                print(f"[{site_key}][{controller_name}] Récupération des abréviations...")
                abbreviations_list = client.get_ppc_abbreviations(site_key, controller_id)
                
                if not abbreviations_list:
                    print(f"[{site_key}][{controller_name}] Aucune abréviation trouvée")
                    controller_data["error"] = "no_abbreviations"
                    site_data["controllers"].append(controller_data)
                    continue
                
                print(f"[{site_key}][{controller_name}] {len(abbreviations_list)} abréviation(s) trouvée(s)")
                
                # 2b. Pour chaque abréviation, récupérer métadonnées + valeur récente
                for abbr_id in abbreviations_list:
                    print(f"[{site_key}][{controller_name}] Traitement de {abbr_id}...")
                    
                    abbr_data = {
                        "id": abbr_id,
                        "metadata": None,
                        "recent_measurement": None,
                        "error": None
                    }
                    
                    try:
                        # Récupérer métadonnées
                        metadata = client.get_ppc_abbreviation_info(site_key, controller_id, abbr_id)
                        abbr_data["metadata"] = metadata
                        
                        # Récupérer une mesure récente (la veille à 18h-19h)
                        now = datetime.now(timezone.utc)
                        yesterday = now - timedelta(days=1)
                        # 18h UTC hier
                        from_time = yesterday.replace(hour=18, minute=0, second=0, microsecond=0)
                        to_time = from_time + timedelta(hours=1)
                        
                        measurements = client.get_ppc_measurements(
                            site_key,
                            controller_id,
                            abbr_id,
                            from_time,
                            to_time,
                            resolution="interval"
                        )
                        
                        # Prendre la dernière mesure disponible
                        if measurements and controller_id in measurements:
                            controller_measurements = measurements[controller_id]
                            if abbr_id in controller_measurements and controller_measurements[abbr_id]:
                                last_measurement = controller_measurements[abbr_id][-1]
                                abbr_data["recent_measurement"] = last_measurement
                        
                    except Exception as e:
                        print(f"[{site_key}][{controller_name}][{abbr_id}] Erreur: {e}")
                        abbr_data["error"] = str(e)
                    
                    controller_data["abbreviations"].append(abbr_data)
                
            except Exception as e:
                print(f"[{site_key}][{controller_name}] Erreur lors de l'exploration: {e}")
                controller_data["error"] = str(e)
            
            site_data["controllers"].append(controller_data)
        
    except Exception as e:
        print(f"[{site_key}] Erreur globale: {e}")
        site_data["error"] = str(e)
    
    return site_data


def main():
    """Point d'entrée principal"""
    
    # Sites de test
    test_sites = ["K46XE", "991S7", "JG9P2", "RPSSB"]
    
    print("="*60)
    print("EXPLORATION DES POWER PLANT CONTROLLERS")
    print("="*60)
    print(f"Date: {datetime.now(timezone.utc).isoformat()}")
    print(f"Sites à tester: {', '.join(test_sites)}")
    print("="*60)
    
    # Initialiser le client VCOM
    client = VCOMAPIClient()
    
    # Structure des résultats
    results = {
        "exploration_date": datetime.now(timezone.utc).isoformat(),
        "test_sites": test_sites,
        "sites": {}
    }
    
    # Explorer chaque site
    for site_key in test_sites:
        site_data = explore_site_ppc(client, site_key)
        results["sites"][site_key] = site_data
    
    # Sauvegarder les résultats
    output_file = Path(__file__).parent / "ppc_exploration_results.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print("\n" + "="*60)
    print(f"✓ Exploration terminée")
    print(f"✓ Résultats sauvegardés dans: {output_file}")
    print("="*60)
    
    # Afficher un résumé
    print("\nRÉSUMÉ:")
    for site_key, site_data in results["sites"].items():
        if site_data.get("error"):
            print(f"  {site_key}: ERREUR - {site_data['error']}")
        else:
            nb_controllers = len(site_data.get("controllers", []))
            total_abbr = sum(len(c.get("abbreviations", [])) for c in site_data.get("controllers", []))
            print(f"  {site_key}: {nb_controllers} controller(s), {total_abbr} abréviation(s) au total")


if __name__ == "__main__":
    main()