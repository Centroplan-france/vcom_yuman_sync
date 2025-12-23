#!/usr/bin/env python3
"""
Script de test pour diagnostiquer le problème de récupération PPC.

Teste plusieurs sites connus avec différents types de PPC :
- JG9P2 : devrait avoir PPC_P_SET_ABS = -1000 W
- K46XE : devrait avoir PPC_P_SET_GRIDOP_REL = -2.37%
- GQRRQ : devrait avoir PPC_P_SET_REL = 100%
- 4FXLS : ne devrait pas avoir de PPC
"""

import json
import logging
from datetime import datetime, timedelta, timezone

from vysync.vcom_client import VCOMAPIClient
from vysync.sync_ppc_data import get_measurement_period, PPC_ABBREVIATIONS_PRIORITY

# Configuration du logging
logging.basicConfig(
    level=logging.DEBUG,  # DEBUG pour voir tous les détails
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Sites de test
TEST_SITES = [
    {"key": "JG9P2", "name": "ALDI France Corbigny", "expected": "PPC_P_SET_ABS"},
    {"key": "K46XE", "name": "ALDI France Cambrai", "expected": "PPC_P_SET_GRIDOP_REL"},
    {"key": "GQRRQ", "name": "ALDI France Longfossé", "expected": "PPC_P_SET_REL"},
    {"key": "4FXLS", "name": "ALDI France Le Nouvion", "expected": "NO_PPC"},
]


def test_site(vc: VCOMAPIClient, site_key: str, site_name: str, expected_abbr: str):
    """
    Teste la récupération PPC complète pour un site.
    
    Args:
        vc: Client VCOM
        site_key: Clé du site (ex: "JG9P2")
        site_name: Nom du site (pour affichage)
        expected_abbr: Abréviation attendue
    """
    print(f"\n{'='*80}")
    print(f"TEST : {site_name} ({site_key})")
    print(f"Abréviation attendue : {expected_abbr}")
    print(f"{'='*80}")
    
    try:
        # ÉTAPE 1 : Récupérer les controllers
        print(f"\n[ÉTAPE 1] Récupération des controllers...")
        controllers = vc.get_power_plant_controllers(site_key)
        print(f"✅ Nombre de controllers trouvés : {len(controllers)}")
        
        if not controllers:
            print("❌ Aucun controller trouvé")
            return
        
        controller = controllers[0]
        controller_id = controller["id"]
        print(f"✅ Controller ID : {controller_id}")
        print(f"   Name : {controller.get('name')}")
        print(f"   UID : {controller.get('uid')}")
        
        # ÉTAPE 2 : Lister les abréviations disponibles
        print(f"\n[ÉTAPE 2] Liste des abréviations disponibles...")
        abbreviations = vc.get_ppc_abbreviations(site_key, controller_id)
        print(f"✅ Abréviations disponibles ({len(abbreviations)}) :")
        for abbr in abbreviations:
            marker = "🎯" if abbr in PPC_ABBREVIATIONS_PRIORITY else "  "
            print(f"   {marker} {abbr}")
        
        # ÉTAPE 3 : Déterminer quelle abréviation utiliser
        print(f"\n[ÉTAPE 3] Sélection de l'abréviation selon priorité...")
        print(f"Ordre de priorité : {PPC_ABBREVIATIONS_PRIORITY}")
        
        target_abbr = None
        for abbr in PPC_ABBREVIATIONS_PRIORITY:
            if abbr in abbreviations:
                target_abbr = abbr
                print(f"✅ Abréviation sélectionnée : {target_abbr}")
                break
        
        if not target_abbr:
            print(f"❌ Aucune abréviation pertinente trouvée")
            return
        
        # ÉTAPE 4 : Récupérer les métadonnées de l'abréviation
        print(f"\n[ÉTAPE 4] Récupération des métadonnées de {target_abbr}...")
        metadata = vc.get_ppc_abbreviation_info(site_key, controller_id, target_abbr)
        print(f"✅ Métadonnées :")
        print(f"   Description : {metadata.get('description')}")
        print(f"   Unité : {metadata.get('unit')}")
        print(f"   Précision : {metadata.get('precision')}")
        print(f"   Agrégation : {metadata.get('aggregation')}")
        
        # ÉTAPE 5 : Récupérer la période de mesure
        print(f"\n[ÉTAPE 5] Calcul de la période de mesure...")
        from_time, to_time = get_measurement_period()
        print(f"✅ Période : {from_time.isoformat()} → {to_time.isoformat()}")
        
        # ÉTAPE 6 : Récupérer les mesures
        print(f"\n[ÉTAPE 6] Récupération des mesures pour {target_abbr}...")
        measurements = vc.get_ppc_measurements(
            system_key=site_key,
            device_id=controller_id,
            abbreviation_id=target_abbr,
            from_time=from_time,
            to_time=to_time,
            resolution="interval"
        )
        
        print(f"✅ Réponse brute de l'API :")
        print(json.dumps(measurements, indent=2, default=str))
        
        # ÉTAPE 7 : Parser les mesures
        print(f"\n[ÉTAPE 7] Parsing des mesures...")
        
        # Vérifier la structure de la réponse
        if not measurements:
            print(f"❌ Réponse vide")
            return
        
        print(f"   Clés dans la réponse : {list(measurements.keys())}")
        
        # Essayer différentes structures possibles
        recent_measurement = None
        
        # Structure 1 : {"recent_measurement": {...}}
        if "recent_measurement" in measurements:
            recent_measurement = measurements["recent_measurement"]
            print(f"✅ Structure 1 détectée : recent_measurement à la racine")
        
        # Structure 2 : {controller_id: {abbr_id: [...]}}
        elif controller_id in measurements:
            controller_measurements = measurements[controller_id]
            print(f"✅ Structure 2 détectée : controller_id dans la réponse")
            print(f"   Clés dans controller : {list(controller_measurements.keys())}")
            
            if target_abbr in controller_measurements:
                measurements_list = controller_measurements[target_abbr]
                print(f"   Type de {target_abbr} : {type(measurements_list)}")
                print(f"   Contenu : {measurements_list}")
                
                if isinstance(measurements_list, list) and len(measurements_list) > 0:
                    recent_measurement = measurements_list[-1]
                    print(f"✅ Dernière mesure extraite : {recent_measurement}")
        
        # Structure 3 : autre ?
        else:
            print(f"⚠️  Structure inconnue, inspection manuelle nécessaire")
        
        if recent_measurement is None:
            print(f"❌ Aucune mesure trouvée (recent_measurement = None)")
            return
        
        # ÉTAPE 8 : Extraire la valeur
        print(f"\n[ÉTAPE 8] Extraction de la valeur...")
        print(f"   Type de recent_measurement : {type(recent_measurement)}")
        print(f"   Contenu : {recent_measurement}")
        
        if isinstance(recent_measurement, dict):
            value = recent_measurement.get("value")
            timestamp = recent_measurement.get("timestamp")
            print(f"✅ Valeur : {value} {metadata.get('unit')}")
            print(f"✅ Timestamp : {timestamp}")
            
            if value is None:
                print(f"❌ Valeur est None")
                return
            
            # ÉTAPE 9 : Conversion
            print(f"\n[ÉTAPE 9] Conversion de la valeur...")
            nominal_power = 1000  # kW fictif pour le test
            
            if target_abbr == "PPC_P_SET_ABS":
                value_kw = value / 1000.0
                print(f"✅ Conversion W → kW : {value} W / 1000 = {value_kw} kW")
            else:
                value_kw = (nominal_power * value / 100.0) / 1000.0
                print(f"✅ Conversion % → kW : ({nominal_power} × {value} / 100) / 1000 = {value_kw} kW")
            
            # RÉSULTAT FINAL
            print(f"\n{'='*80}")
            print(f"✅ SUCCÈS !")
            print(f"   Controller ID : {controller_id}")
            print(f"   Abréviation utilisée : {target_abbr}")
            print(f"   Valeur brute : {value} {metadata.get('unit')}")
            print(f"   Valeur en kW : {value_kw} kW")
            print(f"   Timestamp : {timestamp}")
            print(f"{'='*80}")
        else:
            print(f"❌ Format inattendu pour recent_measurement")
    
    except Exception as e:
        print(f"\n❌ ERREUR : {e}")
        import traceback
        traceback.print_exc()


def main():
    """Point d'entrée principal."""
    print("""
╔════════════════════════════════════════════════════════════════════════════╗
║                    DIAGNOSTIC PPC - SCRIPT DE TEST                         ║
╚════════════════════════════════════════════════════════════════════════════╝

Ce script teste la récupération des données PPC sur plusieurs sites connus
pour identifier où se situe le problème dans la chaîne de traitement.
""")
    
    # Initialiser le client VCOM
    vc = VCOMAPIClient()
    
    # Tester chaque site
    for site in TEST_SITES:
        test_site(vc, site["key"], site["name"], site["expected"])
        input("\n⏸️  Appuyez sur Entrée pour continuer avec le site suivant...")
    
    print(f"\n{'='*80}")
    print("✅ Tests terminés")
    print(f"{'='*80}")


if __name__ == "__main__":
    main()