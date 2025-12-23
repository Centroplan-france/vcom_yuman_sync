#!/usr/bin/env python3
"""
Script de démonstration pour le support Yuman - Champs non-modifiables.

Ce script démontre que certains champs acceptés par l'API (status 200)
ne sont pas réellement modifiés dans Yuman.

Usage:
    python demo_yuman_support.py
"""

import os
import json
from typing import Dict, Any
from dotenv import load_dotenv
from vysync.yuman_client import YumanClient

load_dotenv()

# Couleurs pour le terminal
class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'

def print_header(text: str):
    print(f"\n{Colors.HEADER}{Colors.BOLD}{'='*80}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{text}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{'='*80}{Colors.ENDC}\n")

def print_json(label: str, data: Any):
    print(f"\n{Colors.BOLD}{label}{Colors.ENDC}")
    print(json.dumps(data, indent=2, ensure_ascii=False))

def print_comparison(field: str, before: Any, after: Any, expected: Any):
    """Affiche une comparaison avant/après pour un champ."""
    changed = (before != after)
    matches_expected = (after == expected)
    
    if changed and matches_expected:
        status = f"{Colors.OKGREEN}✅ MODIFIÉ (comme attendu){Colors.ENDC}"
    elif changed and not matches_expected:
        status = f"{Colors.WARNING}⚠️  MODIFIÉ (valeur inattendue){Colors.ENDC}"
    elif not changed and expected != before:
        status = f"{Colors.FAIL}❌ NON MODIFIÉ (API a ignoré la valeur){Colors.ENDC}"
    else:
        status = f"{Colors.OKBLUE}ℹ️  Inchangé (comme attendu){Colors.ENDC}"
    
    print(f"\n  {Colors.BOLD}{field}:{Colors.ENDC}")
    print(f"    Avant:    {before}")
    print(f"    Demandé:  {expected}")
    print(f"    Après:    {after}")
    print(f"    Statut:   {status}")


def main():
    print_header("🧪 DÉMONSTRATION - Champs non-modifiables de l'API Yuman")
    
    print("Ce script démontre que certains champs du PATCH /materials/{id}")
    print("sont acceptés par l'API (HTTP 200) mais ne sont pas réellement modifiés.")
    print("\nÉquipement de test : STRING-04-WR2-MPPT-2.2-E3K2L (ID: 1164852)")
    
    # Initialisation
    yc = YumanClient()
    material_id = 1164852
    
    # =========================================================================
    # ÉTAPE 1 : État initial
    # =========================================================================
    print_header("ÉTAPE 1 : État initial du matériel")
    
    initial_state = yc.get_material(material_id, embed="fields")
    print(f"initial_state : {initial_state}")
    # Extraction des champs pertinents
    initial_data = {
        "name": initial_state.get("name"),
        "count": initial_state.get("count"),
        "model": initial_state.get("model"),
        "parent_id": initial_state.get("parent_id"),
        "site_id": initial_state.get("site_id"),
        "brand": initial_state.get("brand"),
    }
    
    # Extraction des fields custom
    fields = initial_state.get("_embed", {}).get("fields", [])
    custom_fields = {f["name"]: f.get("value") for f in fields}
    
    print_json("État initial (champs standard):", initial_data)
    print_json("État initial (champs custom):", custom_fields)
    
    # =========================================================================
    # ÉTAPE 2 : Tentative de modification
    # =========================================================================
    print_header("ÉTAPE 2 : Tentative de modification via PATCH")
    
    # Payload contenant des champs standards non documentés
    test_payload = {
        "name": "NOUVEAU_NOM_TEST",
        "count": 999,
        "model": "NOUVEAU_MODEL_TEST",
        "parent_id": 123456,
        "site_id": 999999,
        "brand": "NOUVELLE_MARQUE_TEST",
    }
    
    print_json("Payload envoyé à PATCH /materials/1164852:", test_payload)
    
    print(f"\n{Colors.WARNING}⏳ Envoi du PATCH à l'API Yuman...{Colors.ENDC}")
    
    try:
        response = yc.update_material(material_id, test_payload)
        print(f"response : {response}")
        print(f"\n{Colors.OKGREEN}✅ API Response: HTTP 200 OK{Colors.ENDC}")
        print(f"{Colors.OKGREEN}   L'API a accepté la requête sans erreur.{Colors.ENDC}")
    except Exception as e:
        print(f"\n{Colors.FAIL}❌ Erreur lors du PATCH: {e}{Colors.ENDC}")
        return
    
    # =========================================================================
    # ÉTAPE 3 : Vérification de l'état après modification
    # =========================================================================
    print_header("ÉTAPE 3 : Vérification de l'état réel après PATCH")
    
    final_state = yc.get_material(material_id, embed="fields")
    
    final_data = {
        "name": final_state.get("name"),
        "count": final_state.get("count"),
        "model": final_state.get("model"),
        "parent_id": final_state.get("parent_id"),
        "site_id": final_state.get("site_id"),
        "brand": final_state.get("brand"),
    }
    
    print_json("État final (après PATCH):", final_data)
    
    # =========================================================================
    # ÉTAPE 4 : Analyse des différences
    # =========================================================================
    print_header("ÉTAPE 4 : Analyse détaillée des modifications")
    
    for field in test_payload.keys():
        print_comparison(
            field,
            before=initial_data.get(field),
            after=final_data.get(field),
            expected=test_payload[field]
        )
    
    # =========================================================================
    # ÉTAPE 5 : Restauration de la valeur brand
    # =========================================================================
    print_header("ÉTAPE 5 : Restauration de la valeur originale (brand)")
    
    if initial_data["brand"] != final_data["brand"]:
        restore_payload = {"brand": initial_data["brand"]}
        print_json("Payload de restauration:", restore_payload)
        
        try:
            yc.update_material(material_id, restore_payload)
            print(f"\n{Colors.OKGREEN}✅ Valeur originale restaurée avec succès{Colors.ENDC}")
        except Exception as e:
            print(f"\n{Colors.FAIL}❌ Erreur lors de la restauration: {e}{Colors.ENDC}")
    
    # =========================================================================
    # SYNTHÈSE
    # =========================================================================
    print_header("📊 SYNTHÈSE")
    
    modified_fields = []
    ignored_fields = []
    
    for field, expected_value in test_payload.items():
        if final_data.get(field) == expected_value:
            modified_fields.append(field)
        elif final_data.get(field) != initial_data.get(field):
            modified_fields.append(f"{field} (valeur inattendue)")
        else:
            ignored_fields.append(field)
    
    print(f"\n{Colors.OKGREEN}{Colors.BOLD}Champs réellement modifiés:{Colors.ENDC}")
    if modified_fields:
        for f in modified_fields:
            print(f"  ✅ {f}")
    else:
        print("  (aucun)")
    
    print(f"\n{Colors.FAIL}{Colors.BOLD}Champs ignorés par l'API (acceptés mais non modifiés):{Colors.ENDC}")
    if ignored_fields:
        for f in ignored_fields:
            print(f"  ❌ {f}")
    else:
        print("  (aucun)")
    
    # =========================================================================
    # INFORMATIONS POUR LE SUPPORT
    # =========================================================================
    print_header("📧 INFORMATIONS POUR LE SUPPORT YUMAN")
    
    support_info = {
        "issue": "Champs acceptés par l'API mais non modifiés",
        "endpoint": "PATCH /v1/materials/{id}",
        "material_id": material_id,
        "http_status": "200 OK (pas d'erreur)",
        "probleme": "L'API accepte certains champs dans le payload mais ne les modifie pas réellement",
        "champs_ignores": ignored_fields,
        "champs_modifiables": modified_fields,
        "payload_test": test_payload,
        "etat_avant": initial_data,
        "etat_apres": final_data,
        "question": "Quels sont les champs réellement modifiables via PATCH /materials/{id} ? La documentation ne précise pas cette liste complète.",
    }
    
    print_json("Données à fournir au support:", support_info)
    
    # Sauvegarder dans un fichier pour faciliter l'envoi
    output_file = "yuman_support_demo.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(support_info, f, indent=2, ensure_ascii=False)
    
    print(f"\n{Colors.OKGREEN}✅ Rapport complet sauvegardé dans : {output_file}{Colors.ENDC}")
    print(f"{Colors.OKBLUE}ℹ️  Vous pouvez envoyer ce fichier au support Yuman{Colors.ENDC}")
    
    print_header("✅ DÉMONSTRATION TERMINÉE")


if __name__ == "__main__":
    main()