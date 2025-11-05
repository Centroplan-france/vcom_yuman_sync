#!/usr/bin/env python3
"""
Script de diagnostic pour identifier pourquoi le filtrage des workorders échoue.
"""
import os
from supabase import create_client
from vysync.yuman_client import YumanClient
from vysync.vcom_client import VCOMAPIClient

def diagnostic_workorder_filter():
    # Connexions
    sb = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_KEY"))
    yc = YumanClient(os.getenv("YUMAN_TOKEN"))
    vc = VCOMAPIClient()
    
    print("=" * 70)
    print("DIAGNOSTIC: Filtrage des workorders - ANALYSE COMPLÈTE")
    print("=" * 70)
    
    # 1. Récupérer les yuman_site_id valides depuis sites_mapping
    print("\n1. Récupération des yuman_site_id valides depuis sites_mapping...")
    valid_site_ids_result = sb.table("sites_mapping").select("yuman_site_id").execute()
    print(f"   Résultat brut: {len(valid_site_ids_result.data)} lignes")
    
    # Créer le set (comme dans le code)
    valid_site_ids = {row["yuman_site_id"] for row in valid_site_ids_result.data if row["yuman_site_id"] is not None}
    print(f"   Set final: {len(valid_site_ids)} site_id valides")
    
    # 2. Récupérer les workorders de Yuman
    print("\n2. Récupération des workorders depuis Yuman API...")
    workorders = yc.list_workorders()
    print(f"   {len(workorders)} workorders récupérés")
    
    # 3. Vérifier les workorders DÉJÀ EXISTANTS en DB
    print("\n3. Vérification des workorders EXISTANTS dans work_orders DB...")
    existing_wo = sb.table("work_orders").select("workorder_id, site_id").execute()
    print(f"   {len(existing_wo.data)} workorders en DB")
    
    # Vérifier si 747491 existe déjà en DB
    wo_747491_in_db = [w for w in existing_wo.data if w.get("site_id") == 747491]
    if wo_747491_in_db:
        print(f"\n   ⚠️  PROBLÈME DÉTECTÉ: {len(wo_747491_in_db)} workorder(s) avec site_id=747491 DÉJÀ EN DB:")
        for w in wo_747491_in_db:
            print(f"       - workorder_id: {w['workorder_id']}, site_id: {w['site_id']}")
        print(f"\n   → Ces workorders vont être UPDATE par l'upsert (on_conflict='workorder_id')")
        print(f"   → L'UPDATE va essayer de garder site_id=747491 → VIOLATION FK!")
    
    # 4. Analyser le workorder problématique dans l'API
    print("\n4. Analyse du workorder avec site_id=747491 depuis API...")
    wo_747491_api = [w for w in workorders if w.get("site_id") == 747491]
    if wo_747491_api:
        print(f"   Trouvé {len(wo_747491_api)} workorder(s) avec site_id=747491:")
        for w in wo_747491_api:
            print(f"     - workorder_id: {w.get('id')}")
            print(f"     - site_id: {w.get('site_id')}")
            print(f"     - status: {w.get('status')}")
            print(f"     - Sera filtré par upsert_workorders: {w.get('site_id') not in valid_site_ids}")
    
    # 5. Tester les autres fonctions qui manipulent des workorders
    print("\n5. Analyse des autres fonctions qui insèrent/updatent des workorders...")
    
    # Vérifier create_workorders_for_priority_sites
    print("\n   a) Fonction create_workorders_for_priority_sites:")
    print("      Cette fonction CRÉE de nouveaux workorders via API Yuman")
    print("      puis les INSERT dans work_orders sans vérifier site_id")
    
    # Récupérer les tickets pour voir s'ils concernent le site 747491
    tickets = []
    try:
        for status in ["open", "assigned", "inProgress"]:
            chunk = vc.get_tickets(status=status)
            tickets.extend(chunk)
        print(f"      {len(tickets)} tickets récupérés")
        
        # Chercher les tickets qui pourraient créer un WO pour site 747491
        tickets_urgent = [t for t in tickets if t.get("priority") in ("high", "urgent") and t.get("status") == "open"]
        print(f"      {len(tickets_urgent)} tickets urgent/high/open")
        
        # Pour chaque ticket urgent, vérifier son site
        problematic_tickets = []
        for t in tickets_urgent:
            system_key = t.get("systemKey")
            if system_key:
                site_query = sb.table("sites_mapping").select("yuman_site_id").eq("vcom_system_key", system_key).execute()
                if site_query.data and site_query.data[0].get("yuman_site_id") == 747491:
                    problematic_tickets.append(t)
        
        if problematic_tickets:
            print(f"\n      ⚠️  PROBLÈME POTENTIEL: {len(problematic_tickets)} tickets pourraient créer un WO pour site 747491")
            print(f"      → create_workorders_for_priority_sites pourrait insérer avec site_id=747491")
        
    except Exception as e:
        print(f"      ✗ Erreur récupération tickets: {e}")
    
    # 6. Solution
    print("\n" + "=" * 70)
    print("DIAGNOSTIC COMPLET")
    print("=" * 70)
    
    if wo_747491_in_db:
        print("\n⚠️  CAUSE DE L'ERREUR IDENTIFIÉE:")
        print("   Des workorders avec site_id=747491 existent DÉJÀ dans work_orders")
        print("   L'upsert filtre correctement les nouveaux, MAIS:")
        print("   - Si ces WO existent déjà en DB, le filtrage ne les supprime pas")
        print("   - Ils restent en DB avec un site_id invalide")
        print("   - Toute tentative d'UPDATE ultérieure déclenche la violation FK")
        print("\n   SOLUTIONS POSSIBLES:")
        print("   1. Nettoyer manuellement les workorders avec site_id invalide en DB")
        print("   2. Ajouter une vérification AVANT l'upsert pour supprimer les WO avec site_id invalide")
        print("   3. Modifier la contrainte FK en ON DELETE SET NULL ou ON DELETE CASCADE")
    else:
        print("\n✓ Aucun workorder avec site_id invalide en DB")
        print("  L'erreur vient probablement d'une autre fonction (create_workorders_for_priority_sites)")
    
    print("\n" + "=" * 70)

if __name__ == "__main__":
    diagnostic_workorder_filter()