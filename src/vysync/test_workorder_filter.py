#!/usr/bin/env python3
"""
Test complet : Simulation EXACTE du workflow GitHub Actions en 2 étapes
pour identifier précisément où et pourquoi l'erreur FK se produit.
"""
import logging
import os
from supabase import create_client
from vysync.adapters.supabase_adapter import SupabaseAdapter, Client
from vysync.adapters.yuman_adapter import YumanAdapter
from vysync.diff import diff_fill_missing
from vysync.yuman_client import YumanClient
from vysync.vcom_client import VCOMAPIClient
from vysync.sync_tickets_workorders import (
    collect_vcom_tickets,
    collect_yuman_workorders,
    sync_tickets_to_db,
    sync_workorders_to_db,
    assign_tickets_to_active_workorders,
    create_workorders_for_priority_sites,
    close_tickets_of_closed_workorders
)

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s | %(message)s'
)
logger = logging.getLogger(__name__)

def test_github_workflow_complete():
    """
    Simule EXACTEMENT le workflow GitHub Actions en 2 étapes.
    """
    print("=" * 80)
    print("TEST COMPLET - SIMULATION WORKFLOW GITHUB ACTIONS")
    print("=" * 80)
    
    # Setup connexions
    sb_client = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_KEY"))
    sb = SupabaseAdapter()
    yc = YumanClient(os.getenv("YUMAN_TOKEN"))
    vc = VCOMAPIClient()
    
    print("\n" + "=" * 80)
    print("ÉTAPE 1/2 : SYNC YUMAN → SUPABASE MAPPINGS")
    print("=" * 80)
    
    print("\n→ État AVANT la synchro des mappings...")
    
    # Capturer l'état initial
    initial_sites_mapping = sb_client.table("sites_mapping").select("yuman_site_id").execute()
    initial_valid_site_ids = {
        row["yuman_site_id"] 
        for row in initial_sites_mapping.data 
        if row["yuman_site_id"] is not None
    }
    logger.info(f"  Sites valides AVANT sync: {len(initial_valid_site_ids)}")
    
    # Vérifier si 747491 existe avant
    has_747491_before = 747491 in initial_valid_site_ids
    logger.info(f"  Site 747491 présent AVANT: {has_747491_before}")
    
    print("\n→ Exécution de la synchro des mappings (comme GitHub Action)...")
    
    try:
        # 1) Snapshot Yuman (EXACTEMENT comme dans le workflow)
        y = YumanAdapter(sb)
        logger.info("[YUMAN→DB] snapshot & patch fill-missing …")
        y_clients_raw = list(yc.list_clients())
        y_sites = y.fetch_sites()
        
        logger.info(f"  Clients Yuman récupérés: {len(y_clients_raw)}")
        logger.info(f"  Sites Yuman récupérés: {len(y_sites)}")
        
        # Vérifier si 747491 est dans les sites Yuman
        has_747491_in_yuman = 747491 in y_sites
        logger.info(f"  Site 747491 présent dans API Yuman: {has_747491_in_yuman}")
        
        # 2) Mappings existants en base
        db_clients = sb.fetch_clients()
        db_maps_sites = sb.fetch_sites_y()
        
        logger.info(f"  Clients en DB: {len(db_clients)}")
        logger.info(f"  Sites mappings en DB: {len(db_maps_sites)}")
        
        def to_client(row: dict) -> Client:
            return Client(
                yuman_client_id=row["id"],
                name=row.get("name"),
                code=row.get("code"),
                address=row.get("address") or row.get("billing_address")
            )
        
        y_clients = {r["id"]: to_client(r) for r in y_clients_raw}
        
        # 3) Diff « fill missing »
        patch_clients = diff_fill_missing(db_clients, y_clients)
        patch_maps_sites = diff_fill_missing(db_maps_sites, y_sites, fields=[
            "yuman_site_id", "code", "client_map_id", "name",
            "aldi_id", "aldi_store_id", "project_number_cp",
            "commission_date", "nominal_power"
        ])
        
        logger.info(
            "[YUMAN→DB] Clients Δ +%d ~%d -%d",
            len(patch_clients.add), len(patch_clients.update), len(patch_clients.delete),
        )
        logger.info(
            "[YUMAN→DB] SitesMapping Δ +%d ~%d -%d",
            len(patch_maps_sites.add), len(patch_maps_sites.update), len(patch_maps_sites.delete),
        )
        
        # Vérifier si 747491 serait ajouté/supprimé/modifié
        if patch_maps_sites.add:
            added_747491 = any(s.yuman_site_id == 747491 for s in patch_maps_sites.add)
            if added_747491:
                logger.info("  ⚠️  Site 747491 serait AJOUTÉ par cette synchro")
        
        if patch_maps_sites.delete:
            deleted_747491 = any(s.yuman_site_id == 747491 for s in patch_maps_sites.delete)
            if deleted_747491:
                logger.warning("  ⚠️  Site 747491 serait SUPPRIMÉ par cette synchro")
        
        # 4) Application des patchs (MODE DRY - ne pas modifier réellement)
        logger.info("\n  [DRY MODE] Simulation de l'application des patchs...")
        logger.info("  (en production, cela modifierait la DB)")
        
        # sb.apply_clients_mapping_patch(patch_clients)
        # sb.apply_sites_patch(patch_maps_sites)
        
        logger.info("  ✓ ÉTAPE 1 terminée (mode simulation)")
        
    except Exception as e:
        logger.error(f"  ✗ ERREUR dans ÉTAPE 1: {e}")
        import traceback
        traceback.print_exc()
        return
    
    print("\n→ État APRÈS la synchro des mappings...")
    
    # Recapturer l'état (même en dry mode, pour comparaison)
    after_sites_mapping = sb_client.table("sites_mapping").select("yuman_site_id").execute()
    after_valid_site_ids = {
        row["yuman_site_id"] 
        for row in after_sites_mapping.data 
        if row["yuman_site_id"] is not None
    }
    logger.info(f"  Sites valides APRÈS sync: {len(after_valid_site_ids)}")
    
    has_747491_after = 747491 in after_valid_site_ids
    logger.info(f"  Site 747491 présent APRÈS: {has_747491_after}")
    
    # Analyser les changements
    added_sites = after_valid_site_ids - initial_valid_site_ids
    removed_sites = initial_valid_site_ids - after_valid_site_ids
    
    if added_sites:
        logger.info(f"  Sites AJOUTÉS: {len(added_sites)}")
        if 747491 in added_sites:
            logger.info("    → 747491 a été AJOUTÉ")
    
    if removed_sites:
        logger.warning(f"  Sites SUPPRIMÉS: {len(removed_sites)}")
        if 747491 in removed_sites:
            logger.warning("    → 747491 a été SUPPRIMÉ")
    
    print("\n" + "=" * 80)
    print("ÉTAPE 2/2 : SYNC TICKETS & WORKORDERS")
    print("=" * 80)
    
    print("\n→ Collecte des données...")
    
    try:
        # 1. Collecte (EXACTEMENT comme dans main())
        tickets = collect_vcom_tickets(vc)
        workorders = collect_yuman_workorders(yc)
        
        logger.info(f"  Tickets VCOM récupérés: {len(tickets)}")
        logger.info(f"  Workorders Yuman récupérés: {len(workorders)}")
        
        # Analyser les workorders problématiques
        wo_with_747491 = [w for w in workorders if w.get("site_id") == 747491]
        if wo_with_747491:
            logger.warning(f"\n  ⚠️  {len(wo_with_747491)} workorder(s) avec site_id=747491:")
            for w in wo_with_747491:
                logger.warning(f"      - WO#{w.get('id')}: status={w.get('status')}")
        
        # Vérifier si ces workorders existent déjà en DB
        if wo_with_747491:
            existing_wo_ids = [w.get('id') for w in wo_with_747491]
            for wo_id in existing_wo_ids:
                check = sb_client.table("work_orders").select("*").eq("workorder_id", wo_id).execute()
                if check.data:
                    logger.warning(f"      WO#{wo_id} EXISTE DÉJÀ en DB avec site_id={check.data[0].get('site_id')}")
                else:
                    logger.info(f"      WO#{wo_id} N'existe PAS en DB")
        
        print("\n→ Simulation de sync_workorders_to_db...")
        
        # Récupérer les site_ids valides (comme dans upsert_workorders)
        valid_site_ids_result = sb_client.table("sites_mapping").select("yuman_site_id").execute()
        valid_site_ids = {
            row["yuman_site_id"] 
            for row in valid_site_ids_result.data 
            if row["yuman_site_id"] is not None
        }
        
        logger.info(f"  Site_ids valides pour le filtrage: {len(valid_site_ids)}")
        logger.info(f"  747491 est valide: {747491 in valid_site_ids}")
        
        # Filtrage
        valid_orders = []
        ignored_orders = []
        
        for w in workorders:
            site_id = w.get("site_id")
            if site_id in valid_site_ids:
                valid_orders.append(w)
            else:
                ignored_orders.append(w)
        
        logger.info(f"  Workorders valides: {len(valid_orders)}")
        logger.info(f"  Workorders ignorés: {len(ignored_orders)}")
        
        if ignored_orders:
            logger.info(f"\n  Workorders ignorés détails:")
            for w in ignored_orders:
                logger.info(f"    - WO#{w.get('id')}: site_id={w.get('site_id')}")
        
        # Construction des rows
        rows = [
            {
                "workorder_id": w["id"],
                "status": w.get("status"),
                "client_id": w.get("client_id"),
                "site_id": w.get("site_id"),
                "scheduled_date": w.get("date_planned"),
                "description": w.get("description"),
                "title": w.get("title"),
            }
            for w in valid_orders
        ]
        
        logger.info(f"  Rows à upsert: {len(rows)}")
        
        # Vérification finale de sécurité
        invalid_rows = [r for r in rows if r["site_id"] not in valid_site_ids]
        if invalid_rows:
            logger.error(f"\n  ❌ PROBLÈME DÉTECTÉ: {len(invalid_rows)} rows avec site_id INVALIDE!")
            for r in invalid_rows:
                logger.error(f"      - workorder_id={r['workorder_id']}, site_id={r['site_id']}")
            logger.error("  → Ces rows causeraient la violation FK!")
        else:
            logger.info("  ✓ Tous les rows ont un site_id valide")
        
        print("\n→ Test des autres fonctions...")
        
        # Test create_workorders_for_priority_sites
        logger.info("\n  Test create_workorders_for_priority_sites...")
        
        active_sites = {
            w["site_id"] for w in workorders if w.get("status", "").lower() != "closed"
        }
        
        by_site = {}
        for t in tickets:
            if t.get("status") == "open" and t.get("priority") in ("high", "urgent"):
                row = (
                    sb_client.table("sites_mapping")
                    .select("yuman_site_id")
                    .eq("vcom_system_key", t.get("systemKey"))
                    .execute()
                ).data
                if row and row[0]["yuman_site_id"] is not None:
                    site_id = row[0]["yuman_site_id"]
                    by_site.setdefault(site_id, []).append(t)
        
        would_create_wo = []
        for site_id, ts in by_site.items():
            if site_id not in active_sites:
                would_create_wo.append((site_id, len(ts)))
                if site_id not in valid_site_ids:
                    logger.error(f"    ❌ Site {site_id} créerait un WO MAIS n'est PAS valide!")
                    logger.error(f"       → Cela causerait une violation FK lors de l'INSERT")
        
        if would_create_wo:
            logger.info(f"  {len(would_create_wo)} site(s) créeraient un nouveau WO:")
            for site_id, count in would_create_wo:
                status = "✓" if site_id in valid_site_ids else "✗"
                logger.info(f"    {status} site_id={site_id} ({count} ticket(s))")
        
    except Exception as e:
        logger.error(f"  ✗ ERREUR dans ÉTAPE 2: {e}")
        import traceback
        traceback.print_exc()
        return
    
    print("\n" + "=" * 80)
    print("ANALYSE FINALE")
    print("=" * 80)
    
    print("\n📊 RÉSUMÉ DES DÉCOUVERTES:")
    print(f"  • Site 747491 dans Yuman API: {has_747491_in_yuman if 'has_747491_in_yuman' in locals() else 'N/A'}")
    print(f"  • Site 747491 dans sites_mapping AVANT: {has_747491_before}")
    print(f"  • Site 747491 dans sites_mapping APRÈS: {has_747491_after}")
    print(f"  • Workorders avec site_id=747491: {len(wo_with_747491) if 'wo_with_747491' in locals() else 0}")
    print(f"  • Ces WO seraient ignorés par le filtrage: {len(wo_with_747491) > 0 and not has_747491_after}")
    
    print("\n🔍 HYPOTHÈSES:")
    
    if wo_with_747491 and not has_747491_after:
        print("\n  ⚠️  SCÉNARIO PROBABLE IDENTIFIÉ:")
        print("  1. Des workorders Yuman référencent site_id=747491")
        print("  2. MAIS ce site n'existe PAS (ou plus) dans sites_mapping")
        print("  3. Le filtrage les ignore correctement")
        print("  4. CEPENDANT:")
        
        # Vérifier si ces WO existent en DB
        for w in wo_with_747491:
            check = sb_client.table("work_orders").select("*").eq("workorder_id", w.get('id')).execute()
            if check.data:
                print(f"\n     ❌ PROBLÈME: WO#{w.get('id')} existe DÉJÀ en DB!")
                print(f"        • Actuellement en DB avec site_id={check.data[0].get('site_id')}")
                print(f"        • L'upsert va essayer de l'UPDATE")
                print(f"        • Mais site_id={check.data[0].get('site_id')} est invalide")
                print(f"        → VIOLATION FK lors de l'UPDATE!")
                print("\n     💡 SOLUTION:")
                print("        Supprimer ce workorder de la DB ou corriger son site_id")
    
    if not has_747491_in_yuman and has_747491_before and not has_747491_after:
        print("\n  ⚠️  Site 747491 a été SUPPRIMÉ de sites_mapping")
        print("     (probablement via le diff_fill_missing)")
        print("     Mais des workorders le référencent encore")
    
    print("\n" + "=" * 80)

if __name__ == "__main__":
    test_github_workflow_complete()