#!/usr/bin/env python3
"""
Test étendu : Analyse détaillée de 10 onduleurs avec données JSON complètes.
"""

from vysync.vcom_client import VCOMAPIClient
from vysync.adapters.supabase_adapter import SupabaseAdapter
import json
from typing import Dict, List, Optional
from dataclasses import dataclass

@dataclass
class InverterTestResult:
    """Résultat du test pour un onduleur."""
    serial: str
    vcom_system_key: str
    commission_date: Optional[str]
    inverter_id: str
    
    # Données brutes JSON
    list_data: Dict
    detail_data: Dict
    tech_configs: List[Dict]
    
    # Données extraites
    list_vendor: Optional[str]
    list_model: Optional[str]
    detail_vendor: Optional[str]
    detail_model: Optional[str]
    tech_vendor: Optional[str]
    tech_model: Optional[str]
    
    # Diagnostic
    problem_detected: bool
    solution: str

def print_detailed_result(result: InverterTestResult, show_full: bool = False):
    """Affiche les détails complets d'un test."""
    
    print(f"\n{'='*100}")
    print(f"📍 ONDULEUR: {result.serial}")
    print(f"{'='*100}")
    print(f"Site:        {result.vcom_system_key}")
    print(f"Commission:  {result.commission_date or 'N/A'}")
    print(f"Inverter ID: {result.inverter_id}")
    print(f"Diagnostic:  {result.solution}")
    
    # SOURCE 1: get_inverters() (liste)
    print(f"\n{'─'*100}")
    print("📋 SOURCE 1: GET /systems/{key}/inverters (liste)")
    print(f"{'─'*100}")
    print(json.dumps(result.list_data, indent=2))
    print(f"\n  → vendor: {result.list_vendor!r}")
    print(f"  → model:  {result.list_model!r}")
    
    # SOURCE 2: get_inverter_details()
    print(f"\n{'─'*100}")
    print(f"📄 SOURCE 2: GET /systems/{result.vcom_system_key}/inverters/{result.inverter_id}")
    print(f"{'─'*100}")
    print(json.dumps(result.detail_data, indent=2))
    print(f"\n  → vendor: {result.detail_vendor!r}")
    print(f"  → model:  {result.detail_model!r}")
    
    # SOURCE 3: get_technical_data()
    print(f"\n{'─'*100}")
    print(f"⚙️  SOURCE 3: GET /systems/{result.vcom_system_key}/technical-data")
    print(f"{'─'*100}")
    print(f"systemConfigurations (nombre: {len(result.tech_configs)}):")
    for idx, cfg in enumerate(result.tech_configs, 1):
        print(f"\n  Configuration {idx}:")
        print(json.dumps(cfg, indent=4))
    
    print(f"\n  → vendor: {result.tech_vendor!r}")
    print(f"  → model:  {result.tech_model!r}")
    
    # COMPARAISON
    print(f"\n{'─'*100}")
    print("📊 COMPARAISON")
    print(f"{'─'*100}")
    
    print(f"\n{'Source':<30} {'Vendor':<20} {'Model':<30}")
    print(f"{'-'*80}")
    print(f"{'get_inverters() (liste)':<30} {str(result.list_vendor or '-'):<20} {str(result.list_model or '-'):<30}")
    print(f"{'get_inverter_details()':<30} {str(result.detail_vendor or '-'):<20} {str(result.detail_model or '-'):<30}")
    print(f"{'get_technical_data()':<30} {str(result.tech_vendor or '-'):<20} {str(result.tech_model or '-'):<30}")
    
    # DIAGNOSTIC
    print(f"\n{'─'*100}")
    print("🔬 DIAGNOSTIC")
    print(f"{'─'*100}")
    
    if result.problem_detected:
        print("\n⚠️  PROBLÈME DÉTECTÉ:")
        print(f"  • get_inverter_details() retourne vendor='{result.detail_vendor or ''}' model='{result.detail_model or ''}'")
        print(f"  • get_technical_data() contient vendor='{result.tech_vendor}' model='{result.tech_model}'")
        print(f"\n  → SOLUTION: Utiliser systemConfigurations[{result.inverter_id}].inverter au lieu de get_inverter_details()")
    else:
        print("\n✅ Pas de problème détecté")
        print(f"  • get_inverter_details() contient les bonnes données")

def test_single_inverter(vc, sb, serial: str, vcom_key: str) -> Optional[InverterTestResult]:
    """Teste un onduleur et retourne les résultats complets."""
    
    try:
        # Récupérer la commission_date du site
        site_data = sb.sb.table("sites_mapping").select("commission_date").eq("vcom_system_key", vcom_key).single().execute()
        commission_date = site_data.data.get("commission_date") if site_data.data else None
        
        # Source 1: get_inverters() (liste)
        inverters = vc.get_inverters(vcom_key)
        inv_from_list = next((i for i in inverters if i.get("serial") == serial), None)
        
        if not inv_from_list:
            print(f"  ⚠️  {serial} non trouvé dans get_inverters()")
            return None
        
        inverter_id = inv_from_list["id"]
        list_data = inv_from_list.copy()
        list_vendor = inv_from_list.get("vendor")
        list_model = inv_from_list.get("model")
        
        # Source 2: get_inverter_details()
        detail_data = vc.get_inverter_details(vcom_key, inverter_id)
        detail_vendor = detail_data.get("vendor") or None
        detail_model = detail_data.get("model") or None
        
        # Source 3: get_technical_data()
        tech = vc.get_technical_data(vcom_key)
        configs = tech.get("systemConfigurations", [])
        
        # Trouver la config qui correspond à cet onduleur (par index)
        inv_index = next((i for i, inv in enumerate(inverters) if inv["id"] == inverter_id), None)
        
        tech_vendor = None
        tech_model = None
        if inv_index is not None and inv_index < len(configs):
            inv_info = configs[inv_index].get("inverter", {})
            tech_vendor = inv_info.get("vendor")
            tech_model = inv_info.get("model")
        
        # Diagnostic
        problem_detected = False
        solution = "✅ OK"
        
        if not detail_vendor and not detail_model:
            if tech_vendor or tech_model:
                problem_detected = True
                solution = "⚠️  Utiliser technical_data"
            else:
                solution = "❓ Aucune source disponible"
        
        return InverterTestResult(
            serial=serial,
            vcom_system_key=vcom_key,
            commission_date=commission_date,
            inverter_id=inverter_id,
            list_data=list_data,
            detail_data=detail_data,
            tech_configs=configs,
            list_vendor=list_vendor,
            list_model=list_model,
            detail_vendor=detail_vendor,
            detail_model=detail_model,
            tech_vendor=tech_vendor,
            tech_model=tech_model,
            problem_detected=problem_detected,
            solution=solution
        )
        
    except Exception as e:
        print(f"  ❌ Erreur pour {serial}: {e}")
        import traceback
        traceback.print_exc()
        return None

def print_summary_table(results: List[InverterTestResult]):
    """Affiche un tableau récapitulatif."""
    
    print(f"\n{'='*140}")
    print("📊 TABLEAU RÉCAPITULATIF")
    print(f"{'='*140}\n")
    
    # Header
    print(f"{'Serial':<20} {'Site':<8} {'Commission':<12} {'Detail V/M':<20} {'Tech V/M':<20} {'Status':<20}")
    print(f"{'-'*140}")
    
    # Rows
    for r in results:
        detail_vm = f"{(r.detail_vendor or '-')[:8]}/{(r.detail_model or '-')[:8]}"
        tech_vm = f"{(r.tech_vendor or '-')[:8]}/{(r.tech_model or '-')[:8]}"
        
        print(f"{r.serial:<20} {r.vcom_system_key:<8} {r.commission_date or 'N/A':<12} {detail_vm:<20} {tech_vm:<20} {r.solution:<20}")
    
    print(f"{'-'*140}\n")
    
    # Statistiques
    total = len(results)
    problems = sum(1 for r in results if r.problem_detected)
    
    print(f"📈 STATISTIQUES")
    print(f"  Total testés:          {total}")
    print(f"  Problèmes détectés:    {problems} ({problems/total*100:.1f}%)")
    print(f"  get_inverter_details() vide: {sum(1 for r in results if not r.detail_vendor and not r.detail_model)}")
    print(f"  technical_data rempli: {sum(1 for r in results if r.tech_vendor or r.tech_model)}")
    
    # Pattern temporel
    with_date = [r for r in results if r.commission_date]
    if with_date:
        old_sites = [r for r in with_date if r.commission_date and r.commission_date < '2024-01-01']
        new_sites = [r for r in with_date if r.commission_date and r.commission_date >= '2024-01-01']
        
        print(f"\n📅 ANALYSE TEMPORELLE")
        if old_sites:
            old_problems = sum(1 for r in old_sites if r.problem_detected)
            print(f"  Sites < 2024 :  {len(old_sites)} sites, {old_problems} problèmes ({old_problems/len(old_sites)*100:.1f}%)")
        if new_sites:
            new_problems = sum(1 for r in new_sites if r.problem_detected)
            print(f"  Sites >= 2024:  {len(new_sites)} sites, {new_problems} problèmes ({new_problems/len(new_sites)*100:.1f}%)")

def main():
    print("Initialisation...")
    vc = VCOMAPIClient()
    sb = SupabaseAdapter()
    
    print("Récupération d'un échantillon d'onduleurs depuis Supabase...\n")
    
    # Récupérer des onduleurs variés
    equips = sb.sb.table("equipments_mapping") \
        .select("serial_number,brand,model,site_id,vcom_device_id") \
        .eq("category_id", 11102) \
        .eq("is_obsolete", False) \
        .not_.is_("serial_number", "null") \
        .neq("serial_number", "") \
        .limit(15) \
        .execute()
    
    # Enrichir avec les vcom_system_key
    test_data = []
    for eq in equips.data:
        site = sb.sb.table("sites_mapping") \
            .select("vcom_system_key,commission_date") \
            .eq("id", eq["site_id"]) \
            .single() \
            .execute()
        
        if site.data and site.data.get("vcom_system_key"):
            test_data.append({
                "serial": eq["serial_number"],
                "vcom_key": site.data["vcom_system_key"],
                "commission_date": site.data.get("commission_date"),
                "db_brand": eq.get("brand"),
                "db_model": eq.get("model"),
            })
    
    # Limiter à 10
    test_data = test_data[:10]
    
    print(f"✅ {len(test_data)} onduleurs sélectionnés\n")
    
    # Tester chaque onduleur
    results = []
    for idx, data in enumerate(test_data, 1):
        print(f"[{idx}/{len(test_data)}] Test de {data['serial']} (site {data['vcom_key']})...")
        
        result = test_single_inverter(vc, sb, data["serial"], data["vcom_key"])
        if result:
            results.append(result)
    
    # Afficher le tableau récapitulatif
    if results:
        print_summary_table(results)
        
        # Afficher les détails des cas problématiques
        problems = [r for r in results if r.problem_detected]
        ok_cases = [r for r in results if not r.problem_detected]
        
        if problems:
            print(f"\n{'='*100}")
            print(f"🔍 DÉTAILS DES CAS PROBLÉMATIQUES ({len(problems)} onduleur(s))")
            print(f"{'='*100}")
            
            for r in problems:
                print_detailed_result(r)
        
        # Afficher 2 cas OK pour comparaison
        if ok_cases:
            print(f"\n{'='*100}")
            print(f"✅ DÉTAILS DE 2 CAS OK (pour comparaison)")
            print(f"{'='*100}")
            
            for r in ok_cases[:2]:
                print_detailed_result(r)
        
        # Conclusion finale
        print(f"\n{'='*100}")
        print("🎯 CONCLUSION FINALE")
        print(f"{'='*100}\n")
        
        if problems:
            print(f"⚠️  {len(problems)}/{len(results)} onduleurs ont des données vides dans get_inverter_details()")
            print(f"\n🔧 CORRECTION NÉCESSAIRE:")
            print(f"   1. Pour les sites < 2024 : get_inverter_details() peut être vide")
            print(f"   2. Pour TOUS les sites : technical_data contient toujours les bonnes données")
            print(f"   3. RECOMMANDATION : Utiliser TOUJOURS technical_data.systemConfigurations")
            print(f"\n📝 STRATÉGIE:")
            print(f"   • Supprimer l'appel à get_inverter_details() (économie API)")
            print(f"   • Utiliser systemConfigurations[index].inverter.vendor/model")
            print(f"   • Index = position de l'onduleur dans get_inverters()")
        else:
            print("✅ Tous les onduleurs testés ont des données valides dans get_inverter_details()")
            print("   → Le problème initial pourrait être résolu ou spécifique à d'autres sites")
    
    else:
        print("\n❌ Aucun résultat valide obtenu")

if __name__ == "__main__":
    main()