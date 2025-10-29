#!/usr/bin/env python3
"""
Test de l'hypothèse : tri par puissance des onduleurs

Objectif :
  Analyser si VCOM trie les onduleurs par puissance (croissante/décroissante)
  dans get_inverters() vs systemConfigurations[]

Test sur 20 sites avec 2+ onduleurs (dont des sites à 3+ onduleurs).
"""

import sys
import re
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
import json

# Ajouter src au path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from vysync.vcom_client import VCOMAPIClient

# Couleurs
class C:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    END = '\033[0m'
    BOLD = '\033[1m'


def print_header(text: str):
    print(f"\n{C.HEADER}{C.BOLD}{'='*100}{C.END}")
    print(f"{C.HEADER}{C.BOLD}{text.center(100)}{C.END}")
    print(f"{C.HEADER}{C.BOLD}{'='*100}{C.END}\n")


def print_section(text: str):
    print(f"\n{C.BLUE}{C.BOLD}{text}{C.END}")
    print(f"{C.BLUE}{'-'*100}{C.END}")


def extract_power_from_model(model: str) -> Optional[float]:
    """
    Extrait la puissance depuis le modèle de l'onduleur.
    
    Exemples :
      SG125CX-P2 → 125.0
      SG40CX-P2 → 40.0
      RPI M50A → 50.0
      SG110CX → 110.0
    
    Returns:
        Puissance en kW ou None si non extrait
    """
    if not model:
        return None
    
    # Pattern principal : chiffres après SG/RPI/etc
    patterns = [
        r'SG(\d+)',           # Sungrow: SG125, SG40, SG110
        r'RPI\s*[A-Z]?\s*(\d+)',  # Delta: RPI M50A
        r'(\d+)CX',           # Fallback: 125CX, 40CX
        r'(\d+)[A-Z]',        # Fallback générique
    ]
    
    for pattern in patterns:
        match = re.search(pattern, model, re.IGNORECASE)
        if match:
            return float(match.group(1))
    
    return None


def normalize_model(model: str) -> str:
    """Normalise un modèle pour comparaison."""
    if not model:
        return ""
    return model.strip().upper().replace(" ", "").replace("-", "")


def analyze_site_power_sorting(vc: VCOMAPIClient, site_key: str) -> Dict[str, Any]:
    """
    Analyse un site pour détecter si les onduleurs sont triés par puissance.
    
    Returns:
        Dict avec les résultats détaillés
    """
    print_section(f"📍 SITE: {site_key}")
    
    # 1. Récupérer les données
    print(f"  {C.YELLOW}⏳ Récupération des données...{C.END}")
    
    inverters = vc.get_inverters(site_key)
    tech = vc.get_technical_data(site_key)
    configs = tech.get("systemConfigurations", [])
    
    print(f"  {C.GREEN}✓ {len(inverters)} onduleur(s), {len(configs)} configuration(s){C.END}")
    
    # 2. Collecter les infos de chaque onduleur
    inverters_data = []
    
    for idx, inv in enumerate(inverters):
        inv_id = inv.get("id")
        inv_name = inv.get("name")
        inv_serial = inv.get("serial")
        
        # Récupérer les détails
        try:
            det_inv = vc.get_inverter_details(site_key, inv_id)
            det_model = det_inv.get("model") or ""
            det_power = extract_power_from_model(det_model)
        except Exception as e:
            print(f"    {C.RED}✗ Erreur details({inv_id}): {e}{C.END}")
            det_model = ""
            det_power = None
        
        inverters_data.append({
            "index": idx,
            "id": inv_id,
            "serial": inv_serial,
            "name": inv_name,
            "model": det_model,
            "power_kw": det_power,
        })
    
    # 3. Extraire les configs
    configs_data = []
    
    for idx, cfg in enumerate(configs):
        cfg_inv = cfg.get("inverter", {})
        cfg_model = cfg_inv.get("model", "")
        cfg_power = extract_power_from_model(cfg_model)
        
        configs_data.append({
            "index": idx,
            "model": cfg_model,
            "power_kw": cfg_power,
            "count": cfg_inv.get("count", 0),
        })
    
    # 4. Afficher les données
    print(f"\n  {C.BOLD}ONDULEURS (get_inverters + details) :{C.END}")
    for inv in inverters_data:
        power_str = f"{inv['power_kw']}kW" if inv['power_kw'] else "?"
        print(f"    [{inv['index']}] {inv['model']:15} ({power_str:6}) | serial: {inv['serial']}")
    
    print(f"\n  {C.BOLD}CONFIGURATIONS (systemConfigurations) :{C.END}")
    for cfg in configs_data:
        power_str = f"{cfg['power_kw']}kW" if cfg['power_kw'] else "?"
        print(f"    [{cfg['index']}] {cfg['model']:15} ({power_str:6}) | count: {cfg['count']}")
    
    # 5. Tester les hypothèses de tri
    results = {
        "site_key": site_key,
        "inverter_count": len(inverters),
        "config_count": len(configs),
        "inverters": inverters_data,
        "configs": configs_data,
    }
    
    # Test 1 : Index direct (ordre identique)
    match_direct = sum(
        1 for i in range(min(len(inverters_data), len(configs_data)))
        if normalize_model(inverters_data[i]["model"]) == normalize_model(configs_data[i]["model"])
    )
    
    # Test 2 : Index inversé
    match_reverse = sum(
        1 for i in range(min(len(inverters_data), len(configs_data)))
        if normalize_model(inverters_data[i]["model"]) == normalize_model(configs_data[len(configs_data)-1-i]["model"])
    )
    
    # Test 3 : Tri par puissance croissante (inverters) vs ordre config
    inv_powers = [inv["power_kw"] for inv in inverters_data if inv["power_kw"] is not None]
    cfg_powers = [cfg["power_kw"] for cfg in configs_data if cfg["power_kw"] is not None]
    
    inv_sorted_asc = inv_powers == sorted(inv_powers)
    inv_sorted_desc = inv_powers == sorted(inv_powers, reverse=True)
    cfg_sorted_asc = cfg_powers == sorted(cfg_powers)
    cfg_sorted_desc = cfg_powers == sorted(cfg_powers, reverse=True)
    
    # Test 4 : Tri inversé des configs
    if len(cfg_powers) >= 2:
        # Si configs sont triées dans l'ordre inverse des inverters
        match_power_inverse = all(
            inv_powers[i] == cfg_powers[len(cfg_powers)-1-i]
            for i in range(min(len(inv_powers), len(cfg_powers)))
        )
    else:
        match_power_inverse = False
    
    results["tests"] = {
        "match_index_direct": match_direct,
        "match_index_reverse": match_reverse,
        "inverters_sorted_asc": inv_sorted_asc,
        "inverters_sorted_desc": inv_sorted_desc,
        "configs_sorted_asc": cfg_sorted_asc,
        "configs_sorted_desc": cfg_sorted_desc,
        "match_power_inverse": match_power_inverse,
        "total_inverters": len(inverters_data),
    }
    
    # 6. Afficher les résultats
    print(f"\n  {C.BOLD}RÉSULTATS DES TESTS :{C.END}")
    
    n = len(inverters_data)
    
    if match_direct == n:
        print(f"    {C.GREEN}✓ INDEX DIRECT      : {match_direct}/{n} match{C.END}")
    else:
        print(f"    {C.RED}✗ INDEX DIRECT      : {match_direct}/{n} match{C.END}")
    
    if match_reverse == n:
        print(f"    {C.GREEN}✓ INDEX INVERSÉ     : {match_reverse}/{n} match{C.END}")
    else:
        print(f"    {C.YELLOW}~ INDEX INVERSÉ     : {match_reverse}/{n} match{C.END}")
    
    print(f"\n  {C.BOLD}ANALYSE DU TRI PAR PUISSANCE :{C.END}")
    print(f"    Inverters triés croissant   : {inv_sorted_asc}")
    print(f"    Inverters triés décroissant : {inv_sorted_desc}")
    print(f"    Configs triées croissant    : {cfg_sorted_asc}")
    print(f"    Configs triées décroissant  : {cfg_sorted_desc}")
    print(f"    Match puissance inversée    : {match_power_inverse}")
    
    # Diagnostic
    if match_direct == n:
        print(f"\n  {C.GREEN}→ DIAGNOSTIC : Ordre identique (pas de tri){C.END}")
    elif match_reverse == n:
        print(f"\n  {C.YELLOW}→ DIAGNOSTIC : Ordre complètement inversé{C.END}")
    elif match_power_inverse:
        print(f"\n  {C.YELLOW}→ DIAGNOSTIC : Tri inversé par PUISSANCE{C.END}")
    elif inv_sorted_asc and cfg_sorted_desc:
        print(f"\n  {C.YELLOW}→ DIAGNOSTIC : Inverters ↗ croissant, Configs ↘ décroissant{C.END}")
    elif inv_sorted_desc and cfg_sorted_asc:
        print(f"\n  {C.YELLOW}→ DIAGNOSTIC : Inverters ↘ décroissant, Configs ↗ croissant{C.END}")
    else:
        print(f"\n  {C.RED}→ DIAGNOSTIC : Aucun pattern de tri détecté{C.END}")
    
    return results


def main():
    print_header("🔬 TEST HYPOTHÈSE : TRI PAR PUISSANCE DES ONDULEURS")
    
    print(f"{C.BOLD}Objectif :{C.END}")
    print("  Analyser si VCOM trie les onduleurs par puissance")
    print("  dans get_inverters() vs systemConfigurations[]")
    print()
    print(f"{C.BOLD}Hypothèses testées :{C.END}")
    print("  1. Tri croissant par puissance (40kW, 125kW)")
    print("  2. Tri décroissant par puissance (125kW, 40kW)")
    print("  3. Tri inversé entre les deux sources")
    print()
    print(f"{C.BOLD}Nombre de sites ciblés : 20 (dont sites à 3+ onduleurs){C.END}")
    
    # Initialisation
    print_section("🔧 INITIALISATION")
    vc = VCOMAPIClient()
    print(f"  {C.GREEN}✓ Client VCOM initialisé{C.END}")
    
    # Récupérer tous les systèmes
    print_section("📋 RÉCUPÉRATION DES SYSTÈMES")
    print(f"  {C.YELLOW}⏳ Appel get_systems()...{C.END}")
    systems = vc.get_systems()
    print(f"  {C.GREEN}✓ {len(systems)} systèmes actifs{C.END}")
    
    # Filtrer les sites avec 2+ onduleurs
    print_section("🔍 FILTRAGE DES SITES AVEC 2+ ONDULEURS")
    
    sites_by_inv_count: Dict[int, List[str]] = {}
    TARGET_SITES = 20
    
    for sys in systems:
        key = sys["key"]
        try:
            inverters = vc.get_inverters(key)
            inv_count = len(inverters)
            
            if inv_count >= 2:
                sites_by_inv_count.setdefault(inv_count, []).append(key)
                
                # Limiter à 20 sites au total
                total = sum(len(v) for v in sites_by_inv_count.values())
                if total >= TARGET_SITES:
                    break
        except Exception:
            continue
    
    # Sélectionner un mix de sites
    selected_sites = []
    
    # Prioriser la diversité (2, 3, 4+ onduleurs)
    for count in sorted(sites_by_inv_count.keys()):
        sites = sites_by_inv_count[count]
        # Prendre au max 10 sites par catégorie
        for site in sites[:10]:
            selected_sites.append((site, count))
            if len(selected_sites) >= TARGET_SITES:
                break
        if len(selected_sites) >= TARGET_SITES:
            break
    
    print(f"\n  {C.BOLD}Sites sélectionnés par nombre d'onduleurs :{C.END}")
    for count, sites in sorted(sites_by_inv_count.items()):
        site_count = len([s for s in selected_sites if s[1] == count])
        print(f"    {count} onduleurs : {site_count} site(s)")
    
    print(f"\n  {C.BOLD}Total : {len(selected_sites)} sites{C.END}")
    
    # Analyser chaque site
    print_header("📊 ANALYSE DÉTAILLÉE")
    
    all_results = []
    
    for site_key, inv_count in selected_sites:
        try:
            result = analyze_site_power_sorting(vc, site_key)
            all_results.append(result)
        except Exception as e:
            print(f"\n  {C.RED}✗ ERREUR site {site_key}: {e}{C.END}")
            import traceback
            traceback.print_exc()
            continue
    
    # Synthèse globale
    print_header("📈 SYNTHÈSE GLOBALE")
    
    total = len(all_results)
    
    # Compter les patterns détectés
    direct_success = 0
    reverse_success = 0
    power_inverse_success = 0
    inv_asc_cfg_desc = 0
    inv_desc_cfg_asc = 0
    no_pattern = 0
    
    for r in all_results:
        tests = r["tests"]
        n = tests["total_inverters"]
        
        if tests["match_index_direct"] == n:
            direct_success += 1
        elif tests["match_index_reverse"] == n:
            reverse_success += 1
        elif tests["match_power_inverse"]:
            power_inverse_success += 1
        elif tests["inverters_sorted_asc"] and tests["configs_sorted_desc"]:
            inv_asc_cfg_desc += 1
        elif tests["inverters_sorted_desc"] and tests["configs_sorted_asc"]:
            inv_desc_cfg_asc += 1
        else:
            no_pattern += 1
    
    print(f"\n  {C.BOLD}DISTRIBUTION DES PATTERNS :{C.END}")
    print(f"    Total sites testés : {total}")
    print()
    
    pct_direct = (direct_success / total * 100) if total > 0 else 0
    pct_reverse = (reverse_success / total * 100) if total > 0 else 0
    pct_power = (power_inverse_success / total * 100) if total > 0 else 0
    pct_asc_desc = (inv_asc_cfg_desc / total * 100) if total > 0 else 0
    pct_desc_asc = (inv_desc_cfg_asc / total * 100) if total > 0 else 0
    pct_none = (no_pattern / total * 100) if total > 0 else 0
    
    def print_bar(count, pct, label, color):
        bar = "█" * int(pct / 2)  # 1 char = 2%
        print(f"    {color}{label:30} : {count:2}/{total} ({pct:5.1f}%) {bar}{C.END}")
    
    print_bar(direct_success, pct_direct, "Index direct (identique)", C.GREEN if direct_success > total/2 else C.YELLOW)
    print_bar(reverse_success, pct_reverse, "Index inversé complet", C.GREEN if reverse_success > total/2 else C.YELLOW)
    print_bar(power_inverse_success, pct_power, "Tri puissance inversé", C.BLUE)
    print_bar(inv_asc_cfg_desc, pct_asc_desc, "Inv↗ / Cfg↘", C.BLUE)
    print_bar(inv_desc_cfg_asc, pct_desc_asc, "Inv↘ / Cfg↗", C.BLUE)
    print_bar(no_pattern, pct_none, "Aucun pattern", C.RED)
    
    # Recommandations
    print_section("💡 RECOMMANDATIONS")
    
    dominant_pattern = max(
        ("direct", direct_success, pct_direct),
        ("reverse", reverse_success, pct_reverse),
        ("none", no_pattern, pct_none),
        key=lambda x: x[1]
    )
    
    pattern_name, pattern_count, pattern_pct = dominant_pattern
    
    if pattern_pct >= 90:
        print(f"  {C.GREEN}{C.BOLD}✅ PATTERN DOMINANT DÉTECTÉ ({pattern_pct:.1f}%){C.END}")
        
        if pattern_name == "direct":
            print(f"  {C.GREEN}L'ordre est IDENTIQUE entre get_inverters() et systemConfigurations[]{C.END}")
            print()
            print(f"  {C.BOLD}ACTION RECOMMANDÉE :{C.END}")
            print(f"    → SUPPRIMER les appels get_inverter_details()")
            print(f"    → Utiliser directement systemConfigurations[idx].inverter")
        
        elif pattern_name == "reverse":
            print(f"  {C.GREEN}L'ordre est INVERSÉ entre get_inverters() et systemConfigurations[]{C.END}")
            print()
            print(f"  {C.BOLD}ACTION RECOMMANDÉE :{C.END}")
            print(f"    → SUPPRIMER les appels get_inverter_details()")
            print(f"    → Utiliser systemConfigurations[n-1-idx].inverter")
    
    elif pattern_pct >= 70:
        print(f"  {C.YELLOW}{C.BOLD}⚠️  PATTERN MAJORITAIRE ({pattern_pct:.1f}%){C.END}")
        print(f"  {C.YELLOW}Mais {100-pattern_pct:.1f}% des sites ne suivent pas ce pattern.{C.END}")
        print()
        print(f"  {C.BOLD}ACTION RECOMMANDÉE : FALLBACK HYBRIDE{C.END}")
        print()
        print(f"  {C.BLUE}Stratégie :{C.END}")
        print(f"    1. Essayer systemConfigurations[idx] (ou [n-1-idx] selon pattern)")
        print(f"    2. Vérifier si le modèle matche")
        print(f"    3. Si échec → appeler get_inverter_details()")
        print()
        print(f"  {C.BLUE}Gain estimé :{C.END}")
        print(f"    ~{pattern_count}/{total} sites = {pattern_pct:.1f}% d'appels évités")
    
    else:
        print(f"  {C.RED}{C.BOLD}❌ AUCUN PATTERN FIABLE ({pattern_pct:.1f}%){C.END}")
        print(f"  {C.RED}Le mapping est trop incohérent pour optimiser.{C.END}")
        print()
        print(f"  {C.BOLD}ACTION RECOMMANDÉE :{C.END}")
        print(f"    → CONSERVER tous les appels get_inverter_details()")
        print(f"    → C'est la seule méthode fiable à 100%")
    
    # Sauvegarder les résultats
    output_dir = Path(__file__).parent / "logs"
    output_dir.mkdir(exist_ok=True)
    output_file = output_dir / "test_power_hypothesis_results.json"
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)
    
    print(f"\n  {C.BLUE}📄 Résultats détaillés : {output_file}{C.END}")
    
    print_header("✅ TEST TERMINÉ")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n\n{C.YELLOW}⚠️  Interruption utilisateur{C.END}")
        sys.exit(1)
    except Exception as e:
        print(f"\n{C.RED}❌ ERREUR FATALE : {e}{C.END}")
        import traceback
        traceback.print_exc()
        sys.exit(1)