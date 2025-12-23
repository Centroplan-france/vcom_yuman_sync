#!/usr/bin/env python3
"""
Script de test pour visualiser les objets créés par les 3 adapters.

Usage:
    poetry run python test_adapters_comparison.py

Produit 2 fichiers :
    - test_adapters_E3K2L.log : log structuré lisible
    - test_adapters_E3K2L.json : dump JSON complet
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

# Import des modules vysync
from vysync.vcom_client import VCOMAPIClient
from vysync.yuman_client import YumanClient
from vysync.adapters.vcom_adapter import fetch_snapshot
from vysync.adapters.supabase_adapter import SupabaseAdapter
from vysync.adapters.yuman_adapter import YumanAdapter
from vysync.models import Site, Equipment

# Configuration
SITE_KEY = "E3K2L"
OUTPUT_DIR = Path("logs")
OUTPUT_DIR.mkdir(exist_ok=True)

TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = OUTPUT_DIR / f"test_adapters_{SITE_KEY}_{TIMESTAMP}.log"
JSON_FILE = OUTPUT_DIR / f"test_adapters_{SITE_KEY}_{TIMESTAMP}.json"


class TestLogger:
    """Logger personnalisé pour produire log structuré + JSON."""
    
    def __init__(self, log_file: Path):
        self.log_file = log_file
        self.lines = []
        self.json_data = {}
    
    def section(self, title: str):
        """Ajoute une section dans le log."""
        sep = "=" * 80
        self.lines.append(f"\n{sep}")
        self.lines.append(f"{title.center(80)}")
        self.lines.append(f"{sep}\n")
    
    def subsection(self, title: str):
        """Ajoute une sous-section dans le log."""
        self.lines.append(f"\n{'─' * 80}")
        self.lines.append(f"  {title}")
        self.lines.append(f"{'─' * 80}")
    
    def log(self, text: str):
        """Ajoute une ligne de log."""
        self.lines.append(text)
    
    def log_object(self, obj: Site | Equipment, sb_adapter: SupabaseAdapter):
        """Affiche un objet Site ou Equipment de manière structurée."""
        if isinstance(obj, Site):
            self._log_site(obj, sb_adapter)
        elif isinstance(obj, Equipment):
            self._log_equipment(obj, sb_adapter)
    
    def _log_site(self, site: Site, sb_adapter: SupabaseAdapter):
        """Affiche un objet Site."""
        self.log(f"\n📍 SITE")
        self.log(f"  ID (Supabase)       : {site.id}")
        self.log(f"  VCOM System Key     : {site.get_vcom_system_key(sb_adapter)}")
        self.log(f"  Yuman Site ID       : {site.get_yuman_site_id(sb_adapter)}")
        self.log(f"  Name                : {site.name}")
        self.log(f"  Address             : {site.address}")
        self.log(f"  Latitude            : {site.latitude}")
        self.log(f"  Longitude           : {site.longitude}")
        self.log(f"  Nominal Power (kWc) : {site.nominal_power}")
        self.log(f"  Commission Date     : {site.commission_date}")
        self.log(f"  Client Map ID       : {site.client_map_id}")
        self.log(f"  ALDI ID             : {site.aldi_id}")
        self.log(f"  ALDI Store ID       : {site.aldi_store_id}")
        self.log(f"  Project Number CP   : {site.project_number_cp}")
        self.log(f"  Ignore Site         : {site.ignore_site}")
    
    def _log_equipment(self, eq: Equipment, sb_adapter: SupabaseAdapter):
        """Affiche un objet Equipment."""
        # Symbole selon la catégorie
        symbols = {
            11102: "⚡",  # INVERTER
            11103: "☀️",  # MODULE
            12404: "🔗",  # STRING
            11441: "🏭",  # CENTRALE
            11382: "📱",  # SIM
        }
        symbol = symbols.get(eq.category_id, "❓")
        
        self.log(f"\n{symbol} EQUIPMENT - {eq.eq_type.upper()}")
        self.log(f"  Category ID         : {eq.category_id}")
        self.log(f"  VCOM Device ID      : {eq.vcom_device_id}")
        self.log(f"  Serial Number       : {eq.serial_number}")
        self.log(f"  Name                : {eq.name}")
        self.log(f"  Brand               : {eq.brand}")
        self.log(f"  Model               : {eq.model}")
        self.log(f"  Count               : {eq.count}")
        self.log(f"  Parent ID           : {eq.parent_id}")
        self.log(f"  Site ID (Supabase)  : {eq.site_id}")
        self.log(f"  VCOM System Key     : {eq.get_vcom_system_key(sb_adapter)}")
        self.log(f"  Yuman Site ID       : {eq.get_yuman_site_id(sb_adapter)}")
        self.log(f"  Yuman Material ID   : {eq.yuman_material_id}")
    
    def save_log(self):
        """Sauvegarde le fichier log."""
        with open(self.log_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(self.lines))
        print(f"✅ Log sauvegardé : {self.log_file}")
    
    def save_json(self, json_file: Path):
        """Sauvegarde le fichier JSON."""
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(self.json_data, f, indent=2, ensure_ascii=False, default=str)
        print(f"✅ JSON sauvegardé : {json_file}")


def fetch_vcom_data(vc: VCOMAPIClient, sb: SupabaseAdapter, site_key: str):
    """Fetch depuis VCOM adapter."""
    print(f"\n🔄 Fetching VCOM data pour site {site_key}...")
    sites, equips = fetch_snapshot(vc, vcom_system_key=site_key, sb_adapter=sb)
    
    # Convertir en dict sérialisable
    sites_dict = {k: v.to_dict() for k, v in sites.items()}
    equips_dict = {k: v.to_dict() for k, v in equips.items()}
    
    print(f"   ✅ {len(sites)} site(s), {len(equips)} équipement(s)")
    return sites, equips, {"sites": sites_dict, "equipments": equips_dict}


def fetch_supabase_data(sb: SupabaseAdapter, site_key: str):
    """Fetch depuis Supabase adapter."""
    print(f"\n🔄 Fetching Supabase data pour site {site_key}...")
    sites = sb.fetch_sites_v(site_key=site_key)
    equips = sb.fetch_equipments_v(site_key=site_key)
    
    # Convertir en dict sérialisable
    sites_dict = {k: v.to_dict() for k, v in sites.items()}
    equips_dict = {k: v.to_dict() for k, v in equips.items()}
    
    print(f"   ✅ {len(sites)} site(s), {len(equips)} équipement(s)")
    return sites, equips, {"sites": sites_dict, "equipments": equips_dict}


def fetch_yuman_data(y: YumanAdapter, sb: SupabaseAdapter, site_key: str):
    """Fetch depuis Yuman adapter."""
    print(f"\n🔄 Fetching Yuman data pour site {site_key}...")
    
    # 1. Récupérer yuman_site_id depuis le cache Supabase
    # On fetch tous les sites Yuman puis on filtre
    all_sites = y.fetch_sites()
    all_equips = y.fetch_equips()
    
    # Filtrer par vcom_system_key
    target_site = None
    target_site_key = None
    
    for yid, site in all_sites.items():
        if site.get_vcom_system_key(sb) == site_key:
            target_site = site
            target_site_key = yid
            break
    
    if not target_site:
        print(f"   ⚠️  Site {site_key} non trouvé dans Yuman")
        return {}, {}, {"sites": {}, "equipments": {}}
    
    # Filtrer les équipements du site
    site_equips = {
        serial: eq for serial, eq in all_equips.items()
        if eq.get_vcom_system_key(sb) == site_key
    }
    
    # Convertir en dict sérialisable
    sites_dict = {target_site_key: target_site.to_dict()}
    equips_dict = {k: v.to_dict() for k, v in site_equips.items()}
    
    print(f"   ✅ 1 site, {len(site_equips)} équipement(s)")
    return {target_site_key: target_site}, site_equips, {"sites": sites_dict, "equipments": equips_dict}


def log_comparison(logger: TestLogger, vcom_data, sb_data, yuman_data, sb_adapter: SupabaseAdapter):
    """Génère le log de comparaison structuré."""
    
    logger.section(f"TEST ADAPTERS - SITE {SITE_KEY}")
    logger.log(f"Date : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.log(f"Site cible : {SITE_KEY}")
    
    # ===============================
    # VCOM ADAPTER
    # ===============================
    logger.section("1. VCOM ADAPTER")
    
    vcom_sites, vcom_equips = vcom_data
    
    if vcom_sites:
        site = list(vcom_sites.values())[0]
        logger.log_object(site, sb_adapter)
    else:
        logger.log("❌ Aucun site trouvé")
    
    # Grouper par catégorie
    by_category = {}
    for eq in vcom_equips.values():
        by_category.setdefault(eq.category_id, []).append(eq)
    
    for cat_id in sorted(by_category.keys()):
        cat_name = {
            11102: "INVERTER",
            11103: "MODULE",
            12404: "STRING_PV",
            11441: "CENTRALE",
            11382: "SIM",
        }.get(cat_id, f"UNKNOWN_{cat_id}")
        
        logger.subsection(f"{cat_name} ({len(by_category[cat_id])} équipement(s))")
        for eq in by_category[cat_id]:
            logger.log_object(eq, sb_adapter)
    
    # ===============================
    # SUPABASE ADAPTER
    # ===============================
    logger.section("2. SUPABASE ADAPTER")
    
    sb_sites, sb_equips = sb_data
    
    if sb_sites:
        site = list(sb_sites.values())[0]
        logger.log_object(site, sb_adapter)
    else:
        logger.log("❌ Aucun site trouvé")
    
    # Grouper par catégorie
    by_category = {}
    for eq in sb_equips.values():
        by_category.setdefault(eq.category_id, []).append(eq)
    
    for cat_id in sorted(by_category.keys()):
        cat_name = {
            11102: "INVERTER",
            11103: "MODULE",
            12404: "STRING_PV",
            11441: "CENTRALE",
            11382: "SIM",
        }.get(cat_id, f"UNKNOWN_{cat_id}")
        
        logger.subsection(f"{cat_name} ({len(by_category[cat_id])} équipement(s))")
        for eq in by_category[cat_id]:
            logger.log_object(eq, sb_adapter)
    
    # ===============================
    # YUMAN ADAPTER
    # ===============================
    logger.section("3. YUMAN ADAPTER")
    
    yuman_sites, yuman_equips = yuman_data
    
    if yuman_sites:
        site = list(yuman_sites.values())[0]
        logger.log_object(site, sb_adapter)
    else:
        logger.log("❌ Aucun site trouvé")
    
    # Grouper par catégorie
    by_category = {}
    for eq in yuman_equips.values():
        by_category.setdefault(eq.category_id, []).append(eq)
    
    for cat_id in sorted(by_category.keys()):
        cat_name = {
            11102: "INVERTER",
            11103: "MODULE",
            12404: "STRING_PV",
            11441: "CENTRALE",
            11382: "SIM",
        }.get(cat_id, f"UNKNOWN_{cat_id}")
        
        logger.subsection(f"{cat_name} ({len(by_category[cat_id])} équipement(s))")
        for eq in by_category[cat_id]:
            logger.log_object(eq, sb_adapter)
    
    # ===============================
    # RÉSUMÉ
    # ===============================
    logger.section("RÉSUMÉ COMPARATIF")
    
    logger.log(f"\n📊 STATISTIQUES")
    logger.log(f"  VCOM      : {len(vcom_sites)} site(s), {len(vcom_equips)} équipement(s)")
    logger.log(f"  Supabase  : {len(sb_sites)} site(s), {len(sb_equips)} équipement(s)")
    logger.log(f"  Yuman     : {len(yuman_sites)} site(s), {len(yuman_equips)} équipement(s)")
    
    # Compter par catégorie
    logger.log(f"\n📦 RÉPARTITION PAR CATÉGORIE")
    
    for source_name, equips in [("VCOM", vcom_equips), ("Supabase", sb_equips), ("Yuman", yuman_equips)]:
        by_cat = {}
        for eq in equips.values():
            by_cat.setdefault(eq.category_id, 0)
            by_cat[eq.category_id] += 1
        
        logger.log(f"\n  {source_name}:")
        for cat_id in sorted(by_cat.keys()):
            cat_name = {
                11102: "INVERTER",
                11103: "MODULE",
                12404: "STRING_PV",
                11441: "CENTRALE",
                11382: "SIM",
            }.get(cat_id, f"UNKNOWN_{cat_id}")
            logger.log(f"    - {cat_name:15} : {by_cat[cat_id]}")


def main():
    """Point d'entrée principal."""
    print("="*80)
    print(f"TEST ADAPTERS COMPARISON - SITE {SITE_KEY}".center(80))
    print("="*80)
    
    # Initialisation des clients
    print("\n🔧 Initialisation des clients...")
    vc = VCOMAPIClient()
    sb = SupabaseAdapter()
    y = YumanAdapter(sb)
    print("   ✅ Clients initialisés")
    
    # Logger
    logger = TestLogger(LOG_FILE)
    
    # Fetch depuis les 3 sources
    vcom_sites, vcom_equips, vcom_json = fetch_vcom_data(vc, sb, SITE_KEY)
    sb_sites, sb_equips, sb_json = fetch_supabase_data(sb, SITE_KEY)
    yuman_sites, yuman_equips, yuman_json = fetch_yuman_data(y, sb, SITE_KEY)
    
    # Générer le log structuré
    print("\n📝 Génération du log structuré...")
    log_comparison(
        logger,
        (vcom_sites, vcom_equips),
        (sb_sites, sb_equips),
        (yuman_sites, yuman_equips),
        sb
    )
    
    # Préparer le JSON complet
    logger.json_data = {
        "site_key": SITE_KEY,
        "timestamp": TIMESTAMP,
        "vcom": vcom_json,
        "supabase": sb_json,
        "yuman": yuman_json,
    }
    
    # Sauvegarder les fichiers
    print("\n💾 Sauvegarde des résultats...")
    logger.save_log()
    logger.save_json(JSON_FILE)
    
    print("\n" + "="*80)
    print("✅ TEST TERMINÉ".center(80))
    print("="*80)
    print(f"\n📄 Fichiers générés :")
    print(f"   - {LOG_FILE}")
    print(f"   - {JSON_FILE}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Interruption utilisateur")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ ERREUR : {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)