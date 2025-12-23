#!/usr/bin/env python3
"""
diagnostic_site_conflicts.py
============================

poetry run python -m vysync.diagnostic_site_conflicts


Script de diagnostic pour identifier les conflits/doublons entre sites VCOM et Yuman
dans Supabase.

Ce script travaille UNIQUEMENT sur Supabase, sans appeler les APIs VCOM ou Yuman.

IMPORTANT: Inclut les sites avec ignore_site=true car certains sites Yuman sont
temporairement ignorés en attendant la création du site VCOM correspondant.

Usage:
    export SUPABASE_URL="https://xxx.supabase.co"
    export SUPABASE_SERVICE_KEY="xxx"
    python diagnostic_site_conflicts.py

Output:
    - Liste des sites VCOM-only (actifs et ignorés)
    - Liste des sites Yuman-only (actifs et ignorés)
    - Paires potentielles (matching par similarité de nom)
    - Rapport JSON exporté
"""

import os
import json
import re
from datetime import datetime
from difflib import SequenceMatcher
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, asdict

# Supabase client
from supabase import create_client, Client


# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

SIMILARITY_THRESHOLD = 0.6  # Score minimum pour considérer une paire potentielle
OUTPUT_FILE = "diagnostic_conflicts_report.json"


# ═══════════════════════════════════════════════════════════════════════════════
# DATA CLASSES
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class SiteInfo:
    """Informations d'un site pour le diagnostic."""
    id: int
    name: str
    vcom_system_key: Optional[str]
    yuman_site_id: Optional[int]
    code: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    address: Optional[str]
    client_map_id: Optional[int]
    ignore_site: bool
    
    @classmethod
    def from_row(cls, row: dict) -> "SiteInfo":
        return cls(
            id=row["id"],
            name=row.get("name") or "",
            vcom_system_key=row.get("vcom_system_key"),
            yuman_site_id=row.get("yuman_site_id"),
            code=row.get("code"),
            latitude=row.get("latitude"),
            longitude=row.get("longitude"),
            address=row.get("address"),
            client_map_id=row.get("client_map_id"),
            ignore_site=bool(row.get("ignore_site")),
        )
    
    @property
    def status_icon(self) -> str:
        """Retourne une icône indiquant le statut du site."""
        return "🚫" if self.ignore_site else "✅"


@dataclass
class PotentialMatch:
    """Paire potentielle VCOM ↔ Yuman."""
    vcom_site: SiteInfo
    yuman_site: SiteInfo
    name_similarity: float
    distance_km: Optional[float]
    confidence: str  # "HIGH", "MEDIUM", "LOW"
    match_reasons: List[str]


# ═══════════════════════════════════════════════════════════════════════════════
# FONCTIONS UTILITAIRES
# ═══════════════════════════════════════════════════════════════════════════════

def normalize_name(name: str) -> str:
    """
    Normalise un nom de site pour la comparaison.
    - Minuscules
    - Supprime les caractères spéciaux
    - Supprime les suffixes courants (région entre parenthèses, etc.)
    """
    if not name:
        return ""
    
    # Minuscules
    n = name.lower().strip()
    
    # Supprimer le contenu entre parenthèses (souvent la région)
    n = re.sub(r'\([^)]*\)', '', n)
    
    # Supprimer les caractères spéciaux sauf espaces
    n = re.sub(r'[^a-z0-9\s]', ' ', n)
    
    # Normaliser les espaces
    n = ' '.join(n.split())
    
    return n


def calculate_similarity(name1: str, name2: str) -> float:
    """Calcule la similarité entre deux noms (0.0 à 1.0)."""
    n1 = normalize_name(name1)
    n2 = normalize_name(name2)
    
    if not n1 or not n2:
        return 0.0
    
    # SequenceMatcher de difflib (built-in, pas de dépendance externe)
    return SequenceMatcher(None, n1, n2).ratio()


def calculate_distance_km(lat1: Optional[float], lon1: Optional[float],
                          lat2: Optional[float], lon2: Optional[float]) -> Optional[float]:
    """
    Calcule la distance en km entre deux points GPS (formule de Haversine).
    Retourne None si les coordonnées sont manquantes.
    """
    import math
    
    if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
        return None
    
    R = 6371  # Rayon de la Terre en km
    
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)
    
    a = (math.sin(delta_lat / 2) ** 2 +
         math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    return R * c


def evaluate_match(vcom: SiteInfo, yuman: SiteInfo) -> Optional[PotentialMatch]:
    """
    Évalue si deux sites sont potentiellement le même.
    
    Logique :
    - Le NOM est le critère PRINCIPAL (similarité >= 60% requise)
    - La DISTANCE GPS est un critère de VALIDATION qui augmente la confiance
    - Le CODE identique est un bonus
    
    Retourne un PotentialMatch si c'est probable, None sinon.
    """
    reasons = []
    
    # 1. Similarité du nom (critère principal)
    name_sim = calculate_similarity(vcom.name, yuman.name)
    
    # Si le nom n'est pas du tout similaire, pas de match
    if name_sim < SIMILARITY_THRESHOLD:
        return None
    
    # 2. Distance GPS (critère de validation)
    dist = calculate_distance_km(vcom.latitude, vcom.longitude,
                                  yuman.latitude, yuman.longitude)
    
    # 3. Code identique ? (bonus)
    code_match = (vcom.code and yuman.code and 
                  vcom.code.strip().lower() == yuman.code.strip().lower())
    
    # Construire les raisons
    if name_sim >= 0.9:
        reasons.append(f"Noms très similaires ({name_sim:.0%})")
    elif name_sim >= 0.7:
        reasons.append(f"Noms similaires ({name_sim:.0%})")
    else:
        reasons.append(f"Noms partiellement similaires ({name_sim:.0%})")
    
    # La distance valide/confirme le match nominal
    location_confirmed = False
    if dist is not None:
        if dist < 0.5:
            reasons.append(f"Localisation confirmée (<500m)")
            location_confirmed = True
        elif dist < 2:
            reasons.append(f"Localisation proche ({dist:.1f}km)")
            location_confirmed = True
        elif dist > 50:
            reasons.append(f"⚠️ Localisations éloignées ({dist:.0f}km)")
    
    if code_match:
        reasons.append(f"Code identique: {vcom.code}")
    
    # Calcul du niveau de confiance
    # HIGH : bon nom + localisation confirmée, OU très bon nom
    # MEDIUM : bon nom sans localisation, OU nom moyen avec localisation
    # LOW : nom faible (même avec localisation)
    
    if name_sim >= 0.9:
        confidence = "HIGH"
    elif name_sim >= 0.7:
        if location_confirmed:
            confidence = "HIGH"
        else:
            confidence = "MEDIUM"
    elif name_sim >= 0.65:
        if location_confirmed:
            confidence = "MEDIUM"
        else:
            confidence = "LOW"
    else:
        confidence = "LOW"
    
    # Bonus code
    if code_match and confidence == "MEDIUM":
        confidence = "HIGH"
    
    return PotentialMatch(
        vcom_site=vcom,
        yuman_site=yuman,
        name_similarity=name_sim,
        distance_km=dist,
        confidence=confidence,
        match_reasons=reasons,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# FONCTIONS PRINCIPALES
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_sites(sb: Client) -> Tuple[List[SiteInfo], List[SiteInfo], List[SiteInfo]]:
    """
    Récupère tous les sites et les catégorise.
    
    IMPORTANT: Inclut les sites avec ignore_site=true pour permettre
    de détecter les paires où un site Yuman ignoré peut matcher un site VCOM.
    
    Retourne:
        (vcom_only, yuman_only, complete)
    """
    rows = sb.table("sites_mapping").select("*").execute().data or []
    
    vcom_only = []
    yuman_only = []
    complete = []
    
    for row in rows:
        site = SiteInfo.from_row(row)
        
        has_vcom = site.vcom_system_key is not None
        has_yuman = site.yuman_site_id is not None
        
        if has_vcom and has_yuman:
            complete.append(site)
        elif has_vcom and not has_yuman:
            vcom_only.append(site)
        elif has_yuman and not has_vcom:
            yuman_only.append(site)
        # Sinon : site sans aucun identifiant (orphelin total) - ignoré
    
    return vcom_only, yuman_only, complete


def find_potential_matches(vcom_only: List[SiteInfo], 
                           yuman_only: List[SiteInfo]) -> List[PotentialMatch]:
    """
    Compare tous les sites VCOM-only avec tous les sites Yuman-only
    pour trouver des paires potentielles.
    
    Règles :
    - Exclut les sites VCOM avec ignore_site=true (sites de test)
    - Un site ne peut apparaître que dans UNE SEULE paire (la meilleure)
    - Priorité : HIGH > MEDIUM > LOW, puis par similarité de nom
    """
    # Exclure les sites VCOM ignorés (sites de test)
    vcom_active = [v for v in vcom_only if not v.ignore_site]
    
    all_matches = []
    
    for vcom in vcom_active:
        for yuman in yuman_only:
            match = evaluate_match(vcom, yuman)
            if match:
                all_matches.append(match)
    
    # Trier par confiance puis par similarité
    confidence_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    all_matches.sort(key=lambda m: (confidence_order[m.confidence], -m.name_similarity))
    
    # Dédoublonner : chaque site ne peut être dans qu'une seule paire
    # On prend les meilleures paires en premier (déjà triées)
    used_vcom_ids = set()
    used_yuman_ids = set()
    final_matches = []
    
    for match in all_matches:
        vcom_id = match.vcom_site.id
        yuman_id = match.yuman_site.id
        
        # Si un des deux sites est déjà utilisé, on skip
        if vcom_id in used_vcom_ids or yuman_id in used_yuman_ids:
            continue
        
        # Sinon on garde cette paire
        final_matches.append(match)
        used_vcom_ids.add(vcom_id)
        used_yuman_ids.add(yuman_id)
    
    return final_matches


def print_report(vcom_only: List[SiteInfo], 
                 yuman_only: List[SiteInfo],
                 complete: List[SiteInfo],
                 matches: List[PotentialMatch]) -> None:
    """Affiche le rapport de diagnostic."""
    
    # Séparer actifs et ignorés
    vcom_active = [s for s in vcom_only if not s.ignore_site]
    vcom_ignored = [s for s in vcom_only if s.ignore_site]
    yuman_active = [s for s in yuman_only if not s.ignore_site]
    yuman_ignored = [s for s in yuman_only if s.ignore_site]
    complete_active = [s for s in complete if not s.ignore_site]
    complete_ignored = [s for s in complete if s.ignore_site]
    
    print("\n" + "=" * 80)
    print("DIAGNOSTIC DES CONFLITS DE SITES VCOM ↔ YUMAN")
    print("=" * 80)
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Résumé
    print("\n" + "-" * 40)
    print("RÉSUMÉ")
    print("-" * 40)
    print(f"Sites complets (VCOM + Yuman) : {len(complete_active):3d} actifs + {len(complete_ignored):3d} ignorés = {len(complete)}")
    print(f"Sites VCOM-only              : {len(vcom_active):3d} actifs + {len(vcom_ignored):3d} ignorés = {len(vcom_only)}")
    print(f"Sites Yuman-only             : {len(yuman_active):3d} actifs + {len(yuman_ignored):3d} ignorés = {len(yuman_only)}")
    print(f"Paires potentielles trouvées : {len(matches)}")
    print(f"  (Sites VCOM ignorés exclus du matching)")
    print(f"  (Chaque site n'apparaît que dans 1 paire max)")
    
    # Sites VCOM-only
    print("\n" + "-" * 40)
    print(f"SITES VCOM-ONLY ({len(vcom_only)} total : {len(vcom_active)} actifs, {len(vcom_ignored)} ignorés)")
    print("-" * 40)
    if vcom_only:
        for s in sorted(vcom_only, key=lambda x: (x.ignore_site, x.name)):
            coords = f"({s.latitude:.4f}, {s.longitude:.4f})" if s.latitude else "(no GPS)"
            status = "🚫" if s.ignore_site else "✅"
            print(f"  {status} [{s.id:4d}] {s.vcom_system_key:8s} | {s.name[:45]:45s} | {coords}")
    else:
        print("  (aucun)")
    
    # Sites Yuman-only
    print("\n" + "-" * 40)
    print(f"SITES YUMAN-ONLY ({len(yuman_only)} total : {len(yuman_active)} actifs, {len(yuman_ignored)} ignorés)")
    print("-" * 40)
    if yuman_only:
        for s in sorted(yuman_only, key=lambda x: (x.ignore_site, x.name)):
            coords = f"({s.latitude:.4f}, {s.longitude:.4f})" if s.latitude else "(no GPS)"
            status = "🚫" if s.ignore_site else "✅"
            print(f"  {status} [{s.id:4d}] yuman_id={s.yuman_site_id:7d} | {s.name[:40]:40s} | {coords}")
    else:
        print("  (aucun)")
    
    # Paires potentielles
    print("\n" + "-" * 40)
    print(f"PAIRES POTENTIELLES ({len(matches)}) - Dédoublonnées")
    print("-" * 40)
    
    if matches:
        # Grouper par confiance
        by_confidence = {"HIGH": [], "MEDIUM": [], "LOW": []}
        for m in matches:
            by_confidence[m.confidence].append(m)
        
        for level in ["HIGH", "MEDIUM", "LOW"]:
            level_matches = by_confidence[level]
            if not level_matches:
                continue
                
            emoji = {"HIGH": "🟢", "MEDIUM": "🟡", "LOW": "🟠"}[level]
            print(f"\n{emoji} Confiance {level} ({len(level_matches)}):")
            
            for m in level_matches:
                v_status = "🚫" if m.vcom_site.ignore_site else "✅"
                y_status = "🚫" if m.yuman_site.ignore_site else "✅"
                
                print(f"\n  {v_status} VCOM  [{m.vcom_site.id:4d}] {m.vcom_site.vcom_system_key:8s} : {m.vcom_site.name}")
                print(f"  {y_status} YUMAN [{m.yuman_site.id:4d}] yuman_id={m.yuman_site.yuman_site_id:7d} : {m.yuman_site.name}")
                print(f"        Similarité nom: {m.name_similarity:.0%}", end="")
                if m.distance_km is not None:
                    print(f" | Distance: {m.distance_km:.2f} km", end="")
                print()
                print(f"        Raisons: {', '.join(m.match_reasons)}")
                
                # Alerte si un des deux est ignoré
                if m.yuman_site.ignore_site:
                    print(f"        → Site Yuman ignoré, prêt pour fusion")
    else:
        print("  (aucune paire potentielle trouvée)")
    
    # Analyse des non-matchés (exclure les VCOM ignorés qui sont volontairement exclus)
    matched_vcom_ids = {m.vcom_site.id for m in matches}
    matched_yuman_ids = {m.yuman_site.id for m in matches}
    
    # Seulement les VCOM actifs non matchés
    unmatched_vcom = [s for s in vcom_active if s.id not in matched_vcom_ids]
    unmatched_yuman = [s for s in yuman_only if s.id not in matched_yuman_ids]
    
    if unmatched_vcom or unmatched_yuman:
        print("\n" + "-" * 40)
        print("SITES SANS CORRESPONDANCE")
        print("-" * 40)
        
        if unmatched_vcom:
            print(f"\n  Sites VCOM actifs sans match ({len(unmatched_vcom)}):")
            for s in sorted(unmatched_vcom, key=lambda x: x.name):
                print(f"    ✅ [{s.id:4d}] {s.vcom_system_key:8s} : {s.name}")
        
        if unmatched_yuman:
            unmatched_yuman_active = [s for s in unmatched_yuman if not s.ignore_site]
            unmatched_yuman_ignored = [s for s in unmatched_yuman if s.ignore_site]
            print(f"\n  Sites Yuman sans match ({len(unmatched_yuman)} : {len(unmatched_yuman_active)} actifs, {len(unmatched_yuman_ignored)} ignorés):")
            for s in sorted(unmatched_yuman, key=lambda x: (x.ignore_site, x.name)):
                status = "🚫" if s.ignore_site else "✅"
                print(f"    {status} [{s.id:4d}] yuman_id={s.yuman_site_id:7d} : {s.name}")
    
    # Sites VCOM ignorés (pour info)
    if vcom_ignored:
        print(f"\n  ℹ️  Sites VCOM ignorés (exclus du matching) : {len(vcom_ignored)}")
        for s in vcom_ignored:
            print(f"    🚫 [{s.id:4d}] {s.vcom_system_key:8s} : {s.name}")


def export_report(vcom_only: List[SiteInfo], 
                  yuman_only: List[SiteInfo],
                  complete: List[SiteInfo],
                  matches: List[PotentialMatch],
                  filename: str) -> None:
    """Exporte le rapport en JSON."""
    
    # Séparer actifs et ignorés
    vcom_active = [s for s in vcom_only if not s.ignore_site]
    vcom_ignored = [s for s in vcom_only if s.ignore_site]
    yuman_active = [s for s in yuman_only if not s.ignore_site]
    yuman_ignored = [s for s in yuman_only if s.ignore_site]
    complete_active = [s for s in complete if not s.ignore_site]
    complete_ignored = [s for s in complete if s.ignore_site]
    
    report = {
        "generated_at": datetime.now().isoformat(),
        "summary": {
            "complete_sites": {
                "total": len(complete),
                "active": len(complete_active),
                "ignored": len(complete_ignored),
            },
            "vcom_only": {
                "total": len(vcom_only),
                "active": len(vcom_active),
                "ignored": len(vcom_ignored),
            },
            "yuman_only": {
                "total": len(yuman_only),
                "active": len(yuman_active),
                "ignored": len(yuman_ignored),
            },
            "potential_matches": len(matches),
        },
        "vcom_only_sites": [asdict(s) for s in vcom_only],
        "yuman_only_sites": [asdict(s) for s in yuman_only],
        "potential_matches": [
            {
                "vcom_site": asdict(m.vcom_site),
                "yuman_site": asdict(m.yuman_site),
                "name_similarity": m.name_similarity,
                "distance_km": m.distance_km,
                "confidence": m.confidence,
                "match_reasons": m.match_reasons,
            }
            for m in matches
        ],
    }
    
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    print(f"\n📄 Rapport JSON exporté: {filename}")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    # Vérifier les variables d'environnement
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_KEY")
    
    if not url or not key:
        print("❌ ERREUR: Variables d'environnement manquantes")
        print("   Définir SUPABASE_URL et SUPABASE_SERVICE_KEY")
        return 1
    
    print("🔌 Connexion à Supabase...")
    sb = create_client(url, key)
    
    print("📥 Récupération des sites...")
    vcom_only, yuman_only, complete = fetch_sites(sb)
    
    print("🔍 Recherche des paires potentielles...")
    matches = find_potential_matches(vcom_only, yuman_only)
    
    # Afficher le rapport
    print_report(vcom_only, yuman_only, complete, matches)
    
    # Exporter en JSON
    export_report(vcom_only, yuman_only, complete, matches, OUTPUT_FILE)
    
    return 0


if __name__ == "__main__":
    exit(main())