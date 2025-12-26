#!/usr/bin/env python3
"""
auto_merge_sites.py
===================

Script d'automatisation des fusions de sites VCOM ↔ Yuman.

Logique :
1. Détecte les paires potentielles (HIGH + MEDIUM confidence)
2. Fusionne automatiquement toutes les paires trouvées
3. Si des sites VCOM actifs n'ont pas de paire → envoie un email d'alerte
4. Génère un rapport JSON

Usage:
    # Mode dry-run (par défaut) - affiche ce qui va se passer
    poetry run python -m vysync.auto_merge_sites

    # Mode exécution réelle
    poetry run python -m vysync.auto_merge_sites --execute

    # Avec envoi d'email même en dry-run (pour tester)
    poetry run python -m vysync.auto_merge_sites --test-email
"""

import argparse
import json
import os
import smtplib
import sys
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from difflib import SequenceMatcher
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Dict, List, Optional, Tuple
import logging

from supabase import create_client, Client


# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

SIMILARITY_THRESHOLD = 0.60  # Score minimum pour considérer une paire
CONFIDENCE_TO_MERGE = ["HIGH", "MEDIUM"]  # Niveaux de confiance à fusionner auto

SITES_TABLE = "sites_mapping"
EQUIP_TABLE = "equipments_mapping"
SYNC_LOGS_TABLE = "sync_logs"

ALERT_EMAIL = os.getenv("ALERT_EMAIL", "t.roquefeuil@centroplan.fr")
OUTPUT_FILE = "auto_merge_report.json"

# Logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# DATA CLASSES
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class SiteInfo:
    """Informations d'un site."""
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


@dataclass
class PotentialMatch:
    """Paire potentielle VCOM ↔ Yuman."""
    vcom_site: SiteInfo
    yuman_site: SiteInfo
    name_similarity: float
    distance_km: Optional[float]
    confidence: str
    match_reasons: List[str]


@dataclass
class MergeResult:
    """Résultat d'une fusion."""
    vcom_id: int
    yuman_id: int
    yuman_site_id: int
    success: bool
    error: Optional[str] = None


# ═══════════════════════════════════════════════════════════════════════════════
# FONCTIONS UTILITAIRES
# ═══════════════════════════════════════════════════════════════════════════════

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_name(name: str) -> str:
    """Normalise un nom de site pour la comparaison."""
    import re
    if not name:
        return ""
    n = name.lower().strip()
    n = re.sub(r'\([^)]*\)', '', n)  # Supprimer parenthèses
    n = re.sub(r'[^a-z0-9\s]', ' ', n)  # Caractères spéciaux
    n = ' '.join(n.split())
    return n


def calculate_similarity(name1: str, name2: str) -> float:
    """Calcule la similarité entre deux noms."""
    n1 = normalize_name(name1)
    n2 = normalize_name(name2)
    if not n1 or not n2:
        return 0.0
    return SequenceMatcher(None, n1, n2).ratio()


def calculate_distance_km(lat1: Optional[float], lon1: Optional[float],
                          lat2: Optional[float], lon2: Optional[float]) -> Optional[float]:
    """Calcule la distance en km entre deux points GPS."""
    import math
    if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
        return None
    R = 6371
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)
    a = (math.sin(delta_lat / 2) ** 2 +
         math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


# ═══════════════════════════════════════════════════════════════════════════════
# DÉTECTION DES PAIRES
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_sites(sb: Client) -> Tuple[List[SiteInfo], List[SiteInfo], List[SiteInfo]]:
    """Récupère tous les sites et les catégorise."""
    rows = sb.table(SITES_TABLE).select("*").execute().data or []
    
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
    
    return vcom_only, yuman_only, complete


def evaluate_match(vcom: SiteInfo, yuman: SiteInfo) -> Optional[PotentialMatch]:
    """Évalue si deux sites sont potentiellement le même."""
    reasons = []
    name_sim = calculate_similarity(vcom.name, yuman.name)
    
    if name_sim < SIMILARITY_THRESHOLD:
        return None
    
    dist = calculate_distance_km(vcom.latitude, vcom.longitude,
                                  yuman.latitude, yuman.longitude)
    
    code_match = (vcom.code and yuman.code and 
                  vcom.code.strip().lower() == yuman.code.strip().lower())
    
    # Raisons
    if name_sim >= 0.9:
        reasons.append(f"Noms très similaires ({name_sim:.0%})")
    elif name_sim >= 0.7:
        reasons.append(f"Noms similaires ({name_sim:.0%})")
    else:
        reasons.append(f"Noms partiellement similaires ({name_sim:.0%})")
    
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
    
    # Confiance
    if name_sim >= 0.9:
        confidence = "HIGH"
    elif name_sim >= 0.7:
        confidence = "HIGH" if location_confirmed else "MEDIUM"
    elif name_sim >= 0.65:
        confidence = "MEDIUM" if location_confirmed else "LOW"
    else:
        confidence = "LOW"
    
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


def find_potential_matches(vcom_only: List[SiteInfo], 
                           yuman_only: List[SiteInfo]) -> List[PotentialMatch]:
    """Trouve les paires potentielles (dédoublonnées)."""
    # Exclure les sites VCOM ignorés
    vcom_active = [v for v in vcom_only if not v.ignore_site]
    
    all_matches = []
    for vcom in vcom_active:
        for yuman in yuman_only:
            match = evaluate_match(vcom, yuman)
            if match:
                all_matches.append(match)
    
    # Trier par confiance puis similarité
    confidence_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    all_matches.sort(key=lambda m: (confidence_order[m.confidence], -m.name_similarity))
    
    # Dédoublonner
    used_vcom_ids = set()
    used_yuman_ids = set()
    final_matches = []
    
    for match in all_matches:
        if match.vcom_site.id in used_vcom_ids or match.yuman_site.id in used_yuman_ids:
            continue
        final_matches.append(match)
        used_vcom_ids.add(match.vcom_site.id)
        used_yuman_ids.add(match.yuman_site.id)
    
    return final_matches


# ═══════════════════════════════════════════════════════════════════════════════
# FUSION
# ═══════════════════════════════════════════════════════════════════════════════

def merge_single_pair(sb: Client, vcom_id: int, yuman_id: int, yuman_site_id: int) -> MergeResult:
    """Fusionne une paire via RPC."""
    try:
        sb.rpc("merge_sites", {
            "vcom_id": vcom_id,
            "yuman_id": yuman_id,
        }).execute()
        
        # Log
        sb.table(SYNC_LOGS_TABLE).insert({
            "source": "user",
            "action": "merge_site",
            "payload": json.dumps({
                "from_site_id": yuman_id,
                "into_site_id": vcom_id,
                "yuman_site_id": yuman_site_id,
                "script": "auto_merge_sites",
            }),
            "created_at": _now_iso(),
        }).execute()
        
        return MergeResult(vcom_id=vcom_id, yuman_id=yuman_id, 
                          yuman_site_id=yuman_site_id, success=True)
    except Exception as e:
        return MergeResult(vcom_id=vcom_id, yuman_id=yuman_id,
                          yuman_site_id=yuman_site_id, success=False, error=str(e))


# ═══════════════════════════════════════════════════════════════════════════════
# EMAIL
# ═══════════════════════════════════════════════════════════════════════════════

def send_alert_email(unmatched_vcom: List[SiteInfo], 
                     merge_results: List[MergeResult],
                     failed_merges: List[MergeResult]) -> bool:
    """
    Envoie un email d'alerte si des sites VCOM n'ont pas de paire
    ou si des fusions ont échoué.
    """
    smtp_host = os.getenv("SMTP_HOST")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER")
    smtp_password = os.getenv("SMTP_PASSWORD")
    
    if not all([smtp_host, smtp_user, smtp_password]):
        logger.warning("[EMAIL] Variables SMTP manquantes, email non envoyé")
        return False
    
    # Construire le contenu
    subject = "[VYSYNC] Alerte fusion sites VCOM/Yuman"
    
    body_parts = []
    body_parts.append("Rapport de fusion automatique des sites VCOM ↔ Yuman")
    body_parts.append(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    body_parts.append("")
    
    # Résumé des fusions
    successful = [r for r in merge_results if r.success]
    body_parts.append(f"=== FUSIONS RÉUSSIES: {len(successful)} ===")
    if successful:
        for r in successful:
            body_parts.append(f"  • VCOM id={r.vcom_id} ← Yuman id={r.yuman_id} (yuman_site_id={r.yuman_site_id})")
    body_parts.append("")
    
    # Fusions échouées
    if failed_merges:
        body_parts.append(f"=== FUSIONS ÉCHOUÉES: {len(failed_merges)} ===")
        for r in failed_merges:
            body_parts.append(f"  ❌ VCOM id={r.vcom_id} ← Yuman id={r.yuman_id}: {r.error}")
        body_parts.append("")
    
    # Sites VCOM sans paire
    if unmatched_vcom:
        body_parts.append(f"=== SITES VCOM SANS PAIRE YUMAN: {len(unmatched_vcom)} ===")
        body_parts.append("Ces sites VCOM actifs n'ont pas trouvé de correspondance Yuman.")
        body_parts.append("Action requise: créer le site dans Yuman ou trouver manuellement la paire.")
        body_parts.append("")
        for s in unmatched_vcom:
            body_parts.append(f"  • [{s.id}] {s.vcom_system_key}: {s.name}")
            body_parts.append(f"    Adresse: {s.address or '(non renseignée)'}")
        body_parts.append("")
    
    body_parts.append("---")
    body_parts.append("Ce message a été généré automatiquement par vysync.auto_merge_sites")
    
    body = "\n".join(body_parts)
    
    # Envoyer
    try:
        msg = MIMEMultipart()
        msg["From"] = "onboarding@resend.dev"
        msg["To"] = ALERT_EMAIL
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain", "utf-8"))
        
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.send_message(msg)
        
        logger.info(f"[EMAIL] Alerte envoyée à {ALERT_EMAIL}")
        return True
    except Exception as e:
        logger.error(f"[EMAIL] Erreur envoi: {e}")
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# RAPPORT
# ═══════════════════════════════════════════════════════════════════════════════

def generate_report(vcom_only: List[SiteInfo],
                    yuman_only: List[SiteInfo],
                    matches: List[PotentialMatch],
                    merge_results: List[MergeResult],
                    unmatched_vcom: List[SiteInfo],
                    dry_run: bool) -> Dict[str, Any]:
    """Génère le rapport JSON."""
    
    successful = [r for r in merge_results if r.success]
    failed = [r for r in merge_results if not r.success]
    
    return {
        "generated_at": _now_iso(),
        "dry_run": dry_run,
        "summary": {
            "vcom_only_active": len([v for v in vcom_only if not v.ignore_site]),
            "yuman_only_total": len(yuman_only),
            "pairs_found": len(matches),
            "pairs_to_merge": len([m for m in matches if m.confidence in CONFIDENCE_TO_MERGE]),
            "merges_successful": len(successful),
            "merges_failed": len(failed),
            "vcom_unmatched": len(unmatched_vcom),
        },
        "merges": [
            {
                "vcom_id": r.vcom_id,
                "yuman_id": r.yuman_id,
                "yuman_site_id": r.yuman_site_id,
                "success": r.success,
                "error": r.error,
            }
            for r in merge_results
        ],
        "unmatched_vcom_sites": [asdict(s) for s in unmatched_vcom],
        "pairs_found": [
            {
                "vcom_id": m.vcom_site.id,
                "vcom_name": m.vcom_site.name,
                "yuman_id": m.yuman_site.id,
                "yuman_name": m.yuman_site.name,
                "confidence": m.confidence,
                "name_similarity": m.name_similarity,
            }
            for m in matches
        ],
    }


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def run_auto_merge(
    execute: bool = False,
    test_email: bool = False,
    output: str = OUTPUT_FILE
) -> int:
    """
    Logique métier de fusion automatique des sites VCOM ↔ Yuman.

    Args:
        execute: Si True, exécute réellement les fusions (sinon dry-run)
        test_email: Si True, envoie l'email même en dry-run
        output: Chemin du fichier de rapport JSON

    Returns:
        0 en cas de succès, 1 en cas d'erreur
    """
    # Vérifier les variables d'environnement
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_KEY")

    if not url or not key:
        logger.error("Variables d'environnement SUPABASE_URL et SUPABASE_SERVICE_KEY manquantes")
        return 1

    logger.info("=" * 70)
    logger.info("AUTO MERGE SITES - Fusion automatique VCOM ↔ Yuman")
    logger.info("=" * 70)

    if not execute:
        logger.info("MODE DRY-RUN - Aucune modification ne sera effectuée")

    # Connexion
    logger.info("Connexion à Supabase...")
    sb = create_client(url, key)

    # 1. Récupérer les sites
    logger.info("Récupération des sites...")
    vcom_only, yuman_only, complete = fetch_sites(sb)

    vcom_active = [v for v in vcom_only if not v.ignore_site]
    logger.info(f"  Sites complets: {len(complete)}")
    logger.info(f"  Sites VCOM-only actifs: {len(vcom_active)}")
    logger.info(f"  Sites Yuman-only: {len(yuman_only)}")

    # 2. Trouver les paires
    logger.info("Recherche des paires potentielles...")
    matches = find_potential_matches(vcom_only, yuman_only)

    # Filtrer par confiance
    matches_to_merge = [m for m in matches if m.confidence in CONFIDENCE_TO_MERGE]
    matches_low = [m for m in matches if m.confidence not in CONFIDENCE_TO_MERGE]

    logger.info(f"  Paires HIGH/MEDIUM (à fusionner): {len(matches_to_merge)}")
    logger.info(f"  Paires LOW (ignorées): {len(matches_low)}")

    # Afficher les paires à fusionner
    if matches_to_merge:
        logger.info("")
        logger.info("Paires à fusionner:")
        for m in matches_to_merge:
            y_status = "ignoré" if m.yuman_site.ignore_site else "actif"
            logger.info(f"  [{m.confidence:6}] VCOM {m.vcom_site.id} ({m.vcom_site.vcom_system_key}) "
                       f"← Yuman {m.yuman_site.id} (yuman_id={m.yuman_site.yuman_site_id}, {y_status})")
            logger.info(f"           {m.vcom_site.name[:50]}")
            logger.info(f"           {m.yuman_site.name[:50]}")

    # 3. Identifier les sites VCOM sans paire
    matched_vcom_ids = {m.vcom_site.id for m in matches}
    unmatched_vcom = [v for v in vcom_active if v.id not in matched_vcom_ids]

    if unmatched_vcom:
        logger.warning("")
        logger.warning(f"⚠️  {len(unmatched_vcom)} site(s) VCOM actifs SANS PAIRE:")
        for s in unmatched_vcom:
            logger.warning(f"  [{s.id}] {s.vcom_system_key}: {s.name}")

    # 4. Exécuter les fusions
    merge_results: List[MergeResult] = []

    if execute and matches_to_merge:
        logger.info("")
        logger.info("=" * 70)
        logger.info("EXÉCUTION DES FUSIONS")
        logger.info("=" * 70)

        for i, m in enumerate(matches_to_merge, 1):
            logger.info(f"[{i}/{len(matches_to_merge)}] Fusion VCOM {m.vcom_site.id} ← Yuman {m.yuman_site.id}...")
            result = merge_single_pair(sb, m.vcom_site.id, m.yuman_site.id, m.yuman_site.yuman_site_id)
            merge_results.append(result)

            if result.success:
                logger.info(f"         ✅ OK")
            else:
                logger.error(f"         ❌ ERREUR: {result.error}")

    # 5. Résumé
    logger.info("")
    logger.info("=" * 70)
    logger.info("RÉSUMÉ")
    logger.info("=" * 70)

    if execute:
        successful = [r for r in merge_results if r.success]
        failed = [r for r in merge_results if not r.success]
        logger.info(f"  Fusions réussies: {len(successful)}")
        logger.info(f"  Fusions échouées: {len(failed)}")
    else:
        logger.info(f"  Fusions prévues: {len(matches_to_merge)}")

    logger.info(f"  Sites VCOM sans paire: {len(unmatched_vcom)}")

    # 6. Envoi d'email si nécessaire
    failed_merges = [r for r in merge_results if not r.success]
    should_send_email = (unmatched_vcom or failed_merges) and (execute or test_email)

    if should_send_email:
        logger.info("")
        logger.info("Envoi de l'email d'alerte...")
        send_alert_email(unmatched_vcom, merge_results, failed_merges)

    # 7. Générer le rapport
    report = generate_report(
        vcom_only=vcom_only,
        yuman_only=yuman_only,
        matches=matches,
        merge_results=merge_results,
        unmatched_vcom=unmatched_vcom,
        dry_run=not execute,
    )

    with open(output, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    logger.info(f"")
    logger.info(f"📄 Rapport exporté: {output}")

    if not execute:
        logger.info("")
        logger.info("-" * 70)
        logger.info("MODE DRY-RUN - Pour exécuter réellement, ajouter --execute")
        logger.info("-" * 70)

    # Code de retour
    if failed_merges:
        return 1  # Erreur si des fusions ont échoué
    return 0


def main() -> int:
    """Point d'entrée CLI pour le script standalone."""
    parser = argparse.ArgumentParser(
        description="Automatise les fusions de sites VCOM ↔ Yuman"
    )
    parser.add_argument("--execute", action="store_true",
                        help="Exécuter réellement les fusions (sinon dry-run)")
    parser.add_argument("--test-email", action="store_true",
                        help="Envoyer l'email même en dry-run (pour tester)")
    parser.add_argument("--output", type=str, default=OUTPUT_FILE,
                        help=f"Fichier de rapport JSON (défaut: {OUTPUT_FILE})")

    args = parser.parse_args()

    return run_auto_merge(
        execute=args.execute,
        test_email=args.test_email,
        output=args.output
    )


if __name__ == "__main__":
    sys.exit(main())