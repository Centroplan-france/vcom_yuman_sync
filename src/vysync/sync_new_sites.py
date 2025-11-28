#!/usr/bin/env python3
"""
Workflow de synchronisation des nouveaux sites VCOM vers Supabase.

Ce script :
1. Détecte les nouveaux sites VCOM (absents de sites_mapping)
2. Crée les sites + équipements dans Supabase
3. Détecte les changements de nom de sites existants
4. Met à jour les noms + clients (extraits depuis les parenthèses)
5. Génère un rapport JSON

Usage:
    poetry run python -m vysync.sync_new_sites
"""

import re
import json
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional
from dataclasses import replace

from vysync.vcom_client import VCOMAPIClient
from vysync.adapters.supabase_adapter import SupabaseAdapter
from vysync.adapters.vcom_adapter import fetch_snapshot
from vysync.diff import PatchSet
from vysync.logging_config import setup_logging

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# FONCTIONS HELPER
# ═══════════════════════════════════════════════════════════════════════════════


def _load_region_client_mapping(sb: SupabaseAdapter) -> Dict[str, int]:
    """
    Pré-charge le mapping région → client_id pour éviter des requêtes répétées.

    Cette fonction charge tous les clients depuis la table clients_mapping
    et crée un dictionnaire de correspondance entre le nom de région (name_addition)
    et l'id du client.

    Args:
        sb: Instance de SupabaseAdapter pour accéder à la base de données

    Returns:
        Dictionnaire {region_name: client_map_id, ...}

    Exemple:
        {"Sauvian": 42, "Reims": 15, "Lyon": 23, ...}

    Note:
        Les entrées sans name_addition sont ignorées car elles ne peuvent pas
        être associées automatiquement à un site.
    """
    # Récupération de tous les clients avec leur nom de région (name_addition)
    result = sb.sb.table("clients_mapping") \
                   .select("id,name_addition") \
                   .execute()

    # Construction du dictionnaire région → client_id
    # On filtre pour ne garder que les entrées avec une région définie
    return {
        row["name_addition"]: row["id"]
        for row in result.data
        if row.get("name_addition")  # Ignore les clients sans région
    }


def _extract_region(site_name: str) -> Optional[str]:
    """
    Extrait la région depuis le nom du site (texte entre parenthèses).

    Les noms de sites VCOM suivent généralement le format :
    "XX NOM_CLIENT Description (REGION)"

    Cette fonction extrait le contenu entre parenthèses qui correspond
    au nom de la région/client.

    Exemples:
        "01 ALDI France Roffiac (Sauvian)" → "Sauvian"
        "02 Lidl Paris (Reims)" → "Reims"
        "Site sans région" → None
        "Site avec (multiple) (parenthèses)" → "parenthèses" (dernière occurrence)

    Args:
        site_name: Nom complet du site tel que retourné par l'API VCOM

    Returns:
        Région extraite (sans espaces superflus) ou None si pas de parenthèses

    Note:
        Si plusieurs paires de parenthèses existent, seule la dernière est utilisée.
    """
    # Recherche du contenu entre parenthèses (dernière occurrence)
    # Pattern: \( = parenthèse ouvrante, ([^)]+) = capture de tout sauf ), \) = parenthèse fermante
    match = re.search(r"\(([^)]+)\)", site_name)

    # Si match trouvé, on retourne le contenu capturé (groupe 1) sans espaces superflus
    return match.group(1).strip() if match else None


def _extract_and_find_client(
    site_name: str,
    region_to_client_id: Dict[str, int],
    warnings: List[dict],
    site_key: str
) -> Optional[int]:
    """
    Extrait la région du nom et cherche le client_id correspondant.

    Cette fonction combine l'extraction de région et la recherche du client associé.
    Si le client n'existe pas dans la base, un warning est ajouté à la liste pour
    traçabilité, mais le site sera quand même créé (avec client_map_id = NULL).

    Workflow:
        1. Extraction de la région depuis le nom du site
        2. Si pas de région trouvée → retourne None
        3. Recherche du client_id dans le mapping pré-chargé
        4. Si client introuvable → ajout d'un warning et retourne None
        5. Si client trouvé → retourne l'id

    Args:
        site_name: Nom complet du site VCOM
        region_to_client_id: Mapping pré-chargé {région → client_id}
        warnings: Liste accumulatrice de warnings (modifiée in-place)
        site_key: vcom_system_key du site (pour identification dans les warnings)

    Returns:
        client_id (int) si trouvé, None sinon

    Side effects:
        Ajoute un warning dans la liste `warnings` si :
        - Aucune région n'est trouvée dans le nom
        - La région est trouvée mais le client n'existe pas en base

    Note:
        Un retour de None n'empêche pas la création du site, il sera créé
        avec client_map_id = NULL et devra être résolu manuellement.
    """
    # ÉTAPE 1 : Extraction de la région depuis le nom du site
    region = _extract_region(site_name)

    # ÉTAPE 2 : Vérification présence de région
    if not region:
        logger.warning(
            "Site %s : aucune région trouvée dans '%s'",
            site_key,
            site_name
        )
        return None

    # ÉTAPE 3 : Recherche du client dans le mapping
    client_id = region_to_client_id.get(region)

    # ÉTAPE 4 : Gestion du cas client introuvable
    if client_id is None:
        logger.warning(
            "Site %s : client '%s' introuvable dans clients_mapping",
            site_key,
            region
        )
        # Ajout d'un warning structuré pour le rapport JSON
        warnings.append({
            "site_key": site_key,
            "site_name": site_name,
            "region": region,
            "context": "new_site"  # Contexte : création d'un nouveau site
        })

    return client_id


# ═══════════════════════════════════════════════════════════════════════════════
# FONCTION PRINCIPALE
# ═══════════════════════════════════════════════════════════════════════════════


def sync_new_sites_and_names() -> dict:
    """
    Workflow principal : détecte et crée les nouveaux sites + détecte les changements de nom.

    Ce script effectue deux opérations principales en une seule passe :

    1. CRÉATION DE NOUVEAUX SITES :
       - Détecte les sites présents dans VCOM mais absents de Supabase
       - Pour chaque nouveau site :
         * Extrait le client depuis le nom (texte entre parenthèses)
         * Récupère le snapshot complet (site + équipements) depuis VCOM
         * Insère le site et tous ses équipements dans Supabase
         * Gère les erreurs individuellement (un échec ne bloque pas les autres)

    2. MISE À JOUR DES NOMS :
       - Compare les noms de sites entre VCOM et Supabase
       - Pour chaque changement détecté :
         * Extrait l'ancien et le nouveau client
         * Met à jour le nom et le client_map_id dans Supabase
         * Logue tous les changements pour traçabilité

    Architecture :
        - Une seule boucle sur tous les sites VCOM (performance)
        - Gestion d'erreur continue (un échec n'arrête pas le traitement)
        - Mapping client pré-chargé (évite les requêtes répétées)
        - Rapport JSON détaillé avec toutes les opérations et erreurs

    Returns:
        Dictionnaire du rapport complet contenant :
        - execution_date : Timestamp UTC de l'exécution
        - summary : Compteurs globaux (sites créés, erreurs, changements, etc.)
        - new_sites_created : Liste des sites créés avec succès
        - new_sites_errors : Liste des échecs de création avec messages d'erreur
        - name_changes : Liste des changements de nom détectés et appliqués
        - client_warnings : Liste des clients introuvables (nécessitent résolution manuelle)

    Side effects:
        - Crée des sites et équipements dans Supabase
        - Met à jour les noms et clients dans sites_mapping
        - Génère un fichier JSON : sync_new_sites_YYYYMMDD_HHMMSS.json
        - Logs console détaillés de toutes les opérations

    Raises:
        Les exceptions individuelles sont catchées et loguées mais ne remontent pas.
        Seules les erreurs fatales (connexion, etc.) remontent au main().
    """

    # ═══════════════════════════════════════════════════════════════
    # INITIALISATION
    # ═══════════════════════════════════════════════════════════════
    logger.info("═" * 60)
    logger.info("DÉMARRAGE : Synchronisation nouveaux sites VCOM")
    logger.info("═" * 60)

    # Initialisation des clients API
    vc = VCOMAPIClient()  # Client VCOM (API meteocontrol)
    sb = SupabaseAdapter()  # Client Supabase (base de données)

    # ═══════════════════════════════════════════════════════════════
    # RÉCUPÉRATION DES DONNÉES
    # ═══════════════════════════════════════════════════════════════
    logger.info("Récupération des données VCOM et Supabase...")

    # Récupération de tous les sites depuis VCOM
    # Format : [{key: "ABC123", name: "Site Name"}, ...]
    vcom_systems = vc.get_systems()
    logger.info("  • Sites VCOM récupérés : %d", len(vcom_systems))

    # Récupération de tous les sites depuis Supabase (sites_mapping)
    # Format : {vcom_system_key: Site(...), ...}
    db_sites = sb.fetch_sites_v()
    logger.info("  • Sites Supabase récupérés : %d", len(db_sites))

    # Pré-chargement du mapping région → client_id
    # Évite de requêter la base pour chaque site (optimisation performance)
    region_to_client_id = _load_region_client_mapping(sb)
    logger.info("  • Clients mappés : %d", len(region_to_client_id))

    # ═══════════════════════════════════════════════════════════════
    # COMPTEURS ET LOGS (accumulateurs pour le rapport final)
    # ═══════════════════════════════════════════════════════════════
    new_sites_created = []  # Sites créés avec succès
    new_sites_errors = []  # Échecs de création (avec détails erreur)
    name_changes = []  # Changements de nom détectés et appliqués
    client_not_found_warnings = []  # Clients introuvables (nécessitent action manuelle)

    # ═══════════════════════════════════════════════════════════════
    # BOUCLE PRINCIPALE : TRAITEMENT DE TOUS LES SITES VCOM
    # ═══════════════════════════════════════════════════════════════
    logger.info("\nTraitement des sites...")

    for sys in vcom_systems:
        # Extraction des informations de base du site
        key = sys["key"]  # vcom_system_key : identifiant unique VCOM
        vcom_name = sys["name"]  # Nom actuel dans VCOM

        # ───────────────────────────────────────────────────────────
        # CAS 1 : NOUVEAU SITE (absent de Supabase)
        # ───────────────────────────────────────────────────────────
        if key not in db_sites:
            try:
                logger.info("\n[NOUVEAU SITE] %s : %s", key, vcom_name)

                # ── A. EXTRACTION DU CLIENT DEPUIS LE NOM ──
                # Le nom suit le format : "XX CLIENT Description (REGION)"
                # On extrait la région et on cherche le client_id correspondant
                client_id = _extract_and_find_client(
                    vcom_name,
                    region_to_client_id,
                    warnings=client_not_found_warnings,  # Accumule les warnings
                    site_key=key
                )

                # Affichage du résultat de la recherche client
                if client_id:
                    logger.info("  • Client trouvé : id=%d", client_id)
                else:
                    logger.warning("  • Client introuvable → client_map_id sera NULL")

                # ── B. RÉCUPÉRATION DU SNAPSHOT COMPLET DEPUIS VCOM ──
                # fetch_snapshot récupère :
                # - Les données du site (coordonnées, puissance nominale, etc.)
                # - Tous les équipements associés (onduleurs, modules, strings, etc.)
                logger.info("  • Récupération snapshot VCOM...")
                v_sites, v_equips = fetch_snapshot(
                    vc,  # Client VCOM
                    vcom_system_key=key,  # Filtre sur ce site uniquement
                    sb_adapter=sb  # Nécessaire pour résoudre les site_id
                )
                logger.info("  • Équipements récupérés : %d", len(v_equips))

                # ── C. ASSIGNATION DU CLIENT AU SITE ──
                # IMPORTANT : Site est une dataclass frozen=True
                # On ne peut pas faire site.client_map_id = client_id
                # Il faut utiliser dataclasses.replace() pour créer une nouvelle instance
                site = v_sites[key]
                site_with_client = replace(site, client_map_id=client_id)

                # ── D. INSERTION DANS SUPABASE ──
                # Ordre important :
                # 1. D'abord le site (crée l'entrée dans sites_mapping)
                # 2. Ensuite les équipements (ils référencent site_id via FK)
                logger.info("  • Insertion en base de données...")

                # Insertion du site via PatchSet
                # PatchSet(add=[...], update=[], delete=[])
                sb.apply_sites_patch(
                    PatchSet(add=[site_with_client], update=[], delete=[])
                )

                # Insertion des équipements via PatchSet
                # v_equips est un dict, on prend les values pour avoir la liste
                sb.apply_equips_patch(
                    PatchSet(add=list(v_equips.values()), update=[], delete=[])
                )

                # ── E. LOGGING DU SUCCÈS ──
                # Ajout à la liste des sites créés (pour le rapport JSON)
                new_sites_created.append({
                    "vcom_system_key": key,
                    "name": vcom_name,
                    "client_id": client_id,
                    "equipments_count": len(v_equips),
                    "timestamp": datetime.now(timezone.utc).isoformat()
                })

                logger.info("  ✓ Site créé avec succès")

            except Exception as e:
                # ── F. GESTION DES ERREURS ──
                # En cas d'échec :
                # 1. Log l'erreur complète (avec stacktrace via exc_info=True)
                # 2. Ajoute à la liste des erreurs pour le rapport
                # 3. Continue avec les autres sites (pas d'interruption globale)
                logger.error(
                    "  ✗ Échec création site %s : %s",
                    key,
                    e,
                    exc_info=True  # Inclut la stacktrace dans les logs
                )
                new_sites_errors.append({
                    "vcom_system_key": key,
                    "name": vcom_name,
                    "error": str(e),
                    "timestamp": datetime.now(timezone.utc).isoformat()
                })
                continue  # Passe au site suivant

        # ───────────────────────────────────────────────────────────
        # CAS 2 : SITE EXISTANT → Vérifier changement de nom
        # ───────────────────────────────────────────────────────────
        else:
            # Récupération du site existant depuis Supabase
            db_site = db_sites[key]
            db_name = db_site.name

            # VÉRIFICATION : Le nom a-t-il changé ?
            if vcom_name != db_name:
                try:
                    logger.info("\n[CHANGEMENT NOM] %s", key)
                    logger.info("  • Ancien : %s", db_name)
                    logger.info("  • Nouveau : %s", vcom_name)

                    # ── A. EXTRACTION DES ANCIENS/NOUVEAUX CLIENTS ──
                    # Les noms incluent le client entre parenthèses
                    # Il faut extraire et comparer les deux pour détecter un changement de client
                    old_region = _extract_region(db_name)
                    new_region = _extract_region(vcom_name)

                    logger.info("  • Ancien client : %s", old_region or "N/A")
                    logger.info("  • Nouveau client : %s", new_region or "N/A")

                    # ── B. RECHERCHE DU NOUVEAU CLIENT ──
                    # Cherche le nouveau client_id dans le mapping pré-chargé
                    new_client_id = region_to_client_id.get(new_region) if new_region else None

                    # Warning si le nouveau client n'existe pas
                    if new_client_id is None and new_region:
                        logger.warning(
                            "  ⚠️  Nouveau client '%s' introuvable dans clients_mapping",
                            new_region
                        )
                        # Ajout au rapport pour résolution manuelle ultérieure
                        client_not_found_warnings.append({
                            "site_key": key,
                            "region": new_region,
                            "context": "name_change"  # Contexte : changement de nom
                        })

                    # ── C. UPDATE DANS SUPABASE ──
                    # Mise à jour simultanée de :
                    # - name : nouveau nom du site
                    # - client_map_id : nouveau client (ou NULL si introuvable)
                    sb.sb.table("sites_mapping").update({
                        "name": vcom_name,
                        "client_map_id": new_client_id
                    }).eq("vcom_system_key", key).execute()

                    # ── D. LOGGING DU CHANGEMENT ──
                    # Ajout au rapport avec tous les détails pour traçabilité
                    name_changes.append({
                        "vcom_system_key": key,
                        "old_name": db_name,
                        "new_name": vcom_name,
                        "old_client_region": old_region,
                        "new_client_region": new_region,
                        "old_client_id": db_site.client_map_id,
                        "new_client_id": new_client_id,
                        "timestamp": datetime.now(timezone.utc).isoformat()
                    })

                    logger.info("  ✓ Nom et client mis à jour")

                except Exception as e:
                    # Gestion des erreurs pour les mises à jour de nom
                    # Continue avec les autres sites même en cas d'échec
                    logger.error(
                        "  ✗ Échec mise à jour nom %s : %s",
                        key,
                        e,
                        exc_info=True
                    )
                    continue

    # ═══════════════════════════════════════════════════════════════
    # GÉNÉRATION DU RAPPORT JSON
    # ═══════════════════════════════════════════════════════════════
    logger.info("\n" + "═" * 60)
    logger.info("GÉNÉRATION DU RAPPORT")
    logger.info("═" * 60)

    # Construction du rapport avec toutes les informations collectées
    report = {
        # Métadonnées de l'exécution
        "execution_date": datetime.now(timezone.utc).isoformat(),

        # Résumé chiffré (compteurs globaux)
        "summary": {
            "new_sites_detected": len([s for s in vcom_systems if s["key"] not in db_sites]),
            "new_sites_created": len(new_sites_created),
            "new_sites_failed": len(new_sites_errors),
            "name_changes_detected": len(name_changes),
            "clients_not_found": len(client_not_found_warnings)
        },

        # Détails des opérations (avec timestamps et métadonnées complètes)
        "new_sites_created": new_sites_created,
        "new_sites_errors": new_sites_errors,
        "name_changes": name_changes,
        "client_warnings": client_not_found_warnings
    }

    # ── SAUVEGARDE DU RAPPORT EN FICHIER JSON ──
    # Nom de fichier avec timestamp pour éviter les écrasements
    report_filename = f"sync_new_sites_{datetime.now():%Y%m%d_%H%M%S}.json"

    # Écriture du JSON avec indentation pour lisibilité
    with open(report_filename, "w", encoding="utf-8") as f:
        json.dump(
            report,
            f,
            indent=2,  # Indentation à 2 espaces
            ensure_ascii=False  # Permet les caractères UTF-8 (accents, etc.)
        )

    # ── AFFICHAGE DU RÉSUMÉ CONSOLE ──
    logger.info("\n📊 RÉSUMÉ")
    logger.info("  • Nouveaux sites créés : %d", len(new_sites_created))
    logger.info("  • Échecs création      : %d", len(new_sites_errors))
    logger.info("  • Changements de nom   : %d", len(name_changes))
    logger.info("  • Clients introuvables : %d", len(client_not_found_warnings))
    logger.info("  • Rapport sauvegardé   : %s", report_filename)
    logger.info("═" * 60)

    return report


# ═══════════════════════════════════════════════════════════════════════════════
# POINT D'ENTRÉE CLI
# ═══════════════════════════════════════════════════════════════════════════════


def main():
    """
    Point d'entrée CLI du script.

    Configure le logging, lance la synchronisation, et retourne un code de sortie.

    Returns:
        0 : Succès complet (tous les sites traités sans erreur)
        1 : Échecs partiels ou erreur fatale

    Usage:
        poetry run python -m vysync.sync_new_sites
    """
    # Configuration du système de logging
    # Crée les fichiers debug.log et updates.log
    setup_logging()

    try:
        # Exécution de la synchronisation
        report = sync_new_sites_and_names()

        # ── DÉTERMINATION DU CODE DE SORTIE ──
        # Code 1 si au moins un site a échoué (pour alerter dans les scripts/CI)
        if report["summary"]["new_sites_failed"] > 0:
            logger.warning(
                "⚠️  Certains sites n'ont pas pu être créés (voir rapport JSON)"
            )
            return 1

        # Code 0 si tout s'est bien passé
        logger.info("✅ Synchronisation terminée avec succès")
        return 0

    except Exception as e:
        # Gestion des erreurs fatales (connexion DB, API, etc.)
        logger.error("❌ Erreur fatale : %s", e, exc_info=True)
        return 1


# Point d'entrée Python standard
if __name__ == "__main__":
    exit(main())
