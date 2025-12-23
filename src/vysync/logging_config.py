#!/usr/bin/env python3
"""
Configuration centralisée du logging pour vysync.

Stratégie :
- Console (stdout) : niveau INFO, format court
- debug.log : niveau DEBUG, tout
- updates.log : logger dédié aux updates d'équipements
- Nettoyage automatique des logs de plus de 7 jours au démarrage

Structure des logs :
    logs/
    ├── debug_YYYYMMDD_HHMMSS.log
    ├── updates_YYYYMMDD_HHMMSS.log
    └── reports/
        └── *.json
"""

import logging
from pathlib import Path
from datetime import datetime, timedelta

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

# Dossiers logs à la racine du projet
LOGS_DIR = Path(__file__).parent.parent.parent / "logs"
REPORTS_DIR = LOGS_DIR / "reports"

# Durée de rétention des logs (en jours)
LOG_RETENTION_DAYS = 7

# Format pour la console (court et lisible)
CONSOLE_FORMAT = "%(levelname)s | %(message)s"

# Format pour les fichiers (détaillé)
FILE_FORMAT = "%(asctime)s | %(name)s | %(levelname)s | %(message)s"


# ══════════════════════════════════════════════════════════════════════════════
# NETTOYAGE DES ANCIENS LOGS
# ══════════════════════════════════════════════════════════════════════════════


def cleanup_old_logs(directory: Path, pattern: str = "*", days: int = LOG_RETENTION_DAYS) -> int:
    """
    Supprime les fichiers correspondant au pattern plus anciens que `days` jours.

    Args:
        directory: Dossier à nettoyer
        pattern: Glob pattern des fichiers à considérer (ex: "debug_*.log")
        days: Âge maximum en jours (défaut: LOG_RETENTION_DAYS)

    Returns:
        Nombre de fichiers supprimés
    """
    if not directory.exists():
        return 0

    cutoff = datetime.now() - timedelta(days=days)
    deleted = 0

    for file_path in directory.glob(pattern):
        if file_path.is_file():
            # Utilise la date de modification du fichier
            mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
            if mtime < cutoff:
                try:
                    file_path.unlink()
                    deleted += 1
                except OSError:
                    pass  # Ignore les erreurs de suppression

    return deleted


def cleanup_all_old_logs() -> dict[str, int]:
    """
    Nettoie tous les anciens logs (debug, updates, reports).

    Returns:
        Dictionnaire avec le nombre de fichiers supprimés par catégorie
    """
    results = {
        "debug_logs": cleanup_old_logs(LOGS_DIR, "debug_*.log"),
        "updates_logs": cleanup_old_logs(LOGS_DIR, "updates_*.log"),
        "reports": cleanup_old_logs(REPORTS_DIR, "*.json"),
    }
    return results


# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION DU LOGGING
# ══════════════════════════════════════════════════════════════════════════════


def setup_logging() -> logging.Logger:
    """
    Configure le système de logging global.

    Actions :
    1. Crée les dossiers logs/ et logs/reports/ si nécessaire
    2. Nettoie les logs de plus de 7 jours
    3. Configure les handlers (console, debug.log, updates.log)

    Returns:
        Logger dédié aux updates d'équipements
    """
    # Création des dossiers
    LOGS_DIR.mkdir(exist_ok=True)
    REPORTS_DIR.mkdir(exist_ok=True)

    # Nettoyage des anciens logs
    cleanup_results = cleanup_all_old_logs()
    total_cleaned = sum(cleanup_results.values())

    # Root logger à DEBUG (capture tout)
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    # Supprimer les handlers existants (évite les doublons)
    root_logger.handlers.clear()

    # ── Réduire le bruit des bibliothèques tierces ──
    for lib in ("hpack", "httpcore", "httpx", "urllib3", "asyncio"):
        logging.getLogger(lib).setLevel(logging.WARNING)

    # ── Handler 1 : Console (INFO uniquement) ──
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(CONSOLE_FORMAT))
    root_logger.addHandler(console_handler)

    # ── Handler 2 : Fichier debug.log (DEBUG complet) ──
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    debug_file = LOGS_DIR / f"debug_{timestamp}.log"
    debug_handler = logging.FileHandler(debug_file, encoding='utf-8')
    debug_handler.setLevel(logging.DEBUG)
    debug_handler.setFormatter(logging.Formatter(FILE_FORMAT))
    root_logger.addHandler(debug_handler)

    # ── Logger dédié : updates.log (équipements uniquement) ──
    updates_logger = logging.getLogger("vysync.updates")
    updates_logger.setLevel(logging.DEBUG)
    updates_logger.propagate = False  # Ne pas propager au root logger

    updates_file = LOGS_DIR / f"updates_{timestamp}.log"
    updates_handler = logging.FileHandler(updates_file, encoding='utf-8')
    updates_handler.setLevel(logging.DEBUG)
    updates_handler.setFormatter(logging.Formatter(FILE_FORMAT))
    updates_logger.addHandler(updates_handler)

    # Log de confirmation
    cleanup_msg = f", {total_cleaned} ancien(s) log(s) supprimé(s)" if total_cleaned else ""
    logging.info(f"Logs initialisés : debug={debug_file.name}, updates={updates_file.name}{cleanup_msg}")

    return updates_logger


def get_updates_logger() -> logging.Logger:
    """Retourne le logger dédié aux updates d'équipements."""
    return logging.getLogger("vysync.updates")


def get_reports_dir() -> Path:
    """
    Retourne le dossier des rapports, en le créant si nécessaire.

    Returns:
        Path vers logs/reports/
    """
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    return REPORTS_DIR
