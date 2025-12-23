#!/usr/bin/env python3
"""
Configuration centralisée du logging pour vysync.

Structure :
    logs/
    ├── vysync_YYYYMMDD_HHMMSS.log   # Log principal (INFO par défaut)
    └── reports/
        └── *.json                    # Rapports JSON

Niveaux :
    - Console : INFO (messages importants)
    - Fichier : INFO par défaut, DEBUG si LOG_LEVEL=DEBUG

Usage :
    # Mode normal (INFO)
    poetry run python -m vysync.cli sync

    # Mode debug (verbose)
    LOG_LEVEL=DEBUG poetry run python -m vysync.cli sync

Nettoyage automatique des fichiers > 7 jours au démarrage.
"""

import os
import json
import logging
from pathlib import Path
from datetime import datetime, timedelta

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

LOGS_DIR = Path(__file__).parent.parent.parent / "logs"
REPORTS_DIR = LOGS_DIR / "reports"
LOG_RETENTION_DAYS = 7

# Niveau par défaut, modifiable via LOG_LEVEL=DEBUG
DEFAULT_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

# Formats
CONSOLE_FORMAT = "%(levelname)s | %(message)s"
FILE_FORMAT = "%(asctime)s | %(levelname)s | %(message)s"


# ══════════════════════════════════════════════════════════════════════════════
# NETTOYAGE AUTOMATIQUE
# ══════════════════════════════════════════════════════════════════════════════


def _cleanup_old_files(directory: Path, pattern: str, days: int = LOG_RETENTION_DAYS) -> int:
    """Supprime les fichiers correspondant au pattern plus anciens que `days` jours."""
    if not directory.exists():
        return 0

    cutoff = datetime.now() - timedelta(days=days)
    deleted = 0

    for f in directory.glob(pattern):
        if f.is_file() and datetime.fromtimestamp(f.stat().st_mtime) < cutoff:
            try:
                f.unlink()
                deleted += 1
            except OSError:
                pass

    return deleted


# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION DU LOGGING
# ══════════════════════════════════════════════════════════════════════════════


def setup_logging() -> None:
    """
    Configure le système de logging.

    - Crée logs/ et logs/reports/
    - Nettoie les fichiers > 7 jours
    - Configure console (INFO) et fichier (INFO ou DEBUG selon LOG_LEVEL)
    """
    LOGS_DIR.mkdir(exist_ok=True)
    REPORTS_DIR.mkdir(exist_ok=True)

    # Nettoyage
    cleaned = _cleanup_old_files(LOGS_DIR, "vysync_*.log")
    cleaned += _cleanup_old_files(REPORTS_DIR, "*.json")

    # Niveau effectif
    level = getattr(logging, DEFAULT_LEVEL, logging.INFO)

    # Root logger
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)  # Capture tout, filtrage par handlers
    root.handlers.clear()

    # Réduire le bruit des libs tierces
    for lib in ("hpack", "httpcore", "httpx", "urllib3", "asyncio"):
        logging.getLogger(lib).setLevel(logging.WARNING)

    # Handler console (toujours INFO)
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter(CONSOLE_FORMAT))
    root.addHandler(console)

    # Handler fichier (INFO ou DEBUG selon env)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = LOGS_DIR / f"vysync_{timestamp}.log"
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(level)
    file_handler.setFormatter(logging.Formatter(FILE_FORMAT))
    root.addHandler(file_handler)

    # Message de confirmation
    mode = "DEBUG" if level == logging.DEBUG else "INFO"
    clean_msg = f", {cleaned} ancien(s) supprimé(s)" if cleaned else ""
    logging.info(f"Logs: {log_file.name} (mode={mode}{clean_msg})")


def get_reports_dir() -> Path:
    """Retourne le dossier des rapports, en le créant si nécessaire."""
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    return REPORTS_DIR


# ══════════════════════════════════════════════════════════════════════════════
# HELPER DEBUG
# ══════════════════════════════════════════════════════════════════════════════


def dump(label: str, obj, *, logger: logging.Logger | None = None) -> None:
    """
    Affiche un objet en JSON formaté au niveau DEBUG.

    Ne fait rien si DEBUG n'est pas activé (zéro overhead en prod).

    Args:
        label: Description de l'objet
        obj: Objet à sérialiser (dict, list, etc.)
        logger: Logger à utiliser (défaut: root logger)

    Exemple:
        dump("Réponse API", response_data)
        dump("Config site", site_config, logger=my_logger)
    """
    log = logger or logging.getLogger()
    if not log.isEnabledFor(logging.DEBUG):
        return
    log.debug("%s\n%s", label, json.dumps(obj, default=str, indent=2))
