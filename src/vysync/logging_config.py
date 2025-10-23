#!/usr/bin/env python3
"""
Configuration centralisée du logging pour vysync.

Stratégie :
- Console (stdout) : niveau INFO, format court
- debug.log : niveau DEBUG, tout
- updates.log : logger dédié aux updates d'équipements
"""

import logging
from pathlib import Path
from datetime import datetime

# Dossier logs à la racine du projet
LOGS_DIR = Path(__file__).parent.parent.parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)

# Format pour la console (court et lisible)
CONSOLE_FORMAT = "%(levelname)s | %(message)s"

# Format pour les fichiers (détaillé)
FILE_FORMAT = "%(asctime)s | %(name)s | %(levelname)s | %(message)s"

def setup_logging():
    """Configure le système de logging global."""

    # Root logger à DEBUG (capture tout)
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    # Supprimer les handlers existants (évite les doublons)
    root_logger.handlers.clear()

    # ── Handler 1 : Console (INFO uniquement) ──
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(CONSOLE_FORMAT))
    root_logger.addHandler(console_handler)

    # ── Handler 2 : Fichier debug.log (DEBUG complet) ──
    debug_file = LOGS_DIR / f"debug_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    debug_handler = logging.FileHandler(debug_file, encoding='utf-8')
    debug_handler.setLevel(logging.DEBUG)
    debug_handler.setFormatter(logging.Formatter(FILE_FORMAT))
    root_logger.addHandler(debug_handler)

    # ── Logger dédié : updates.log (équipements uniquement) ──
    updates_logger = logging.getLogger("vysync.updates")
    updates_logger.setLevel(logging.DEBUG)
    updates_logger.propagate = False  # Ne pas propager au root logger

    updates_file = LOGS_DIR / f"updates_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    updates_handler = logging.FileHandler(updates_file, encoding='utf-8')
    updates_handler.setLevel(logging.DEBUG)
    updates_handler.setFormatter(logging.Formatter(FILE_FORMAT))
    updates_logger.addHandler(updates_handler)

    # Log de confirmation
    logging.info(f"Logs initialisés : console=INFO, debug={debug_file.name}, updates={updates_file.name}")

    return updates_logger


def get_updates_logger():
    """Retourne le logger dédié aux updates d'équipements."""
    return logging.getLogger("vysync.updates")
