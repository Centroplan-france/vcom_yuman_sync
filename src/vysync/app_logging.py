#!/usr/bin/env python3
"""
Helper de debug universel pour le logging.

Ce module fournit uniquement la fonction _dump() pour sérialiser
les objets en JSON lisible au niveau DEBUG.

La configuration du logging est centralisée dans logging_config.py.
"""

import logging
import json


def _dump(label: str, obj, *, logger: logging.Logger | None = None) -> None:
    """
    Écrit « label » puis l'objet (pretty-JSON) au niveau DEBUG.
    Ne fait rien si le logger n'est pas en DEBUG – zéro overhead en prod.

    Args:
        label: Label à afficher avant l'objet
        obj: Objet à sérialiser en JSON (dict, list, ou tout objet sérialisable)
        logger: Logger à utiliser (par défaut: root logger)

    Note:
        - Les clés None sont gérées automatiquement par default=str
        - sort_keys est désactivé pour éviter les crashs avec des clés None
    """
    log = logger or logging.getLogger()
    if not log.isEnabledFor(logging.DEBUG):
        return
    log.debug("%s\n%s", label, json.dumps(obj, default=str, indent=2))
