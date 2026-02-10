"""Fonctions utilitaires partagées par les modules vysync."""

from __future__ import annotations

import re


def norm_serial(s: str | None) -> str:
    """Normalise un serial_number : strip + majuscules."""
    return (s or "").strip().upper()


def normalize_site_name(name: str) -> str:
    """Normalise un nom de site en enlevant le préfixe numérique, 'France' et le suffixe entre parenthèses."""
    if not name:
        return ""
    return re.sub(r'^\d+\s+|\s*\(.*?\)| France', '', name).strip()


def normalize_name(name: str) -> str:
    """Normalise un nom de site pour la comparaison (minuscules, sans caractères spéciaux)."""
    if not name:
        return ""
    n = name.lower().strip()
    n = re.sub(r'\([^)]*\)', '', n)  # Supprimer parenthèses
    n = re.sub(r'[^a-z0-9\s]', ' ', n)  # Caractères spéciaux
    n = ' '.join(n.split())
    return n
