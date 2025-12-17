#!/usr/bin/env python3
"""
Parser pour les noms d'onduleurs VCOM.

Extrait les informations structurées des noms VCOM qui peuvent avoir plusieurs formats :
- "WR 1 - RPI M50A - O3618B0830" → WR=1, Model=RPI M50A, Serial=O3618B0830
- "WR2 - SunGrow - SG40CX-P2 - E/O - A2341007101" → WR=2, Vendor=SunGrow, Model=SG40CX-P2
- "Solplanet ASW xxxK LT AQ00806052370055" → Vendor=Solplanet, Model=ASW xxxK LT
- "SunGrow SG110CX A21B0203116" → Vendor=SunGrow, Model=SG110CX
- "Onduleur 2 SN A2162600126" → WR=2, Serial=A2162600126
- "Carport A WR1 SG125CX-P2 A2372424429" → WR=1, Model=SG125CX-P2, Carport=True
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional


# Vendors connus (case-insensitive pour la recherche)
KNOWN_VENDORS = {
    "sungrow": "SunGrow",
    "solaredge": "SolarEdge",
    "solplanet": "Solplanet",
    "delta": "Delta",
    "power-one": "Power-One",
    "kaco": "KACO",
    "huawei": "Huawei",
    "abb": "ABB",
    "fronius": "Fronius",
    "sma": "SMA",
    "rpi": "RPI",  # Ajouté car présent dans les exemples
}

# Patterns de modèles connus pour détection
MODEL_PATTERNS = [
    r"SG\d+CX(?:-P2)?",          # SunGrow: SG40CX, SG110CX, SG125CX-P2
    r"ASW\s*\d*K?\s*LT",         # Solplanet: ASW xxxK LT
    r"M\d+A",                     # RPI: M50A
    r"SE\d+",                     # SolarEdge
]


@dataclass
class ParsedInverterName:
    """Résultat du parsing d'un nom d'onduleur VCOM."""
    wr_number: Optional[int] = None
    vendor: Optional[str] = None
    model: Optional[str] = None
    serial_from_name: Optional[str] = None
    is_carport: bool = False


def _normalize_vendor(vendor_str: str) -> Optional[str]:
    """Normalise un vendor vers sa forme canonique."""
    if not vendor_str:
        return None
    vendor_lower = vendor_str.lower().strip()
    return KNOWN_VENDORS.get(vendor_lower)


def _is_known_vendor(segment: str) -> bool:
    """Vérifie si un segment correspond à un vendor connu."""
    return segment.lower().strip() in KNOWN_VENDORS


def _extract_wr_number(name: str) -> Optional[int]:
    """Extrait le numéro WR ou Onduleur du nom."""
    # Pattern: WR suivi optionnellement d'un espace puis d'un nombre
    # ou Onduleur suivi d'un espace et d'un nombre
    patterns = [
        r"WR\s*(\d+)",           # WR 1, WR1, WR 12
        r"Onduleur\s+(\d+)",     # Onduleur 2
    ]

    for pattern in patterns:
        match = re.search(pattern, name, re.IGNORECASE)
        if match:
            return int(match.group(1))

    return None


def _is_serial_like(segment: str) -> bool:
    """
    Vérifie si un segment ressemble à un numéro de série.
    Les numéros de série sont généralement alphanumériques,
    commençant souvent par une lettre suivie de chiffres.
    """
    if not segment:
        return False

    segment = segment.strip()

    # Trop court pour être un serial (moins de 6 caractères)
    if len(segment) < 6:
        return False

    # Pattern typique : commence par 1-2 lettres puis des chiffres
    # ou uniquement des chiffres/lettres mélangés (min 8 chars alphanumériques)
    serial_patterns = [
        r"^[A-Z]{1,2}\d{6,}$",           # A1234567, AB12345678
        r"^[A-Z]\d+[A-Z]\d+$",           # A21B0203116
        r"^[A-Z]{2}\d{10,}$",            # AQ00806052370055
        r"^\d{10,}$",                     # Purement numérique long
    ]

    for pattern in serial_patterns:
        if re.match(pattern, segment, re.IGNORECASE):
            return True

    # Fallback: plus de 50% de chiffres et alphanumériques uniquement
    if segment.isalnum():
        digit_ratio = sum(c.isdigit() for c in segment) / len(segment)
        if digit_ratio > 0.5 and len(segment) >= 8:
            return True

    return False


def parse_vcom_inverter_name(name: str) -> ParsedInverterName:
    """
    Parse le name VCOM pour extraire les informations de l'onduleur.

    Args:
        name: Nom complet de l'onduleur depuis l'API VCOM

    Returns:
        ParsedInverterName avec les champs extraits

    Formats supportés:
        - "WR 1 - RPI M50A - O3618B0830" → WR=1, Model=RPI M50A, Serial=O3618B0830
        - "WR2 - SunGrow - SG40CX-P2 - E/O - A2341007101" → WR=2, Vendor=SunGrow, Model=SG40CX-P2
        - "Solplanet ASW xxxK LT AQ00806052370055" → Vendor=Solplanet, Model=ASW xxxK LT
        - "SunGrow SG110CX A21B0203116" → Vendor=SunGrow, Model=SG110CX
        - "Onduleur 2 SN A2162600126" → WR=2, Serial=A2162600126
        - "Carport A WR1 SG125CX-P2 A2372424429" → WR=1, Model=SG125CX-P2, Carport=True
    """
    if not name:
        return ParsedInverterName()

    result = ParsedInverterName()

    # 1. Détecter "Carport" ou "Ombrière" (case insensitive)
    result.is_carport = bool(re.search(r"(carport|ombrière|ombriere)", name, re.IGNORECASE))

    # 2. Extraire le numéro WR/Onduleur
    result.wr_number = _extract_wr_number(name)

    # 3. Parser selon le format détecté

    # Format avec tirets " - " (séparateur principal)
    if " - " in name:
        result = _parse_dash_format(name, result)

    # Format "Onduleur X SN Serial"
    elif re.search(r"Onduleur\s+\d+\s+SN\s+", name, re.IGNORECASE):
        result = _parse_onduleur_sn_format(name, result)

    # Format "Carport X WRY Model Serial"
    elif result.is_carport and result.wr_number is not None:
        result = _parse_carport_format(name, result)

    # Format "Vendor Model Serial" (sans tirets)
    else:
        result = _parse_space_format(name, result)

    return result


def _parse_dash_format(name: str, result: ParsedInverterName) -> ParsedInverterName:
    """
    Parse les formats avec tirets " - ".

    Exemples:
        - "WR 1 - RPI M50A - O3618B0830" → WR=1, Model=RPI M50A, Serial=O3618B0830
        - "WR2 - SunGrow - SG40CX-P2 - E/O - A2341007101" → WR=2, Vendor=SunGrow, Model=SG40CX-P2
    """
    segments = [s.strip() for s in name.split(" - ")]

    if len(segments) < 2:
        return result

    # Premier segment : généralement WR X ou le nom
    # Ignorer si c'est juste "WR X"
    first_clean = re.sub(r"WR\s*\d+", "", segments[0], flags=re.IGNORECASE).strip()

    # Si premier segment vide après nettoyage WR, on commence au segment 1
    start_idx = 0 if first_clean else 1

    if start_idx >= len(segments):
        return result

    # Analyser les segments restants
    remaining = segments[start_idx:]

    # Si le premier segment restant est un vendor connu
    if remaining and _is_known_vendor(remaining[0]):
        result.vendor = _normalize_vendor(remaining[0])
        remaining = remaining[1:]

    # Le segment suivant devrait être le model
    if remaining:
        # Vérifier si c'est un serial ou un model
        if _is_serial_like(remaining[0]):
            result.serial_from_name = remaining[0]
        else:
            # C'est probablement le model
            # Peut inclure le vendor si format "RPI M50A"
            model_segment = remaining[0]

            # Vérifier si le model commence par un vendor
            for vendor_key, vendor_name in KNOWN_VENDORS.items():
                if model_segment.lower().startswith(vendor_key):
                    if result.vendor is None:
                        result.vendor = vendor_name
                    # Extraire le model après le vendor
                    model_part = model_segment[len(vendor_key):].strip()
                    if model_part:
                        result.model = model_part
                    break
            else:
                # Pas de vendor détecté, c'est le model complet
                result.model = model_segment

            remaining = remaining[1:]

    # Chercher le serial dans les segments restants
    for seg in remaining:
        if _is_serial_like(seg):
            result.serial_from_name = seg
            break
        # Ignorer les segments comme "E/O" (indicateurs divers)

    return result


def _parse_onduleur_sn_format(name: str, result: ParsedInverterName) -> ParsedInverterName:
    """
    Parse le format "Onduleur X SN Serial".

    Exemple: "Onduleur 2 SN A2162600126" → WR=2, Serial=A2162600126
    """
    match = re.search(r"Onduleur\s+(\d+)\s+SN\s+(\S+)", name, re.IGNORECASE)
    if match:
        result.wr_number = int(match.group(1))
        result.serial_from_name = match.group(2)

    return result


def _parse_carport_format(name: str, result: ParsedInverterName) -> ParsedInverterName:
    """
    Parse le format "Carport X WRY Model Serial".

    Exemple: "Carport A WR1 SG125CX-P2 A2372424429" → WR=1, Model=SG125CX-P2, Carport=True
    """
    # Enlever "Carport X" du début
    cleaned = re.sub(r"^Carport\s+\S+\s+", "", name, flags=re.IGNORECASE)

    # Enlever "WRX"
    cleaned = re.sub(r"WR\s*\d+\s*", "", cleaned, flags=re.IGNORECASE).strip()

    # Les parties restantes sont Model et Serial (séparés par espace)
    parts = cleaned.split()

    if parts:
        # Premier élément = Model
        result.model = parts[0]

        # Si un deuxième élément existe et ressemble à un serial
        if len(parts) > 1 and _is_serial_like(parts[-1]):
            result.serial_from_name = parts[-1]

    return result


def _parse_space_format(name: str, result: ParsedInverterName) -> ParsedInverterName:
    """
    Parse le format "Vendor Model Serial" (séparé par espaces).

    Exemples:
        - "Solplanet ASW xxxK LT AQ00806052370055" → Vendor=Solplanet, Model=ASW xxxK LT
        - "SunGrow SG110CX A21B0203116" → Vendor=SunGrow, Model=SG110CX
    """
    # Retirer le préfixe WR/Carport si présent
    cleaned = re.sub(r"^(Carport\s+\S+\s+)?(WR\s*\d+\s+)?", "", name, flags=re.IGNORECASE).strip()

    parts = cleaned.split()
    if not parts:
        return result

    # Vérifier si le premier mot est un vendor connu
    if _is_known_vendor(parts[0]):
        result.vendor = _normalize_vendor(parts[0])
        parts = parts[1:]

    if not parts:
        return result

    # Identifier le serial (dernier élément si ressemble à un serial)
    serial_idx = None
    if _is_serial_like(parts[-1]):
        result.serial_from_name = parts[-1]
        serial_idx = len(parts) - 1

    # Le model est ce qui reste entre vendor et serial
    model_parts = parts[:serial_idx] if serial_idx is not None else parts
    if model_parts:
        result.model = " ".join(model_parts)

    return result
