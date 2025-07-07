#!/usr/bin/env python3
from __future__ import annotations
"""
Dataclasses métier : Site, Equipment, Client
La clé de comparaison d'un Equipment est désormais **un simple string** :
        key()  ->  vcom_device_id
"""

from dataclasses import dataclass, asdict
from typing import Any, Dict, Optional

# ────────────────────────── Sites ────────────────────────────
@dataclass(frozen=True)
class Site:
    vcom_system_key: str
    name: str
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    nominal_power: Optional[float] = None
    commission_date: Optional[str] = None  # ISO
    address: Optional[str] = None
    yuman_site_id: Optional[int] = None

    def key(self) -> str:
        return self.vcom_system_key

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

# ──────────────────────── Equipements ────────────────────────
@dataclass(frozen=True)
class Equipment:
    # NB: site_key reste stocké pour filtre/lookup mais ne participe plus à la clé
    site_key: str
    category_id: int
    eq_type: str
    vcom_device_id: str
    name: str
    brand: Optional[str] = None
    model: Optional[str] = None
    serial_number: Optional[str] = None
    count: Optional[int] = None
    parent_vcom_id: Optional[str] = None      # pour STRING → onduleur
    yuman_material_id: Optional[int] = None

    def key(self) -> str:                      # ← CHANGEMENT MAJEUR
        return self.vcom_device_id

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d.pop("site_key", None)                # jamais envoyé vers la DB
        d.pop("parent_vcom_id", None)          # géré ailleurs
        return d

# ───────────────────────── Clients ───────────────────────────
@dataclass(frozen=True)
class Client:
    yuman_client_id: int
    code: Optional[str]
    name: str

    def key(self) -> int:
        return self.yuman_client_id

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

# ───────────────────────── Constantes ────────────────────────
CAT_INVERTER = 11102
CAT_MODULE   = 11103
CAT_STRING   = 12404
CAT_CENTRALE = 11441
