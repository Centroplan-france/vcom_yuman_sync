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
    name: str
    id: Optional[int] = None   
    yuman_site_id: Optional[int] = None
    vcom_system_key: Optional[str] = None
    client_map_id: Optional[int] = None 
    code: Optional[int] = None                      # code yuman
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    nominal_power: Optional[float] = None
    site_area: Optional[float] = None
    commission_date: Optional[str] = None  # ISO
    address: Optional[str] = None
    aldi_id: Optional[str] = None                   # "ALDI ID"
    aldi_store_id: Optional[str] = None             # "ID magasin (n° interne Aldi)"
    project_number_cp: Optional[str] = None         # "Project number (Centroplan ID)"      
    ignore_site:   bool = False  
        
    def key(self) -> str:
        return self.vcom_system_key

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

# ──────────────────────── Equipements ────────────────────────
@dataclass(frozen=True, eq=False)               # ① on désactive l’__eq__ auto
class Equipment:
    category_id: int
    eq_type: str
    name: str
    site_id: Optional[int] | None = None
    vcom_system_key: Optional[str] = None
    yuman_material_id: Optional[int] = None
    vcom_device_id: Optional[str] = None
    brand: Optional[str] = None
    model: Optional[str] = None
    serial_number: Optional[str] = None
    count: Optional[int] = None                # ex-string_count ?
    parent_id: Optional[str] = None
    yuman_site_id: Optional[int] | None = None
    


    # --- clé « métier » -----------------------------------
    def key(self) -> str:
        return self.vcom_device_id

    # --- sérialisation -----------------------------------
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    # --- égalité (cohérente avec la doc-string) ---------- ②
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Equipment):
            return NotImplemented
        return self.vcom_device_id == other.vcom_device_id

    # --- hash cohérent avec __eq__ ----------------------- ③
    def __hash__(self) -> int:
        return hash(self.vcom_device_id)

# ───────────────────────── Clients ───────────────────────────
@dataclass(frozen=True)
class Client:
    yuman_client_id: int
    name: str
    code: Optional[str]
    address: Optional[str]

    def key(self) -> int:
        return self.yuman_client_id

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

# ───────────────────────── Constantes ────────────────────────
CAT_INVERTER = 11102
CAT_MODULE   = 11103
CAT_STRING   = 12404
CAT_CENTRALE = 11441
CAT_SIM      = 11382
