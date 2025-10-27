#!/usr/bin/env python3
from __future__ import annotations
"""
Dataclasses métier : Site, Equipment, Client
La clé de comparaison d'un Equipment est désormais **un simple string** :
        key()  ->  vcom_device_id
"""

from dataclasses import dataclass, asdict, field
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
@dataclass(frozen=True, eq=False)               # ① on désactive l'__eq__ auto
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

    # NOUVEAU : Stockage des champs custom Yuman
    _custom_fields: Dict[str, Any] = field(default_factory=dict)

    # --- clé « métier » -----------------------------------
    def key(self) -> str:
        return self.serial_number

    # --- sérialisation -----------------------------------
    def to_dict(self) -> Dict[str, Any]:
        """Sérialisation complète pour usage métier (logs, diffs, comparaisons)."""
        d = asdict(self)
        # Inclure les custom fields dans la sérialisation
        d['_custom_fields'] = self._custom_fields
        return d

    def to_db_dict(self) -> Dict[str, Any]:
        """Sérialisation pour persistance Supabase (exclut les colonnes supprimées)."""
        d = asdict(self)
        d.pop("vcom_system_key", None)
        d.pop("yuman_site_id", None)
        d.pop("_custom_fields", None)  # Ne pas persister les custom fields en DB
        return d

    # ────────────────────── MÉTHODES D'ACCÈS NORMALISÉES ──────────────────────

    def get_nb_modules(self) -> Optional[int]:
        """
        Retourne le nombre de modules.
        Pour STRING : depuis _custom_fields["nb_modules"] (prioritaire) ou count
        Pour autres : depuis count
        """
        if self.category_id == CAT_STRING:
            # Priorité aux custom fields
            nb = self._custom_fields.get("nb_modules")
            if nb:
                try:
                    return int(nb)
                except (ValueError, TypeError):
                    pass
            return self.count
        return self.count

    def get_module_model(self) -> str:
        """
        Retourne le modèle de module.
        Pour STRING/INVERTER : depuis _custom_fields["module_model"] ou "Modèle"
        Pour autres : depuis model standard
        """
        if self.category_id in (CAT_STRING, CAT_INVERTER):
            # Priorité aux custom fields
            return (
                self._custom_fields.get("module_model") or
                self._custom_fields.get("Modèle") or
                self.model or
                ""
            )
        return self.model or ""

    def get_module_brand(self) -> str:
        """
        Retourne la marque de module.
        Pour STRING : depuis _custom_fields["module_brand"] (prioritaire) ou brand
        Pour autres : depuis brand standard
        """
        if self.category_id == CAT_STRING:
            return self._custom_fields.get("module_brand") or self.brand or ""
        return self.brand or ""

    def get_mppt_index(self) -> str:
        """Retourne l'index MPPT (STRING uniquement)."""
        return self._custom_fields.get("mppt_idx", "")

    # ────────────────────── GÉNÉRATION PAYLOAD YUMAN ──────────────────────

    def to_yuman_update_payload(self) -> Dict[str, Any]:
        """
        Génère le payload pour PATCH /materials/{id} selon le type d'équipement.
        NE contient QUE les champs modifiables par l'API Yuman.
        """
        # Champs standard modifiables (communs à tous)
        payload: Dict[str, Any] = {}

        if self.brand:
            payload["brand"] = self.brand
        if self.serial_number:
            payload["serial_number"] = self.serial_number

        # Champs custom selon la catégorie
        fields = []

        if self.category_id == CAT_STRING:
            # STRING : MPPT index, nombre de modules, marque/modèle module
            fields = [
                {"blueprint_id": 16020, "value": self.get_mppt_index()},
                {"blueprint_id": 16021, "value": str(self.get_nb_modules() or "")},
                {"blueprint_id": 16022, "value": self.get_module_brand()},
                {"blueprint_id": 16023, "value": self.get_module_model()},
            ]

        elif self.category_id == CAT_INVERTER:
            # INVERTER : Inverter ID (Vcom), Modèle
            fields = [
                {"blueprint_id": 13977, "value": self.vcom_device_id or ""},
                {"blueprint_id": 13548, "value": self.get_module_model()},
            ]

        elif self.category_id == CAT_MODULE:
            # MODULE : Modèle uniquement
            fields = [
                {"blueprint_id": 13548, "value": self.get_module_model()},
            ]

        if fields:
            payload["fields"] = fields

        return payload

    # ────────────────────── COMPARAISON INTELLIGENTE ──────────────────────

    def __eq__(self, other: object) -> bool:
        """
        Comparaison intelligente qui vérifie uniquement les champs synchronisables.
        """
        if not isinstance(other, Equipment):
            return NotImplemented

        # 1. Serial number (identifiant unique)
        if self.serial_number != other.serial_number:
            return False

        # 2. Brand (modifiable pour tous)
        if (self.brand or "").lower().strip() != (other.brand or "").lower().strip():
            return False

        # 3. Comparaisons spécifiques par catégorie
        if self.category_id == CAT_STRING:
            # Pour STRING : comparer via les méthodes normalisées
            return (
                self.get_nb_modules() == other.get_nb_modules() and
                self.get_module_model().lower().strip() == other.get_module_model().lower().strip() and
                self.get_module_brand().lower().strip() == other.get_module_brand().lower().strip() and
                self.get_mppt_index() == other.get_mppt_index()
            )

        elif self.category_id == CAT_INVERTER:
            # Pour INVERTER : comparer Modèle via custom fields
            return (
                self.vcom_device_id == other.vcom_device_id and
                self.get_module_model().lower().strip() == other.get_module_model().lower().strip()
            )

        elif self.category_id == CAT_MODULE:
            # Pour MODULE : comparer count et model
            return (
                self.count == other.count and
                self.get_module_model().lower().strip() == other.get_module_model().lower().strip()
            )

        # Pour autres catégories (SIM, PLANT) : juste serial + brand suffit
        return True

    # --- hash cohérent avec __eq__ ----------------------- ③
    def __hash__(self) -> int:
        return hash(self.serial_number)

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
