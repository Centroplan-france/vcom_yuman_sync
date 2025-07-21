from __future__ import annotations

# ===============================
# File: vysync/diff.py
# ===============================
"""Fonctions génériques de comparaison entre deux snapshots.
Chaque snapshot est un ``dict[key -> Entity]``.  
Le résultat est un PatchSet (add, update, delete) sérialisable.
"""

from dataclasses import asdict, is_dataclass
from typing import Any, Dict, Generic, List, Tuple, TypeVar, NamedTuple
from vysync.models import Site, Equipment
from dateutil.parser import isoparse
from datetime import datetime

T = TypeVar("T")

class PatchSet(NamedTuple, Generic[T]):
    add: List[T]
    update: List[Tuple[T, T]]  # (old, new)
    delete: List[T]

    def is_empty(self) -> bool:
        return not (self.add or self.update or self.delete)


def _equals(a: T, b: T) -> bool:
    """Égalité ‘profonde’ compatible dataclass/non-dataclass."""
    if is_dataclass(a) and is_dataclass(b):
        if isinstance(a, Site) and isinstance(b, Site):
            da, db = asdict(a), asdict(b)
            for d in (da, db):
                d.pop("yuman_site_id", None)
                d.pop("ignore_site", None)

                # normaliser commission_date  (ex. 29/04/2025 → 2025-04-29)
                cd = d.get("commission_date")
                if cd and "/" in cd:
                    j, m, a = cd.split("/")[:3]
                    d["commission_date"] = f"{a}-{m.zfill(2)}-{j.zfill(2)}"


                # arrondir puissance
                np = d.get("nominal_power")
                if np is not None:
                    d["nominal_power"] = round(float(np), 2)

                # strip adresse
                adr = d.get("address")
                if adr:
                    d["address"] = adr.strip()

                # normaliser lat & long à 5 décimales
                lat = d.get("latitude")
                lng = d.get("longitude")
                if lat is not None:
                    d["latitude"]  = round(float(lat), 5)
                if lng is not None:
                    d["longitude"] = round(float(lng), 5)

            return da == db
        
        if isinstance(a, Equipment) and isinstance(b, Equipment):
            return _equip_equals(a, b)
    return a == b

def _equip_equals(a: Equipment, b: Equipment) -> bool:
    # Normalisation commune
    for d in (a, b):
        if d.brand is None:  d.brand  = ""
        if d.model is None:  d.model  = ""
        if d.serial_number is None: d.serial_number = ""
        if d.count in (None, ""): d.count = 0

    # Dispatcher par category_id
    if a.category_id == CAT_MODULE:
        # On compare seulement (brand, model, serial_number, parent_id, count)
        return (
            a.brand.lower()  == b.brand.lower() and
            a.model.lower()  == b.model.lower() and
            a.serial_number == b.serial_number and
            (a.parent_id or "") == (b.parent_id or "") and
            int(a.count) == int(b.count)
        )
    elif a.category_id == CAT_STRING:
        # Ici on veut que parent_id et fields MPPT soient identiques
        return (
            (a.parent_id or "") == (b.parent_id or "") and
            a.serial_number == b.serial_number
        )
    elif a.category_id == CAT_INVERTER:
        return (
            a.serial_number == b.serial_number and
            a.model.lower() == b.model.lower()
        )
    else:
        # Fall‑back : tout comparer sauf yuman_material_id
        da, db = asdict(a), asdict(b)
        da.pop("yuman_material_id", None); db.pop("yuman_material_id", None)
        return da == db


def diff_entities(
    current: Dict[Any, T],
    target: Dict[Any, T],
) -> PatchSet[T]:
    add: List[T] = []
    upd: List[Tuple[T, T]] = []
    delete: List[T] = []

    # Inserts & updates
    for k, tgt in target.items():
        cur = current.get(k)
        if cur is None:
            add.append(tgt)
        elif not _equals(cur, tgt):
            upd.append((cur, tgt))

    # Deletions
    for k, cur in current.items():
        if k not in target:
            delete.append(cur)

    return PatchSet(add, upd, delete)

# ---------------------------------------------------------------------
#  FILL‑MISSING : ne complète QUE les cases vides de la DB
# ---------------------------------------------------------------------
def _is_missing(v: Any) -> bool:
    """
    Renvoie True si la valeur est « vide » :
      – None
      – chaîne vide ou uniquement espaces
      – 0 (entier ou float)
    """
    if v is None:
        return True
    if isinstance(v, (str, bytes)) and str(v).strip() == "":
        return True
    if isinstance(v, (int, float)) and v == 0:
        return True
    return False


def diff_fill_missing(
    db_snapshot: Dict[Any, T],       # ce qu’il y a déjà en base
    src_snapshot: Dict[Any, T],      # ce que Yuman (ou autre) nous fournit
) -> PatchSet[T]:
    """
    Compare *champ par champ* ; on ne signale qu’un **ADD** ou un **UPDATE**
    lorsqu’au moins un champ est manquant dans la DB **et** présent dans
    le snapshot source.

    - Jamais de DELETE ⇒ patch.delete sera toujours vide.
    - La structure PatchSet est conservée pour ré‑utiliser les
      apply_*_patch existants ; ils ignoreront simplement la partie delete.
    """
    add: List[T] = []
    upd: List[Tuple[T, T]] = []

    for k, src in src_snapshot.items():
        db_obj = db_snapshot.get(k)

        # --- ligne totalement absente en DB -> ADD --------------------
        if db_obj is None:
            add.append(src)
            continue

        # --- champ manquant dans la DB ? ------------------------------
        if is_dataclass(src) and is_dataclass(db_obj):
            d_db  = asdict(db_obj)
            d_src = asdict(src)

            need_update = any(
                _is_missing(d_db[col]) and not _is_missing(d_src[col])
                for col in d_db
            )
        else:
            # Sur des objets simples / scalaires on ne fait rien
            need_update = False

        if need_update:
            upd.append((db_obj, src))

    return PatchSet(add=add, update=upd, delete=[])  # delete toujours vide
