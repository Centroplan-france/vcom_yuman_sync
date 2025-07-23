from __future__ import annotations

# ===============================
# File: vysync/diff.py
# ===============================
"""Fonctions génériques de comparaison entre deux snapshots.
Chaque snapshot est un ``dict[key -> Entity]``.  
Le résultat est un PatchSet (add, update, delete) sérialisable.
"""

from dataclasses import asdict, is_dataclass
from typing import Any, Dict, Generic, List, Tuple, TypeVar, NamedTuple, Optional, Set
from vysync.models import Site, Equipment, CAT_MODULE, CAT_STRING, CAT_INVERTER, CAT_CENTRALE, CAT_SIM
from dateutil.parser import isoparse
from datetime import datetime
import logging
import re
from vysync.app_logging import init_logger
logger = init_logger(__name__)
logger.setLevel(logging.DEBUG)


T = TypeVar("T")

class PatchSet(NamedTuple, Generic[T]):
    add: List[T]
    update: List[Tuple[T, T]]  # (old, new)
    delete: List[T]

    def is_empty(self) -> bool:
        return not (self.add or self.update or self.delete)


def _equals(a: T, b: T, ignore_fields: Optional[set[str]] = None) -> bool:
    """Égalité ‘profonde’ compatible dataclass/non-dataclass."""
    if is_dataclass(a) and is_dataclass(b):
        if isinstance(a, Site) and isinstance(b, Site):
            da, db = asdict(a), asdict(b)
            for d in (da, db):
                if ignore_fields:
                    for field in ignore_fields:
                        d.pop(field, None)

                # normaliser name   clean_new_name = re.sub(r'^\d+\s+|\s*\(.*?\)| France', '', new.name)
                n = d.get("name")
                if n is not None:
                    d["name"] = re.sub(r'^\d+\s+|\s*\(.*?\)| France', '', n)

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
            return _equip_equals(a, b, ignore_fields=ignore_fields)
    return a == b

def _equip_equals(a: Equipment, b: Equipment, ignore_fields: Optional[Set[str]] = None) -> bool:
    da = a.to_dict()
    db = b.to_dict()

    # Retirer les champs à ignorer
    if ignore_fields:
        for field in ignore_fields:
            da.pop(field, None)
            db.pop(field, None)

    # Normalisation
    for d in (da, db):
        for key in ("brand", "model", "serial_number", "parent_id"):
            if d.get(key) is None:
                d[key] = ""
            elif isinstance(d[key], str):
                d[key] = d[key].strip()
        d["count"] = int(d.get("count") or 0)

    cat = da.get("category_id")

    if cat == CAT_MODULE:
        return (
            da["brand"].lower()       == db["brand"].lower() and
            da["model"].lower()       == db["model"].lower() and
            da["count"]               == db["count"] and
            da["serial_number"]       == db["serial_number"]
        )
    elif cat == CAT_STRING:
        return (
            da["name"]                == db["name"] and
            da["brand"].lower()       == db["brand"].lower() and
            da["model"].lower()       == db["model"].lower() and
            da["count"]               == db["count"] and
            da["name"]                == db["name"] and
            da["vcom_device_id"]      == db["vcom_device_id"] and
            da["parent_id"]           == db["parent_id"] and
            da["serial_number"]       == db["serial_number"]
        )
    elif cat == CAT_INVERTER:
        return (
            da["name"]                == db["name"] and
            da["brand"].lower()       == db["brand"].lower() and
            da["model"].lower()       == db["model"].lower() and
            da["serial_number"]       == db["serial_number"] and
            da["vcom_device_id"]      == db["vcom_device_id"] 
        )
    elif cat == CAT_CENTRALE:
        return (
            da["name"]                == db["name"]            
        )
    elif cat == CAT_SIM:
        return (
            da["name"]                == db["name"] and
            da["brand"].lower()       == db["brand"].lower() and
            da["model"].lower()       == db["model"].lower() and
            da["serial_number"]       == db["serial_number"] and
            da["vcom_device_id"]      == db["vcom_device_id"] 
        )
    else:
        return da == db



def diff_entities(
    current: Dict[Any, T],
    target: Dict[Any, T],
    ignore_fields: Optional[Set[str]] = None,
) -> PatchSet[T]:
    add: List[T] = []
    upd: List[Tuple[T, T]] = []
    delete: List[T] = []

    for k, tgt in target.items():
        cur = current.get(k)
        if cur is None:
            logger.debug("AJOUT (clé=%s) cible=%r", k, tgt)
            add.append(tgt)
        elif not _equals(cur, tgt, ignore_fields=ignore_fields):
            logger.debug("MISE À JOUR (clé=%s) → actuel=%r, cible=%r", k, cur, tgt)
            upd.append((cur, tgt))

    for k, cur in current.items():
        if k not in target:
            logger.debug("SUPPRESSION (clé=%s) actuel=%r", k, cur)
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
    db_snapshot: Dict[Any, T],
    src_snapshot: Dict[Any, T],
    *,
    fields: Optional[List[str]] = None,
    skip_categories: Optional[List[int]] = None,
    skip_obsolete: bool = False,
    category_field_exclusions: Optional[Dict[int, List[str]]] = None,
) -> PatchSet[T]:
    """
    Complète uniquement les champs vides spécifiés, en pouvant :
      – ignorer toute catégorie (ex. modules),
      – sauter les objets marqués obsolètes,
      – retirer certains champs du check pour certaines catégories.
    """
    # 1) paramètres par défaut
    to_check_base = fields or [
        "brand", "model", "serial_number", "count",
        "mppt_idx", "module_brand", "module_model",
    ]
    skip_cats = set(skip_categories or [])
    excl_map  = category_field_exclusions or {}

    add: List[T] = []
    upd: List[Tuple[T, T]] = []

    for key, src in src_snapshot.items():
        # 2) skip obsolètes
        if skip_obsolete and getattr(src, "is_obsolete", False):
            continue

        # 3) skip catégories entières (modules, etc.)
        cat = getattr(src, "category_id", None)
        if cat in skip_cats:
            continue

        db_obj = db_snapshot.get(key)

        # 4) ligne absente → ADD
        if db_obj is None:
            add.append(src)
            continue

        # 5) build la liste de champs à checker pour cet objet
        to_check = list(to_check_base)
        for c in excl_map.get(cat, []):
            if c in to_check:
                to_check.remove(c)

        # 6) comparaison ciblée
        if is_dataclass(src) and is_dataclass(db_obj):
            d_db  = asdict(db_obj)
            d_src = asdict(src)

            missing = [
                f for f in to_check
                if _is_missing(d_db.get(f)) and not _is_missing(d_src.get(f))
            ]
            if missing:
                logger.debug(
                    "MISE À JOUR (clé=%s) champs manquants=%s | actuel=%r | cible=%r",
                    key, ", ".join(missing), db_obj, src
                )
                upd.append((db_obj, src))

    return PatchSet(add=add, update=upd, delete=[])  # on ne supprime jamais