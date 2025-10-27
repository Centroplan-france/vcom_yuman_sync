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
import logging
import re

logger = logging.getLogger(__name__)


def _format_diff(old: Any, new: Any) -> str:
    """
    Formate les différences entre deux objets en affichant uniquement les champs modifiés.

    Format: champ: {ancienne_valeur -> nouvelle_valeur}; champ2: {ancienne -> nouvelle}
    """
    if not (is_dataclass(old) and is_dataclass(new)):
        return f"{old!r} -> {new!r}"

    old_dict = asdict(old)
    new_dict = asdict(new)

    changes = []
    for key in old_dict.keys():
        old_val = old_dict.get(key)
        new_val = new_dict.get(key)

        # Ignorer si les valeurs sont identiques
        if old_val == new_val:
            continue

        # Formater la différence
        changes.append(f"{key}: {{{old_val!r} -> {new_val!r}}}")

    if not changes:
        return "(aucun changement détecté)"

    return "; ".join(changes)


T = TypeVar("T")

class PatchSet(NamedTuple, Generic[T]):
    add: List[T]
    update: List[Tuple[T, T]]  # (old, new)
    delete: List[T]

    def is_empty(self) -> bool:
        return not (self.add or self.update or self.delete)

_parent_map: Dict[str, int] = {}

def set_parent_map(mapping: Dict[str, int]) -> None:
    """
    Fournit le mapping { vcom_device_id: yuman_material_id }
    utilisable dans _equip_equals pour normaliser parent_id.
    """
    global _parent_map
    _parent_map = mapping


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

            # ✅ RÈGLE MÉTIER : Si target (b/db) a None, ignorer ce champ dans la comparaison
            for key in list(db.keys()):
                if db[key] is None and key in da:
                    db[key] = da[key]

            return da == db
        
        if isinstance(a, Equipment) and isinstance(b, Equipment):
            return _equip_equals(a, b, ignore_fields=ignore_fields)
    return a == b

def _equip_equals(a: Equipment, b: Equipment, ignore_fields: Optional[Set[str]] = None) -> bool:
    """
    Comparaison d'équipements - délègue à Equipment.__eq__().
    Cette fonction est conservée pour compatibilité.

    Note: ignore_fields n'est plus utilisé car la logique de comparaison
    est maintenant dans Equipment.__eq__() qui compare uniquement les champs synchronisables.
    """
    return a == b



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
            logger.debug("AJOUT (clé=%s)", k)
            add.append(tgt)
        elif not _equals(cur, tgt, ignore_fields=ignore_fields):
            logger.debug("MISE À JOUR (clé=%s) → %s", k, _format_diff(cur, tgt))
            upd.append((cur, tgt))

    for k, cur in current.items():
        if k not in target:
            logger.debug("SUPPRESSION (clé=%s)", k)
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


def _serial_key(s: str | None) -> str:
    return (s or "").strip().upper()

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
    Complète uniquement les champs vides spécifiés, avec requalification
    ADD→UPDATE si on retrouve l'objet par serial_number ou yuman_material_id.
    Refuse les ADD avec serial vide.
    """
    # 0) index secondaires (indépendants de la clé 'key' du dict)
    db_by_serial = {
        _serial_key(getattr(v, "serial_number", None)): v
        for v in db_snapshot.values()
        if getattr(v, "serial_number", None)
    }
    db_by_mid = {
        getattr(v, "yuman_material_id"): v
        for v in db_snapshot.values()
        if getattr(v, "yuman_material_id", None) is not None
    }

    # 1) paramètres
    to_check_base = fields or [
        "brand", "model", "serial_number", "count",
        "mppt_idx", "module_brand", "module_model"
    ]
    skip_cats = set(skip_categories or [])
    excl_map  = category_field_exclusions or {}

    add: List[T] = []
    upd: List[Tuple[T, T]] = []

    for key, src in src_snapshot.items():
        # 2) obsolètes
        if skip_obsolete and getattr(src, "is_obsolete", False):
            continue

        # 3) catégories à ignorer
        cat = getattr(src, "category_id", None)
        if cat in skip_cats:
            continue

        db_obj = db_snapshot.get(key)

        # 4) ligne absente sous la clé → tenter une requalification
        if db_obj is None:
            sk  = _serial_key(getattr(src, "serial_number", None))
            mid = getattr(src, "yuman_material_id", None)

            # 4.a) serial vide → on NE créé PAS (sinon collision et incohérence)
            if not sk:
                logger.warning(
                    "diff_fill_missing: ADD SKIPPED (serial vide) key=%r src=%r",
                    key, src
                )
                continue

            # 4.b) trouvé en DB par yuman_material_id → UPDATE
            if mid is not None and mid in db_by_mid:
                logger.debug(
                    "diff_fill_missing: REQUALIFY ADD→UPDATE via yuman_material_id=%r | key=%r",
                    mid, key
                )
                upd.append((db_by_mid[mid], src))
                continue

            # 4.c) trouvé en DB par serial → UPDATE
            if sk in db_by_serial:
                logger.debug(
                    "diff_fill_missing: REQUALIFY ADD→UPDATE via serial=%r | key=%r",
                    sk, key
                )
                upd.append((db_by_serial[sk], src))
                continue

            # 4.d) vraiment nouveau → ADD
            add.append(src)
            continue

        # 5) champs à vérifier (fill-missing)
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
                    "MISE À JOUR (clé=%s) champs manquants=[%s] → %s",
                    key, ", ".join(missing), _format_diff(db_obj, src)
                )
                upd.append((db_obj, src))

    return PatchSet(add=add, update=upd, delete=[])  # jamais de delete ici
