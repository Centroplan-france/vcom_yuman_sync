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
            da = asdict(a); db = asdict(b)
            da.pop("yuman_site_id", None); db.pop("yuman_site_id", None)
            return da == db
        if isinstance(a, Equipment) and isinstance(b, Equipment):
            da = asdict(a); db = asdict(b)
            da.pop("yuman_material_id", None); db.pop("yuman_material_id", None)
            return da == db
        return asdict(a) == asdict(b)
    return a == b


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
