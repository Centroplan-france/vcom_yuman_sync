from __future__ import annotations

# ===============================
# File: vysync/diff.py
# ===============================
"""Fonctions génériques de comparaison entre deux snapshots.
Chaque snapshot est un ``dict[key -> Entity]``.  
Le résultat est un PatchSet (add, update, delete) sérialisable.
"""

from dataclasses import asdict
from typing import Dict, Generic, List, Tuple, TypeVar, NamedTuple

T = TypeVar("T")


class PatchSet(NamedTuple):
    add: List[T]
    update: List[Tuple[T, T]]  # (old, new)
    delete: List[T]

    def is_empty(self) -> bool:
        return not (self.add or self.update or self.delete)


def diff_entities(current: Dict[Any, T], target: Dict[Any, T]) -> PatchSet[T]:
    add, upd, delete = [], [], []
    for k, tgt in target.items():
        cur = current.get(k)
        if cur is None:
            add.append(tgt)
        elif asdict(cur) != asdict(tgt):  # comparaison champ à champ
            upd.append((cur, tgt))
    for k, cur in current.items():
        if k not in target:
            delete.append(cur)
    return PatchSet(add, upd, delete)
