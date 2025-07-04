# ===============================
# File: vysync/models.py
# ===============================
"""Dataclasses représentant les entités métier (Site, Equipment, Client).
Toutes les structures exposent une méthode ``key`` permettant d'obtenir la
clé unique de comparaison et un ``to_dict`` pour la sérialisation.  
Elles sont *hashable* et ordre‑indépendantes, ce qui facilite le diff.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class Site:
    vcom_system_key: str
    name: str
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    nominal_power: Optional[float] = None
    commission_date: Optional[str] = None  # ISO
    address: Optional[str] = None

    yuman_site_id: Optional[int] = None  # renseigné après mapping

    def key(self) -> str:  # unique id pour le diff
        return self.vcom_system_key

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class Equipment:
    site_key: str  # FK logique vers Site.key()
    category_id: int
    eq_type: str
    vcom_device_id: str
    name: str
    brand: Optional[str] = None
    model: Optional[str] = None
    serial_number: Optional[str] = None
    count: Optional[int] = None
    parent_vcom_id: Optional[str] = None  # pour STRING -> parent inverter

    yuman_material_id: Optional[int] = None

    def key(self) -> tuple[str, str]:  # (site, vcom_device_id)
        return (self.site_key, self.vcom_device_id)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class Client:
    yuman_client_id: int
    code: Optional[str]
    name: str

    def key(self) -> int:
        return self.yuman_client_id

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

# ---------------------------------------------------------------------------------
# Convenience enum-like constants centralised here to avoid desynchronisation.
# ---------------------------------------------------------------------------------
CAT_INVERTER = 11102
CAT_MODULE = 11103
CAT_STRING = 12404
CAT_CENTRALE = 11441

# ===============================
# File: vysync/diff.py
# ===============================
"""Fonctions génériques de comparaison entre deux snapshots.
Chaque snapshot est un ``dict[key -> Entity]``.  
Le résultat est un PatchSet (add, update, delete) sérialisable.
"""
from __future__ import annotations
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

# ===============================
# File: vysync/adapters/supabase_adapter.py
# ===============================
"""Accès Supabase encapsulé → snapshot + patch apply.
Toutes les requêtes passent par cette classe afin de garder un seul point
pour la journalisation / debug.
"""
from __future__ import annotations

import os
from typing import Dict, List
from supabase import create_client, Client as SupabaseClient

from vysync.app_logging import init_logger
from vysync.models import Site, Equipment, Client, CAT_INVERTER, CAT_MODULE, CAT_STRING

logger = init_logger(__name__)

SITE_TABLE = "sites_mapping"
EQUIP_TABLE = "equipments_mapping"
CLIENT_TABLE = "clients_mapping"


class SupabaseAdapter:
    """Thin wrapper around supabase‑py allowing snapshot/diff style."""

    def __init__(self):
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_SERVICE_KEY")
        if not url or not key:
            raise EnvironmentError("SUPABASE_URL or SUPABASE_SERVICE_KEY missing")
        self.sb: SupabaseClient = create_client(url, key)

    # ---------------------------------------------------------------------
    # SNAPSHOTS
    # ---------------------------------------------------------------------
    def fetch_sites(self) -> Dict[str, Site]:
        rows = self.sb.table(SITE_TABLE).select("*").execute().data or []
        out: Dict[str, Site] = {}
        for r in rows:
            if not r.get("vcom_system_key"):
                continue  # hors périmètre
            out[r["vcom_system_key"]] = Site(
                vcom_system_key=r["vcom_system_key"],
                name=r.get("name") or r["vcom_system_key"],
                latitude=r.get("latitude"),
                longitude=r.get("longitude"),
                nominal_power=r.get("nominal_power"),
                commission_date=r.get("commission_date"),
                address=r.get("address"),
                yuman_site_id=r.get("yuman_site_id"),
            )
        logger.debug("[SB] fetched %s sites", len(out))
        return out

    def fetch_equipments(self) -> Dict[tuple[str, str], Equipment]:
        rows = (
            self.sb.table(EQUIP_TABLE)
            .select("*")
            .in_("category_id", [CAT_INVERTER, CAT_MODULE, CAT_STRING])
            .execute()
            .data
            or []
        )
        out: Dict[tuple[str, str], Equipment] = {}
        for r in rows:
            k = (r["vcom_system_key"], r["vcom_device_id"])
            out[k] = Equipment(
                site_key=r["vcom_system_key"],
                category_id=r["category_id"],
                eq_type=r["eq_type"],
                vcom_device_id=r["vcom_device_id"],
                name=r["name"],
                brand=r.get("brand"),
                model=r.get("model"),
                serial_number=r.get("serial_number"),
                count=r.get("count"),
                parent_vcom_id=r.get("parent_vcom_id"),
                yuman_material_id=r.get("yuman_material_id"),
            )
        logger.debug("[SB] fetched %s equipments", len(out))
        return out

    # ---------------------------------------------------------------------
    # APPLY PATCHES (simplifié pour Add/Update, ignore Delete par sécurité)
    # ---------------------------------------------------------------------
    def apply_sites_patch(self, patch):
        for s in patch.add:
            logger.debug("[SB] INSERT site %s", s.key())
            self.sb.table(SITE_TABLE).insert([s.to_dict()]).execute()
        for old, new in patch.update:
            logger.debug("[SB] UPDATE site %s", new.key())
            self.sb.table(SITE_TABLE).update(new.to_dict()).eq("vcom_system_key", new.key()).execute()

    def apply_equips_patch(self, patch):
        for e in patch.add:
            logger.debug("[SB] INSERT equip %s", e.key())
            self.sb.table(EQUIP_TABLE).insert([e.to_dict()]).execute()
        for old, new in patch.update:
            logger.debug("[SB] UPDATE equip %s", new.key())
            self.sb.table(EQUIP_TABLE).update(new.to_dict()).eq("vcom_device_id", new.vcom_device_id).eq("vcom_system_key", new.site_key).execute()

# ===============================
# File: vysync/adapters/vcom_adapter.py
# ===============================
"""Transforme la sortie de VCOMAPIClient en snapshot ``Site`` / ``Equipment``."""
from __future__ import annotations

from typing import Dict
from vysync.models import Site, Equipment, CAT_INVERTER, CAT_MODULE, CAT_STRING
from vysync.vcom_client import VCOMAPIClient  # réutilise ton client existant
from vysync.app_logging import init_logger

logger = init_logger(__name__)


def fetch_snapshot(vc: VCOMAPIClient) -> tuple[Dict[str, Site], Dict[tuple[str, str], Equipment]]:
    sites: Dict[str, Site] = {}
    equips: Dict[tuple[str, str], Equipment] = {}

    for sys in vc.get_systems():
        key = sys["key"]
        tech = vc.get_technical_data(key)
        det = vc.get_system_details(key)

        site = Site(
            vcom_system_key=key,
            name=sys.get("name") or key,
            latitude=det.get("coordinates", {}).get("latitude"),
            longitude=det.get("coordinates", {}).get("longitude"),
            nominal_power=tech.get("nominalPower"),
            commission_date=det.get("commissionDate"),
            address=det.get("address", {}).get("street"),
        )
        sites[site.key()] = site

        # Modules (on suppose une seule référence)
        panels = tech.get("panels") or []
        if panels:
            p = panels[0]
            mod = Equipment(
                site_key=key,
                category_id=CAT_MODULE,
                eq_type="module",
                vcom_device_id=f"MODULES-{key}",
                name=p.get("model") or "Modules",
                brand=p.get("vendor"),
                model=p.get("model"),
                count=p.get("count"),
            )
            equips[mod.key()] = mod

        # Onduleurs
        for inv in vc.get_inverters(key):
            det_inv = vc.get_inverter_details(key, inv["id"])
            inv_eq = Equipment(
                site_key=key,
                category_id=CAT_INVERTER,
                eq_type="inverter",
                vcom_device_id=inv["id"],
                name=inv.get("name") or inv["id"],
                brand=det_inv.get("vendor"),
                model=det_inv.get("model"),
                serial_number=inv.get("serial"),
            )
            equips[inv_eq.key()] = inv_eq

        # TODO : strings si besoin
    logger.info("[VCOM] snapshot: %s sites, %s equips", len(sites), len(equips))
    return sites, equips

# ===============================
# File: vysync/cli.py
# ===============================
"""Entry‑point: récupère snapshots VCOM & DB, calcule diff, applique, puis récap."""
import argparse
from vysync.adapters.vcom_adapter import fetch_snapshot as vcom_snapshot
from vysync.adapters.supabase_adapter import SupabaseAdapter
from vysync.vcom_client import VCOMAPIClient
from vysync.diff import diff_entities
from vysync.app_logging import init_logger

logger = init_logger(__name__)

def main():
    parser = argparse.ArgumentParser(description="Sync VCOM → Supabase (snapshot/diff)")
    args = parser.parse_args()

    vc = VCOMAPIClient()
    sb = SupabaseAdapter()

    # 1. snapshots
    v_sites, v_equips = vcom_snapshot(vc)
    db_sites = sb.fetch_sites()
    db_equips = sb.fetch_equipments()

    # 2. diff
    patch_sites = diff_entities(db_sites, v_sites)
    patch_equips = diff_entities(db_equips, v_equips)

    logger.info("Sites Δ: +%s / ~%s / -%s", len(patch_sites.add), len(patch_sites.update), len(patch_sites.delete))
    logger.info("Equips Δ: +%s / ~%s / -%s", len(patch_equips.add), len(patch_equips.update), len(patch_equips.delete))

    # 3. apply patches
    sb.apply_sites_patch(patch_sites)
    sb.apply_equips_patch(patch_equips)

    logger.info("✅ DB updated successfully")

if __name__ == "__main__":
    main()
