from __future__ import annotations


# ===============================
# File: vysync/adapters/yuman_adapter.py
# ===============================
"""Snapshot / Patch applicateur côté **Yuman**.

– Toutes les requêtes sortantes sont LOGGÉES en DEBUG avec le
  payload et la réponse (status + extrait JSON).
– Limitation 60 req/min : un décorateur `@rate_limited` gère
  le sleep/back-off + journalise les 429.

Cette classe **n’écrit jamais** en DB ; elle délègue au
SupabaseAdapter passé en paramètre pour propager les
`yuman_site_id` et `yuman_material_id` créés.
"""

import time
import functools
import logging
from typing import Dict, Tuple, List, Optional

from vysync.models import (
    Site, Equipment,
    CAT_INVERTER, CAT_MODULE, CAT_STRING,
)
from vysync.diff import PatchSet, diff_entities
from vysync.app_logging import init_logger
from vysync.yuman_client import YumanClient          # ton client existant
from vysync.adapters.supabase_adapter import SupabaseAdapter

logger = init_logger(__name__)

# ─────────────────────────── Helper anti-429 ────────────────────────────
_REQ_TS: List[float] = []          # timestamps des appels
RATE = 60                          # 60 req/min

def rate_limited(func):
    @functools.wraps(func)
    def wrapper(*a, **k):
        now = time.time()
        _REQ_TS[:] = [t for t in _REQ_TS if now - t < 60]
        if len(_REQ_TS) >= RATE:
            sleep_for = 60 - (now - _REQ_TS[0]) + 0.1
            logger.debug("[YUMAN] quota 60/min atteint → sleep %.1fs", sleep_for)
            time.sleep(sleep_for)
        try:
            return func(*a, **k)
        finally:
            _REQ_TS.append(time.time())
    return wrapper


# ─────────────────────────── Champs Custom Site ─────────────────────────
# Blueprint → DB mapping
SITE_FIELDS = {
    "System Key (Vcom ID)": 13583,
    "Nominal Power (kWc)":  13585,
    "Commission Date":      13586,
}
FIELD_LABELS = {v: k for k, v in SITE_FIELDS.items()}

CUSTOM_INVERTER_ID = "Inverter ID (Vcom)"          # champ custom onduleur


class YumanAdapter:
    def __init__(self, sb_adapter: SupabaseAdapter):
        self.yc = YumanClient()
        self.sb = sb_adapter

    # ------------------------------------------------------------------ #
    # SNAPSHOTS                                                          #
    # ------------------------------------------------------------------ #
    @rate_limited
    def _list_sites(self) -> List[dict]:
        return self.yc.list_sites(embed="fields,client")

    def fetch_sites(self) -> Dict[str, Site]:
        out: Dict[str, Site] = {}
        for s in self._list_sites():
            # custom-fields → dict {label: value}
            cvals = {f["name"]: f.get("value") for f in s.get("_embed", {}).get("fields", [])}
            vcom_key = cvals.get("System Key (Vcom ID)")
            if not vcom_key:
                continue                     # site pas encore mappé → hors périmètre
            out[vcom_key] = Site(
                vcom_system_key=vcom_key,
                name=s.get("name"),
                address=s.get("address"),
                commission_date=cvals.get("Commission Date"),
                nominal_power=float(cvals["Nominal Power (kWc)"]) if cvals.get("Nominal Power (kWc)") else None,
                latitude=s.get("latitude"),
                longitude=s.get("longitude"),
                yuman_site_id=s["id"],
            )
        logger.debug("[YUMAN] snapshot: %s sites", len(out))
        return out

    @rate_limited
    def _list_materials(self) -> List[dict]:
        # embed fields to access custom inverter id
        return self.yc.list_materials(embed="fields,site")

    def fetch_equips(self) -> Dict[Tuple[str, str], Equipment]:
        sites_cache = self.fetch_sites()          # for mapping vcom_system_key
        out: Dict[Tuple[str, str], Equipment] = {}
        for m in self._list_materials():
            s_id = m["site_id"]
            # only consider materials whose site is mapped
            site = next((s for s in sites_cache.values() if s.yuman_site_id == s_id), None)
            if not site:
                continue
            cvals = {f["name"]: f.get("value") for f in m.get("_embed", {}).get("fields", [])}
            vcom_id = cvals.get(CUSTOM_INVERTER_ID) if m["category_id"] == CAT_INVERTER else m["name"]
            if m["category_id"] == CAT_MODULE:
                vcom_id = f"MODULES-{site.vcom_system_key}"
            eq = Equipment(
                vcom_system_key=site.vcom_system_key,
                category_id=m["category_id"],
                eq_type=("inverter" if m["category_id"] == CAT_INVERTER else
                         "module" if m["category_id"] == CAT_MODULE else
                         "string_pv"),
                vcom_device_id=vcom_id,
                name=m.get("name"),
                brand=m.get("brand"),
                model=m.get("model"),
                serial_number=m.get("serial_number"),
                count=m.get("count"),
                yuman_material_id=m["id"],
            )
            out[eq.key()] = eq
        logger.debug("[YUMAN] snapshot: %s equips", len(out))
        return out

    # ------------------------------------------------------------------ #
    # APPLY PATCH – sites                                                #
    # ------------------------------------------------------------------ #
    def apply_sites_patch(self, db_sites: Dict[str, Site]):
        """
        Objectif : pousser dans Yuman les données manquantes ou divergentes
        par rapport à la DB (la DB est ici la *source de vérité*).
        """
        y_sites = self.fetch_sites()
        patch = diff_entities(y_sites, db_sites)

        # ADD ► create_site + écrire yuman_site_id en DB
        for s in patch.add:
            payload = {
                "name": s.name,
                "address": s.address or "",
                "fields": [
                    {"blueprint_id": SITE_FIELDS["System Key (Vcom ID)"], "name": "System Key (Vcom ID)",  "value": s.vcom_system_key},
                    {"blueprint_id": SITE_FIELDS["Nominal Power (kWc)"],  "name": "Nominal Power (kWc)",   "value": s.nominal_power},
                    {"blueprint_id": SITE_FIELDS["Commission Date"],       "name": "Commission Date",       "value": s.commission_date},
                ],
            }
            logger.debug("[YUMAN] create_site payload=%s", payload)
            new_site = self.yc.create_site(payload)
            # propagate id in DB
            self.sb.sb.table("sites_mapping").update({"yuman_site_id": new_site["id"]}).eq("vcom_system_key", s.vcom_system_key).execute()

        # UPDATE ► uniquement via custom-fields (pas de renommage massif)
        for old, new in patch.update:
            fields_patch = []
            if old.nominal_power != new.nominal_power and new.nominal_power is not None:
                fields_patch.append({
                    "blueprint_id": SITE_FIELDS["Nominal Power (kWc)"],
                    "name": "Nominal Power (kWc)",
                    "value": new.nominal_power,
                })
            if old.commission_date != new.commission_date and new.commission_date:
                fields_patch.append({
                    "blueprint_id": SITE_FIELDS["Commission Date"],
                    "name": "Commission Date",
                    "value": new.commission_date,
                })
            if fields_patch:
                logger.debug("[YUMAN] update_site %s fields=%s", old.yuman_site_id, fields_patch)
                self.yc.update_site(old.yuman_site_id, {"fields": fields_patch})

    # ------------------------------------------------------------------ #
    # APPLY PATCH – equipments                                           #
    # ------------------------------------------------------------------ #
    def apply_equips_patch(self, db_equips: Dict[Tuple[str, str], Equipment]):
        y_equips = self.fetch_equips()
        patch = diff_entities(y_equips, db_equips)

        # ADD ► create_material + update yuman_material_id in DB
        for e in patch.add:
            site_row = self.sb.sb.table("sites_mapping").select("yuman_site_id").eq("vcom_system_key", e.vcom_system_key).single().execute().data
            if not site_row or not site_row["yuman_site_id"]:
                logger.warning("Site %s sans yuman_site_id, skip equip creation", e.vcom_system_key)
                continue
            payload = {
                "site_id": site_row["yuman_site_id"],
                "name": e.name,
                "category_id": e.category_id,
                "brand": e.brand,
                "model": e.model,
                "serial_number": e.serial_number,
            }
            if e.category_id == CAT_INVERTER:
                payload["fields"] = [{"name": CUSTOM_INVERTER_ID, "value": e.vcom_device_id}]
            logger.debug("[YUMAN] create_material payload=%s", payload)
            mat = self.yc.create_material(payload)
            # propagate id
            self.sb.sb.table("equipments_mapping").update({"yuman_material_id": mat["id"]}).eq("vcom_device_id", e.vcom_device_id).eq("vcom_system_key", e.vcom_system_key).execute()

        # UPDATE ► pour onduleurs : champ Inverter-ID + modèle si absent
        for old, new in patch.update:
            fields_patch = []
            if old.category_id == CAT_INVERTER and old.vcom_device_id != new.vcom_device_id:
                fields_patch.append({"name": CUSTOM_INVERTER_ID, "value": new.vcom_device_id})
            if not old.model and new.model:
                payload = {"model": new.model}
            else:
                payload = {}
            if fields_patch:
                payload["fields"] = fields_patch
            if payload:
                logger.debug("[YUMAN] update_material %s payload=%s", old.yuman_material_id, payload)
                self.yc.update_material(old.yuman_material_id, payload)
