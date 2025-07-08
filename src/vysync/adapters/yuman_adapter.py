#!/usr/bin/env python3
from __future__ import annotations
"""
File: vysync/adapters/yuman_adapter.py

Synchronisation Yuman ⇄ Supabase / VCOM.

– Toutes les requêtes HTTP sont journalisées en DEBUG.
– Limite d’API : 60 req/min. Le décorateur `@rate_limited`
  protège **toutes** les méthodes réseau (GET, POST, PATCH).
– La source de vérité reste la base Supabase ; Yuman est créé /
  mis à jour si nécessaire. Les IDs Yuman générés sont ensuite
  réinjectés en base.
"""

from typing import Dict, List, Tuple, Optional
import functools
import logging
import time

from vysync.app_logging import init_logger, _dump
from vysync.diff import PatchSet, diff_entities
from vysync.models import (
    Site,
    Equipment,
    CAT_INVERTER,
    CAT_MODULE,
    CAT_STRING,
)
from vysync.yuman_client import YumanClient               # client HTTP existant
from vysync.adapters.supabase_adapter import SupabaseAdapter

# ────────────────────────────── Logging ────────────────────────────────
logger = init_logger(__name__)

# ───────────────────── Anti-429 : limite 60 requêtes / min ─────────────
_RATE_LIMIT = 60                                # 60 req/min
_REQ_TS: List[float] = []                       # timestamps des requêtes réussies


def rate_limited(func):
    """Décorateur : pause automatique si la cadence dépasse 60 req/min."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        now = time.time()
        # conserve les requêtes des 60 dernières secondes
        _REQ_TS[:] = [t for t in _REQ_TS if now - t < 60]

        if len(_REQ_TS) >= _RATE_LIMIT:
            sleep_for = 60 - (now - _REQ_TS[0]) + 0.1
            logger.debug("[YUMAN] quota atteint → sleep %.1f s", sleep_for)
            time.sleep(sleep_for)

        try:
            return func(*args, **kwargs)
        finally:
            _REQ_TS.append(time.time())

    return wrapper


# ───────────────────────── Blueprints custom fields ────────────────────
SITE_FIELDS = {
    "System Key (Vcom ID)": 13583,
    "Nominal Power (kWc)":  13585,
    "Commission Date":      13586,
}

STRING_FIELDS = {
    "MPPT index":         16020,
    "nombre de module":   16021,
    "marque du module":   16022,
    "model de module":    16023,
}

CUSTOM_INVERTER_ID = "Inverter ID (Vcom)"       # champ custom onduleur

# ───────────────────────────── Adapter Yuman ───────────────────────────
class YumanAdapter:
    def __init__(self, sb_adapter: SupabaseAdapter) -> None:
        self.yc = YumanClient()
        self.sb = sb_adapter            # accès indirect à Supabase

    # ------------------------------------------------------------------ #
    #  Low-level : wrappers décorés                                       #
    # ------------------------------------------------------------------ #
    @rate_limited
    def _list_sites(self) -> List[dict]:
        return self.yc.list_sites(embed="fields,client")

    @rate_limited
    def _list_materials(self) -> List[dict]:
        return self.yc.list_materials(embed="fields,site")

    @rate_limited
    def _create_site(self, payload: dict) -> dict:
        return self.yc.create_site(payload)

    @rate_limited
    def _update_site(self, site_id: int, payload: dict) -> dict:
        return self.yc.update_site(site_id, payload)

    @rate_limited
    def _create_material(self, payload: dict) -> dict:
        return self.yc.create_material(payload)

    @rate_limited
    def _update_material(self, mat_id: int, payload: dict) -> dict:
        return self.yc.update_material(mat_id, payload)

    # ------------------------------------------------------------------ #
    #  SNAPSHOTS                                                          #
    # ------------------------------------------------------------------ #
    def fetch_sites(self) -> Dict[str, Site]:
        """Retourne tous les sites Yuman mappés (clef : vcom_system_key)."""
        sites: Dict[str, Site] = {}

        for s in self._list_sites():
            cvals = {f["name"]: f.get("value")             # custom fields
                     for f in s.get("_embed", {}).get("fields", [])}
            vcom_key = cvals.get("System Key (Vcom ID)")
            if not vcom_key:                               # site non mappé
                continue

            sites[vcom_key] = Site(
                vcom_system_key=vcom_key,
                name=s.get("name"),
                address=s.get("address"),
                commission_date=cvals.get("Commission Date"),
                nominal_power=float(cvals["Nominal Power (kWc)"])
                              if cvals.get("Nominal Power (kWc)") else None,
                latitude=s.get("latitude"),
                longitude=s.get("longitude"),
                yuman_site_id=s["id"],
            )

        logger.debug("[YUMAN] snapshot: %s sites", len(sites))
        _dump("[YUMAN] snapshot sites", sites)
        return sites

    # ------------------------------------------------------------------ #
    #  LECTURE DES ÉQUIPEMENTS YUMAN                                      #
    # ------------------------------------------------------------------ #
    def fetch_equips(self) -> Dict[str, Equipment]:
        """
        Charge tous les matériels (modules, onduleurs, strings) rattachés
        aux sites déjà mappés. Clef retournée : vcom_device_id.
        Les champs custom (MPPT, nombre de modules, …) sont remontés
        pour pouvoir être comparés plus tard.
        """
        # — cache sites {yuman_site_id → Site}
        sites_by_id = {
            s.yuman_site_id: s
            for s in self.fetch_sites().values()
        }

        equips: Dict[str, Equipment] = {}

        for m in self._list_materials():
            site = sites_by_id.get(m["site_id"])
            if not site:                                             # site hors périmètre
                continue

            cat_id  = m["category_id"]
            fields  = {f["name"]: f.get("value") for f in m.get("_embed", {}).get("fields", [])}

            # ── reconstruction du vcom_device_id ──────────────────────────
            if cat_id == CAT_INVERTER:
                vdid = fields.get(CUSTOM_INVERTER_ID) or m.get("serial_number")
            elif cat_id == CAT_STRING:
                vdid = m.get("serial_number") or m["name"]           # déjà WR-X-STRING-… côté VCOM
            elif cat_id == CAT_MODULE:
                vdid = f"MODULES-{site.vcom_system_key}"
            else:
                vdid = m.get("serial_number") or m["name"]

            eq_type = (
                "inverter"   if cat_id == CAT_INVERTER else
                "module"     if cat_id == CAT_MODULE   else
                "string_pv"
            )

            # — extraction du nombre de modules (int ou None)
            raw_count: Optional[str] = fields.get("nombre de module")
            count: Optional[int] = int(raw_count) if raw_count and raw_count.isdigit() else None

            equip = Equipment(
                vcom_system_key   = site.vcom_system_key,
                category_id       = cat_id,
                eq_type           = eq_type,
                vcom_device_id    = vdid,
                name              = m.get("name"),
                brand             = m.get("brand"),
                model             = m.get("model"),
                serial_number     = m.get("serial_number"),
                count             = count,
                yuman_material_id = m["id"],
                parent_id         = m.get("parent_id"),
            )

            # ▸ on complète les *custom fields* pour comparaison ultérieure
            # (1) MPPT index
            object.__setattr__(equip, "mppt_idx",
                            fields.get("MPPT index", "") or "")
            # (2) nombre de modules
            object.__setattr__(equip, "nb_modules",
                            fields.get("nombre de module", "") or "")
            # (3) marque module
            object.__setattr__(equip, "module_brand",
                            fields.get("marque du module", "") or "")
            # (4) modèle module
            object.__setattr__(equip, "module_model",
                            fields.get("model de module", "") or "")
            # (5) index onduleur (WR N)
            if cat_id == CAT_INVERTER:
                try:
                    idx = int(vdid.split(".")[-1])          # ex. “…WR-3” → 3
                except Exception:
                    idx = None
                object.__setattr__(equip, "index", idx)

            equips[equip.key()] = equip

        logger.debug("[YUMAN] snapshot: %s equips", len(equips))
        _dump("[YUMAN] snapshot equips", equips)
        return equips


    # ------------------------------------------------------------------ #
    #  APPLY PATCH – SITES                                               #
    # ------------------------------------------------------------------ #
    def apply_sites_patch(self, db_sites: Dict[str, Site]) -> None:
        """
        Fait converger Yuman vers la vérité Supabase (db_sites).
        """
        y_sites = self.fetch_sites()
        patch   = diff_entities(y_sites, db_sites)

        # ---------- ADD ----------
        for s in patch.add:
            payload = {
                "name":    s.name,
                "address": s.address or "",
                "fields": [
                    {
                        "blueprint_id": SITE_FIELDS["System Key (Vcom ID)"],
                        "name":  "System Key (Vcom ID)",
                        "value": s.vcom_system_key,
                    },
                    {
                        "blueprint_id": SITE_FIELDS["Nominal Power (kWc)"],
                        "name":  "Nominal Power (kWc)",
                        "value": s.nominal_power,
                    },
                    {
                        "blueprint_id": SITE_FIELDS["Commission Date"],
                        "name":  "Commission Date",
                        "value": s.commission_date,
                    },
                ],
            }
            logger.debug("[YUMAN] create_site payload=%s", payload)
            new_site = self._create_site(payload)

            # Propager l’ID en DB
            (self.sb.sb.table("sites_mapping")
                .update({"yuman_site_id": new_site["id"]})
                .eq("vcom_system_key", s.vcom_system_key)
                .execute())

        # ---------- UPDATE ----------
        for old, new in patch.update:
            fields_patch = []
            if old.nominal_power != new.nominal_power and new.nominal_power is not None:
                fields_patch.append({
                    "blueprint_id": SITE_FIELDS["Nominal Power (kWc)"],
                    "name":  "Nominal Power (kWc)",
                    "value": new.nominal_power,
                })
            if old.commission_date != new.commission_date and new.commission_date:
                fields_patch.append({
                    "blueprint_id": SITE_FIELDS["Commission Date"],
                    "name":  "Commission Date",
                    "value": new.commission_date,
                })

            if fields_patch:
                logger.debug("[YUMAN] update_site %s fields=%s",
                             old.yuman_site_id, fields_patch)
                self._update_site(old.yuman_site_id, {"fields": fields_patch})

            # Back-fill de l’ID Yuman manquant en DB
            if new.yuman_site_id is None and old.yuman_site_id:
                (self.sb.sb.table("sites_mapping")
                    .update({"yuman_site_id": old.yuman_site_id})
                    .eq("vcom_system_key", new.vcom_system_key)
                    .execute())

    # ------------------------------------------------------------------ #
    #  APPLY PATCH – EQUIPMENTS                                           #
    # ------------------------------------------------------------------ #
    def apply_equips_patch(self, db_equips: Dict[str, Equipment]) -> None:
        """
        Synchronise modules, onduleurs et strings entre VCOM (db_equips)
        et Yuman (liste courante). Ordre : modules → onduleurs → strings.
        """
        # ───────────────────────── preparation ───────────────────────────
        y_equips     = self.fetch_equips()
        patch        = diff_entities(y_equips, db_equips)

        # cache {vcom_device_id → yuman_material_id} (utile pour la parenté des strings)
        id_by_vcom = {
            e.vcom_device_id: e.yuman_material_id
            for e in y_equips.values() if e.yuman_material_id
        }

        _ORDER = {CAT_MODULE: 0, CAT_INVERTER: 1, CAT_STRING: 2}

        # blueprints (une seule source d’infos)
        BP_MODEL          = 13548
        BP_MPPT_IDX       = STRING_FIELDS["MPPT index"]
        BP_NB_MODULES     = STRING_FIELDS["nombre de module"]
        BP_MODULE_BRAND   = STRING_FIELDS["marque du module"]
        BP_MODULE_MODEL   = STRING_FIELDS["model de module"]

        # ───────────────────────── INSERTIONS ────────────────────────────
        for e in sorted(patch.add, key=lambda x: _ORDER.get(x.category_id, 99)):

            # 1) mapping site VCOM → Yuman
            site_row = (
                self.sb.sb.table("sites_mapping")
                .select("yuman_site_id")
                .eq("vcom_system_key", e.vcom_system_key)
                .single()
                .execute()
                .data
            )
            if not site_row or not site_row["yuman_site_id"]:
                logger.warning("Site %s sans yuman_site_id → skip equip %s",
                            e.vcom_system_key, e.vcom_device_id)
                continue

            payload: Dict[str, Any] = {
                "site_id":      site_row["yuman_site_id"],
                "category_id":  e.category_id,
                "brand":        e.brand,
                "serial_number": e.vcom_device_id,   # unicité garantie
            }

            # — nommage des onduleurs  « WR N onduleur »
            if e.category_id == CAT_INVERTER:
                idx = getattr(e, "index", None) or 1
                payload["name"] = f"WR {idx} onduleur"
            else:
                payload["name"] = e.name

            # — constitution des custom-fields
            fields: List[Dict[str, Any]] = []

            if e.model:
                fields.append({"blueprint_id": BP_MODEL, "value": e.model})

            if e.category_id == CAT_INVERTER:
                fields.append({"name": CUSTOM_INVERTER_ID, "value": e.vcom_device_id})

            elif e.category_id == CAT_STRING:
                try:
                    mppt_idx = e.vcom_device_id.split("-MPPT-")[1].split(".")[0]
                except IndexError:
                    mppt_idx = "?"
                fields.extend([
                    {"blueprint_id": BP_MPPT_IDX,     "value": mppt_idx},
                    {"blueprint_id": BP_NB_MODULES,   "value": str(e.count or "")},
                    {"blueprint_id": BP_MODULE_BRAND, "value": e.brand},
                    {"blueprint_id": BP_MODULE_MODEL, "value": e.model},
                ])

            if fields:
                payload["fields"] = fields

            # — parent (string → onduleur)
            if e.category_id == CAT_STRING and e.parent_id:
                parent_mat = id_by_vcom.get(e.parent_id)
                if parent_mat:
                    payload["parent_id"] = parent_mat

            logger.debug("[YUMAN] create_material payload=%s", payload)
            mat = self._create_material(payload)
            _dump("[YUMAN] material created", mat)

            # on stocke l’ID Yuman nouvellement créé
            (self.sb.sb.table("equipments_mapping")
                .update({"yuman_material_id": mat["id"]})
                .eq("vcom_device_id", e.vcom_device_id)
                .eq("vcom_system_key", e.vcom_system_key)
                .execute())

            id_by_vcom[e.vcom_device_id] = mat["id"]

        # ─────────────────────────  MISE À JOUR  ─────────────────────────
        for old, new in patch.update:

            # back-fill éventuel de l’ID Yuman
            if new.yuman_material_id is None and old.yuman_material_id:
                (self.sb.sb.table("equipments_mapping")
                    .update({"yuman_material_id": old.yuman_material_id})
                    .eq("vcom_device_id", new.vcom_device_id)
                    .eq("vcom_system_key", new.vcom_system_key)
                    .execute())

            payload: Dict[str, Any]            = {}
            fields_patch: List[Dict[str, Any]] = []

            # ── renommage onduleur
            if old.category_id == CAT_INVERTER:
                idx = getattr(new, "index", None) or 1
                desired_name = f"WR {idx} onduleur"
                if old.name != desired_name:
                    payload["name"] = desired_name

            # ── parent pour STRING
            if old.category_id == CAT_STRING and new.parent_id:
                parent_mat = id_by_vcom.get(new.parent_id)
                if parent_mat and old.parent_id != parent_mat:
                    payload["parent_id"] = parent_mat

            # ── “Inverter ID (Vcom)”
            if (old.category_id == CAT_INVERTER
                    and old.vcom_device_id != new.vcom_device_id):
                fields_patch.append(
                    {"name": CUSTOM_INVERTER_ID, "value": new.vcom_device_id}
                )

            # ── modèle (blueprint 13548)
            if old.model != new.model and new.model:
                fields_patch.append({"blueprint_id": BP_MODEL, "value": new.model})

            # ── STRING PV : MPPT / nbre modules / marque / modèle
            if old.category_id == CAT_STRING:
                def _maybe(bp, old_val, new_val):
                    if (old_val or "") != (new_val or ""):
                        fields_patch.append({"blueprint_id": bp, "value": new_val})

                # valeurs stockées actuellement dans Yuman
                old_mppt       = getattr(old, "mppt_idx", "")
                old_nb_mod     = getattr(old, "nb_modules", "")
                old_mod_brand  = getattr(old, "module_brand", "")
                old_mod_model  = getattr(old, "module_model", "")

                # nouvelles valeurs provenant de VCOM
                try:
                    new_mppt = new.vcom_device_id.split("-MPPT-")[1].split(".")[0]
                except IndexError:
                    new_mppt = "?"
                new_nb_mod    = str(new.count or "")
                new_mod_brand = new.brand or ""
                new_mod_model = new.model or ""

                _maybe(BP_MPPT_IDX,     old_mppt,      new_mppt)
                _maybe(BP_NB_MODULES,   old_nb_mod,    new_nb_mod)
                _maybe(BP_MODULE_BRAND, old_mod_brand, new_mod_brand)
                _maybe(BP_MODULE_MODEL, old_mod_model, new_mod_model)

            # ── PATCH final
            if fields_patch:
                payload["fields"] = fields_patch

            if payload:
                logger.debug("[YUMAN] update_material %s payload=%s",
                            old.yuman_material_id, payload)
                self._update_material(old.yuman_material_id, payload)