#!/usr/bin/env python3
from __future__ import annotations
"""
Accès Supabase : snapshot & patch.
La clé logique d’un équipement est « vcom_device_id » (string).
"""

import os
from datetime import datetime, timezone
from typing import Dict, List

from supabase import create_client, Client as SupabaseClient
import logging
from vysync.app_logging import init_logger
from vysync.models import (
    Site,
    Equipment,
    CAT_INVERTER,
    CAT_MODULE,
    CAT_STRING,
)

logger = init_logger(__name__)
logger.setLevel(logging.DEBUG)

SITE_TABLE  = "sites_mapping"
EQUIP_TABLE = "equipments_mapping"

# ──────────────────────────── Adapter ───────────────────────────
class SupabaseAdapter:
    def __init__(self):
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_SERVICE_KEY")
        if not (url and key):
            raise EnvironmentError("SUPABASE_URL or SUPABASE_SERVICE_KEY missing")
        self.sb: SupabaseClient = create_client(url, key)

    # ----------------------------- SITES -------------------------------
    def fetch_sites(self) -> Dict[str, Site]:
        rows = self.sb.table(SITE_TABLE).select("*").execute().data or []
        out: Dict[str, Site] = {}
        for r in rows:
            if not r.get("vcom_system_key"):
                continue
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

    # --------------------------- EQUIPMENTS ----------------------------
    def fetch_equipments(self) -> Dict[str, Equipment]:
        rows = (
            self.sb.table(EQUIP_TABLE)
            .select("*")
            .in_("category_id", [CAT_INVERTER, CAT_MODULE, CAT_STRING])
            .execute()
            .data or []
        )
        out: Dict[str, Equipment] = {}
        for r in rows:
            out[r["vcom_device_id"]] = Equipment(
                site_key=r["vcom_system_key"],
                category_id=r["category_id"],
                eq_type=r["eq_type"],
                vcom_device_id=r["vcom_device_id"],
                name=r["name"],
                brand=r.get("brand"),
                model=r.get("model"),
                serial_number=r.get("serial_number"),
                count=r.get("count"),
                parent_vcom_id=None,
                yuman_material_id=r.get("yuman_material_id"),
            )
        logger.debug("[SB] fetched %s equipments", len(out))
        return out

    # ------------------------- APPLY SITES -----------------------------
    def apply_sites_patch(self, patch):
        for s in patch.add:
            logger.debug("[SB] INSERT site %s", s.key())
            self.sb.table(SITE_TABLE).insert([s.to_dict()]).execute()
        for _, new in patch.update:
            logger.debug("[SB] UPDATE site %s", new.key())
            self.sb.table(SITE_TABLE) \
                  .update(new.to_dict()) \
                  .eq("vcom_system_key", new.key()) \
                  .execute()

    # ------------------------ APPLY EQUIPS -----------------------------
    def apply_equips_patch(self, patch):
        # cache vcom_system_key -> id
        if not hasattr(self, "_site_cache"):
            rows = (
                self.sb.table(SITE_TABLE)
                .select("id,vcom_system_key")
                .execute()
                .data
            )
            self._site_cache = {r["vcom_system_key"]: r["id"] for r in rows}

        VALID_COLS = {
            "yuman_material_id", "category_id", "eq_type",
            "vcom_system_key", "vcom_device_id", "serial_number",
            "brand", "model", "name", "site_id", "created_at",
            "count", "parent_id", "is_obsolete", "obsolete_at",
        }

        # ---------- ADD ----------
        for e in patch.add:
            row = e.to_dict()                    # sans site_key, parent_vcom_id
            vkey = e.site_key
            row["vcom_system_key"] = vkey
            row["site_id"] = self._site_cache.get(vkey)
            if row["site_id"] is None:
                logger.error("[SB] site %s introuvable → skip %s", vkey, e.vcom_device_id)
                continue

            row.setdefault("created_at", datetime.now(timezone.utc).isoformat())
            row.setdefault("name", row["vcom_device_id"])
            row = {k: v for k, v in row.items() if k in VALID_COLS}

            logger.debug("[SB] INSERT equip %s", row["vcom_device_id"])
            self.sb.table(EQUIP_TABLE).insert([row]).execute()

        # ---------- UPDATE ----------
        for _, new in patch.update:
            upd = new.to_dict()
            logger.debug("[SB] UPDATE equip %s", new.key())
            self.sb.table(EQUIP_TABLE) \
                  .update(upd) \
                  .eq("vcom_device_id", new.vcom_device_id) \
                  .execute()
