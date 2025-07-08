#!/usr/bin/env python3
from __future__ import annotations
"""
Accès Supabase : snapshot & patch.
La clé logique d’un équipement est « vcom_device_id » (string).
"""

import os
import logging
from datetime import datetime, timezone
from typing import Dict, List, Tuple

from supabase import create_client, Client as SupabaseClient

from vysync.app_logging import init_logger, _dump
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
    def __init__(self) -> None:
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_SERVICE_KEY")
        if not (url and key):
            raise EnvironmentError("SUPABASE_URL or SUPABASE_SERVICE_KEY missing")
        self.sb: SupabaseClient = create_client(url, key)
        self._refresh_site_cache()

    # -----------------------------------------------------------------
    # Internal helpers
    # -----------------------------------------------------------------
    def _refresh_site_cache(self) -> None:
        """(Re)charge le mapping vcom_system_key → site.id."""
        rows = (
            self.sb.table(SITE_TABLE)
            .select("id,vcom_system_key")
            .execute()
            .data
            or []
        )
        self._site_cache: Dict[str, int] = {
            r["vcom_system_key"]: r["id"] for r in rows
        }
        logger.debug("[SB] site cache refreshed (%s entries)", len(self._site_cache))

    def _site_id(self, vcom_key: str) -> int | None:
        """Retourne l’ID PostgreSQL du site, None si inconnu."""
        return self._site_cache.get(vcom_key)

    # ----------------------------- SITES -------------------------------
    def fetch_sites(self) -> Dict[str, Site]:
        rows = self.sb.table(SITE_TABLE).select("*").execute().data or []
        sites: Dict[str, Site] = {}
        for r in rows:
            if not r.get("vcom_system_key"):
                continue
            sites[r["vcom_system_key"]] = Site(
                vcom_system_key=r["vcom_system_key"],
                name=r.get("name") or r["vcom_system_key"],
                latitude=r.get("latitude"),
                longitude=r.get("longitude"),
                nominal_power=r.get("nominal_power"),
                commission_date=r.get("commission_date"),
                address=r.get("address"),
                yuman_site_id=r.get("yuman_site_id"),
            )
        logger.debug("[SB] fetched %s sites", len(sites))
        return sites

    # --------------------------- EQUIPMENTS ----------------------------
    def fetch_equipments(self) -> Dict[str, Equipment]:
        rows = (
            self.sb.table(EQUIP_TABLE)
            .select("*")
            .in_("category_id", [CAT_INVERTER, CAT_MODULE, CAT_STRING])
            .eq("is_obsolete", False)
            .execute()
            .data
            or []
        )
        equips: Dict[str, Equipment] = {}
        for r in rows:
            equips[r["vcom_device_id"]] = Equipment(
                vcom_system_key=r["vcom_system_key"],
                category_id=r["category_id"],
                eq_type=r["eq_type"],
                vcom_device_id=r["vcom_device_id"],
                name=r["name"],
                brand=r.get("brand"),
                model=r.get("model"),
                serial_number=r.get("serial_number"),
                count=r.get("count"),
                parent_id=r.get("parent_id"),
                yuman_material_id=r.get("yuman_material_id"),
            )
        logger.debug("[SB] fetched %s equipments", len(equips))
        return equips

    # ------------------------- APPLY SITES -----------------------------
    def apply_sites_patch(self, patch) -> None:
        """Insère/maj les sites.  
        `patch` doit exposer `.add` et `.update` comme itérables."""
        for s in patch.add:
            logger.debug("Site %s → %s", _.yuman_site_id, new.yuman_site_id)
            logger.debug("[SB] INSERT site %s", s.key())
            self.sb.table(SITE_TABLE).insert([s.to_dict()]).execute()

        IMMUTABLE_COLS = {"vcom_system_key", "created_at"}

        for old, new in patch.update:
            # Construire le dict des champs à updater
            upd = {
                k: v
                for k, v in new.to_dict().items()
                if v is not None               # on ignore les None
                and k not in IMMUTABLE_COLS  # on n’override pas les colonnes immuables
            }
            if upd:
                # Si yuman_site_id est absent dans VCOM, il ne sera pas dans upd
                logger.debug("Updating sites_mapping %s → %s", old.vcom_system_key, upd)
                self.sb.table("sites_mapping") \
                    .update(upd) \
                    .eq("vcom_system_key", old.vcom_system_key) \
                    .execute()

        # Le cache doit refléter les nouveaux sites avant d’insérer des équipements
        self._refresh_site_cache()

    # ------------------------ APPLY EQUIPS -----------------------------
    def apply_equips_patch(self, patch) -> None:
        """
        • `patch.add`  : Iterable[Equipment]  
        • `patch.update`: Iterable[tuple[Equipment, Equipment]]
        """
        VALID_COLS: set[str] = {
            "yuman_material_id", "category_id", "eq_type",
            "vcom_system_key",   "vcom_device_id", "serial_number",
            "brand", "model", "name", "site_id", "created_at",
            "count", "parent_id", "is_obsolete", "obsolete_at",
        }

        # ---------- ADD ----------
        for e in patch.add:
            site_id = self._site_id(e.vcom_system_key)
            if site_id is None:
                logger.error("[SB] site %s introuvable → skip %s",
                             e.vcom_system_key, e.vcom_device_id)
                continue

            row = e.to_dict()
            row.update(
                site_id      = site_id,
                created_at   = datetime.now(timezone.utc).isoformat(),
                name         = row["name"] or row["vcom_device_id"],
            )
            row = {k: v for k, v in row.items() if k in VALID_COLS}

            logger.debug("[SB] INSERT equip %s", row["vcom_device_id"])
            _dump("[SB] row inserted", row)
            try:
                self.sb.table(EQUIP_TABLE).insert([row]).execute()
            except:
                logger.exception("[SB] INSERT failed: %s", exc)

        # ---------- UPDATE ----------
        IMMUTABLE_COLS = {"vcom_device_id", "vcom_system_key", "site_id", "created_at"}

        for _, new in patch.update:
            upd = {
                k: v
                for k, v in new.to_dict().items()
                if v is not None and k not in IMMUTABLE_COLS
            }

            if upd.get("yuman_material_id") is None:
                upd.pop("yuman_material_id", None)

            if not upd:
                continue  # rien à mettre à jour

            try:
                self.sb.table(EQUIP_TABLE) \
                    .update(upd) \
                    .eq("vcom_device_id", new.vcom_device_id) \
                    .execute()
            except:
                logger.exception("[SB] UPDATE failed: %s", exc)


        # ---------- DELETE ----------
        for e in patch.delete:
            logger.debug("[SB] Obsolete equip %s", e.vcom_device_id)
            try:
                self.sb.table(EQUIP_TABLE) \
                    .update({"is_obsolete": True, "obsolete_at": datetime.now(timezone.utc).isoformat()}) \
                    .eq("vcom_device_id", e.vcom_device_id) \
                    .execute()
            except Exception as exc:
                logger.exception("[SB] Obsolete flag failed: %s", exc)
