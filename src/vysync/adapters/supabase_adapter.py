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

from datetime import datetime, timezone
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

from vysync.app_logging import init_logger, _dump
from vysync.models import (
    Site,
    Equipment,
    Client,
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
    def fetch_sites_v(self) -> Dict[str, Site]:
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
                client_map_id=r.get("client_map_id"),
                ignore_site=bool(r.get("ignore_site")),
            )
        logger.debug("[SB] fetched %s sites", len(sites))
        return sites

    def fetch_sites_y(self) -> Dict[str, Site]:
        rows = self.sb.table(SITE_TABLE).select("*").execute().data or []
        sites: Dict[str, Site] = {}
        for r in rows:
            if not r.get("yuman_site_id"):
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
                client_map_id=r.get("client_map_id"),
                ignore_site=bool(r.get("ignore_site")),
            )
        logger.debug("[SB] fetched %s sites", len(sites))
        return sites

    # --------------------------- EQUIPMENTS ----------------------------
    def fetch_equipments_v(self) -> Dict[str, Equipment]:
        equips = {}
        from_row, step = 0, 1000       # page de 1 000
        while True:
            page = (
                self.sb.table(EQUIP_TABLE)
                .select("*")
                .in_("category_id", [CAT_INVERTER, CAT_MODULE, CAT_STRING])
                .eq("is_obsolete", False)
                .range(from_row, from_row + step - 1)   # pagination
                .execute()
                .data or []
            )
            for r in page:
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
            if len(page) < step:
                break        # dernière page atteinte
            from_row += step
        logger.debug("[SB] fetched %s equipments", len(equips))
        return equips

    def fetch_equipments_y(self) -> Dict[str, Equipment]:
        equips = {}
        from_row, step = 0, 1000       # page de 1 000
        while True:
            page = (
                self.sb.table(EQUIP_TABLE)
                .select("*")
                .in_("category_id", [CAT_INVERTER, CAT_MODULE, CAT_STRING])
                .eq("is_obsolete", False)
                .range(from_row, from_row + step - 1)   # pagination
                .execute()
                .data or []
            )
            for r in page:
                equips[r["yuman_material_id"]] = Equipment(
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
            if len(page) < step:
                break        # dernière page atteinte
            from_row += step
        logger.debug("[SB] fetched %s equipments", len(equips))
        return equips

    # ------------------------- APPLY SITES -----------------------------
    def apply_sites_patch(self, patch) -> None:
        """Insère/maj les sites.  
        `patch` doit exposer `.add` et `.update` comme itérables."""
        for s in patch.add:
            logger.debug("[SB] INSERT site %s", s.key())
            row = s.to_dict()
            row["created_at"] = _now_iso()        # horodatage UTC
            self.sb.table(SITE_TABLE).insert([row]).execute()

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
        # Récupérer tous les équipements existants pour détecter les doublons
        existing_rows = (
            self.sb.table(EQUIP_TABLE)
            .select("vcom_device_id,vcom_system_key,yuman_material_id")
            .execute()
            .data or []
        )
        ids_in_db = {r["yuman_material_id"] for r in existing_rows if r.get("yuman_material_id")}
        keys_in_db = {(r["vcom_device_id"], r["vcom_system_key"]) for r in existing_rows}
        
        VALID_COLS: set[str] = {
                                    "parent_id",
                                    "is_obsolete",
                                    "obsolete_at",
                                    "count",
                                    "vcom_system_key",
                                    "eq_type",
                                    "vcom_device_id",
                                    "serial_number",
                                    "brand",
                                    "model",
                                    "name",
                                    "site_id",
                                    "created_at",
                                    "extra",
                                    "yuman_material_id",
                                    "category_id",
                                }
        inserts = []
        updates = []
        for e in patch.add:
            site_id = self._site_id(e.vcom_system_key)
            if site_id is None:
                logger.error("[SB] site %s introuvable → skip %s", e.vcom_system_key, e.vcom_device_id)
                continue
            row = e.to_dict()
            row.update(
                site_id    = site_id,
                created_at = datetime.now(timezone.utc).isoformat(),
                name       = row.get("name") or row["vcom_device_id"]
            )
            # Filtrer les colonnes valides
            row = {k: v for k, v in row.items() if k in VALID_COLS}
            # Décider insert vs update en fonction des doublons
            if (row.get("yuman_material_id") in ids_in_db) or ((row["vcom_device_id"], row["vcom_system_key"]) in keys_in_db):
                updates.append(row)
            else:
                inserts.append(row)
        
        # Insérer les nouveaux équipements (upsert sur vcom_device_id + yuman_material_id)
        if inserts:
            try:
                self.sb.table(EQUIP_TABLE).upsert(
                    inserts,
                    on_conflict=["yuman_material_id", "vcom_device_id"],
                    ignore_duplicates=True
                ).execute()
            except Exception as exc:
                logger.exception("[SB] INSERT failed: %s", exc)
        
        # Mettre à jour les équipements existants
        for row in updates:
            try:
                if row.get("yuman_material_id"):
                    # Mise à jour par yuman_material_id si disponible
                    self.sb.table(EQUIP_TABLE).update(row) \
                          .eq("yuman_material_id", row["yuman_material_id"]).execute()
                else:
                    # Sinon, mise à jour par vcom_device_id
                    self.sb.table(EQUIP_TABLE).update(row) \
                          .eq("vcom_device_id", row["vcom_device_id"]).execute()
                logger.debug("[SB] UPDATE equip %s", row["vcom_device_id"])
            except Exception as exc:
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


    def fetch_clients(self) -> Dict[int, Client]:
        """
        Lit la table `clients_mapping` et renvoie un dict
        { yuman_client_id → Client(...) }.
        """
        rows = (
            self.sb
                .table("clients_mapping")
                .select("yuman_client_id,code,name,address")
                .execute()
                .data
            or []
        )
        clients: Dict[int, Client] = {}
        for r in rows:
            yid = r.get("yuman_client_id")
            if not yid:
                continue
            clients[yid] = Client(
                yuman_client_id=yid,
                code=            r.get("code"),
                name=            r["name"],
                address=         r.get("address"),
            )
        return clients
    
    def apply_clients_mapping_patch(self, patch) -> None:
        """
        Applique en base Supabase lePatchSet[Client] sur la table `clients_mapping`.
        • insert les nouveaux clients (patch.add)  
        • update les clients existants (patch.update)
        """
        # INSERT / UPSERT des nouveaux clients
        for client in patch.add:
            row = client.to_dict()
            # garantir un created_at
            row.setdefault(
                "created_at",
                datetime.now(timezone.utc).isoformat()
            )
            # on upsert pour éviter doublons si jamais
            self.sb.table("clients_mapping") \
                .upsert(row, on_conflict="yuman_client_id") \
                .execute()

        # MISE À JOUR des clients existants
        for old, new in patch.update:
            updates: dict[str, any] = {}
            if old.code    != new.code:    updates["code"]    = new.code
            if old.name    != new.name:    updates["name"]    = new.name
            if old.address != new.address: updates["address"] = new.address

            if updates:
                self.sb.table("clients_mapping") \
                    .update(updates) \
                    .eq("yuman_client_id", new.yuman_client_id) \
                    .execute()
