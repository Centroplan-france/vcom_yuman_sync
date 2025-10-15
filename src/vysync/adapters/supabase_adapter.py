#!/usr/bin/env python3
from __future__ import annotations
"""
Accès Supabase : snapshot & patch.
La clé logique d’un équipement est « vcom_device_id » (string).
"""

import os
import logging
from datetime import datetime, timezone
from typing import Dict, Optional

from supabase import create_client, Client as SupabaseClient

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

from vysync.app_logging import init_logger
from vysync.models import (
    Site,
    Equipment,
    Client,
    CAT_INVERTER,
    CAT_MODULE,
    CAT_STRING,
    CAT_SIM,
    CAT_CENTRALE,
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
        """Recharge les mappings vcom_system_key → id et yuman_site_id → id."""
        rows = (
            self.sb.table(SITE_TABLE)
            .select("id, vcom_system_key, yuman_site_id")
            .execute()
            .data
            or []
        )

        self._map_vcom_to_id  = {}   
        self._map_yid_to_id   = {}

        for r in rows:
            if r["vcom_system_key"]:
                self._map_vcom_to_id[r["vcom_system_key"]] = r["id"]
            if r["yuman_site_id"] is not None:
                self._map_yid_to_id[r["yuman_site_id"]] = r["id"]

        logger.debug("[SB] site cache refreshed (%s entries)", len(rows))

    def _site_id(self, vcom_key: str | None) -> int | None:
        """Retourne l’ID Supabase via vcom_system_key."""
        if vcom_key is None:
            return None
        return self._map_vcom_to_id.get(vcom_key)

    def _site_id_by_yuman(self, yuman_site_id: int | None) -> int | None:
        """Retourne l’ID Supabase via yuman_site_id."""
        if yuman_site_id is None:
            return None
        return self._map_yid_to_id.get(yuman_site_id)



    # ----------------------------- SITES -------------------------------
    def fetch_sites_v(self, site_key: Optional[str] = None) -> Dict[str, Site]:
        query = self.sb.table(SITE_TABLE).select("*")
        if site_key:
            query = query.eq('vcom_system_key', site_key)  # Filtrer par site_key
        rows = query.execute().data or []
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
                site_area=r.get("site_area"),
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
            sites[r["yuman_site_id"]] = Site(
                id=r["id"],  
                vcom_system_key=r["vcom_system_key"],
                name=r.get("name") or r["vcom_system_key"],
                latitude=r.get("latitude"),
                longitude=r.get("longitude"),
                nominal_power=r.get("nominal_power"),
                commission_date=r.get("commission_date"),
                address=r.get("address"),
                yuman_site_id=r.get("yuman_site_id"),
                client_map_id=r.get("client_map_id"),
                project_number_cp=r.get("project_number_cp"),
                aldi_store_id=r.get("aldi_store_id"),
                aldi_id=r.get("aldi_id"),
                ignore_site=bool(r.get("ignore_site")),
            )
        logger.debug("[SB] fetched %s sites", len(sites))
        return sites

    # --------------------------- EQUIPMENTS ----------------------------
    def fetch_equipments_v(self, site_key: Optional[str] = None) -> Dict[str, Equipment]:
        equips = {}
        from_row, step = 0, 1000       # page de 1 000
        while True:
            # 1. Prépare la requête de base
            query = (
                self.sb.table(EQUIP_TABLE)
                .select("*")
                .in_("category_id", [CAT_INVERTER, CAT_MODULE, CAT_STRING])
                .eq("is_obsolete", False)
            )

            # 2. Ajoute le filtre site si demandé
            if site_key:
                query = query.eq("vcom_system_key", site_key)

            # 3. Paginate
            page = (
                query
                .range(from_row, from_row + step - 1)
                .execute()
                .data
                or []
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
                .in_("category_id", [CAT_INVERTER, CAT_MODULE, CAT_STRING, CAT_SIM, CAT_CENTRALE])
                .eq("is_obsolete", False)
                .range(from_row, from_row + step - 1)   # pagination
                .execute()
                .data or []
            )
            for r in page:
                equips[r["serial_number"]] = Equipment(
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
                    yuman_site_id=r.get("yuman_site_id"),
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
            row.pop("id", None)
            self.sb.table(SITE_TABLE).insert([row]).execute()

        IMMUTABLE_COLS = {"vcom_system_key", "created_at", "ignore_site"}

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
        Applique le diff (add / update / delete) sur la table des équipements.
        - patch.add      : list[Equipment]
        - patch.update   : list[tuple[Equipment, Equipment]]  # (actuel, cible)
        - patch.delete   : list[Equipment]
        """
        VALID_COLS = {
            "parent_id", "is_obsolete", "obsolete_at", "count",
            "vcom_system_key", "eq_type", "vcom_device_id",
            "serial_number", "brand", "model", "name", "site_id",
            "created_at", "extra", "yuman_material_id", "category_id","yuman_site_id"
        }
        now_iso = datetime.now(timezone.utc).isoformat()

        # ---------- ADD / UPSERT ----------
        if patch.add:
            upserts = []
            for e in patch.add:
                site_id = self._site_id(e.vcom_system_key)
                if site_id is None:
                    logger.error("[SB] site %s introuvable → skip %s",
                                e.vcom_system_key, e.vcom_device_id)
                    continue
                row = e.to_dict()
                row.update(
                    site_id=site_id,
                    created_at=now_iso,
                    name=row.get("name") or row["vcom_device_id"],
                )
                upserts.append({k: v for k, v in row.items() if v is not None and k in VALID_COLS})

            if upserts:
                res = (
                    self.sb.table(EQUIP_TABLE)
                    .upsert(upserts, on_conflict=["vcom_device_id"], ignore_duplicates=True)
                    .execute()
                )
                logger.debug("[SB] UPSERT %d equips → %s", len(upserts), res.data)

        # ---------- UPDATE ----------
        for item in patch.update:
            # item = (ancien, nouveau)
            e_new = item[1] if isinstance(item, tuple) else item
            site_id = self._site_id(e_new.vcom_system_key)

            payload = {
                k: v for k, v in e_new.to_dict().items()
                if v is not None and k in VALID_COLS and k not in {"vcom_device_id", "vcom_system_key"}
            }
            if not payload:
                continue  # rien à modifier
            payload["site_id"]=site_id

            res = (
                self.sb.table(EQUIP_TABLE)
                .update(payload)
                .eq("vcom_device_id", e_new.vcom_device_id)
                .execute()
            )
            logger.debug("[SB] UPDATE %s → %s", e_new.vcom_device_id, res.data)

        # ---------- DELETE (flag obsolète) ----------
        if patch.delete:
            dev_ids = [e.vcom_device_id for e in patch.delete]
            res = (
                self.sb.table(EQUIP_TABLE)
                .update({"is_obsolete": True, "obsolete_at": now_iso})
                .in_("vcom_device_id", dev_ids)
                .execute()
            )
            logger.debug("[SB] FLAG obsolete %d equips → %s", len(dev_ids), res.data)



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
            print(type(client), client)
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


    def apply_equips_mapping_patch(self, patch) -> None:
        TABLE = "equipments_mapping"
        VALID = {
            "parent_id", "is_obsolete", "obsolete_at", "count",
            "vcom_system_key", "eq_type", "vcom_device_id",
            "serial_number", "brand", "model", "name", "site_id",
            "created_at", "extra", "yuman_material_id", "category_id","yuman_site_id"
            # champs custom : si besoin, décommente
            # "mppt_idx", "module_brand", "module_model",
        }
        now_iso = datetime.now(timezone.utc).isoformat()

        # --------------------- ADD / UPSERT ------------------------
        dedup: dict[str, dict] = {}          # vcom_device_id → row unique

        for e in patch.add:
            sid = e.site_id or self._site_id_by_yuman(e.yuman_site_id)
            if sid is None:
                logger.error("[SB] site Yuman %s introuvable → skip %s",
                            e.yuman_site_id, e.yuman_material_id)
                continue

            row = {k: v for k, v in e.to_dict().items() if k in VALID}
            row["site_id"] = sid
            row.setdefault("created_at", now_iso)

            dedup.setdefault(row["vcom_device_id"], row)

        if dedup:
            res = (
                self.sb.table(TABLE)
                .upsert(
                    list(dedup.values()),
                    on_conflict=["vcom_device_id", "yuman_material_id"],      # contrainte unique
                    ignore_duplicates=True
                )
                .execute()
            )
            logger.debug("[SB] UPSERT %d equipsMapping → %s",
                        len(dedup), res.data)

        # ----------------------- UPDATE ---------------------------
        for old, e in patch.update:
            # 1) Résolution du site
            sid = e.site_id or self._site_id_by_yuman(e.yuman_site_id)
            if sid is None:
                continue

            # 2) Construction du payload : on exclut les None, et les clés d'identification
            payload = {
                k: v
                for k, v in e.to_dict().items()
                if v is not None
                and k in VALID
                and k not in {"vcom_device_id", "yuman_material_id", "vcom_system_key"}
            }
            # on remet site_id à jour
            payload["site_id"] = sid

            # si payload vide, on skip
            if not payload:
                continue

            # 3) Construction de la requête : on utilise vcom_device_id si dispo,
            #    sinon yuman_material_id comme filtre
            query = self.sb.table(TABLE).update(payload)
            if e.vcom_device_id:
                query = query.eq("vcom_device_id", e.vcom_device_id)
                ident = f"vcom_device_id = {e.vcom_device_id}"
            else:
                query = query.eq("yuman_material_id", e.yuman_material_id)
                ident = f"yuman_material_id = {e.yuman_material_id}"

            # 4) Exécution
            res = query.execute()
            logger.debug(f"[SB] UPDATE ({ident}) → {res.data}")
