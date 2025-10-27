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

def _norm_serial(s: str | None) -> str:
    return (s or "").strip().upper()

from vysync.logging_config import get_updates_logger
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

# Logger principal (configuré par setup_logging)
logger = logging.getLogger(__name__)

# Logger dédié aux updates (fichier séparé)
updates_logger = get_updates_logger()

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
        """Retourne l'ID Supabase via yuman_site_id."""
        if yuman_site_id is None:
            return None
        return self._map_yid_to_id.get(yuman_site_id)

    def _enrich_equipment_with_vcom_key(self, equipment: Equipment) -> Equipment:
        """Reconstruit vcom_system_key depuis site_id via le cache."""
        if equipment.site_id is None:
            return equipment

        # Trouver le vcom_system_key correspondant
        vcom_key = None
        for key, sid in self._map_vcom_to_id.items():
            if sid == equipment.site_id:
                vcom_key = key
                break

        # Reconstruire l'objet avec le vcom_system_key
        if vcom_key and vcom_key != equipment.vcom_system_key:
            return Equipment(
                **{**equipment.to_dict(), "vcom_system_key": vcom_key}
            )
        return equipment



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
                .eq("is_obsolete", False)
            )

            # 2. Ajoute le filtre site si demandé
            if site_key:
                site_id = self._site_id(site_key)
                if site_id:
                    query = query.eq("site_id", site_id)

            # 3. Paginate
            page = (
                query
                .range(from_row, from_row + step - 1)
                .execute()
                .data
                or []
            )
            for r in page:
                base_eq = Equipment(
                    site_id=r["site_id"],
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
                equips[r.get("serial_number")] = self._enrich_equipment_with_vcom_key(base_eq)
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
                .eq("is_obsolete", False)
                .range(from_row, from_row + step - 1)   # pagination
                .execute()
                .data or []
            )
            for r in page:
                base_eq = Equipment(
                    site_id=r["site_id"],
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
                equips[r["serial_number"]] = self._enrich_equipment_with_vcom_key(base_eq)
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
        VALID_COLS = {
            "parent_id", "is_obsolete", "obsolete_at", "count",
            "eq_type", "vcom_device_id",
            "serial_number", "brand", "model", "name", "site_id",
            "created_at", "extra", "yuman_material_id", "category_id"
        }
        now_iso = datetime.now(timezone.utc).isoformat()

        # ---------- ADD / UPSERT (update-on-conflict) ----------
        if patch.add:
            upserts = []
            for e in patch.add:
                site_id = self._site_id(e.vcom_system_key)
                if site_id is None:
                    logger.error("[SB] site %s introuvable → skip ADD %s",
                                e.vcom_system_key, e.vcom_device_id)
                    continue

                row = e.to_db_dict()
                # normalisation serial
                row["serial_number"] = _norm_serial(row.get("serial_number"))
                if not row["serial_number"]:
                    logger.error("[SB] ADD SKIPPED (serial vide) cat=%s vcom=%s mid=%s",
                                e.category_id, e.vcom_system_key, e.yuman_material_id)
                    continue

                row.update(
                    site_id=site_id,
                    created_at=now_iso,
                    name=row.get("name") or row.get("vcom_device_id"),
                )
                upserts.append({k: v for k, v in row.items() if v is not None and k in VALID_COLS})

            if upserts:
                # IMPORTANT: pas de ignore_duplicates → on veut UPDATE sur conflit
                res = (
                    self.sb.table(EQUIP_TABLE)
                    .upsert(upserts, on_conflict="serial_number")
                    .execute()
                )
                logger.debug("[SB] UPSERT %d equips → %s", len(upserts), res.data)

        # ---------- UPDATE ----------
        for item in patch.update:
            e_old = item[0] if isinstance(item, tuple) else None
            e_new = item[1] if isinstance(item, tuple) else item

            # NE METTRE À JOUR QUE LES CHAMPS QUI ONT CHANGÉ
            payload = {}
            for k, v in e_new.to_db_dict().items():
                # Skip les champs exclus
                if k in {"vcom_device_id", "vcom_system_key"}:
                    continue
                # Skip les champs non valides
                if k not in VALID_COLS:
                    continue
                # Skip si la valeur est None
                if v is None:
                    continue

                # AJOUTER SEULEMENT SI LA VALEUR A CHANGÉ
                old_value = getattr(e_old, k, None) if e_old else None
                if old_value != v:
                    payload[k] = v

            # normaliser serial côté payload si présent
            if "serial_number" in payload:
                payload["serial_number"] = _norm_serial(payload["serial_number"])

            # site_id (si résoluble et changé)
            site_id = self._site_id(e_new.vcom_system_key)
            if site_id is not None and (not e_old or site_id != e_old.site_id):
                payload["site_id"] = site_id

            if not payload:
                logger.debug("[SB] UPDATE SKIPPED (aucun changement): serial=%s mid=%s",
                            e_new.serial_number, e_new.yuman_material_id)
                continue

            # LOG des changements détectés
            if e_old:
                changes = {k: (getattr(e_old, k, None), v) for k, v in payload.items()
                           if getattr(e_old, k, None) != v}
                if changes:
                    updates_logger.info("UPDATE detected for serial=%s mid=%s | Changes: %s",
                                       e_new.serial_number, e_new.yuman_material_id, changes)

            # UPDATE par serial d'abord
            serial_new = _norm_serial(e_new.serial_number)
            updated = False
            if serial_new:
                updates_logger.debug("Attempting UPDATE by serial=%s with payload=%s", serial_new, payload)
                res = (
                    self.sb.table(EQUIP_TABLE)
                    .update(payload)
                    .eq("serial_number", serial_new)
                    .execute()
                )
                updated = bool(res.data)
                if updated:
                    updates_logger.info("✅ UPDATE OK by serial=%s: %d row(s) affected", serial_new, len(res.data))
                else:
                    updates_logger.warning("❌ UPDATE by serial=%s: 0 rows affected", serial_new)

            # Fallback par yuman_material_id si 0 ligne touchée
            if not updated and e_new.yuman_material_id is not None:
                updates_logger.debug("Fallback UPDATE by yuman_material_id=%s", e_new.yuman_material_id)
                res = (
                    self.sb.table(EQUIP_TABLE)
                    .update(payload)
                    .eq("yuman_material_id", e_new.yuman_material_id)
                    .execute()
                )
                updated = bool(res.data)
                if updated:
                    updates_logger.info("✅ UPDATE OK by yuman_material_id=%s: %d row(s) affected",
                                       e_new.yuman_material_id, len(res.data))

            if not updated:
                updates_logger.error("❌ UPDATE FAILED (0 rows): serial=%s mid=%s vcom=%s | Payload: %s",
                                    serial_new, e_new.yuman_material_id, e_new.vcom_system_key, payload)
                # Log aussi en console pour visibilité
                logger.warning("UPDATE échoué pour serial=%s (voir updates.log pour détails)", serial_new)

        # ---------- DELETE (flag obsolète) ----------
        if patch.delete:
            # priorité au serial si présent
            serials = [_norm_serial(e.serial_number) for e in patch.delete if _norm_serial(e.serial_number)]
            vcom_ids = [e.vcom_device_id for e in patch.delete if not _norm_serial(e.serial_number) and e.vcom_device_id]

            if serials:
                res = (
                    self.sb.table(EQUIP_TABLE)
                    .update({"is_obsolete": True, "obsolete_at": now_iso})
                    .in_("serial_number", serials)
                    .execute()
                )
                logger.debug("[SB] FLAG obsolete by serial %d equips → %s", len(serials), res.data)

            if vcom_ids:
                res = (
                    self.sb.table(EQUIP_TABLE)
                    .update({"is_obsolete": True, "obsolete_at": now_iso})
                    .in_("vcom_device_id", vcom_ids)
                    .execute()
                )
                logger.debug("[SB] FLAG obsolete by vcom_id %d equips → %s", len(vcom_ids), res.data)



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
            "eq_type", "vcom_device_id",
            "serial_number", "brand", "model", "name", "site_id",
            "created_at", "extra", "yuman_material_id", "category_id"
        }
        now_iso = datetime.now(timezone.utc).isoformat()

        # --------------------- ADD / UPSERT (idempotent) ---------------------
        upserts = []
        seen_serials: set[str] = set()

        for e in patch.add:
            # resolve site
            sid = e.site_id
            if sid is None:
                logger.error("[SB] site_id manquant → skip ADD (mid=%s)",
                            e.yuman_material_id)
                continue

            row = {k: v for k, v in e.to_db_dict().items() if k in VALID}
            # normaliser serial
            row["serial_number"] = _norm_serial(row.get("serial_number"))

            if not row["serial_number"]:
                logger.error("[SB] SKIP ADD (serial vide) cat=%s vcom=%s mid=%s",
                            e.category_id, e.vcom_system_key, e.yuman_material_id)
                continue

            # dédoublonner au sein du batch par serial
            if row["serial_number"] in seen_serials:
                logger.warning("[SB] SKIP ADD (doublon batch) serial=%s", row["serial_number"])
                continue
            seen_serials.add(row["serial_number"])

            row["site_id"] = sid
            row.setdefault("created_at", now_iso)
            row["name"] = row.get("name") or row.get("vcom_device_id")

            upserts.append(row)

        if upserts:
            # IMPORTANT :
            # - on_conflict sur 'serial_number' (aligne avec uq_equips_serial)
            # - PAS de ignore_duplicates → DO UPDATE (et pas DO NOTHING)
            res = (
                self.sb.table(TABLE)
                .upsert(upserts, on_conflict="serial_number")
                .execute()
            )
            logger.debug("[SB] UPSERT %d equipsMapping (key=serial_number) → %s",
                        len(upserts), res.data)

        # -------------------------- UPDATE --------------------------
        for old, e in patch.update:
            # resolve site
            sid = e.site_id

            # NE METTRE À JOUR QUE LES CHAMPS QUI ONT CHANGÉ
            payload = {}
            for k, v in e.to_db_dict().items():
                # Skip les champs exclus
                if k in {"vcom_device_id", "yuman_material_id", "vcom_system_key"}:
                    continue
                # Skip les champs non valides
                if k not in VALID:
                    continue
                # Skip si la valeur est None
                if v is None:
                    continue

                # AJOUTER SEULEMENT SI LA VALEUR A CHANGÉ
                old_value = getattr(old, k, None)
                if old_value != v:
                    payload[k] = v

            # Ajouter site_id si résolu et différent
            if sid is not None and sid != old.site_id:
                payload["site_id"] = sid

            if not payload:
                logger.debug("[SB] UPDATE SKIPPED (aucun changement): serial=%s mid=%s",
                            e.serial_number, e.yuman_material_id)
                continue

            # normaliser serial dans le payload si présent
            if "serial_number" in payload:
                payload["serial_number"] = _norm_serial(payload["serial_number"])

            # LOG des changements détectés
            if old:
                changes = {k: (getattr(old, k, None), v) for k, v in payload.items()
                           if getattr(old, k, None) != v}
                if changes:
                    updates_logger.info("UPDATE detected for serial=%s mid=%s | Changes: %s",
                                       e.serial_number, e.yuman_material_id, changes)

            serial_new = _norm_serial(e.serial_number)

            # 1) UPDATE par serial (voie royale)
            updated = False
            if serial_new:
                updates_logger.debug("Attempting UPDATE by serial=%s with payload=%s", serial_new, payload)
                res = (
                    self.sb.table(TABLE)
                    .update(payload)
                    .eq("serial_number", serial_new)
                    .execute()
                )
                # Supabase renvoie [] si 0 ligne, sinon la/les lignes modifiées
                updated = bool(res.data)
                if updated:
                    updates_logger.info("✅ UPDATE OK by serial=%s: %d row(s) affected", serial_new, len(res.data))
                else:
                    updates_logger.warning("❌ UPDATE by serial=%s: 0 rows affected", serial_new)

            # 2) Fallback par yuman_material_id
            if not updated and e.yuman_material_id is not None:
                updates_logger.debug("Fallback UPDATE by yuman_material_id=%s", e.yuman_material_id)
                res = (
                    self.sb.table(TABLE)
                    .update(payload)
                    .eq("yuman_material_id", e.yuman_material_id)
                    .execute()
                )
                updated = bool(res.data)
                if updated:
                    updates_logger.info("✅ UPDATE OK by yuman_material_id=%s: %d row(s) affected",
                                       e.yuman_material_id, len(res.data))

            # 3) Dernier recours : vcom_device_id
            if not updated and e.vcom_device_id:
                updates_logger.debug("Fallback UPDATE by vcom_device_id=%s", e.vcom_device_id)
                res = (
                    self.sb.table(TABLE)
                    .update(payload)
                    .eq("vcom_device_id", e.vcom_device_id)
                    .execute()
                )
                updated = bool(res.data)
                if updated:
                    updates_logger.info("✅ UPDATE OK by vcom_device_id=%s: %d row(s) affected",
                                       e.vcom_device_id, len(res.data))

            if not updated:
                updates_logger.error("❌ UPDATE FAILED (0 rows): serial=%s mid=%s vcom=%s | Payload: %s",
                                    serial_new, e.yuman_material_id, e.vcom_system_key, payload)
                # Log aussi en console pour visibilité
                logger.warning("UPDATE échoué pour serial=%s (voir updates.log pour détails)", serial_new)