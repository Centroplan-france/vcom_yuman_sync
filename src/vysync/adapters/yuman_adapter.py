#!/usr/bin/env python3
from __future__ import annotations

"""
File: vysync/adapters/yuman_adapter.py

Synchronisation Yuman ⇄ Supabase / VCOM.

– Toutes les requêtes HTTP sont journalisées en DEBUG.  
– Limite d’API ~60 req/min, gérée directement par le client Yuman.  
– La source de vérité reste la base Supabase ; Yuman est créé /  
  mis à jour si nécessaire. Les IDs Yuman générés sont ensuite  
  réinjectés en base.
"""

from typing import Dict, List, Tuple, Optional, Any
from dataclasses import asdict, fields as dc_fields
from vysync.app_logging import init_logger, _dump
from vysync.diff import diff_entities, PatchSet
from vysync.models import (
    Site,
    Equipment,
    Client, 
    CAT_INVERTER,
    CAT_MODULE,
    CAT_STRING,
    CAT_CENTRALE
)
from vysync.yuman_client import YumanClient
from vysync.adapters.supabase_adapter import SupabaseAdapter
import re
from datetime import datetime, timezone
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

# ────────────────────────────── Logging ────────────────────────────────
logger = init_logger(__name__)

# ───────────────────────── Blueprints custom fields ────────────────────
SITE_FIELDS = {
    "System Key (Vcom ID)": 13583,
    "Nominal Power (kWc)":  13585,
    "Commission Date":      13586,
}
STRING_FIELDS = {
    "MPPT index":       16020,
    "nombre de module": 16021,
    "marque du module": 16022,
    "model de module":  16023,
}
CUSTOM_INVERTER_ID = "Inverter ID (Vcom)"

# ───────────────────────────── Adapter Yuman ───────────────────────────
class YumanAdapter:
    def __init__(self, sb_adapter: SupabaseAdapter) -> None:
        self.yc = YumanClient()
        self.sb = sb_adapter  # accès indirect à Supabase


    # ───────────────────────── Helpers Clients ───────────────────────────
    def _yuman_client_for_site(self, site_row) -> int:
        """
        Retourne l'id Yuman du client associé à un site.
        Accepte un objet Site ou un dict.
        """
        # --- 1. ID interne ----------------------------------------------
        cm_id = getattr(site_row, "client_map_id", None)
        if cm_id is None and isinstance(site_row, dict):
            cm_id = site_row.get("client_map_id")

        if not cm_id:
            site_name = getattr(site_row, "name", None)
            if site_name is None and isinstance(site_row, dict):
                site_name = site_row.get("name")
            raise RuntimeError(
                f"[SITE] « {site_name or 'unknown'} » n’a pas de client_map_id – "
                "exécute resolve_clients_for_sites."
            )

        # --- 2. Lookup clients_mapping ----------------------------------
        row = (
            self.sb.sb.table("clients_mapping")
            .select("yuman_client_id")
            .eq("id", cm_id)
            .single()
            .execute()
            .data
        )

        yid = row["yuman_client_id"] if row else None
        if not yid:
            raise RuntimeError(
                f"[CLIENT] clients_mapping.id={cm_id} sans yuman_client_id."
            )
        return yid


    # ------------------------------------------------------------------ #
    #  SNAPSHOTS                                                         #
    # ------------------------------------------------------------------ #
    def fetch_sites(self) -> Dict[str, Site]:
      """
      Retourne un dictionnaire de *tous* les sites Yuman.
  
      ➜  Clé du dictionnaire
          site_id
      """
      sites: Dict[str, Site] = {}
  
      # 1) Itération brute de l’API Yuman
      for s in self.yc.list_sites(embed="fields,client"):
          # --- Custom fields → dict {nom: valeur}
          cvals = {
              f["name"]: f.get("value")
              for f in s.get("_embed", {}).get("fields", [])
          }
  
          vcom_key = (cvals.get("System Key (Vcom ID)") or "").strip() or None
          yuman_site_id   = s["id"]
  
          # Champs optionnels
          aldi_id           = (cvals.get("ALDI ID")                    or "").strip() or None
          aldi_store_id     = (cvals.get("ID magasin (n° interne Aldi)") or "").strip() or None
          project_number_cp = (cvals.get("Project number (Centroplan ID)") or "").strip() or None
  
          # Date de commission → ISO
          raw_cd = (cvals.get("Commission Date") or "").strip()
          if raw_cd and "/" in raw_cd:            # "JJ/MM/AAAA"
              j, m, a = raw_cd.split("/")[:3]
              commission_iso = f"{a}-{m.zfill(2)}-{j.zfill(2)}"
          else:
              commission_iso = raw_cd or None     # "" → None
  
          # --- Construction de l’objet Site
          site_obj = Site(
              vcom_system_key = vcom_key,
              name            = s.get("name"),
              address         = s.get("address"),
              commission_date = commission_iso,
              nominal_power   = (
                  float(cvals["Nominal Power (kWc)"])
                  if cvals.get("Nominal Power (kWc)") else None
              ),
              latitude        = s.get("latitude"),
              longitude       = s.get("longitude"),
              yuman_site_id   = s["id"],
              aldi_id           = aldi_id,
              aldi_store_id     = aldi_store_id,
              project_number_cp = project_number_cp,
          )
  
          # --- Choix de la clé du dict
          key = yuman_site_id
          sites[key] = site_obj
  
      logger.debug("[YUMAN] snapshot: %d sites",
                   len(sites),
                   )
      _dump("[YUMAN] snapshot sites", sites)
      return sites

    # ------------------------------------------------------------------ #
    #  LECTURE DES ÉQUIPEMENTS YUMAN                                     #
    # ------------------------------------------------------------------ #
    def fetch_equips(self) -> Dict[str, Equipment]:
        """
        Charge tous les matériels (modules, onduleurs, strings) déjà mappés.
        Clef : vcom_device_id. On normalise ici tous les champs custom.
        """
        sites_by_id = {s.yuman_site_id: s for s in self.fetch_sites().values()}
        equips: Dict[str, Equipment] = {}

        for m in self.yc.list_materials(embed="fields,site"):
            site = sites_by_id.get(m["site_id"])
            if not site:
                continue

            cat_id = m["category_id"]
            raw_fields = {
                f["name"]: f.get("value")
                for f in m.get("_embed", {}).get("fields", [])
            }

            # — rebuild vcom_device_id —
            if cat_id == CAT_INVERTER:
                vdid = raw_fields.get(CUSTOM_INVERTER_ID) or m.get("serial_number", "")
            elif cat_id == CAT_STRING:
                vdid = m.get("serial_number") or m["name"]
            elif cat_id == CAT_MODULE:
                vdid = f"MODULES-{site.vcom_system_key}"
            else:
                vdid = m.get("serial_number") or m["name"]

            # — type pour le dataclass
            eq_type = (
                "inverter" if cat_id == CAT_INVERTER else
                "module"   if cat_id == CAT_MODULE else
                "string_pv"
            )

            # — normalisation du count (nombre de module) —
            raw_nb = raw_fields.get("nombre de module")
            if raw_nb is None or raw_nb == "":
                count = None
            else:
                # forcer int
                try:
                    count = int(raw_nb)
                except ValueError:
                    count = None

            # — normalisation MPPT index —
            raw_mppt = raw_fields.get("MPPT index")
            mppt_idx = str(raw_mppt).strip() if raw_mppt is not None else ""

            # — normalisation des autres custom fields —
            module_brand = (raw_fields.get("marque du module") or "").strip()
            module_model = (raw_fields.get("model de module")  or "").strip()

            # — strip name/brand/model/serial_number pour éviter les espaces parasites
            name  = (m.get("name") or "").strip()
            brand = (m.get("brand") or "").strip()
            model = (m.get("model") or "").strip()
            serial = (m.get("serial_number") or "").strip()

            equip = Equipment(
                vcom_system_key = site.vcom_system_key,
                category_id     = cat_id,
                eq_type         = eq_type,
                vcom_device_id  = vdid.strip(),
                name            = name,
                brand           = brand,
                model           = model,
                serial_number   = serial,
                count           = count,
                yuman_material_id = m["id"],
                parent_id       = m.get("parent_id"),
            )

            # — on stocke les custom attribs pour le diff plus tard —
            object.__setattr__(equip, "mppt_idx",     mppt_idx)
            object.__setattr__(equip, "nb_modules",   str(count or ""))
            object.__setattr__(equip, "module_brand", module_brand)
            object.__setattr__(equip, "module_model", module_model)

            
            key =   m["id"]
            equips[key] = equip

        logger.debug("[YUMAN] snapshot: %s equips", len(equips))
        _dump("[YUMAN] snapshot equips", equips)
        return equips


    # ------------------------------------------------------------------ #
    #  APPLY PATCH – SITES                                               #
    # ------------------------------------------------------------------ #
    def apply_sites_patch(
        self,
        db_sites: Dict[str, Site],
        *,
        y_sites: dict[str, Site] | None = None,
        patch: PatchSet[Site] | None = None,
    ) -> None:
        """
        Fait converger Yuman vers la vérité Supabase (db_sites).
        Si `y_sites` est fourni, on l’utilise plutôt que de refetch.
        Si `patch` est fourni, on l’utilise sans recomparer.
        """
        # 1) récupération / diff
        if patch is None:
            if y_sites is None:
                y_sites = self.fetch_sites()
            patch = diff_entities(y_sites, db_sites)

        # 2) ADD
        for s in patch.add:
            payload = {
                "name":     re.sub(r'^\d+\s+|\s*\(.*?\)| France', '', s.name),
                "address":  s.address or "",
                "client_id": self._yuman_client_for_site(s),
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
            new_site = self.yc.create_site(payload)
            # propager l’ID en DB
            self.sb.sb.table("sites_mapping") \
                .update({"yuman_site_id": new_site["id"]}) \
                .eq("vcom_system_key", s.vcom_system_key) \
                .execute()
            self._ensure_centrale(new_site["id"])

        # 3) UPDATE
        for old, new in patch.update:
            site_patch: dict[str, Any] = {}
            fields_patch: list[dict[str, Any]] = []

            # nom & address
            clean_new_name = re.sub(r'^\d+\s+|\s*\(.*?\)| France', '', new.name)
            if old.name != clean_new_name and clean_new_name:
                site_patch["name"] = clean_new_name
            if (old.address or "") != (new.address or ""):
                site_patch["address"] = new.address or ""

            # nominal_power
            if old.nominal_power != new.nominal_power and new.nominal_power is not None:
                fields_patch.append({
                    "blueprint_id": SITE_FIELDS["Nominal Power (kWc)"],
                    "name": "Nominal Power (kWc)",
                    "value": new.nominal_power,
                })
            # commission_date
            if old.commission_date != new.commission_date and new.commission_date:
                fields_patch.append({
                    "blueprint_id": SITE_FIELDS["Commission Date"],
                    "name": "Commission Date",
                    "value": new.commission_date,
                })

            if fields_patch:
                site_patch["fields"] = fields_patch

            if site_patch:
                logger.debug("[YUMAN] update_site %s payload=%s", old.yuman_site_id, site_patch)
                self.yc.update_site(old.yuman_site_id, site_patch)
            # back‑fill Yuman ID si besoin
            if new.yuman_site_id is None and old.yuman_site_id:
                self.sb.sb.table("sites_mapping") \
                    .update({"yuman_site_id": old.yuman_site_id}) \
                    .eq("vcom_system_key", new.vcom_system_key) \
                    .execute()
                
    # ------------------------------------------------------------------
    #  Équipement “Centrale”                                             #
    # ------------------------------------------------------------------
    def _ensure_centrale(self, yuman_site_id: int) -> None:
        if any(m["id"] for m in self.yc.list_materials(
                category_id=CAT_CENTRALE, embed=None)
                if m["site_id"] == yuman_site_id):
            return                       # déjà présent
        self.yc.create_material({
            "site_id":     yuman_site_id,
            "name":        "Centrale",
            "category_id": CAT_CENTRALE,
        })


    # ------------------------------------------------------------------ #
    #  APPLY PATCH – EQUIPMENTS                                          #
    # ------------------------------------------------------------------ #
    def apply_equips_patch(
        self,
        db_equips: Dict[str, Equipment],
        *,
        y_equips: Dict[str, Equipment] | None = None,
        patch: PatchSet[Equipment] | None = None,
    ) -> None:
        """
        Synchronise modules, onduleurs et strings entre VCOM (db_equips)
        et Yuman (liste courante). On peut passer y_equips ou directement patch
        pour éviter un double fetch/diff.
        """
        # 1) récupération / diff si besoin
        if patch is None:
            if y_equips is None:
                y_equips = self.fetch_equips()
            patch = diff_entities(y_equips, db_equips)

        # cache {vcom_device_id → yuman_material_id} pour lien parent string→onduleur
        id_by_vcom = {
            e.vcom_device_id: e.yuman_material_id
            for e in (y_equips or {}).values()
            if e.yuman_material_id
        }

        # ordre d’insertion : modules → onduleurs → strings
        _ORDER = {CAT_MODULE: 0, CAT_INVERTER: 1, CAT_STRING: 2}

        # Blueprints custom
        BP_MODEL        = 13548
        BP_INVERTER_ID  = 13977
        BP_MPPT_IDX     = STRING_FIELDS["MPPT index"]
        BP_NB_MODULES   = STRING_FIELDS["nombre de module"]
        BP_MODULE_BRAND = STRING_FIELDS["marque du module"]
        BP_MODULE_MODEL = STRING_FIELDS["model de module"]

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
                "site_id":       site_row["yuman_site_id"],
                "category_id":   e.category_id,
                "brand":         e.brand,
                "serial_number": e.serial_number or e.vcom_device_id,
                "name":          e.name,
            }
            fields: List[Dict[str, Any]] = []

            # blueprint "Modèle"
            if e.model:
                fields.append({"blueprint_id": BP_MODEL, "name": "Modèle", "value": e.model})
                payload["model"] = e.model

            # champ custom "Numéro de série"
            if e.serial_number:
                payload["Numéro de série"] = e.serial_number

            # blueprint "Inverter ID (Vcom)"
            if e.category_id == CAT_INVERTER:
                fields.append({
                    "blueprint_id": BP_INVERTER_ID,
                    "name": "Inverter ID (Vcom)",
                    "value": e.vcom_device_id
                })

            # champs string_pv
            if e.category_id == CAT_STRING:
                try:
                    mppt_idx = e.vcom_device_id.split("-MPPT-", 1)[1]
                except Exception:
                    mppt_idx = "?"
                fields.extend([
                    {"blueprint_id": BP_MPPT_IDX,     "value": mppt_idx},
                    {"blueprint_id": BP_NB_MODULES,   "value": str(e.count or "")},
                    {"blueprint_id": BP_MODULE_BRAND, "value": e.brand},
                    {"blueprint_id": BP_MODULE_MODEL, "value": e.model},
                ])

            if fields:
                payload["fields"] = fields

            # associer strings → onduleur
            if e.category_id == CAT_STRING and e.parent_id:
                parent_mat = id_by_vcom.get(e.parent_id)
                if parent_mat:
                    payload["parent_id"] = parent_mat

            logger.debug("[YUMAN] create_material payload=%s", payload)
            mat = self.yc.create_material(payload)
            _dump("[YUMAN] material created", mat)

            # re-patcher immédiatement les fields (limite API)
            if "fields" in payload and payload["fields"]:
                try:
                    self.yc.update_material(mat["id"], {"fields": payload["fields"]})
                except Exception as exc:
                    logger.warning("Yuman post-patch (fields) failed on %s: %s",
                                   mat["id"], exc)

            # stocke l’ID Yuman en DB et met à jour le cache
            self.sb.sb.table("equipments_mapping") \
                .update({"yuman_material_id": mat["id"]}) \
                .eq("vcom_device_id", e.vcom_device_id) \
                .eq("vcom_system_key", e.vcom_system_key) \
                .execute()
            id_by_vcom[e.vcom_device_id] = mat["id"]

        # ─────────────────────────  MISE À JOUR  ─────────────────────────
        for old, new in patch.update:
            # back-fill de l’ID Yuman si manquant
            if new.yuman_material_id is None and old.yuman_material_id:
                self.sb.sb.table("equipments_mapping") \
                    .update({"yuman_material_id": old.yuman_material_id}) \
                    .eq("vcom_device_id", new.vcom_device_id) \
                    .eq("vcom_system_key", new.vcom_system_key) \
                    .execute()

            payload: Dict[str, Any] = {}
            fields_patch: List[Dict[str, Any]] = []

            # renommage onduleur
            if old.category_id == CAT_INVERTER and old.name != new.name:
                payload["name"] = new.name

            # mise à jour parent pour string
            if old.category_id == CAT_STRING and new.parent_id:
                parent_mat = id_by_vcom.get(new.parent_id)
                if parent_mat and old.parent_id != parent_mat:
                    payload["parent_id"] = parent_mat

            # custom "Inverter ID (Vcom)"
            if old.category_id == CAT_INVERTER and old.vcom_device_id != new.vcom_device_id:
                fields_patch.append({"blueprint_id": BP_INVERTER_ID, "value": new.vcom_device_id})

            # custom "Model"
            if old.model != new.model and new.model:
                fields_patch.append({"blueprint_id": BP_MODEL, "value": new.model})

            # champs custom string
            if old.category_id == CAT_STRING:
                def _maybe(bp, ov, nv):
                    if (ov or "") != (nv or ""):
                        fields_patch.append({"blueprint_id": bp, "value": nv})

                old_mppt      = getattr(old, "mppt_idx", "")
                old_nb        = getattr(old, "nb_modules", "")
                old_brand     = getattr(old, "module_brand", "")
                old_modmodel  = getattr(old, "module_model", "")

                try:
                    new_mppt = new.vcom_device_id.split("-MPPT-")[1].split(".")[0]
                except Exception:
                    new_mppt = "?"
                new_nb        = str(new.count or "")
                new_brand     = new.brand or ""
                new_modmodel  = new.model or ""

                _maybe(BP_MPPT_IDX,     old_mppt,     new_mppt)
                _maybe(BP_NB_MODULES,   old_nb,       new_nb)
                _maybe(BP_MODULE_BRAND, old_brand,    new_brand)
                _maybe(BP_MODULE_MODEL, old_modmodel, new_modmodel)

            if fields_patch:
                payload["fields"] = fields_patch

            if payload:
                logger.debug("[YUMAN] update_material %s payload=%s",
                             old.yuman_material_id, payload)
                self.yc.update_material(old.yuman_material_id, payload)
