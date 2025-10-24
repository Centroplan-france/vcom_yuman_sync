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
import logging
from vysync.app_logging import _dump
from vysync.diff import diff_entities, PatchSet
from vysync.models import (
    Site,
    Equipment,
    Client,
    CAT_INVERTER,
    CAT_MODULE,
    CAT_STRING,
    CAT_CENTRALE,
    CAT_SIM
)
from vysync.yuman_client import YumanClient
from vysync.adapters.supabase_adapter import SupabaseAdapter
import re
from datetime import datetime, timezone
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

# ────────────────────────────── Logging ────────────────────────────────
logger = logging.getLogger(__name__)

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
        Récupère tous les matériels Yuman (modules, onduleurs, strings, SIM, etc.)
        et les normalise en objets `Equipment`.

        • Clé du dictionnaire retourné : `yuman_material_id` (m["id"])
        • Chaque Equipment contient :
            - `site_id`         : clé étrangère Supabase (résolue ici)
            - `yuman_site_id`   : id du site côté Yuman
            - `vcom_system_key` : si le site est déjà mappé à VCOM
        """
        # -------------------------------------------------------------
        # 1) Index rapide : yuman_site_id  ➜  (site_id, vcom_system_key)
        # -------------------------------------------------------------
        sites_by_yid: dict[int, tuple[int, str | None]] = {
            s.yuman_site_id: (s.id, s.vcom_system_key)
            for s in self.fetch_sites().values()
            if s.yuman_site_id is not None
        }

        equips: Dict[str, Equipment] = {}

        # -------------------------------------------------------------
        # 2) Parcours de tous les matériels Yuman
        # -------------------------------------------------------------
        for m in self.yc.list_materials(embed="fields,site"):
            site_info = sites_by_yid.get(m["site_id"])
            if site_info is None:          # site non importé / ignoré
                continue

            site_id, vcom_key = site_info
            cat_id = m["category_id"]

            # --- champs personnalisés --------------------------------
            raw_fields = {
                f["name"]: f.get("value")
                for f in m.get("_embed", {}).get("fields", [])
            }

            # --- reconstruction du vcom_device_id --------------------
            if cat_id == CAT_INVERTER:
                vdid = raw_fields.get(CUSTOM_INVERTER_ID) or m.get("serial_number", "")
            elif cat_id == CAT_STRING:
                vdid = m.get("serial_number") or m["name"]
            elif cat_id == CAT_MODULE:
                vdid = f"MODULES-{vcom_key or 'UNKNOWN'}"
            else:
                vdid = m.get("serial_number") or m["name"]

            # --- typage ----------------------------------------------
            eq_type = (
                "inverter"  if cat_id == CAT_INVERTER  else
                "module"    if cat_id == CAT_MODULE    else
                "string_pv" if cat_id == CAT_STRING    else
                "sim"       if cat_id == CAT_SIM       else
                "plant"     if cat_id == CAT_CENTRALE  else
                "other"
            )

            # --- count (nombre de modules) ---------------------------
            raw_nb = raw_fields.get("nombre de module")
            try:
                count = int(raw_nb) if raw_nb not in (None, "") else None
            except ValueError:
                count = None

            # --- autres normalisations -------------------------------
            mppt_idx     = str(raw_fields.get("MPPT index", "")).strip()
            module_brand = (raw_fields.get("marque du module") or "").strip()
            module_model = (raw_fields.get("model de module")  or "").strip()

            name   = (m.get("name")          or "").strip()
            brand  = (m.get("brand")         or "").strip()
            model  = (raw_fields.get("Modèle")         or "").strip()
            serial = (m.get("serial_number") or "").strip()

            # ---------------------------------------------------------
            # 3) Construction de l'objet Equipment
            # ---------------------------------------------------------
            equip = Equipment(
                site_id          = site_id,          # clé étrangère Supabase
                yuman_site_id    = m["site_id"],     # id Yuman du site
                vcom_system_key  = vcom_key,         # peut être None
                category_id      = cat_id,
                eq_type          = eq_type,
                vcom_device_id   = vdid.strip(),
                name             = name,
                brand            = brand,
                model            = model,
                serial_number    = serial,
                count            = count,
                yuman_material_id = m["id"],
                parent_id        = m.get("parent_id"),
            )

            # champs custom pour diff ultérieur
            object.__setattr__(equip, "mppt_idx",     mppt_idx)
            object.__setattr__(equip, "nb_modules",   str(count or ""))
            object.__setattr__(equip, "module_brand", module_brand)
            object.__setattr__(equip, "module_model", module_model)

            equips[m["serial_number"]] = equip  # clé = serial_number

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
                "name":      re.sub(r'^\d+\s+|\s*\(.*?\)| France', '', s.name),
                "address":   s.address or "",
                "client_id": self._yuman_client_for_site(s),
                # coordonnées si dispo
                **({"latitude": s.latitude}   if s.latitude  is not None else {}),
                **({"longitude": s.longitude} if s.longitude is not None else {}),
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

            # Nom & adresse
            clean_new_name = re.sub(r'^\d+\s+|\s*\(.*?\)| France', '', new.name)
            if old.name != clean_new_name and clean_new_name:
                site_patch["name"] = clean_new_name
            if (old.address or "") != (new.address or ""):
                site_patch["address"] = new.address or ""

            # Latitude / longitude
            if old.latitude != new.latitude and new.latitude is not None:
                site_patch["latitude"] = new.latitude
            if old.longitude != new.longitude and new.longitude is not None:
                site_patch["longitude"] = new.longitude

            # Client ID
            new_client_id = self._yuman_client_for_site(new)
            if old.client_map_id != new.client_map_id and new_client_id is not None:
                site_patch["client_id"] = new_client_id

            # System Key
            if old.vcom_system_key != new.vcom_system_key and new.vcom_system_key:
                fields_patch.append({
                    "blueprint_id": SITE_FIELDS["System Key (Vcom ID)"],
                    "name": "System Key (Vcom ID)",
                    "value": new.vcom_system_key,
                })

            # Nominal Power
            if old.nominal_power != new.nominal_power and new.nominal_power is not None:
                fields_patch.append({
                    "blueprint_id": SITE_FIELDS["Nominal Power (kWc)"],
                    "name": "Nominal Power (kWc)",
                    "value": new.nominal_power,
                })

            # Commission Date
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
            "serial_number" : f"Centrale-{yuman_site_id}"
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
        Fait converger Yuman vers la vérité Supabase pour **tous** les
        équipements (modules, onduleurs, strings, SIM, centrale).

        • add      → création Yuman
        • update   → PATCH Yuman sur *tous* les champs métier listés
        • delete   → flag `is_obsolete = true` côté Supabase + suppression Yuman
        """
        # 1 ─ Fetch / diff si absent
        if patch is None:
            if y_equips is None:
                y_equips = self.fetch_equips()
            patch = diff_entities(y_equips, db_equips)

        # 2 ─ Index (vcom_device_id → yuman_material_id) pour lier les strings
        id_by_vcom: dict[str, int] = {
            e.vcom_device_id: e.yuman_material_id
            for e in (y_equips or {}).values()
            if e.yuman_material_id
        }

        # 3 ─ Constantes utiles
        _ORDER = {CAT_MODULE: 0, CAT_INVERTER: 1, CAT_STRING: 2, CAT_CENTRALE: 3, CAT_SIM: 4}

        BP_MODEL        = 13548
        BP_INVERTER_ID  = 13977
        BP_MPPT_IDX     = STRING_FIELDS["MPPT index"]
        BP_NB_MODULES   = STRING_FIELDS["nombre de module"]
        BP_MODULE_BRAND = STRING_FIELDS["marque du module"]
        BP_MODULE_MODEL = STRING_FIELDS["model de module"]

        # ───────────────────────── INSERTIONS ──────────────────────────── #
        for e in sorted(patch.add, key=lambda x: _ORDER.get(x.category_id, 99)):
            # 3.1 mapping site
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
                "name":          e.name,
                "brand":         e.brand,
                "serial_number": e.serial_number or e.vcom_device_id,
            }
            fields: List[Dict[str, Any]] = []

            # modèle générique
            if e.model:
                fields.append({"blueprint_id": BP_MODEL, "value": e.model})

            # MODULE spécifique (compte, marque, modèle)
            if e.category_id == CAT_MODULE:
                # pas de champs custom pour l’instant : payload suffit
                if e.count is not None:
                    payload["count"] = e.count

            # INVERTER spécifique
            if e.category_id == CAT_INVERTER:
                fields.append({
                    "blueprint_id": BP_INVERTER_ID,
                    "name": "Inverter ID (Vcom)",
                    "value": e.vcom_device_id
                })

            # STRING spécifique
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
                # parent → onduleur
                if e.parent_id and (pid := id_by_vcom.get(e.parent_id)):
                    payload["parent_id"] = pid

            # appliquer les champs custom
            if fields:
                payload["fields"] = fields

            logger.debug("[YUMAN] create_material payload=%s", payload)
            mat = self.yc.create_material(payload)

            # patch immédiat des fields (quota Yuman oblige)
            if fields:
                try:
                    self.yc.update_material(mat["id"], {"fields": fields})
                except Exception as exc:
                    logger.warning("Yuman post‑patch fields failed on %s: %s", mat["id"], exc)

            # persistance en DB
            if e.serial_number:
                self.sb.sb.table("equipments_mapping") \
                    .update({"yuman_material_id": mat["id"]}) \
                    .eq("serial_number", e.serial_number) \
                    .execute()
            else:
                site_id = self.sb._site_id(e.vcom_system_key) if e.vcom_system_key else e.site_id
                if site_id:
                    self.sb.sb.table("equipments_mapping") \
                        .update({"yuman_material_id": mat["id"]}) \
                        .eq("vcom_device_id", e.vcom_device_id) \
                        .eq("site_id", site_id) \
                        .execute()
                else:
                    logger.error("[YUMAN] Cannot update yuman_material_id: no serial, no site_id for vcom_device_id=%s",
                                 e.vcom_device_id)
            id_by_vcom[e.vcom_device_id] = mat["id"]

        # ─────────────────────────  MISE À JOUR  ───────────────────────── #
        for old, new in patch.update:
            # back‑fill yuman_material_id si manquant
            if new.yuman_material_id is None and old.yuman_material_id:
                if new.serial_number:
                    self.sb.sb.table("equipments_mapping") \
                        .update({"yuman_material_id": old.yuman_material_id}) \
                        .eq("serial_number", new.serial_number) \
                        .execute()
                else:
                    site_id = self.sb._site_id(new.vcom_system_key) if new.vcom_system_key else new.site_id
                    if site_id:
                        self.sb.sb.table("equipments_mapping") \
                            .update({"yuman_material_id": old.yuman_material_id}) \
                            .eq("vcom_device_id", new.vcom_device_id) \
                            .eq("site_id", site_id) \
                            .execute()
                    else:
                        logger.error("[YUMAN] Cannot update yuman_material_id: no serial, no site_id for vcom_device_id=%s",
                                     new.vcom_device_id)

            payload: Dict[str, Any] = {}
            fields_patch: List[Dict[str, Any]] = []

            # -------- CHAMPS COMMUNS (toutes catégories) --------
            def _set(attr: str, target: Dict[str, Any] = payload):
                ov, nv = getattr(old, attr), getattr(new, attr)
                if (ov or "") != (nv or "") and nv is not None:
                    target[attr] = nv

            _set("name")
            _set("brand")
            _set("serial_number")
            _set("count")

            # -------- CAT_SPÉCIFIQUES --------
            if old.category_id == CAT_INVERTER:
                if old.vcom_device_id != new.vcom_device_id:
                    fields_patch.append({"blueprint_id": BP_INVERTER_ID,
                                        "value": new.vcom_device_id})
                if old.model != new.model:
                    fields_patch.append({"blueprint_id": BP_MODEL,
                                        "value": new.model})

            if old.category_id == CAT_MODULE:
                if old.model != new.model:
                    fields_patch.append({"blueprint_id": BP_MODEL,
                                        "value": new.model})
                    
            if old.category_id == CAT_STRING:
                # parent
                if new.parent_id and old.parent_id != new.parent_id:
                    parent_mat = id_by_vcom.get(new.parent_id)
                    if parent_mat:
                        payload["parent_id"] = parent_mat
                # champs custom
                def _maybe(bp, ov, nv):
                    if (ov or "") != (nv or ""):
                        fields_patch.append({"blueprint_id": bp, "value": nv})

                old_mppt     = getattr(old, "mppt_idx", "")
                old_nb       = getattr(old, "nb_modules", "")
                old_bmod     = getattr(old, "module_brand", "")
                old_mmodel   = getattr(old, "module_model", "")

                try:
                    new_mppt = new.vcom_device_id.split("-MPPT-")[1].split(".")[0]
                except Exception:
                    new_mppt = "?"

                _maybe(BP_MPPT_IDX,     old_mppt,   new_mppt)
                _maybe(BP_NB_MODULES,   old_nb,     str(new.count or ""))
                _maybe(BP_MODULE_BRAND, old_bmod,   new.brand or "")
                _maybe(BP_MODULE_MODEL, old_mmodel, new.model or "")

            # aucun champ custom particulier pour CAT_MODULE / CAT_SIM /
            # CAT_CENTRALE ; le payload générique suffit.

            if fields_patch:
                payload["fields"] = fields_patch

            if payload:
                logger.debug("[YUMAN] update_material %s payload=%s",
                            old.yuman_material_id, payload)
                self.yc.update_material(old.yuman_material_id, payload)

        # ─────────────────────────  DELETE  ─────────────────────────── #
        # if patch.delete:
        #     # flag « obsoletes » côté Supabase + suppression Yuman
        #     dev_ids = [e.vcom_device_id for e in patch.delete]
        #     self.sb.sb.table("equipments_mapping") \
        #         .update({"is_obsolete": True, "obsolete_at": _now_iso()}) \
        #         .in_("vcom_device_id", dev_ids) \
        #         .execute()

        #     for e in patch.delete:
        #         if e.yuman_material_id:
        #             try:
        #                 self.yc.delete_material(e.yuman_material_id)
        #             except Exception as exc:
        #                 logger.warning("Yuman delete_material failed on %s: %s",
        #                             e.yuman_material_id, exc)
