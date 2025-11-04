from __future__ import annotations

# ===============================
# File: vysync/adapters/vcom_adapter.py
# ===============================
"""Transforme la sortie de VCOMAPIClient en snapshot ``Site`` / ``Equipment``."""


from typing import Dict, Tuple, Any
import logging
from vysync.app_logging import _dump
from vysync.models import Site, Equipment, CAT_INVERTER, CAT_MODULE, CAT_STRING

logger = logging.getLogger(__name__)


def build_address(addr: Dict[str, Any]) -> str | None:
    if not addr:
        return None
    parts = [addr.get("street"), f"{addr.get('postalCode', '')} {addr.get('city', '')}".strip()]
    return ", ".join(filter(None, parts)) or None

from typing import Tuple, Dict, TYPE_CHECKING

if TYPE_CHECKING:
    from vysync.adapters.supabase_adapter import SupabaseAdapter

def fetch_snapshot(vc, vcom_system_key: str | None = None, skip_keys: set[str] | None = None, sb_adapter: 'SupabaseAdapter | None' = None) -> Tuple[Dict[str, Site], Dict[Tuple[str, str], Equipment]]:
    """Retourne deux dictionnaires : ``sites`` et ``equips``.

    • Si ``vcom_system_key`` est fourni, on ne récupère que ce système.
    • Les STRING PV sont inclus ; leur ``parent_vcom_id`` pointe vers l'onduleur
      (utile plus tard pour déterminer la hiérarchie).
    • Si ``sb_adapter`` est fourni, on utilise le cache sites_mapping pour construire site_id.
    """
    sites: Dict[str, Site] = {}
    equips: Dict[tuple[str, str], Equipment] = {}

    # Créer un mapping vcom_system_key → site_id si sb_adapter disponible
    vcom_to_site_id: Dict[str, int] = {}
    if sb_adapter:
        vcom_to_site_id = sb_adapter._map_vcom_to_id.copy()

    for sys in vc.get_systems():
        key = sys["key"]
        # -- filtre ----------------------------------------------------------------
        if vcom_system_key and key != vcom_system_key:
            continue                         # ① on ne veut qu’un site précis
        if skip_keys and key in skip_keys:
            continue                         # ② déjà connu en DB – on saute

        tech = vc.get_technical_data(key)
        det  = vc.get_system_details(key)

        # --- Site ----------------------------------------------------------------
        site = Site(
            name            = sys.get("name") or key,
            vcom_system_key = key,  # identifiant VCOM
            yuman_site_id   = None,  # NULL pour sites VCOM
            id              = vcom_to_site_id.get(key) if vcom_to_site_id else None,
            latitude        = det.get("coordinates", {}).get("latitude"),
            longitude       = det.get("coordinates", {}).get("longitude"),
            nominal_power   = tech.get("nominalPower"),
            commission_date = det.get("commissionDate"),
            address         = build_address(det.get("address", {})),
            site_area       = tech.get("siteArea"),
        )
        sites[key] = site  # Indexé par vcom_system_key (variable locale 'key')

        # Résoudre site_id via le mapping
        site_id = vcom_to_site_id.get(key) if vcom_to_site_id else None

        # -------------------------------------------------------------------------
        # SIM (category_id = 11382), eq_type = "sim"
        sim_sn = f"SIM-{key}"
        sim_eq = Equipment(
            site_id         = site_id,
            category_id     = 11382,            # SIM
            eq_type         = "sim",
            vcom_device_id  = sim_sn,             # demandé
            serial_number   = sim_sn,             # demandé
            name            = "Carte SIM",
        )
        equips[sim_eq.key()] = sim_eq

        # -------------------------------------------------------------------------
        # PLANT (category_id = 11441), eq_type = "plant"
        plant_sn = f"central-{key}"            # demandé : "Centrale-<vcom_system_key>"
        plant_eq = Equipment(
            site_id         = site_id,
            category_id     = 11441,            # PLANT
            eq_type         = "plant",
            vcom_device_id  = plant_sn,         # demandé
            serial_number   = plant_sn,         # demandé
            name            = "Centrale",
        )
        equips[plant_eq.key()] = plant_eq
        

        # --- Modules --------------------------------------------------------------
        panels = tech.get("panels") or []
        if panels:
            p = panels[0]
            mod = Equipment(
                site_id         = site_id,
                category_id     = CAT_MODULE,
                eq_type         = "module",
                vcom_device_id  = f"MODULES-{key}",
                serial_number   = f"MODULES-{key}",
                name            = "Modules",
                brand           = p.get("vendor"),
                model           = p.get("model"),
                count           = p.get("count"),
            )
            equips[mod.key()] = mod

        # --- Onduleurs -----------------------------------------------------------
        inverters = vc.get_inverters(key)

        # on garantit un ordre stable pour attribuer les index (WR 1, WR 2, …)
        for idx, inv in enumerate(inverters, start=1):
            # Source unique et fiable : get_inverter_details()
            det_inv = vc.get_inverter_details(key, inv["id"])
            brand = det_inv.get("vendor") or None
            model = det_inv.get("model") or None

            # Si vide : on log mais on ne remplace pas (protection des données DB)
            if not brand or not model:
                logger.warning(
                    f"⚠️  Onduleur {inv['id']} (site {key}) sans vendor/model dans l'API VCOM"
                )

            inv_eq = Equipment(
                site_id         = site_id,
                category_id     = CAT_INVERTER,
                eq_type         = "inverter",
                vcom_device_id  = inv["id"],
                name            = f"WR {idx} - Onduleur",
                brand           = brand,  # Peut être None
                model           = model,  # Peut être None
                serial_number   = inv.get("serial"),
            )
            equips[inv_eq.key()] = inv_eq

        # --- STRING PV -----------------------------------------------------------
        inv_by_idx      = {idx: inv for idx, inv in enumerate(inverters, start=1)}
        SLOTS_PER_MPPT  = 2  # au max 2 strings (1.1 / 1.2) par MPPT

        for idx_cfg, cfg in enumerate(tech.get("systemConfigurations", []), start=1):
            inv = inv_by_idx.get(idx_cfg)
            if not inv:
                continue

            slot_idx    = 0  # index théorique de slot pour l'onduleur
            mppt_inputs = cfg.get("mpptInputs", {})

            # tri numérique des MPPT : "1", "2", "3" ...
            for mppt_num in sorted(mppt_inputs, key=int):
                inp = mppt_inputs[mppt_num]

                for n in range(1, SLOTS_PER_MPPT + 1):   # slot 1 puis 2
                    slot_idx += 1                        # avance toujours
                    if n > inp["stringCount"]:
                        continue                         # slot vide -> on saute

                    parent_vcom = inv["id"]
                    idx_str     = f"{mppt_num}.{n}"      # ex. "3.1"

                    # forcer deux chiffres sur l'index de string
                    slot_label  = f"{slot_idx:02d}"      # 1 -> "01", 7 -> "07", 12 -> "12"

                    # utiliser le label paddé pour le nom et l'ID
                    vdid_base   = f"STRING-{slot_label}-WR{idx_cfg}-MPPT-{idx_str}"
                    vdid_unique = f"{vdid_base}-{key}"   # unicité inter-sites

                    str_eq = Equipment(
                        site_id         = site_id,
                        category_id     = CAT_STRING,
                        eq_type         = "string_pv",
                        vcom_device_id  = vdid_unique,   # DB/Yuman → serial_number
                        name            = vdid_base,     # Yuman « name » sans clé site
                        brand           = inp["module"].get("vendor"),
                        model           = inp["module"].get("model"),
                        serial_number   = vdid_unique,
                        count           = inp["modulesPerString"],
                        parent_id       = inv.get("serial"),
                    )
                    equips[str_eq.key()] = str_eq


    logger.info("[VCOM] snapshot: %s sites, %s equips", len(sites), len(equips))
    _dump("[VCOM] sites", {k: s.to_dict() for k,s in sites.items()})
    _dump("[VCOM] equips", {k: e.to_dict() for k,e in equips.items()})
    return sites, equips
