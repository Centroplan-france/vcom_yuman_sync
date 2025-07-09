from __future__ import annotations

# ===============================
# File: vysync/adapters/vcom_adapter.py
# ===============================
"""Transforme la sortie de VCOMAPIClient en snapshot ``Site`` / ``Equipment``."""


from typing import Dict, Tuple
from vysync.models import Site, Equipment, CAT_INVERTER, CAT_MODULE, CAT_STRING
from vysync.vcom_client import VCOMAPIClient  # réutilise ton client existant
from vysync.app_logging import init_logger, _dump
from vysync.models import (
    Site,
    Equipment,
    CAT_INVERTER,
    CAT_MODULE,
    CAT_STRING,
)

logger = init_logger(__name__)


def fetch_snapshot(vc, vcom_system_key: str | None = None,) -> Tuple[Dict[str, Site], Dict[Tuple[str, str], Equipment]]:
    """Retourne deux dictionnaires : ``sites`` et ``equips``.

    • Si ``vcom_system_key`` est fourni, on ne récupère que ce système.  
    • Les STRING PV sont inclus ; leur ``parent_vcom_id`` pointe vers
      l’onduleur (utile plus tard pour déterminer la hiérarchie).
    """
    sites: Dict[str, Site] = {}
    equips: Dict[tuple[str, str], Equipment] = {}

    for sys in vc.get_systems():
        key = sys["key"]
        if vcom_system_key and key != vcom_system_key:
            continue
        tech = vc.get_technical_data(key)
        det = vc.get_system_details(key)

        # --- Site ------------------------------------------------------
        site = Site(
            vcom_system_key = key,
            name            = sys.get("name") or key,
            latitude        = det.get("coordinates", {}).get("latitude"),
            longitude       = det.get("coordinates", {}).get("longitude"),
            nominal_power   = tech.get("nominalPower"),
            commission_date = det.get("commissionDate"),
            address         = det.get("address", {}).get("street"),
        )
        sites[site.key()] = site

        # --- Modules ---------------------------------------------------
        panels = tech.get("panels") or []
        if panels:
            p = panels[0]
            mod = Equipment(
                vcom_system_key = key,
                category_id     = CAT_MODULE,
                eq_type         = "module",
                vcom_device_id  = f"MODULES-{key}",
                name            = "Modules",
                brand           = p.get("vendor"),
                model           = p.get("model"),
                count           = p.get("count"),
            )
            equips[mod.key()] = mod

        # --- Onduleurs -------------------------------------------------
        inverters = vc.get_inverters(key)

        # on garantit un ordre stable pour attribuer les index (WR 1, WR 2, …)
        for idx, inv in enumerate(inverters, start=1):

            det_inv = vc.get_inverter_details(key, inv["id"])

            inv_eq = Equipment(
                vcom_system_key = key,
                category_id     = CAT_INVERTER,
                eq_type         = "inverter",
                vcom_device_id  = inv["id"],
                name            = f"WR {idx} - Onduleur",
                brand           = det_inv.get("vendor"),
                model           = det_inv.get("model"),
                serial_number   = inv.get("serial"),
            )
            equips[inv_eq.key()] = inv_eq

        # STRING PV ------------------------------------------------------
        inv_by_idx   = {idx: inv for idx, inv in enumerate(inverters, start=1)}
        
        SLOTS_PER_MPPT = 2          # hypothèse : au max 2 strings (1.1 / 1.2) par MPPT

        for idx_cfg, cfg in enumerate(tech.get("systemConfigurations", []), start=1):
            inv = inv_by_idx.get(idx_cfg)
            if not inv:
                continue

            slot_idx = 0  # index théorique de slot pour l'onduleur
            mppt_inputs = cfg.get("mpptInputs", {})
            # tri numérique des MPPT : "1", "2", "3" ...
            for mppt_num in sorted(mppt_inputs, key=int):
                inp = mppt_inputs[mppt_num]

                for n in range(1, SLOTS_PER_MPPT + 1):     # slot 1 puis 2
                    slot_idx += 1                          # avance toujours
                    if n > inp["stringCount"]:
                        continue                           # slot vide -> on saute la création
                    parent_vcom = inv["id"]        
                    idx_str   = f"{mppt_num}.{n}"          # ex. "3.1"
                    vdid_base = f"STRING-{slot_idx}-WR{idx_cfg}-MPPT-{idx_str}"
                    vdid_unique   = f"{vdid_base}-{key}"       # ← unicité inter-sites
                    str_eq = Equipment(
                        vcom_system_key = key,
                        category_id     = CAT_STRING,
                        eq_type         = "string_pv",
                        vcom_device_id  = vdid_unique,         # DB/Yuman → serial_number
                        name            = vdid_base,           # Yuman « name » sans clé site
                        brand=inp["module"].get("vendor"),
                        model=inp["module"].get("model"),
                        serial_number = vdid_unique,
                        count=inp["modulesPerString"],
                        parent_id=parent_vcom,
                    )
                    equips[str_eq.key()] = str_eq

    logger.info("[VCOM] snapshot: %s sites, %s equips", len(sites), len(equips))
    _dump("[VCOM] sites", {k: s.to_dict() for k,s in sites.items()})
    _dump("[VCOM] equips", {k: e.to_dict() for k,e in equips.items()})
    return sites, equips
