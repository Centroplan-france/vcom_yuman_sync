from __future__ import annotations

# ===============================
# File: vysync/adapters/vcom_adapter.py
# ===============================
"""Transforme la sortie de VCOMAPIClient en snapshot ``Site`` / ``Equipment``."""


from typing import Dict, Tuple
from vysync.models import Site, Equipment, CAT_INVERTER, CAT_MODULE, CAT_STRING
from vysync.vcom_client import VCOMAPIClient  # réutilise ton client existant
from vysync.app_logging import init_logger
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

        site = Site(
            vcom_system_key=key,
            name=sys.get("name") or key,
            latitude=det.get("coordinates", {}).get("latitude"),
            longitude=det.get("coordinates", {}).get("longitude"),
            nominal_power=tech.get("nominalPower"),
            commission_date=det.get("commissionDate"),
            address=det.get("address", {}).get("street"),
        )
        sites[site.key()] = site

        # Modules (on suppose une seule référence)
        panels = tech.get("panels") or []
        if panels:
            p = panels[0]
            mod = Equipment(
                vcom_system_key=key,
                category_id=CAT_MODULE,
                eq_type="module",
                vcom_device_id=f"MODULES-{key}",
                name=p.get("model") or "Modules",
                brand=p.get("vendor"),
                model=p.get("model"),
                count=p.get("count"),
            )
            equips[mod.key()] = mod

        # Onduleurs
        for inv in vc.get_inverters(key):
            det_inv = vc.get_inverter_details(key, inv["id"])
            inv_eq = Equipment(
                vcom_system_key=key,
                category_id=CAT_INVERTER,
                eq_type="inverter",
                vcom_device_id=inv["id"],
                name=inv.get("name") or inv["id"],
                brand=det_inv.get("vendor"),
                model=det_inv.get("model"),
                serial_number=inv.get("serial"),
            )
            equips[inv_eq.key()] = inv_eq

        # STRING PV ------------------------------------------------------
        inv_list = vc.get_inverters(key)
        inv_by_idx = {idx: inv for idx, inv in enumerate(inv_list, start=1)}

        for idx_cfg, cfg in enumerate(tech.get("systemConfigurations", []), start=1):
            inv = inv_by_idx.get(idx_cfg)
            if not inv:
                continue
            parent_vcom = inv["id"]

            for mppt_key, inp in cfg.get("mpptInputs", {}).items():
                for n in range(1, inp["stringCount"] + 1):
                    idx_str = f"{mppt_key}.{n}"
                    vdid = f"STRING-WR{idx_cfg}-MPPT-{idx_str}"
                    str_eq = Equipment(
                        vcom_system_key=key,
                        category_id=CAT_STRING,
                        eq_type="string_pv",
                        vcom_device_id=vdid,
                        name=f"STRING-{vdid}",
                        brand=inp["module"].get("vendor"),
                        model=inp["module"].get("model"),
                        count=inp["modulesPerString"],
                        parent_id=parent_vcom,
                    )
                    equips[str_eq.key()] = str_eq

    logger.info("[VCOM] snapshot: %s sites, %s equips", len(sites), len(equips))
    return sites, equips
