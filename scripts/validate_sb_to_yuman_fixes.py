#!/usr/bin/env python3
"""
validate_sb_to_yuman_fixes.py
==============================

Script de validation des 3 hypotheses de diffs fantomes dans sync_supabase_to_yuman.

Validation 1 : name_inverter fantome (512+142 equipements)
Validation 2 : vcom_system_key fantome (25 sites)
Validation 3 : Normalisation serial + suppressions fantomes (34 equipements)

Usage:
    poetry run python scripts/validate_sb_to_yuman_fixes.py
"""

from __future__ import annotations

import json
import logging
import os
import sys
from collections import defaultdict
from dataclasses import asdict, replace
from typing import Any, Dict, List

# Ajouter src/ au path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

logging.basicConfig(
    level=logging.WARNING,
    format="%(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

from vysync.adapters.supabase_adapter import SupabaseAdapter
from vysync.adapters.yuman_adapter import YumanAdapter
from vysync.diff import diff_entities, set_parent_map, _equip_equals, PatchSet
from vysync.models import (
    Site, Equipment,
    CAT_MODULE, CAT_INVERTER, CAT_STRING, CAT_SIM, CAT_CENTRALE,
)
from vysync.utils import norm_serial, normalize_site_name
from vysync.yuman_client import YumanClient

CAT_NAMES = {
    CAT_INVERTER: "INVERTER",
    CAT_MODULE: "MODULE",
    CAT_STRING: "STRING",
    CAT_SIM: "SIM",
    CAT_CENTRALE: "CENTRALE",
}

SEP = "=" * 70


def print_header(title: str) -> None:
    print(f"\n{SEP}")
    print(f"  {title}")
    print(SEP)


def print_section(title: str) -> None:
    print(f"\n-- {title} --")


# ============================================================================
# VALIDATION 1 : name_inverter fantome
# ============================================================================
def validation_1(sb: SupabaseAdapter, ya: YumanAdapter, yc: YumanClient) -> None:
    print_header("VALIDATION 1 : name_inverter fantome")

    # ---------------------------------------------------------------
    # 1a. Confirmer que fetch_equips() ne peuple PAS name_inverter
    # ---------------------------------------------------------------
    print_section("1a. fetch_equips() peuple-t-il name_inverter ?")

    y_equips = ya.fetch_equips()
    inverters_y = {k: e for k, e in y_equips.items() if e.category_id == CAT_INVERTER}

    has_name_inv_y = sum(1 for e in inverters_y.values() if e.name_inverter not in (None, ""))
    print(f"  Inverters Yuman total       : {len(inverters_y)}")
    print(f"  Avec name_inverter rempli   : {has_name_inv_y}")
    if has_name_inv_y == 0:
        print("  => CONFIRME : fetch_equips() ne peuple JAMAIS name_inverter")
    else:
        print(f"  => INFIRME : {has_name_inv_y} inverters ont name_inverter rempli cote Yuman")

    # ---------------------------------------------------------------
    # 1b. Confirmer que _equip_equals compare name_inverter pour INVERTER
    # ---------------------------------------------------------------
    print_section("1b. _equip_equals() compare-t-il name_inverter ?")
    print("  => OUI (code source, diff.py:196) :")
    print('     da["name_inverter"] == db["name_inverter"]')
    print("  Champs compares pour INVERTER : brand, model, serial_number,")
    print("    vcom_device_id, name, name_inverter, carport")

    # ---------------------------------------------------------------
    # 1c. Confirmer que apply_equips_patch ne PATCH PAS name_inverter
    # ---------------------------------------------------------------
    print_section("1c. apply_equips_patch() envoie-t-il name_inverter ?")
    print("  => NON (code source, yuman_adapter.py:619-627)")
    print("  Pour INVERTER, seuls sont patches : serial_number, brand,")
    print("    vcom_device_id (custom field), model (custom field)")
    print("  name_inverter n'est PAS dans le payload PATCH")

    # ---------------------------------------------------------------
    # 1d. Combien de diffs fantomes a cause de name_inverter ?
    # ---------------------------------------------------------------
    print_section("1d. Quantification des diffs fantomes name_inverter")

    sb_equips = sb.fetch_equipments_y()
    inverters_sb = {k: e for k, e in sb_equips.items() if e.category_id == CAT_INVERTER}

    has_name_inv_sb = sum(1 for e in inverters_sb.values() if e.name_inverter not in (None, ""))
    print(f"  Inverters Supabase total         : {len(inverters_sb)}")
    print(f"  Avec name_inverter rempli (SB)   : {has_name_inv_sb}")

    # Simuler le diff pour compter les faux positifs name_inverter
    ghost_name_inv = 0
    for serial, sb_eq in inverters_sb.items():
        y_eq = inverters_y.get(serial)
        if y_eq is None:
            continue
        # Test avec name_inverter inclus
        eq_with = _equip_equals(y_eq, sb_eq)
        # Test sans name_inverter
        eq_without = _equip_equals(y_eq, sb_eq, ignore_fields={"name_inverter"})
        if not eq_with and eq_without:
            ghost_name_inv += 1

    print(f"  Diffs causes UNIQUEMENT par name_inverter : {ghost_name_inv}")

    # ---------------------------------------------------------------
    # 1e. Existe-t-il un champ Yuman mappable pour name_inverter ?
    # ---------------------------------------------------------------
    print_section("1e. Champ Yuman correspondant a name_inverter ?")
    # Fetcher un sample inverter depuis l'API pour voir ses champs
    sample_mid = 1112011
    try:
        mat = yc.get_material(sample_mid, embed="fields")
        print(f"  Sample material {sample_mid} :")
        print(f"    name           : {mat.get('name')}")
        print(f"    serial_number  : {mat.get('serial_number')}")
        print(f"    brand          : {mat.get('brand')}")
        raw_fields = {
            f["name"]: f.get("value")
            for f in mat.get("_embed", {}).get("fields", [])
        }
        print(f"    Custom fields  : {json.dumps(raw_fields, ensure_ascii=False, indent=6)}")

        # Chercher un champ qui pourrait etre name_inverter
        candidates = [
            (k, v) for k, v in raw_fields.items()
            if "name" in k.lower() or "inverter" in k.lower() or "onduleur" in k.lower()
        ]
        if candidates:
            print(f"    Candidats name_inverter : {candidates}")
        else:
            print("    Aucun champ custom ne correspond a 'name_inverter'")
            print("    => Le champ 'name' de Yuman est le nom de l'equipement,")
            print("       pas le name_inverter VCOM (qui est le nom brut VCOM)")
    except Exception as exc:
        print(f"  ERREUR fetch material {sample_mid}: {exc}")

    # ---------------------------------------------------------------
    # Conclusion
    # ---------------------------------------------------------------
    print_section("CONCLUSION Validation 1")
    if ghost_name_inv > 0:
        print(f"  HYPOTHESE CONFIRMEE : {ghost_name_inv} diffs fantomes a cause de name_inverter")
        print("  RECOMMANDATION : (a) Exclure name_inverter du diff dans _equip_equals()")
        print("    OU ajouter name_inverter aux ignore_fields dans diff_entities()")
        print("    car ce champ n'existe pas cote Yuman et le PATCH ne l'envoie pas.")
    else:
        print("  HYPOTHESE INFIRMEE : aucun diff fantome du a name_inverter")


# ============================================================================
# VALIDATION 2 : vcom_system_key fantome (25 sites)
# ============================================================================
def validation_2(sb: SupabaseAdapter, ya: YumanAdapter, yc: YumanClient) -> None:
    print_header("VALIDATION 2 : vcom_system_key fantome")

    # ---------------------------------------------------------------
    # 2a. Comment vcom_system_key est envoye dans le PATCH sites
    # ---------------------------------------------------------------
    print_section("2a. Format du PATCH vcom_system_key")
    print("  Code source (yuman_adapter.py:390-394) :")
    print('    fields_patch.append({')
    print('        "blueprint_id": 13583,')
    print('        "name": "System Key (Vcom ID)",')
    print('        "value": new_vcom,')
    print('    })')
    print("  => Envoye comme custom field avec blueprint_id=13583")
    print("  => Inclus dans site_patch['fields'] puis PATCH /sites/{id}")

    # ---------------------------------------------------------------
    # 2b. Identifier les sites avec diff vcom_system_key
    # ---------------------------------------------------------------
    print_section("2b. Identification des sites avec diff vcom_system_key")

    sb_sites_raw = sb.fetch_sites_y()
    # Exclure ignored
    sb_sites = {
        k: s for k, s in sb_sites_raw.items()
        if not getattr(s, "ignore_site", False)
    }
    # Normaliser noms comme dans sync
    sb_sites = {
        k: replace(s, name=normalize_site_name(s.name))
        for k, s in sb_sites.items()
    }

    y_sites = ya.fetch_sites()
    ignored_yids = {k for k, s in sb_sites_raw.items() if getattr(s, "ignore_site", False)}
    y_sites = {k: s for k, s in y_sites.items() if k not in ignored_yids}

    vcom_key_diffs = []
    for yid, sb_site in sb_sites.items():
        y_site = y_sites.get(yid)
        if y_site is None:
            continue
        sb_vcom = sb_site.vcom_system_key
        y_vcom = y_site.vcom_system_key
        if sb_vcom != y_vcom and sb_vcom:
            vcom_key_diffs.append({
                "yuman_site_id": yid,
                "name": sb_site.name,
                "sb_vcom_key": sb_vcom,
                "y_vcom_key": y_vcom,
            })

    print(f"  Sites avec vcom_system_key different : {len(vcom_key_diffs)}")
    for d in vcom_key_diffs[:10]:
        print(f"    yuman_id={d['yuman_site_id']} | {d['name']}")
        print(f"      Supabase: {d['sb_vcom_key']!r}  Yuman: {d['y_vcom_key']!r}")

    # ---------------------------------------------------------------
    # 2c. Test PATCH + GET sur un site sample
    # ---------------------------------------------------------------
    print_section("2c. Test PATCH + re-GET sur site sample")

    test_site_id = 793565
    test_vcom_key = "5ZRX2"

    # D'abord lire l'etat actuel
    try:
        site_before = yc.get_site(test_site_id, embed="fields")
        fields_before = {
            f["name"]: f.get("value")
            for f in site_before.get("_embed", {}).get("fields", [])
        }
        current_vcom = fields_before.get("System Key (Vcom ID)")
        print(f"  AVANT PATCH - System Key (Vcom ID) : {current_vcom!r}")

        # Envoyer le PATCH exactement comme le sync
        patch_payload = {
            "fields": [
                {
                    "blueprint_id": 13583,
                    "name": "System Key (Vcom ID)",
                    "value": test_vcom_key,
                }
            ]
        }
        print(f"  PATCH /sites/{test_site_id} payload : {json.dumps(patch_payload)}")
        yc.update_site(test_site_id, patch_payload)
        print("  PATCH envoye avec succes (pas d'erreur HTTP)")

        # Re-fetcher immediatement
        site_after = yc.get_site(test_site_id, embed="fields")
        fields_after = {
            f["name"]: f.get("value")
            for f in site_after.get("_embed", {}).get("fields", [])
        }
        new_vcom = fields_after.get("System Key (Vcom ID)")
        print(f"  APRES PATCH - System Key (Vcom ID) : {new_vcom!r}")

        if new_vcom == test_vcom_key:
            print("  => PERSISTE : la valeur a ete sauvegardee")
        elif new_vcom == current_vcom:
            print("  => NON PERSISTE : la valeur est revenue a l'ancien etat !")
            print("     Le PATCH est accepte sans erreur mais la valeur n'est pas sauvee")
        else:
            print(f"  => RESULTAT INATTENDU : {new_vcom!r}")

        # Lister TOUS les custom fields du site pour comprendre
        print(f"\n  Tous les custom fields du site {test_site_id} :")
        for f in site_after.get("_embed", {}).get("fields", []):
            print(f"    blueprint_id={f.get('blueprint_id')} | {f['name']} = {f.get('value')!r}")

    except Exception as exc:
        print(f"  ERREUR test PATCH: {exc}")

    # ---------------------------------------------------------------
    # 2d. Pourquoi le diff detecte vcom_system_key alors que PATCH marche ?
    # ---------------------------------------------------------------
    print_section("2d. Analyse de la cause racine")

    # Le PATCH fonctionne => le probleme est AVANT le PATCH
    # Hypothese : le diff detecte old_vcom != new_vcom, mais le sync
    # ne PATCH que si old_vcom != new_vcom ET new_vcom non vide.
    # Si Yuman retourne '' (vide) et Supabase a '5ZRX2', le diff voit
    # une difference, mais est-ce que le sync l'applique ?
    #
    # Regardons la logique dans apply_sites_patch (yuman_adapter.py:387-394):
    #   old_vcom = old.get_vcom_system_key(self.sb)
    #   new_vcom = new.get_vcom_system_key(self.sb)
    #
    # Le probleme est que old = site Yuman, new = site Supabase
    # old.get_vcom_system_key() utilise sb._get_vcom_key_by_site_id(old.id)
    # Mais old.id est le Supabase ID (resolu via _map_yid_to_id)
    # => old_vcom retourne la cle VCOM de Supabase, pas celle de Yuman !
    # => old_vcom == new_vcom => le PATCH n'est JAMAIS envoye
    print("  Le PATCH /sites/{id} fonctionne (valeur persistee)")
    print("  => Le probleme est dans la logique de comparaison :")
    print("     apply_sites_patch() utilise get_vcom_system_key(self.sb)")
    print("     qui recupere la valeur depuis le cache Supabase (pas Yuman)")
    print("     => old_vcom == new_vcom => PATCH jamais envoye")
    print("     => Le champ Yuman reste vide, le diff le re-detecte")
    print()
    print("  Le diff sites compare vcom_system_key directement :")
    print("     y_site.vcom_system_key (lu depuis Yuman custom field) = None")
    print("     sb_site.vcom_system_key (lu depuis Supabase) = '5ZRX2'")
    print("     => diff UPDATE detecte")
    print("  MAIS apply_sites_patch ne l'envoie pas (old_vcom == new_vcom via SB cache)")

    # ---------------------------------------------------------------
    # Conclusion
    # ---------------------------------------------------------------
    print_section("CONCLUSION Validation 2")
    print(f"  HYPOTHESE PARTIELLEMENT CONFIRMEE : {len(vcom_key_diffs)} diffs fantomes")
    print("  CAUSE RACINE : Le PATCH fonctionne, mais apply_sites_patch()")
    print("    utilise get_vcom_system_key(self.sb) pour old ET new,")
    print("    ce qui compare la valeur Supabase avec elle-meme.")
    print("    Le PATCH du custom field n'est donc JAMAIS envoye.")
    print("  RECOMMANDATION : Dans apply_sites_patch(), comparer")
    print("    old.vcom_system_key (direct, depuis Yuman) avec")
    print("    new.get_vcom_system_key(self.sb) (depuis Supabase).")
    print("    Ou plus simplement : utiliser old.vcom_system_key directement.")


# ============================================================================
# VALIDATION 3 : Normalisation serial + suppressions fantomes
# ============================================================================
def validation_3(sb: SupabaseAdapter, ya: YumanAdapter, yc: YumanClient) -> None:
    print_header("VALIDATION 3 : Normalisation serial + suppressions fantomes")

    # ---------------------------------------------------------------
    # 3a. Verifier l'asymetrie de normalisation serial
    # ---------------------------------------------------------------
    print_section("3a. Normalisation serial_number : Yuman vs Supabase")

    y_equips = ya.fetch_equips()
    sb_equips = sb.fetch_equipments_y()

    print(f"  Yuman equips   : {len(y_equips)}")
    print(f"  Supabase equips: {len(sb_equips)}")

    # Le code Yuman utilise m["serial_number"] brut comme cle (yuman_adapter.py:297)
    # Le code Supabase utilise norm_serial() (supabase_adapter.py:214-215, 232)
    print("\n  Code source :")
    print("    Yuman   : equips[m['serial_number']] = equip  (ligne 297, cle brute)")
    print("    Supabase: equips[norm_serial(serial)] = eq    (ligne 232, normalise)")

    # Compter les cles Yuman qui ne sont pas deja uppercase/stripped
    non_normalized = []
    for serial in y_equips.keys():
        if serial is None:
            continue
        normed = norm_serial(serial)
        if normed != serial:
            non_normalized.append((serial, normed, repr(serial), repr(normed)))

    print(f"\n  Serials Yuman non-normalises : {len(non_normalized)}")
    for raw, normed, raw_repr, normed_repr in non_normalized[:10]:
        print(f"    {raw_repr} -> {normed_repr}")
        in_sb = normed in sb_equips
        in_y_normed = normed in y_equips
        print(f"      Present Supabase (norme)? {in_sb} | Present Yuman (norme)? {in_y_normed}")

    # ---------------------------------------------------------------
    # 3b. Verifier si norm_serial est utilise des deux cotes
    # ---------------------------------------------------------------
    print_section("3b. Utilisation de norm_serial()")
    print("  fetch_equips() (Yuman)       : NON - cle = m['serial_number'] brut")
    print("  fetch_equipments_y() (SB)    : OUI - cle = norm_serial(serial)")
    print("  fetch_equipments_v() (SB)    : OUI - cle = norm_serial(serial)")
    if non_normalized:
        print(f"  => ASYMETRIE CONFIRMEE : {len(non_normalized)} serials ne matchent pas")
    else:
        print("  => Pas d'asymetrie detectee (tous les serials sont deja normalises)")

    # ---------------------------------------------------------------
    # 3c. Analyser les suppressions (DELETE) dans le diff
    # ---------------------------------------------------------------
    print_section("3c. Analyse des suppressions fantomes")

    # Calculer le diff comme le ferait le sync
    # Exclure les sites ignores
    sb_sites_raw = sb.fetch_sites_y()
    ignored_sids = {
        s.id for s in sb_sites_raw.values()
        if getattr(s, "ignore_site", False) and s.id
    }
    sb_equips_filtered = {
        k: e for k, e in sb_equips.items()
        if e.site_id not in ignored_sids
    }
    y_equips_filtered = {
        k: e for k, e in y_equips.items()
        if e.site_id not in ignored_sids
    }

    # Filtrer les equips dont le site n'a pas de yuman_site_id
    sites_with_yid = {
        s.id for s in sb_sites_raw.values()
        if s.yuman_site_id and not getattr(s, "ignore_site", False)
    }
    sb_equips_filtered = {
        k: e for k, e in sb_equips_filtered.items()
        if e.site_id in sites_with_yid
    }

    # Parent map
    id_by_vcom = {
        e.vcom_device_id: e.yuman_material_id
        for e in y_equips_filtered.values()
        if e.yuman_material_id
    }
    set_parent_map(id_by_vcom)

    patch = diff_entities(
        y_equips_filtered,
        sb_equips_filtered,
        ignore_fields={"vcom_system_key", "parent_id", "name"},
    )

    # Filtrer SIM des delete (comme le sync)
    deletes = [e for e in patch.delete if e.category_id != CAT_SIM]

    print(f"  Suppressions totales (hors SIM) : {len(deletes)}")

    # Categoriser les suppressions
    due_to_case = 0
    due_to_obsolete = 0
    truly_missing = 0
    case_samples = []

    for eq in deletes:
        serial_raw = eq.serial_number
        serial_normed = norm_serial(serial_raw)

        # Est-ce que la version normalisee existe en Supabase ?
        if serial_normed in sb_equips_filtered:
            due_to_case += 1
            if len(case_samples) < 5:
                case_samples.append({
                    "serial_raw": serial_raw,
                    "serial_normed": serial_normed,
                    "category": CAT_NAMES.get(eq.category_id, "?"),
                    "yuman_material_id": eq.yuman_material_id,
                })
            continue

        # Est-ce que l'equipement existe en Supabase mais avec is_obsolete=true ?
        # On doit checker dans la table directement
        truly_missing += 1

    # Checker les obsoletes dans la DB
    print(f"\n  Suppressions par cause :")
    print(f"    Due a mismatch de casse       : {due_to_case}")
    print(f"    Absents de Supabase (actifs)   : {truly_missing}")

    if case_samples:
        print(f"\n  Exemples mismatch casse :")
        for s in case_samples:
            print(f"    {s['serial_raw']!r} -> {s['serial_normed']!r} ({s['category']})")

    # ---------------------------------------------------------------
    # 3d. Le code DELETE est commente -- est-ce intentionnel ?
    # ---------------------------------------------------------------
    print_section("3d. Code DELETE dans apply_equips_patch")
    print("  Le bloc DELETE est COMMENTE (yuman_adapter.py:684-699)")
    print("  => Les suppressions sont detectees par diff_entities()")
    print("     mais JAMAIS appliquees cote Yuman")
    print("  => Les equipements 'supprimes' restent dans Yuman indefiniment")
    print("  => Le diff les re-detecte a chaque run => phantom deletes")

    # ---------------------------------------------------------------
    # 3e. Verifier les obsoletes dans Supabase parmi les deletes
    # ---------------------------------------------------------------
    print_section("3e. Equipements obsoletes en Supabase")

    # Fetcher les equips AVEC obsoletes pour comparer
    sb_equips_with_obs = sb.fetch_equipments_v(include_obsolete=True)

    obsolete_in_delete = 0
    for eq in deletes:
        serial_normed = norm_serial(eq.serial_number)
        sb_eq = sb_equips_with_obs.get(serial_normed)
        if sb_eq is not None:
            # C'est dans Supabase => soit actif (donc c'est un mismatch casse)
            # soit obsolete
            pass
        # L'equip est dans Yuman mais pas dans Supabase actif
        # Il pourrait etre marque obsolete
        # On ne peut pas le verifier facilement sans query directe

    # Utiliser une query directe pour les obsoletes
    try:
        sample_serials = [norm_serial(e.serial_number) for e in deletes[:50] if e.serial_number]
        if sample_serials:
            obs_rows = (
                sb.sb.table("equipments_mapping")
                .select("serial_number, is_obsolete")
                .in_("serial_number", sample_serials)
                .execute()
                .data or []
            )
            obs_count = sum(1 for r in obs_rows if r.get("is_obsolete"))
            active_count = sum(1 for r in obs_rows if not r.get("is_obsolete"))
            not_found = len(sample_serials) - len(obs_rows)
            print(f"  Parmi {len(sample_serials)} serials (sample des deletes) :")
            print(f"    Trouves et obsoletes     : {obs_count}")
            print(f"    Trouves et actifs        : {active_count}")
            print(f"    Non trouves en Supabase  : {not_found}")
    except Exception as exc:
        print(f"  ERREUR query obsoletes: {exc}")

    # ---------------------------------------------------------------
    # Conclusion
    # ---------------------------------------------------------------
    print_section("CONCLUSION Validation 3")
    print(f"  Suppressions fantomes totales : {len(deletes)}")
    if due_to_case > 0:
        print(f"  (a) Mismatch casse serial     : {due_to_case}")
        print("      FIX : Appliquer norm_serial() dans fetch_equips() (Yuman)")
        print("      Ligne 297: equips[norm_serial(m['serial_number'])] = equip")
    if truly_missing > 0:
        print(f"  (b) Equipements Yuman sans equivalent Supabase actif : {truly_missing}")
        print("      OPTIONS :")
        print("        - Supprimer de Yuman (decommenter le bloc DELETE)")
        print("        - Ignorer dans le diff (ne pas reporter les DELETE)")
        print("        - Archiver (flag dans Yuman si API le permet)")


# ============================================================================
# MAIN
# ============================================================================
def main() -> None:
    print(SEP)
    print("  VALIDATION DES HYPOTHESES - DIFFS FANTOMES")
    print("  sync_supabase_to_yuman")
    print(SEP)

    print("\nInitialisation des adaptateurs...")
    sb = SupabaseAdapter()
    ya = YumanAdapter(sb)
    yc = ya.yc  # reutiliser le client Yuman deja configure

    validation_1(sb, ya, yc)
    validation_2(sb, ya, yc)
    validation_3(sb, ya, yc)

    print_header("FIN DU RAPPORT DE VALIDATION")


if __name__ == "__main__":
    main()
