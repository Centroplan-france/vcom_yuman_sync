#!/usr/bin/env python3
"""
diag_sb_to_yuman.py
====================
Script de diagnostic pour le workflow sync_supabase_to_yuman.
Aucune écriture — uniquement lecture Supabase + API Yuman.

Investigations :
  1. ALDI Puget Ville (client_map_id NULL)
  2. 25 sites avec vcom_system_key fantôme
  3. 654 diffs équipements fantômes
  4. Analyse statique du diff
"""

from __future__ import annotations

import json
import os
import sys
import traceback
from dataclasses import asdict
from typing import Any, Dict, List, Optional

# ── path setup ────────────────────────────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from vysync.yuman_client import YumanClient
from vysync.adapters.supabase_adapter import SupabaseAdapter
from vysync.utils import norm_serial
from vysync.models import CAT_MODULE, CAT_INVERTER, CAT_STRING, CAT_SIM, CAT_CENTRALE

CAT_NAMES = {
    CAT_MODULE: "MODULE",
    CAT_INVERTER: "INVERTER",
    CAT_STRING: "STRING",
    CAT_SIM: "SIM",
    CAT_CENTRALE: "CENTRALE",
}

SEP = "═" * 72
SEP2 = "─" * 72


def hr(title: str) -> None:
    print(f"\n{SEP}")
    print(f"  {title}")
    print(SEP)


def h2(title: str) -> None:
    print(f"\n{SEP2}")
    print(f"  {title}")
    print(SEP2)


def pp(label: str, value: Any) -> None:
    """Pretty-print a labelled value."""
    if isinstance(value, (dict, list)):
        print(f"  {label}:")
        print("    " + json.dumps(value, indent=2, ensure_ascii=False, default=str)
              .replace("\n", "\n    "))
    else:
        print(f"  {label}: {value!r}")


# ══════════════════════════════════════════════════════════════════════════════
# INIT
# ══════════════════════════════════════════════════════════════════════════════

hr("INIT")
try:
    sb = SupabaseAdapter()
    yc = YumanClient()
    print("  ✓ SupabaseAdapter initialisé")
    print("  ✓ YumanClient initialisé")
except Exception as e:
    print(f"  ✗ Erreur init : {e}")
    sys.exit(1)


# ══════════════════════════════════════════════════════════════════════════════
# INVESTIGATION 1 — ALDI Puget Ville (client_map_id NULL)
# ══════════════════════════════════════════════════════════════════════════════

hr("INVESTIGATION 1 — ALDI Puget Ville (client_map_id NULL)")

INV1_YUMAN_SITE_ID = 673236
INV1_VCOM_KEY      = "69MUV"
INV1_SB_ID         = 4613

# 1a. Site depuis l'API Yuman
h2("1a. GET /sites/673236 (Yuman)")
try:
    y_site = yc.get_site(INV1_YUMAN_SITE_ID, embed="fields,client")
    pp("id",        y_site.get("id"))
    pp("name",      y_site.get("name"))
    pp("client_id", y_site.get("client_id"))
    pp("address",   y_site.get("address"))

    # Custom fields
    cfields = {
        f["name"]: f.get("value")
        for f in y_site.get("_embed", {}).get("fields", [])
    }
    pp("custom_fields", cfields)

    y_client_id = y_site.get("client_id")
    print(f"\n  → client_id Yuman du site : {y_client_id!r}")
except Exception as e:
    y_client_id = None
    print(f"  ✗ Erreur Yuman GET /sites/{INV1_YUMAN_SITE_ID} : {e}")

# 1b. Vérifier si ce client_id existe dans clients_mapping (Supabase)
h2("1b. Lookup clients_mapping (Supabase) pour ce client_id")
try:
    if y_client_id is not None:
        rows = (
            sb.sb.table("clients_mapping")
            .select("*")
            .eq("yuman_client_id", y_client_id)
            .execute()
            .data or []
        )
        if rows:
            for r in rows:
                pp("clients_mapping row", r)
            print(f"\n  → client_map_id à affecter sur sites_mapping.id={INV1_SB_ID} : {rows[0]['id']!r}")
        else:
            print(f"  ✗ Aucune ligne dans clients_mapping pour yuman_client_id={y_client_id}")
    else:
        print("  ⚠ client_id Yuman inconnu — skip")
except Exception as e:
    print(f"  ✗ Erreur Supabase clients_mapping : {e}")

# 1c. État actuel de la ligne Supabase
h2("1c. État Supabase de sites_mapping.id=4613")
try:
    row = (
        sb.sb.table("sites_mapping")
        .select("*")
        .eq("id", INV1_SB_ID)
        .single()
        .execute()
        .data
    )
    pp("sites_mapping row", row)
except Exception as e:
    print(f"  ✗ Erreur : {e}")

# 1d. Le script resolve_clients_for_sites existe-t-il ?
h2("1d. Existence du script resolve_clients_for_sites")
import glob as _glob

candidates = _glob.glob(
    os.path.join(os.path.dirname(__file__), "..", "**", "*resolve_client*"),
    recursive=True,
)
if candidates:
    for c in candidates:
        print(f"  ✓ Trouvé : {c}")
else:
    print("  ✗ Aucun fichier resolve_clients* trouvé dans le repo.")
    print("    → La référence dans yuman_adapter.py:88 est un message d'erreur,")
    print("      pas un script existant. Le script n'existe pas encore.")


# ══════════════════════════════════════════════════════════════════════════════
# INVESTIGATION 2 — 25 sites avec vcom_system_key fantôme
# ══════════════════════════════════════════════════════════════════════════════

hr("INVESTIGATION 2 — vcom_system_key fantôme (3 sites sample)")

SAMPLE_SITES = [
    {"yuman_site_id": 793565, "label": "ALDI Salbris",          "expected_key": "5ZRX2"},
    {"yuman_site_id": 655354, "label": "ALDI Blamont",          "expected_key": "594I8"},
    {"yuman_site_id": 794137, "label": "Thiriet Kingersheim",   "expected_key": "L969G"},
]

for s in SAMPLE_SITES:
    h2(f"2. Site {s['label']} (yuman_site_id={s['yuman_site_id']}, expected_key={s['expected_key']})")

    # Yuman
    try:
        y_data = yc.get_site(s["yuman_site_id"], embed="fields,client")
        cfields = {
            f["name"]: f.get("value")
            for f in y_data.get("_embed", {}).get("fields", [])
        }
        pp("Yuman: name",      y_data.get("name"))
        pp("Yuman: client_id", y_data.get("client_id"))
        pp("Yuman: ALL custom fields", cfields)
        yuman_vcom_key = (cfields.get("System Key (Vcom ID)") or "").strip() or None
        print(f"\n  → Yuman 'System Key (Vcom ID)' = {yuman_vcom_key!r}")
    except Exception as e:
        yuman_vcom_key = None
        print(f"  ✗ Erreur Yuman GET /sites/{s['yuman_site_id']} : {e}")

    # Supabase
    try:
        sb_id = sb._map_yid_to_id.get(s["yuman_site_id"])
        row = (
            sb.sb.table("sites_mapping")
            .select("id, name, vcom_system_key, yuman_site_id, client_map_id")
            .eq("yuman_site_id", s["yuman_site_id"])
            .single()
            .execute()
            .data
        )
        sb_vcom_key = row.get("vcom_system_key") if row else None
        pp("Supabase sites_mapping row", row)
        print(f"\n  → Supabase vcom_system_key = {sb_vcom_key!r}")
        print(f"  → Yuman    vcom_system_key = {yuman_vcom_key!r}")
    except Exception as e:
        print(f"  ✗ Erreur Supabase : {e}")
        sb_vcom_key = None

    # Diagnostic
    print("\n  ANALYSE :")
    if yuman_vcom_key is None and sb_vcom_key:
        print(f"  ⚠ DIFF FANTÔME CONFIRMÉ : Supabase a '{sb_vcom_key}' mais Yuman a None/vide.")
        print("    → Le diff détecte une différence (target=Supabase ≠ current=Yuman).")
        print("    → La règle métier dans _equals copie target=None → da si db[key] is None,")
        print("      MAIS ici c'est da (Yuman) qui est None et db (Supabase) qui a la valeur.")
        print("    → Résultat : _equals retourne False → UPDATE déclenché.")
        print("    → Dans apply_sites_patch UPDATE : old_vcom != new_vcom AND new_vcom is set")
        print("      → champ envoyé à Yuman MAIS si le blueprint ne persiste pas → fantôme permanent.")
    elif yuman_vcom_key == sb_vcom_key:
        print(f"  ✓ Pas de diff attendu (identiques : {yuman_vcom_key!r})")
    else:
        print(f"  ℹ Yuman={yuman_vcom_key!r} vs Supabase={sb_vcom_key!r}")

print("\n  CONCLUSION INVESTIGATION 2 :")
print("  → vcom_system_key est stocké côté Yuman comme custom field 'System Key (Vcom ID)'")
print("    blueprint_id=13583 (voir yuman_adapter.py SITE_FIELDS).")
print("  → Ce n'est PAS un champ standard de l'API Yuman.")
print("  → Si l'API Yuman ne retourne pas la valeur persistée (blueprint absent,")
print("    ou embed='fields' non fonctionnel sur ces sites), le diff se répète")
print("    indéfiniment à chaque run.")


# ══════════════════════════════════════════════════════════════════════════════
# INVESTIGATION 3 — 654 diffs équipements fantômes
# ══════════════════════════════════════════════════════════════════════════════

hr("INVESTIGATION 3 — 654 diffs équipements fantômes")

# ─── 3a. 512 "sans changement" ───────────────────────────────────────────────
h2("3a. Sample 5 équipements 'sans changement'")

SAMPLE_SERIALS_NO_CHANGE = [
    "A2333103118",
    "A2330409481",
    "A2333103054",
    "O3616B0351",
    "A2331702892",
]

for serial in SAMPLE_SERIALS_NO_CHANGE:
    h2(f"  Serial : {serial}")

    # Supabase
    try:
        rows = (
            sb.sb.table("equipments_mapping")
            .select("*")
            .eq("serial_number", serial)
            .eq("is_obsolete", False)
            .execute()
            .data or []
        )
        if not rows:
            print(f"    ✗ Non trouvé en Supabase (serial={serial})")
            continue
        sb_row = rows[0]
        yuid = sb_row.get("yuman_material_id")
        pp("    Supabase row", {
            "id": sb_row.get("id"),
            "serial_number": sb_row.get("serial_number"),
            "category_id": sb_row.get("category_id"),
            "brand": sb_row.get("brand"),
            "model": sb_row.get("model"),
            "name": sb_row.get("name"),
            "vcom_device_id": sb_row.get("vcom_device_id"),
            "name_inverter": sb_row.get("name_inverter"),
            "carport": sb_row.get("carport"),
            "count": sb_row.get("count"),
            "parent_id": sb_row.get("parent_id"),
            "yuman_material_id": yuid,
        })
    except Exception as e:
        print(f"    ✗ Erreur Supabase : {e}")
        continue

    # Yuman
    if not yuid:
        print("    ⚠ yuman_material_id manquant — skip Yuman fetch")
        continue
    try:
        y_mat = yc.get_material(yuid, embed="fields,site")
        raw_fields = {
            f["name"]: f.get("value")
            for f in y_mat.get("_embed", {}).get("fields", [])
        }
        pp("    Yuman row", {
            "id": y_mat.get("id"),
            "serial_number": y_mat.get("serial_number"),
            "category_id": y_mat.get("category_id"),
            "brand": y_mat.get("brand"),
            "name": y_mat.get("name"),
            "ALL custom fields": raw_fields,
        })
        y_serial_raw = y_mat.get("serial_number") or ""
        y_serial_norm = norm_serial(y_serial_raw)
        sb_serial_norm = norm_serial(sb_row.get("serial_number") or "")
        print(f"\n    Comparaison clés dict :")
        print(f"      Yuman  serial (raw)  = {y_serial_raw!r}")
        print(f"      Yuman  serial (norm) = {y_serial_norm!r}")
        print(f"      Supabase serial (norm) = {sb_serial_norm!r}")
        print(f"      Clés identiques : {y_serial_norm == sb_serial_norm}")
        if y_serial_raw != y_serial_norm:
            print(f"      ⚠ La clé Yuman dans y_equips = {y_serial_raw!r} (non normalisée!)")
            print(f"        La clé Supabase = {sb_serial_norm!r}")
            print(f"        → MISMATCH de clé dict → ADD + DELETE fantôme possible!")
    except Exception as e:
        print(f"    ✗ Erreur Yuman GET /materials/{yuid} : {e}")

# ─── 3b. 142 name swaps (WR 1 ↔ WR 2) ────────────────────────────────────────
h2("3b. 142 name swaps — investigation des paires inverters")

print("  Chargement de y_equips + sb_equips pour trouver les paires WR 1 ↔ WR 2...")

try:
    from vysync.adapters.yuman_adapter import YumanAdapter
    ya = YumanAdapter(sb)

    y_equips = ya.fetch_equips()
    sb_equips = sb.fetch_equipments_y()

    # On identifie les paires : même serial, name différent entre Yuman et Supabase
    # et les deux noms ressemblent à "WR N"
    import re as _re
    swap_pairs = []
    for serial, sb_e in sb_equips.items():
        y_e = y_equips.get(serial)
        if y_e is None:
            continue
        y_name = (y_e.name or "").strip()
        sb_name = (sb_e.name or "").strip()
        if y_name != sb_name and _re.search(r'WR\s*\d', y_name, _re.I):
            swap_pairs.append((serial, y_e, sb_e))

    print(f"  → {len(swap_pairs)} paires WR-swap trouvées")

    for serial, y_e, sb_e in swap_pairs[:3]:
        h2(f"  Paire WR-swap : serial={serial}")
        pp("    Yuman  name",       y_e.name)
        pp("    Supabase name",     sb_e.name)
        pp("    Yuman  category",   CAT_NAMES.get(y_e.category_id, y_e.category_id))
        pp("    Yuman  brand",      y_e.brand)
        pp("    Yuman  model",      y_e.model)
        pp("    Yuman  vcom_device_id", y_e.vcom_device_id)
        pp("    Supabase brand",    sb_e.brand)
        pp("    Supabase model",    sb_e.model)
        pp("    Supabase vcom_device_id", sb_e.vcom_device_id)
        pp("    Supabase name_inverter",  sb_e.name_inverter)
        pp("    Supabase carport",        sb_e.carport)
        pp("    Yuman  name_inverter",    y_e.name_inverter)
        pp("    Yuman  carport",          y_e.carport)
        # 'name' est dans ignore_fields → ne devrait PAS déclencher le diff
        # Mais vcom_device_id / name_inverter / carport pourraient le faire
        name_in_ignore = True  # ignore_fields={"vcom_system_key","parent_id","name"}
        print(f"\n    ANALYSE : 'name' est dans ignore_fields → ne déclenche PAS le diff.")
        print(f"    Vérifions les champs réellement comparés pour INVERTER :")
        print(f"      brand:        Yuman={y_e.brand!r}  vs Supabase={sb_e.brand!r}  → {'DIFF' if (y_e.brand or '').lower() != (sb_e.brand or '').lower() else 'OK'}")
        print(f"      model:        Yuman={y_e.model!r}  vs Supabase={sb_e.model!r}  → {'DIFF' if (y_e.model or '').lower() != (sb_e.model or '').lower() else 'OK'}")
        print(f"      serial:       Yuman={y_e.serial_number!r}  vs Supabase={sb_e.serial_number!r}  → {'DIFF' if y_e.serial_number != sb_e.serial_number else 'OK'}")
        print(f"      vcom_device_id: Yuman={y_e.vcom_device_id!r}  vs Supabase={sb_e.vcom_device_id!r}  → {'DIFF' if y_e.vcom_device_id != sb_e.vcom_device_id else 'OK'}")
        print(f"      name_inverter: Yuman={y_e.name_inverter!r}  vs Supabase={sb_e.name_inverter!r}  → {'DIFF' if (y_e.name_inverter or '').strip() != (sb_e.name_inverter or '').strip() else 'OK'}")
        print(f"      carport:      Yuman={y_e.carport!r}  vs Supabase={sb_e.carport!r}  → {'DIFF' if y_e.carport != sb_e.carport else 'OK'}")

except Exception as e:
    print(f"  ✗ Erreur lors du chargement des équipements : {e}")
    traceback.print_exc()

# ─── 3c. 34 suppressions ─────────────────────────────────────────────────────
h2("3c. 34 suppressions — vérification des 5 yuman_material_id")

SAMPLE_DELETIONS = [1112004, 1112050, 1112084, 1112117, 1112123]

for mid in SAMPLE_DELETIONS:
    h2(f"  yuman_material_id={mid}")

    # Vérifier si l'objet existe encore dans Yuman
    try:
        y_mat = yc.get_material(mid, embed="fields,site")
        print(f"    ✓ Existe dans Yuman : id={y_mat.get('id')}, name={y_mat.get('name')!r}, "
              f"serial={y_mat.get('serial_number')!r}, site_id={y_mat.get('site_id')!r}")
    except Exception as e:
        print(f"    ✗ Non trouvé dans Yuman (ou erreur) : {e}")
        y_mat = None

    # Vérifier dans Supabase
    try:
        rows = (
            sb.sb.table("equipments_mapping")
            .select("id, serial_number, is_obsolete, site_id, category_id, vcom_device_id, name")
            .eq("yuman_material_id", mid)
            .execute()
            .data or []
        )
        if rows:
            for r in rows:
                pp("    Supabase row", r)
        else:
            # Chercher par serial si possible
            if y_mat and y_mat.get("serial_number"):
                serial = norm_serial(y_mat["serial_number"])
                rows2 = (
                    sb.sb.table("equipments_mapping")
                    .select("id, serial_number, is_obsolete, site_id, yuman_material_id, name")
                    .eq("serial_number", serial)
                    .execute()
                    .data or []
                )
                if rows2:
                    pp("    Supabase (par serial)", rows2)
                else:
                    print(f"    ✗ Non trouvé en Supabase (ni par yuman_material_id ni par serial)")
            else:
                print(f"    ✗ Non trouvé en Supabase (yuman_material_id={mid})")
    except Exception as e:
        print(f"    ✗ Erreur Supabase : {e}")

print("\n  ANALYSE direction des suppressions :")
print("  → diff_entities(current=y_equips, target=sb_equips)")
print("  → delete = clés présentes dans y_equips MAIS ABSENTES de sb_equips")
print("  → Autrement dit : équipements dans Yuman qui n'existent PLUS dans Supabase")
print("  → Sens de la suppression = suppression dans Yuman (et flag is_obsolete=True en Supabase)")
print("  → Dans yuman_adapter.apply_equips_patch : le bloc DELETE est COMMENTÉ (lignes 685-699)")
print("  → Seul supabase_adapter.apply_equips_patch flag les équipements en is_obsolete=True")
print("  → Ces 34 suppressions sont probablement des équipements Yuman sans correspondant Supabase")


# ══════════════════════════════════════════════════════════════════════════════
# INVESTIGATION 4 — Analyse statique du diff
# ══════════════════════════════════════════════════════════════════════════════

hr("INVESTIGATION 4 — Analyse statique du code de diff")

print("""
  4.1 MATCHING — Clé de comparaison pour les équipements
  ───────────────────────────────────────────────────────
  • Dans diff_entities(y_equips, sb_equips) :
      - current = y_equips  → indexé par m["serial_number"] (RAW, non normalisé)
                               [yuman_adapter.py:297]
      - target  = sb_equips → indexé par norm_serial(r["serial_number"]) (UPPERCASE)
                               [supabase_adapter.py:214-216]
  • Pour chaque clé k dans sb_equips, on cherche current.get(k)
  • BUG POTENTIEL : Si Yuman retourne un serial en minuscules ou avec espaces,
    la clé dans y_equips ≠ clé dans sb_equips → l'équipement est traité comme
    AJOUT (côté Supabase) ET SUPPRESSION (côté Yuman) au lieu d'un UPDATE.

  4.2 CHAMPS COMPARÉS — par catégorie dans _equip_equals()
  ──────────────────────────────────────────────────────────
  • ignore_fields passés = {"vcom_system_key", "parent_id", "name"}
    (ces champs sont popped de da/db avant comparaison)

  • CAT_MODULE (11103) :
      brand.lower(), model.lower(), serial_number

  • CAT_INVERTER (11102) :
      brand.lower(), model.lower(), serial_number, vcom_device_id,
      name (via d.get après normalisation — "name" était poppé, donc = "" des deux côtés → toujours OK),
      name_inverter, carport
      ⚠ "name" est poppé par ignore_fields MAIS _equip_equals le re-définit à ""
        pour les deux côtés → égaux → ne déclenche PAS de diff

  • CAT_STRING (12404) :
      brand.lower(), model.lower(), count, vcom_device_id, parent_id (remappé), serial_number

  • CAT_CENTRALE (11441) :
      serial_number uniquement

  • CAT_SIM (11382) :
      brand.lower(), model.lower(), serial_number, vcom_device_id

  4.3 SUPPRESSIONS — Direction et comportement
  ─────────────────────────────────────────────
  • diff_entities(current=y_equips, target=sb_equips)
  • delete = {k ∈ y_equips | k ∉ sb_equips}
  • Interprétation : équipement PRÉSENT dans Yuman mais ABSENT de Supabase
  • Action attendue : supprimer dans Yuman + flag is_obsolete=True en Supabase
  • Réalité code :
    - yuman_adapter.apply_equips_patch  : bloc DELETE entièrement COMMENTÉ
    - supabase_adapter.apply_equips_patch : flag is_obsolete=True par serial/vcom_id
  • Résultat : ces équipements ne sont jamais supprimés de Yuman → diff permanent.

  4.4 BUG : Updates avec changeset vide
  ──────────────────────────────────────
  OUI — ce bug EXISTE. Mécanisme :

  a) _equip_equals() retourne False sur des champs comme vcom_device_id,
     name_inverter, ou carport → diff déclare UPDATE.

  b) Dans sync_supabase_to_yuman.py, le rapport "changes" est calculé
     uniquement sur 5 champs : ['name', 'brand', 'model', 'count', 'serial_number']
     (lignes 406-420) — il MANQUE vcom_device_id, name_inverter, carport.
     → Le rapport affiche changes={} alors qu'un diff existe sur d'autres champs.

  c) Dans yuman_adapter.apply_equips_patch UPDATE :
     Le payload est construit dynamiquement en ne gardant que les champs réellement
     différents. Si la différence était sur un champ non-envoyable à l'API (ou déjà
     géré autrement), payload={} ET fields_patch=[] → aucun appel API.
     Mais l'UPDATE était quand même compté dans le diff → exit code 1.

  CONCLUSION GÉNÉRALE
  ────────────────────
  Les 3 catégories de fantômes correspondent à :

  ① "512 sans changement" :
     Diff déclenché par un champ (ex. vcom_device_id ou name_inverter) qui diffère
     entre Yuman et Supabase, mais le rapport ne le montre pas (champs non listés).
     Possiblement aussi des mismatches de clés (serial non normalisé côté Yuman).

  ② "142 name swaps (WR 1 ↔ WR 2)" :
     'name' est dans ignore_fields → NE déclenche PAS le diff directement.
     La différence réelle est probablement sur vcom_device_id ou name_inverter
     qui encode le numéro de WR. Le nom swap est un symptôme visible mais pas
     la cause du diff.

  ③ "34 suppressions" :
     Équipements présents dans Yuman mais absents de Supabase (is_obsolete=True
     ou serial manquant). Le DELETE Yuman est commenté → ces items restent
     indéfiniment dans le diff.
""")

hr("FIN DU DIAGNOSTIC")
print("  Rapport terminé. Aucune modification n'a été effectuée.")
