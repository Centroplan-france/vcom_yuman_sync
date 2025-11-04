#!/usr/bin/env python3
"""Tests de non-régression : protection des données existantes"""

import pytest
from vysync.models import Equipment, CAT_INVERTER
from vysync.diff import _equip_equals


def test_protection_brand_model_none():
    """Vérifie qu'un brand/model=None ne remplace pas une valeur existante"""

    # État DB actuel (valeurs valides)
    old = Equipment(
        site_id=1,
        category_id=CAT_INVERTER,
        eq_type="inverter",
        vcom_device_id="Id123.1",
        serial_number="ABC123",
        brand="Delta",
        model="RPI M30A",
        name="WR 1",
    )

    # Nouvel état VCOM (valeurs vides de l'API)
    new = Equipment(
        site_id=1,
        category_id=CAT_INVERTER,
        eq_type="inverter",
        vcom_device_id="Id123.1",
        serial_number="ABC123",
        brand=None,  # ← API a retourné ""
        model=None,  # ← API a retourné ""
        name="WR 1",
    )

    # La comparaison doit retourner True (pas de changement détecté)
    assert _equip_equals(old, new) == True, "Le diff doit ignorer brand/model=None"


def test_protection_brand_model_empty_string():
    """Vérifie qu'un brand/model="" ne remplace pas une valeur existante"""

    # État DB actuel (valeurs valides)
    old = Equipment(
        site_id=1,
        category_id=CAT_INVERTER,
        eq_type="inverter",
        vcom_device_id="Id123.1",
        serial_number="ABC123",
        brand="Sungrow",
        model="SG125HV",
        name="WR 2",
    )

    # Nouvel état VCOM (valeurs vides de l'API)
    new = Equipment(
        site_id=1,
        category_id=CAT_INVERTER,
        eq_type="inverter",
        vcom_device_id="Id123.1",
        serial_number="ABC123",
        brand="",  # ← API a retourné une chaîne vide
        model="",  # ← API a retourné une chaîne vide
        name="WR 2",
    )

    # La comparaison doit retourner True (pas de changement détecté)
    assert _equip_equals(old, new) == True, "Le diff doit ignorer brand/model=''"


def test_update_valide_autorise():
    """Vérifie qu'un changement valide est bien détecté"""

    old = Equipment(
        site_id=1,
        category_id=CAT_INVERTER,
        eq_type="inverter",
        vcom_device_id="Id123.1",
        serial_number="ABC123",
        brand="Delta",
        model="RPI M30A",
        name="WR 1",
    )

    new = Equipment(
        site_id=1,
        category_id=CAT_INVERTER,
        eq_type="inverter",
        vcom_device_id="Id123.1",
        serial_number="ABC123",
        brand="Delta",
        model="RPI M50A",  # ← Changement valide
        name="WR 1",
    )

    # La comparaison doit retourner False (changement détecté)
    assert _equip_equals(old, new) == False, "Un changement valide doit être détecté"


def test_protection_partielle():
    """Vérifie qu'un champ vide est protégé et l'autre mis à jour"""

    old = Equipment(
        site_id=1,
        category_id=CAT_INVERTER,
        eq_type="inverter",
        vcom_device_id="Id123.1",
        serial_number="ABC123",
        brand="Delta",
        model="RPI M30A",
        name="WR 1",
    )

    new = Equipment(
        site_id=1,
        category_id=CAT_INVERTER,
        eq_type="inverter",
        vcom_device_id="Id123.1",
        serial_number="ABC123",
        brand="",  # ← Vide, doit être protégé
        model="RPI M50A",  # ← Changement valide, doit être détecté
        name="WR 1",
    )

    # La comparaison doit retourner False car model a changé
    assert _equip_equals(old, new) == False, "Un changement partiel doit être détecté"


def test_nouveau_vers_vide_autorise():
    """Vérifie qu'un changement de valide vers vide est autorisé"""

    old = Equipment(
        site_id=1,
        category_id=CAT_INVERTER,
        eq_type="inverter",
        vcom_device_id="Id123.1",
        serial_number="ABC123",
        brand="Delta",
        model="RPI M30A",
        name="WR 1",
    )

    new = Equipment(
        site_id=1,
        category_id=CAT_INVERTER,
        eq_type="inverter",
        vcom_device_id="Id123.1",
        serial_number="ABC456",  # ← Changement valide du serial
        brand="Delta",
        model="RPI M30A",
        name="WR 1",
    )

    # La comparaison doit retourner False car serial_number a changé
    assert _equip_equals(old, new) == False, "Un changement de serial doit être détecté"


def test_vide_vers_valide_autorise():
    """Vérifie qu'un remplissage de champ vide est autorisé"""

    old = Equipment(
        site_id=1,
        category_id=CAT_INVERTER,
        eq_type="inverter",
        vcom_device_id="Id123.1",
        serial_number="ABC123",
        brand="",  # ← Vide dans DB
        model="",  # ← Vide dans DB
        name="WR 1",
    )

    new = Equipment(
        site_id=1,
        category_id=CAT_INVERTER,
        eq_type="inverter",
        vcom_device_id="Id123.1",
        serial_number="ABC123",
        brand="Delta",  # ← API retourne une valeur
        model="RPI M30A",  # ← API retourne une valeur
        name="WR 1",
    )

    # La comparaison doit retourner False car les valeurs ont changé de vide à rempli
    assert _equip_equals(old, new) == False, "Un remplissage de champ vide doit être détecté"
