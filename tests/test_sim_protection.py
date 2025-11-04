#!/usr/bin/env python3
"""
Tests de protection des données SIM.

Les cartes SIM ont une particularité : leur source de vérité est Yuman (pas VCOM/Supabase).
Ces tests vérifient que :
1. PHASE 1B (Yuman→DB) force la mise à jour de brand/model depuis Yuman
2. PHASE 2 (DB→Yuman) skip les UPDATE de SIM (seule la création est autorisée)
"""

import pytest
from unittest.mock import Mock, MagicMock, call, patch as mock_patch
from vysync.models import Equipment, CAT_SIM, CAT_INVERTER
from vysync.diff import diff_fill_missing, PatchSet
from vysync.adapters.yuman_adapter import YumanAdapter


def test_diff_fill_missing_force_update_sim():
    """
    Test 1 : Vérifier que force_update_categories=[CAT_SIM] force bien
    la mise à jour de brand/model même si la DB a déjà des valeurs.
    """
    # État DB actuel (anciennes valeurs)
    db_sim = Equipment(
        site_id=1,
        category_id=CAT_SIM,
        eq_type="sim",
        vcom_device_id="SIM-001",
        serial_number="SIM-001",
        brand="Ancienne valeur",  # Valeur obsolète en DB
        model="123456",            # Valeur obsolète en DB
        name="Carte SIM Site 1",
    )

    # État Yuman actuel (nouvelles valeurs saisies manuellement)
    yuman_sim = Equipment(
        site_id=1,
        category_id=CAT_SIM,
        eq_type="sim",
        vcom_device_id="SIM-001",
        serial_number="SIM-001",
        brand="Onomondo",  # Nouvelle valeur depuis Yuman
        model="789012",    # Nouvelle valeur depuis Yuman
        name="Carte SIM Site 1",
    )

    # Appel à diff_fill_missing avec force_update_categories=[CAT_SIM]
    patch = diff_fill_missing(
        {"SIM-001": db_sim},
        {"SIM-001": yuman_sim},
        fields=["brand", "model"],
        force_update_categories=[CAT_SIM]
    )

    # Assertions
    assert len(patch.add) == 0, "Pas de nouvel équipement"
    assert len(patch.update) == 1, "Une mise à jour doit être détectée"

    old, new = patch.update[0]
    assert new.brand == "Onomondo", "brand doit être mis à jour depuis Yuman"
    assert new.model == "789012", "model doit être mis à jour depuis Yuman"


def test_diff_fill_missing_no_force_keeps_existing():
    """
    Test 2 : Vérifier que sans force_update_categories, les valeurs
    existantes ne sont PAS écrasées (comportement fill-missing normal).
    """
    # État DB actuel (valeurs existantes)
    db_sim = Equipment(
        site_id=1,
        category_id=CAT_SIM,
        eq_type="sim",
        vcom_device_id="SIM-001",
        serial_number="SIM-001",
        brand="Ancienne valeur",  # Valeur non-vide en DB
        model="123456",            # Valeur non-vide en DB
        name="Carte SIM Site 1",
    )

    # État Yuman actuel (nouvelles valeurs)
    yuman_sim = Equipment(
        site_id=1,
        category_id=CAT_SIM,
        eq_type="sim",
        vcom_device_id="SIM-001",
        serial_number="SIM-001",
        brand="Onomondo",  # Nouvelle valeur depuis Yuman
        model="789012",    # Nouvelle valeur depuis Yuman
        name="Carte SIM Site 1",
    )

    # Appel à diff_fill_missing SANS force_update_categories
    patch = diff_fill_missing(
        {"SIM-001": db_sim},
        {"SIM-001": yuman_sim},
        fields=["brand", "model"]
        # PAS de force_update_categories
    )

    # Assertions
    assert len(patch.add) == 0, "Pas de nouvel équipement"
    assert len(patch.update) == 0, "Pas de mise à jour car champs non-vides (fill-missing)"


def test_diff_fill_missing_force_update_only_sim():
    """
    Test bonus : Vérifier que force_update_categories affecte uniquement
    les SIM et pas les autres catégories.
    """
    # SIM avec valeurs existantes
    db_sim = Equipment(
        site_id=1,
        category_id=CAT_SIM,
        eq_type="sim",
        vcom_device_id="SIM-001",
        serial_number="SIM-001",
        brand="Old",
        model="111",
        name="SIM 1",
    )
    yuman_sim = Equipment(
        site_id=1,
        category_id=CAT_SIM,
        eq_type="sim",
        vcom_device_id="SIM-001",
        serial_number="SIM-001",
        brand="New",
        model="222",
        name="SIM 1",
    )

    # INVERTER avec valeurs existantes
    db_inv = Equipment(
        site_id=1,
        category_id=CAT_INVERTER,
        eq_type="inverter",
        vcom_device_id="INV-001",
        serial_number="INV-001",
        brand="Old",
        model="111",
        name="WR 1",
    )
    yuman_inv = Equipment(
        site_id=1,
        category_id=CAT_INVERTER,
        eq_type="inverter",
        vcom_device_id="INV-001",
        serial_number="INV-001",
        brand="New",
        model="222",
        name="WR 1",
    )

    # Appel avec force_update_categories=[CAT_SIM] uniquement
    patch = diff_fill_missing(
        {"SIM-001": db_sim, "INV-001": db_inv},
        {"SIM-001": yuman_sim, "INV-001": yuman_inv},
        fields=["brand", "model"],
        force_update_categories=[CAT_SIM]
    )

    # Assertions
    assert len(patch.add) == 0
    assert len(patch.update) == 1, "Seule la SIM doit être mise à jour"

    old, new = patch.update[0]
    assert new.category_id == CAT_SIM, "Seule la SIM doit être mise à jour"
    assert new.brand == "New"
    assert new.model == "222"


def test_apply_equips_patch_skip_sim_update():
    """
    Test 3 : Vérifier que apply_equips_patch skip les UPDATE de SIM
    (DB→Yuman) car Yuman est la source de vérité.
    """
    # Setup mocks
    mock_sb_adapter = Mock()
    mock_sb_adapter._get_yuman_site_id_by_site_id = Mock(return_value=100)

    # Patcher YumanClient pour éviter l'initialisation réelle
    with mock_patch('vysync.adapters.yuman_adapter.YumanClient') as MockYumanClient:
        mock_yc = Mock()
        MockYumanClient.return_value = mock_yc

        yuman_adapter = YumanAdapter(sb_adapter=mock_sb_adapter)

        # Équipements existants (old state)
        old_sim = Equipment(
            site_id=1,
            category_id=CAT_SIM,
            eq_type="sim",
            vcom_device_id="SIM-001",
            serial_number="SIM-001",
            yuman_material_id=5001,
            brand="Onomondo",
            model="789012",
            name="SIM 1",
        )

        old_inv = Equipment(
            site_id=1,
            category_id=CAT_INVERTER,
            eq_type="inverter",
            vcom_device_id="INV-001",
            serial_number="INV-001",
            yuman_material_id=5002,
            brand="Delta",
            model="RPI M30A",
            name="WR 1",
        )

        # Nouvelles valeurs (new state)
        new_sim = Equipment(
            site_id=1,
            category_id=CAT_SIM,
            eq_type="sim",
            vcom_device_id="SIM-001",
            serial_number="SIM-001",
            yuman_material_id=5001,
            brand="Orange",  # Changement
            model="999999",  # Changement
            name="SIM 1",
        )

        new_inv = Equipment(
            site_id=1,
            category_id=CAT_INVERTER,
            eq_type="inverter",
            vcom_device_id="INV-001",
            serial_number="INV-001",
            yuman_material_id=5002,
            brand="Sungrow",  # Changement
            model="SG125HV",  # Changement
            name="WR 1",
        )

        # Patch contenant : 1 UPDATE SIM, 1 UPDATE INVERTER
        patch = PatchSet(
            add=[],
            update=[(old_sim, new_sim), (old_inv, new_inv)],
            delete=[]
        )

        # Appel à apply_equips_patch
        db_equips = {"SIM-001": new_sim, "INV-001": new_inv}
        y_equips = {"SIM-001": old_sim, "INV-001": old_inv}

        yuman_adapter.apply_equips_patch(db_equips, y_equips=y_equips, patch=patch)

        # Vérifications
        # update_material doit être appelé uniquement pour l'INVERTER (pas la SIM)
        assert mock_yc.update_material.call_count == 1, "update_material doit être appelé une seule fois (pour INVERTER)"

        # Vérifier que l'appel concerne bien l'INVERTER
        call_args = mock_yc.update_material.call_args_list[0]
        material_id = call_args[0][0]
        assert material_id == 5002, "update_material doit être appelé pour l'INVERTER (id=5002)"


def test_apply_equips_patch_create_sim_allowed():
    """
    Test 4 : Vérifier que les SIM peuvent quand même être CRÉÉES
    (DB→Yuman) lors de l'initialisation d'un nouveau site.
    """
    # Setup mocks
    mock_sb_adapter = Mock()
    mock_sb_adapter._get_yuman_site_id_by_site_id = Mock(return_value=100)

    # Mock pour la mise à jour DB
    mock_sb_table = Mock()
    mock_sb_adapter.sb = Mock()
    mock_sb_adapter.sb.table = Mock(return_value=mock_sb_table)
    mock_sb_table.update = Mock(return_value=mock_sb_table)
    mock_sb_table.eq = Mock(return_value=mock_sb_table)
    mock_sb_table.execute = Mock()

    # Patcher YumanClient pour éviter l'initialisation réelle
    with mock_patch('vysync.adapters.yuman_adapter.YumanClient') as MockYumanClient:
        mock_yc = Mock()
        mock_yc.create_material = Mock(return_value={"id": 6001})
        mock_yc.update_material = Mock()
        MockYumanClient.return_value = mock_yc

        yuman_adapter = YumanAdapter(sb_adapter=mock_sb_adapter)

        # Nouvelle SIM à créer
        new_sim = Equipment(
            site_id=1,
            category_id=CAT_SIM,
            eq_type="sim",
            vcom_device_id="SIM-002",
            serial_number="SIM-002",
            brand="",      # Valeurs minimales à la création
            model="",      # Valeurs minimales à la création
            name="Carte SIM Site 1",
        )

        # Patch contenant : 1 ADD SIM
        patch = PatchSet(
            add=[new_sim],
            update=[],
            delete=[]
        )

        # Appel à apply_equips_patch
        db_equips = {"SIM-002": new_sim}

        yuman_adapter.apply_equips_patch(db_equips, y_equips={}, patch=patch)

        # Vérifications
        assert mock_yc.create_material.call_count == 1, "create_material doit être appelé une fois"

        # Vérifier les paramètres de create_material
        call_args = mock_yc.create_material.call_args_list[0]
        payload = call_args[0][0]

        assert payload["category_id"] == CAT_SIM, "La SIM doit être créée avec category_id=CAT_SIM"
        assert payload["site_id"] == 100, "La SIM doit être créée sur le bon site"
        assert payload["serial_number"] == "SIM-002", "La SIM doit avoir le bon serial_number"
