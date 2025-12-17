#!/usr/bin/env python3
"""
Tests unitaires pour le parser de noms d'onduleurs VCOM.

Ces tests couvrent tous les formats de noms documentés dans la spécification :
- "WR 1 - RPI M50A - O3618B0830" → WR=1, Model=RPI M50A, Serial=O3618B0830
- "WR2 - SunGrow - SG40CX-P2 - E/O - A2341007101" → WR=2, Vendor=SunGrow, Model=SG40CX-P2
- "Solplanet ASW xxxK LT AQ00806052370055" → Vendor=Solplanet, Model=ASW xxxK LT
- "SunGrow SG110CX A21B0203116" → Vendor=SunGrow, Model=SG110CX
- "Onduleur 2 SN A2162600126" → WR=2, Serial=A2162600126
- "Carport A WR1 SG125CX-P2 A2372424429" → WR=1, Model=SG125CX-P2, Carport=True
"""

import pytest
from vysync.inverter_parser import (
    parse_vcom_inverter_name,
    ParsedInverterName,
    _normalize_vendor,
    _is_known_vendor,
    _extract_wr_number,
    _is_serial_like,
)


class TestNormalizeVendor:
    """Tests pour la normalisation des vendors."""

    def test_sungrow_lowercase(self):
        assert _normalize_vendor("sungrow") == "SunGrow"

    def test_sungrow_mixed_case(self):
        assert _normalize_vendor("SunGrow") == "SunGrow"

    def test_solplanet(self):
        assert _normalize_vendor("solplanet") == "Solplanet"

    def test_solaredge(self):
        assert _normalize_vendor("SolarEdge") == "SolarEdge"

    def test_unknown_vendor(self):
        assert _normalize_vendor("UnknownBrand") is None

    def test_empty_string(self):
        assert _normalize_vendor("") is None

    def test_whitespace(self):
        assert _normalize_vendor("  sungrow  ") == "SunGrow"


class TestIsKnownVendor:
    """Tests pour la détection des vendors connus."""

    def test_known_vendors(self):
        known = ["SunGrow", "sungrow", "Solplanet", "SolarEdge", "Delta",
                 "Power-One", "KACO", "Huawei", "ABB", "Fronius", "SMA", "RPI"]
        for v in known:
            assert _is_known_vendor(v), f"{v} should be recognized as known vendor"

    def test_unknown_vendor(self):
        assert not _is_known_vendor("UnknownBrand")

    def test_partial_match(self):
        # Partial match should not work
        assert not _is_known_vendor("Sun")


class TestExtractWrNumber:
    """Tests pour l'extraction du numéro WR/Onduleur."""

    def test_wr_space_number(self):
        assert _extract_wr_number("WR 1 - Test") == 1

    def test_wr_no_space(self):
        assert _extract_wr_number("WR2 - Test") == 2

    def test_wr_double_digit(self):
        assert _extract_wr_number("WR12 - Test") == 12

    def test_onduleur_number(self):
        assert _extract_wr_number("Onduleur 2 SN A123") == 2

    def test_onduleur_double_digit(self):
        assert _extract_wr_number("Onduleur 11 - Test") == 11

    def test_no_wr_number(self):
        assert _extract_wr_number("SunGrow SG110CX A21B0203116") is None

    def test_case_insensitive(self):
        assert _extract_wr_number("wr 5 - Test") == 5
        assert _extract_wr_number("ONDULEUR 3 SN A123") == 3


class TestIsSerialLike:
    """Tests pour la détection des numéros de série."""

    def test_typical_serial_a_prefix(self):
        # Format A + 10+ chiffres
        assert _is_serial_like("A2341007101")

    def test_typical_serial_o_prefix(self):
        # Format O + 10+ chiffres
        assert _is_serial_like("O3618B0830")

    def test_long_alphanumeric(self):
        # Format long alphanumériques
        assert _is_serial_like("AQ00806052370055")

    def test_mixed_alphanumeric(self):
        # Format mixte lettre-chiffre-lettre-chiffre
        assert _is_serial_like("A21B0203116")

    def test_short_string_not_serial(self):
        assert not _is_serial_like("ABC")

    def test_model_not_serial(self):
        # Models should not be detected as serials
        assert not _is_serial_like("SG40CX-P2")
        assert not _is_serial_like("M50A")

    def test_empty_string(self):
        assert not _is_serial_like("")

    def test_none(self):
        assert not _is_serial_like(None)


class TestParseDashFormat:
    """Tests pour le format avec tirets " - "."""

    def test_wr_model_serial(self):
        """WR 1 - RPI M50A - O3618B0830"""
        result = parse_vcom_inverter_name("WR 1 - RPI M50A - O3618B0830")
        assert result.wr_number == 1
        assert result.vendor == "RPI"
        assert result.model == "M50A"
        assert result.serial_from_name == "O3618B0830"
        assert result.is_carport is False

    def test_wr_vendor_model_extra_serial(self):
        """WR2 - SunGrow - SG40CX-P2 - E/O - A2341007101"""
        result = parse_vcom_inverter_name("WR2 - SunGrow - SG40CX-P2 - E/O - A2341007101")
        assert result.wr_number == 2
        assert result.vendor == "SunGrow"
        assert result.model == "SG40CX-P2"
        assert result.serial_from_name == "A2341007101"
        assert result.is_carport is False

    def test_wr_with_spaces(self):
        """WR 3 - Huawei - Model123 - SerialXYZ"""
        result = parse_vcom_inverter_name("WR 3 - Huawei - Model123 - Serial123456789")
        assert result.wr_number == 3
        assert result.vendor == "Huawei"


class TestSpaceFormat:
    """Tests pour le format avec espaces (sans tirets)."""

    def test_vendor_model_serial(self):
        """Solplanet ASW xxxK LT AQ00806052370055"""
        result = parse_vcom_inverter_name("Solplanet ASW xxxK LT AQ00806052370055")
        assert result.wr_number is None
        assert result.vendor == "Solplanet"
        assert result.model == "ASW xxxK LT"
        assert result.serial_from_name == "AQ00806052370055"
        assert result.is_carport is False

    def test_simple_vendor_model_serial(self):
        """SunGrow SG110CX A21B0203116"""
        result = parse_vcom_inverter_name("SunGrow SG110CX A21B0203116")
        assert result.wr_number is None
        assert result.vendor == "SunGrow"
        assert result.model == "SG110CX"
        assert result.serial_from_name == "A21B0203116"
        assert result.is_carport is False


class TestOnduleurSnFormat:
    """Tests pour le format "Onduleur X SN Serial"."""

    def test_onduleur_sn_format(self):
        """Onduleur 2 SN A2162600126"""
        result = parse_vcom_inverter_name("Onduleur 2 SN A2162600126")
        assert result.wr_number == 2
        assert result.serial_from_name == "A2162600126"
        assert result.is_carport is False

    def test_onduleur_higher_number(self):
        """Onduleur 11 SN B9876543210"""
        result = parse_vcom_inverter_name("Onduleur 11 SN B9876543210")
        assert result.wr_number == 11
        assert result.serial_from_name == "B9876543210"


class TestCarportFormat:
    """Tests pour le format Carport."""

    def test_carport_wr_model_serial(self):
        """Carport A WR1 SG125CX-P2 A2372424429"""
        result = parse_vcom_inverter_name("Carport A WR1 SG125CX-P2 A2372424429")
        assert result.wr_number == 1
        assert result.model == "SG125CX-P2"
        assert result.serial_from_name == "A2372424429"
        assert result.is_carport is True

    def test_carport_detection_case_insensitive(self):
        """carport B WR2 Model Serial123456789"""
        result = parse_vcom_inverter_name("carport B WR2 Model Serial123456789")
        assert result.is_carport is True
        assert result.wr_number == 2

    def test_ombriere_detection(self):
        """Ombrière 1 WR3 SG40CX A1234567890"""
        result = parse_vcom_inverter_name("Ombrière 1 WR3 SG40CX A1234567890")
        assert result.is_carport is True
        assert result.wr_number == 3

    def test_ombriere_without_accent(self):
        """Ombriere B WR4 Model A9876543210"""
        result = parse_vcom_inverter_name("Ombriere B WR4 Model A9876543210")
        assert result.is_carport is True
        assert result.wr_number == 4


class TestEdgeCases:
    """Tests pour les cas limites."""

    def test_empty_string(self):
        result = parse_vcom_inverter_name("")
        assert result.wr_number is None
        assert result.vendor is None
        assert result.model is None
        assert result.serial_from_name is None
        assert result.is_carport is False

    def test_none_input(self):
        result = parse_vcom_inverter_name(None)
        assert result.wr_number is None
        assert result.vendor is None
        assert result.model is None
        assert result.serial_from_name is None
        assert result.is_carport is False

    def test_only_wr_number(self):
        """WR 5"""
        result = parse_vcom_inverter_name("WR 5")
        assert result.wr_number == 5

    def test_unknown_format(self):
        """Random text that doesn't match any format"""
        result = parse_vcom_inverter_name("Some random text")
        assert result.wr_number is None
        assert result.is_carport is False


class TestRealWorldExamples:
    """Tests avec des exemples réels des sites mentionnés."""

    def test_site_mvm9x_onduleur_format(self):
        """Site MVM9X : format "Onduleur X SN" """
        # 11 onduleurs dont format "Onduleur X SN"
        for i in range(1, 8):
            result = parse_vcom_inverter_name(f"Onduleur {i} SN A216260012{i}")
            assert result.wr_number == i
            assert result.serial_from_name == f"A216260012{i}"
            assert result.is_carport is False

    def test_site_mvm9x_carport_format(self):
        """Site MVM9X : 4 carports"""
        # Format "Carport X WRY Model Serial"
        for i, (carport_letter, wr) in enumerate([("A", 8), ("B", 9), ("C", 10), ("D", 11)], 1):
            name = f"Carport {carport_letter} WR{wr} SG125CX-P2 A237242442{i}"
            result = parse_vcom_inverter_name(name)
            assert result.is_carport is True
            assert result.wr_number == wr
            assert result.model == "SG125CX-P2"

    def test_site_29ewb_wr_inverse(self):
        """Site 29EWB : WR inversés (WR2 en index 0, WR1 en index 1)"""
        # WR2 apparaît en premier dans l'API mais doit être parsé correctement
        result = parse_vcom_inverter_name("WR2 - SunGrow - SG40CX-P2 - A2341007101")
        assert result.wr_number == 2

        result = parse_vcom_inverter_name("WR1 - SunGrow - SG40CX-P2 - A2341007102")
        assert result.wr_number == 1

    def test_parsed_inverter_name_dataclass(self):
        """Test que le dataclass retourné a tous les champs attendus."""
        result = parse_vcom_inverter_name("WR1 - SunGrow - SG40CX - A123456789")

        # Vérifier que tous les champs existent
        assert hasattr(result, 'wr_number')
        assert hasattr(result, 'vendor')
        assert hasattr(result, 'model')
        assert hasattr(result, 'serial_from_name')
        assert hasattr(result, 'is_carport')

        # Vérifier les types
        assert isinstance(result.is_carport, bool)
