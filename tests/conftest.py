import os
import logging
import sys
from pathlib import Path
import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))
from vcom_client import VCOMAPIClient

REQUIRED_VARS = ("VCOM_API_KEY", "VCOM_USERNAME", "VCOM_PASSWORD")


def _missing():
    return [k for k in REQUIRED_VARS if not os.getenv(k)]


@pytest.fixture(scope="session")
def vcom_client():
    if _missing():
        pytest.skip("VCOM secrets missing: " + ", ".join(_missing()), allow_module_level=True)
    return VCOMAPIClient(log_level=logging.WARNING)


def pytest_addoption(parser):
    """
    Ajoute le flag --run-integration pour exécuter les tests live.
    Usage :
      pytest --run-integration
    Sans ce flag, les tests marqués @pytest.mark.integration seront ignorés.
    """
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="run tests that hit external APIs",
    )

def pytest_configure(config):
    config.addinivalue_line(
        "markers", "integration: mark test as hitting external APIs"
    )

def pytest_collection_modifyitems(config, items):
    """Skip integration tests unless --run-integration is passed."""
    if config.getoption("--run-integration"):
        return  # on exécute tout
    skip_live = pytest.mark.skip(reason="integration tests skipped (add --run-integration)")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip_live)
