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
