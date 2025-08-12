import os
import logging
import sys
from pathlib import Path
from types import SimpleNamespace
import pytest

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from vysync.vcom_client import VCOMAPIClient

# Provide a minimal 'supabase' module if the real package is missing.
import types
if "supabase" not in sys.modules:
    sys.modules["supabase"] = types.SimpleNamespace(create_client=None, Client=object)


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


# ---------------------------------------------------------------------------
# Supabase mock
# ---------------------------------------------------------------------------


class _MockTable:
    def __init__(self, storage):
        self.storage = storage
        self._filter = None

    def select(self, *_, **__):
        return self

    def eq(self, key, value):
        self._filter = (key, value)
        return self

    def in_(self, *_, **__):
        return self

    def range(self, *_, **__):
        return self

    def upsert(self, rows, **_):
        self.storage[:] = rows
        return self

    def execute(self):
        data = list(self.storage)
        if self._filter:
            k, v = self._filter
            data = [r for r in data if r.get(k) == v]
        return SimpleNamespace(data=data)


class _MockSupabase:
    def __init__(self):
        self.tables = {
            "tmp_upsert_test": [],
            "sites_mapping": [{"id": 1, "vcom_system_key": "SYS1", "yuman_site_id": None, "name": "Site 1"}],
        }

    def table(self, name):
        return _MockTable(self.tables.setdefault(name, []))

    def rpc(self, *_args, **_kwargs):
        return SimpleNamespace(execute=lambda: SimpleNamespace(data=None))


@pytest.fixture
def mock_supabase(monkeypatch):
    """Provide a mocked Supabase client and patch environment variables."""
    client = _MockSupabase()
    monkeypatch.setenv("SUPABASE_URL", "http://mock")
    monkeypatch.setenv("SUPABASE_SERVICE_KEY", "mock-key")
    monkeypatch.setattr(
        "vysync.adapters.supabase_adapter.create_client",
        lambda url, key: client,
    )
    return client
