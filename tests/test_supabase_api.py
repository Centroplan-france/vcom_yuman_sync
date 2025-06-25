import os
import pytest

try:
    from supabase import create_client
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    create_client = None

pytestmark = pytest.mark.skipif(
    not os.getenv("SUPABASE_URL") or not os.getenv("SUPABASE_SERVICE_KEY"),
    reason="Supabase credentials not configured",
)

@pytest.mark.skipif(create_client is None, reason="supabase package missing")
def test_supabase_ping():
    """Simple call to Supabase API returning at least one row."""
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_KEY")
    sb = create_client(url, key)
    res = sb.table("sites_mapping").select("id").limit(1).execute()
    assert res.data is not None
