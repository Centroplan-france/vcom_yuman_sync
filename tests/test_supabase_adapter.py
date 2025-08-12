from vysync.adapters.supabase_adapter import SupabaseAdapter
from vysync.models import Site


def test_fetch_sites_v(mock_supabase):
    adapter = SupabaseAdapter()
    sites = adapter.fetch_sites_v()
    assert "SYS1" in sites
    assert isinstance(sites["SYS1"], Site)
