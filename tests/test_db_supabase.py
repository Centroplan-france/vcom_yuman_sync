import random
import string

from vysync.adapters.supabase_adapter import SupabaseAdapter


def random_code(n: int = 6) -> str:
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=n))


def test_supabase_upsert_roundtrip(mock_supabase):
    """Insert and update a row using the SupabaseAdapter client."""
    table = "tmp_upsert_test"
    sb = SupabaseAdapter().sb

    # upsert row
    row1 = {"code": random_code(), "value": 1}
    sb.table(table).upsert([row1], on_conflict="code").execute()

    # update same row → value = 2
    row1["value"] = 2
    sb.table(table).upsert([row1], on_conflict="code").execute()

    # fetch back and assert value==2
    data = sb.table(table).select("*").eq("code", row1["code"]).execute().data
    assert data and data[0]["value"] == 2
