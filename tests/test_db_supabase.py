import os
import random
import string
import pytest

try:  # pragma: no cover - optional dependency
    from src.vysync.adapters.supabase_adapter import SupabaseAdapter
except ModuleNotFoundError:  # supabase package likely missing
    SupabaseAdapter = None

try:  # pragma: no cover - optional dependency
    from postgrest.exceptions import APIError
except ModuleNotFoundError:
    APIError = None

# Skip if Supabase credentials, adapter, or postgrest library are missing
pytestmark = pytest.mark.skipif(
    SupabaseAdapter is None
    or not os.getenv("SUPABASE_URL")
    or not os.getenv("SUPABASE_SERVICE_KEY")
    or APIError is None,
    reason="Supabase client or postgrest library not available",
)


def random_code(n: int = 6) -> str:
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=n))


def test_supabase_upsert_roundtrip():
    """Insert and update a row using the SupabaseAdapter client."""
    table = "tmp_upsert_test"
    sb = SupabaseAdapter().sb

    # 1. create temp table if not exists
    try:
        sb.rpc(
            "execute_sql",
            {
                "sql": f"""
                CREATE TABLE IF NOT EXISTS {table}(
                    code text PRIMARY KEY,
                    value int
                );
            """,
            },
        ).execute()
    except APIError as exc:  # pragma: no cover - env dependant
        # Function not exposed ⇒ skip the test in this environment
        if exc.code == "PGRST106":
            pytest.skip("execute_sql RPC not exposed in this Supabase project")
        raise

    # 2. upsert row
    row1 = {"code": random_code(), "value": 1}
    sb.table(table).upsert([row1], on_conflict="code").execute()

    # 3. update same row → value = 2
    row1["value"] = 2
    sb.table(table).upsert([row1], on_conflict="code").execute()

    # 4. fetch back and assert value==2
    data = sb.table(table).select("*").eq("code", row1["code"]).execute().data
    assert data and data[0]["value"] == 2
