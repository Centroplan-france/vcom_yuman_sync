import random
import string
import pytest
from src.vysync.db import supabase, sb_upsert

try:
    from postgrest.exceptions import APIError
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    APIError = None

pytestmark = pytest.mark.skipif(
    supabase is None or APIError is None,
    reason="Supabase client or postgrest library not available",
)


def random_code(n: int = 6) -> str:
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=n))


def test_sb_upsert_roundtrip(tmp_path):
    """
    Insert a temp row into a dedicated test table and ensure upsert succeeds.
    Table: public.tmp_upsert_test (created on-the-fly).
    """
    table = "tmp_upsert_test"


    # 1. create temp table if not exists
    try:
        supabase.rpc(
            "execute_sql",
            {
                "sql": f"""
                CREATE TABLE IF NOT EXISTS {table}(
                    code text PRIMARY KEY,
                    value int
                );
            """
            },
        ).execute()
    except APIError as exc:
        # Function not exposed ⇒ skip the test in this environment
        if exc.code == "PGRST106":
            pytest.skip("execute_sql RPC not exposed in this Supabase project")
        raise

    # 2. upsert row
    row1 = {"code": random_code(), "value": 1}
    sb_upsert(table, [row1], on_conflict="code")

    # 3. update same row → value = 2
    row1["value"] = 2
    sb_upsert(table, [row1], on_conflict="code")

    # 4. fetch back and assert value==2
    data = (
        supabase.table(table)
        .select("*")
        .eq("code", row1["code"])
        .execute()
        .data
    )
    assert data and data[0]["value"] == 2
