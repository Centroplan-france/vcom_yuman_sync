import os
from dotenv import load_dotenv
from sqlmodel import create_engine
from typing import Any, Dict, List
from supabase import create_client, Client as SupabaseClient


from .app_logging import init_logger
logger = init_logger(__name__)

load_dotenv()

engine = create_engine(os.environ["DATABASE_URL"], pool_pre_ping=True)

# ---------------------------------------------------------------------------
# Supabase PostgREST client (requis pour les upserts rapides)
# ---------------------------------------------------------------------------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

supabase: SupabaseClient | None = None
if SUPABASE_URL and SUPABASE_SERVICE_KEY:
    supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    logger.info("Supabase client initialised")
else:
    logger.warning("SUPABASE_URL / SUPABASE_SERVICE_KEY not set – supabase client disabled")

# ---------------------------------------------------------------------------
# Helpers – generic UPSERT via Supabase REST
# ---------------------------------------------------------------------------
def sb_upsert(
    table: str,
    rows: List[Dict[str, Any]],
    *,
    on_conflict: str,
    ignore_duplicates: bool = True,
) -> None:
    """
    Insert / update a batch of rows through Supabase PostgREST.

    Args:
        table:  destination table name
        rows:   list of dict rows
        on_conflict: comma-separated column(s) for upsert key
        ignore_duplicates: if True, ignore constraint errors (do nothing)

    Raises:
        RuntimeError if Supabase client is not initialised.
    """
    if not rows:
        return
    if supabase is None:
        raise RuntimeError("Supabase client unavailable")

    resp = (
        supabase.table(table)
        .upsert(rows, on_conflict=on_conflict, ignore_duplicates=ignore_duplicates)
        .execute()
    )
    logger.debug("Upsert %s OK – rows sent: %s", table, len(rows))


__all__ = ["engine"]

