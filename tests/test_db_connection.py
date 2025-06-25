import os
import pytest
from sqlalchemy import text
from src.vysync.db import engine

if engine is None:
    pytest.skip("DATABASE_URL not set", allow_module_level=True)


def test_db_engine_connect():
    """Engine connects and minimum SQL works."""
    with engine.connect() as conn:
        assert conn.execute(text("SELECT 1")).scalar() == 1
