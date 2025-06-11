import os
import sys
from pathlib import Path
import pytest
from sqlalchemy import text
from src.vysync.db import engine

sys.path.append(str(Path(__file__).resolve().parents[1]))

if "DATABASE_URL" not in os.environ:
    pytest.skip("DATABASE_URL not set", allow_module_level=True)



def test_db_connection():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        assert result.scalar() == 1

