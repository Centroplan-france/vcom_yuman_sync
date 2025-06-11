import os
from dotenv import load_dotenv
from sqlmodel import create_engine

from .logging import init_logger
logger = init_logger(__name__)

load_dotenv()

engine = create_engine(os.environ["DATABASE_URL"], pool_pre_ping=True)

__all__ = ["engine"]

