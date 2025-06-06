import os
from dotenv import load_dotenv
from sqlmodel import SQLModel, create_engine

load_dotenv()

engine = create_engine(os.environ["DATABASE_URL"], pool_pre_ping=True)
SQLModel.metadata.create_all(engine)

__all__ = ["engine"]

