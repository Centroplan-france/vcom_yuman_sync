from typing import Optional
from sqlmodel import SQLModel, Field

class Client(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str

class Site(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    client_id: int = Field(foreign_key="client.id")
    name: str

# TODO: Technician table
# TODO: Ticket table
# TODO: Inverter table

