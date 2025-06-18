"""Database models for the VCOM–Yuman synchronisation."""

from __future__ import annotations

import enum
from datetime import datetime, date
from typing import Optional, List

from sqlmodel import SQLModel, Field, Relationship, Column, JSON
from sqlalchemy import Enum as PgEnum, Numeric
from sqlalchemy.dialects.postgresql import JSONB

# ---------------------------------------------------------------------------
# Enum helpers
# ---------------------------------------------------------------------------

class EqType(str, enum.Enum):
    """Normalized equipment types derived from Yuman `category_id`."""

    INVERTER = "inverter"
    MODULE = "module"
    SIM = "sim"
    PLANT = "plant"
    OTHER = "other"


class Source(str, enum.Enum):
    """Origin of a sync_log entry."""

    vcom = "vcom"
    yuman = "yuman"
    auto = "auto"
    user = "user"


# ---------------------------------------------------------------------------
# Lookup table – equipment categories (mirrors Yuman category catalogue)
# ---------------------------------------------------------------------------

class EquipmentCategory(SQLModel, table=True):
    __tablename__ = "equipment_categories"

    id: int = Field(
        primary_key=True,
        description="`category_id` from Yuman (fixed set)"
    )
    name: str = Field(index=True)
    bg_color: Optional[str] = Field(default=None, max_length=7)
    created_at: datetime = Field(default_factory=datetime.utcnow)

    equipments: List["Equipment"] = Relationship(back_populates="category")


# ---------------------------------------------------------------------------
# Clients
# ---------------------------------------------------------------------------

class Client(SQLModel, table=True):
    __tablename__ = "clients_mapping"

    id: int = Field(primary_key=True)
    yuman_client_id: int = Field(unique=True, index=True, nullable=False)

    code: Optional[str] = Field(default=None, description="Client code in Yuman")
    name: str
    name_addition: Optional[str] = None
    email: Optional[str] = None
    active: bool = Field(default=True)

    extra: Optional[dict] = Field(sa_column=Column(JSON), default=None)
    created_at: datetime = Field(default_factory=datetime.utcnow)

    # relationships
    sites: List["Site"] = Relationship(back_populates="client")


# ---------------------------------------------------------------------------
# Sites (VCOM systems  <->  Yuman sites)
# ---------------------------------------------------------------------------

class Site(SQLModel, table=True):
    __tablename__ = "sites_mapping"

    id: int = Field(primary_key=True)
    yuman_site_id: int = Field(default=None, unique=True, index=True, nullable=True)
    vcom_system_key: Optional[str] = Field(default=None, unique=True, index=True, nullable=True)
    # --- Champs custom Yuman -------------------------------------------------
    aldi_id: Optional[str] = Field(default=None, index=True)
    aldi_store_id: Optional[str] = Field(default=None, index=True)
    project_number_cp: Optional[str] = Field(default=None, index=True)



    client_map_id: int = Field(foreign_key="clients_mapping.id", nullable=False)
    code: Optional[str] = None
    name: str

    latitude: Optional[float] = None
    longitude: Optional[float] = None
    address: Optional[str] = None

    nominal_power: Optional[float] = Field(default=None, sa_column=Column(Numeric))
    site_area: Optional[float] = Field(default=None, sa_column=Column(Numeric))

    commission_date: Optional[date] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)

    ignore_site: bool = Field(default=False, nullable=False, sa_column_kwargs={"server_default": "false"})


    # relationships
    client: Client = Relationship(back_populates="sites")
    equipments: List["Equipment"] = Relationship(back_populates="site")


# ---------------------------------------------------------------------------
# Equipments (Yuman materials  <->  VCOM devices)
# ---------------------------------------------------------------------------

class Equipment(SQLModel, table=True):
    __tablename__ = "equipments_mapping"

    id: int = Field(primary_key=True)
    yuman_material_id: int = Field(unique=True, index=True, nullable=False)

    category_id: int = Field(foreign_key="equipment_categories.id", nullable=False)
    eq_type: EqType = Field(sa_column=Column(PgEnum(EqType, name="eq_type")))

    vcom_system_key: Optional[str] = Field(default=None, index=True)
    vcom_device_id: Optional[str] = Field(default=None, unique=True, index=True)

    serial_number: Optional[str] = Field(default=None, index=True)
    brand: Optional[str] = None
    model: Optional[str] = None
    name: Optional[str] = None

    site_id: int = Field(foreign_key="sites_mapping.id", nullable=False, index=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)

    extra: Optional[dict] = Field(sa_column=Column(JSON), default=None)

    # relationships
    category: EquipmentCategory = Relationship(back_populates="equipments")
    site: Site = Relationship(back_populates="equipments")
    fields: List["EquipmentFieldValue"] = Relationship(back_populates="equipment")


# ---------------------------------------------------------------------------
# Custom field values (1‑N with equipments)
# ---------------------------------------------------------------------------

class EquipmentFieldValue(SQLModel, table=True):
    __tablename__ = "equipment_field_values"

    id: int = Field(primary_key=True)
    equipment_id: int = Field(foreign_key="equipments_mapping.id", nullable=False, index=True)

    field_id: Optional[int] = Field(default=None, description="Yuman field id")
    field_name: str = Field(description="Human‑readable name of the custom field")
    value: Optional[str] = None

    created_at: datetime = Field(default_factory=datetime.utcnow)

    # relationships
    equipment: Equipment = Relationship(back_populates="fields")


# ---------------------------------------------------------------------------
# Ticket mapping VCOM <-> Yuman workorders
# ---------------------------------------------------------------------------

class Ticket(SQLModel, table=True):
    __tablename__ = "tickets_mapping"

    id: int = Field(primary_key=True)

    vcom_ticket_id: str = Field(index=True, unique=True)
    yuman_workorder_id: int = Field(index=True)

    status: Optional[str] = None
    priority: Optional[str] = None
    last_sync: Optional[datetime] = None


# ---------------------------------------------------------------------------
# Sync logs (audit trail)
# ---------------------------------------------------------------------------

class SyncLog(SQLModel, table=True):
    __tablename__ = "sync_logs"

    id: int = Field(primary_key=True)
    source: Source = Field(sa_column=Column(PgEnum(Source, name="sync_source")))
    action: str
    payload: Optional[dict] = Field(sa_column=Column(JSON), default=None)
    created_at: datetime = Field(default_factory=datetime.utcnow)


class Conflict(SQLModel, table=True):
    """Entities that could not be synced automatically."""

    id: int | None = Field(default=None, primary_key=True)
    entity_type: str = Field(index=True)
    entity_id: int | None = Field(default=None, index=True)
    description: str | None = None
    status: str = Field(default="pending")
    payload: dict | None = Field(sa_column=Column(JSONB))
    created_at: datetime = Field(default_factory=datetime.utcnow)
    resolved_at: datetime | None = None
