"""Metadata schema.

    File 1──* Chunk 1──* Replica *──1 Node

A File is split into ordered Chunks. Each Chunk has several Replicas, each
living on one Node. Nothing here holds file *content* — only where it is.
"""
import os
from datetime import datetime, timezone

from sqlalchemy import BigInteger, ForeignKey, String, UniqueConstraint, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, sessionmaker

DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///./data/metadata.db")

_connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}
engine = create_engine(DATABASE_URL, connect_args=_connect_args)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)


def utcnow() -> datetime:
    # Naive UTC everywhere so comparisons work the same on SQLite and Postgres.
    return datetime.now(timezone.utc).replace(tzinfo=None)


class Base(DeclarativeBase):
    pass


class Node(Base):
    __tablename__ = "nodes"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    url: Mapped[str]
    status: Mapped[str] = mapped_column(default="ALIVE")  # ALIVE | DEAD
    last_heartbeat: Mapped[datetime]
    chunk_count: Mapped[int] = mapped_column(default=0)
    free_bytes: Mapped[int] = mapped_column(BigInteger, default=0)


class File(Base):
    __tablename__ = "files"
    __table_args__ = (UniqueConstraint("name", "version"),)

    id: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str] = mapped_column(index=True)
    version: Mapped[int]
    size: Mapped[int] = mapped_column(BigInteger)
    checksum: Mapped[str]  # sha256 of the whole file
    created_at: Mapped[datetime]
    deleted_at: Mapped[datetime | None] = mapped_column(nullable=True)  # soft delete

    chunks: Mapped[list["Chunk"]] = relationship(order_by="Chunk.index", cascade="all, delete-orphan")


class Chunk(Base):
    __tablename__ = "chunks"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    file_id: Mapped[str] = mapped_column(ForeignKey("files.id"))
    index: Mapped[int]
    size: Mapped[int]
    checksum: Mapped[str]  # sha256 of this chunk, recorded at upload time

    replicas: Mapped[list["Replica"]] = relationship(cascade="all, delete-orphan")


class Replica(Base):
    __tablename__ = "replicas"

    chunk_id: Mapped[str] = mapped_column(ForeignKey("chunks.id"), primary_key=True)
    node_id: Mapped[str] = mapped_column(ForeignKey("nodes.id"), primary_key=True)

    node: Mapped[Node] = relationship()


def init_db() -> None:
    if DATABASE_URL.startswith("sqlite:///"):
        os.makedirs(os.path.dirname(DATABASE_URL.removeprefix("sqlite:///")) or ".", exist_ok=True)
    Base.metadata.create_all(engine)
