"""SQLite startup, integrity constraints, and the legacy endpoint index migration."""

from pathlib import Path

import pytest
from sqlalchemy import create_engine, event, inspect, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from replicafs.common import sha256_hex
from replicafs.metadata import db as database


@pytest.fixture
def engine(tmp_path, monkeypatch):
    path = tmp_path / "new" / "nested" / "metadata.db"
    url = f"sqlite:///{path}"
    local = create_engine(url, connect_args={"check_same_thread": False})
    event.listen(local, "connect", database.configure_sqlite)
    monkeypatch.setattr(database, "DATABASE_URL", url)
    monkeypatch.setattr(database, "engine", local)
    try:
        yield local
    finally:
        local.dispose()


def node(node_id="node1", url="http://node1:8001"):
    return database.Node(id=node_id, url=url, last_heartbeat=database.utcnow())


def file_record(file_id="file1", name="notes.txt", version=1):
    return database.File(
        id=file_id,
        name=name,
        version=version,
        size=0,
        checksum=sha256_hex(b""),
        created_at=database.utcnow(),
    )


def create_legacy_nodes(engine, duplicate=False):
    """Create the project's original nodes schema, before uq_nodes_url existed."""
    Path(engine.url.database).parent.mkdir(parents=True)
    with engine.begin() as connection:
        connection.execute(
            text("""
            CREATE TABLE nodes (
                id VARCHAR NOT NULL PRIMARY KEY,
                url VARCHAR NOT NULL,
                status VARCHAR NOT NULL,
                last_heartbeat DATETIME NOT NULL,
                chunk_count INTEGER NOT NULL,
                free_bytes BIGINT NOT NULL
            )
        """)
        )
        values = [{"id": "node1", "url": "http://node1:8001"}]
        if duplicate:
            values.append({"id": "node2", "url": "http://node1:8001"})
        connection.execute(
            text("""
                INSERT INTO nodes (id, url, status, last_heartbeat, chunk_count, free_bytes)
                VALUES (:id, :url, 'ALIVE', '2026-10-31 00:00:00', 7, 12345)
            """),
            values,
        )


def test_init_creates_parent_directories_and_is_safe_to_repeat(engine):
    path = Path(engine.url.database)
    assert not path.parent.exists()
    database.init_db()
    assert path.is_file()
    assert set(inspect(engine).get_table_names()) == {"nodes", "files", "chunks", "replicas"}
    sessions = sessionmaker(engine)
    with sessions() as session:
        session.add(node())
        session.commit()
    database.init_db()
    with sessions() as session:
        assert session.query(database.Node).one().url == "http://node1:8001"


def test_sqlite_integrity_settings_apply_to_every_new_connection(engine):
    database.init_db()
    for _ in range(2):
        with engine.connect() as connection:
            assert connection.execute(text("PRAGMA foreign_keys")).scalar() == 1
            assert connection.execute(text("PRAGMA busy_timeout")).scalar() == 30000
        engine.dispose()


def test_unique_node_endpoint_is_enforced_by_the_database(engine):
    database.init_db()
    sessions = sessionmaker(engine)
    with sessions() as session:
        session.add(node())
        session.commit()
        session.add(node("node2", "http://node1:8001"))
        with pytest.raises(IntegrityError):
            session.commit()
        session.rollback()
        assert session.query(database.Node).count() == 1


@pytest.mark.parametrize("kind", ["chunk", "replica"])
def test_foreign_keys_reject_orphaned_storage_claims(engine, kind):
    database.init_db()
    sessions = sessionmaker(engine)
    with sessions() as session:
        if kind == "chunk":
            session.add(
                database.Chunk(
                    id="orphan",
                    file_id="missing-file",
                    index=0,
                    size=1,
                    checksum=sha256_hex(b"x"),
                )
            )
        else:
            session.add(database.Replica(chunk_id="missing-chunk", node_id="missing-node"))
        with pytest.raises(IntegrityError, match="FOREIGN KEY"):
            session.commit()
        session.rollback()
        assert session.query(database.Chunk).count() == 0
        assert session.query(database.Replica).count() == 0


def test_file_version_constraint_is_the_final_concurrent_write_guard(engine):
    database.init_db()
    sessions = sessionmaker(engine)
    with sessions() as session:
        session.add(file_record())
        session.commit()
        session.add(file_record("duplicate", version=1))
        with pytest.raises(IntegrityError):
            session.commit()
        session.rollback()
        session.add(file_record("next", version=2))
        session.commit()
        assert [
            record.version for record in session.query(database.File).order_by(database.File.version)
        ] == [1, 2]


def test_legacy_database_gains_unique_endpoint_index_without_losing_data(engine):
    create_legacy_nodes(engine)
    assert "uq_nodes_url" not in {index["name"] for index in inspect(engine).get_indexes("nodes")}
    database.init_db()
    database.init_db()
    assert "uq_nodes_url" in {index["name"] for index in inspect(engine).get_indexes("nodes")}
    with engine.connect() as connection:
        row = connection.execute(text("SELECT id, url, chunk_count, free_bytes FROM nodes")).one()
        assert tuple(row) == ("node1", "http://node1:8001", 7, 12345)
    with engine.begin() as connection, pytest.raises(IntegrityError):
        connection.execute(
            text("""
            INSERT INTO nodes (id, url, status, last_heartbeat, chunk_count, free_bytes)
            VALUES ('node2', 'http://node1:8001', 'ALIVE', '2026-10-31 00:00:00', 0, 0)
        """)
        )


def test_duplicate_legacy_endpoints_fail_migration_without_deleting_records(engine):
    create_legacy_nodes(engine, duplicate=True)
    with pytest.raises(IntegrityError):
        database.init_db()
    with engine.connect() as connection:
        rows = connection.execute(text("SELECT id, url FROM nodes ORDER BY id")).all()
        assert [tuple(row) for row in rows] == [
            ("node1", "http://node1:8001"),
            ("node2", "http://node1:8001"),
        ]
    assert "uq_nodes_url" not in {index["name"] for index in inspect(engine).get_indexes("nodes")}
    # An operator can reconcile the duplicate explicitly, then retry startup.
    with engine.begin() as connection:
        connection.execute(text("DELETE FROM nodes WHERE id = 'node2'"))
    database.init_db()
    assert "uq_nodes_url" in {index["name"] for index in inspect(engine).get_indexes("nodes")}
