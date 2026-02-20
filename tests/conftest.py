import os
import sqlite3
import pytest


@pytest.fixture
def sample_sqlite_db(tmp_path) -> str:
    db_path = str(tmp_path / "test.db")
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
    conn.executemany("INSERT INTO users VALUES (?, ?)", [(1, "Alice"), (2, "Bob")])
    conn.commit()
    conn.close()
    return db_path


@pytest.fixture
def sample_binary_file(tmp_path) -> str:
    path = str(tmp_path / "sample.bin")
    with open(path, "wb") as fh:
        fh.write(b"dbvault-test-content " * 200)
    return path
