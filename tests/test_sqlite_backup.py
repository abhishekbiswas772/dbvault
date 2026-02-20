import gzip
import os
import sqlite3
import pytest
from core.helpers.cryptographic_helper import decrypt_file, generate_key
from core.services.sqllite_backup_utility import SQLiteDatabaseBackupManager


@pytest.fixture
def manager():
    return SQLiteDatabaseBackupManager()


@pytest.fixture
def connected(manager, sample_sqlite_db):
    manager.connect("", "", "", sample_sqlite_db)
    yield manager
    if manager.connection:
        manager.connection.close()


class TestConnect:
    def test_valid_db_opens_connection(self, manager, sample_sqlite_db):
        manager.connect("", "", "", sample_sqlite_db)
        assert manager.connection is not None

    def test_stores_database_path(self, manager, sample_sqlite_db):
        manager.connect("", "", "", sample_sqlite_db)
        assert manager.database == sample_sqlite_db

    def test_missing_database_name_raises_value_error(self, manager):
        with pytest.raises(ValueError):
            manager.connect("", "", "", "")

    def test_nonexistent_file_raises_backup_error(self, manager):
        with pytest.raises(SQLiteDatabaseBackupManager.BackupError):
            manager.connect("", "", "", "/no/such/file.db")

class TestValidate:
    def test_valid_backup_returns_true(self, connected, tmp_path):
        backup_path = str(tmp_path / "backup.db")
        connected._run_sqlite_backup(backup_path)
        assert connected.validate(backup_path) is True

    def test_missing_file_raises(self, connected):
        with pytest.raises(SQLiteDatabaseBackupManager.BackupError):
            connected.validate("/no/such/backup.db")

    def test_empty_file_raises(self, connected, tmp_path):
        empty = str(tmp_path / "empty.db")
        open(empty, "w").close()
        with pytest.raises(SQLiteDatabaseBackupManager.BackupError):
            connected.validate(empty)

    def test_corrupted_file_raises(self, connected, tmp_path):
        bad = str(tmp_path / "bad.db")
        with open(bad, "wb") as fh:
            fh.write(b"this is definitely not SQLite")
        with pytest.raises(SQLiteDatabaseBackupManager.BackupError):
            connected.validate(bad)

    def test_backup_preserves_data(self, connected, tmp_path):
        backup_path = str(tmp_path / "backup.db")
        connected._run_sqlite_backup(backup_path)
        conn = sqlite3.connect(backup_path)
        rows = conn.execute("SELECT * FROM users ORDER BY id").fetchall()
        conn.close()
        assert rows == [(1, "Alice"), (2, "Bob")]


class TestCompress:
    def test_creates_gz_file(self, connected, tmp_path):
        bp = str(tmp_path / "backup.db")
        connected._run_sqlite_backup(bp)
        gz = connected.compress(bp)
        assert gz.endswith(".gz")
        assert os.path.exists(gz)

    def test_removes_original(self, connected, tmp_path):
        bp = str(tmp_path / "backup.db")
        connected._run_sqlite_backup(bp)
        gz = connected.compress(bp)
        assert not os.path.exists(bp)

    def test_output_is_valid_gzip(self, connected, tmp_path):
        bp = str(tmp_path / "backup.db")
        connected._run_sqlite_backup(bp)
        gz = connected.compress(bp)
        with gzip.open(gz, "rb") as fh:
            assert len(fh.read()) > 0

    def test_nonexistent_raises_file_not_found(self, connected):
        with pytest.raises(FileNotFoundError):
            connected.compress("/no/such/file.db")

class TestEncrypt:
    def test_returns_enc_path(self, connected, tmp_path):
        bp = str(tmp_path / "backup.db")
        connected._run_sqlite_backup(bp)
        gz = connected.compress(bp)
        enc = connected.encrypt(gz, generate_key())
        assert enc.endswith(".enc")
        assert os.path.exists(enc)

    def test_encrypt_decrypt_roundtrip(self, connected, tmp_path):
        bp = str(tmp_path / "backup.db")
        connected._run_sqlite_backup(bp)
        gz = connected.compress(bp)
        key = generate_key()
        enc = connected.encrypt(gz, key)
        dec = decrypt_file(enc, key)
        assert dec == gz
        with gzip.open(dec, "rb") as fh:
            assert len(fh.read()) > 0


class TestBackupPipeline:
    def test_sync_pipeline_returns_success(self, manager, sample_sqlite_db, tmp_path):
        ok, path = manager.perform_backup_pipeline(
            host="", user="", password="",
            database_name=sample_sqlite_db,
            file_path=str(tmp_path),
        )
        assert ok is True
        assert os.path.exists(path)
        assert path.endswith(".gz")

    def test_sync_pipeline_with_encryption(self, manager, sample_sqlite_db, tmp_path):
        key = generate_key()
        ok, path = manager.perform_backup_pipeline(
            host="", user="", password="",
            database_name=sample_sqlite_db,
            file_path=str(tmp_path),
            encryption_key=key,
        )
        assert ok is True
        assert path.endswith(".enc")

    def test_no_output_arg_raises(self, manager, sample_sqlite_db):
        # Test backup() directly to avoid tenacity retrying a deterministic ValueError
        manager.connect("", "", "", sample_sqlite_db)
        with pytest.raises(ValueError):
            manager.backup(file_path="")

    async def test_async_pipeline_returns_success(self, manager, sample_sqlite_db, tmp_path):
        ok, path = await manager.async_perform_backup_pipeline(
            host="", user="", password="",
            database_name=sample_sqlite_db,
            file_path=str(tmp_path),
        )
        assert ok is True
        assert os.path.exists(path)
