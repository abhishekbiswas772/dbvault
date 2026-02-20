from unittest.mock import MagicMock, patch
import pytest
from core.services.sql_backup_utility import SQLDatabaseBackupManager


@pytest.fixture
def manager():
    return SQLDatabaseBackupManager()


@pytest.fixture
def connected_manager(manager):
    """Manager with a pre-wired fake connection."""
    manager.connection = MagicMock()
    manager.host = "localhost"
    manager.user = "root"
    manager.password = "secret"
    manager.database = "mydb"
    return manager


class TestConnect:
    def test_empty_params_raises_value_error(self, manager):
        with pytest.raises(ValueError):
            manager.connect("", "", "", "")

    def test_successful_connect_stores_attrs(self, manager):
        with patch("core.services.sql_backup_utility.pymysql.connect") as mock_conn:
            mock_conn.return_value = MagicMock()
            manager.connect("localhost", "root", "pass", "mydb")
        assert manager.host == "localhost"
        assert manager.user == "root"
        assert manager.database == "mydb"

    def test_pymysql_error_raises_backup_error(self, manager):
        with patch(
            "core.services.sql_backup_utility.pymysql.connect",
            side_effect=Exception("refused"),
        ):
            with pytest.raises(SQLDatabaseBackupManager.BackupError, match="connection failed"):
                manager.connect("localhost", "root", "bad", "mydb")


class TestUploadToCloud:
    def test_s3_missing_bucket_raises(self, manager):
        with pytest.raises(ValueError, match="s3_bucket"):
            manager._upload_to_cloud("/f.gz", cloud_provider="s3", s3_expected_owner="123")

    def test_s3_missing_owner_raises(self, manager):
        with pytest.raises(ValueError, match="s3_expected_owner"):
            manager._upload_to_cloud("/f.gz", cloud_provider="s3", s3_bucket="b")

    def test_azure_missing_conn_str_raises(self, manager):
        with pytest.raises(ValueError, match="azure_conn_str"):
            manager._upload_to_cloud("/f.gz", cloud_provider="azure", azure_container="c")

    def test_azure_missing_container_raises(self, manager):
        with pytest.raises(ValueError, match="azure_conn_str"):
            manager._upload_to_cloud("/f.gz", cloud_provider="azure")

    def test_unknown_provider_raises(self, manager):
        with pytest.raises(ValueError, match="Unsupported"):
            manager._upload_to_cloud("/f.gz", cloud_provider="dropbox")

    def test_s3_upload_dispatched(self, manager):
        with patch("core.helpers.blobstorage_uploader.upload_to_s3") as mock_s3:
            manager._upload_to_cloud(
                "/f.gz",
                cloud_provider="s3",
                s3_bucket="my-bucket",
                s3_expected_owner="123456789012",
            )
            mock_s3.assert_called_once()

    def test_azure_upload_dispatched(self, manager):
        with patch("core.helpers.blobstorage_uploader.upload_to_azure") as mock_az:
            manager._upload_to_cloud(
                "/f.gz",
                cloud_provider="azure",
                azure_conn_str="DefaultEndpointsProtocol=https;...",
                azure_container="backups",
            )
            mock_az.assert_called_once()



class TestBackup:
    def test_backup_without_connection_raises(self, manager):
        with pytest.raises(SQLDatabaseBackupManager.BackupError, match="not connected"):
            manager.backup(file_path="/tmp")

    def test_backup_without_any_destination_raises(self, connected_manager):
        with pytest.raises(ValueError):
            connected_manager.backup()

    def test_backup_calls_mysqldump_and_returns_compressed(self, connected_manager, tmp_path):
        with (
            patch.object(connected_manager, "_run_mysqldump", return_value=str(tmp_path / "b.sql")) as mock_dump,
            patch.object(connected_manager, "validate", return_value=True),
            patch.object(connected_manager, "compress", return_value=str(tmp_path / "b.sql.gz")),
        ):
            ok, path = connected_manager.backup(file_path=str(tmp_path))

        assert ok is True
        assert path.endswith(".gz")
        mock_dump.assert_called_once()

    def test_backup_with_encryption_calls_encrypt(self, connected_manager, tmp_path):
        from core.helpers.cryptographic_helper import generate_key
        key = generate_key()
        enc_path = str(tmp_path / "b.sql.gz.enc")

        with (
            patch.object(connected_manager, "_run_mysqldump", return_value=str(tmp_path / "b.sql")),
            patch.object(connected_manager, "validate", return_value=True),
            patch.object(connected_manager, "compress", return_value=str(tmp_path / "b.sql.gz")),
            patch.object(connected_manager, "encrypt", return_value=enc_path) as mock_enc,
        ):
            ok, path = connected_manager.backup(file_path=str(tmp_path), encryption_key=key)

        mock_enc.assert_called_once()
        assert path == enc_path

    def test_backup_with_cloud_calls_upload(self, connected_manager, tmp_path):
        with (
            patch.object(connected_manager, "_run_mysqldump", return_value=str(tmp_path / "b.sql")),
            patch.object(connected_manager, "validate", return_value=True),
            patch.object(connected_manager, "compress", return_value=str(tmp_path / "b.sql.gz")),
            patch.object(connected_manager, "_upload_to_cloud") as mock_upload,
        ):
            connected_manager.backup(
                file_path=str(tmp_path),
                cloud_provider="s3",
                s3_bucket="b",
                s3_expected_owner="123",
            )

        mock_upload.assert_called_once()

    def test_validation_failure_raises(self, connected_manager, tmp_path):
        with (
            patch.object(connected_manager, "_run_mysqldump", return_value=str(tmp_path / "b.sql")),
            patch.object(
                connected_manager, "validate",
                side_effect=SQLDatabaseBackupManager.BackupError("bad"),
            ),
        ):
            with pytest.raises(SQLDatabaseBackupManager.BackupError):
                connected_manager.backup(file_path=str(tmp_path))
