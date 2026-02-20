import asyncio
import gzip
import os
import shutil
import sqlite3
from typing import Any, Optional, Tuple
from tenacity import retry, stop_after_attempt, wait_exponential
from core.helpers.blobstorage_uploader import upload_to_azure, upload_to_s3
from core.helpers.cryptographic_helper import encrypt_file
from core.interfaces.backup_utility_interface import DatabaseBackupManager


class SQLiteDatabaseBackupManager(DatabaseBackupManager):
    """
    host / user / password are accepted for interface compliance but are not
    used — SQLite is a file-based database identified solely by database_name
    (the path to the .db file).
    """

    class BackupError(Exception):
        pass

    _TEMP_BACKUP_PATH = "./backup_temp.db"

    def __init__(self) -> None:
        self.connection: sqlite3.Connection | None = None
        self.host: str | None = None
        self.user: str | None = None
        self.password: str | None = None
        self.database: str | None = None

    def connect(self, host: str, user: str, password: str, database_name: str) -> None:
        if not database_name:
            raise ValueError("database_name (path to .db file) is required")

        if not os.path.exists(database_name):
            raise self.BackupError(f"SQLite database file not found: {database_name}")

        try:
            self.connection = sqlite3.connect(database_name, check_same_thread=False)
            self.host = host
            self.user = user
            self.password = password
            self.database = database_name
        except sqlite3.Error as e:
            raise self.BackupError(f"Database connection failed: {e}")

    def _run_sqlite_backup(self, output_path: str) -> str:
        dest = sqlite3.connect(output_path)
        try:
            self.connection.backup(dest)
        except sqlite3.Error as e:
            raise self.BackupError(f"Backup failed: {e}")
        finally:
            dest.close()
        return output_path

    def _upload_to_cloud(self, file_path: str, **kwargs: Any) -> None:
        provider = kwargs.get("cloud_provider", "").lower()

        if provider == "s3":
            bucket = kwargs.get("s3_bucket")
            key = kwargs.get("s3_key", os.path.basename(file_path))
            expected_owner = kwargs.get("s3_expected_owner")
            if not bucket:
                raise ValueError("s3_bucket is required for S3 uploads")
            if not expected_owner:
                raise ValueError("s3_expected_owner (AWS account ID) is required for S3 uploads")
            upload_to_s3(file_path, bucket, key, expected_owner)

        elif provider == "azure":
            conn_str = kwargs.get("azure_conn_str")
            container = kwargs.get("azure_container")
            blob_name = kwargs.get("azure_blob_name", os.path.basename(file_path))
            if not conn_str or not container:
                raise ValueError("azure_conn_str and azure_container are required for Azure uploads")
            upload_to_azure(file_path, conn_str, container, blob_name)

        else:
            raise ValueError(f"Unsupported cloud_provider: '{provider}'. Use 's3' or 'azure'.")

    def backup(
        self,
        file_path: Optional[str] = None,
        storage_account_link: Optional[str] = None,
        **kwargs: Any
    ) -> Tuple[bool, str]:

        if not self.connection:
            raise self.BackupError("Database not connected")

        if not file_path and not storage_account_link and not kwargs.get("cloud_provider"):
            raise ValueError("Either file_path or cloud_provider (with credentials) required")

        backup_path = (
            os.path.join(file_path, "backup.db")
            if file_path
            else self._TEMP_BACKUP_PATH
        )

        try:
            db_path = self._run_sqlite_backup(backup_path)

            if not self.validate(db_path):
                raise self.BackupError("Backup validation failed")

            compressed_path = self.compress(db_path)

            if encryption_key := kwargs.get("encryption_key"):
                compressed_path = self.encrypt(compressed_path, encryption_key)

            if kwargs.get("cloud_provider"):
                self._upload_to_cloud(compressed_path, **kwargs)

            return True, compressed_path

        finally:
            if os.path.exists(self._TEMP_BACKUP_PATH):
                os.remove(self._TEMP_BACKUP_PATH)

    def validate(self, file_path: str) -> bool:
        if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
            raise self.BackupError("Backup file missing or empty")

        try:
            conn = sqlite3.connect(file_path)
            result = conn.execute("PRAGMA integrity_check").fetchone()
            conn.close()
        except sqlite3.Error as e:
            raise self.BackupError(f"Validation failed: {e}")

        if result[0] != "ok":
            raise self.BackupError(f"Integrity check failed: {result[0]}")

        return True

    def compress(self, file_path: str) -> str:
        if not os.path.exists(file_path):
            raise FileNotFoundError(file_path)

        compressed_path = f"{file_path}.gz"

        try:
            with open(file_path, "rb") as src:
                with gzip.open(compressed_path, "wb") as dst:
                    shutil.copyfileobj(src, dst)
        except Exception as e:
            raise self.BackupError(f"Compression failed: {e}")

        os.remove(file_path)
        return compressed_path

    def encrypt(self, file_path: str, key: bytes) -> str:
        try:
            return encrypt_file(file_path, key)
        except Exception as e:
            raise self.BackupError(f"Encryption failed: {e}")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=10),
    )
    def perform_backup_pipeline(
        self,
        host: str,
        user: str,
        password: str,
        database_name: str,
        file_path: str,
        storage_account_link: Optional[str] = None,
        **kwargs: Any
    ) -> Tuple[bool, str]:

        try:
            self.connect(host, user, password, database_name)
            return self.backup(
                file_path=file_path,
                storage_account_link=storage_account_link,
                **kwargs
            )
        finally:
            if self.connection:
                self.connection.close()

    async def async_perform_backup_pipeline(
        self,
        host: str,
        user: str,
        password: str,
        database_name: str,
        file_path: str,
        storage_account_link: Optional[str] = None,
        **kwargs: Any
    ) -> Tuple[bool, str]:
        return await asyncio.to_thread(
            self.perform_backup_pipeline,
            host, user, password, database_name,
            file_path, storage_account_link,
            **kwargs
        )
