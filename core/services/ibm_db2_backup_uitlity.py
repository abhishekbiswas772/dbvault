import asyncio
import gzip
import os
import shutil
import subprocess
from typing import Any, Optional, Tuple
from tenacity import retry, stop_after_attempt, wait_exponential
from core.helpers.blobstorage_uploader import upload_to_azure, upload_to_s3
from core.helpers.cryptographic_helper import encrypt_file
from core.interfaces.backup_utility_interface import DatabaseBackupManager


class IBMDB2DatabaseBackupManager(DatabaseBackupManager):

    class BackupError(Exception):
        pass

    _TEMP_BACKUP_DIR = "./backup_temp_db2"

    def __init__(self) -> None:
        self.connection: bool = False
        self.host: str | None = None
        self.user: str | None = None
        self.password: str | None = None
        self.database: str | None = None

    def _db2(self, sql: str) -> str:
        """Run a single db2 CLP statement and return stdout."""
        result = subprocess.run(
            ["db2", sql],
            capture_output=True,
            text=True,
        )
        # db2 returns rc=1 for warnings; only raise on rc >= 2
        if result.returncode >= 2:
            raise self.BackupError(result.stderr or result.stdout)
        return result.stdout

    def connect(self, host: str, user: str, password: str, database_name: str) -> None:
        if not all([host, user, password, database_name]):
            raise ValueError("host, user, password, database_name are required")

        try:
            self._db2(f"CONNECT TO {database_name} USER {user} USING {password}")
            self.host = host
            self.user = user
            self.password = password
            self.database = database_name
            self.connection = True
        except Exception as e:
            raise self.BackupError(f"Database connection failed: {e}")

    def _run_db2_backup(self, backup_dir: str) -> str:
        os.makedirs(backup_dir, exist_ok=True)
        self._db2(
            f"BACKUP DATABASE {self.database} USER {self.user} "
            f"USING {self.password} TO {backup_dir} COMPRESS"
        )
        images = sorted(
            (
                f for f in os.listdir(backup_dir)
                if f.upper().startswith(self.database.upper())
            ),
            reverse=True,
        )
        if not images:
            raise self.BackupError("No backup image found after db2 BACKUP")
        return os.path.join(backup_dir, images[0])

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

        backup_dir = (
            os.path.join(file_path, "db2_backup")
            if file_path
            else self._TEMP_BACKUP_DIR
        )

        try:
            image_path = self._run_db2_backup(backup_dir)

            if not self.validate(image_path):
                raise self.BackupError("Backup validation failed")

            compressed_path = self.compress(image_path)

            if encryption_key := kwargs.get("encryption_key"):
                compressed_path = self.encrypt(compressed_path, encryption_key)

            if kwargs.get("cloud_provider"):
                self._upload_to_cloud(compressed_path, **kwargs)

            return True, compressed_path

        finally:
            if os.path.exists(self._TEMP_BACKUP_DIR):
                shutil.rmtree(self._TEMP_BACKUP_DIR, ignore_errors=True)

    def validate(self, file_path: str) -> bool:
        if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
            raise self.BackupError("Backup file missing or empty")

        try:
            result = subprocess.run(
                ["db2ckbkp", file_path],
                capture_output=True,
                text=True,
            )
        except FileNotFoundError:
            raise self.BackupError(
                "db2ckbkp not found — ensure IBM Db2 client tools are installed"
            )

        if result.returncode != 0:
            raise self.BackupError(f"Validation failed: {result.stderr or result.stdout}")

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
                self._db2("CONNECT RESET")
                self.connection = False

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
