import asyncio
import gzip
import os
import shutil
import subprocess
import uuid
from typing import Any, Optional, Tuple

import pymongo
from tenacity import retry, stop_after_attempt, wait_exponential

from core.helpers.blobstorage_uploader import dispatch_cloud_upload
from core.helpers.cryptographic_helper import encrypt_file
from core.interfaces.backup_utility_interface import DatabaseBackupManager


class MongoDatabaseBackupManager(DatabaseBackupManager):

    class BackupError(Exception):
        pass

    _TEMP_BACKUP_PATH = "./backup_temp.archive"

    def __init__(self) -> None:
        self.client: pymongo.MongoClient | None = None
        self.host: str | None = None
        self.user: str | None = None
        self.password: str | None = None
        self.database: str | None = None

    def connect(self, host: str, user: str, password: str, database_name: str) -> None:
        if not all([host, user, password, database_name]):
            raise ValueError("host, user, password, database_name are required")

        try:
            uri = f"mongodb://{user}:{password}@{host}/{database_name}?authSource=admin"
            self.client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=5000)
            self.client.server_info()
            self.host = host
            self.user = user
            self.password = password
            self.database = database_name
        except Exception as e:
            raise self.BackupError(f"Database connection failed: {e}")

    def _run_mongodump(self, output_path: str) -> str:
        uri = f"mongodb://{self.user}:{self.password}@{self.host}/{self.database}?authSource=admin"
        command = ["mongodump", f"--uri={uri}", f"--archive={output_path}"]

        try:
            subprocess.run(command, stderr=subprocess.PIPE, check=True)
            return output_path
        except subprocess.CalledProcessError as e:
            raise self.BackupError(e.stderr.decode())

    def _upload_to_cloud(self, file_path: str, **kwargs: Any) -> None:
        dispatch_cloud_upload(file_path, **kwargs)

    def backup(
        self,
        file_path: Optional[str] = None,
        storage_account_link: Optional[str] = None,
        **kwargs: Any
    ) -> Tuple[bool, str]:

        if not self.client:
            raise self.BackupError("Database not connected")

        if not file_path and not storage_account_link and not kwargs.get("cloud_provider"):
            raise ValueError("Either file_path or cloud_provider (with credentials) required")

        backup_path = (
            os.path.join(file_path, "backup.archive")
            if file_path
            else self._TEMP_BACKUP_PATH
        )

        try:
            archive_path = self._run_mongodump(backup_path)

            if not self.validate(archive_path):
                raise self.BackupError("Backup validation failed")

            compressed_path = self.compress(archive_path)

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

        temp_db = f"backup_validate_{uuid.uuid4().hex[:8]}"
        uri = f"mongodb://{self.user}:{self.password}@{self.host}/?authSource=admin"

        try:
            subprocess.run(
                [
                    "mongorestore",
                    f"--uri={uri}",
                    f"--archive={file_path}",
                    f"--nsFrom={self.database}.*",
                    f"--nsTo={temp_db}.*",
                ],
                stderr=subprocess.PIPE,
                check=True,
            )
            return True

        except subprocess.CalledProcessError as e:
            raise self.BackupError(f"Validation failed: {e.stderr.decode()}")

        finally:
            if self.client:
                self.client[temp_db].command("dropDatabase")

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
            if self.client:
                self.client.close()

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
