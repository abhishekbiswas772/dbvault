from abc import ABC, abstractmethod
from typing import Optional, Any, Tuple


class DatabaseBackupManager(ABC):

    @abstractmethod
    def connect(
        self,
        host: str,
        user: str,
        password: str,
        database_name: str
    ) -> None:
        pass

    @abstractmethod
    def backup(
        self,
        file_path: Optional[str] = None,
        storage_account_link: Optional[str] = None,
        **kwargs: Any
    ) -> Tuple[bool, str]:
        pass

    @abstractmethod
    def validate(self, file_path: str) -> bool:
        pass

    @abstractmethod
    def compress(self, file_path: str) -> str:
        pass

    @abstractmethod
    def encrypt(self, file_path: str, key: bytes) -> str:
        pass

    @abstractmethod
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
        pass

    @abstractmethod
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
        pass
