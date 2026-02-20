


class PostgresDatabaseBackupManager(DatabaseBackupManager):
    class BackupError(Exception):
        pass

    def __init__(self) -> None:
        self.connection = None 
        self.host : str | None = None 
        self.user : str | None = None 
        self.password : str | None = None 
        self.database : str | None = None 