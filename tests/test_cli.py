import os
import pytest
from click.testing import CliRunner
from cryptography.fernet import Fernet
from cli.app import cli
from core.helpers.cryptographic_helper import encrypt_file, generate_key


@pytest.fixture
def runner():
    return CliRunner()


class TestHelp:
    def test_no_subcommand_shows_help(self, runner):
        result = runner.invoke(cli, [])
        assert result.exit_code == 0
        assert "backup" in result.output
        assert "keygen" in result.output
        assert "decrypt" in result.output

    def test_help_flag(self, runner):
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0

    def test_backup_help(self, runner):
        result = runner.invoke(cli, ["backup", "--help"])
        assert result.exit_code == 0
        assert "--db" in result.output
        assert "--output" in result.output
        assert "--encrypt" in result.output

    def test_keygen_help(self, runner):
        result = runner.invoke(cli, ["keygen", "--help"])
        assert result.exit_code == 0

    def test_decrypt_help(self, runner):
        result = runner.invoke(cli, ["decrypt", "--help"])
        assert result.exit_code == 0

    def test_version_flag(self, runner):
        result = runner.invoke(cli, ["--version"])
        assert result.exit_code == 0
        assert "dbvault" in result.output.lower()


class TestKeygen:
    def test_prints_valid_fernet_key(self, runner):
        result = runner.invoke(cli, ["keygen"])
        assert result.exit_code == 0
        key = _extract_fernet_key(result.output)
        assert key is not None, "No valid Fernet key found in output"
        Fernet(key.encode()) 

    def test_save_writes_key_file(self, runner, tmp_path):
        key_file = str(tmp_path / "key.txt")
        result = runner.invoke(cli, ["keygen", "--save", key_file])
        assert result.exit_code == 0
        assert os.path.exists(key_file)
        content = open(key_file).read().strip()
        Fernet(content.encode()) 

    def test_each_call_produces_different_key(self, runner):
        r1 = runner.invoke(cli, ["keygen"])
        r2 = runner.invoke(cli, ["keygen"])
        k1 = _extract_fernet_key(r1.output)
        k2 = _extract_fernet_key(r2.output)
        assert k1 != k2


class TestDecrypt:
    def test_decrypts_valid_file(self, runner, tmp_path):
        original = tmp_path / "data.bin"
        original.write_bytes(b"hello dbvault backup data!")
        key = generate_key()
        enc = encrypt_file(str(original), key)
        result = runner.invoke(cli, ["decrypt", "--file", enc, "--key", key.decode()])
        assert result.exit_code == 0
        assert "complete" in result.output.lower()
        assert os.path.exists(str(original))

    def test_wrong_key_exits_nonzero(self, runner, tmp_path):
        original = tmp_path / "data.bin"
        original.write_bytes(b"payload")
        key1 = generate_key()
        key2 = generate_key()
        enc = encrypt_file(str(original), key1)
        result = runner.invoke(cli, ["decrypt", "--file", enc, "--key", key2.decode()])
        assert result.exit_code != 0

    def test_missing_file_arg_fails(self, runner):
        result = runner.invoke(cli, ["decrypt", "--key", "somekey"])
        assert result.exit_code != 0

    def test_missing_key_arg_fails(self, runner, tmp_path):
        dummy = tmp_path / "f.enc"
        dummy.write_bytes(b"x")
        result = runner.invoke(cli, ["decrypt", "--file", str(dummy)])
        assert result.exit_code != 0


class TestBackupValidation:
    def test_missing_db_flag_fails(self, runner):
        result = runner.invoke(cli, ["backup", "--database", "mydb", "--output", "/tmp"])
        assert result.exit_code != 0

    def test_missing_database_flag_fails(self, runner):
        result = runner.invoke(cli, ["backup", "--db", "mysql", "--output", "/tmp"])
        assert result.exit_code != 0

    def test_missing_output_flag_fails(self, runner):
        result = runner.invoke(cli, ["backup", "--db", "mysql", "--database", "mydb"])
        assert result.exit_code != 0

    def test_invalid_db_choice_fails(self, runner):
        result = runner.invoke(cli, [
            "backup", "--db", "oracle", "--database", "mydb", "--output", "/tmp",
        ])
        assert result.exit_code != 0

    def test_s3_missing_bucket_fails(self, runner, tmp_path):
        result = runner.invoke(
            cli,
            [
                "backup", "--db", "sqlite",
                "--database", "/nonexistent.db",
                "--output", str(tmp_path),
                "--cloud", "s3",
                "--s3-owner", "123456789012",
            ],
            input="password\n",
        )
        assert result.exit_code != 0

    def test_s3_missing_owner_fails(self, runner, tmp_path):
        result = runner.invoke(
            cli,
            [
                "backup", "--db", "sqlite",
                "--database", "/nonexistent.db",
                "--output", str(tmp_path),
                "--cloud", "s3",
                "--s3-bucket", "my-bucket",
            ],
            input="password\n",
        )
        assert result.exit_code != 0

    def test_azure_missing_conn_str_fails(self, runner, tmp_path):
        result = runner.invoke(
            cli,
            [
                "backup", "--db", "sqlite",
                "--database", "/nonexistent.db",
                "--output", str(tmp_path),
                "--cloud", "azure",
                "--azure-container", "backups",
            ],
            input="password\n",
        )
        assert result.exit_code != 0


class TestBackupSQLite:
    def test_sqlite_backup_succeeds(self, runner, sample_sqlite_db, tmp_path):
        result = runner.invoke(
            cli,
            [
                "backup", "--db", "sqlite",
                "--database", sample_sqlite_db,
                "--output", str(tmp_path),
            ],
            input="irrelevant\n",
        )
        assert result.exit_code == 0
        assert "complete" in result.output.lower()

    def test_sqlite_backup_with_encryption_succeeds(self, runner, sample_sqlite_db, tmp_path):
        result = runner.invoke(
            cli,
            [
                "backup", "--db", "sqlite",
                "--database", sample_sqlite_db,
                "--output", str(tmp_path),
                "--encrypt",
            ],
            input="irrelevant\n",
        )
        assert result.exit_code == 0
        key = _extract_fernet_key(result.output)
        assert key is not None

    def test_sqlite_backup_nonexistent_db_exits_nonzero(self, runner, tmp_path):
        result = runner.invoke(
            cli,
            [
                "backup", "--db", "sqlite",
                "--database", "/does/not/exist.db",
                "--output", str(tmp_path),
            ],
            input="irrelevant\n",
        )
        assert result.exit_code != 0

def _extract_fernet_key(output: str) -> str | None:
    for word in output.split():
        stripped = word.strip()
        if len(stripped) == 44:
            try:
                Fernet(stripped.encode())
                return stripped
            except Exception:
                pass
    return None
