import asyncio
import os
import re
import sys
from importlib.metadata import PackageNotFoundError, version

import click
import pyfiglet

from core.helpers.cryptographic_helper import decrypt_file, generate_key
from core.services.ibm_db2_backup_uitlity import IBMDB2DatabaseBackupManager
from core.services.mongo_backup_utility import MongoDatabaseBackupManager
from core.services.postgres_backup_utility import PostgresDatabaseBackupManager
from core.services.redis_backup_utility import RedisDatabaseBackupManager
from core.services.sql_backup_utility import SQLDatabaseBackupManager
from core.services.sqllite_backup_utility import SQLiteDatabaseBackupManager

try:
    __version__ = version("dbvault")
except PackageNotFoundError:
    __version__ = "0.1.0"

DB_MANAGERS = {
    "mysql": SQLDatabaseBackupManager,
    "postgres": PostgresDatabaseBackupManager,
    "mongo": MongoDatabaseBackupManager,
    "redis": RedisDatabaseBackupManager,
    "sqlite": SQLiteDatabaseBackupManager,
    "db2": IBMDB2DatabaseBackupManager,
}


def _banner() -> None:
    art = pyfiglet.figlet_format("DBVault", font="slant")
    click.echo(click.style(art, fg="cyan", bold=True))
    click.echo(
        click.style(
            "  Encrypted · Cloud-ready · Multi-database backup utility\n",
            fg="bright_white",
        )
    )


@click.group(invoke_without_command=True, context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(__version__, "-V", "--version", prog_name="dbvault")
@click.pass_context
def cli(ctx: click.Context) -> None:
    """DBVault — encrypted, cloud-ready database backup utility."""
    if ctx.invoked_subcommand is None:
        _banner()
        click.echo(ctx.get_help())


# ── backup ────────────────────────────────────────────────────────────────────

@cli.command()
@click.option(
    "--db", "-d", required=True,
    type=click.Choice(list(DB_MANAGERS), case_sensitive=False),
    help="Database engine to back up.",
)
@click.option("--host", "-H", default="localhost", show_default=True, help="Database host.")
@click.option("--user", "-u", default="", help="Database username.")
@click.option(
    "--password", "-p", default=None,
    help="Database password (prompted securely if omitted).",
)
@click.option(
    "--database", "-D", required=True,
    help="Database name. For SQLite: path to the .db file.",
)
@click.option(
    "--output", "-o", required=True,
    type=click.Path(file_okay=False, writable=True),
    help="Directory to write the backup file into.",
)
@click.option("--encrypt", "-e", is_flag=True, help="Fernet-encrypt the backup.")
@click.option(
    "--key", "-k", default=None,
    help="Base64 Fernet key for encryption. Auto-generated when --encrypt is set and omitted.",
)
@click.option(
    "--cloud", "-c", default=None,
    type=click.Choice(["s3", "azure"], case_sensitive=False),
    help="Upload the finished backup to cloud storage.",
)
@click.option("--s3-bucket", default=None, help="S3 bucket name.")
@click.option("--s3-key", default=None, help="S3 object key (defaults to the backup filename).")
@click.option(
    "--s3-owner", default=None,
    help="Expected S3 bucket owner — your 12-digit AWS account ID (required for S3).",
)
@click.option("--azure-conn-str", default=None, help="Azure Storage connection string.")
@click.option("--azure-container", default=None, help="Azure Blob container name.")
@click.option("--azure-blob", default=None, help="Blob name (defaults to the backup filename).")
@click.option("--async-mode", "-a", is_flag=True, help="Run the pipeline asynchronously.")
def backup( #NOSONAR
    db, host, user, password, database, output, #NOSONAR
    encrypt, key, cloud,
    s3_bucket, s3_key, s3_owner,
    azure_conn_str, azure_container, azure_blob,
    async_mode,
) -> None:
    """Run a database backup pipeline: dump → validate → compress → [encrypt] → [upload]."""
    _banner()

    if password is None:
        password = click.prompt("  Password", hide_input=True, default="", prompt_suffix=" ")

    # ── encryption ────────────────────────────────────────────────────────────
    encryption_key: bytes | None = None
    if encrypt:
        if key:
            encryption_key = key.encode()
        else:
            encryption_key = generate_key()
            click.echo(
                click.style("  ⚠  New encryption key (store this — required to decrypt):\n", fg="yellow")
                + click.style(f"     {encryption_key.decode()}\n", fg="bright_yellow", bold=True)
            )

    # ── cloud kwargs ──────────────────────────────────────────────────────────
    kwargs: dict = {}
    if cloud:
        kwargs["cloud_provider"] = cloud
        if cloud == "s3":
            if not s3_bucket:
                raise click.UsageError("--s3-bucket is required for S3 uploads.")
            if not s3_owner:
                raise click.UsageError("--s3-owner (AWS account ID) is required for S3 uploads.")
            kwargs["s3_bucket"] = s3_bucket
            kwargs["s3_expected_owner"] = s3_owner
            if s3_key:
                kwargs["s3_key"] = s3_key
        elif cloud == "azure":
            if not azure_conn_str:
                raise click.UsageError("--azure-conn-str is required for Azure uploads.")
            if not azure_container:
                raise click.UsageError("--azure-container is required for Azure uploads.")
            kwargs["azure_conn_str"] = azure_conn_str
            kwargs["azure_container"] = azure_container
            if azure_blob:
                kwargs["azure_blob_name"] = azure_blob

    if encryption_key:
        kwargs["encryption_key"] = encryption_key

    # ── run pipeline ──────────────────────────────────────────────────────────
    os.makedirs(output, exist_ok=True)
    manager = DB_MANAGERS[db.lower()]()

    click.echo(
        click.style(f"  [{db.upper()}] ", fg="cyan", bold=True)
        + click.style(f"Backing up '{database}' …", fg="bright_white")
    )

    try:
        if async_mode:
            success, path = asyncio.run(
                manager.async_perform_backup_pipeline(
                    host=host, user=user, password=password,
                    database_name=database, file_path=output, **kwargs,
                )
            )
        else:
            success, path = manager.perform_backup_pipeline(
                host=host, user=user, password=password,
                database_name=database, file_path=output, **kwargs,
            )

        if success:
            click.echo(click.style("  ✓ Backup complete!", fg="green", bold=True))
            click.echo(click.style(f"  → {path}", fg="bright_white"))
        else:
            click.echo(click.style("  ✗ Backup reported failure.", fg="red", bold=True))
            sys.exit(1)

    except Exception as exc:
        click.echo(click.style(f"  ✗ {exc}", fg="red", bold=True))
        sys.exit(1)


# ── keygen ────────────────────────────────────────────────────────────────────

@cli.command()
@click.option(
    "--save", "-s", default=None,
    type=click.Path(dir_okay=False, writable=True),
    help="Write the key to a file instead of only printing it.",
)
def keygen(save: str | None) -> None:
    """Generate a new Fernet encryption key."""
    _banner()
    key = generate_key()
    click.echo(click.style("  Generated encryption key:", fg="yellow"))
    click.echo(click.style(f"  {key.decode()}", fg="bright_yellow", bold=True))
    if save:
        with open(save, "w") as fh:
            fh.write(key.decode())
        click.echo(click.style(f"\n  Key saved → {save}", fg="green"))


# ── decrypt ───────────────────────────────────────────────────────────────────

@cli.command()
@click.option(
    "--file", "-f", required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Path to the .enc backup file to decrypt.",
)
@click.option("--key", "-k", required=True, help="Base64 Fernet key used during encryption.")
def decrypt(file: str, key: str) -> None:
    """Decrypt a Fernet-encrypted backup file."""
    _banner()
    click.echo(click.style(f"  Decrypting: {file}", fg="bright_white"))
    try:
        out = decrypt_file(file, key.encode())
        click.echo(click.style("  ✓ Decryption complete!", fg="green", bold=True))
        click.echo(click.style(f"  → {out}", fg="bright_white"))
    except Exception as exc:
        click.echo(click.style(f"  ✗ {exc}", fg="red", bold=True))
        sys.exit(1)


def main() -> None:
    cli()
