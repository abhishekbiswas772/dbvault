import asyncio
import os
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
    if ctx.invoked_subcommand is None:
        _banner()
        click.echo(ctx.get_help())


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
    type=click.Choice(["s3", "azure", "gcs", "minio"], case_sensitive=False),
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
@click.option("--gcs-bucket", default=None, help="GCS bucket name.")
@click.option("--gcs-blob", default=None, help="GCS object/blob name (defaults to filename).")
@click.option(
    "--gcs-credentials", default=None,
    help="Path to GCS service account JSON (defaults to GOOGLE_APPLICATION_CREDENTIALS env var).",
)
@click.option("--minio-endpoint", default=None, help="MinIO server endpoint (e.g. localhost:9000).")
@click.option("--minio-access-key", default=None, help="MinIO access key.")
@click.option("--minio-secret-key", default=None, help="MinIO secret key.")
@click.option("--minio-bucket", default=None, help="MinIO bucket name.")
@click.option("--minio-object", default=None, help="MinIO object name (defaults to filename).")
@click.option("--minio-secure/--no-minio-secure", default=True, help="Use TLS for MinIO (default: on).")
@click.option("--async-mode", "-a", is_flag=True, help="Run the pipeline asynchronously.")
def backup( #NOSONAR
    db, host, user, password, database, output, #NOSONAR
    encrypt, key, cloud,
    s3_bucket, s3_key, s3_owner,
    azure_conn_str, azure_container, azure_blob,
    gcs_bucket, gcs_blob, gcs_credentials,
    minio_endpoint, minio_access_key, minio_secret_key, minio_bucket, minio_object, minio_secure,
    async_mode,
) -> None:
    _banner()

    if password is None:
        password = click.prompt("  Password", hide_input=True, default="", prompt_suffix=" ")

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
        elif cloud == "gcs":
            if not gcs_bucket:
                raise click.UsageError("--gcs-bucket is required for GCS uploads.")
            kwargs["gcs_bucket"] = gcs_bucket
            if gcs_blob:
                kwargs["gcs_blob_name"] = gcs_blob
            if gcs_credentials:
                kwargs["gcs_credentials"] = gcs_credentials
        elif cloud == "minio":
            if not minio_endpoint:
                raise click.UsageError("--minio-endpoint is required for MinIO uploads.")
            if not minio_access_key:
                raise click.UsageError("--minio-access-key is required for MinIO uploads.")
            if not minio_secret_key:
                raise click.UsageError("--minio-secret-key is required for MinIO uploads.")
            if not minio_bucket:
                raise click.UsageError("--minio-bucket is required for MinIO uploads.")
            kwargs["minio_endpoint"] = minio_endpoint
            kwargs["minio_access_key"] = minio_access_key
            kwargs["minio_secret_key"] = minio_secret_key
            kwargs["minio_bucket"] = minio_bucket
            kwargs["minio_secure"] = minio_secure
            if minio_object:
                kwargs["minio_object_name"] = minio_object

    if encryption_key:
        kwargs["encryption_key"] = encryption_key

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


@cli.command()
@click.option(
    "--save", "-s", default=None,
    type=click.Path(dir_okay=False, writable=True),
    help="Write the key to a file instead of only printing it.",
)
def keygen(save: str | None) -> None:
    _banner()
    key = generate_key()
    click.echo(click.style("  Generated encryption key:", fg="yellow"))
    click.echo(click.style(f"  {key.decode()}", fg="bright_yellow", bold=True))
    if save:
        with open(save, "w") as fh:
            fh.write(key.decode())
        click.echo(click.style(f"\n  Key saved → {save}", fg="green"))


@cli.command()
@click.option(
    "--file", "-f", required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Path to the .enc backup file to decrypt.",
)
@click.option("--key", "-k", required=True, help="Base64 Fernet key used during encryption.")
def decrypt(file: str, key: str) -> None:
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
