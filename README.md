# DBVault

```
   ___  ____  _   __          ____
  / _ \/ __ )| | / /___ _____/ / /_
 / // / __  || |/ / __ `/ __/ / __/
/____/_/ /_/ |___/\__,_/\__/_/\__/
```

**Encrypted · Cloud-ready · Multi-database backup utility**

[![Python](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://python.org)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![PyPI](https://img.shields.io/pypi/v/dbvault.svg)](https://pypi.org/project/dbvault/)
[![Tests](https://img.shields.io/badge/tests-pytest-orange.svg)](tests/)

DBVault is a command-line backup utility that gives you a consistent pipeline across six database engines: **dump → validate → compress → encrypt → upload**. Every step is covered with retry/backoff logic and all operations are available in both sync and async modes.

---

## Features

| Feature | Detail |
|---|---|
| **6 database engines** | MySQL, PostgreSQL, MongoDB, Redis, SQLite, IBM Db2 |
| **Gzip compression** | Every backup is compressed before storage |
| **Fernet encryption** | Optional AES-128-CBC + HMAC-SHA256 (symmetric, authenticated) |
| **Cloud upload** | AWS S3, Azure Blob Storage, Google Cloud Storage (GCS), and MinIO |
| **Retry + backoff** | 3 attempts, exponential 2–10 s (powered by `tenacity`) |
| **Async execution** | `async_perform_backup_pipeline` via `asyncio.to_thread` |
| **Validation** | Each backup is restored to a temp target and verified before being kept |
| **Clean CLI** | `click`-powered interface with `pyfiglet` banner |

---

## Supported Databases

| Alias | Engine | Backup tool | Validation method |
|---|---|---|---|
| `mysql` | MySQL / MariaDB | `mysqldump` | restore to temp DB via `mysql` |
| `postgres` | PostgreSQL | `pg_dump` | restore to temp DB via `psql` |
| `mongo` | MongoDB | `mongodump --archive` | `mongorestore --nsFrom/--nsTo` |
| `redis` | Redis | `redis-cli --rdb` | RDB magic-byte check (`REDIS`) |
| `sqlite` | SQLite | `sqlite3.Connection.backup()` | `PRAGMA integrity_check` |
| `db2` | IBM Db2 | `db2 BACKUP DATABASE` | `db2ckbkp` |

---

## Installation

### From PyPI

```bash
pip install dbvault
```

### From source (development)

```bash
git clone https://github.com/Abhishek772/dbvault
cd dbvault
uv sync --group dev
```

---

## Quick Start

### 1. Back up a MySQL database

```bash
dbvault backup \
  --db mysql \
  --host localhost \
  --user root \
  --database mydb \
  --output ./backups
```

### 2. Back up with encryption

```bash
dbvault backup \
  --db postgres \
  --host db.internal \
  --user admin \
  --database analytics \
  --output ./backups \
  --encrypt
# DBVault prints the generated key — save it!
```

### 3. Back up directly to S3 (with encryption)

```bash
dbvault backup \
  --db mysql \
  --host localhost \
  --user root \
  --database mydb \
  --output ./backups \
  --encrypt \
  --cloud s3 \
  --s3-bucket my-backup-bucket \
  --s3-owner 123456789012
```

### 4. Back up directly to GCP Cloud Storage

```bash
dbvault backup \
  --db postgres \
  --host localhost \
  --user admin \
  --database mydb \
  --output ./backups \
  --cloud gcp \
  --gcp-bucket my-gcp-backup-bucket
  # Uses GOOGLE_APPLICATION_CREDENTIALS environment variable
```

### 5. Back up to MinIO

```bash
dbvault backup \
  --db mongo \
  --host localhost \
  --user admin \
  --database myapp \
  --output ./backups \
  --cloud minio \
  --minio-endpoint play.min.io \
  --minio-bucket my-minio-bucket
  # Uses MINIO_ACCESS_KEY and MINIO_SECRET_KEY environment variables
```

### 4. Decrypt a backup

```bash
dbvault decrypt \
  --file ./backups/backup.sql.gz.enc \
  --key <your-fernet-key>
```

### 5. Generate an encryption key

```bash
dbvault keygen
# or save directly to a file
dbvault keygen --save ~/.dbvault.key
```

---

## CLI Reference

### `dbvault backup`

```
Options:
  -d, --db       [mysql|postgres|mongo|redis|sqlite|db2]  Database engine  [required]
  -H, --host     TEXT    Database host  [default: localhost]
  -u, --user     TEXT    Database username
  -p, --password TEXT    Database password (prompted if omitted)
  -D, --database TEXT    Database name / SQLite file path  [required]
  -o, --output   PATH    Output directory  [required]
  -e, --encrypt          Fernet-encrypt the backup
  -k, --key      TEXT    Existing Fernet key (generated if --encrypt and omitted)
  -c, --cloud    [s3|azure|gcp|minio]  Upload to cloud after backup
  --s3-bucket    TEXT    S3 bucket name
  --s3-key       TEXT    S3 object key
  --s3-owner     TEXT    Expected S3 bucket owner — 12-digit AWS account ID
  --azure-conn-str TEXT  Azure Storage connection string
  --azure-container TEXT Azure container name
  --azure-blob   TEXT    Azure blob name
  --gcp-bucket   TEXT    GCP bucket name
  --gcp-blob     TEXT    GCP blob name (optional)
  --minio-endpoint TEXT  MinIO endpoint URL
  --minio-bucket TEXT    MinIO bucket name
  --minio-object TEXT    MinIO object name (optional)
  -a, --async-mode       Run asynchronously
  -h, --help             Show this message and exit.
```

### `dbvault keygen`

```
Options:
  -s, --save PATH  Write key to a file
  -h, --help       Show this message and exit.
```

### `dbvault decrypt`

```
Options:
  -f, --file PATH  Encrypted backup file (.enc)  [required]
  -k, --key  TEXT  Fernet key used during encryption  [required]
  -h, --help       Show this message and exit.
```

---

## Architecture

```
dbvault backup
     │
     ▼
DatabaseBackupManager (ABC)
     │
     ├── connect()              — open DB connection
     ├── _run_*dump()           — engine-specific dump subprocess
     ├── validate()             — restore to temp target, verify, drop
     ├── compress()             — gzip the dump file
     ├── encrypt()              — Fernet encrypt (optional)
     ├── _upload_to_cloud()     — S3 / Azure / GCP / MinIO upload (optional)
     └── perform_backup_pipeline()   ← @retry(3×, exp backoff 2-10 s)
         async_perform_backup_pipeline()  ← asyncio.to_thread wrapper
```

```
core/
├── interfaces/
│   └── backup_utility_interface.py   # Abstract base class
├── helpers/
│   ├── cryptographic_helper.py       # Fernet generate / encrypt / decrypt
│   └── blobstorage_uploader.py       # S3, Azure, GCP, MinIO upload (sync + async)
└── services/
    ├── sql_backup_utility.py         # MySQL
    ├── postgres_backup_utility.py    # PostgreSQL
    ├── mongo_backup_utility.py       # MongoDB
    ├── redis_backup_utility.py       # Redis
    ├── sqllite_backup_utility.py     # SQLite
    └── ibm_db2_backup_uitlity.py     # IBM Db2
cli/
└── app.py                            # Click CLI entry point
tests/
├── conftest.py
├── test_cryptographic_helper.py
├── test_sqlite_backup.py
├── test_sql_backup.py
├── test_blobstorage_uploader.py
└── test_cli.py
```

---

## Development

### Setup

```bash
git clone https://github.com/Abhishek772/dbvault
cd dbvault
uv sync --group dev
```

### Run tests

```bash
pytest
# with coverage
pytest --cov=core --cov=cli --cov-report=term-missing
```

### Run the CLI from source

```bash
python main.py backup --db sqlite --database ./my.db --output ./out
# or
uv run dbvault backup --db sqlite --database ./my.db --output ./out
```

### Build for PyPI

```bash
uv build
# produces dist/dbvault-0.1.0-py3-none-any.whl and .tar.gz
```

### Publish

```bash
uv publish --token $PYPI_TOKEN
```

---

## Adding a New Database Engine

1. Create `core/services/<engine>_backup_utility.py`
2. Extend `DatabaseBackupManager` and implement all abstract methods:
   - `connect()`, `backup()`, `validate()`, `compress()`, `encrypt()`,
     `perform_backup_pipeline()`, `async_perform_backup_pipeline()`
3. Register the alias in `cli/app.py → DB_MANAGERS`
4. Add test coverage in `tests/test_<engine>_backup.py`

---

## Security Notes

- Passwords are passed via environment variables (`MYSQL_PWD`, `PGPASSWORD`) or the tool's own `--password` flag and are never written to disk.
- S3 uploads enforce `ExpectedBucketOwner` to prevent confused-deputy bucket hijacking.
- Fernet encryption is authenticated (HMAC-SHA256); tampering with the ciphertext raises `InvalidToken`.
- Encryption keys are printed once at generation time and are never stored by DBVault — keep them safe.

---

## License

MIT — see [LICENSE](LICENSE).
