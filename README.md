# Parkbench — DuckLake Streaming Benchmark Tool

A Go CLI that benchmarks data insertion performance into a DuckLake catalog. Metadata can live in **PostgreSQL** (local or remote/managed, e.g. Supabase) or a local **DuckDB** file, and Parquet data can be written to the **local filesystem** or **S3**. Connections can be passed inline via flags, or resolved from a **persistent DuckDB secret** (`CREATE PERSISTENT SECRET`) so credentials never need to be typed into the CLI at all — see [Remote Catalogs & DuckDB Secrets](#remote-catalogs--duckdb-secrets).

![Parkbench architecture diagram](parkbench-diagram.svg)

## Quick Install

Download and run the installer script in one command:

```bash
curl -fsSL https://raw.githubusercontent.com/early-signal-tech/parkbench/main/install.sh | bash
```

**Or with a specific version:**

```bash
curl -fsSL https://raw.githubusercontent.com/early-signal-tech/parkbench/main/install.sh | bash -s v1.0.0
```

**Or to a custom location:**

```bash
curl -fsSL https://raw.githubusercontent.com/early-signal-tech/parkbench/main/install.sh | bash -s latest /opt/parkbench
```

The installer will:

- Detect your OS and architecture automatically
- Download the latest binary from GitHub releases
- Install to `/usr/local/bin/parkbench` (or your custom path)
- Verify the installation works

## Manual Installation

**macOS (Apple Silicon / ARM64):**
```bash
curl -L https://github.com/early-signal-tech/parkbench/releases/download/v1.0.0/parkbench-darwin-arm64 | sudo install -m 755 /dev/stdin /usr/local/bin/parkbench
parkbench --help
```

**macOS (Intel / AMD64):**
```bash
curl -L https://github.com/early-signal-tech/parkbench/releases/download/v1.0.0/parkbench-darwin-amd64 | sudo install -m 755 /dev/stdin /usr/local/bin/parkbench
parkbench --help
```

**Windows (AMD64) - PowerShell (Admin):**
```powershell
$url = "https://github.com/early-signal-tech/parkbench/releases/download/v1.0.0/parkbench-windows-amd64.exe"
$installPath = "$env:ProgramFiles\parkbench\parkbench.exe"
New-Item -ItemType Directory -Force -Path "$env:ProgramFiles\parkbench" | Out-Null
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
Invoke-WebRequest -Uri $url -OutFile $installPath
$env:Path += ";$env:ProgramFiles\parkbench"
[Environment]::SetEnvironmentVariable("Path", $env:Path, [EnvironmentVariableTarget]::User)
parkbench --help
```

## Quick Start

### DuckDB metadata store (no Postgres required)

```bash
# Setup catalog (creates ./ducklake_data/wh.ducklake)
parkbench setup --metadata-store duckdb

# Run benchmarks
parkbench run --metadata-store duckdb

# Reset and start fresh
parkbench reset --metadata-store duckdb --force
```

### Postgres metadata store (default)

```bash
# Prerequisites: Postgres must be running with the catalog database created
brew services start postgresql@18
psql -d postgres -c "CREATE DATABASE ducklake_v1;"

# Setup catalog
parkbench setup

# Run benchmarks
parkbench run

# Reset and start fresh
parkbench reset --force
```

## Metadata Store Backends

The `--metadata-store` flag is available on all commands (`setup`, `run`, `reset`).

| Backend | Flag | Metadata location | Parquet data |
|---------|------|-------------------|--------------|
| `postgres` (default) | `--metadata-store postgres` | Any PostgreSQL database — local, or remote/managed like Supabase | Local directory or `s3://bucket/prefix` |
| `duckdb` | `--metadata-store duckdb` | Local `./ducklake_data/wh.ducklake` file | Local directory |

Use `--data-path` and `--catalog` to customize both the catalog location and Parquet data path. See [Remote Catalogs & DuckDB Secrets](#remote-catalogs--duckdb-secrets) below for connecting to Supabase + S3.

## Remote Catalogs & DuckDB Secrets

Parkbench can reach a DuckLake catalog three ways. All three work with `setup`, `run`, and `reset`.

### 1. Inline, local (default) — zero config

```bash
parkbench setup   # local Postgres (dbname=ducklake_v1 host=localhost) + ./ducklake_data
parkbench run
```

### 2. Inline, remote — pass credentials directly

`--pg-dsn` accepts any Postgres DSN, and `--data-path` accepts an `s3://bucket/prefix` URI. When the data path is S3, parkbench creates a **temporary, in-memory** S3 secret for that session only — nothing is written to disk.

```bash
parkbench setup \
  --pg-dsn "host=db.xxxx.supabase.co port=5432 dbname=postgres user=postgres password=*** sslmode=require" \
  --metadata-schema ducklake_meta \
  --data-path "s3://my-bucket/prefix" \
  --s3-key-id AKIA... --s3-secret-key ... --s3-region us-east-1
```

`--s3-key-id`/`--s3-secret-key`/`--s3-region` fall back to the `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_REGION` environment variables, so you can omit them from the command line entirely:

```bash
export AWS_ACCESS_KEY_ID=AKIA...
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=us-east-1
parkbench setup --pg-dsn "host=db.xxxx.supabase.co ... sslmode=require" --data-path "s3://my-bucket/prefix"
```

`--metadata-schema` maps to DuckLake's `METADATA_SCHEMA` option, scoping the catalog to a single Postgres schema (e.g. `ducklake_meta`) instead of `public`, so unrelated tables/schemas in a shared database don't show up.

### 3. Named persistent secret — one flag, no credentials on the command line

This mirrors the `CREATE PERSISTENT SECRET` pattern from [DuckLake in Production: Catalog and Storage](https://thefulldatastack.substack.com/p/ducklake-in-production-catalog-storage). Create the secrets once (either via the `parkbench secrets` subcommand below, or the `duckdb` CLI directly), then reference them by name forever after:

```bash
parkbench secrets create-postgres --name supabase_pg \
  --host db.xxxx.supabase.co --database postgres --user postgres --password ***

parkbench secrets create-ducklake --name ducklake_prod \
  --data-path "s3://my-bucket/prefix" \
  --metadata-secret supabase_pg \
  --metadata-schema ducklake_meta

parkbench setup --ducklake-secret ducklake_prod
parkbench run   --ducklake-secret ducklake_prod --num-batches 0
```

Persistent secrets live in DuckDB's local, unencrypted secret store and are available across any DuckDB or parkbench session on the machine, regardless of working directory. For shared/production environments, prefer creating secrets yourself via the `duckdb` CLI so credentials never pass through this tool's flags or process list.

### `secrets` — manage persistent DuckDB secrets

```bash
parkbench secrets create-s3       --name s3_bucket   --key-id ... --secret-key ... --region us-east-1
parkbench secrets create-postgres --name supabase_pg --host ... --database postgres --user postgres --password ...
parkbench secrets create-ducklake --name ducklake_prod --data-path s3://bucket/prefix --metadata-secret supabase_pg --metadata-schema ducklake_meta
parkbench secrets list
parkbench secrets drop <name>
```

## Commands

### `setup` — Initialize a DuckLake catalog

Creates the `events`, `events_rich`, and `events_rejected` tables in the catalog.

```bash
parkbench setup
parkbench setup --metadata-store duckdb
parkbench setup --metadata-store postgres --pg-dsn "dbname=ducklake_v1 host=localhost" --data-path "./ducklake_data" --catalog wh
parkbench setup --ducklake-secret ducklake_prod
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--catalog`, `-c` | `wh` | Catalog alias used inside DuckDB SQL |
| `--ducklake-secret` | _(none)_ | Name of a pre-created persistent `DUCKLAKE` secret; when set, all flags below are ignored |
| `--metadata-catalog-name` | _(none)_ | Expose DuckLake's metadata tables under this name in DuckDB (`METADATA_CATALOG`) |
| `--metadata-store` | `postgres` | Metadata store backend: `postgres` or `duckdb` (ignored with `--ducklake-secret`) |
| `--pg-dsn` | `dbname=ducklake_v1 host=localhost` | Postgres DSN — local or remote/managed, e.g. Supabase (postgres mode only) |
| `--metadata-schema` | _(none)_ | Postgres schema for DuckLake's metadata tables (`METADATA_SCHEMA`); defaults to `public` (postgres mode only) |
| `--data-path` | `./ducklake_data` | Where Parquet files are written: a local directory, or an `s3://bucket/prefix` URI |
| `--s3-key-id` / `--s3-secret-key` / `--s3-region` | _(none)_ | S3 credentials, used only when `--data-path` is `s3://...`; fall back to `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_REGION` |

### `run` — Benchmark insertion

Inserts rows continuously and prints throughput stats.

```bash
# Batch mode, simple schema (default)
parkbench run

# DuckDB metadata store
parkbench run --metadata-store duckdb

# Rich schema
parkbench run --mode rich

# Ticker mode (1 row/sec for 60s)
parkbench run --run-mode ticker --duration 60

# Ticker mode with 15% duplicate injection
parkbench run --run-mode ticker --duration 60 --duplicate-rate 0.15

# Ticker mode with 20% schema drift
parkbench run --run-mode ticker --duration 60 --schema-drift-rate 0.20

# Run forever
parkbench run --num-batches 0

# Against a remote catalog via a named secret
parkbench run --ducklake-secret ducklake_prod --num-batches 0
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--catalog`, `-c` | `wh` | Catalog alias |
| `--ducklake-secret` | _(none)_ | Name of a pre-created persistent `DUCKLAKE` secret; when set, connection flags below are ignored |
| `--metadata-catalog-name` | _(none)_ | Expose DuckLake's metadata tables under this name in DuckDB (`METADATA_CATALOG`) |
| `--metadata-store` | `postgres` | Metadata store backend: `postgres` or `duckdb` (ignored with `--ducklake-secret`) |
| `--pg-dsn` | `dbname=ducklake_v1 host=localhost` | Postgres DSN — local or remote/managed, e.g. Supabase (postgres mode only) |
| `--metadata-schema` | _(none)_ | Postgres schema for DuckLake's metadata tables (`METADATA_SCHEMA`); defaults to `public` (postgres mode only) |
| `--data-path` | `./ducklake_data` | Parquet data directory: a local directory, or an `s3://bucket/prefix` URI |
| `--s3-key-id` / `--s3-secret-key` / `--s3-region` | _(none)_ | S3 credentials, used only when `--data-path` is `s3://...`; fall back to `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_REGION` |
| `--mode`, `-m` | `simple` | Schema mode: `simple` or `rich` |
| `--run-mode`, `-r` | `batch` | Run mode: `batch` or `ticker` |
| `--table`, `-t` | _(auto)_ | Table name (defaults to `events` or `events_rich`) |
| `--batch-size`, `-b` | `100000` | Rows per batch (batch mode only) |
| `--num-batches`, `-n` | `10` | Number of batches; `0` = run forever |
| `--flush-interval`, `-k` | `10` | Flush inlined rows to Parquet every N batches (batch mode) or at end if inlined rows > N (ticker mode); `0` = never |
| `--duration`, `-d` | `60` | Duration in seconds (ticker mode only) |
| `--duplicate-rate` | `0.0` | Probability (0.0–1.0) of injecting a duplicate row each tick (ticker mode only) |
| `--schema-drift-rate` | `0.0` | Probability (0.0–1.0) of injecting a schema-breaking row each tick (ticker mode only) |

### `reset` — Drop and recreate the catalog

Behavior depends on whether the catalog is local or remote:

- **Local** (local Postgres + local data path, or `--metadata-store duckdb`): drops and recreates the Postgres database via `psql` (or deletes the `.ducklake` file), and wipes the local data directory — the original destructive behavior, unchanged.
- **Remote** (remote/managed Postgres, S3 data path, or `--ducklake-secret`): parkbench doesn't own that shared infrastructure outright, so instead it just drops the DuckLake-tracked tables (`events`, `events_rich`, `events_rejected`) inside the catalog. Parquet files already written to S3 are **not** deleted automatically — a note is printed suggesting a lifecycle rule or manual `aws s3 rm` if you need a full wipe.

Either way, `setup` is re-run afterward to recreate fresh tables.

```bash
parkbench reset               # prompts "yes" to confirm (local postgres)
parkbench reset --force       # skip confirmation
parkbench reset --metadata-store duckdb --force
parkbench reset --ducklake-secret ducklake_prod --force   # drops tables only, leaves Supabase/S3 infra intact
```

**Flags:** same connection flags as `setup`/`run` (see above), plus:

| Flag | Default | Description |
|------|---------|-------------|
| `--force`, `-f` | `false` | Skip confirmation prompt |

## Querying from DuckDB CLI

Run `duckdb` from the same working directory used by `parkbench run`, then:

### Postgres-backed catalog

```sql
INSTALL ducklake;
LOAD ducklake;
ATTACH 'ducklake:postgres:dbname=ducklake_v1 host=localhost' AS wh
  (DATA_PATH './ducklake_data', AUTOMATIC_MIGRATION TRUE);

SELECT COUNT(*) FROM wh.events;
SELECT * FROM wh.events LIMIT 10;
```

### DuckDB-backed catalog

```sql
INSTALL ducklake;
LOAD ducklake;
ATTACH 'ducklake:duckdb:./ducklake_data/wh.ducklake' AS wh
  (DATA_PATH './ducklake_data', AUTOMATIC_MIGRATION TRUE);

SELECT COUNT(*) FROM wh.events;
SELECT * FROM wh.events LIMIT 10;
```

### Useful queries

```sql
-- Check schema-drift rejections
SELECT COUNT(*) FROM wh.events_rejected WHERE anomaly_type = 'schema_drift';
SELECT * FROM wh.events_rejected ORDER BY rejected_at DESC LIMIT 10;

-- Check DuckLake settings
SELECT * FROM ducklake_settings('wh');
```

## Schema Modes

**simple** — `wh.events`
```sql
id INTEGER, ts TIMESTAMP, event_type VARCHAR
```

**rich** — `wh.events_rich`
```sql
id INTEGER, user_id VARCHAR, event_type VARCHAR, ts TIMESTAMP, payload JSON, metadata JSON
```

**rejected (dead letter)** — `wh.events_rejected`
```sql
rejected_at TIMESTAMP, source_table VARCHAR, anomaly_type VARCHAR,
attempted_id INTEGER, error_message VARCHAR, payload JSON
```

## Building

```bash
go build -o parkbench .
```

## Requirements

- Go 1.24.0+
- DuckDB via `github.com/duckdb/duckdb-go/v2`
- PostgreSQL (only for `--metadata-store postgres`)

## Development

The tool uses:
- `github.com/spf13/cobra` for CLI framework
- `github.com/duckdb/duckdb-go/v2` for database access

Data is inserted using randomized SQL generation from predefined vocabularies:
- Event types: click, view, purchase, signup, logout, search, share
- Pages: /home, /product, /cart, /checkout, /profile, /search, /settings
- Sources: web, mobile, api, email, push
- Countries: US, GB, DE, FR, JP, CA, AU, BR
