# parkbench — DuckLake Streaming Benchmark Tool

A Go CLI that benchmarks data insertion performance into a DuckLake catalog. Metadata can live in PostgreSQL (local or remote/managed, e.g. Supabase) or a local DuckDB file; Parquet data can live on the local filesystem or S3. Connections are resolved via `ConnectionConfig` (see `connection.go`), which supports three modes — see "Connection Modes" below.

## Architecture

- **Metadata store**: PostgreSQL (local or remote, e.g. Supabase) or a local DuckDB `.ducklake` file
- **Data store**: Parquet files, either on local disk (default: `./ducklake_data/`) or S3 (`s3://bucket/prefix`)
- **Query engine**: DuckDB with the `ducklake` extension (via `github.com/duckdb/duckdb-go/v2`)
- **Catalog alias**: `wh` (how the catalog is referenced inside DuckDB SQL)

The tool uses DuckDB in-process (not a DuckDB server). Each command opens an in-memory DuckDB instance and `ATTACH`es to the DuckLake catalog via `openAndAttach()` in `connection.go`.

### Connection Modes

`ConnectionConfig` (in `connection.go`) resolves the `ATTACH` statement one of three ways:

1. **Named persistent secret** (`--ducklake-secret <name>`) — everything else is ignored; attaches via `ATTACH 'ducklake:<secret>' AS wh (...)`. The secret (created via `parkbench secrets create-ducklake` or the `duckdb` CLI) already bundles the Postgres connection, `DATA_PATH`, and metadata schema.
2. **Inline, local DuckDB** (`--metadata-store duckdb`) — a local `.ducklake` file plus a local data directory. Unchanged from the original implementation; zero-config local dev.
3. **Inline, Postgres** (`--metadata-store postgres`, default) — any Postgres via `--pg-dsn` (local or remote/managed like Supabase), writing to `--data-path` (a local directory or an `s3://bucket/prefix` URI). When the data path is S3 and no secret is set, parkbench creates a **temporary, non-persistent** `CREATE SECRET` for S3 credentials (`--s3-key-id`/`--s3-secret-key`/`--s3-region`, or `AWS_*` env vars) for that session only.

`ConnectionConfig.isRemote()` determines whether a connection points at infrastructure parkbench doesn't own outright (named secret, S3 storage, or non-localhost Postgres host) — this changes `reset` behavior (see below).

### ATTACH string formats

```sql
-- inline, local postgres (default)
ATTACH 'ducklake:postgres:dbname=ducklake_v1 host=localhost' AS wh
    (DATA_PATH './ducklake_data', AUTOMATIC_MIGRATION TRUE)

-- inline, remote postgres + s3, scoped to a metadata schema
ATTACH 'ducklake:postgres:host=db.xxxx.supabase.co port=5432 dbname=postgres user=postgres password=*** sslmode=require' AS wh
    (DATA_PATH 's3://bucket/prefix', AUTOMATIC_MIGRATION TRUE, METADATA_SCHEMA 'ducklake_meta')

-- named secret
ATTACH 'ducklake:ducklake_prod' AS wh (AUTOMATIC_MIGRATION TRUE, METADATA_CATALOG 'ducklake_prod_meta')
```

## DuckDB Secrets

Parkbench can create and consume DuckDB's persistent secrets (`CREATE PERSISTENT SECRET`), matching the pattern from [DuckLake in Production: Catalog and Storage](https://thefulldatastack.substack.com/p/ducklake-in-production-catalog-storage):

```bash
./parkbench secrets create-s3       --name s3_bucket   --key-id ... --secret-key ... --region us-east-1
./parkbench secrets create-postgres --name supabase_pg --host db.xxxx.supabase.co --database postgres --user postgres --password ...
./parkbench secrets create-ducklake --name ducklake_prod --data-path s3://bucket/prefix --metadata-secret supabase_pg --metadata-schema ducklake_meta
./parkbench secrets list
./parkbench secrets drop <name>
```

These are implemented in `secrets.go` and just execute the literal `CREATE PERSISTENT SECRET`/`DROP PERSISTENT SECRET` SQL via an ephemeral DuckDB connection — persistent secrets live in DuckDB's local secret store regardless of parkbench's process, so they're also visible/manageable from the plain `duckdb` CLI (`FROM duckdb_secrets();`).

## Prerequisites

For local Postgres mode (the default), PostgreSQL must be running and the catalog database must exist before any command is run:

```bash
brew services start postgresql@18
psql -d postgres -c "CREATE DATABASE ducklake_v1;"
```

> The Go tool does NOT start Postgres or create the database — that must be done manually as a one-time step. This is unnecessary for remote Postgres (Supabase, etc.), since the database already exists.

## Build

```bash
go build -o parkbench .
```

## Commands

### `setup` — one-time catalog initialization

Creates the `events`, `events_rich`, and `events_rejected` tables inside the DuckLake catalog. `CREATE TABLE IF NOT EXISTS` is used throughout, so `setup` is safe to re-run against an existing catalog (including a real production one).

```bash
./parkbench setup
./parkbench setup --pg-dsn "dbname=ducklake_v1 host=localhost" --data-path "./ducklake_data" --catalog wh
./parkbench setup --ducklake-secret ducklake_prod
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--catalog`, `-c` | `wh` | Catalog alias used inside DuckDB SQL |
| `--ducklake-secret` | _(none)_ | Name of a pre-created persistent `DUCKLAKE` secret; when set, all flags below are ignored |
| `--metadata-catalog-name` | _(none)_ | Expose DuckLake's metadata tables under this name in DuckDB (`METADATA_CATALOG`) |
| `--metadata-store` | `postgres` | Metadata store backend: `postgres` or `duckdb` (ignored with `--ducklake-secret`) |
| `--pg-dsn` | `dbname=ducklake_v1 host=localhost` | Postgres DSN for the metadata store — local or remote/managed, e.g. Supabase |
| `--metadata-schema` | _(none)_ | Postgres schema for DuckLake's metadata tables (`METADATA_SCHEMA`); defaults to `public` |
| `--data-path` | `./ducklake_data` | Where Parquet files are written: a local directory, or an `s3://bucket/prefix` URI |
| `--s3-key-id` / `--s3-secret-key` / `--s3-region` | _(none)_ | S3 credentials, used only when `--data-path` is `s3://...`; fall back to `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_REGION` |

### `run` — benchmark insertion

Inserts rows continuously and prints throughput stats.

```bash
# batch mode, simple schema (default)
./parkbench run

# batch mode, rich schema
./parkbench run --mode rich

# batch mode, rich_ml schema (200 users, ML-optimized data)
./parkbench run --mode rich_ml --num-users 200 --batch-size 50000 --num-batches 20

# ticker mode (1 row/sec for 60s)
./parkbench run --run-mode ticker --duration 60

# ticker mode, rich_ml schema (streaming ML training data)
./parkbench run --mode rich_ml --num-users 500 --run-mode ticker --duration 300

# rich_ml with the ML label column (adds is_converted BOOLEAN)
./parkbench run --mode rich_ml --num-users 200 --ml-labels

# rich_ml with a custom positive-class rate
./parkbench run --mode rich_ml --ml-labels --conversion-rate 0.30

# ticker mode with 15% duplicate injection
./parkbench run --run-mode ticker --duration 60 --duplicate-rate 0.15

# ticker mode with 20% schema drift (inserts with unknown column are rejected)
./parkbench run --run-mode ticker --duration 60 --schema-drift-rate 0.20

# ticker mode with both duplicate and schema-drift anomalies
./parkbench run --run-mode ticker --duration 60 --duplicate-rate 0.10 --schema-drift-rate 0.15

# run forever
./parkbench run --num-batches 0

# against a remote catalog via a named secret
./parkbench run --ducklake-secret ducklake_prod --num-batches 0
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--catalog`, `-c` | `wh` | Catalog alias |
| `--ducklake-secret` | _(none)_ | Name of a pre-created persistent `DUCKLAKE` secret; when set, connection flags below are ignored |
| `--metadata-catalog-name` | _(none)_ | Expose DuckLake's metadata tables under this name in DuckDB (`METADATA_CATALOG`) |
| `--metadata-store` | `postgres` | Metadata store backend: `postgres` or `duckdb` (ignored with `--ducklake-secret`) |
| `--pg-dsn` | `dbname=ducklake_v1 host=localhost` | Postgres DSN — local or remote/managed, e.g. Supabase |
| `--metadata-schema` | _(none)_ | Postgres schema for DuckLake's metadata tables (`METADATA_SCHEMA`); defaults to `public` |
| `--data-path` | `./ducklake_data` | Parquet data directory: a local directory, or an `s3://bucket/prefix` URI |
| `--s3-key-id` / `--s3-secret-key` / `--s3-region` | _(none)_ | S3 credentials, used only when `--data-path` is `s3://...`; fall back to `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_REGION` |
| `--mode`, `-m` | `simple` | Schema mode: `simple`, `rich`, or `rich_ml` (ML-optimized) |
| `--num-users` | `100` | Number of distinct users for `rich_ml` mode (configurable user profiles with realistic behavior patterns); ignored for `simple` and `rich` modes |
| `--ml-labels` | `false` | Add an `is_converted BOOLEAN` label column to `rich_ml` events; when unset the column is absent from both the DDL and every insert. Requires `--mode rich_ml` |
| `--conversion-rate` | `0.15` | Probability (0.0–1.0) that `is_converted` is true; requires `--ml-labels` |
| `--run-mode`, `-r` | `batch` | Run mode: `batch` or `ticker` |
| `--table`, `-t` | _(auto)_ | Table name (defaults to `events`, `events_rich`, or `events_rich_ml` depending on schema mode) |
| `--batch-size`, `-b` | `100000` | Rows per batch (batch mode only) |
| `--num-batches`, `-n` | `10` | Number of batches; `0` = run forever |
| `--flush-interval`, `-k` | `10` | Flush inlined rows to Parquet every N batches (batch mode) or at end if inlined rows > N (ticker mode); `0` = never |
| `--duration`, `-d` | `60` | Duration in seconds (ticker mode only) |
| `--duplicate-rate` | `0.0` | Probability (0.0–1.0) of injecting a duplicate row on each tick (ticker mode only); e.g. `0.15` = ~15% duplicates |
| `--schema-drift-rate` | `0.0` | Probability (0.0–1.0) of injecting a schema-breaking row on each tick (ticker mode only); the insert targets an unknown column (`event_category` for simple, `schema_version` for rich) that doesn't exist in the table, causing DuckDB to reject it — the rejected event is dead-lettered to `wh.events_rejected` |

## Schema Modes

**simple** — `wh.events`
```sql
id INTEGER, ts TIMESTAMP, event_type VARCHAR
```

**rich** — `wh.events_rich`
```sql
id INTEGER, user_id VARCHAR, event_type VARCHAR, ts TIMESTAMP, payload JSON, metadata JSON
```

**rich_ml** — `wh.events_rich_ml` (ML-optimized with enriched features and user profiles)
```sql
id INTEGER,
user_id VARCHAR,
event_type VARCHAR,
ts TIMESTAMP,
payload JSON,        -- includes: page, duration_ms, value, device_type, referrer, session_number, previous_purchase_count
metadata JSON,       -- includes: source, country, session_id, ab_variant, user_tier, days_since_signup
user_attributes JSON -- includes: tier, signup_days_ago, country, mrr_value
```

**rich_ml** is designed for machine learning use cases with:
- **20+ event types** (view, click, add_to_cart, checkout_start, purchase, etc.)
- **Configurable user pool** (default 100, configurable via `--num-users`)
- **User profiles** with tier (free/starter/pro/enterprise), account age, MRR value, and cohort patterns
- **Enriched payload** with device types (web, mobile_ios, mobile_android, tablet), referrers, session tracking
- **Realistic distributions** that simulate power-law user behavior and time-based patterns

**Optional ML label** — pass `--ml-labels` to add a single binary classification target as an eighth column:

```sql
is_converted BOOLEAN
```

It is a real typed column, not a JSON key, so it needs no extraction or cast:

```sql
SELECT COUNT(*) FROM wh.events_rich_ml WHERE is_converted;
```

The column is opt-in. Without the flag it is absent from the `CREATE TABLE` and from every insert, so `events_rich_ml` keeps its seven-column shape. `--conversion-rate` controls the positive-class rate (default `0.15`, i.e. ~15% `true`), which is a realistic level of class imbalance for conversion modelling.

### Schema evolution

Because `is_converted` is part of the table definition, a table created without `--ml-labels` will not gain the column on a later run. Flipping the flag on against an existing table fails cleanly, with nothing written:

```
Error: batch 1 insert: Binder Error: table events_rich_ml has 7 columns but 8 values were supplied
```

Either reset the table (or use a different `--table`), or evolve it in place — DuckLake records this as its own snapshot with a `schema_version` bump, and does not rewrite existing Parquet files:

```sql
ALTER TABLE wh.events_rich_ml ADD COLUMN is_converted BOOLEAN;
```

Rows written before the `ALTER` read back as `NULL` for the new column; rows written after carry the label.

**Example rich_ml queries:**
```sql
-- User segments
SELECT user_attributes->>'tier' as tier, COUNT(*) as events FROM wh.events_rich_ml GROUP BY 1;

-- Conversion events
SELECT COUNT(*) FROM wh.events_rich_ml WHERE event_type = 'purchase';

-- Device breakdown
SELECT payload->>'device_type' as device, COUNT(*) as count FROM wh.events_rich_ml GROUP BY 1;

-- User lifetime value correlation
SELECT 
  user_attributes->>'tier' as tier,
  COUNT(*) as events,
  AVG((payload->>'value')::NUMERIC) as avg_value
FROM wh.events_rich_ml
GROUP BY 1;
```

**rejected (dead letter)** — `wh.events_rejected`

Schema-drift inserts that fail against the main table are recorded here so monitoring agents can detect pipeline issues:

```sql
rejected_at TIMESTAMP, source_table VARCHAR, anomaly_type VARCHAR,
attempted_id INTEGER, error_message VARCHAR, payload JSON
```

Example queries:

```sql
SELECT COUNT(*) FROM wh.events_rejected WHERE anomaly_type = 'schema_drift';
SELECT * FROM wh.events_rejected ORDER BY rejected_at DESC LIMIT 10;
```

## Run Modes

**batch** — inserts `--batch-size` rows per iteration using a single `INSERT INTO ... SELECT range(N)` SQL statement. Fast bulk ingest. Calls `ducklake_flush_inlined_data()` every `--flush-interval` batches to materialize inlined rows into Parquet files.

**ticker** — inserts one row per second via `time.NewTicker`. Simulates a low-volume real-time stream. At the end, if the number of new inlined rows exceeds `--flush-interval`, calls `ducklake_flush_inlined_data()` to write Parquet files.

## DuckLake Inlining Behavior

DuckLake stores small row counts **inline in Postgres** before flushing to Parquet. This means:

- **During a run**: rows are in Postgres inline storage — queries work from any directory
- **After `ducklake_flush_inlined_data()`**: rows move to Parquet files at the `DATA_PATH`

### Critical: DATA_PATH must be consistent

The `DATA_PATH` passed to `ATTACH` is stored verbatim in Postgres metadata. If you used a relative path (e.g. `./ducklake_data`), all clients querying the catalog **must run from the same working directory** that `parkbench` used when writing data.

**Best practice**: use an absolute path to avoid this:

```bash
./parkbench setup --data-path "/Users/yourname/data/ducklake_data"
./parkbench run   --data-path "/Users/yourname/data/ducklake_data"
```

## Querying from DuckDB CLI

Must attach from the same working directory used during `parkbench run`:

```bash
cd /path/where/parkbench/ran
duckdb
```

```sql
INSTALL ducklake;
LOAD ducklake;
ATTACH 'ducklake:postgres:dbname=ducklake_v1 host=localhost' AS wh
    (DATA_PATH './ducklake_data', AUTOMATIC_MIGRATION TRUE);

SELECT COUNT(*) FROM wh.events;
SELECT * FROM wh.events LIMIT 10;
SELECT COUNT(*) FROM wh.events_rejected WHERE anomaly_type = 'schema_drift';
SELECT * FROM ducklake_settings('wh');
```

If parkbench was run with `--ducklake-secret <name>`, there's no working-directory dependency — the secret bundles `DATA_PATH` and the metadata connection, so you can attach from anywhere:

```sql
ATTACH 'ducklake:ducklake_prod' AS wh (AUTOMATIC_MIGRATION TRUE);
SELECT COUNT(*) FROM wh.events;
```

### If you get a DATA_PATH mismatch error

If the catalog was set up with an absolute path but you're trying to attach with a relative path (or vice versa), you'll see:

```
DATA_PATH parameter "./ducklake_data/" does not match existing data path in the catalog "/absolute/path/ducklake_data/".
You can override the DATA_PATH by setting OVERRIDE_DATA_PATH to True.
```

You have two options:

**Option 1: Use the absolute path that matches the catalog metadata**

```sql
ATTACH 'ducklake:postgres:dbname=ducklake_v1 host=localhost' AS wh
    (DATA_PATH '/absolute/path/to/ducklake_data', AUTOMATIC_MIGRATION TRUE);
```

**Option 2: Override the stored DATA_PATH (quick fix)**

```sql
ATTACH 'ducklake:postgres:dbname=ducklake_v1 host=localhost' AS wh
    (DATA_PATH './ducklake_data', AUTOMATIC_MIGRATION TRUE, OVERRIDE_DATA_PATH TRUE);
```

Use Option 1 if you're querying from the location where the data was written. Use Option 2 if you want to query from a different directory or with a different path.

## Resetting / Starting Over

Use the built-in `reset` command. Behavior depends on `ConnectionConfig.isRemote()` (see `runReset` in `connection.go`):

```bash
./parkbench reset              # prompts "yes" to confirm (local postgres)
./parkbench reset --force      # skips confirmation
./parkbench reset --force --data-path "/absolute/path/ducklake_data"
./parkbench reset --ducklake-secret ducklake_prod --force   # remote-safe: drops tables only
```

**Flags:** same connection flags as `setup`/`run` (see above), plus:

| Flag | Default | Description |
|------|---------|-------------|
| `--force`, `-f` | `false` | Skip the confirmation prompt |

**Local reset** (`runLocalReset`, local Postgres + local data path, or `--metadata-store duckdb`) — original destructive behavior, unchanged:
1. Drops the Postgres database via `psql` (or deletes the `.ducklake` file for `--metadata-store duckdb`)
2. Recreates the Postgres database via `psql`
3. Removes the Parquet data directory (`os.RemoveAll`)
4. Removes `.flush_state.json` (silently skips if missing)
5. Re-runs `setup` to create fresh tables

**Remote reset** (`runRemoteReset`, remote/managed Postgres, S3 data path, or `--ducklake-secret`) — parkbench doesn't own that shared infrastructure outright, so it does NOT drop a database or attempt to wipe an S3 bucket:
1. Drops just the DuckLake-tracked tables (`events`, `events_rich`, `events_rejected`) inside the catalog via `DROP TABLE IF EXISTS`
2. Removes `.flush_state.json` (silently skips if missing)
3. If the data path is S3, prints a note that existing Parquet files are NOT deleted automatically (suggests an S3 lifecycle rule or manual `aws s3 rm --recursive` for a full wipe)
4. Re-runs `setup` to recreate the tables
