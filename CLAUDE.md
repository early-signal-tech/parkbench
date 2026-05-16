# parkbench — DuckLake Streaming Benchmark Tool

A Go CLI that benchmarks data insertion performance into a DuckLake catalog backed by local PostgreSQL as the metadata store, with Parquet files for data storage.

## Architecture

- **Metadata store**: PostgreSQL (local, `ducklake_v1` database)
- **Data store**: Parquet files on disk (default: `./ducklake_data/`)
- **Query engine**: DuckDB with the `ducklake` extension (via `github.com/duckdb/duckdb-go/v2`)
- **Catalog alias**: `wh` (how the catalog is referenced inside DuckDB SQL)

The tool uses DuckDB in-process (not a DuckDB server). Each command opens an in-memory DuckDB instance, loads the `ducklake` extension, and `ATTACH`es to the Postgres-backed catalog.

### ATTACH string format

```sql
ATTACH 'ducklake:postgres:dbname=ducklake_v1 host=localhost' AS wh
    (DATA_PATH './ducklake_data', AUTOMATIC_MIGRATION TRUE)
```

## Prerequisites

PostgreSQL must be running and the catalog database must exist before any command is run:

```bash
brew services start postgresql@18
psql -d postgres -c "CREATE DATABASE ducklake_v1;"
```

> The Go tool does NOT start Postgres or create the database — that must be done manually as a one-time step.

## Build

```bash
go build -o parkbench .
```

## Commands

### `setup` — one-time catalog initialization

Creates the `events` and `events_rich` tables inside the DuckLake catalog.

```bash
./parkbench setup
./parkbench setup --pg-dsn "dbname=ducklake_v1 host=localhost" --data-path "./ducklake_data" --catalog wh
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--catalog`, `-c` | `wh` | Catalog alias used inside DuckDB SQL |
| `--pg-dsn` | `dbname=ducklake_v1 host=localhost` | Postgres DSN for the metadata store |
| `--data-path` | `./ducklake_data` | Directory where Parquet files are written |

### `run` — benchmark insertion

Inserts rows continuously and prints throughput stats.

```bash
# batch mode, simple schema (default)
./parkbench run

# batch mode, rich schema
./parkbench run --mode rich

# ticker mode (1 row/sec for 60s)
./parkbench run --run-mode ticker --duration 60

# ticker mode with 15% duplicate injection
./parkbench run --run-mode ticker --duration 60 --duplicate-rate 0.15

# ticker mode with 20% schema drift (inserts with unknown column are rejected)
./parkbench run --run-mode ticker --duration 60 --schema-drift-rate 0.20

# ticker mode with both duplicate and schema-drift anomalies
./parkbench run --run-mode ticker --duration 60 --duplicate-rate 0.10 --schema-drift-rate 0.15

# run forever
./parkbench run --num-batches 0
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--catalog`, `-c` | `wh` | Catalog alias |
| `--pg-dsn` | `dbname=ducklake_v1 host=localhost` | Postgres DSN |
| `--data-path` | `./ducklake_data` | Parquet data directory |
| `--mode`, `-m` | `simple` | Schema mode: `simple` or `rich` |
| `--run-mode`, `-r` | `batch` | Run mode: `batch` or `ticker` |
| `--table`, `-t` | _(auto)_ | Table name (defaults to `events` or `events_rich`) |
| `--batch-size`, `-b` | `100000` | Rows per batch (batch mode only) |
| `--num-batches`, `-n` | `10` | Number of batches; `0` = run forever |
| `--flush-interval`, `-k` | `10` | Flush inlined rows to Parquet every N batches (batch mode) or at end if inlined rows > N (ticker mode); `0` = never |
| `--duration`, `-d` | `60` | Duration in seconds (ticker mode only) |
| `--duplicate-rate` | `0.0` | Probability (0.0–1.0) of injecting a duplicate row on each tick (ticker mode only); e.g. `0.15` = ~15% duplicates |
| `--schema-drift-rate` | `0.0` | Probability (0.0–1.0) of injecting a schema-breaking row on each tick (ticker mode only); the insert targets an unknown column (`event_category` for simple, `schema_version` for rich) that doesn't exist in the table, causing DuckDB to reject it — simulating a source schema change that breaks downstream data capture |

## Schema Modes

**simple** — `wh.events`
```sql
id INTEGER, ts TIMESTAMP, event_type VARCHAR
```

**rich** — `wh.events_rich`
```sql
id INTEGER, user_id VARCHAR, event_type VARCHAR, ts TIMESTAMP, payload JSON, metadata JSON
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
SELECT * FROM ducklake_settings('wh');
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

Use the built-in `reset` command — it drops the Postgres database, removes the data directory, deletes `.flush_state.json`, and re-runs `setup` automatically:

```bash
./parkbench reset              # prompts "yes" to confirm
./parkbench reset --force      # skips confirmation
./parkbench reset --force --data-path "/absolute/path/ducklake_data"
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--catalog`, `-c` | `wh` | Catalog alias used inside DuckDB |
| `--pg-dsn` | `dbname=ducklake_v1 host=localhost` | Postgres DSN for the metadata store |
| `--data-path` | `./ducklake_data` | Parquet data directory to remove |
| `--force`, `-f` | `false` | Skip the confirmation prompt |

**What reset does (in order):**
1. Drops the Postgres database via `psql`
2. Recreates the Postgres database via `psql`
3. Removes the Parquet data directory (`os.RemoveAll`)
4. Removes `.flush_state.json` (silently skips if missing)
5. Re-runs `setup` to create fresh tables
