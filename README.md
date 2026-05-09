# Parkbench — DuckLake Streaming Benchmark Tool

A high-performance CLI tool for benchmarking DuckLake catalog inserts with flexible path handling and multiple run modes.

## Quick Start

### Setup a new catalog

```bash
# Create catalog in current directory (default: my_ducklake.ducklake)
./parkbench setup

# Create catalog in specific directory with custom name
./parkbench setup /data --catalog prod
```

### Run benchmarks

```bash
# Simple schema, batch mode, 10 batches of 100k rows each
./parkbench run

# Rich schema, ticker mode, 60 seconds of 1 row/sec inserts
./parkbench run --run-mode ticker --mode rich --duration 60

# Custom catalog, specific path
./parkbench run /data --catalog prod --run-mode batch --num-batches 5
```

## Commands

### `setup [path]` - Initialize a DuckLake catalog

Creates a new DuckLake catalog with both simple and rich schema tables.

**Options:**
- `-c, --catalog` - Catalog name (default: `my_ducklake`)

**Examples:**
```bash
./parkbench setup                          # ./my_ducklake.ducklake
./parkbench setup --catalog test           # ./test.ducklake
./parkbench setup /mnt/data --catalog prod # /mnt/data/prod.ducklake
```

### `run [path]` - Run benchmarks

Execute insert benchmarks against an existing DuckLake catalog.

**Schema modes:**
- `simple` (default) - {id, ts, event_type}
- `rich` - {id, user_id, event_type, ts, payload JSON, metadata JSON}

**Run modes:**
- `batch` (default) - Insert large batches of rows
- `ticker` - Insert one row per second for a specified duration

**Key options:**
- `-c, --catalog` - Catalog name (default: `my_ducklake`)
- `-m, --mode` - Schema mode: simple or rich
- `-r, --run-mode` - Run mode: batch or ticker
- `-d, --duration` - Duration in seconds (ticker mode only, default: 60)
- `-b, --batch-size` - Rows per batch (batch mode only, default: 100,000)
- `-n, --num-batches` - Number of batches (batch mode only, default: 10, 0 = forever)
- `-k, --checkpoint-interval` - CHECKPOINT frequency (batch mode only, default: 2, 0 = never)

**Examples:**
```bash
# Ticker mode, current directory
./parkbench run --run-mode ticker --duration 30

# Batch mode, custom path and catalog
./parkbench run /data --catalog prod --num-batches 3

# Rich schema, ticker mode for 2 minutes
./parkbench run --mode rich --run-mode ticker --duration 120
```

## Path Handling

The tool provides flexible path resolution:

1. **Default (current directory):**
   ```bash
   ./parkbench setup              # Creates ./my_ducklake.ducklake
   ./parkbench run                # Uses ./my_ducklake.ducklake
   ```

2. **Custom directory:**
   ```bash
   ./parkbench setup /mnt/data    # Creates /mnt/data/my_ducklake.ducklake
   ./parkbench run /mnt/data      # Uses /mnt/data/my_ducklake.ducklake
   ```

3. **Custom catalog name:**
   ```bash
   ./parkbench setup --catalog test           # Creates ./test.ducklake
   ./parkbench run --catalog test             # Uses ./test.ducklake
   ```

4. **Both:**
   ```bash
   ./parkbench setup /mnt/data --catalog prod # Creates /mnt/data/prod.ducklake
   ./parkbench run /mnt/data --catalog prod   # Uses /mnt/data/prod.ducklake
   ```

## Output

Both commands display:
- Configuration banner
- Per-operation stats (batch or tick)
- Row count, inlining ratio, file storage metrics
- Final summary with overall throughput

### Batch Mode Example
```
╭─ Batch 1    ──────────────────────────────────────────╮
│ Batch                          10 / 10 │
│ Batch rows inserted         100,000 │
│ Batch throughput         1,234,567 rows/sec │
│ Overall throughput       1,234,567 rows/sec │
│ Total rows (COUNT)         100,000 │
│ Inlined rows                100.0% │
│ File-stored rows                0 │
│ Parquet file count              0 │
╰────────────────────────────────────────────╯
```

### Ticker Mode Example
```
╭─ Tick 1     ────────────────────────────────────────╮
│ Tick number                     1 │
│ Total rows (COUNT)              1 │
│ Inlined rows           1  (100.0%) │
│ File-stored rows           0  (0%) │
│ Parquet file count              0 │
│ Overall throughput         0 rows/sec │
╰────────────────────────────────────────╯
```

## Building

```bash
go build -o parkbench main.go
```

## Requirements

- Go 1.24.0+
- DuckDB (via duckdb-go driver)
- DuckLake support in DuckDB

## Development

The tool uses:
- `github.com/spf13/cobra` for CLI framework
- `github.com/duckdb/duckdb-go/v2` for database access

Data is inserted using randomized SQL generation from predefined vocabularies:
- Event types: click, view, purchase, signup, logout, search, share
- Pages: /home, /product, /cart, /checkout, /profile, /search, /settings
- Sources: web, mobile, api, email, push
- Countries: US, GB, DE, FR, JP, CA, AU, BR
