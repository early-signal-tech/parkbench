// Connection resolution for parkbench.
//
// A DuckLake catalog can be reached three ways:
//
//  1. Named persistent secret (DucklakeSecret set) — everything else on
//     ConnectionConfig is ignored. ATTACH uses 'ducklake:<secret>' directly.
//     The secret (created via `parkbench secrets create-ducklake` or the
//     DuckDB CLI, per the DuckLake docs) already bundles the metadata
//     connection, DATA_PATH, and metadata schema.
//  2. Inline local DuckDB catalog (MetadataStore == "duckdb") — a local
//     .ducklake file plus a local data directory. Zero-config local dev.
//  3. Inline Postgres catalog (MetadataStore == "postgres") — any Postgres,
//     local or remote (e.g. Supabase), writing data locally or to S3.
package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
)

type ConnectionConfig struct {
	CatalogName string

	// Named persistent secret mode.
	DucklakeSecret string

	// METADATA_CATALOG: exposes DuckLake's metadata tables under this name in
	// the current DuckDB session (e.g. so SHOW ALL TABLES surfaces them).
	// Applies in every mode; omit to keep them hidden (DuckLake's default).
	MetadataCatalogName string

	// Inline mode.
	MetadataStore string // "postgres" | "duckdb"
	PgDSN         string
	// METADATA_SCHEMA: the Postgres schema DuckLake stores its metadata
	// tables in. Defaults to Postgres "public" if left empty. Set this to
	// scope a shared/managed Postgres (like Supabase) to a dedicated schema.
	MetadataSchema string
	DataPath       string // local directory, or an s3://bucket/prefix URI

	// S3 credentials, used only when DataPath is s3:// and DucklakeSecret is
	// unset. Fall back to AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_REGION.
	S3KeyID     string
	S3SecretKey string
	S3Region    string
}

func (c ConnectionConfig) isS3() bool {
	return strings.HasPrefix(c.DataPath, "s3://")
}

// isRemote reports whether this connection points at infrastructure
// parkbench doesn't own outright: a named secret it can't introspect, a
// non-local Postgres host, or S3 storage. Remote catalogs get a gentler
// `reset` — parkbench won't try to drop a shared database or wipe a bucket.
func (c ConnectionConfig) isRemote() bool {
	if c.DucklakeSecret != "" {
		return true
	}
	if c.isS3() {
		return true
	}
	return c.MetadataStore == "postgres" && !isLocalHostDSN(c.PgDSN)
}

func isLocalHostDSN(dsn string) bool {
	for _, part := range strings.Fields(dsn) {
		if strings.HasPrefix(part, "host=") {
			host := strings.TrimPrefix(part, "host=")
			return host == "localhost" || host == "127.0.0.1" || host == ""
		}
	}
	// No host= means libpq's default, which is a local Unix socket.
	return true
}

func extractDBName(dsn string) string {
	dbName := "ducklake_v1"
	for _, part := range strings.Fields(dsn) {
		if strings.HasPrefix(part, "dbname=") {
			dbName = strings.TrimPrefix(part, "dbname=")
		}
	}
	return dbName
}

func attachOption(key, value string) string {
	if value == "" {
		return ""
	}
	return fmt.Sprintf("%s %s", key, sqlStringLiteral(value))
}

func joinAttachOptions(opts ...string) string {
	var nonEmpty []string
	for _, o := range opts {
		if o != "" {
			nonEmpty = append(nonEmpty, o)
		}
	}
	return strings.Join(nonEmpty, ", ")
}

// buildAttachSQL returns the ATTACH statement for this connection. Callers
// must run any prerequisite statements (extension loads, ephemeral secrets)
// via prepareConnection first.
func (c ConnectionConfig) buildAttachSQL() string {
	if c.DucklakeSecret != "" {
		return fmt.Sprintf("ATTACH 'ducklake:%s' AS %s (%s)", c.DucklakeSecret, c.CatalogName,
			joinAttachOptions(
				"AUTOMATIC_MIGRATION TRUE",
				attachOption("METADATA_CATALOG", c.MetadataCatalogName),
			))
	}

	dataPath := c.DataPath
	if dataPath == "" {
		dataPath = "."
	}

	if c.MetadataStore == "duckdb" {
		catalogFile := filepath.Join(dataPath, c.CatalogName+".ducklake")
		return fmt.Sprintf("ATTACH 'ducklake:duckdb:%s' AS %s (%s)", catalogFile, c.CatalogName,
			joinAttachOptions(
				attachOption("DATA_PATH", dataPath),
				"AUTOMATIC_MIGRATION TRUE",
			))
	}

	return fmt.Sprintf("ATTACH 'ducklake:postgres:%s' AS %s (%s)", c.PgDSN, c.CatalogName,
		joinAttachOptions(
			attachOption("DATA_PATH", dataPath),
			"AUTOMATIC_MIGRATION TRUE",
			attachOption("METADATA_SCHEMA", c.MetadataSchema),
			attachOption("METADATA_CATALOG", c.MetadataCatalogName),
		))
}

// prepareConnection runs any statements needed before ATTACH: loading httpfs
// and creating an in-memory (non-persistent) S3 secret when the data path is
// s3:// and no named DuckLake secret is already supplying credentials.
func (c ConnectionConfig) prepareConnection(db *sql.DB) error {
	if c.DucklakeSecret != "" {
		return nil // credentials already live inside the named secret.
	}
	if !c.isS3() {
		return nil
	}
	if _, err := db.Exec("INSTALL httpfs; LOAD httpfs;"); err != nil {
		return fmt.Errorf("load httpfs extension: %w", err)
	}

	keyID := firstNonEmpty(c.S3KeyID, os.Getenv("AWS_ACCESS_KEY_ID"))
	secretKey := firstNonEmpty(c.S3SecretKey, os.Getenv("AWS_SECRET_ACCESS_KEY"))
	region := firstNonEmpty(c.S3Region, os.Getenv("AWS_REGION"), os.Getenv("AWS_DEFAULT_REGION"))
	if keyID == "" || secretKey == "" {
		return fmt.Errorf("--data-path %q is an S3 URI but no S3 credentials were provided: "+
			"pass --s3-key-id/--s3-secret-key, set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY, "+
			"or use --ducklake-secret with a pre-created DUCKLAKE secret", c.DataPath)
	}
	if region == "" {
		return fmt.Errorf("--data-path %q is an S3 URI but no region was provided: "+
			"pass --s3-region or set AWS_REGION", c.DataPath)
	}

	query := fmt.Sprintf(
		"CREATE OR REPLACE SECRET parkbench_s3 (TYPE s3, PROVIDER config, KEY_ID %s, SECRET %s, REGION %s)",
		sqlStringLiteral(keyID), sqlStringLiteral(secretKey), sqlStringLiteral(region),
	)
	if _, err := db.Exec(query); err != nil {
		return fmt.Errorf("create ephemeral s3 secret: %w", err)
	}
	return nil
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// openAndAttach opens a fresh in-process DuckDB connection and attaches the
// configured DuckLake catalog, creating local directories first if needed.
func openAndAttach(c ConnectionConfig) (*sql.DB, error) {
	if c.DucklakeSecret == "" && !c.isS3() {
		dataPath := c.DataPath
		if dataPath == "" {
			dataPath = "."
		}
		if err := os.MkdirAll(dataPath, 0755); err != nil {
			return nil, fmt.Errorf("create data directory: %w", err)
		}
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}

	if err := c.prepareConnection(db); err != nil {
		db.Close()
		return nil, err
	}

	if _, err := db.Exec(c.buildAttachSQL()); err != nil {
		db.Close()
		return nil, fmt.Errorf("attach catalog: %w", err)
	}

	return db, nil
}

// registerConnectionFlags wires the shared connection flags onto a cobra
// command, binding them into cfg.
func registerConnectionFlags(cmd *cobra.Command, cfg *ConnectionConfig) {
	cmd.Flags().StringVarP(&cfg.CatalogName, "catalog", "c", "wh", "Catalog alias used inside DuckDB")
	cmd.Flags().StringVar(&cfg.DucklakeSecret, "ducklake-secret", "", "Name of a pre-created persistent DUCKLAKE secret (see 'parkbench secrets create-ducklake'); when set, all other connection flags below are ignored")
	cmd.Flags().StringVar(&cfg.MetadataCatalogName, "metadata-catalog-name", "", "Expose DuckLake's metadata tables under this name in DuckDB (METADATA_CATALOG); omit to keep them hidden")
	cmd.Flags().StringVar(&cfg.MetadataStore, "metadata-store", "postgres", "Metadata store backend: 'postgres' or 'duckdb' (ignored when --ducklake-secret is set)")
	cmd.Flags().StringVar(&cfg.PgDSN, "pg-dsn", "dbname=ducklake_v1 host=localhost", "Postgres DSN for the metadata store; works for local Postgres or a remote/managed Postgres such as Supabase, e.g. \"host=db.xxxx.supabase.co port=5432 dbname=postgres user=postgres password=... sslmode=require\" (postgres mode only)")
	cmd.Flags().StringVar(&cfg.MetadataSchema, "metadata-schema", "", "Postgres schema DuckLake stores its metadata tables in (METADATA_SCHEMA); defaults to Postgres 'public' if unset (postgres mode only)")
	cmd.Flags().StringVar(&cfg.DataPath, "data-path", "./ducklake_data", "Where Parquet data files are stored: a local directory, or an s3://bucket/prefix URI")
	cmd.Flags().StringVar(&cfg.S3KeyID, "s3-key-id", "", "AWS access key ID, used only when --data-path is an s3:// URI (falls back to AWS_ACCESS_KEY_ID)")
	cmd.Flags().StringVar(&cfg.S3SecretKey, "s3-secret-key", "", "AWS secret access key, used only when --data-path is an s3:// URI (falls back to AWS_SECRET_ACCESS_KEY)")
	cmd.Flags().StringVar(&cfg.S3Region, "s3-region", "", "AWS region for the S3 bucket, used only when --data-path is an s3:// URI (falls back to AWS_REGION)")
}

func printConnectionSummary(cfg ConnectionConfig) {
	if cfg.DucklakeSecret != "" {
		fmt.Printf("  Connection   : %svia DuckLake secret%s (%s%s%s)\n", green, reset, green, cfg.DucklakeSecret, reset)
		fmt.Printf("  Catalog name : %s%s%s\n", green, cfg.CatalogName, reset)
		return
	}

	if cfg.MetadataStore == "duckdb" {
		dataPath := cfg.DataPath
		if dataPath == "" {
			dataPath = "."
		}
		catalogFile := filepath.Join(dataPath, cfg.CatalogName+".ducklake")
		fmt.Printf("  Metadata store : %sDuckDB%s (%s%s%s)\n", green, reset, green, catalogFile, reset)
	} else {
		kind := "local"
		if !isLocalHostDSN(cfg.PgDSN) {
			kind = "remote"
		}
		fmt.Printf("  Metadata store : %sPostgreSQL%s (%s%s%s)\n", green, reset, dim, kind, reset)
		fmt.Printf("  Postgres DSN   : %s%s%s\n", green, cfg.PgDSN, reset)
		if cfg.MetadataSchema != "" {
			fmt.Printf("  Metadata schema: %s%s%s\n", green, cfg.MetadataSchema, reset)
		}
	}

	storageKind := "local filesystem"
	if cfg.isS3() {
		storageKind = "S3"
	}
	fmt.Printf("  Data path    : %s%s%s (%s%s%s)\n", green, cfg.DataPath, reset, dim, storageKind, reset)
	fmt.Printf("  Catalog name : %s%s%s\n", green, cfg.CatalogName, reset)
}

// ── reset ─────────────────────────────────────────────────────────────────

func runReset(cfg ConnectionConfig, force bool) error {
	remote := cfg.isRemote()

	if !force {
		fmt.Printf("%s%sWARNING:%s This will permanently delete:\n", bold, yellow, reset)
		switch {
		case remote:
			fmt.Printf("  • DuckLake tables   : %sevents, events_rich, %s%s (in catalog %s%s%s)\n",
				bold, rejectedEventsTable, reset, bold, cfg.CatalogName, reset)
			fmt.Printf("  • Local state file  : %s%s%s\n", bold, stateFileName, reset)
			if cfg.isS3() {
				fmt.Printf("  %s(Parquet files already written to %s%s%s are NOT deleted; see note after reset)%s\n",
					dim, bold, cfg.DataPath, reset, reset)
			}
		case cfg.MetadataStore == "duckdb":
			dataPath := cfg.DataPath
			if dataPath == "" {
				dataPath = "."
			}
			catalogFile := filepath.Join(dataPath, cfg.CatalogName+".ducklake")
			fmt.Printf("  • Catalog file      : %s%s%s\n", bold, catalogFile, reset)
			fmt.Printf("  • Data directory    : %s%s%s\n", bold, cfg.DataPath, reset)
		default:
			dbName := extractDBName(cfg.PgDSN)
			fmt.Printf("  • Postgres database : %s%s%s\n", bold, dbName, reset)
			fmt.Printf("  • Data directory    : %s%s%s\n", bold, cfg.DataPath, reset)
		}
		fmt.Printf("\nType %syes%s to continue: ", bold, reset)
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Scan()
		if strings.TrimSpace(scanner.Text()) != "yes" {
			fmt.Printf("%sReset cancelled.%s\n", yellow, reset)
			return nil
		}
	}

	rule("DuckLake Reset")

	if remote {
		return runRemoteReset(cfg)
	}
	return runLocalReset(cfg)
}

// runRemoteReset drops just the DuckLake-tracked tables rather than dropping
// a shared/managed database or trying to wipe an S3 bucket, since parkbench
// doesn't own that infrastructure outright.
func runRemoteReset(cfg ConnectionConfig) error {
	db, err := openAndAttach(cfg)
	if err != nil {
		return err
	}

	for _, table := range []string{"events", "events_rich", rejectedEventsTable} {
		fqt := cfg.CatalogName + "." + table
		fmt.Printf("  Dropping table     %s%s%s ...", bold, fqt, reset)
		if _, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", fqt)); err != nil {
			fmt.Println()
			db.Close()
			return fmt.Errorf("drop table %s: %w", fqt, err)
		}
		fmt.Printf(" %s✓%s\n", green, reset)
	}
	db.Close()

	fmt.Printf("  Removing state     %s%s%s ...", bold, stateFileName, reset)
	if err := os.Remove(stateFileName); err != nil && !os.IsNotExist(err) {
		fmt.Println()
		return fmt.Errorf("remove state file: %w", err)
	}
	fmt.Printf(" %s✓%s\n", green, reset)

	if cfg.isS3() {
		fmt.Printf("\n%sNote:%s Parquet files already written to %s%s%s are not deleted automatically.\n",
			yellow, reset, bold, cfg.DataPath, reset)
		fmt.Printf("%sConfigure an S3 lifecycle rule, or run `aws s3 rm --recursive %s` manually for a full wipe.%s\n",
			dim, cfg.DataPath, reset)
	}

	fmt.Println()
	return runSetup(cfg)
}

// runLocalReset preserves the original behavior: drop/recreate the local
// Postgres database (or delete the local .ducklake file) and wipe the local
// data directory.
func runLocalReset(cfg ConnectionConfig) error {
	if cfg.MetadataStore == "duckdb" {
		dataPath := cfg.DataPath
		if dataPath == "" {
			dataPath = "."
		}
		catalogFile := filepath.Join(dataPath, cfg.CatalogName+".ducklake")
		fmt.Printf("  Removing catalog   %s%s%s ...", bold, catalogFile, reset)
		if err := os.Remove(catalogFile); err != nil && !os.IsNotExist(err) {
			fmt.Println()
			return fmt.Errorf("remove catalog file: %w", err)
		}
		fmt.Printf(" %s✓%s\n", green, reset)
	} else {
		dbName := extractDBName(cfg.PgDSN)

		maintDSN := strings.ReplaceAll(cfg.PgDSN, "dbname="+dbName, "dbname=postgres")
		if !strings.Contains(maintDSN, "dbname=") {
			maintDSN = "dbname=postgres " + cfg.PgDSN
		}

		fmt.Printf("  Dropping database  %s%s%s ...", bold, dbName, reset)
		drop := exec.Command("psql", "-d", maintDSN, "-c", fmt.Sprintf("DROP DATABASE IF EXISTS %s;", dbName))
		drop.Env = os.Environ()
		if out, err := drop.CombinedOutput(); err != nil {
			fmt.Println()
			return fmt.Errorf("drop database: %w\n%s", err, out)
		}
		fmt.Printf(" %s✓%s\n", green, reset)

		fmt.Printf("  Creating database  %s%s%s ...", bold, dbName, reset)
		create := exec.Command("psql", "-d", maintDSN, "-c", fmt.Sprintf("CREATE DATABASE %s;", dbName))
		create.Env = os.Environ()
		if out, err := create.CombinedOutput(); err != nil {
			fmt.Println()
			return fmt.Errorf("create database: %w\n%s", err, out)
		}
		fmt.Printf(" %s✓%s\n", green, reset)
	}

	fmt.Printf("  Removing data dir  %s%s%s ...", bold, cfg.DataPath, reset)
	if err := os.RemoveAll(cfg.DataPath); err != nil {
		fmt.Println()
		return fmt.Errorf("remove data directory: %w", err)
	}
	fmt.Printf(" %s✓%s\n", green, reset)

	fmt.Printf("  Removing state     %s%s%s ...", bold, stateFileName, reset)
	if err := os.Remove(stateFileName); err != nil && !os.IsNotExist(err) {
		fmt.Println()
		return fmt.Errorf("remove state file: %w", err)
	}
	fmt.Printf(" %s✓%s\n", green, reset)

	fmt.Println()
	return runSetup(cfg)
}
