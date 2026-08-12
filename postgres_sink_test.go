package main

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/lib/pq"
	"github.com/spf13/cobra"
)

// TestPostgresBatchSQL executes the generated postgres-sink SQL against a real
// database, since the DuckDB generators' syntax is not portable and only a live
// server can confirm the difference. Set PARKBENCH_TEST_PG_DSN to run.
func TestPostgresBatchSQL(t *testing.T) {
	dsn := os.Getenv("PARKBENCH_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("set PARKBENCH_TEST_PG_DSN to run")
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Drop first so an interrupted earlier run can't fail this one.
	if _, err := db.Exec("DROP SCHEMA IF EXISTS parkbench_test CASCADE"); err != nil {
		t.Fatalf("drop stale schema: %v", err)
	}
	if _, err := db.Exec("CREATE SCHEMA parkbench_test"); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		db.Exec("DROP SCHEMA IF EXISTS parkbench_test CASCADE")
	})

	simple := "parkbench_test.events_source"
	rich := "parkbench_test.events_rich_source"

	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS ` + simple + ` (
		id INTEGER, ts TIMESTAMP, event_type VARCHAR)`); err != nil {
		t.Fatalf("create simple: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS ` + rich + ` (
		id INTEGER, user_id VARCHAR, event_type VARCHAR, ts TIMESTAMP,
		payload JSON, metadata JSON)`); err != nil {
		t.Fatalf("create rich: %v", err)
	}

	distributions := []string{"uniform", "hotspot", "yesterday", "last_week"}

	for _, dist := range distributions {
		for _, tc := range []struct {
			mode  string
			table string
			gen   func(string, int, int, string, int) string
		}{
			{"simple", simple, batchSQLSimplePostgres},
			{"rich", rich, batchSQLRichPostgres},
		} {
			t.Run(dist+"/"+tc.mode, func(t *testing.T) {
				const startID, batchSize = 1, 10
				if _, err := db.Exec("TRUNCATE " + tc.table); err != nil {
					t.Fatalf("truncate: %v", err)
				}

				query := tc.gen(tc.table, startID, batchSize, dist, 30)
				if _, err := db.Exec(query); err != nil {
					t.Fatalf("insert failed: %v\nSQL:\n%s", err, query)
				}

				var count, minID, maxID int
				err := db.QueryRow("SELECT count(*), min(id), max(id) FROM " + tc.table).
					Scan(&count, &minID, &maxID)
				if err != nil {
					t.Fatalf("verify: %v", err)
				}
				if count != batchSize {
					t.Errorf("row count = %d, want %d", count, batchSize)
				}
				if minID != startID || maxID != startID+batchSize-1 {
					t.Errorf("id range = [%d,%d], want [%d,%d]",
						minID, maxID, startID, startID+batchSize-1)
				}

				var outOfRange int
				if err := db.QueryRow("SELECT count(*) FROM " + tc.table +
					" WHERE event_type IS NULL").Scan(&outOfRange); err != nil {
					t.Fatalf("null check: %v", err)
				}
				if outOfRange != 0 {
					t.Errorf("%d rows have NULL event_type (bad array index)", outOfRange)
				}

				assertTimestampWindow(t, db, tc.table, dist)
			})
		}
	}
}

// TestPostgresTickerSQL covers the single-row generators, whose DuckDB
// counterparts use struct literals Postgres cannot parse.
func TestPostgresTickerSQL(t *testing.T) {
	dsn := os.Getenv("PARKBENCH_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("set PARKBENCH_TEST_PG_DSN to run")
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Drop first so an interrupted earlier run can't fail this one.
	if _, err := db.Exec("DROP SCHEMA IF EXISTS parkbench_tick_test CASCADE"); err != nil {
		t.Fatalf("drop stale schema: %v", err)
	}
	if _, err := db.Exec("CREATE SCHEMA parkbench_tick_test"); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		db.Exec("DROP SCHEMA IF EXISTS parkbench_tick_test CASCADE")
	})

	simple := "parkbench_tick_test.events_source"
	rich := "parkbench_tick_test.events_rich_source"

	if _, err := db.Exec(`CREATE TABLE ` + simple + ` (
		id INTEGER, ts TIMESTAMP, event_type VARCHAR)`); err != nil {
		t.Fatalf("create simple: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE ` + rich + ` (
		id INTEGER, user_id VARCHAR, event_type VARCHAR, ts TIMESTAMP,
		payload JSON, metadata JSON)`); err != nil {
		t.Fatalf("create rich: %v", err)
	}

	t.Run("simple", func(t *testing.T) {
		query := tickerSQLSimplePostgres(simple, 7)
		if _, err := db.Exec(query); err != nil {
			t.Fatalf("insert failed: %v\nSQL:\n%s", err, query)
		}
	})

	t.Run("rich", func(t *testing.T) {
		query := tickerSQLRichPostgres(rich, 7, "user_3")
		if _, err := db.Exec(query); err != nil {
			t.Fatalf("insert failed: %v\nSQL:\n%s", err, query)
		}
		var page string
		if err := db.QueryRow(
			"SELECT payload->>'page' FROM " + rich + " WHERE id = 7").Scan(&page); err != nil {
			t.Fatalf("read payload: %v", err)
		}
		if page == "" {
			t.Error("payload->>'page' is empty; JSON was not built correctly")
		}
	})

	// Drift inserts must fail: that failure is the simulated anomaly.
	for _, tc := range []struct{ mode, table string }{
		{"simple", simple},
		{"rich", rich},
	} {
		t.Run("drift/"+tc.mode, func(t *testing.T) {
			if _, err := db.Exec(tickerSQLDriftedPostgres(tc.table, 99, tc.mode)); err == nil {
				t.Error("drifted insert succeeded; expected an unknown-column error")
			}
		})
	}
}

func TestResolvePgDSN(t *testing.T) {
	newCmd := func(cfg *ConnectionConfig) *cobra.Command {
		cmd := &cobra.Command{Use: "test", RunE: func(*cobra.Command, []string) error { return nil }}
		registerConnectionFlags(cmd, cfg)
		return cmd
	}

	writeFile := func(t *testing.T, contents string, mode os.FileMode) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), "dsn")
		if err := os.WriteFile(path, []byte(contents), mode); err != nil {
			t.Fatalf("write: %v", err)
		}
		return path
	}

	t.Run("reads single line", func(t *testing.T) {
		path := writeFile(t, "dbname=mydb host=localhost\n", 0o600)
		var cfg ConnectionConfig
		cmd := newCmd(&cfg)
		if err := cmd.ParseFlags([]string{"--pg-dsn-file", path}); err != nil {
			t.Fatalf("parse: %v", err)
		}
		if err := resolvePgDSN(cmd, &cfg); err != nil {
			t.Fatalf("resolve: %v", err)
		}
		if want := "dbname=mydb host=localhost"; cfg.PgDSN != want {
			t.Errorf("PgDSN = %q, want %q", cfg.PgDSN, want)
		}
	})

	t.Run("joins lines and skips comments", func(t *testing.T) {
		path := writeFile(t, "# prod\nhost=db.example.com\n\nport=5432\ndbname=postgres\n", 0o600)
		var cfg ConnectionConfig
		cmd := newCmd(&cfg)
		if err := cmd.ParseFlags([]string{"--pg-dsn-file", path}); err != nil {
			t.Fatalf("parse: %v", err)
		}
		if err := resolvePgDSN(cmd, &cfg); err != nil {
			t.Fatalf("resolve: %v", err)
		}
		if want := "host=db.example.com port=5432 dbname=postgres"; cfg.PgDSN != want {
			t.Errorf("PgDSN = %q, want %q", cfg.PgDSN, want)
		}
	})

	t.Run("rejects combining with --pg-dsn", func(t *testing.T) {
		path := writeFile(t, "dbname=mydb\n", 0o600)
		var cfg ConnectionConfig
		cmd := newCmd(&cfg)
		if err := cmd.ParseFlags([]string{"--pg-dsn-file", path, "--pg-dsn", "dbname=other"}); err != nil {
			t.Fatalf("parse: %v", err)
		}
		if err := resolvePgDSN(cmd, &cfg); err == nil {
			t.Error("expected an error when both flags are set")
		}
	})

	t.Run("errors on empty file", func(t *testing.T) {
		path := writeFile(t, "# only a comment\n\n", 0o600)
		var cfg ConnectionConfig
		cmd := newCmd(&cfg)
		if err := cmd.ParseFlags([]string{"--pg-dsn-file", path}); err != nil {
			t.Fatalf("parse: %v", err)
		}
		if err := resolvePgDSN(cmd, &cfg); err == nil {
			t.Error("expected an error for a file with no connection string")
		}
	})

	t.Run("errors on missing file", func(t *testing.T) {
		var cfg ConnectionConfig
		cmd := newCmd(&cfg)
		if err := cmd.ParseFlags([]string{"--pg-dsn-file", "/nonexistent/dsn"}); err != nil {
			t.Fatalf("parse: %v", err)
		}
		if err := resolvePgDSN(cmd, &cfg); err == nil {
			t.Error("expected an error for a missing file")
		}
	})

	t.Run("leaves default untouched when unset", func(t *testing.T) {
		var cfg ConnectionConfig
		cmd := newCmd(&cfg)
		if err := cmd.ParseFlags(nil); err != nil {
			t.Fatalf("parse: %v", err)
		}
		if err := resolvePgDSN(cmd, &cfg); err != nil {
			t.Fatalf("resolve: %v", err)
		}
		if want := "dbname=ducklake_v1 host=localhost"; cfg.PgDSN != want {
			t.Errorf("PgDSN = %q, want default %q", cfg.PgDSN, want)
		}
	})
}

func TestRedactDSN(t *testing.T) {
	cases := []struct{ name, in, want string }{
		{
			"keyword form",
			"host=db.example.com dbname=postgres user=postgres password=s3cret sslmode=require",
			"host=db.example.com dbname=postgres user=postgres password=*** sslmode=require",
		},
		{
			"url form",
			"postgresql://postgres:s3cret@db.example.com:5432/postgres",
			"postgresql://postgres:***@db.example.com:5432/postgres",
		},
		{
			"url without password",
			"postgresql://postgres@db.example.com:5432/postgres",
			"postgresql://postgres@db.example.com:5432/postgres",
		},
		{
			"no password at all",
			"dbname=ducklake_v1 host=localhost",
			"dbname=ducklake_v1 host=localhost",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := redactDSN(tc.in); got != tc.want {
				t.Errorf("redactDSN(%q)\n got %q\nwant %q", tc.in, got, tc.want)
			}
			if strings.Contains(redactDSN(tc.in), "s3cret") {
				t.Error("password leaked through redaction")
			}
		})
	}
}

func assertTimestampWindow(t *testing.T, db *sql.DB, table, dist string) {
	t.Helper()

	var predicate string
	switch dist {
	case "yesterday":
		predicate = "ts >= (CURRENT_DATE - 1)::timestamp AND ts < CURRENT_DATE::timestamp"
	case "last_week":
		predicate = "ts > now() - INTERVAL '7 days' AND ts <= now()"
	case "hotspot":
		predicate = "ts > now() - INTERVAL '31 days' AND ts <= now()"
	default:
		predicate = "ts > now() - INTERVAL '25 hours' AND ts <= now()"
	}

	var bad int
	if err := db.QueryRow("SELECT count(*) FROM " + table + " WHERE NOT (" + predicate + ")").Scan(&bad); err != nil {
		t.Fatalf("timestamp window check: %v", err)
	}
	if bad != 0 {
		t.Errorf("%d rows fall outside the %q window", bad, dist)
	}
}
