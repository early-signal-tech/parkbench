// Parkbench — DuckLake streaming benchmark tool.
//
// Connects to an existing local DuckLake catalog and continuously inserts
// large batches of events, reporting throughput and inlining ratio.
//
// Schema Modes:
//
//	simple — {id, ts, event_type}
//	rich   — {id, user_id, event_type, ts, payload JSON, metadata JSON}
//
// Run Modes:
//
//	batch  — Inserts large batches of rows (default)
//	ticker — Inserts one row per second for a specified duration
package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	_ "github.com/lib/pq"
	"github.com/spf13/cobra"
)

// ── vocabulary used in generated SQL ─────────────────────────────────────────

var (
	eventTypes = []string{"click", "view", "purchase", "signup", "logout", "search", "share"}
	pages      = []string{"/home", "/product", "/cart", "/checkout", "/profile", "/search", "/settings"}
	sources    = []string{"web", "mobile", "api", "email", "push"}
	countries  = []string{"US", "GB", "DE", "FR", "JP", "CA", "AU", "BR"}
	streamUsers = func() []string {
		users := make([]string, 20)
		for i := range users {
			users[i] = fmt.Sprintf("user_%d", i+1)
		}
		return users
	}()
)

// ── flush state management ────────────────────────────────────────────────

type flushState struct {
	LastFlushedCount int64  `json:"last_flushed_count"`
	LastFlushedAt    string `json:"last_flushed_at"`
}

type stateFile struct {
	Catalogs map[string]map[string]flushState `json:"catalogs"`
}

const stateFileName = ".flush_state.json"

func loadState(catalogName, tableName string) flushState {
	data, err := os.ReadFile(stateFileName)
	if err != nil {
		return flushState{LastFlushedCount: 0}
	}

	var state stateFile
	if err := json.Unmarshal(data, &state); err != nil {
		return flushState{LastFlushedCount: 0}
	}

	if catalog, ok := state.Catalogs[catalogName]; ok {
		if table, ok := catalog[tableName]; ok {
			return table
		}
	}

	return flushState{LastFlushedCount: 0}
}

func saveState(catalogName, tableName string, state flushState) error {
	data, err := os.ReadFile(stateFileName)
	var stateFile stateFile
	if err == nil {
		json.Unmarshal(data, &stateFile)
	}

	if stateFile.Catalogs == nil {
		stateFile.Catalogs = make(map[string]map[string]flushState)
	}
	if stateFile.Catalogs[catalogName] == nil {
		stateFile.Catalogs[catalogName] = make(map[string]flushState)
	}

	state.LastFlushedAt = time.Now().Format(time.RFC3339)
	stateFile.Catalogs[catalogName][tableName] = state

	jsonData, err := json.MarshalIndent(stateFile, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(stateFileName, jsonData, 0644)
}

// ── SQL generation ────────────────────────────────────────────────────────

func sqlArray(ss []string) string {
	quoted := make([]string, len(ss))
	for i, s := range ss {
		quoted[i] = "'" + s + "'"
	}
	return "[" + strings.Join(quoted, ", ") + "]"
}

// sqlArrayPostgres formats a string array as a Postgres ARRAY literal
func sqlArrayPostgres(ss []string) string {
	quoted := make([]string, len(ss))
	for i, s := range ss {
		// Escape single quotes in the string
		escaped := strings.ReplaceAll(s, "'", "''")
		quoted[i] = "'" + escaped + "'"
	}
	return "ARRAY[" + strings.Join(quoted, ", ") + "]"
}

// ── SQL generation ────────────────────────────────────────────────────────────

func batchSQLSimple(fqt string, startID, batchSize int, distribution string, hotspotDays int) string {
	var tsExpr string
	if distribution == "hotspot" {
		// 70% in last 6 hours, 30% spread across past N days
		oldestSeconds := hotspotDays * 24 * 3600
		tsExpr = fmt.Sprintf(`CASE
			WHEN random() < 0.7 THEN now() - INTERVAL (random() * 21600) SECOND
			ELSE now() - INTERVAL (21600 + random() * %d) SECOND
		END`, oldestSeconds-21600)
	} else if distribution == "yesterday" {
		// timestamps only for the previous day
		tsExpr = "CAST(CURRENT_DATE() - 1 AS TIMESTAMP) + INTERVAL (random() * 86400) SECOND"
	} else if distribution == "last_week" {
		// timestamps for the past 7 days
		tsExpr = "now() - INTERVAL (random() * 604800) SECOND"
	} else {
		// default: uniform across past 24 hours
		tsExpr = "now() - INTERVAL (random() * 86400) SECOND"
	}

	return fmt.Sprintf(`
		INSERT INTO %s
		SELECT
			range + %d                                                    AS id,
			%s                                                            AS ts,
			(%s)[1 + (random() * %d)::INT]                               AS event_type
		FROM range(%d)`,
		fqt, startID,
		tsExpr,
		sqlArray(eventTypes), len(eventTypes)-1,
		batchSize,
	)
}

func batchSQLRich(fqt string, startID, batchSize int, distribution string, hotspotDays int) string {
	var tsExpr string
	if distribution == "hotspot" {
		// 70% in last 6 hours, 30% spread across past N days
		oldestSeconds := hotspotDays * 24 * 3600
		tsExpr = fmt.Sprintf(`CASE
			WHEN random() < 0.7 THEN now() - INTERVAL (random() * 21600) SECOND
			ELSE now() - INTERVAL (21600 + random() * %d) SECOND
		END`, oldestSeconds-21600)
	} else if distribution == "yesterday" {
		// timestamps only for the previous day
		tsExpr = "CAST(CURRENT_DATE() - 1 AS TIMESTAMP) + INTERVAL (random() * 86400) SECOND"
	} else if distribution == "last_week" {
		// timestamps for the past 7 days
		tsExpr = "now() - INTERVAL (random() * 604800) SECOND"
	} else {
		// default: uniform across past 24 hours
		tsExpr = "now() - INTERVAL (random() * 86400) SECOND"
	}

	return fmt.Sprintf(`
		INSERT INTO %s
		SELECT
			range + %d                                                           AS id,
			(%s)[1 + (random() * %d)::INT]                                      AS user_id,
			(%s)[1 + (random() * %d)::INT]                                      AS event_type,
			%s                                                                   AS ts,
			{
				'page':        (%s)[1 + (random() * %d)::INT],
				'duration_ms': (100 + (random() * 9900)::INT),
				'value':       round(random() * 499.99 + 0.01, 2)
			}::JSON                                                           AS payload,
			{
				'source':      (%s)[1 + (random() * %d)::INT],
				'country':     (%s)[1 + (random() * %d)::INT],
				'session_id':  'sess_' || (1 + (random() * 999999)::BIGINT)::VARCHAR,
				'ab_variant':  CASE WHEN random() > 0.5 THEN 'A' ELSE 'B' END
			}::JSON                                                           AS metadata
		FROM range(%d)`,
		fqt, startID,
		sqlArray(streamUsers), len(streamUsers)-1,
		sqlArray(eventTypes), len(eventTypes)-1,
		tsExpr,
		sqlArray(pages), len(pages)-1,
		sqlArray(sources), len(sources)-1,
		sqlArray(countries), len(countries)-1,
		batchSize,
	)
}

// ── Postgres-specific batch SQL (uses standard SQL, no DuckDB extensions) ────

func batchSQLSimplePostgres(fqt string, startID, batchSize int, distribution string, hotspotDays int) string {
	var tsExpr string
	if distribution == "hotspot" {
		// 70% in last 6 hours, 30% spread across past N days
		oldestSeconds := hotspotDays * 24 * 3600
		tsExpr = fmt.Sprintf(`CASE
			WHEN random() < 0.7 THEN now() - (random() * 21600)::text || ' seconds'::interval
			ELSE now() - ((21600 + random() * %d)::int)::text || ' seconds'::interval
		END`, oldestSeconds-21600)
	} else if distribution == "yesterday" {
		// timestamps only for the previous day
		tsExpr = "CURRENT_DATE - 1 + (random() * 86400)::text || ' seconds'::interval"
	} else if distribution == "last_week" {
		// timestamps for the past 7 days
		tsExpr = "now() - (random() * 604800)::text || ' seconds'::interval"
	} else {
		// default: uniform across past 24 hours
		tsExpr = "now() - (random() * 86400)::text || ' seconds'::interval"
	}

	// Generate VALUES clause for Postgres
	return fmt.Sprintf(`
		INSERT INTO %s (id, ts, event_type)
		SELECT
			row_number() OVER () + %d AS id,
			%s AS ts,
			%s[((random() * %d)::int %% %d) + 1] AS event_type
		FROM generate_series(1, %d)`,
		fqt, startID,
		tsExpr,
		sqlArrayPostgres(eventTypes), len(eventTypes), len(eventTypes),
		batchSize,
	)
}

func batchSQLRichPostgres(fqt string, startID, batchSize int, distribution string, hotspotDays int) string {
	var tsExpr string
	if distribution == "hotspot" {
		oldestSeconds := hotspotDays * 24 * 3600
		tsExpr = fmt.Sprintf(`CASE
			WHEN random() < 0.7 THEN now() - (random() * 21600)::text || ' seconds'::interval
			ELSE now() - ((21600 + random() * %d)::int)::text || ' seconds'::interval
		END`, oldestSeconds-21600)
	} else if distribution == "yesterday" {
		tsExpr = "CURRENT_DATE - 1 + (random() * 86400)::text || ' seconds'::interval"
	} else if distribution == "last_week" {
		tsExpr = "now() - (random() * 604800)::text || ' seconds'::interval"
	} else {
		tsExpr = "now() - (random() * 86400)::text || ' seconds'::interval"
	}

	// Build JSON payload inline
	payloadExpr := fmt.Sprintf(`jsonb_build_object(
		'event_type', %s[((random() * %d)::int %% %d) + 1],
		'page', %s[((random() * %d)::int %% %d) + 1],
		'source', %s[((random() * %d)::int %% %d) + 1],
		'country', %s[((random() * %d)::int %% %d) + 1]
	)`,
		sqlArrayPostgres(eventTypes), len(eventTypes), len(eventTypes),
		sqlArrayPostgres(pages), len(pages), len(pages),
		sqlArrayPostgres(sources), len(sources), len(sources),
		sqlArrayPostgres(countries), len(countries), len(countries),
	)

	return fmt.Sprintf(`
		INSERT INTO %s (id, user_id, event_type, ts, payload, metadata)
		SELECT
			row_number() OVER () + %d AS id,
			%s[((random() * %d)::int %% %d) + 1] AS user_id,
			%s[((random() * %d)::int %% %d) + 1] AS event_type,
			%s AS ts,
			%s AS payload,
			jsonb_build_object('timestamp', now()) AS metadata
		FROM generate_series(1, %d)`,
		fqt, startID,
		sqlArrayPostgres(streamUsers), len(streamUsers), len(streamUsers),
		sqlArrayPostgres(eventTypes), len(eventTypes), len(eventTypes),
		tsExpr,
		payloadExpr,
		batchSize,
	)
}

// ── Single-row SQL generation (for ticker mode) ────────────────────────────

// tickerSQLSimpleDrifted inserts a row with a new column (event_category) that
// does not exist in the table, simulating a source schema change that breaks
// downstream data capture with an "unknown column" error.
func tickerSQLSimpleDrifted(fqt string, id int) string {
	return fmt.Sprintf(`
		INSERT INTO %s (id, ts, event_type, event_category)
		VALUES (
			%d,
			now(),
			%s,
			'engagement'
		)`,
		fqt, id,
		"'"+eventTypes[id%len(eventTypes)]+"'",
	)
}

// tickerSQLRichDrifted inserts a row with a new column (schema_version) that
// does not exist in the table, simulating a source schema change.
func tickerSQLRichDrifted(fqt string, id int, userID string) string {
	return fmt.Sprintf(`
		INSERT INTO %s (id, user_id, event_type, ts, payload, metadata, schema_version)
		VALUES (
			%d,
			'%s',
			%s,
			now(),
			{
				'page':        %s,
				'duration_ms': %d,
				'value':       %.2f
			}::JSON,
			{
				'source':      %s,
				'country':     %s,
				'session_id':  'sess_' || (%d)::VARCHAR,
				'ab_variant':  '%s'
			}::JSON,
			2
		)`,
		fqt,
		id,
		userID,
		"'"+eventTypes[id%len(eventTypes)]+"'",
		"'"+pages[id%len(pages)]+"'",
		100+id%9900,
		float64(id)*0.01+0.01,
		"'"+sources[id%len(sources)]+"'",
		"'"+countries[id%len(countries)]+"'",
		1+id%999999,
		map[int]string{0: "A", 1: "B"}[id%2],
	)
}

func tickerSQLSimple(fqt string, id int) string {
	return fmt.Sprintf(`
		INSERT INTO %s
		VALUES (
			%d,
			now(),
			%s
		)`,
		fqt, id,
		"'"+eventTypes[id%len(eventTypes)]+"'",
	)
}

func tickerSQLRich(fqt string, id int, userID string) string {
	return fmt.Sprintf(`
		INSERT INTO %s
		VALUES (
			%d,
			'%s',
			%s,
			now(),
			{
				'page':        %s,
				'duration_ms': %d,
				'value':       %.2f
			}::JSON,
			{
				'source':      %s,
				'country':     %s,
				'session_id':  'sess_' || (%d)::VARCHAR,
				'ab_variant':  '%s'
			}::JSON
		)`,
		fqt,
		id,
		userID,
		"'"+eventTypes[id%len(eventTypes)]+"'",
		"'"+pages[id%len(pages)]+"'",
		100+id%9900,
		float64(id)*0.01+0.01,
		"'"+sources[id%len(sources)]+"'",
		"'"+countries[id%len(countries)]+"'",
		1+id%999999,
		map[int]string{0: "A", 1: "B"}[id%2],
	)
}

// ── storage stats ─────────────────────────────────────────────────────────────

type storageStats struct {
	total int64
}

func getStorageStats(db *sql.DB, catalog, table string) storageStats {
	var s storageStats

	row := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", catalog, table))
	_ = row.Scan(&s.total)

	return s
}

func getMaxID(db *sql.DB, catalog, table string) (int, error) {
	var maxID sql.NullInt64
	err := db.QueryRow(fmt.Sprintf("SELECT MAX(id) FROM %s.%s", catalog, table)).Scan(&maxID)
	if err != nil {
		return 0, err
	}
	if !maxID.Valid {
		return -1, nil
	}
	return int(maxID.Int64), nil
}

// ── terminal output helpers ───────────────────────────────────────────────────

const (
	cyan   = "\033[36m"
	green  = "\033[32m"
	yellow = "\033[33m"
	red    = "\033[31m"
	bold   = "\033[1m"
	dim    = "\033[2m"
	reset  = "\033[0m"
)

func rule(label string) {
	width := 72
	if label == "" {
		fmt.Println(strings.Repeat("─", width))
		return
	}
	pad := (width - len(label) - 2) / 2
	if pad < 0 {
		pad = 0
	}
	fmt.Printf("%s%s %s %s%s\n",
		cyan+bold, strings.Repeat("─", pad), label, strings.Repeat("─", pad), reset)
}

func commas(n int64) string {
	s := fmt.Sprintf("%d", n)
	out := make([]byte, 0, len(s)+len(s)/3)
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, byte(c))
	}
	return string(out)
}

func printStats(
	batchNum int,
	numBatches int, // 0 = forever
	batchRows int,
	batchSecs float64,
	cumulative int64,
	elapsedTotal float64,
	s storageStats,
	flushInfo string,
) {
	throughput := 0.0
	if batchSecs > 0 {
		throughput = float64(batchRows) / batchSecs
	}
	overall := 0.0
	if elapsedTotal > 0 {
		overall = float64(cumulative) / elapsedTotal
	}

	batchLabel := fmt.Sprintf("%d / ∞", batchNum)
	if numBatches > 0 {
		batchLabel = fmt.Sprintf("%d / %d", batchNum, numBatches)
	}

	// top border
	fmt.Printf("\n%s╭─ Batch %-4d ───────────────────────────────────────╮%s\n", cyan+bold, batchNum, reset)

	row := func(label, value string) {
		fmt.Printf("%s│%s %-30s %s%20s %s│%s\n",
			cyan+bold, reset,
			dim+label+reset,
			bold, value, reset,
			cyan+bold,
		)
	}

	row("Batch", batchLabel)
	row("Batch rows inserted", commas(int64(batchRows)))
	row("Batch throughput", fmt.Sprintf("%s rows/sec", commas(int64(throughput))))
	row("Overall throughput", fmt.Sprintf("%s rows/sec", commas(int64(overall))))
	row("Total rows (COUNT)", commas(s.total))
	if flushInfo != "" {
		row("Last flush to Parquet", flushInfo)
	}

	fmt.Printf("%s╰────────────────────────────────────────────────────╯%s\n", cyan+bold, reset)
}

func printTickerStats(
	tickNum int,
	tickSecs float64,
	cumulative int64,
	elapsedTotal float64,
	s storageStats,
	dupCount int64,
	driftCount int64,
) {
	overall := 0.0
	if elapsedTotal > 0 {
		overall = float64(cumulative) / elapsedTotal
	}

	fmt.Printf("\n%s╭─ Tick %-5d ────────────────────────────────────────╮%s\n", cyan+bold, tickNum, reset)

	row := func(label, value string) {
		fmt.Printf("%s│%s %-30s %s%20s %s│%s\n",
			cyan+bold, reset,
			dim+label+reset,
			bold, value, reset,
			cyan+bold,
		)
	}

	row("Tick number", commas(int64(tickNum)))
	row("Total rows (COUNT)", commas(s.total))
	row("Overall throughput", fmt.Sprintf("%s rows/sec", commas(int64(overall))))
	if dupCount > 0 {
		row("Duplicate rows injected", commas(dupCount))
	}
	if driftCount > 0 {
		fmt.Printf("%s│%s %-30s %s%20s %s│%s\n",
			cyan+bold, reset,
			dim+red+"Schema drift errors"+reset,
			red+bold, commas(driftCount), reset,
			cyan+bold,
		)
	}

	fmt.Printf("%s╰────────────────────────────────────────────────────╯%s\n", cyan+bold, reset)
}

// ── DDL ───────────────────────────────────────────────────────────────────────

const simpleDDL = `CREATE TABLE IF NOT EXISTS %s (
	id          INTEGER,
	ts          TIMESTAMP,
	event_type  VARCHAR
)`

const richDDL = `CREATE TABLE IF NOT EXISTS %s (
	id          INTEGER,
	user_id     VARCHAR,
	event_type  VARCHAR,
	ts          TIMESTAMP,
	payload     JSON,
	metadata    JSON
)`

const rejectedEventsDDL = `CREATE TABLE IF NOT EXISTS %s (
	rejected_at     TIMESTAMP,
	source_table    VARCHAR,
	anomaly_type    VARCHAR,
	attempted_id    INTEGER,
	error_message   VARCHAR,
	payload         JSON
)`

const rejectedEventsTable = "events_rejected"

func sqlStringLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func createRejectedEventsTable(db *sql.DB, catalogName string) error {
	fqt := catalogName + "." + rejectedEventsTable
	_, err := db.Exec(fmt.Sprintf(rejectedEventsDDL, fqt))
	return err
}

func simpleDriftPayload(id int) map[string]any {
	return map[string]any{
		"id":             id,
		"event_type":     eventTypes[id%len(eventTypes)],
		"event_category": "engagement",
	}
}

func richDriftPayload(id int, userID string) map[string]any {
	return map[string]any{
		"id":             id,
		"user_id":        userID,
		"event_type":     eventTypes[id%len(eventTypes)],
		"schema_version": 2,
		"payload": map[string]any{
			"page":        pages[id%len(pages)],
			"duration_ms": 100 + id%9900,
			"value":       float64(id)*0.01 + 0.01,
		},
		"metadata": map[string]any{
			"source":     sources[id%len(sources)],
			"country":    countries[id%len(countries)],
			"session_id": fmt.Sprintf("sess_%d", 1+id%999999),
			"ab_variant": map[int]string{0: "A", 1: "B"}[id%2],
		},
	}
}

func recordRejectedEvent(
	db *sql.DB,
	catalogName string,
	sourceTable string,
	anomalyType string,
	attemptedID int,
	insertErr error,
	payload map[string]any,
) error {
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	fqt := catalogName + "." + rejectedEventsTable
	query := fmt.Sprintf(`
		INSERT INTO %s (rejected_at, source_table, anomaly_type, attempted_id, error_message, payload)
		VALUES (
			now(),
			%s,
			%s,
			%d,
			%s,
			%s::JSON
		)`,
		fqt,
		sqlStringLiteral(sourceTable),
		sqlStringLiteral(anomalyType),
		attemptedID,
		sqlStringLiteral(insertErr.Error()),
		sqlStringLiteral(string(payloadJSON)),
	)
	_, err = db.Exec(query)
	return err
}

// ── run loops ──────────────────────────────────────────────────────────────

func runSetup(cfg ConnectionConfig) error {
	if cfg.isPostgresSink() {
		return runSetupPostgresSink(cfg)
	}

	db, err := openAndAttach(cfg)
	if err != nil {
		return err
	}
	defer db.Close()

	simpleFQT := cfg.CatalogName + ".events"
	if _, err := db.Exec(fmt.Sprintf(simpleDDL, simpleFQT)); err != nil {
		return fmt.Errorf("create simple table: %w", err)
	}

	richFQT := cfg.CatalogName + ".events_rich"
	if _, err := db.Exec(fmt.Sprintf(richDDL, richFQT)); err != nil {
		return fmt.Errorf("create rich table: %w", err)
	}

	if err := createRejectedEventsTable(db, cfg.CatalogName); err != nil {
		return fmt.Errorf("create rejected events table: %w", err)
	}

	rule("DuckLake Setup Complete")
	printConnectionSummary(cfg)
	fmt.Printf("  Tables       : %s%s.events, %s.events_rich, %s.%s%s\n",
		green, cfg.CatalogName, cfg.CatalogName, cfg.CatalogName, rejectedEventsTable, reset)
	fmt.Println()
	fmt.Printf("%s✓ Setup complete. Ready for benchmarking!%s\n", green, reset)
	return nil
}

func runSetupPostgresSink(cfg ConnectionConfig) error {
	db, err := openPostgres(cfg)
	if err != nil {
		return err
	}
	defer db.Close()

	// Create schema if it doesn't exist (and is specified)
	if cfg.PostgresSchema != "" {
		createSchemaDDL := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", cfg.PostgresSchema)
		if _, err := db.Exec(createSchemaDDL); err != nil {
			return fmt.Errorf("create schema: %w", err)
		}
	}

	// Create source tables for both simple and rich schemas
	simpleTable := cfg.getPostgresTableName("events_source")
	simpleDDL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id          INTEGER,
		ts          TIMESTAMP,
		event_type  VARCHAR
	)`, simpleTable)
	if _, err := db.Exec(simpleDDL); err != nil {
		return fmt.Errorf("create %s table: %w", simpleTable, err)
	}

	richTable := cfg.getPostgresTableName("events_rich_source")
	richDDL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id          INTEGER,
		user_id     VARCHAR,
		event_type  VARCHAR,
		ts          TIMESTAMP,
		payload     JSON,
		metadata    JSON
	)`, richTable)
	if _, err := db.Exec(richDDL); err != nil {
		return fmt.Errorf("create %s table: %w", richTable, err)
	}

	rule("Postgres Source Setup Complete")
	fmt.Printf("  Tables       : %s%s, %s%s\n", green, simpleTable, richTable, reset)
	fmt.Println()
	fmt.Printf("%s✓ Setup complete. Ready for data loading!%s\n", green, reset)
	return nil
}

func runBatchMode(
	db *sql.DB,
	catalogName string,
	table string,
	schemaMode string,
	batchSize int,
	batchSizeMin int,
	batchSizeMax int,
	numBatches int,
	checkpointInterval int,
	distribution string,
	hotspotDays int,
	sigCh chan os.Signal,
) error {
	fqt := catalogName + "." + table

	ddl := simpleDDL
	if schemaMode == "rich" {
		ddl = richDDL
	}
	if _, err := db.Exec(fmt.Sprintf(ddl, fqt)); err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	sqlGen := batchSQLSimple
	if schemaMode == "rich" {
		sqlGen = batchSQLRich
	}

	maxID, err := getMaxID(db, catalogName, table)
	if err != nil {
		return fmt.Errorf("query max id: %w", err)
	}
	nextID := maxID + 1
	fmt.Printf("  Starting ID   : %s%d%s (max existing: %s%d%s)\n",
		green, nextID, reset, dim, maxID, reset)

	var (
		cumulative int64
		startTotal = time.Now()
		flushInfo  string
		batchNum   int
	)

	runForever := numBatches == 0

loop:
	for runForever || batchNum < numBatches {
		select {
		case <-sigCh:
			fmt.Printf("\n%sInterrupted by user.%s\n", yellow, reset)
			break loop
		default:
		}

	batchNum++
	startID := nextID
	
	// Use randomized batch size if min/max provided, otherwise use fixed batchSize
	currentBatchSize := batchSize
	if batchSizeMin > 0 || batchSizeMax > 0 {
		min, max := batchSizeMin, batchSizeMax
		if min <= 0 {
			min = 1
		}
		if max <= 0 {
			max = batchSize
		}
		if min > max {
			min, max = max, min
		}
		currentBatchSize = min + rand.Intn(max-min+1)
	}
	nextID += currentBatchSize

	query := sqlGen(fqt, startID, currentBatchSize, distribution, hotspotDays)
	t0 := time.Now()
	if _, err := db.Exec(query); err != nil {
		return fmt.Errorf("batch %d insert: %w", batchNum, err)
	}
	batchSecs := time.Since(t0).Seconds()

	cumulative += int64(currentBatchSize)
		elapsedTotal := time.Since(startTotal).Seconds()

		if checkpointInterval > 0 && batchNum%checkpointInterval == 0 {
			f0 := time.Now()
			if _, err := db.Exec(fmt.Sprintf("CALL ducklake_flush_inlined_data('%s')", catalogName)); err != nil {
				fmt.Fprintf(os.Stderr, "warning: flush failed: %v\n", err)
			}
			flushInfo = fmt.Sprintf("%.2fs", time.Since(f0).Seconds())
		}

	s := getStorageStats(db, catalogName, table)
	printStats(batchNum, numBatches, currentBatchSize, batchSecs,
		cumulative, elapsedTotal, s, flushInfo)
	}

	elapsedTotal := time.Since(startTotal).Seconds()
	overall := 0.0
	if elapsedTotal > 0 {
		overall = float64(cumulative) / elapsedTotal
	}
	rule("")
	fmt.Printf("%s%sDone.%s Inserted %s%s%s rows in %.2fs  (%s rows/sec overall)\n",
		bold, green, reset,
		bold, commas(cumulative), reset,
		elapsedTotal,
		commas(int64(overall)),
	)
	return nil
}

func runTickerMode(
	db *sql.DB,
	catalogName string,
	table string,
	schemaMode string,
	duration int,
	checkpointInterval int,
	duplicateRate float64,
	schemaDriftRate float64,
	sigCh chan os.Signal,
) error {
	fqt := catalogName + "." + table

	ddl := simpleDDL
	if schemaMode == "rich" {
		ddl = richDDL
	}
	if _, err := db.Exec(fmt.Sprintf(ddl, fqt)); err != nil {
		return fmt.Errorf("create table: %w", err)
	}
	if err := createRejectedEventsTable(db, catalogName); err != nil {
		return fmt.Errorf("create rejected events table: %w", err)
	}

	maxID, err := getMaxID(db, catalogName, table)
	if err != nil {
		return fmt.Errorf("query max id: %w", err)
	}
	nextID := maxID + 1
	fmt.Printf("  Starting ID   : %s%d%s (max existing: %s%d%s)\n",
		green, nextID, reset, dim, maxID, reset)
	if schemaMode == "rich" {
		fmt.Printf("  User pool     : %s%d%s users (%suser_1..user_%d%s), random per event\n",
			green, len(streamUsers), reset, dim, len(streamUsers), reset)
	}

	var (
		cumulative int64
		dupCount   int64
		driftCount int64
		startTotal = time.Now()
		tickNum    int
	)

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	durationTimer := time.After(time.Duration(duration) * time.Second)

loop:
	for {
		select {
		case <-sigCh:
			fmt.Printf("\n%sInterrupted by user.%s\n", yellow, reset)
			break loop
		case <-durationTimer:
			fmt.Printf("\n%sDuration complete.%s\n", yellow, reset)
			break loop
		case <-ticker.C:
			tickNum++
			id := nextID
			nextID++

			var query string
			var driftSQL string
			var driftPayload map[string]any
			var userID string
			if schemaMode == "rich" {
				userID = streamUsers[rand.Intn(len(streamUsers))]
				query = tickerSQLRich(fqt, id, userID)
				driftSQL = tickerSQLRichDrifted(fqt, id, userID)
				driftPayload = richDriftPayload(id, userID)
			} else {
				query = tickerSQLSimple(fqt, id)
				driftSQL = tickerSQLSimpleDrifted(fqt, id)
				driftPayload = simpleDriftPayload(id)
			}

			if _, err := db.Exec(query); err != nil {
				return fmt.Errorf("tick %d insert: %w", tickNum, err)
			}
			cumulative++

			// Inject a duplicate row with probability duplicateRate.
			if duplicateRate > 0 && rand.Float64() < duplicateRate {
				if _, err := db.Exec(query); err != nil {
					fmt.Fprintf(os.Stderr, "warning: duplicate inject failed at tick %d: %v\n", tickNum, err)
				} else {
					dupCount++
					cumulative++
				}
			}

			// Inject a schema-drift row with probability schemaDriftRate.
			// The drifted SQL targets a column that doesn't exist in the table,
			// so DuckDB rejects it — simulating a downstream data-capture break
			// when the source system emits a new field the pipeline doesn't know about.
			if schemaDriftRate > 0 && rand.Float64() < schemaDriftRate {
				if _, err := db.Exec(driftSQL); err != nil {
					driftCount++
					fmt.Printf("\n%s⚠  Schema drift at tick %d:%s %v\n", red+bold, tickNum, reset, err)
					if dlqErr := recordRejectedEvent(db, catalogName, table, "schema_drift", id, err, driftPayload); dlqErr != nil {
						fmt.Fprintf(os.Stderr, "warning: failed to record rejected event at tick %d: %v\n", tickNum, dlqErr)
					}
				}
			}

			elapsedTotal := time.Since(startTotal).Seconds()

			s := getStorageStats(db, catalogName, table)
			printTickerStats(tickNum, 1.0, cumulative, elapsedTotal, s, dupCount, driftCount)
		}
	}

	elapsedTotal := time.Since(startTotal).Seconds()
	overall := 0.0
	if elapsedTotal > 0 {
		overall = float64(cumulative) / elapsedTotal
	}

	// Load previous flush state
	state := loadState(catalogName, table)

	// Check final row count and flush if new rows > checkpointInterval
	if checkpointInterval > 0 {
		s := getStorageStats(db, catalogName, table)
		newRowsSinceFlush := s.total - state.LastFlushedCount
		
		if newRowsSinceFlush > int64(checkpointInterval) {
			fmt.Printf("\n%sCheckpointing %s%d%s rows to Parquet...%s\n",
				cyan+bold, bold, newRowsSinceFlush, reset, reset)
			fmt.Printf("%s(Total in table: %s%d%s, Last flushed: %s%d%s)%s\n",
				dim, bold, s.total, reset,
				bold, state.LastFlushedCount, reset, reset)
			
			if _, err := db.Exec(fmt.Sprintf("CALL ducklake_flush_inlined_data('%s')", catalogName)); err != nil {
				fmt.Fprintf(os.Stderr, "warning: final flush failed: %v\n", err)
			} else {
				// Update state with new flushed count
				state.LastFlushedCount = s.total
				if err := saveState(catalogName, table, state); err != nil {
					fmt.Fprintf(os.Stderr, "warning: failed to save state: %v\n", err)
				}
				
				// Refresh stats after flush
				s = getStorageStats(db, catalogName, table)
				fmt.Printf("%s✓ Flush complete.%s Total rows in table: %s%d%s\n",
					green, reset,
					bold, s.total, reset)
			}
		}
	}

	rule("")
	fmt.Printf("%s%sDone.%s Inserted %s%s%s rows in %.2fs  (%s rows/sec overall)\n",
		bold, green, reset,
		bold, commas(cumulative), reset,
		elapsedTotal,
		commas(int64(overall)),
	)
	if dupCount > 0 {
		fmt.Printf("%s  ↳ %s duplicate rows injected (duplicate rate %.0f%%)%s\n",
			yellow, commas(dupCount), duplicateRate*100, reset)
	}
	if driftCount > 0 {
		fmt.Printf("%s  ↳ %s schema drift errors (drift rate %.0f%%) — recorded in %s.%s%s\n",
			red, commas(driftCount), schemaDriftRate*100, catalogName, rejectedEventsTable, reset)
	}
	return nil
}

// ── postgres sink modes (simplified, no metrics) ────────────────────────────

func runBatchModePostgresSink(
	db *sql.DB,
	cfg ConnectionConfig,
	table string,
	schemaMode string,
	batchSize int,
	batchSizeMin int,
	batchSizeMax int,
	numBatches int,
	distribution string,
	hotspotDays int,
	sigCh chan os.Signal,
) error {
	// Use schema-qualified table name if schema is specified
	fqt := cfg.getPostgresTableName(table)

	// Create table if it doesn't exist
	ddl := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id          INTEGER,
		ts          TIMESTAMP,
		event_type  VARCHAR
	)`, fqt)
	if schemaMode == "rich" {
		ddl = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			id          INTEGER,
			user_id     VARCHAR,
			event_type  VARCHAR,
			ts          TIMESTAMP,
			payload     JSON,
			metadata    JSON
		)`, fqt)
	}
	if _, err := db.Exec(ddl); err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	// Get max existing ID
	maxID := int64(0)
	err := db.QueryRow("SELECT COALESCE(MAX(id), 0) FROM " + fqt).Scan(&maxID)
	if err != nil {
		return fmt.Errorf("query max id: %w", err)
	}
	nextID := maxID + 1
	fmt.Printf("  Starting ID   : %s%d%s (max existing: %s%d%s)\n",
		green, nextID, reset, dim, maxID, reset)

	sqlGen := batchSQLSimplePostgres
	if schemaMode == "rich" {
		sqlGen = batchSQLRichPostgres
	}

	var (
		cumulative int64
		startTotal = time.Now()
		batchNum   int
	)

	runForever := numBatches == 0

loop:
	for runForever || batchNum < numBatches {
		select {
		case <-sigCh:
			fmt.Printf("\n%sInterrupted by user.%s\n", yellow, reset)
			break loop
		default:
		}

		batchNum++
		startID := nextID

		// Use randomized batch size if min/max provided, otherwise use fixed batchSize
		currentBatchSize := batchSize
		if batchSizeMin > 0 || batchSizeMax > 0 {
			min, max := batchSizeMin, batchSizeMax
			if min <= 0 {
				min = 1
			}
			if max <= 0 {
				max = batchSize
			}
			if min > max {
				min, max = max, min
			}
			currentBatchSize = min + rand.Intn(max-min+1)
		}
		nextID += int64(currentBatchSize)

		query := sqlGen(fqt, int(startID), currentBatchSize, distribution, hotspotDays)
		t0 := time.Now()
		if _, err := db.Exec(query); err != nil {
			return fmt.Errorf("batch %d insert: %w", batchNum, err)
		}
		batchSecs := time.Since(t0).Seconds()

		cumulative += int64(currentBatchSize)

		// Simple status per batch (no metrics)
		fmt.Printf("  Batch %d: %s rows in %.3fs\n", batchNum, commas(int64(currentBatchSize)), batchSecs)
	}

	elapsedTotal := time.Since(startTotal).Seconds()
	overall := 0.0
	if elapsedTotal > 0 {
		overall = float64(cumulative) / elapsedTotal
	}
	rule("")
	fmt.Printf("%s%sDone.%s Inserted %s%s%s rows in %.2fs  (%s rows/sec overall)\n",
		bold, green, reset,
		bold, commas(cumulative), reset,
		elapsedTotal,
		commas(int64(overall)),
	)
	return nil
}

func runTickerModePostgresSink(
	db *sql.DB,
	cfg ConnectionConfig,
	table string,
	schemaMode string,
	duration int,
	duplicateRate float64,
	schemaDriftRate float64,
	sigCh chan os.Signal,
) error {
	// Use schema-qualified table name if schema is specified
	fqt := cfg.getPostgresTableName(table)

	// Create table if it doesn't exist
	ddl := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id          INTEGER,
		ts          TIMESTAMP,
		event_type  VARCHAR
	)`, fqt)
	if schemaMode == "rich" {
		ddl = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			id          INTEGER,
			user_id     VARCHAR,
			event_type  VARCHAR,
			ts          TIMESTAMP,
			payload     JSON,
			metadata    JSON
		)`, fqt)
	}
	if _, err := db.Exec(ddl); err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	// Get max existing ID
	maxID := int64(0)
	err := db.QueryRow("SELECT COALESCE(MAX(id), 0) FROM " + fqt).Scan(&maxID)
	if err != nil {
		return fmt.Errorf("query max id: %w", err)
	}
	nextID := maxID + 1

	var (
		cumulative int64
		startTotal = time.Now()
		tickNum    int
	)

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	durationTimer := time.After(time.Duration(duration) * time.Second)

loop:
	for {
		select {
		case <-sigCh:
			fmt.Printf("\n%sInterrupted by user.%s\n", yellow, reset)
			break loop
		case <-durationTimer:
			fmt.Printf("\n%sDuration complete.%s\n", yellow, reset)
			break loop
		case <-ticker.C:
			tickNum++
			id := nextID
			nextID++

		var query string
		if schemaMode == "rich" {
			userID := streamUsers[rand.Intn(len(streamUsers))]
			query = tickerSQLRich(fqt, int(id), userID)
		} else {
			query = tickerSQLSimple(fqt, int(id))
		}

			if _, err := db.Exec(query); err != nil {
				return fmt.Errorf("tick %d insert: %w", tickNum, err)
			}
			cumulative++

			// Inject a duplicate row with probability duplicateRate.
			if duplicateRate > 0 && rand.Float64() < duplicateRate {
				if _, err := db.Exec(query); err != nil {
					fmt.Fprintf(os.Stderr, "warning: duplicate inject failed at tick %d: %v\n", tickNum, err)
				} else {
					cumulative++
				}
			}

			elapsedTotal := time.Since(startTotal).Seconds()
			if tickNum%10 == 0 {
				fmt.Printf("  Tick %d: %s rows inserted so far in %.1fs\n", tickNum, commas(cumulative), elapsedTotal)
			}
		}
	}

	elapsedTotal := time.Since(startTotal).Seconds()
	overall := 0.0
	if elapsedTotal > 0 {
		overall = float64(cumulative) / elapsedTotal
	}
	rule("")
	fmt.Printf("%s%sDone.%s Inserted %s%s%s rows in %.2fs  (%s rows/sec overall)\n",
		bold, green, reset,
		bold, commas(cumulative), reset,
		elapsedTotal,
		commas(int64(overall)),
	)
	return nil
}

func main() {
	var (
		table              string
		schemaMode         string
		runMode            string
		distribution       string
		hotspotDays        int
		batchSize          int
		batchSizeMin       int
		batchSizeMax       int
		numBatches         int
		checkpointInterval int
		duration           int
		duplicateRate      float64
		schemaDriftRate    float64
	)

	rootCmd := &cobra.Command{
		Use:   "parkbench",
		Short: "DuckLake streaming benchmark tool",
		Long: `Parkbench — A DuckLake streaming benchmark tool for testing data insertion performance.

Supports setup and run operations against any DuckLake catalog: a local
Postgres + local filesystem for quick dev, a remote Postgres (e.g. Supabase)
with S3 storage for production, or a named persistent DuckDB secret bundling
both.`,
	}

	// ── Setup command ───────────────────────────────────────────────────────

	var setupConn ConnectionConfig

	setupCmd := &cobra.Command{
		Use:   "setup",
		Short: "Initialize a new DuckLake catalog",
		Long: `Initialize a new DuckLake catalog. Parquet data files are written to
--data-path (a local directory or an s3:// URI), backed by a Postgres or
local DuckDB metadata store, or by a pre-created --ducklake-secret.

If using a local Postgres metadata store, the database must already exist:
  psql -d postgres -c "CREATE DATABASE ducklake_v1;"`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if setupConn.DucklakeSecret == "" && setupConn.MetadataStore != "postgres" && setupConn.MetadataStore != "duckdb" {
				return fmt.Errorf("--metadata-store must be 'postgres' or 'duckdb', got %q", setupConn.MetadataStore)
			}
			return runSetup(setupConn)
		},
	}
	registerConnectionFlags(setupCmd, &setupConn)

	// ── Reset command ────────────────────────────────────────────────────────

	var resetConn ConnectionConfig
	var forceReset bool

	resetCmd := &cobra.Command{
		Use:   "reset",
		Short: "Drop and recreate the DuckLake catalog from scratch",
		Long: `For local catalogs (local Postgres + local data path), drops and recreates
the Postgres database and wipes the local data directory. For remote
catalogs (remote/managed Postgres, S3 storage, or --ducklake-secret),
instead drops just the DuckLake-tracked tables, since parkbench doesn't own
that shared infrastructure outright. Either way, re-runs setup afterward.
Prompts for confirmation unless --force is set.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if resetConn.DucklakeSecret == "" && resetConn.MetadataStore != "postgres" && resetConn.MetadataStore != "duckdb" {
				return fmt.Errorf("--metadata-store must be 'postgres' or 'duckdb', got %q", resetConn.MetadataStore)
			}
			return runReset(resetConn, forceReset)
		},
	}
	registerConnectionFlags(resetCmd, &resetConn)
	resetCmd.Flags().BoolVarP(&forceReset, "force", "f", false, "Skip confirmation prompt")

	// ── Run command ─────────────────────────────────────────────────────────

	var runConn ConnectionConfig

	runCmd := &cobra.Command{
		Use:   "run",
		Short: "Run benchmarks on a DuckLake catalog",
		Long: `Run benchmarks on an existing DuckLake catalog.

Modes:
  Schema modes (simple, rich):
    simple  — {id, ts, event_type}
    rich    — {id, user_id, event_type, ts, payload JSON, metadata JSON}

  Run modes (batch, ticker):
    batch   — Inserts large batches of rows (default)
    ticker  — Inserts one row per second for a specified duration`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if table == "" {
				if schemaMode == "rich" {
					table = "events_rich"
				} else {
					table = "events"
				}
			}
			if schemaMode != "simple" && schemaMode != "rich" {
				return fmt.Errorf("--mode must be 'simple' or 'rich', got %q", schemaMode)
			}
			if runMode != "batch" && runMode != "ticker" {
				return fmt.Errorf("--run-mode must be 'batch' or 'ticker', got %q", runMode)
			}
			if runConn.DucklakeSecret == "" && runConn.MetadataStore != "postgres" && runConn.MetadataStore != "duckdb" {
				return fmt.Errorf("--metadata-store must be 'postgres' or 'duckdb', got %q", runConn.MetadataStore)
			}
			if distribution != "uniform" && distribution != "hotspot" && distribution != "yesterday" && distribution != "last_week" {
				return fmt.Errorf("--distribution must be 'uniform', 'hotspot', 'yesterday', or 'last_week', got %q", distribution)
			}
			if duplicateRate < 0 || duplicateRate > 1 {
				return fmt.Errorf("--duplicate-rate must be between 0.0 and 1.0, got %.2f", duplicateRate)
			}
			if schemaDriftRate < 0 || schemaDriftRate > 1 {
				return fmt.Errorf("--schema-drift-rate must be between 0.0 and 1.0, got %.2f", schemaDriftRate)
			}

		// ── banner ──────────────────────────────────────────────────────
		if runConn.isPostgresSink() {
			rule("Postgres Source Data Load")
		} else {
			rule("DuckLake Stream Benchmark")
		}
		printConnectionSummary(runConn)
		if runConn.isPostgresSink() {
			fmt.Printf("  Table        : %s%s%s\n", green, table, reset)
		} else {
			fmt.Printf("  Table        : %s%s.%s%s\n", green, runConn.CatalogName, table, reset)
		}
			fmt.Printf("  Schema mode  : %s%s%s\n", green, schemaMode, reset)
			fmt.Printf("  Run mode     : %s%s%s\n", green, runMode, reset)
			fmt.Printf("  Distribution : %s%s%s\n", green, distribution, reset)
			if distribution == "hotspot" {
				fmt.Printf("  Hotspot span : %s%d days%s\n", green, hotspotDays, reset)
			}

			if runMode == "batch" {
				fmt.Printf("  Batch size   : %s%s%s rows\n", green, commas(int64(batchSize)), reset)
				if numBatches == 0 {
					fmt.Printf("  Batches      : %s∞%s\n", green, reset)
				} else {
					fmt.Printf("  Batches      : %s%d%s\n", green, numBatches, reset)
				}
				if checkpointInterval == 0 {
					fmt.Printf("  Flush        : %sdisabled%s\n", green, reset)
				} else {
					fmt.Printf("  Flush        : every %s%d%s batches\n", green, checkpointInterval, reset)
				}
			} else if runMode == "ticker" {
				fmt.Printf("  Duration     : %s%d%s seconds\n", green, duration, reset)
				if checkpointInterval == 0 {
					fmt.Printf("  Flush        : %sdisabled%s (no flush at end)\n", green, reset)
				} else {
					fmt.Printf("  Flush        : at end if inlined rows > %s%d%s\n", green, checkpointInterval, reset)
				}
				if duplicateRate > 0 {
					fmt.Printf("  Duplicate rate : %s%.0f%%%s duplicate rows\n", yellow, duplicateRate*100, reset)
				} else {
					fmt.Printf("  Duplicate rate : %sdisabled%s\n", green, reset)
				}
				if schemaDriftRate > 0 {
					fmt.Printf("  Schema drift : %s%.0f%%%s of ticks emit unknown column (insert will fail)\n", red, schemaDriftRate*100, reset)
				} else {
					fmt.Printf("  Schema drift : %sdisabled%s\n", green, reset)
				}
			}
		fmt.Println()

		// ── open connection ─────────────────────────────────────────────
		var db *sql.DB
		var err error
		if runConn.isPostgresSink() {
			db, err = openPostgres(runConn)
		} else {
			db, err = openAndAttach(runConn)
		}
		if err != nil {
			return err
		}
		defer db.Close()

		// ── signal handling ─────────────────────────────────────────────
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

		// ── run selected mode ───────────────────────────────────────────
		if runConn.isPostgresSink() {
			if runMode == "ticker" {
				return runTickerModePostgresSink(db, runConn, table, schemaMode, duration, duplicateRate, schemaDriftRate, sigCh)
			}
			return runBatchModePostgresSink(db, runConn, table, schemaMode, batchSize, batchSizeMin, batchSizeMax, numBatches, distribution, hotspotDays, sigCh)
		}
		if runMode == "ticker" {
			return runTickerMode(db, runConn.CatalogName, table, schemaMode, duration, checkpointInterval, duplicateRate, schemaDriftRate, sigCh)
		}
		return runBatchMode(db, runConn.CatalogName, table, schemaMode, batchSize, batchSizeMin, batchSizeMax, numBatches, checkpointInterval, distribution, hotspotDays, sigCh)
		},
	}

	registerConnectionFlags(runCmd, &runConn)
	runCmd.Flags().StringVarP(&table, "table", "t", "", "Target table (defaults to 'events' or 'events_rich')")
	runCmd.Flags().StringVarP(&schemaMode, "mode", "m", "simple", "Schema mode: simple or rich")
	runCmd.Flags().StringVarP(&runMode, "run-mode", "r", "batch", "Run mode: batch or ticker")
	runCmd.Flags().StringVar(&distribution, "distribution", "uniform", "Timestamp distribution: 'uniform' (past 24h), 'hotspot' (70% in last 6h, 30% spread), 'yesterday' (previous day only), or 'last_week' (past 7 days)")
	runCmd.Flags().IntVar(&hotspotDays, "hotspot-days", 30, "Number of days to spread the 30% tail in hotspot distribution (hotspot mode only)")
	runCmd.Flags().IntVarP(&batchSize, "batch-size", "b", 100_000, "Fixed number of rows to insert per batch (batch mode only); overridden if --batch-size-min/--batch-size-max are set")
	runCmd.Flags().IntVar(&batchSizeMin, "batch-size-min", 0, "Minimum batch size for randomization (batch mode only); if set with --batch-size-max, each batch gets a random size in [min, max]")
	runCmd.Flags().IntVar(&batchSizeMax, "batch-size-max", 0, "Maximum batch size for randomization (batch mode only); if set with --batch-size-min, each batch gets a random size in [min, max]")
	runCmd.Flags().IntVarP(&numBatches, "num-batches", "n", 10, "Number of batches (batch mode only, 0 = forever)")
	runCmd.Flags().IntVarP(&checkpointInterval, "flush-interval", "k", 10, "Flush inlined rows to Parquet every N batches, or at end if inlined rows > N (0 = never)")
	runCmd.Flags().IntVarP(&duration, "duration", "d", 60, "Duration in seconds (ticker mode only)")
	runCmd.Flags().Float64Var(&duplicateRate, "duplicate-rate", 0.0, "Probability (0.0–1.0) of injecting a duplicate row each tick (ticker mode only)")
	runCmd.Flags().Float64Var(&schemaDriftRate, "schema-drift-rate", 0.0, "Probability (0.0–1.0) of injecting a schema-breaking row each tick (ticker mode only); the insert targets an unknown column and is rejected, simulating a source schema change")

	// ── Root command setup ──────────────────────────────────────────────────

	rootCmd.AddCommand(setupCmd)
	rootCmd.AddCommand(resetCmd)
	rootCmd.AddCommand(runCmd)
	rootCmd.AddCommand(newSecretsCmd())

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
