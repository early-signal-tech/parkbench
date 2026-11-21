package main

// Machine-readable run output (`run --output jsonl`).
//
// In jsonl mode stdout carries only JSON Lines, one object per event, so a
// host process (e.g. the PB-1 panel via cli-bridge) can follow a run step by
// step. Everything human-facing — the banner, warnings, progress text — goes
// to stderr instead. Every object has an "event" field naming its type:
//
//	batch  one per inserted batch (batch mode)
//	tick   one per tick (ticker mode)
//	drift  a schema-drift insert was rejected (ticker mode; follows its tick's
//	       inserts and precedes that tick's "tick" event)
//	flush  inlined rows were flushed to Parquet (DuckLake only)
//	done   the run ended; always the last line of a run that didn't error
//
// Events never include connection strings, credentials, or row contents.

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"
)

const (
	outputText  = "text"
	outputJSONL = "jsonl"
)

// Reasons a run ended, reported in the "done" event.
const (
	doneComplete    = "complete"    // --num-batches reached
	doneDuration    = "duration"    // --duration elapsed (ticker mode)
	doneInterrupted = "interrupted" // SIGINT / SIGTERM
)

type batchEvent struct {
	Event       string  `json:"event"`
	Batch       int     `json:"batch"`
	NumBatches  int     `json:"num_batches"` // 0 = run forever
	Rows        int     `json:"rows"`
	BatchSecs   float64 `json:"batch_secs"`
	RowsTotal   int64   `json:"rows_total"`
	ElapsedSecs float64 `json:"elapsed_secs"`
	RowsPerSec  float64 `json:"rows_per_sec"`
	TableRows   *int64  `json:"table_rows,omitempty"` // COUNT(*) after the batch; DuckLake only
}

type tickEvent struct {
	Event       string  `json:"event"`
	Tick        int     `json:"tick"`
	Rows        int     `json:"rows"` // rows inserted this tick (1, or 2 with a duplicate)
	Duplicate   bool    `json:"duplicate"`
	Drift       bool    `json:"drift"`
	RowsTotal   int64   `json:"rows_total"`
	Duplicates  int64   `json:"duplicates"`
	Drifts      int64   `json:"drifts"`
	ElapsedSecs float64 `json:"elapsed_secs"`
	RowsPerSec  float64 `json:"rows_per_sec"`
	TableRows   *int64  `json:"table_rows,omitempty"` // DuckLake only
}

type driftEvent struct {
	Event string `json:"event"`
	Tick  int    `json:"tick"`
	ID    int    `json:"id"`
	Error string `json:"error"`
}

type flushEvent struct {
	Event string  `json:"event"`
	Batch int     `json:"batch,omitempty"` // batch mode: the batch that triggered it
	Rows  int64   `json:"rows,omitempty"`  // ticker mode: rows flushed at the end
	Secs  float64 `json:"secs"`
}

type doneEvent struct {
	Event       string  `json:"event"`
	Reason      string  `json:"reason"`
	Steps       int     `json:"steps"` // batches or ticks completed
	RowsTotal   int64   `json:"rows_total"`
	ElapsedSecs float64 `json:"elapsed_secs"`
	RowsPerSec  float64 `json:"rows_per_sec"`
	Duplicates  int64   `json:"duplicates"`
	Drifts      int64   `json:"drifts"`
}

// events is nil in text mode, so every emit is a no-op there.
var events *json.Encoder

// enableJSONL points JSON events at the real stdout and moves all other
// output to stderr. fmt.Print* reads os.Stdout at call time, so the existing
// human-facing prints follow without being rewritten.
func enableJSONL() {
	enableJSONLTo(os.Stdout)
	os.Stdout = os.Stderr
}

func enableJSONLTo(w io.Writer) {
	events = json.NewEncoder(w)
}

func jsonlEnabled() bool { return events != nil }

func emit(ev any) {
	if events == nil {
		return
	}
	if err := events.Encode(ev); err != nil {
		fmt.Fprintf(os.Stderr, "warning: write event: %v\n", err)
	}
}

func rate(rows int64, secs float64) float64 {
	if secs <= 0 {
		return 0
	}
	return float64(rows) / secs
}

func emitDone(reason string, steps int, rows int64, secs float64, dups, drifts int64) {
	emit(doneEvent{
		Event: "done", Reason: reason, Steps: steps, RowsTotal: rows,
		ElapsedSecs: secs, RowsPerSec: rate(rows, secs), Duplicates: dups, Drifts: drifts,
	})
}

// minTickInterval keeps a mistyped --tick-interval from hammering the catalog.
const minTickInterval = 10 * time.Millisecond

func tickIntervalFrom(secs float64) (time.Duration, error) {
	d := time.Duration(secs * float64(time.Second))
	if d < minTickInterval {
		return 0, fmt.Errorf("--tick-interval must be at least %gs, got %g", minTickInterval.Seconds(), secs)
	}
	return d, nil
}
