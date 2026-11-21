package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestTickIntervalFrom(t *testing.T) {
	cases := []struct {
		secs    float64
		want    time.Duration
		wantErr bool
	}{
		{1, time.Second, false},
		{0.5, 500 * time.Millisecond, false},
		{5, 5 * time.Second, false},
		{0.01, 10 * time.Millisecond, false},
		{0.001, 0, true},
		{0, 0, true},
		{-1, 0, true},
	}
	for _, c := range cases {
		got, err := tickIntervalFrom(c.secs)
		if (err != nil) != c.wantErr {
			t.Errorf("tickIntervalFrom(%g) err = %v, wantErr %v", c.secs, err, c.wantErr)
			continue
		}
		if got != c.want {
			t.Errorf("tickIntervalFrom(%g) = %v, want %v", c.secs, got, c.want)
		}
	}
}

// TestEmitJSONL checks the wire format a host parses: one object per line,
// each with an "event" field, and table_rows omitted when unknown.
func TestEmitJSONL(t *testing.T) {
	var buf bytes.Buffer
	enableJSONLTo(&buf)
	t.Cleanup(func() { events = nil })

	n := int64(42)
	emit(batchEvent{Event: "batch", Batch: 1, NumBatches: 10, Rows: 42, RowsTotal: 42, TableRows: &n})
	emit(batchEvent{Event: "batch", Batch: 2, NumBatches: 10, Rows: 42, RowsTotal: 84}) // postgres sink: no table_rows
	emit(driftEvent{Event: "drift", Tick: 3, ID: 7, Error: "Binder Error: line one\nline two"})
	emit(flushEvent{Event: "flush", Batch: 2, Secs: 0.5})
	emitDone(doneInterrupted, 2, 84, 2, 0, 1)

	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	wantEvents := []string{"batch", "batch", "drift", "flush", "done"}
	if len(lines) != len(wantEvents) {
		t.Fatalf("got %d lines, want %d:\n%s", len(lines), len(wantEvents), buf.String())
	}
	parsed := make([]map[string]any, len(lines))
	for i, line := range lines {
		if err := json.Unmarshal([]byte(line), &parsed[i]); err != nil {
			t.Fatalf("line %d is not JSON: %v\n%s", i+1, err, line)
		}
		if parsed[i]["event"] != wantEvents[i] {
			t.Errorf("line %d event = %v, want %s", i+1, parsed[i]["event"], wantEvents[i])
		}
	}

	if parsed[0]["table_rows"] != float64(42) {
		t.Errorf("batch 1 table_rows = %v, want 42", parsed[0]["table_rows"])
	}
	if _, ok := parsed[1]["table_rows"]; ok {
		t.Errorf("batch 2 should omit table_rows: %s", lines[1])
	}
	if _, ok := parsed[3]["rows"]; ok {
		t.Errorf("batch-mode flush should omit rows: %s", lines[3])
	}
	done := parsed[4]
	if done["reason"] != doneInterrupted || done["steps"] != float64(2) || done["rows_per_sec"] != float64(42) {
		t.Errorf("unexpected done event: %s", lines[4])
	}
}

func TestEmitTextModeIsNoop(t *testing.T) {
	events = nil
	emit(tickEvent{Event: "tick"}) // must not panic or write anywhere
	if jsonlEnabled() {
		t.Fatal("jsonl should be off by default")
	}
}
