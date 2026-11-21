package main

import (
	"errors"
	"strings"
	"testing"
)

func TestReadSecretValues(t *testing.T) {
	ok := []struct {
		typ, in string
		want    map[string]string
	}{
		{"postgres", `{"host": "db.example.com", "password": "pw", "port": 6543}`,
			map[string]string{"host": "db.example.com", "password": "pw", "port": "6543"}},
		{"postgres", `{"host": "h", "password": "pw", "port": "5433", "user": ""}`,
			map[string]string{"host": "h", "password": "pw", "port": "5433"}},
		{"s3", `{"key_id": "AKIA", "secret": "s", "scope": "s3://bucket"}`,
			map[string]string{"key_id": "AKIA", "secret": "s", "scope": "s3://bucket"}},
		{"ducklake", `{"data_path": "s3://b/p", "metadata_secret": "supabase_pg", "metadata_schema": null}`,
			map[string]string{"data_path": "s3://b/p", "metadata_secret": "supabase_pg"}},
	}
	for _, c := range ok {
		got, err := readSecretValues(strings.NewReader(c.in), c.typ)
		if err != nil {
			t.Errorf("%s %s: %v", c.typ, c.in, err)
			continue
		}
		if len(got) != len(c.want) {
			t.Errorf("%s: got %v, want %v", c.typ, got, c.want)
		}
		for k, v := range c.want {
			if got[k] != v {
				t.Errorf("%s: %s = %q, want %q", c.typ, k, got[k], v)
			}
		}
	}

	bad := []struct{ typ, in, wantErr string }{
		{"mysql", `{}`, "--type must be one of"},
		{"postgres", `not json`, "one JSON object"},
		{"postgres", `["a"]`, "one JSON object"},
		{"postgres", `{"host": "h"}`, "needs password"},
		{"postgres", `{"host": "h", "password": ""}`, "password can't be empty"},
		{"postgres", `{"host": "h", "password": "hunter2", "sslmode": "x"}`, "doesn't take: sslmode"},
		{"postgres", `{"host": "h", "password": "hunter2", "port": 99999}`, "port must be a port number"},
		{"postgres", `{"host": "h", "password": "hunter2", "port": 1.5}`, "port must be a string"},
		{"s3", `{"key_id": "AKIA", "secret": 7}`, "secret must be a string"},
		{"ducklake", `{"data_path": "p", "metadata_secret": "x; DROP"}`, "metadata_secret must be"},
	}
	for _, c := range bad {
		_, err := readSecretValues(strings.NewReader(c.in), c.typ)
		if err == nil || !strings.Contains(err.Error(), c.wantErr) {
			t.Errorf("%s %s: err = %v, want %q", c.typ, c.in, err, c.wantErr)
			continue
		}
		if strings.Contains(err.Error(), "hunter2") {
			t.Errorf("error repeats a value: %v", err)
		}
	}

	if _, err := readSecretValues(strings.NewReader(`{"host":"`+strings.Repeat("x", maxSecretInput)+`"}`), "postgres"); err == nil {
		t.Error("oversized input accepted")
	}
}

func TestCreateSecretSQL(t *testing.T) {
	got, err := createSecretSQL("postgres", "supabase_pg", map[string]string{"host": "h", "password": "it's"})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"PERSISTENT SECRET supabase_pg (", "PORT 5432", "DATABASE 'postgres'", "USER 'postgres'", "PASSWORD 'it''s'"} {
		if !strings.Contains(got, want) {
			t.Errorf("postgres SQL missing %q:\n%s", want, got)
		}
	}

	got, _ = createSecretSQL("s3", "s3_main", map[string]string{"key_id": "k", "secret": "s"})
	if strings.Contains(got, "REGION") || strings.Contains(got, "SCOPE") {
		t.Errorf("s3 SQL has optional fields it wasn't given:\n%s", got)
	}

	got, _ = createSecretSQL("ducklake", "lake", map[string]string{"data_path": "s3://b/p", "metadata_secret": "pg", "metadata_schema": "meta"})
	if !strings.Contains(got, "'SECRET': 'pg', 'SCHEMA': 'meta'}") || !strings.Contains(got, "DATA_PATH 's3://b/p'") {
		t.Errorf("ducklake SQL:\n%s", got)
	}

	for _, name := range []string{"", "1abc", "a b", "x; DROP SECRET y", "-flag"} {
		if _, err := createSecretSQL("s3", name, map[string]string{"key_id": "k", "secret": "s"}); err == nil {
			t.Errorf("name %q accepted", name)
		}
	}
}

func TestRedactValues(t *testing.T) {
	values := map[string]string{"host": "db.example.com", "password": "o'hara"}
	err := redactValues(errors.New("Parser Error: near PASSWORD 'o''hara' and o'hara on db.example.com"), "postgres", values)
	if strings.Contains(err.Error(), "hara") {
		t.Errorf("password left in: %v", err)
	}
	if !strings.Contains(err.Error(), "db.example.com") {
		t.Errorf("a host isn't sensitive and should stay: %v", err)
	}
}
