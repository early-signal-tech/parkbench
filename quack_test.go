package main

import (
	"errors"
	"strings"
	"testing"
)

func TestCheckMetadataStore(t *testing.T) {
	ok := []ConnectionConfig{
		{MetadataStore: "postgres"},
		{MetadataStore: "duckdb"},
		{MetadataStore: "quack", QuackURI: "quack:localhost:9495"},
		{MetadataStore: "quack", QuackURI: "quack:127.0.0.1:19000"},
		{MetadataStore: "anything", DucklakeSecret: "lake"}, // a secret ignores the store
	}
	for _, cfg := range ok {
		if err := checkMetadataStore(cfg); err != nil {
			t.Errorf("%+v: %v", cfg, err)
		}
	}
	bad := []ConnectionConfig{
		{MetadataStore: "sqlite"},
		{MetadataStore: "quack", QuackURI: "quack:db.example.com:9495"}, // only a local server
		{MetadataStore: "quack", QuackURI: "quack:localhost:9495; DROP"},
		{MetadataStore: "quack", QuackURI: ""},
	}
	for _, cfg := range bad {
		if err := checkMetadataStore(cfg); err == nil {
			t.Errorf("%+v accepted", cfg)
		}
	}
}

func TestQuackAttachSQL(t *testing.T) {
	cfg := ConnectionConfig{CatalogName: "wh", MetadataStore: "quack", QuackURI: "quack:localhost:9495", DataPath: "./ducklake_data", Migrate: true}
	got := cfg.buildAttachSQL()
	want := "ATTACH 'ducklake:quack:localhost:9495' AS wh (DATA_PATH './ducklake_data', AUTOMATIC_MIGRATION TRUE)"
	if got != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}
}

func TestQuackToken(t *testing.T) {
	t.Setenv(quackTokenEnv, "")
	if _, err := quackToken(); err == nil || !strings.Contains(err.Error(), quackTokenEnv) {
		t.Errorf("missing token: %v", err)
	}
	t.Setenv(quackTokenEnv, "it's")
	if _, err := quackToken(); err == nil {
		t.Error("a token with a quote was accepted")
	}
	t.Setenv(quackTokenEnv, "tok-123")
	if tok, err := quackToken(); err != nil || tok != "tok-123" {
		t.Errorf("got %q, %v", tok, err)
	}
	if err := withoutToken(errors.New("bad TOKEN 'tok-123' here"), "tok-123"); strings.Contains(err.Error(), "tok-123") {
		t.Errorf("token left in: %v", err)
	}
}
