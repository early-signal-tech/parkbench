package main

import "testing"

// A named secret with no other options used to send `AS wh ()`, which DuckDB rejects with
// `Parser Error: syntax error at or near ")"`.
func TestAttachStatements(t *testing.T) {
	cases := []struct {
		cfg  ConnectionConfig
		want string
	}{
		{ConnectionConfig{CatalogName: "wh", DucklakeSecret: "ducklake_prod"},
			"ATTACH 'ducklake:ducklake_prod' AS wh"},
		{ConnectionConfig{CatalogName: "wh", DucklakeSecret: "ducklake_prod", Migrate: true},
			"ATTACH 'ducklake:ducklake_prod' AS wh (AUTOMATIC_MIGRATION TRUE)"},
		{ConnectionConfig{CatalogName: "wh", DucklakeSecret: "ducklake_prod", MetadataCatalogName: "meta"},
			"ATTACH 'ducklake:ducklake_prod' AS wh (METADATA_CATALOG 'meta')"},
		{ConnectionConfig{CatalogName: "wh", MetadataStore: "duckdb", DataPath: "./ducklake_data"},
			"ATTACH 'ducklake:duckdb:ducklake_data/wh.ducklake' AS wh (DATA_PATH './ducklake_data')"},
		{ConnectionConfig{CatalogName: "wh", MetadataStore: "postgres", PgDSN: "dbname=ducklake_v1 host=localhost", DataPath: "./ducklake_data"},
			"ATTACH 'ducklake:postgres:dbname=ducklake_v1 host=localhost' AS wh (DATA_PATH './ducklake_data')"},
	}
	for _, c := range cases {
		if got := c.cfg.buildAttachSQL(); got != c.want {
			t.Errorf("got  %s\nwant %s", got, c.want)
		}
	}
}
