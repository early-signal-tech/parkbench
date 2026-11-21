package main

// Creating a secret without its values on the command line.
//
// `parkbench secrets create --type <type> --name <name>` reads the values as one JSON
// object on stdin, so a password or key never shows up in the process list or shell
// history. This is how a host process (PB-1, via cli-bridge) creates secrets. The
// create-s3/-postgres/-ducklake commands build the same SQL from flags.
//
// Errors never repeat a value: anything that can echo the SQL is redacted first.

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// A secret name goes into SQL unquoted, as DuckDB requires, so it's held to an identifier.
var secretNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)

const maxSecretInput = 64 << 10

type secretField struct {
	key      string
	required bool
	integer  bool
	// Shown nowhere, but marked so the list of what's sensitive lives in one place.
	sensitive bool
}

// The fields each type takes on stdin, in the order they go into the SQL.
var secretTypes = map[string][]secretField{
	"s3": {
		{key: "key_id", required: true, sensitive: true},
		{key: "secret", required: true, sensitive: true},
		{key: "region"},
		{key: "scope"},
	},
	"postgres": {
		{key: "host", required: true},
		{key: "port", integer: true},
		{key: "database"},
		{key: "user"},
		{key: "password", required: true, sensitive: true},
	},
	"ducklake": {
		{key: "data_path", required: true},
		{key: "metadata_secret", required: true},
		{key: "metadata_schema"},
	},
}

func secretTypeNames() string {
	names := make([]string, 0, len(secretTypes))
	for name := range secretTypes {
		names = append(names, name)
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}

func checkSecretName(label, name string) error {
	if !secretNamePattern.MatchString(name) {
		return fmt.Errorf("%s must be a letter or _ followed by letters, digits or _ (at most 63)", label)
	}
	return nil
}

// readSecretValues reads one JSON object of string (or, for port, number) values. It
// rejects unknown and missing fields by name only, never repeating a value.
func readSecretValues(r io.Reader, typ string) (map[string]string, error) {
	fields, ok := secretTypes[typ]
	if !ok {
		return nil, fmt.Errorf("--type must be one of %s", secretTypeNames())
	}
	raw, err := io.ReadAll(io.LimitReader(r, maxSecretInput+1))
	if err != nil {
		return nil, fmt.Errorf("read values from stdin: %w", err)
	}
	if len(raw) > maxSecretInput {
		return nil, errors.New("the values on stdin are too long")
	}
	var input map[string]any
	if err := json.Unmarshal(raw, &input); err != nil || input == nil {
		return nil, errors.New("stdin must hold one JSON object of the secret's values")
	}

	known := map[string]secretField{}
	for _, f := range fields {
		known[f.key] = f
	}
	var unknown []string
	for key := range input {
		if _, ok := known[key]; !ok {
			unknown = append(unknown, key)
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return nil, fmt.Errorf("a %s secret doesn't take: %s", typ, strings.Join(unknown, ", "))
	}

	values := map[string]string{}
	for _, f := range fields {
		v, present := input[f.key]
		switch v := v.(type) {
		case nil:
		case string:
			if v != "" {
				values[f.key] = v
			}
		case float64:
			if !f.integer || v != float64(int(v)) {
				return nil, fmt.Errorf("%s must be a string", f.key)
			}
			values[f.key] = strconv.Itoa(int(v))
		default:
			return nil, fmt.Errorf("%s must be a string", f.key)
		}
		if f.required && values[f.key] == "" {
			if present {
				return nil, fmt.Errorf("%s can't be empty", f.key)
			}
			return nil, fmt.Errorf("a %s secret needs %s", typ, f.key)
		}
		if f.integer && values[f.key] != "" {
			if n, err := strconv.Atoi(values[f.key]); err != nil || n < 1 || n > 65535 {
				return nil, fmt.Errorf("%s must be a port number", f.key)
			}
		}
	}
	if typ == "ducklake" {
		if err := checkSecretName("metadata_secret", values["metadata_secret"]); err != nil {
			return nil, err
		}
	}
	return values, nil
}

// createSecretSQL builds the CREATE OR REPLACE PERSISTENT SECRET statement. Values are
// quoted as SQL strings; the name was checked by checkSecretName.
func createSecretSQL(typ, name string, v map[string]string) (string, error) {
	if err := checkSecretName("--name", name); err != nil {
		return "", err
	}
	or := func(key, fallback string) string {
		if v[key] != "" {
			return v[key]
		}
		return fallback
	}
	switch typ {
	case "s3":
		parts := []string{"TYPE s3", "PROVIDER config", "KEY_ID " + sqlStringLiteral(v["key_id"]), "SECRET " + sqlStringLiteral(v["secret"])}
		if v["region"] != "" {
			parts = append(parts, "REGION "+sqlStringLiteral(v["region"]))
		}
		if v["scope"] != "" {
			parts = append(parts, "SCOPE "+sqlStringLiteral(v["scope"]))
		}
		return fmt.Sprintf("CREATE OR REPLACE PERSISTENT SECRET %s (\n\t%s\n)", name, strings.Join(parts, ",\n\t")), nil
	case "postgres":
		return fmt.Sprintf(`CREATE OR REPLACE PERSISTENT SECRET %s (
	TYPE POSTGRES,
	HOST %s,
	PORT %s,
	DATABASE %s,
	USER %s,
	PASSWORD %s
)`, name, sqlStringLiteral(v["host"]), or("port", "5432"), sqlStringLiteral(or("database", "postgres")),
			sqlStringLiteral(or("user", "postgres")), sqlStringLiteral(v["password"])), nil
	case "ducklake":
		meta := fmt.Sprintf("MAP {'TYPE': 'POSTGRES', 'SECRET': %s", sqlStringLiteral(v["metadata_secret"]))
		if v["metadata_schema"] != "" {
			meta += fmt.Sprintf(", 'SCHEMA': %s", sqlStringLiteral(v["metadata_schema"]))
		}
		meta += "}"
		return fmt.Sprintf(`CREATE OR REPLACE PERSISTENT SECRET %s (
	TYPE DUCKLAKE,
	METADATA_PATH '',
	DATA_PATH %s,
	METADATA_PARAMETERS %s
)`, name, sqlStringLiteral(v["data_path"]), meta), nil
	}
	return "", fmt.Errorf("--type must be one of %s", secretTypeNames())
}

// redactValues takes every sensitive value out of an error before it's shown, in case
// DuckDB quotes the statement back.
func redactValues(err error, typ string, values map[string]string) error {
	if err == nil {
		return nil
	}
	text := err.Error()
	for _, f := range secretTypes[typ] {
		if f.sensitive && values[f.key] != "" {
			text = strings.ReplaceAll(text, values[f.key], "***")
			text = strings.ReplaceAll(text, strings.ReplaceAll(values[f.key], "'", "''"), "***")
		}
	}
	return errors.New(text)
}
