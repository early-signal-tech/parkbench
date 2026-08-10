// The `secrets` subcommand manages the persistent DuckDB secrets that can
// back a DuckLake connection: an S3 secret for storage, a Postgres secret for
// the metadata catalog, and a combined DUCKLAKE secret bundling both (plus
// DATA_PATH and metadata schema) so `parkbench run --ducklake-secret <name>`
// can attach with a single flag — mirroring the CREATE PERSISTENT SECRET
// pattern from https://thefulldatastack.substack.com/p/ducklake-in-production-catalog-storage.
//
// Persistent secrets are stored in DuckDB's local, unencrypted secret file
// and are available across any DuckDB or parkbench session on this machine,
// regardless of working directory. This is a convenience for local/dev use;
// managing secrets directly via the DuckDB CLI remains fully supported and
// is preferable for shared or production environments.
package main

import (
	"database/sql"
	"fmt"

	"github.com/spf13/cobra"
)

func newSecretsCmd() *cobra.Command {
	secretsCmd := &cobra.Command{
		Use:   "secrets",
		Short: "Create, list, and drop persistent DuckDB secrets for a DuckLake connection",
	}

	secretsCmd.AddCommand(newSecretsCreateS3Cmd())
	secretsCmd.AddCommand(newSecretsCreatePostgresCmd())
	secretsCmd.AddCommand(newSecretsCreateDucklakeCmd())
	secretsCmd.AddCommand(newSecretsListCmd())
	secretsCmd.AddCommand(newSecretsDropCmd())

	return secretsCmd
}

func withDuckDB(fn func(db *sql.DB) error) error {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()
	return fn(db)
}

func newSecretsCreateS3Cmd() *cobra.Command {
	var name, keyID, secretKey, region string

	cmd := &cobra.Command{
		Use:   "create-s3",
		Short: "Create a persistent S3 secret",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return withDuckDB(func(db *sql.DB) error {
				query := fmt.Sprintf(`CREATE OR REPLACE PERSISTENT SECRET %s (
	TYPE s3,
	PROVIDER config,
	KEY_ID %s,
	SECRET %s,
	REGION %s
)`, name, sqlStringLiteral(keyID), sqlStringLiteral(secretKey), sqlStringLiteral(region))
				if _, err := db.Exec(query); err != nil {
					return fmt.Errorf("create s3 secret: %w", err)
				}
				fmt.Printf("%s✓ Created persistent S3 secret %q%s\n", green, name, reset)
				return nil
			})
		},
	}

	cmd.Flags().StringVar(&name, "name", "s3_bucket", "Name of the persistent secret")
	cmd.Flags().StringVar(&keyID, "key-id", "", "AWS access key ID")
	cmd.Flags().StringVar(&secretKey, "secret-key", "", "AWS secret access key")
	cmd.Flags().StringVar(&region, "region", "", "AWS region, e.g. us-east-1")
	cmd.MarkFlagRequired("key-id")
	cmd.MarkFlagRequired("secret-key")
	cmd.MarkFlagRequired("region")

	return cmd
}

func newSecretsCreatePostgresCmd() *cobra.Command {
	var name, host, database, user, password string
	var port int

	cmd := &cobra.Command{
		Use:   "create-postgres",
		Short: "Create a persistent Postgres secret (works for Supabase or any Postgres)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return withDuckDB(func(db *sql.DB) error {
				query := fmt.Sprintf(`CREATE OR REPLACE PERSISTENT SECRET %s (
	TYPE POSTGRES,
	HOST %s,
	PORT %d,
	DATABASE %s,
	USER %s,
	PASSWORD %s
)`, name, sqlStringLiteral(host), port, sqlStringLiteral(database), sqlStringLiteral(user), sqlStringLiteral(password))
				if _, err := db.Exec(query); err != nil {
					return fmt.Errorf("create postgres secret: %w", err)
				}
				fmt.Printf("%s✓ Created persistent Postgres secret %q%s\n", green, name, reset)
				return nil
			})
		},
	}

	cmd.Flags().StringVar(&name, "name", "supabase_pg", "Name of the persistent secret")
	cmd.Flags().StringVar(&host, "host", "", "Postgres host, e.g. db.xxxx.supabase.co")
	cmd.Flags().IntVar(&port, "port", 5432, "Postgres port")
	cmd.Flags().StringVar(&database, "database", "postgres", "Postgres database name")
	cmd.Flags().StringVar(&user, "user", "postgres", "Postgres user")
	cmd.Flags().StringVar(&password, "password", "", "Postgres password")
	cmd.MarkFlagRequired("host")
	cmd.MarkFlagRequired("password")

	return cmd
}

func newSecretsCreateDucklakeCmd() *cobra.Command {
	var name, dataPath, metadataSecret, metadataSchema string

	cmd := &cobra.Command{
		Use:   "create-ducklake",
		Short: "Create a combined DUCKLAKE secret bundling a Postgres metadata secret and a DATA_PATH",
		Long: `Creates a DUCKLAKE-type secret that bundles a previously created Postgres
secret (see 'secrets create-postgres') with a DATA_PATH (typically an s3://
URI) and a metadata schema, so 'parkbench run --ducklake-secret <name>' can
attach with a single flag:

  parkbench secrets create-ducklake \
    --name ducklake_prod \
    --data-path s3://my-bucket/prefix \
    --metadata-secret supabase_pg \
    --metadata-schema ducklake_meta`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return withDuckDB(func(db *sql.DB) error {
				metaParams := fmt.Sprintf("MAP {'TYPE': 'POSTGRES', 'SECRET': %s", sqlStringLiteral(metadataSecret))
				if metadataSchema != "" {
					metaParams += fmt.Sprintf(", 'SCHEMA': %s", sqlStringLiteral(metadataSchema))
				}
				metaParams += "}"

				query := fmt.Sprintf(`CREATE OR REPLACE PERSISTENT SECRET %s (
	TYPE DUCKLAKE,
	METADATA_PATH '',
	DATA_PATH %s,
	METADATA_PARAMETERS %s
)`, name, sqlStringLiteral(dataPath), metaParams)
				if _, err := db.Exec(query); err != nil {
					return fmt.Errorf("create ducklake secret: %w", err)
				}
				fmt.Printf("%s✓ Created persistent DuckLake secret %q%s\n", green, name, reset)
				fmt.Printf("  Use it with: %sparkbench run --ducklake-secret %s%s\n", dim, name, reset)
				return nil
			})
		},
	}

	cmd.Flags().StringVar(&name, "name", "ducklake_prod", "Name of the persistent secret")
	cmd.Flags().StringVar(&dataPath, "data-path", "", "DATA_PATH for Parquet files, e.g. s3://bucket/prefix")
	cmd.Flags().StringVar(&metadataSecret, "metadata-secret", "", "Name of a previously created Postgres secret (see 'secrets create-postgres')")
	cmd.Flags().StringVar(&metadataSchema, "metadata-schema", "", "Postgres schema to scope DuckLake's metadata tables to")
	cmd.MarkFlagRequired("data-path")
	cmd.MarkFlagRequired("metadata-secret")

	return cmd
}

func newSecretsListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all persistent and temporary DuckDB secrets",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return withDuckDB(func(db *sql.DB) error {
				rows, err := db.Query("SELECT * FROM duckdb_secrets()")
				if err != nil {
					return fmt.Errorf("list secrets: %w", err)
				}
				defer rows.Close()
				return printRows(rows)
			})
		},
	}
}

func newSecretsDropCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "drop <name>",
		Short: "Drop a persistent DuckDB secret",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			return withDuckDB(func(db *sql.DB) error {
				if _, err := db.Exec(fmt.Sprintf("DROP PERSISTENT SECRET IF EXISTS %s", name)); err != nil {
					return fmt.Errorf("drop secret: %w", err)
				}
				fmt.Printf("%s✓ Dropped secret %q%s\n", green, name, reset)
				return nil
			})
		},
	}
}

// printRows prints an arbitrary result set as a simple column-aligned table,
// used for `secrets list` since duckdb_secrets() columns vary across DuckDB
// versions.
func printRows(rows *sql.Rows) error {
	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	for _, c := range cols {
		fmt.Printf("%s%-22s%s", bold, c, reset)
	}
	fmt.Println()

	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}

	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}
		for _, v := range vals {
			fmt.Printf("%-22v", v)
		}
		fmt.Println()
	}
	return rows.Err()
}
