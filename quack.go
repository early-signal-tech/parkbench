package main

// A local DuckDB catalog that several programs can use at once, through a Quack server.
//
// DuckDB lets one process open a database file for writing, so a local .ducklake catalog
// normally can't be read (say, from the duckdb CLI) while a run writes to it. Quack, the
// DuckDB 2.0 server protocol, gets around that: `parkbench quack-serve` opens the catalog
// file and serves it on localhost, and every run, setup or reset with
// `--metadata-store quack` attaches through it (`ATTACH 'ducklake:quack:localhost:9495'`),
// as can any DuckDB client with the token.
//
// The token comes from PARKBENCH_QUACK_TOKEN, never a flag, so it isn't in the process list.

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"

	"github.com/spf13/cobra"
)

const (
	quackTokenEnv     = "PARKBENCH_QUACK_TOKEN"
	defaultQuackURI   = "quack:localhost:9495"
	quackSecretName   = "parkbench_quack"
	metadataStoreList = "'postgres', 'duckdb', or 'quack'"
)

// Only a local server: the token is a shared secret between programs on this machine.
var quackURIPattern = regexp.MustCompile(`^quack:(localhost|127\.0\.0\.1):[0-9]{2,5}$`)

func checkMetadataStore(cfg ConnectionConfig) error {
	if cfg.DucklakeSecret != "" {
		return nil
	}
	switch cfg.MetadataStore {
	case "postgres", "duckdb":
		return nil
	case "quack":
		return checkQuackURI(cfg.QuackURI)
	}
	return fmt.Errorf("--metadata-store must be %s, got %q", metadataStoreList, cfg.MetadataStore)
}

func checkQuackURI(uri string) error {
	if !quackURIPattern.MatchString(uri) {
		return fmt.Errorf("--quack-uri must look like %s (a local server), got %q", defaultQuackURI, uri)
	}
	return nil
}

func quackToken() (string, error) {
	token := os.Getenv(quackTokenEnv)
	if token == "" {
		return "", fmt.Errorf("%s isn't set: it holds the Quack server's token", quackTokenEnv)
	}
	if strings.ContainsAny(token, "'\n\r") {
		return "", fmt.Errorf("%s holds characters a token can't have", quackTokenEnv)
	}
	return token, nil
}

// withoutToken takes the token out of an error before it's shown.
func withoutToken(err error, token string) error {
	if err == nil || token == "" {
		return err
	}
	return errors.New(strings.ReplaceAll(err.Error(), token, "***"))
}

// prepareQuack loads the quack extension and gives this session the server's token.
func prepareQuack(db *sql.DB) error {
	token, err := quackToken()
	if err != nil {
		return err
	}
	if _, err := db.Exec("INSTALL quack; LOAD quack;"); err != nil {
		return fmt.Errorf("load quack extension: %w", err)
	}
	query := fmt.Sprintf("CREATE OR REPLACE SECRET %s (TYPE quack, TOKEN %s)", quackSecretName, sqlStringLiteral(token))
	if _, err := db.Exec(query); err != nil {
		return withoutToken(fmt.Errorf("create quack secret: %w", err), token)
	}
	return nil
}

func newQuackServeCmd() *cobra.Command {
	var dataPath, catalog, uri string
	cmd := &cobra.Command{
		Use:   "quack-serve",
		Short: "Serve a local DuckDB catalog over Quack, so runs and readers can share it",
		Long: `Opens the local DuckDB catalog (<data-path>/<catalog>.ducklake, created if it's new) and
serves it on a local Quack server until interrupted. Then runs, setup and reset with
--metadata-store quack, and any DuckDB client with the token, can use the catalog at the
same time:

  export PARKBENCH_QUACK_TOKEN=$(openssl rand -hex 16)
  parkbench quack-serve &
  parkbench run --metadata-store quack
  duckdb -cmd "LOAD quack; CREATE SECRET (TYPE quack, TOKEN '$PARKBENCH_QUACK_TOKEN');
               ATTACH 'ducklake:quack:localhost:9495' AS wh (DATA_PATH './ducklake_data/'); USE wh;"

The token comes from PARKBENCH_QUACK_TOKEN. Run it in the folder the runs use, since
--data-path is stored in the catalog as given. Once it's serving it prints one line,
{"event":"serving","uri":"..."}, on stdout.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := checkQuackURI(uri); err != nil {
				return err
			}
			token, err := quackToken()
			if err != nil {
				return err
			}
			if err := os.MkdirAll(dataPath, 0o755); err != nil {
				return fmt.Errorf("create data directory: %w", err)
			}
			catalogFile := filepath.Join(dataPath, catalog+".ducklake")
			db, err := sql.Open("duckdb", catalogFile)
			if err != nil {
				return fmt.Errorf("open %s: %w", catalogFile, err)
			}
			defer db.Close()
			// One connection: quack_serve runs on that DuckDB instance's own threads.
			db.SetMaxOpenConns(1)
			if _, err := db.Exec("INSTALL quack; LOAD quack;"); err != nil {
				return fmt.Errorf("load quack extension: %w", err)
			}
			if _, err := db.Exec(fmt.Sprintf("CALL quack_serve(%s, token = %s)", sqlStringLiteral(uri), sqlStringLiteral(token))); err != nil {
				return withoutToken(fmt.Errorf("start quack server: %w", err), token)
			}
			fmt.Printf("{\"event\":\"serving\",\"uri\":%q,\"catalog\":%q}\n", uri, catalogFile)

			stop := make(chan os.Signal, 1)
			signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
			<-stop
			fmt.Fprintln(os.Stderr, "quack-serve: stopped")
			return nil
		},
	}
	cmd.Flags().StringVar(&dataPath, "data-path", "./ducklake_data", "Folder holding the catalog file (and the Parquet the runs write)")
	cmd.Flags().StringVarP(&catalog, "catalog", "c", "wh", "Catalog name: the file served is <data-path>/<catalog>.ducklake")
	cmd.Flags().StringVar(&uri, "quack-uri", defaultQuackURI, "Where to serve, on this machine: quack:localhost:<port>")
	return cmd
}
