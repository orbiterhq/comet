package comet_test

import (
	"log/slog"
	"os"
	"path/filepath"

	"github.com/orbiterhq/comet"
)

func Example_customLogger() {
	// Example: Using the default logger (writes to stderr)
	config := comet.DefaultCometConfig()
	config.Log.Level = "info" // Options: debug, info, warn, error, none

	// Example: Using slog
	slogger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	config.Log.Logger = comet.NewSlogAdapter(slogger)

	// Example: Disabling logs entirely
	config.Log.Logger = comet.NoOpLogger{}

	// Create client with configured logging
	client, err := comet.NewClient(filepath.Join(os.TempDir(), "logger"), config)
	if err != nil {
		panic(err)
	}
	defer client.Close()
}
