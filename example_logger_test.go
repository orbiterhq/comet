package comet_test

import (
	"log/slog"
	"os"

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

	// Example: Enabling debug mode
	config.Log.EnableDebug = true // or set COMET_DEBUG=1 environment variable

	// Create client with configured logging
	client, err := comet.NewClientWithConfig("/tmp/comet", config)
	if err != nil {
		panic(err)
	}
	defer client.Close()
}
