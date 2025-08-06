package comet_test

import (
	"testing"

	"github.com/orbiterhq/comet"
)

// TestConfigHelpers verifies the configuration helper functions work correctly
func TestConfigHelpers(t *testing.T) {
	t.Run("OptimizedConfig_256_shards", func(t *testing.T) {
		cfg := comet.OptimizedConfig(256, 3072) // 256 shards, 3GB memory

		// Verify optimized settings
		expectedFileSize := int64(12 << 20) // 12MB per file with 3GB/256 shards
		if cfg.Storage.MaxFileSize != expectedFileSize {
			t.Errorf("Expected MaxFileSize=%d, got %d", expectedFileSize, cfg.Storage.MaxFileSize)
		}
		if cfg.Storage.FlushEntries != 50000 {
			t.Errorf("Expected FlushEntries=50000, got %d", cfg.Storage.FlushEntries)
		}
		if cfg.Storage.CheckpointEntries != 100000 {
			t.Errorf("Expected CheckpointEntries=100000, got %d", cfg.Storage.CheckpointEntries)
		}

		// Ensure we can create a client with this config
		dir := t.TempDir()
		client, err := comet.NewClient(dir, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
	})

	t.Run("OptimizedConfig_small_files", func(t *testing.T) {
		cfg := comet.OptimizedConfig(1024, 3072) // 1024 shards, should get minimum file size

		// Should clamp to minimum 10MB
		expectedFileSize := int64(10 << 20)
		if cfg.Storage.MaxFileSize != expectedFileSize {
			t.Errorf("Expected MaxFileSize=%d (minimum), got %d", expectedFileSize, cfg.Storage.MaxFileSize)
		}

		// Should use optimal flush entries even for small files
		if cfg.Storage.FlushEntries != 50000 {
			t.Errorf("Expected FlushEntries=50000, got %d", cfg.Storage.FlushEntries)
		}
		if cfg.Storage.CheckpointEntries != 100000 {
			t.Errorf("Expected CheckpointEntries=100000, got %d", cfg.Storage.CheckpointEntries)
		}
	})
}

// ExampleOptimizedConfig demonstrates using the optimized configuration
func ExampleOptimizedConfig() {
	// Create a client optimized for 256 shards with 3GB memory budget
	cfg := comet.OptimizedConfig(256, 3072)
	client, err := comet.NewClient("/tmp/comet", cfg)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// Use 256 shards when writing
	stream := comet.PickShardStream("user-123", "events", "v1", 256)
	// Write to the stream...
	_ = stream
}
