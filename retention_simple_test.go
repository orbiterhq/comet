package comet

import (
	"context"
	"testing"
	"time"
)

// TestRetentionSimple verifies basic retention functionality
func TestRetentionSimple(t *testing.T) {
	dir := t.TempDir()

	// Create config with retention enabled
	config := DefaultCometConfig()
	config.Retention.MaxAge = 100 * time.Millisecond
	config.Retention.CleanupInterval = 50 * time.Millisecond

	client, err := NewClient(dir, config)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	// Write some data
	ctx := context.Background()
	streamName := "test:v1:shard:0000"

	_, err = client.Append(ctx, streamName, [][]byte{
		[]byte(`{"test": "retention"}`),
	})
	if err != nil {
		t.Fatalf("failed to write data: %v", err)
	}

	// Verify retention stats are available
	stats := client.GetRetentionStats()
	if stats.TotalFiles == 0 {
		t.Error("expected at least one file")
	}

	// Force a cleanup to exercise the code path
	client.ForceRetentionCleanup()

	// Get stats again
	stats2 := client.GetRetentionStats()
	t.Logf("Retention is active: MaxAge=%v, CleanupInterval=%v, TotalFiles=%d",
		stats2.RetentionAge, config.Retention.CleanupInterval, stats2.TotalFiles)

	// The important thing is that retention runs without crashing
	t.Log("Retention cleanup executed successfully")
}
