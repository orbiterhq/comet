package comet

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestRetention_Basic(t *testing.T) {
	dir := t.TempDir()

	// Create config with fast cleanup
	config := DefaultCometConfig()
	config.Retention.MaxAge = 200 * time.Millisecond
	config.Retention.CleanupInterval = 100 * time.Millisecond
	config.Retention.MinFilesToKeep = 1

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "events:v1:shard:0001"

	// Write some data
	for i := 0; i < 10; i++ {
		data := [][]byte{[]byte(fmt.Sprintf(`{"id": %d}`, i))}
		_, err := client.Append(ctx, streamName, data)
		if err != nil {
			t.Fatalf("failed to add data: %v", err)
		}
	}

	// Get shard to manually create old files
	shard, err := client.getOrCreateShard(1)
	if err != nil {
		t.Fatalf("failed to get shard: %v", err)
	}

	// Manually mark the current file as old
	shard.mu.Lock()
	if len(shard.index.Files) > 0 {
		shard.index.Files[0].EndTime = time.Now().Add(-300 * time.Millisecond)
	}
	shard.mu.Unlock()

	// Wait for cleanup
	time.Sleep(300 * time.Millisecond)

	// Get retention stats
	stats := client.GetRetentionStats()
	t.Logf("Retention stats: %+v", stats)

	// The old file should have been cleaned up
	if stats.TotalFiles > config.Retention.MinFilesToKeep {
		t.Errorf("expected at most %d files after cleanup, got %d", config.Retention.MinFilesToKeep, stats.TotalFiles)
	}
}

func TestRetention_Disabled(t *testing.T) {
	dir := t.TempDir()

	// Create config with retention disabled
	config := DefaultCometConfig()
	config.Retention.CleanupInterval = 0 // Disabled

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	// Should not crash and retention should not run
	time.Sleep(100 * time.Millisecond)

	// Verify no retention goroutine is running
	if client.stopCh != nil {
		select {
		case <-client.stopCh:
			t.Error("stop channel should not be closed")
		default:
			// Good - channel is still open
		}
	}
}

func TestRetentionStats_NewestData(t *testing.T) {
	dir := t.TempDir()
	config := DefaultCometConfig()
	config.Retention.CleanupInterval = time.Hour // Don't run cleanup during test
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Write some data
	beforeWrite := time.Now()
	time.Sleep(10 * time.Millisecond) // Ensure time difference

	streamName := "test:v1:shard:0001"
	_, err = client.Append(ctx, streamName, [][]byte{
		[]byte("test data 1"),
		[]byte("test data 2"),
	})
	if err != nil {
		t.Fatalf("failed to write data: %v", err)
	}

	time.Sleep(10 * time.Millisecond)
	afterWrite := time.Now()

	// Get stats without checkpoint
	stats := client.GetRetentionStats()

	// NewestData should be close to current time, not zero or old
	if stats.NewestData.Before(beforeWrite) {
		t.Errorf("NewestData %v is before write time %v", stats.NewestData, beforeWrite)
	}

	// Allow small time difference due to execution time
	if stats.NewestData.After(afterWrite.Add(time.Second)) {
		t.Errorf("NewestData %v is significantly after current time %v", stats.NewestData, afterWrite)
	}

	// OldestData should also be reasonable
	if stats.OldestData.IsZero() {
		t.Error("OldestData should not be zero")
	}

	if stats.OldestData.After(afterWrite) {
		t.Errorf("OldestData %v is after current time %v", stats.OldestData, afterWrite)
	}

	t.Logf("Stats: OldestData=%v, NewestData=%v, TotalFiles=%d",
		stats.OldestData, stats.NewestData, stats.TotalFiles)

	// Write more data and check again
	time.Sleep(100 * time.Millisecond)

	_, err = client.Append(ctx, streamName, [][]byte{
		[]byte("test data 3"),
	})
	if err != nil {
		t.Fatalf("failed to write more data: %v", err)
	}

	stats2 := client.GetRetentionStats()

	// NewestData should have advanced
	if !stats2.NewestData.After(stats.NewestData) {
		t.Errorf("NewestData did not advance: was %v, now %v", stats.NewestData, stats2.NewestData)
	}

	// OldestData should remain the same
	if !stats2.OldestData.Equal(stats.OldestData) {
		t.Errorf("OldestData changed unexpectedly: was %v, now %v", stats.OldestData, stats2.OldestData)
	}
}
