package comet

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestIsolateRaceCondition isolates the exact race condition
func TestIsolateRaceCondition(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	// Use multi-process config to trigger the race
	config := MultiProcessConfig()
	config.Storage.MaxFileSize = 2048

	streamName := "events:v1:shard:0001"

	// Phase 1: Write data and ensure it's flushed
	client1, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		data := fmt.Sprintf(`{"id": %d, "test": "race_debug"}`, i)
		_, err := client1.Append(ctx, streamName, [][]byte{[]byte(data)})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Force explicit flush
	shard, _ := client1.getOrCreateShard(1)
	shard.mu.Lock()
	if shard.mmapWriter != nil {
		shard.mmapWriter.Sync()
	}
	if shard.writer != nil {
		shard.writer.Flush()
	}
	if shard.dataFile != nil {
		shard.dataFile.Sync()
	}
	shard.mu.Unlock()

	client1.Close()

	// Phase 2: Corrupt index to force rebuild
	indexPath := filepath.Join(dir, "shard-0001", "index.bin")
	if err := os.WriteFile(indexPath, []byte("corrupted"), 0644); err != nil {
		t.Fatal(err)
	}

	// Phase 3: Create client2 and immediately check state
	client2, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client2.Close()

	shard2, _ := client2.getOrCreateShard(1)

	// Capture state immediately after rebuild
	shard2.mu.RLock()
	initialFiles0Start := shard2.index.Files[0].StartEntry
	t.Logf("ISOLATE: Immediately after rebuild - Files[0].StartEntry = %d", initialFiles0Start)
	shard2.mu.RUnlock()

	// Small delay to let any background processes settle
	time.Sleep(10 * time.Millisecond)

	// Check state again
	shard2.mu.RLock()
	laterFiles0Start := shard2.index.Files[0].StartEntry
	t.Logf("ISOLATE: After 10ms delay - Files[0].StartEntry = %d", laterFiles0Start)
	shard2.mu.RUnlock()

	// Trigger consumer read (this causes loadIndexWithRecovery)
	consumer := NewConsumer(client2, ConsumerOptions{Group: "race-test"})
	defer consumer.Close()

	// Check state before consumer read
	shard2.mu.RLock()
	preConsumerFiles0Start := shard2.index.Files[0].StartEntry
	t.Logf("ISOLATE: Before consumer read - Files[0].StartEntry = %d", preConsumerFiles0Start)
	shard2.mu.RUnlock()

	// This should trigger the race condition
	messages, err := consumer.Read(ctx, []uint32{1}, 5)
	if err != nil {
		t.Fatal(err)
	}

	// Check state after consumer read
	shard2.mu.RLock()
	postConsumerFiles0Start := shard2.index.Files[0].StartEntry
	t.Logf("ISOLATE: After consumer read - Files[0].StartEntry = %d", postConsumerFiles0Start)
	shard2.mu.RUnlock()

	t.Logf("ISOLATE: Consumer read %d messages starting from entry %d",
		len(messages), messages[0].ID.EntryNumber)

	// Analysis
	if initialFiles0Start != postConsumerFiles0Start {
		t.Logf("RACE DETECTED: Files[0].StartEntry changed from %d to %d during consumer read",
			initialFiles0Start, postConsumerFiles0Start)
	}

	if messages[0].ID.EntryNumber != 0 {
		t.Errorf("RACE EFFECT: Consumer started from entry %d instead of 0", messages[0].ID.EntryNumber)
	}
}
