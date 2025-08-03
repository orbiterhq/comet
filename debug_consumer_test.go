// +build integration

package comet

import (
	"context"
	"testing"
)

func init() {
	Debug = true
}

// TestDebugConsumerRead - debug why consumer reads are returning 0 messages
func TestDebugConsumerRead(t *testing.T) {
	dir := t.TempDir()
	
	config := DefaultCometConfig()
	config.Log.EnableDebug = true
	
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "test:v1:shard:0001"

	// Check initial shard state
	shard, _ := client.getOrCreateShard(1)
	shard.mu.RLock()
	t.Logf("Initial shard state:")
	if shard.state != nil {
		t.Logf("  State exists, LastEntryNumber: %d", shard.state.GetLastEntryNumber())
	} else {
		t.Logf("  State is nil!")
	}
	shard.mu.RUnlock()

	// Write some test data
	for i := 0; i < 5; i++ {
		// Check state before write
		shard.mu.RLock()
		if shard.state != nil {
			t.Logf("Before write %d: State LastEntryNumber: %d", i, shard.state.GetLastEntryNumber())
		} else {
			t.Logf("Before write %d: State is nil!", i)
		}
		shard.mu.RUnlock()

		// Check which shard Append gets
		testShard, _ := client.getOrCreateShard(1)
		t.Logf("Before write %d: testShard=%p, shard=%p", i, testShard, shard)
		
		ids, err := client.Append(ctx, streamName, [][]byte{[]byte("test message")})
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("Wrote message %d with ID %d", i, ids[0].EntryNumber)

		// Check state after write
		shard.mu.RLock()
		if shard.state != nil {
			t.Logf("After write %d: State LastEntryNumber: %d", i, shard.state.GetLastEntryNumber())
		} else {
			t.Logf("After write %d: State is nil!", i)
		}
		shard.mu.RUnlock()
	}

	// Check shard state before reading
	shard.mu.RLock()
	t.Logf("Shard state before consumer read:")
	t.Logf("  Index CurrentEntryNumber: %d", shard.index.CurrentEntryNumber)
	t.Logf("  Index Files: %d", len(shard.index.Files))
	if len(shard.index.Files) > 0 {
		for i, f := range shard.index.Files {
			t.Logf("    File %d: %s (start=%d, entries=%d)", i, f.Path, f.StartEntry, f.Entries)
		}
	}
	if shard.state != nil {
		t.Logf("  State LastEntryNumber: %d", shard.state.GetLastEntryNumber())
	}
	shard.mu.RUnlock()

	// Create consumer and try to read
	consumer := NewConsumer(client, ConsumerOptions{Group: "test-group"})
	defer consumer.Close()

	messages, err := consumer.Read(ctx, []uint32{1}, 5)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Consumer.Read returned %d messages", len(messages))
	
	// Check consumer offset state after read
	shard.mu.RLock()
	t.Logf("Shard state after consumer read:")
	offset, exists := shard.index.ConsumerOffsets["test-group"]
	t.Logf("  Consumer offset: %d (exists: %v)", offset, exists)
	shard.mu.RUnlock()

	// Also try ScanAll to see if basic reading works
	var scanCount int
	err = client.ScanAll(ctx, streamName, func(ctx context.Context, msg StreamMessage) bool {
		scanCount++
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("ScanAll found %d messages", scanCount)
}