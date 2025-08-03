// +build integration

package comet

import (
	"context"
	"testing"
)

func init() {
	Debug = true
}

// TestSimpleWrite - minimal test to understand write path
func TestSimpleWrite(t *testing.T) {
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

	t.Logf("=== BEFORE ANY WRITES ===")
	
	// Get shard and check initial state
	shard, err := client.getOrCreateShard(1)
	if err != nil {
		t.Fatal(err)
	}
	
	shard.mu.RLock()
	if shard.state != nil {
		t.Logf("Initial state: LastEntryNumber=%d, ptr=%p", 
			shard.state.GetLastEntryNumber(), shard.state)
	} else {
		t.Logf("Initial state: nil")
	}
	t.Logf("Initial index: CurrentEntryNumber=%d", shard.index.CurrentEntryNumber)
	shard.mu.RUnlock()

	t.Logf("=== PERFORMING SINGLE WRITE ===")
	
	// Single write with tracing
	ids, err := client.Append(ctx, streamName, [][]byte{[]byte("test message")})
	if err != nil {
		t.Fatal(err)
	}
	
	t.Logf("Write returned ID: %d", ids[0].EntryNumber)

	t.Logf("=== AFTER WRITE ===")
	
	shard.mu.RLock()
	if shard.state != nil {
		t.Logf("Final state: LastEntryNumber=%d, ptr=%p", 
			shard.state.GetLastEntryNumber(), shard.state)
	} else {
		t.Logf("Final state: nil")
	}
	t.Logf("Final index: CurrentEntryNumber=%d", shard.index.CurrentEntryNumber)
	shard.mu.RUnlock()
}