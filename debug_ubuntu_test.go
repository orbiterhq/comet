package comet

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// TestUbuntuMultiProcessDebug is an isolated test to debug Ubuntu CI failures
func TestUbuntuMultiProcessDebug(t *testing.T) {
	dir := t.TempDir()
	streamName := "debug:v1:shard:0001"

	t.Logf("=== TEST START: dir=%s, pid=%d ===", dir, os.Getpid())

	// Create config with explicit settings
	config := MultiProcessConfig()
	config.Indexing.BoundaryInterval = 10 // More frequent indexing

	// Process 1: Create initial client and write data
	t.Log("=== PROCESS 1: Creating initial client ===")
	client1, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("Failed to create client1: %v", err)
	}

	// Log initial state
	shard1, _ := client1.getOrCreateShard(1)
	if state := shard1.loadState(); state != nil {
		t.Logf("PROCESS 1 - Initial state: Version=%d, LastEntry=%d, TotalEntries=%d",
			atomic.LoadUint64(&state.Version),
			atomic.LoadInt64(&state.LastEntryNumber),
			atomic.LoadInt64(&state.TotalEntries))
	}

	// Write 5 entries
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		data := []byte(fmt.Sprintf(`{"process": 1, "seq": %d}`, i))
		t.Logf("PROCESS 1 - Writing entry %d", i)
		_, err := client1.Append(ctx, streamName, [][]byte{data})
		if err != nil {
			t.Fatalf("Process 1 failed to write entry %d: %v", i, err)
		}
	}

	// Force sync and log state
	client1.Sync(ctx)
	shard1.mu.Lock()
	shard1.persistIndex()
	t.Logf("PROCESS 1 - After writes: Files=%d, CurrentEntryNumber=%d",
		len(shard1.index.Files), shard1.index.CurrentEntryNumber)
	for i, f := range shard1.index.Files {
		t.Logf("  File[%d]: %s, entries=%d, start=%d", i, f.Path, f.Entries, f.StartEntry)
	}
	shard1.mu.Unlock()

	if state := shard1.loadState(); state != nil {
		t.Logf("PROCESS 1 - Final state: LastEntry=%d, TotalEntries=%d, TotalWrites=%d",
			atomic.LoadInt64(&state.LastEntryNumber),
			atomic.LoadInt64(&state.TotalEntries),
			atomic.LoadUint64(&state.TotalWrites))
	}

	client1.Close()

	// Small delay to ensure file system sync
	time.Sleep(100 * time.Millisecond)

	// Process 2: Create new client and verify it can see Process 1's data
	t.Log("=== PROCESS 2: Creating new client ===")
	client2, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("Failed to create client2: %v", err)
	}
	defer client2.Close()

	// Check what Process 2 sees
	shard2, _ := client2.getOrCreateShard(1)
	if state := shard2.loadState(); state != nil {
		t.Logf("PROCESS 2 - Initial state: Version=%d, LastEntry=%d, TotalEntries=%d",
			atomic.LoadUint64(&state.Version),
			atomic.LoadInt64(&state.LastEntryNumber),
			atomic.LoadInt64(&state.TotalEntries))
	}

	// Check index before any operations
	shard2.mu.RLock()
	t.Logf("PROCESS 2 - Initial index: Files=%d, CurrentEntryNumber=%d",
		len(shard2.index.Files), shard2.index.CurrentEntryNumber)
	shard2.mu.RUnlock()

	// Use Len to trigger any rebuild if needed
	length, err := client2.Len(ctx, streamName)
	if err != nil {
		t.Fatalf("Process 2 failed to get length: %v", err)
	}
	t.Logf("PROCESS 2 - Stream length: %d (expected 5)", length)

	// Check index after Len
	shard2.mu.RLock()
	t.Logf("PROCESS 2 - After Len: Files=%d, CurrentEntryNumber=%d",
		len(shard2.index.Files), shard2.index.CurrentEntryNumber)
	for i, f := range shard2.index.Files {
		t.Logf("  File[%d]: %s, entries=%d, start=%d", i, f.Path, f.Entries, f.StartEntry)
	}
	shard2.mu.RUnlock()

	if length != 5 {
		t.Errorf("Process 2 sees %d entries, expected 5", length)

		// Debug: Check what files exist on disk
		shardDir := filepath.Join(dir, "shard-0001")
		entries, _ := os.ReadDir(shardDir)
		t.Logf("Files in shard directory:")
		for _, e := range entries {
			info, _ := e.Info()
			t.Logf("  %s (size=%d)", e.Name(), info.Size())
		}
	}
}

// TestUbuntuTailDebug isolates the Tail issue
func TestUbuntuTailDebug(t *testing.T) {
	dir := t.TempDir()
	streamName := "tail:v1:shard:0001"

	t.Logf("=== TAIL TEST START: dir=%s ===", dir)

	config := MultiProcessConfig()
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Write initial data
	for i := 0; i < 3; i++ {
		data := []byte(fmt.Sprintf(`{"initial": %d}`, i))
		_, err := client.Append(ctx, streamName, [][]byte{data})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Start tailing
	received := make(chan StreamMessage, 10)
	tailErr := make(chan error, 1)

	go func() {
		t.Log("TAIL: Starting tail goroutine")
		err := client.Tail(ctx, streamName, func(ctx context.Context, msg StreamMessage) error {
			t.Logf("TAIL: Received message: entry=%d, data=%s", msg.ID.EntryNumber, string(msg.Data))
			received <- msg
			return nil
		})
		tailErr <- err
	}()

	// Give tail time to start
	time.Sleep(300 * time.Millisecond)

	// Write new data
	t.Log("TAIL: Writing new entries")
	for i := 0; i < 3; i++ {
		data := []byte(fmt.Sprintf(`{"new": %d}`, i))
		t.Logf("TAIL: Writing entry %d", i)
		_, err := client.Append(ctx, streamName, [][]byte{data})
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Wait for messages
	time.Sleep(500 * time.Millisecond)

	// Cancel context to stop tail
	cancel()

	// Wait for tail to finish
	select {
	case <-tailErr:
		// Tail finished
	case <-time.After(1 * time.Second):
		t.Log("TAIL: Warning - tail didn't finish within 1 second")
	}

	// Check results
	receivedCount := len(received)
	t.Logf("TAIL: Received %d messages (expected 3)", receivedCount)

	if receivedCount != 3 {
		// Debug shard state
		shard, _ := client.getOrCreateShard(1)
		shard.mu.RLock()
		t.Logf("TAIL DEBUG: Files=%d, CurrentEntryNumber=%d",
			len(shard.index.Files), shard.index.CurrentEntryNumber)
		for i, f := range shard.index.Files {
			t.Logf("  File[%d]: %s, entries=%d, start=%d", i, f.Path, f.Entries, f.StartEntry)
		}
		shard.mu.RUnlock()
	}
}
