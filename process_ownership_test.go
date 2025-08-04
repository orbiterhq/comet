package comet

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

func TestProcessExclusiveShardOwnership(t *testing.T) {
	dir := t.TempDir()

	// Test 1: Process 0 should own shards 0, 2, 4...
	t.Run("Process0Ownership", func(t *testing.T) {
		config := DefaultCometConfig()
		// Multi-process mode is enabled by setting ProcessCount > 1
		config.Concurrency.ProcessID = 0
		config.Concurrency.ProcessCount = 2

		client, err := NewClientWithConfig(dir, config)
		if err != nil {
			t.Fatalf("failed to create client: %v", err)
		}
		defer client.Close()

		ctx := context.Background()

		// Should succeed - process 0 owns shard 0
		_, err = client.Append(ctx, "test:v1:shard:0000", [][]byte{[]byte("test")})
		if err != nil {
			t.Errorf("Process 0 should be able to write to shard 0: %v", err)
		}

		// Should succeed - process 0 owns shard 2
		_, err = client.Append(ctx, "test:v1:shard:0002", [][]byte{[]byte("test")})
		if err != nil {
			t.Errorf("Process 0 should be able to write to shard 2: %v", err)
		}

		// Should fail - process 0 doesn't own shard 1
		_, err = client.Append(ctx, "test:v1:shard:0001", [][]byte{[]byte("test")})
		if err == nil {
			t.Error("Process 0 should NOT be able to write to shard 1")
		}

		// Should fail - process 0 doesn't own shard 3
		_, err = client.Append(ctx, "test:v1:shard:0003", [][]byte{[]byte("test")})
		if err == nil {
			t.Error("Process 0 should NOT be able to write to shard 3")
		}
	})

	// Test 2: Process 1 should own shards 1, 3, 5...
	t.Run("Process1Ownership", func(t *testing.T) {
		config := DefaultCometConfig()
		// Multi-process mode is enabled by setting ProcessCount > 1
		config.Concurrency.ProcessID = 1
		config.Concurrency.ProcessCount = 2

		client, err := NewClientWithConfig(dir, config)
		if err != nil {
			t.Fatalf("failed to create client: %v", err)
		}
		defer client.Close()

		ctx := context.Background()

		// Should succeed - process 1 owns shard 1
		_, err = client.Append(ctx, "test:v1:shard:0001", [][]byte{[]byte("test")})
		if err != nil {
			t.Errorf("Process 1 should be able to write to shard 1: %v", err)
		}

		// Should succeed - process 1 owns shard 3
		_, err = client.Append(ctx, "test:v1:shard:0003", [][]byte{[]byte("test")})
		if err != nil {
			t.Errorf("Process 1 should be able to write to shard 3: %v", err)
		}

		// Should fail - process 1 doesn't own shard 0
		_, err = client.Append(ctx, "test:v1:shard:0000", [][]byte{[]byte("test")})
		if err == nil {
			t.Error("Process 1 should NOT be able to write to shard 0")
		}

		// Should fail - process 1 doesn't own shard 2
		_, err = client.Append(ctx, "test:v1:shard:0002", [][]byte{[]byte("test")})
		if err == nil {
			t.Error("Process 1 should NOT be able to write to shard 2")
		}
	})

	// Test 3: Reads should work from any process
	t.Run("CrossProcessReads", func(t *testing.T) {
		// First, write data with process 0
		config0 := DefaultCometConfig()
		config0.Concurrency.ProcessID = 0
		config0.Concurrency.ProcessCount = 2

		client0, err := NewClientWithConfig(dir, config0)
		if err != nil {
			t.Fatalf("failed to create client0: %v", err)
		}

		ctx := context.Background()
		_, err = client0.Append(ctx, "test:v1:shard:0000", [][]byte{[]byte("from process 0")})
		if err != nil {
			t.Fatalf("failed to write from process 0: %v", err)
		}
		client0.Close()

		// Write data with process 1
		config1 := DefaultCometConfig()
		config1.Concurrency.ProcessID = 1
		config1.Concurrency.ProcessCount = 2

		client1, err := NewClientWithConfig(dir, config1)
		if err != nil {
			t.Fatalf("failed to create client1: %v", err)
		}

		_, err = client1.Append(ctx, "test:v1:shard:0001", [][]byte{[]byte("from process 1")})
		if err != nil {
			t.Fatalf("failed to write from process 1: %v", err)
		}

		// Now process 1 should be able to read from shard 0 (owned by process 0)
		consumer := NewConsumer(client1, ConsumerOptions{Group: "test-cross-read"})
		messages, err := consumer.Read(ctx, []uint32{0}, 10)
		if err != nil {
			t.Fatalf("Process 1 should be able to read from shard 0: %v", err)
		}
		// Find the message we wrote
		found := false
		for _, msg := range messages {
			if string(msg.Data) == "from process 0" {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected to find 'from process 0' in messages, got %v", messages)
		}

		// Process 1 should also be able to read from its own shard
		messages, err = consumer.Read(ctx, []uint32{1}, 10)
		if err != nil {
			t.Fatalf("Process 1 should be able to read from shard 1: %v", err)
		}
		// Find the message we wrote
		found = false
		for _, msg := range messages {
			if string(msg.Data) == "from process 1" {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected to find 'from process 1' in messages, got %v", messages)
		}

		consumer.Close()
		client1.Close()
	})

	// Test 4: Verify no lock contention for owned shards
	t.Run("NoLockContentionForOwnedShards", func(t *testing.T) {
		config := DefaultCometConfig()
		// Multi-process mode is enabled by setting ProcessCount > 1
		config.Concurrency.ProcessID = 0
		config.Concurrency.ProcessCount = 2

		client, err := NewClientWithConfig(dir, config)
		if err != nil {
			t.Fatalf("failed to create client: %v", err)
		}
		defer client.Close()

		ctx := context.Background()

		// Write many entries concurrently to owned shards - should be fast with no lock contention
		var wg sync.WaitGroup
		errCh := make(chan error, 100)

		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				// Write to shard 0 (owned by process 0)
				data := fmt.Sprintf("entry %d", i)
				_, err := client.Append(ctx, "test:v1:shard:0000", [][]byte{[]byte(data)})
				if err != nil {
					errCh <- err
				}
			}(i)
		}

		wg.Wait()
		close(errCh)

		// Check for errors
		for err := range errCh {
			t.Errorf("Concurrent write to owned shard failed: %v", err)
		}

		// Verify all entries were written
		length, err := client.Len(ctx, "test:v1:shard:0000")
		if err != nil {
			t.Fatalf("Failed to get length: %v", err)
		}
		// Should have at least 100 entries (plus any from previous tests)
		if length < 100 {
			t.Errorf("Expected at least 100 entries, got %d", length)
		}
	})
}

func TestProcessOwnershipWithThreeProcesses(t *testing.T) {
	dir := t.TempDir()

	// Test with 3 processes
	// Process 0 owns: 0, 3, 6, 9...
	// Process 1 owns: 1, 4, 7, 10...
	// Process 2 owns: 2, 5, 8, 11...

	testCases := []struct {
		processID   int
		ownedShards []uint32
		notOwned    []uint32
	}{
		{0, []uint32{0, 3, 6, 9}, []uint32{1, 2, 4, 5}},
		{1, []uint32{1, 4, 7, 10}, []uint32{0, 2, 3, 5}},
		{2, []uint32{2, 5, 8, 11}, []uint32{0, 1, 3, 4}},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("Process%d", tc.processID), func(t *testing.T) {
			config := DefaultCometConfig()
			// Multi-process mode is enabled by setting ProcessCount > 1
			config.Concurrency.ProcessID = tc.processID
			config.Concurrency.ProcessCount = 3

			client, err := NewClientWithConfig(dir, config)
			if err != nil {
				t.Fatalf("failed to create client: %v", err)
			}
			defer client.Close()

			ctx := context.Background()

			// Test owned shards - writes should succeed
			for _, shardID := range tc.ownedShards {
				stream := fmt.Sprintf("test:v1:shard:%04d", shardID)
				_, err := client.Append(ctx, stream, [][]byte{[]byte(fmt.Sprintf("from process %d", tc.processID))})
				if err != nil {
					t.Errorf("Process %d should own shard %d but write failed: %v", tc.processID, shardID, err)
				}
			}

			// Test not owned shards - writes should fail
			for _, shardID := range tc.notOwned {
				stream := fmt.Sprintf("test:v1:shard:%04d", shardID)
				_, err := client.Append(ctx, stream, [][]byte{[]byte("should fail")})
				if err == nil {
					t.Errorf("Process %d should NOT own shard %d but write succeeded", tc.processID, shardID)
				}
			}
		})
	}
}
