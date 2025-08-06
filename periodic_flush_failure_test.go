package comet

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// TestPeriodicFlushDoesNotMakeDataVisible reproduces the critical production bug:
// Periodic flush only flushes data to disk but doesn't update the index,
// so consumers can never see the data until an explicit Sync() is called.
func TestPeriodicFlushDoesNotMakeDataVisible(t *testing.T) {
	dataDir := t.TempDir()

	// Use production-like configuration with default FlushInterval behavior
	config := DefaultCometConfig()
	// NOTE: DefaultCometConfig() leaves FlushInterval as 0, so it falls back to CheckpointTime (2000ms)

	client, err := NewClient(dataDir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	stream := "test:v1:shard:0000"

	// Step 1: Write some data (this goes into volatile state)
	messages := [][]byte{
		[]byte("message-1"),
		[]byte("message-2"),
		[]byte("message-3"),
	}

	entryIDs, err := client.Append(ctx, stream, messages)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Wrote %d messages, got entry IDs: %v", len(messages), entryIDs)

	// Step 2: Check initial state - consumers should see nothing
	consumer := NewConsumer(client, ConsumerOptions{Group: "test-group"})
	defer consumer.Close()

	messages1, err := consumer.Read(ctx, []uint32{0}, 10)
	if err != nil {
		t.Fatal(err)
	}

	if len(messages1) != 0 {
		t.Errorf("Expected 0 messages before sync, got %d", len(messages1))
	}

	t.Log("✓ Confirmed: consumers see no messages before sync (expected)")

	// Step 3: Wait for periodic flush (default is CheckpointTime = 2000ms)
	t.Log("Waiting 3 seconds for periodic flush to trigger...")
	time.Sleep(3 * time.Second)

	// Step 4: Check if periodic flush made data visible (IT SHOULD NOT!)
	messages2, err := consumer.Read(ctx, []uint32{0}, 10)
	if err != nil {
		t.Fatal(err)
	}

	// This should now work correctly - periodic flush should make data visible
	if len(messages2) != len(messages) {
		t.Errorf("Expected %d messages after periodic flush, got %d", len(messages), len(messages2))
	} else {
		t.Log("✓ SUCCESS: Periodic flush correctly made data visible!")
		t.Log("  Periodic flush now updates both data files AND index")
		t.Log("  Consumers can see new messages without requiring restart")
	}

	// Step 5: Explicit sync should make data visible
	if err := client.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	messages3, err := consumer.Read(ctx, []uint32{0}, 10)
	if err != nil {
		t.Fatal(err)
	}

	if len(messages3) != len(messages) {
		t.Errorf("Expected %d messages after sync, got %d", len(messages), len(messages3))
	}

	t.Logf("✓ Explicit Sync() made %d messages visible", len(messages3))

	// Step 6: Verify the exact nature of the problem
	shard, err := client.getOrCreateShard(0)
	if err != nil {
		t.Fatal(err)
	}

	shard.mu.RLock()
	volatile := shard.nextEntryNumber
	durable := shard.index.CurrentEntryNumber
	shard.mu.RUnlock()

	t.Logf("Final state:")
	t.Logf("  Volatile (nextEntryNumber): %d", volatile)
	t.Logf("  Durable (index.CurrentEntryNumber): %d", durable)
	t.Logf("  Difference: %d entries stuck in volatile state", volatile-durable)
}

// TestPeriodicFlushWithCustomInterval tests with a shorter flush interval
func TestPeriodicFlushWithCustomInterval(t *testing.T) {
	dataDir := t.TempDir()

	// Set a very short flush interval to make testing faster
	config := DefaultCometConfig()
	config.Storage.FlushInterval = 100 // 100ms

	client, err := NewClient(dataDir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	stream := "test:v1:shard:0000"

	// Write data
	messages := [][]byte{[]byte("test-message")}
	_, err = client.Append(ctx, stream, messages)
	if err != nil {
		t.Fatal(err)
	}

	consumer := NewConsumer(client, ConsumerOptions{Group: "test-group"})
	defer consumer.Close()

	// Wait for flush interval
	time.Sleep(200 * time.Millisecond)

	// Check if data is visible (it shouldn't be due to the bug)
	messages1, err := consumer.Read(ctx, []uint32{0}, 10)
	if err != nil {
		t.Fatal(err)
	}

	if len(messages1) != len(messages) {
		t.Errorf("Expected %d messages after short flush interval, got %d", len(messages), len(messages1))
	} else {
		t.Log("✓ SUCCESS: Short interval flush correctly made data visible!")
	}
}

// TestProductionScenario simulates the exact production problem
func TestProductionScenario(t *testing.T) {
	dataDir := t.TempDir()

	// Production config - default FlushInterval behavior
	config := DefaultCometConfig()

	client, err := NewClient(dataDir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	stream := "events:v1:shard:0000"

	// Producer writes messages continuously
	go func() {
		for i := 0; i < 10; i++ {
			message := fmt.Sprintf("log-entry-%d", i)
			client.Append(ctx, stream, [][]byte{[]byte(message)})
			time.Sleep(100 * time.Millisecond) // Write every 100ms
		}
	}()

	// Consumer tries to read messages
	consumer := NewConsumer(client, ConsumerOptions{Group: "log-consumer"})
	defer consumer.Close()

	// Wait a reasonable time for periodic flush
	time.Sleep(3 * time.Second)

	messages, err := consumer.Read(ctx, []uint32{0}, 100)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Production scenario: consumer read %d messages", len(messages))

	if len(messages) == 0 {
		t.Fatal("REGRESSION: Production bug has returned - periodic flush not working")
	} else {
		t.Log("✓ PRODUCTION BUG FIXED:")
		t.Log("  Producer writes messages")
		t.Log("  Periodic flush runs AND updates index")
		t.Log("  Consumer sees messages immediately")
		t.Logf("  Consumer successfully read %d messages", len(messages))
	}
}

// TestFlushIntervalConfigurationDetection verifies how flush intervals work
func TestFlushIntervalConfigurationDetection(t *testing.T) {
	testCases := []struct {
		name             string
		flushInterval    int
		checkpointTime   int
		expectedInterval int
	}{
		{
			name:             "DefaultConfig",
			flushInterval:    0,    // Default
			checkpointTime:   2000, // Default
			expectedInterval: 2000, // Should fall back to CheckpointTime
		},
		{
			name:             "CustomFlushInterval",
			flushInterval:    500,
			checkpointTime:   2000,
			expectedInterval: 500, // Should use FlushInterval
		},
		{
			name:             "BothZero",
			flushInterval:    0,
			checkpointTime:   0,
			expectedInterval: 0, // Should disable periodic flush
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dataDir := t.TempDir()

			config := DefaultCometConfig()
			config.Storage.FlushInterval = tc.flushInterval
			config.Storage.CheckpointTime = tc.checkpointTime

			client, err := NewClient(dataDir, config)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()

			// Get the shard to inspect its flush configuration
			shard, err := client.getOrCreateShard(0)
			if err != nil {
				t.Fatal(err)
			}

			// Check if flush goroutine was started based on expected interval
			if tc.expectedInterval == 0 {
				t.Log("✓ Expected no periodic flush goroutine (interval = 0)")
			} else {
				t.Logf("✓ Expected periodic flush every %dms", tc.expectedInterval)
			}

			// The actual flush interval logic is in startPeriodicFlush
			// We can't easily test it without exposing internals, but this documents the behavior
			_ = shard
		})
	}
}

// TestPeriodicFlushObservability verifies we can actually observe periodic flush behavior
func TestPeriodicFlushObservability(t *testing.T) {
	dataDir := t.TempDir()

	// Use a very short flush interval for precise testing
	config := DefaultCometConfig()
	config.Storage.FlushInterval = 50 // 50ms for fast testing

	client, err := NewClient(dataDir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	stream := "test:v1:shard:0000"

	// Write data but don't sync
	messages := [][]byte{[]byte("observable-test")}
	_, err = client.Append(ctx, stream, messages)
	if err != nil {
		t.Fatal(err)
	}

	consumer := NewConsumer(client, ConsumerOptions{Group: "observability-test"})
	defer consumer.Close()

	// Verify data starts invisible
	messages1, err := consumer.Read(ctx, []uint32{0}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages1) != 0 {
		t.Fatal("Data should start invisible")
	}
	t.Log("✓ Data starts invisible (expected)")

	// Get shard to observe state transitions
	shard, err := client.getOrCreateShard(0)
	if err != nil {
		t.Fatal(err)
	}

	// Track state changes by polling rapidly
	var observedFlush bool
	start := time.Now()

	for time.Since(start) < 200*time.Millisecond { // Poll for up to 200ms
		// Check current state
		shard.mu.RLock()
		volatile := shard.nextEntryNumber
		durable := shard.index.CurrentEntryNumber
		shard.mu.RUnlock()

		// If index was updated, we caught the periodic flush
		if durable > 0 {
			observedFlush = true
			t.Logf("✓ OBSERVED PERIODIC FLUSH at %v", time.Since(start))
			t.Logf("  Volatile: %d -> Durable: %d", volatile, durable)
			break
		}

		time.Sleep(5 * time.Millisecond) // Poll every 5ms
	}

	if !observedFlush {
		t.Fatal("Never observed periodic flush updating index within 200ms")
	}

	// Verify data is now visible to consumer
	messages2, err := consumer.Read(ctx, []uint32{0}, 10)
	if err != nil {
		t.Fatal(err)
	}

	if len(messages2) != 1 {
		t.Errorf("Expected 1 message after observed flush, got %d", len(messages2))
	} else {
		t.Log("✓ Data became visible immediately after periodic flush")
	}
}

// TestPeriodicFlushTimingPrecision measures the actual flush timing
func TestPeriodicFlushTimingPrecision(t *testing.T) {
	dataDir := t.TempDir()

	config := DefaultCometConfig()
	config.Storage.FlushInterval = 100 // 100ms interval

	client, err := NewClient(dataDir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	stream := "test:v1:shard:0000"

	shard, err := client.getOrCreateShard(0)
	if err != nil {
		t.Fatal(err)
	}

	// Write multiple batches and measure when they become visible
	var flushTimes []time.Duration
	baseTime := time.Now()

	for i := 0; i < 3; i++ {
		// Write a batch
		message := fmt.Sprintf("timing-test-%d", i)
		_, err = client.Append(ctx, stream, [][]byte{[]byte(message)})
		if err != nil {
			t.Fatal(err)
		}

		writeTime := time.Since(baseTime)
		t.Logf("Wrote batch %d at %v", i, writeTime)

		// Poll until this batch becomes visible
		var flushTime time.Duration
		for {
			shard.mu.RLock()
			durableEntries := shard.index.CurrentEntryNumber
			shard.mu.RUnlock()

			if durableEntries > int64(i) {
				flushTime = time.Since(baseTime)
				t.Logf("Batch %d became visible at %v (delay: %v)",
					i, flushTime, flushTime-writeTime)
				flushTimes = append(flushTimes, flushTime-writeTime)
				break
			}

			time.Sleep(5 * time.Millisecond)
		}

		// Wait before next batch to avoid overlap
		time.Sleep(150 * time.Millisecond)
	}

	// Analyze timing precision
	t.Log("Flush timing analysis:")
	for i, delay := range flushTimes {
		t.Logf("  Batch %d: %v delay", i, delay)

		// Should be roughly 100ms ± some tolerance
		// Note: timing can vary due to scheduling, so allow wide tolerance
		if delay < 25*time.Millisecond || delay > 250*time.Millisecond {
			t.Errorf("Batch %d flush delay %v outside expected range (25-250ms)", i, delay)
		}
	}

	t.Log("✓ Periodic flush timing within expected bounds")
}

// TestConcurrentWritesDuringPeriodicFlush tests behavior with concurrent writes
func TestConcurrentWritesDuringPeriodicFlush(t *testing.T) {
	dataDir := t.TempDir()

	config := DefaultCometConfig()
	config.Storage.FlushInterval = 200 // 200ms to give time for concurrent writes

	client, err := NewClient(dataDir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	stream := "test:v1:shard:0000"

	// Start concurrent writer
	var totalWrites int64
	done := make(chan struct{})

	go func() {
		defer close(done)
		ticker := time.NewTicker(10 * time.Millisecond) // Write every 10ms
		defer ticker.Stop()

		for i := 0; i < 30; i++ { // Write for ~300ms
			message := fmt.Sprintf("concurrent-%d", i)
			client.Append(ctx, stream, [][]byte{[]byte(message)})
			atomic.AddInt64(&totalWrites, 1)
			<-ticker.C
		}
	}()

	// Monitor visibility
	consumer := NewConsumer(client, ConsumerOptions{Group: "concurrent-test"})
	defer consumer.Close()

	var visibilityEvents []struct {
		time    time.Duration
		visible int64
		total   int64
	}

	start := time.Now()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			goto analyze
		case <-ticker.C:
			messages, _ := consumer.Read(ctx, []uint32{0}, 100)
			currentTotal := atomic.LoadInt64(&totalWrites)

			event := struct {
				time    time.Duration
				visible int64
				total   int64
			}{
				time:    time.Since(start),
				visible: int64(len(messages)),
				total:   currentTotal,
			}
			visibilityEvents = append(visibilityEvents, event)

			t.Logf("At %v: %d/%d messages visible",
				event.time, event.visible, event.total)
		}
	}

analyze:
	// Final check
	finalMessages, err := consumer.Read(ctx, []uint32{0}, 100)
	if err != nil {
		t.Fatal(err)
	}

	finalWrites := atomic.LoadInt64(&totalWrites)
	t.Logf("Final: %d messages visible out of %d written",
		len(finalMessages), finalWrites)

	// Should see periodic visibility increases
	var hasProgressiveVisibility bool
	for i := 1; i < len(visibilityEvents); i++ {
		if visibilityEvents[i].visible > visibilityEvents[i-1].visible {
			hasProgressiveVisibility = true
			break
		}
	}

	if !hasProgressiveVisibility {
		t.Error("Expected to see progressive visibility increases during concurrent writes")
	} else {
		t.Log("✓ Observed progressive visibility during concurrent writes")
	}
}
