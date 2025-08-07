package comet

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestRealtimeBulletproof tests 100% realtime performance with graceful shutdown
// Uses goroutines with separate clients to simulate cross-process behavior
func TestRealtimeBulletproof(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping bulletproof test in short mode")
	}

	dir := t.TempDir()

	// Real-world production configuration - no cheating!
	cfg := DefaultCometConfig()
	cfg.Storage.FlushInterval = 1000     // 1 second flush (production default)
	cfg.Storage.CheckpointInterval = 2000 // 2 second checkpoint (production default)

	ctx := context.Background()
	// Use fewer shards to ensure each shard gets more messages
	// With 600 messages across 4 shards = 150 messages per shard
	// This ensures more consistent flushing
	shards := make([]string, 4)
	for i := 0; i < 4; i++ {
		shards[i] = fmt.Sprintf("events:v1:shard:%04d", i)
	}

	// Realistic production test parameters - longer duration, higher message volume
	testDuration := 30 * time.Second                      // 30 seconds of sustained writing
	writeInterval := 50 * time.Millisecond                // Write every 50ms (high throughput: ~20/sec)
	expectedWrites := int64(testDuration / writeInterval) // ~600 writes

	var writtenCount int64
	var readCount int64
	var wg sync.WaitGroup

	// Coordination channels
	writerDone := make(chan struct{})

	t.Logf("Starting bulletproof test: duration=%v, writeInterval=%v, expectedWrites=%d",
		testDuration, writeInterval, expectedWrites)

	// Writer goroutine with graceful shutdown
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(writerDone) // Signal when writer is completely done

		writerClient, err := NewClient(dir, cfg)
		if err != nil {
			t.Errorf("Failed to create writer client: %v", err)
			return
		}
		// Note: We explicitly close the client during graceful shutdown, no defer needed

		timeout := time.After(testDuration)
		ticker := time.NewTicker(writeInterval)
		defer ticker.Stop()

		for {
			select {
			case <-timeout:
				// Graceful shutdown - ensure final flush happens
				written := atomic.LoadInt64(&writtenCount)
				t.Logf("[WRITER] Stopping after %d writes, waiting for final flush cycles...", written)
				
				// Wait for TWO full flush cycles to ensure all messages are flushed
				// First cycle: flush any messages written just before timeout
				// Second cycle: ensure everything is visible 
				// This is the real-world approach - let the automatic flush handle it
				time.Sleep(2200 * time.Millisecond) // Just over 2 seconds to ensure 2 flushes happen

				// Now sync to make everything durable
				if err := writerClient.Sync(ctx); err != nil {
					t.Logf("[WRITER] Warning: Failed to sync: %v", err)
				}
				
				if err := writerClient.Close(); err != nil {
					t.Logf("[WRITER] Warning: Failed to close: %v", err)
				}

				// Give time for memory-mapped state updates to be visible across processes
				// This is critical for cross-process coordination
				time.Sleep(1 * time.Second)

				t.Logf("[WRITER] All shards synced. Final total written: %d", written)
				
				// Debug: Check what's actually in the indexes after close
				t.Logf("[WRITER] Checking shard states after close...")
				for i := 0; i < len(shards); i++ {
					shard := writerClient.shards[uint32(i)]
					if shard != nil {
						t.Logf("[WRITER] Shard %d: nextEntryNumber=%d, index.CurrentEntryNumber=%d", 
							i, shard.nextEntryNumber, shard.index.CurrentEntryNumber)
					}
				}
				
				// Immediately check what the consumer can see after close
				finalRead := atomic.LoadInt64(&readCount)
				t.Logf("[WRITER] Consumer has read %d messages immediately after writer close", finalRead)
				return

			case <-ticker.C:
				count := atomic.AddInt64(&writtenCount, 1)
				msg := fmt.Sprintf("event-%d-%d", time.Now().UnixMilli(), count)

				// Write to different shards in round-robin fashion to test lazy loading
				shardIndex := (count - 1) % int64(len(shards))
				targetShard := shards[shardIndex]

				// Production write - no manual sync
				_, err := writerClient.Append(ctx, targetShard, [][]byte{[]byte(msg)})
				if err != nil {
					t.Errorf("Write failed: %v", err)
					continue
				}

				if count%100 == 0 {
					t.Logf("[WRITER] Progress: %d writes across %d shards", count, len(shards))
				}
			}
		}
	}()

	// Give writer time to start and create initial data
	time.Sleep(1 * time.Second)

	// Consumer goroutine with proper coordination
	wg.Add(1)
	go func() {
		defer wg.Done()

		consumerClient, err := NewClient(dir, cfg)
		if err != nil {
			t.Errorf("Failed to create consumer client: %v", err)
			return
		}
		defer consumerClient.Close()

		consumer := NewConsumer(consumerClient, ConsumerOptions{
			Group: "bulletproof-test",
		})

		// Consumer context that gets cancelled when writer finishes + catch-up time
		consumerCtx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Wait for writer to finish, then give consumer enough time to capture final flushes
		// With 1s flush intervals, we need at least 2-3 seconds for final messages
		go func() {
			<-writerDone
			startCatchup := atomic.LoadInt64(&readCount)
			t.Logf("[CONSUMER] Writer finished, consumer at %d reads, giving 15 seconds to capture final flushes...", startCatchup)
			
			// Monitor progress during catch-up
			for i := 0; i < 15; i++ {
				time.Sleep(1 * time.Second)
				current := atomic.LoadInt64(&readCount)
				if current > startCatchup {
					t.Logf("[CONSUMER] Catch-up progress: %d messages captured (total: %d)", current-startCatchup, current)
				}
			}
			
			finalCatchup := atomic.LoadInt64(&readCount)
			t.Logf("[CONSUMER] Catch-up complete: captured %d additional messages (total: %d)", finalCatchup-startCatchup, finalCatchup)
			cancel() // Stop the consumer
		}()

		err = consumer.Process(consumerCtx, func(ctx context.Context, msgs []StreamMessage) error {
			if len(msgs) > 0 {
				// Count all messages in this batch
				batchSize := int64(len(msgs))
				newTotal := atomic.AddInt64(&readCount, batchSize)

				currentWritten := atomic.LoadInt64(&writtenCount)

				// Log progress periodically to avoid spam
				if newTotal%100 == 0 || batchSize >= 10 {
					t.Logf("[CONSUMER] Read batch: +%d messages, total=%d, written=%d, lag=%d",
						batchSize, newTotal, currentWritten, currentWritten-newTotal)
				}
			}
			return nil
		}, WithStream("events:v1:shard:*"), // Consumer watches all shards
			WithBatchSize(1),                      // Small batch size to ensure fair shard reading
			WithPollInterval(100*time.Millisecond)) // Normal production polling interval

		if err != nil && err != context.DeadlineExceeded && err != context.Canceled {
			t.Errorf("Consumer process error: %v", err)
		}

		final := atomic.LoadInt64(&readCount)
		t.Logf("[CONSUMER] Finished with %d reads", final)
	}()

	// Wait for both to complete
	wg.Wait()

	// Analyze results
	written := atomic.LoadInt64(&writtenCount)
	read := atomic.LoadInt64(&readCount)

	t.Logf("\n=== BULLETPROOF TEST RESULTS ===")
	t.Logf("Duration: %.1fs", testDuration.Seconds())
	t.Logf("Shards: %d (lazy loaded by consumer)", len(shards))
	t.Logf("Messages written: %d (expected ~%d)", written, expectedWrites)
	t.Logf("Messages read: %d", read)
	t.Logf("Realtime performance: %.1f%%", float64(read)*100/float64(written))

	// Expectations for 100% realtime performance
	if written < expectedWrites*90/100 {
		t.Errorf("Writer underperformed: expected ~%d, got %d", expectedWrites, written)
	}

	// With 1-second flush intervals and proper catch-up time, we MUST achieve 100%
	// The 10-second catch-up period should be more than enough to capture all messages
	if read < written {
		t.Errorf("Failed 100%% capture test: read %d/%d (%.1f%%), expected 100%% with proper catch-up",
			read, written, float64(read)*100/float64(written))
	}

	// Success criteria - expect 100% with proper catch-up time
	if read >= written {
		t.Logf("✅ PRODUCTION 100%% SUCCESS: %d/%d messages captured with 1s flush intervals",
			read, written)
	} else {
		t.Logf("⚠️  PERFORMANCE: %.1f%% capture rate - may need more catch-up time",
			float64(read)*100/float64(written))
	}
}
