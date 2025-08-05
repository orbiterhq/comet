//go:build integration

package comet

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestACKDebug tests ACK persistence with detailed logging
func TestACKDebug(t *testing.T) {
	dir := t.TempDir()

	// Enable debug logging
	SetDebug(true)
	defer SetDebug(false)

	// Step 1: Write messages to multiple shards using default config
	streams := []string{
		"debug:v1:shard:0000",
		"debug:v1:shard:0001",
	}

	t.Logf("=== Writing 20 messages across %d shards ===", len(streams))
	client, err := NewClient(dir, DefaultCometConfig())
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	messagesPerShard := 10 // 20 messages total, 10 per shard

	for shardIdx, stream := range streams {
		var messages [][]byte
		for i := 0; i < messagesPerShard; i++ {
			msgNum := shardIdx*messagesPerShard + i
			messages = append(messages, []byte(fmt.Sprintf("msg-%02d", msgNum)))
		}
		_, err = client.Append(ctx, stream, messages)
		if err != nil {
			t.Fatal(err)
		}
	}
	client.Close()

	// Step 2: Simulate multiple consumers with restarts
	t.Logf("\n=== Running 2 consumers with frequent restarts ===")

	// Track what each consumer sees
	type ConsumerEvent struct {
		ConsumerID int
		SessionID  int
		Event      string
		Message    string
		Offset     int64
		Time       time.Time
	}

	var events []ConsumerEvent
	var eventsMu sync.Mutex

	logEvent := func(consumerID, sessionID int, event, message string, offset int64) {
		eventsMu.Lock()
		events = append(events, ConsumerEvent{
			ConsumerID: consumerID,
			SessionID:  sessionID,
			Event:      event,
			Message:    message,
			Offset:     offset,
			Time:       time.Now(),
		})
		eventsMu.Unlock()
		t.Logf("[C%d-S%d] %s: %s (offset=%d)", consumerID, sessionID, event, message, offset)
	}

	// Run consumers
	var wg sync.WaitGroup
	processedCount := &sync.Map{}

	for consumerID := 0; consumerID < 2; consumerID++ {
		wg.Add(1)
		go func(cID int) {
			defer wg.Done()

			// Each consumer runs 3 short sessions with its own process ID
			for sessionID := 0; sessionID < 3; sessionID++ {
				// Each consumer gets its own process ID to avoid race conditions
				processConfig := DeprecatedMultiProcessConfig(cID, 2)
				client, err := NewClient(dir, processConfig)
				if err != nil {
					logEvent(cID, sessionID, "ERROR", fmt.Sprintf("Failed to create client: %v", err), -1)
					return
				}

				// Determine which shard this consumer owns
				ownedShard := uint32(cID) // Consumer 0 owns shard 0, Consumer 1 owns shard 1

				// Check initial offset
				shard, _ := client.getOrCreateShard(ownedShard)
				shard.mu.RLock()
				initialOffset := shard.index.ConsumerOffsets[fmt.Sprintf("consumer-%d", cID)]
				shard.mu.RUnlock()
				logEvent(cID, sessionID, "START", fmt.Sprintf("Initial offset: %d", initialOffset), initialOffset)

				consumer := NewConsumer(client, ConsumerOptions{
					Group: fmt.Sprintf("consumer-%d", cID),
				})

				messagesInSession := 0
				ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)

				processFunc := func(ctx context.Context, msgs []StreamMessage) error {
					for _, msg := range msgs {
						msgStr := string(msg.Data)
						logEvent(cID, sessionID, "PROCESS", msgStr, msg.ID.EntryNumber)

						// Track duplicates
						key := fmt.Sprintf("%d-%s", cID, msgStr)
						if count, _ := processedCount.LoadOrStore(key, int32(0)); count != nil {
							oldCount := count.(int32)
							newCount := atomic.AddInt32(&oldCount, 1)
							processedCount.Store(key, newCount)
							if newCount > 1 {
								logEvent(cID, sessionID, "DUPLICATE", fmt.Sprintf("%s (count=%d)", msgStr, newCount), msg.ID.EntryNumber)
							}
						}
					}

					messagesInSession += len(msgs)

					// Process only 5 messages per session
					if messagesInSession >= 5 {
						logEvent(cID, sessionID, "STOPPING", fmt.Sprintf("Processed %d messages", messagesInSession), -1)
						cancel()
					}

					return nil
				}

				// Process messages only from owned shard
				err = consumer.Process(ctx, processFunc,
					WithShards(ownedShard),
					WithBatchSize(3),
					WithAutoAck(true),
					WithPollInterval(50*time.Millisecond),
				)

				// Check final offset before closing
				shard.mu.RLock()
				finalOffset := shard.index.ConsumerOffsets[fmt.Sprintf("consumer-%d", cID)]
				shard.mu.RUnlock()
				logEvent(cID, sessionID, "PRE-CLOSE", fmt.Sprintf("Offset before close: %d", finalOffset), finalOffset)

				// Close consumer
				consumer.Close()
				client.Close()

				// Check persisted offset
				client2, _ := NewClient(dir, processConfig)
				shard2, _ := client2.getOrCreateShard(ownedShard)
				shard2.mu.RLock()
				persistedOffset := shard2.index.ConsumerOffsets[fmt.Sprintf("consumer-%d", cID)]
				shard2.mu.RUnlock()
				client2.Close()

				logEvent(cID, sessionID, "PERSISTED", fmt.Sprintf("Offset after close: %d", persistedOffset), persistedOffset)

				// Wait between sessions
				time.Sleep(100 * time.Millisecond)
			}
		}(consumerID)
	}

	wg.Wait()

	// Analyze results
	t.Logf("\n=== ANALYSIS ===")

	// Count duplicates per consumer
	duplicates := make(map[int]int)
	processedCount.Range(func(key, value interface{}) bool {
		k := key.(string)
		count := value.(int32)
		var consumerID int
		fmt.Sscanf(k, "%d-", &consumerID)
		if count > 1 {
			duplicates[consumerID] += int(count - 1)
		}
		return true
	})

	for cID := 0; cID < 2; cID++ {
		t.Logf("Consumer %d: %d duplicates", cID, duplicates[cID])
	}

	// Print timeline
	t.Logf("\n=== TIMELINE ===")
	for _, e := range events {
		t.Logf("%s [C%d-S%d] %s: %s (offset=%d)",
			e.Time.Format("15:04:05.000"), e.ConsumerID, e.SessionID, e.Event, e.Message, e.Offset)
	}

	// Final check
	totalDuplicates := 0
	for _, d := range duplicates {
		totalDuplicates += d
	}

	if totalDuplicates > 0 {
		t.Errorf("FAIL: Found %d total duplicates across all consumers", totalDuplicates)
	}
}
