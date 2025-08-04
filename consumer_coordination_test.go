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

// TestConsumerCoordination tests message loss in high-contention scenarios
func TestConsumerCoordination(t *testing.T) {
	dir := t.TempDir()
	config := MultiProcessConfig(0, 2)
	stream := "coord:v1:shard:0000"

	// Enable debug
	SetDebug(true)
	defer SetDebug(false)

	// Write 1000 messages
	messageCount := 1000
	t.Logf("=== Writing %d messages ===", messageCount)
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	var messages [][]byte
	for i := 0; i < messageCount; i++ {
		messages = append(messages, []byte(fmt.Sprintf("msg-%04d", i)))
	}

	ctx := context.Background()
	_, err = client.Append(ctx, stream, messages)
	if err != nil {
		t.Fatal(err)
	}
	client.Close()

	// Test high-contention scenario with 5 consumers in same group
	t.Logf("\n=== Testing 5 consumers in same group with rapid restarts ===")

	var wg sync.WaitGroup
	var totalProcessed atomic.Int64
	uniqueMessages := &sync.Map{}
	consumerStats := make([]int64, 5)

	// Run 5 consumers that will restart frequently
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(consumerID int) {
			defer wg.Done()

			localProcessed := int64(0)
			restarts := 0

			for restarts < 10 { // Each consumer will restart 10 times
				client, err := NewClientWithConfig(dir, config)
				if err != nil {
					t.Logf("Consumer %d: Failed to create client: %v", consumerID, err)
					return
				}

				consumer := NewConsumer(client, ConsumerOptions{
					Group: "shared-group",
				})

				// Check starting offset
				shard, _ := client.getOrCreateShard(0)
				shard.mu.RLock()
				startOffset := shard.index.ConsumerOffsets["shared-group"]
				shard.mu.RUnlock()

				// Process for a short time then restart
				ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)

				batchCount := 0
				consumer.Process(ctx, func(ctx context.Context, msgs []StreamMessage) error {
					batchCount++
					for _, msg := range msgs {
						msgStr := string(msg.Data)
						localProcessed++
						totalProcessed.Add(1)

						// Track unique messages and who processed them
						if firstConsumer, loaded := uniqueMessages.LoadOrStore(msgStr, consumerID); loaded {
							t.Logf("DUPLICATE: Consumer %d processed %s (first processed by %d)",
								consumerID, msgStr, firstConsumer)
						}
					}

					// Simulate work
					time.Sleep(10 * time.Millisecond)

					// Force restart after 3 batches
					if batchCount >= 3 {
						cancel()
					}

					return nil
				}, WithStream("coord:v1:shard:*"), WithBatchSize(20), WithAutoAck(true))

				// Check ending offset
				shard.mu.RLock()
				endOffset := shard.index.ConsumerOffsets["shared-group"]
				shard.mu.RUnlock()

				t.Logf("Consumer %d restart %d: processed from offset %d to %d",
					consumerID, restarts, startOffset, endOffset)

				consumer.Close()
				client.Close()
				cancel()

				// Check if all messages have been processed
				currentTotal := totalProcessed.Load()
				if currentTotal >= int64(messageCount) {
					break
				}

				restarts++
				time.Sleep(50 * time.Millisecond) // Brief pause between restarts
			}

			consumerStats[consumerID] = localProcessed
		}(i)
	}

	wg.Wait()

	// Analyze results
	t.Logf("\n=== COORDINATION TEST RESULTS ===")

	// Count unique messages
	uniqueCount := 0
	missingMessages := make([]string, 0)
	processedMap := make(map[string]bool)

	uniqueMessages.Range(func(key, value interface{}) bool {
		uniqueCount++
		processedMap[key.(string)] = true
		return true
	})

	// Find missing messages
	for i := 0; i < messageCount; i++ {
		msg := fmt.Sprintf("msg-%04d", i)
		if !processedMap[msg] {
			missingMessages = append(missingMessages, msg)
		}
	}

	t.Logf("Total messages written: %d", messageCount)
	t.Logf("Total processing attempts: %d", totalProcessed.Load())
	t.Logf("Unique messages processed: %d", uniqueCount)
	t.Logf("Messages lost: %d", len(missingMessages))

	for i, count := range consumerStats {
		t.Logf("Consumer %d processed: %d messages", i, count)
	}

	if len(missingMessages) > 0 {
		t.Errorf("CRITICAL: %d messages were never processed!", len(missingMessages))

		// Show first 10 missing messages
		showCount := 10
		if len(missingMessages) < showCount {
			showCount = len(missingMessages)
		}
		t.Logf("First %d missing messages: %v", showCount, missingMessages[:showCount])

		// Analyze pattern of missing messages
		if len(missingMessages) > 1 {
			// Check if they're consecutive
			consecutive := true
			for i := 1; i < len(missingMessages); i++ {
				prev := missingMessages[i-1]
				curr := missingMessages[i]
				var prevNum, currNum int
				fmt.Sscanf(prev, "msg-%d", &prevNum)
				fmt.Sscanf(curr, "msg-%d", &currNum)
				if currNum != prevNum+1 {
					consecutive = false
					break
				}
			}

			if consecutive {
				t.Logf("Missing messages are CONSECUTIVE - likely a range was skipped")
			} else {
				t.Logf("Missing messages are SCATTERED - likely a race condition")
			}
		}
	}

	// Check final offset
	finalClient, _ := NewClientWithConfig(dir, config)
	finalShard, _ := finalClient.getOrCreateShard(0)
	finalShard.mu.RLock()
	finalOffset := finalShard.index.ConsumerOffsets["shared-group"]
	finalShard.mu.RUnlock()
	finalClient.Close()

	t.Logf("Final consumer group offset: %d", finalOffset)

	if finalOffset < int64(messageCount) && len(missingMessages) > 0 {
		t.Logf("WARNING: Offset is %d but messages are missing - offset tracking is broken", finalOffset)
	}
}
