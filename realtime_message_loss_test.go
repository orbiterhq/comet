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

// TestRealtimeMessageLoss simulates your production scenario to identify message loss
func TestRealtimeMessageLoss(t *testing.T) {
	dir := t.TempDir()
	config := DefaultCometConfig()
	config.Storage.FlushInterval = 100 * time.Millisecond // Match your production setting

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Single writer client (simulating your API)
	writer, err := NewClient(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()

	// Single reader client (simulating your processor)
	reader, err := NewClient(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	// Track messages
	type messageInfo struct {
		id        int
		writeTime time.Time
		seenTime  time.Time
		data      string
	}

	var messagesMu sync.Mutex
	messages := make(map[int]*messageInfo)
	var totalWritten atomic.Int64
	var totalSeen atomic.Int64

	// Consumer setup
	consumer := NewConsumer(reader, ConsumerOptions{
		Group: "realtime-processor",
	})
	defer consumer.Close()

	// Start consumer BEFORE any writes
	consumerErrors := make(chan error, 100)
	go func() {
		// Pre-specify shards we expect (0-255 like your setup)
		shardIDs := make([]uint32, 256)
		for i := range shardIDs {
			shardIDs[i] = uint32(i)
		}

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			// Read from all shards
			msgs, err := consumer.Read(ctx, shardIDs, 100)
			if err != nil {
				consumerErrors <- err
				time.Sleep(10 * time.Millisecond)
				continue
			}

			if len(msgs) == 0 {
				time.Sleep(10 * time.Millisecond)
				continue
			}

			// Process messages
			for _, msg := range msgs {
				var id int
				var writeTimeStr string
				fmt.Sscanf(string(msg.Data), "msg-%d-time-%s", &id, &writeTimeStr)
				writeTime, _ := time.Parse(time.RFC3339Nano, writeTimeStr)

				messagesMu.Lock()
				if info, exists := messages[id]; exists {
					info.seenTime = time.Now()
					latency := info.seenTime.Sub(writeTime)
					if latency > 500*time.Millisecond {
						t.Logf("HIGH LATENCY: Message %d took %v", id, latency)
					}
				} else {
					t.Errorf("UNKNOWN MESSAGE: Consumer saw message %d that wasn't tracked!", id)
				}
				messagesMu.Unlock()

				totalSeen.Add(1)

				// ACK the message
				if err := consumer.Ack(ctx, msg.ID); err != nil {
					t.Errorf("ACK error: %v", err)
				}
			}
		}
	}()

	// Simulate production traffic pattern
	t.Log("Starting simulated production traffic...")

	// Pattern 1: Steady stream (like your normal traffic)
	t.Log("\n=== Phase 1: Steady stream (10 msg/sec for 5 sec) ===")

	for i := 0; i < 50; i++ {
		id := int(totalWritten.Add(1))
		writeTime := time.Now()

		// Use consistent sharding like your app
		shardID := uint32(id % 256)
		stream := fmt.Sprintf("events:v1:shard:%04d", shardID)

		data := fmt.Sprintf("msg-%d-time-%s", id, writeTime.Format(time.RFC3339Nano))

		messagesMu.Lock()
		messages[id] = &messageInfo{
			id:        id,
			writeTime: writeTime,
			data:      data,
		}
		messagesMu.Unlock()

		_, err := writer.Append(ctx, stream, [][]byte{[]byte(data)})
		if err != nil {
			t.Errorf("Write error: %v", err)
		}

		time.Sleep(100 * time.Millisecond) // 10 msg/sec
	}

	// Wait for catch up
	time.Sleep(2 * time.Second)
	phase1Seen := totalSeen.Load()
	t.Logf("Phase 1 complete: %d/%d messages seen (%.1f%% loss)",
		phase1Seen, 50, float64(50-phase1Seen)/50*100)

	// Pattern 2: Burst (like your screenshot shows)
	t.Log("\n=== Phase 2: Burst (20 messages instantly) ===")

	for i := 0; i < 20; i++ {
		id := int(totalWritten.Add(1))
		writeTime := time.Now()

		shardID := uint32(id % 256)
		stream := fmt.Sprintf("events:v1:shard:%04d", shardID)

		data := fmt.Sprintf("msg-%d-time-%s", id, writeTime.Format(time.RFC3339Nano))

		messagesMu.Lock()
		messages[id] = &messageInfo{
			id:        id,
			writeTime: writeTime,
			data:      data,
		}
		messagesMu.Unlock()

		_, err := writer.Append(ctx, stream, [][]byte{[]byte(data)})
		if err != nil {
			t.Errorf("Write error: %v", err)
		}
	}

	// Wait for processing
	time.Sleep(2 * time.Second)
	phase2Seen := totalSeen.Load() - phase1Seen
	t.Logf("Phase 2 complete: %d/%d burst messages seen (%.1f%% loss)",
		phase2Seen, 20, float64(20-phase2Seen)/20*100)

	// Pattern 3: Intermittent (gaps between messages)
	t.Log("\n=== Phase 3: Intermittent (5 messages with 1s gaps) ===")

	for i := 0; i < 5; i++ {
		id := int(totalWritten.Add(1))
		writeTime := time.Now()

		shardID := uint32(id % 256)
		stream := fmt.Sprintf("events:v1:shard:%04d", shardID)

		data := fmt.Sprintf("msg-%d-time-%s", id, writeTime.Format(time.RFC3339Nano))

		messagesMu.Lock()
		messages[id] = &messageInfo{
			id:        id,
			writeTime: writeTime,
			data:      data,
		}
		messagesMu.Unlock()

		_, err := writer.Append(ctx, stream, [][]byte{[]byte(data)})
		if err != nil {
			t.Errorf("Write error: %v", err)
		}

		time.Sleep(1 * time.Second)
	}

	// Final wait
	time.Sleep(2 * time.Second)
	phase3Seen := totalSeen.Load() - phase1Seen - phase2Seen
	t.Logf("Phase 3 complete: %d/%d intermittent messages seen (%.1f%% loss)",
		phase3Seen, 5, float64(5-phase3Seen)/5*100)

	// Final analysis
	cancel()

	t.Log("\n=== Final Analysis ===")
	finalTotal := totalWritten.Load()
	finalSeen := totalSeen.Load()

	t.Logf("Total messages written: %d", finalTotal)
	t.Logf("Total messages seen: %d", finalSeen)
	t.Logf("Total messages lost: %d (%.1f%%)", finalTotal-finalSeen,
		float64(finalTotal-finalSeen)/float64(finalTotal)*100)

	// Analyze unseen messages
	var unseenMessages []int
	var latencies []time.Duration

	messagesMu.Lock()
	for id, info := range messages {
		if info.seenTime.IsZero() {
			unseenMessages = append(unseenMessages, id)
		} else {
			latencies = append(latencies, info.seenTime.Sub(info.writeTime))
		}
	}
	messagesMu.Unlock()

	if len(unseenMessages) > 0 {
		t.Logf("\nLost message IDs: %v", unseenMessages)

		// Check which shards they were on
		shardMap := make(map[uint32]int)
		for _, id := range unseenMessages {
			shardID := uint32(id % 256)
			shardMap[shardID]++
		}
		t.Logf("Lost messages by shard: %v", shardMap)
	}

	// Latency analysis
	if len(latencies) > 0 {
		var total time.Duration
		var max time.Duration
		for _, l := range latencies {
			total += l
			if l > max {
				max = l
			}
		}
		avg := total / time.Duration(len(latencies))
		t.Logf("\nLatency stats:")
		t.Logf("  Average: %v", avg)
		t.Logf("  Maximum: %v", max)
	}

	// Check for consumer errors
	close(consumerErrors)
	errCount := 0
	for err := range consumerErrors {
		if errCount < 5 {
			t.Logf("Consumer error: %v", err)
		}
		errCount++
	}
	if errCount > 5 {
		t.Logf("... and %d more consumer errors", errCount-5)
	}

	// Verify writer flush is working
	shard0, exists := writer.shards[0]
	if exists {
		shard0.mu.RLock()
		t.Logf("\nShard 0 state:")
		t.Logf("  CurrentEntryNumber: %d", shard0.index.CurrentEntryNumber)
		t.Logf("  NextEntryNumber: %d", shard0.nextEntryNumber)
		if shard0.state != nil {
			lastUpdate := time.Unix(0, shard0.state.GetLastIndexUpdate())
			t.Logf("  Last index update: %v ago", time.Since(lastUpdate))
		}
		shard0.mu.RUnlock()
	}
}
