package comet

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// TestProcessContinuousBatchingIntegration is a comprehensive integration test
// that verifies Process() correctly handles continuous batching in various scenarios
func TestProcessContinuousBatchingIntegration(t *testing.T) {
	tests := []struct {
		name        string
		config      func() CometConfig
		totalMsgs   int
		batchSize   int
		shardNumber uint32
		streamName  string
	}{
		{
			name:        "SingleProcess_SmallBatches",
			config:      DefaultCometConfig,
			totalMsgs:   200,
			batchSize:   25,
			shardNumber: 1,
			streamName:  "integration:v1:shard:0000",
		},
		{
			name:        "MultiProcess_LargeBatches",
			config:      func() CometConfig { return DeprecatedMultiProcessConfig(0, 2) },
			totalMsgs:   500,
			batchSize:   100,
			shardNumber: 166, // Use higher shard number like user had
			streamName:  "integration:v1:shard:0166",
		},
		{
			name:        "SingleProcess_TinyBatches",
			config:      DefaultCometConfig,
			totalMsgs:   50,
			batchSize:   5,
			shardNumber: 42,
			streamName:  "integration:v1:shard:0042",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			config := tt.config()

			client, err := NewClientWithConfig(dir, config)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()

			ctx := context.Background()

			// Write test messages
			t.Logf("Writing %d messages to %s", tt.totalMsgs, tt.streamName)
			var messages [][]byte
			for i := 0; i < tt.totalMsgs; i++ {
				messages = append(messages, []byte(fmt.Sprintf("%s-msg-%04d", tt.name, i)))
			}

			_, err = client.Append(ctx, tt.streamName, messages)
			if err != nil {
				t.Fatal(err)
			}

			// Sync to ensure data is visible to consumers
			if err := client.Sync(ctx); err != nil {
				t.Fatal(err)
			}

			// Verify all messages written
			length, err := client.Len(ctx, tt.streamName)
			if err != nil {
				t.Fatal(err)
			}
			if length != int64(tt.totalMsgs) {
				t.Fatalf("Expected %d messages written, got %d", tt.totalMsgs, length)
			}

			// Create consumer and start processing
			consumer := NewConsumer(client, ConsumerOptions{Group: fmt.Sprintf("%s-group", tt.name)})
			defer consumer.Close()

			var processedCount int64
			var batchCount int64
			var lastBatchSize int

			processCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()

			processFunc := func(ctx context.Context, msgs []StreamMessage) error {
				currentBatch := atomic.AddInt64(&batchCount, 1)
				processed := atomic.AddInt64(&processedCount, int64(len(msgs)))
				lastBatchSize = len(msgs)

				t.Logf("%s: Batch %d processed %d messages (total: %d/%d)",
					tt.name, currentBatch, len(msgs), processed, tt.totalMsgs)

				// Verify message contents
				for i, msg := range msgs {
					expectedPrefix := fmt.Sprintf("%s-msg-", tt.name)
					if !containsPrefix(string(msg.Data), expectedPrefix) {
						t.Errorf("Batch %d msg %d: expected prefix %s, got %s", currentBatch, i, expectedPrefix, string(msg.Data))
					}
				}

				// Cancel when all messages processed
				if int(processed) >= tt.totalMsgs {
					t.Logf("%s: All messages processed, canceling", tt.name)
					cancel()
				}

				return nil
			}

			// Start processing
			startTime := time.Now()
			consumer.Process(processCtx, processFunc,
				WithStream("integration:v1:shard:*"),
				WithBatchSize(tt.batchSize),
				WithAutoAck(true),
				WithPollInterval(50*time.Millisecond),
			)
			duration := time.Since(startTime)

			// Verify results
			finalProcessed := atomic.LoadInt64(&processedCount)
			finalBatches := atomic.LoadInt64(&batchCount)
			expectedBatches := (tt.totalMsgs + tt.batchSize - 1) / tt.batchSize

			t.Logf("%s: Final results in %v:", tt.name, duration)
			t.Logf("  Processed: %d/%d messages", finalProcessed, tt.totalMsgs)
			t.Logf("  Batches: %d (expected ~%d)", finalBatches, expectedBatches)
			t.Logf("  Last batch size: %d", lastBatchSize)

			// Assertions
			if finalProcessed != int64(tt.totalMsgs) {
				t.Errorf("Expected %d messages processed, got %d", tt.totalMsgs, finalProcessed)
			}

			if finalBatches < 1 {
				t.Error("Expected at least 1 batch processed")
			}

			// Sync to ensure ACKs are persisted
			if err := client.Sync(ctx); err != nil {
				t.Errorf("Failed to sync: %v", err)
			}

			// Verify consumer lag is zero (all messages consumed)
			lag, err := consumer.GetLag(ctx, tt.shardNumber)
			if err != nil {
				t.Errorf("Failed to get consumer lag: %v", err)
			} else if lag != 0 {
				t.Errorf("Expected consumer lag 0, got %d", lag)
			}

			// Performance check - should complete reasonably quickly
			if duration > 10*time.Second {
				t.Errorf("Processing took too long: %v (expected < 10s)", duration)
			}
		})
	}
}

// TestProcessWithDynamicDataAddition tests Process() continues when new data is added during processing
func TestProcessWithDynamicDataAddition(t *testing.T) {
	dir := t.TempDir()
	config := DefaultCometConfig()

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	stream := "dynamic:v1:shard:0000"

	// Write initial batch
	initialCount := 50
	var messages [][]byte
	for i := 0; i < initialCount; i++ {
		messages = append(messages, []byte(fmt.Sprintf("initial-msg-%d", i)))
	}

	_, err = client.Append(ctx, stream, messages)
	if err != nil {
		t.Fatal(err)
	}

	// Sync to ensure initial data is visible
	if err := client.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	consumer := NewConsumer(client, ConsumerOptions{Group: "dynamic-group"})
	defer consumer.Close()

	var processedCount int64
	var batchCount int64
	dynamicDataAdded := false

	processCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	processFunc := func(ctx context.Context, msgs []StreamMessage) error {
		currentBatch := atomic.AddInt64(&batchCount, 1)
		processed := atomic.AddInt64(&processedCount, int64(len(msgs)))

		t.Logf("Dynamic test batch %d: processed %d messages (total: %d)",
			currentBatch, len(msgs), processed)

		// After processing initial messages, add more data
		if processed >= int64(initialCount) && !dynamicDataAdded {
			dynamicDataAdded = true
			go func() {
				time.Sleep(100 * time.Millisecond)

				additionalCount := 25
				var additionalMessages [][]byte
				for i := 0; i < additionalCount; i++ {
					additionalMessages = append(additionalMessages, []byte(fmt.Sprintf("dynamic-msg-%d", i)))
				}

				_, err := client.Append(context.Background(), stream, additionalMessages)
				if err != nil {
					t.Errorf("Failed to add dynamic data: %v", err)
				}
				// Sync to ensure dynamic data is visible
				if err := client.Sync(context.Background()); err != nil {
					t.Errorf("Failed to sync dynamic data: %v", err)
				}
				t.Logf("Added %d dynamic messages", additionalCount)
			}()
		}

		// Cancel after processing both initial and dynamic data
		expectedTotal := int64(initialCount + 25)
		if processed >= expectedTotal {
			t.Logf("Processed all data including dynamic additions")
			cancel()
		}

		return nil
	}

	consumer.Process(processCtx, processFunc,
		WithStream("dynamic:v1:shard:*"),
		WithBatchSize(10),
		WithAutoAck(true),
		WithPollInterval(200*time.Millisecond), // Longer poll to catch dynamic data
	)

	finalProcessed := atomic.LoadInt64(&processedCount)
	finalBatches := atomic.LoadInt64(&batchCount)
	expectedTotal := int64(initialCount + 25)

	t.Logf("Dynamic test results: processed %d/%d messages in %d batches",
		finalProcessed, expectedTotal, finalBatches)

	if finalProcessed < expectedTotal {
		t.Errorf("Expected at least %d messages (initial + dynamic), got %d",
			expectedTotal, finalProcessed)
	}

	if !dynamicDataAdded {
		t.Error("Dynamic data addition was not triggered")
	}
}

// containsPrefix is a helper function for string prefix matching
func containsPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}
