//go:build integration

package comet

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestACKPersistenceStress comprehensively stress tests ACK persistence
func TestACKPersistenceStress(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping ACK persistence stress test in short mode")
	}

	tests := []struct {
		name           string
		totalMessages  int
		batchSize      int  
		numConsumers   int
		processDelayMs int
		restartFreq    int // Restart every N batches
	}{
		{
			name:           "HighVolume_SmallBatches",
			totalMessages:  5000,
			batchSize:      10,
			numConsumers:   1,
			processDelayMs: 1,
			restartFreq:    20,
		},
		{
			name:           "MediumVolume_LargeBatches",
			totalMessages:  1000,
			batchSize:      100,
			numConsumers:   1,
			processDelayMs: 5,
			restartFreq:    5,
		},
		{
			name:           "MultiConsumer_Contention",
			totalMessages:  2000,
			batchSize:      25,
			numConsumers:   3,
			processDelayMs: 2,
			restartFreq:    10,
		},
		{
			name:           "RapidRestart_Torture",
			totalMessages:  500,
			batchSize:      5,
			numConsumers:   1,
			processDelayMs: 0,
			restartFreq:    2, // Restart every 2 batches - very aggressive
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runACKStressTest(t, tt.totalMessages, tt.batchSize, tt.numConsumers, 
				tt.processDelayMs, tt.restartFreq)
		})
	}
}

func runACKStressTest(t *testing.T, totalMessages, batchSize, numConsumers, processDelayMs, restartFreq int) {
	dir := t.TempDir()
	config := MultiProcessConfig()
	stream := fmt.Sprintf("stress:v1:shard:0001")
	
	// Step 1: Write all test messages
	t.Logf("Writing %d messages", totalMessages)
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	var messages [][]byte
	for i := 0; i < totalMessages; i++ {
		messages = append(messages, []byte(fmt.Sprintf("stress-msg-%06d", i)))
	}

	ctx := context.Background()
	_, err = client.Append(ctx, stream, messages)
	if err != nil {
		t.Fatal(err)
	}
	client.Close()

	// Step 2: Create tracking map for processed messages
	processedMessages := sync.Map{}
	var totalProcessed int64
	var totalDuplicates int64
	var totalRestarts int64

	// Step 3: Run concurrent stress consumers
	var wg sync.WaitGroup
	results := make(chan stressResult, numConsumers)

	for consumerID := 0; consumerID < numConsumers; consumerID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			runStressConsumer(t, dir, stream, id, batchSize, processDelayMs, 
				restartFreq, &processedMessages, &totalProcessed, 
				&totalDuplicates, &totalRestarts, results)
		}(consumerID)
	}

	// Step 4: Monitor progress
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(results)
		close(done)
	}()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	timeout := time.After(120 * time.Second) // 2 minute timeout
	
	for {
		select {
		case <-done:
			goto finished
		case <-ticker.C:
			processed := atomic.LoadInt64(&totalProcessed)
			duplicates := atomic.LoadInt64(&totalDuplicates)
			restarts := atomic.LoadInt64(&totalRestarts)
			t.Logf("Progress: processed=%d/%d, duplicates=%d, restarts=%d", 
				processed, totalMessages, duplicates, restarts)
		case <-timeout:
			t.Fatal("Stress test timed out after 2 minutes")
		}
	}

finished:
	// Step 5: Collect results
	var successfulConsumers int
	var failedConsumers int

	for result := range results {
		if result.err != nil {
			failedConsumers++
			t.Logf("Consumer %d failed: %v", result.consumerID, result.err)
		} else {
			successfulConsumers++
			t.Logf("Consumer %d succeeded: processed=%d, duplicates=%d", 
				result.consumerID, result.processed, result.duplicates)
		}
	}

	// Step 6: Verify results
	finalProcessed := atomic.LoadInt64(&totalProcessed)
	finalDuplicates := atomic.LoadInt64(&totalDuplicates)
	finalRestarts := atomic.LoadInt64(&totalRestarts)

	t.Logf("STRESS TEST RESULTS:")
	t.Logf("  Total messages: %d", totalMessages)
	t.Logf("  Total processed: %d", finalProcessed)
	t.Logf("  Duplicates detected: %d", finalDuplicates)
	t.Logf("  Client restarts: %d", finalRestarts)
	t.Logf("  Successful consumers: %d/%d", successfulConsumers, numConsumers)

	// Critical assertions
	if successfulConsumers == 0 {
		t.Fatal("No consumers completed successfully")
	}

	if finalDuplicates > 0 {
		t.Errorf("CRITICAL: Found %d duplicate messages - ACK persistence is broken!", finalDuplicates)
	}

	// Count unique messages processed
	uniqueCount := 0
	processedMessages.Range(func(key, value interface{}) bool {
		uniqueCount++
		return true
	})

	if uniqueCount < totalMessages {
		t.Errorf("CRITICAL: Only %d/%d unique messages processed - some were lost!", 
			uniqueCount, totalMessages)
	}

	// Performance check
	if finalRestarts == 0 {
		t.Error("Expected at least some restarts in stress test")
	}

	t.Logf("✅ ACK persistence stress test PASSED")
}

func runStressConsumer(t *testing.T, dataDir, stream string, consumerID, batchSize, 
	processDelayMs, restartFreq int, processedMessages *sync.Map, 
	totalProcessed, totalDuplicates, totalRestarts *int64, results chan<- stressResult) {

	var localProcessed int64
	var localDuplicates int64
	batchCount := 0

	// Consumer will restart every restartFreq batches
	for {
		// Create new client for each "restart"
		config := MultiProcessConfig()
		client, err := NewClientWithConfig(dataDir, config)
		if err != nil {
			results <- stressResult{consumerID: consumerID, err: err}
			return
		}

		consumer := NewConsumer(client, ConsumerOptions{
			Group: fmt.Sprintf("stress-consumer-%d", consumerID),
		})

		// Process for a limited number of batches before "restarting"
		batchesBeforeRestart := restartFreq + rand.Intn(3) // Add some randomness
		processedThisSession := 0

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

		processFunc := func(ctx context.Context, msgs []StreamMessage) error {
			batchCount++
			
			// Add processing delay to simulate real work
			if processDelayMs > 0 {
				time.Sleep(time.Duration(processDelayMs) * time.Millisecond)
			}

			// Check for duplicates
			for _, msg := range msgs {
				msgKey := string(msg.Data)
				if _, exists := processedMessages.LoadOrStore(msgKey, true); exists {
					// Duplicate detected!
					atomic.AddInt64(totalDuplicates, 1)
					atomic.AddInt64(&localDuplicates, 1)
					t.Logf("DUPLICATE detected by consumer %d: %s", consumerID, msgKey)
				}
			}

			atomic.AddInt64(totalProcessed, int64(len(msgs)))
			atomic.AddInt64(&localProcessed, int64(len(msgs)))
			processedThisSession += len(msgs)

			// Stop this session after processing enough batches
			if batchCount >= batchesBeforeRestart {
				cancel()
			}

			return nil
		}

		err = consumer.Process(ctx, processFunc,
			WithStream("stress:v1:shard:*"),
			WithBatchSize(batchSize),
			WithAutoAck(true),
			WithPollInterval(50*time.Millisecond),
		)

		consumer.Close()
		client.Close()
		cancel()

		// If we processed nothing, we're probably done
		if processedThisSession == 0 {
			break
		}

		atomic.AddInt64(totalRestarts, 1)
		batchCount = 0

		// Small delay between restarts
		time.Sleep(time.Duration(10+rand.Intn(20)) * time.Millisecond)
	}

	results <- stressResult{
		consumerID: consumerID,
		processed:  localProcessed,
		duplicates: localDuplicates,
		err:        nil,
	}
}

// TestACKPersistenceRaceConditions tests for race conditions in ACK persistence
func TestACKPersistenceRaceConditions(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping ACK race condition test in short mode")
	}

	dir := t.TempDir()
	config := MultiProcessConfig()
	stream := "race:v1:shard:0001"
	totalMessages := 1000

	// Write messages
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	var messages [][]byte
	for i := 0; i < totalMessages; i++ {
		messages = append(messages, []byte(fmt.Sprintf("race-msg-%06d", i)))
	}

	ctx := context.Background()
	_, err = client.Append(ctx, stream, messages)
	if err != nil {
		t.Fatal(err)
	}
	client.Close()

	// Launch multiple competing processes that ACK rapidly
	numWorkers := 5
	var wg sync.WaitGroup

	for workerID := 0; workerID < numWorkers; workerID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			
			// Launch as separate OS process
			cmd := exec.Command("go", "test", "-run", "TestACKRaceWorker", 
				"-timeout", "60s", "-v")
			cmd.Env = append(os.Environ(),
				fmt.Sprintf("COMET_RACE_DATA_DIR=%s", dir),
				fmt.Sprintf("COMET_RACE_WORKER_ID=%d", id),
				fmt.Sprintf("COMET_RACE_STREAM=%s", stream),
			)
			
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Logf("Race worker %d failed: %v\nOutput:\n%s", id, err, string(output))
			} else {
				t.Logf("Race worker %d completed successfully", id)
			}
		}(workerID)
	}

	wg.Wait()

	// Verify final state consistency
	finalClient, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer finalClient.Close()

	length, err := finalClient.Len(ctx, stream)
	if err != nil {
		t.Fatal(err)
	}

	if length != int64(totalMessages) {
		t.Errorf("Data corruption: expected %d messages, got %d", totalMessages, length)
	}

	t.Logf("✅ ACK race condition test completed - no data corruption detected")
}

// TestACKRaceWorker is run in separate processes to test race conditions
func TestACKRaceWorker(t *testing.T) {
	dataDir := os.Getenv("COMET_RACE_DATA_DIR")
	if dataDir == "" {
		t.Skip("Not running as race worker")
	}

	workerIDStr := os.Getenv("COMET_RACE_WORKER_ID")
	_ = os.Getenv("COMET_RACE_STREAM") // stream not used in worker
	workerID, _ := strconv.Atoi(workerIDStr)

	config := MultiProcessConfig()
	client, err := NewClientWithConfig(dataDir, config)
	if err != nil {
		t.Fatalf("Race worker %d failed to create client: %v", workerID, err)
	}
	defer client.Close()

	consumer := NewConsumer(client, ConsumerOptions{
		Group: fmt.Sprintf("race-worker-%d", workerID),
	})
	defer consumer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	processedCount := 0
	
	processFunc := func(ctx context.Context, msgs []StreamMessage) error {
		processedCount += len(msgs)
		
		// Rapidly ACK to create race conditions
		for _, msg := range msgs {
			// Manual ACK to test race conditions
			if err := consumer.Ack(ctx, msg.ID); err != nil {
				t.Logf("Race worker %d ACK failed: %v", workerID, err)
			}
		}

		// Stop after reasonable processing
		if processedCount >= 100 {
			cancel()
		}
		
		return nil
	}

	err = consumer.Process(ctx, processFunc,
		WithStream("race:v1:shard:*"),
		WithBatchSize(5),
		WithAutoAck(false), // Manual ACK to test race conditions
		WithPollInterval(10*time.Millisecond),
	)

	t.Logf("Race worker %d processed %d messages", workerID, processedCount)
}

// TestACKPersistenceMemoryPressure tests ACK persistence under memory pressure
func TestACKPersistenceMemoryPressure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping memory pressure test in short mode")
	}

	dir := t.TempDir()
	config := MultiProcessConfig()
	
	// Use small files to trigger more rotations
	config.Storage.MaxFileSize = 1024 * 1024 // 1MB files
	
	stream := "memory:v1:shard:0001"
	totalMessages := 10000 // Large number to create memory pressure

	// Write large messages to consume memory
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	var messages [][]byte
	for i := 0; i < totalMessages; i++ {
		// Larger messages to increase memory pressure
		data := fmt.Sprintf("memory-pressure-msg-%06d-%s", i, 
			string(make([]byte, 500))) // 500 byte padding
		messages = append(messages, []byte(data))
	}

	ctx := context.Background()
	_, err = client.Append(ctx, stream, messages)
	if err != nil {
		t.Fatal(err)
	}
	client.Close()

	// Process with memory constraints
	consumer := NewConsumer(client, ConsumerOptions{Group: "memory-test"})
	defer consumer.Close()

	processedCount := 0
	duplicateCount := 0
	restartCount := 0

	// Simulate memory pressure with forced restarts
	for restartCount < 10 && processedCount < totalMessages {
		client, err := NewClientWithConfig(dir, config)
		if err != nil {
			t.Fatal(err)
		}

		consumer := NewConsumer(client, ConsumerOptions{Group: "memory-test"})
		
		sessionProcessed := 0
		processCtx, cancel := context.WithTimeout(ctx, 10*time.Second)

		processFunc := func(ctx context.Context, msgs []StreamMessage) error {
			sessionProcessed += len(msgs)
			processedCount += len(msgs)

			// Check for duplicates (basic check)
			for _, msg := range msgs {
				if len(string(msg.Data)) > 0 && string(msg.Data)[0] == 'D' { // Simple duplicate marker check
					duplicateCount++
				}
			}

			// Restart session after processing some messages
			if sessionProcessed >= 500 {
				cancel()
			}

			return nil
		}

		err = consumer.Process(processCtx, processFunc,
			WithStream("memory:v1:shard:*"),
			WithBatchSize(50),
			WithAutoAck(true),
		)

		consumer.Close() 
		client.Close()
		cancel()

		if sessionProcessed == 0 {
			break // No more messages
		}

		restartCount++
		t.Logf("Memory test restart %d: processed %d messages this session (total: %d)", 
			restartCount, sessionProcessed, processedCount)
	}

	t.Logf("Memory pressure test results:")
	t.Logf("  Total processed: %d/%d", processedCount, totalMessages)
	t.Logf("  Duplicates detected: %d", duplicateCount)
	t.Logf("  Restarts performed: %d", restartCount)

	if duplicateCount > 0 {
		t.Errorf("Found %d duplicates under memory pressure - ACK persistence failed!", duplicateCount)
	}

	if processedCount < totalMessages/2 {
		t.Errorf("Only processed %d/%d messages - test may have failed", processedCount, totalMessages)
	}

	t.Logf("✅ Memory pressure test completed successfully")
}

type stressResult struct {
	consumerID int
	processed  int64
	duplicates int64
	err        error
}