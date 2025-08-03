// +build integration

package comet

import (
	"context"
	"fmt"
	"testing"
)

func init() {
	Debug = true
}

// TestConsumerInterference - isolate what consumer operation triggers rollback
func TestConsumerInterference(t *testing.T) {
	testCases := []struct {
		name string
		testFunc func(*testing.T)
	}{
		{
			name: "Control_No_Consumer",
			testFunc: func(t *testing.T) {
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
				
				// Write 20 entries
				for i := 0; i < 20; i++ {
					data := []byte(fmt.Sprintf(`{"id": %d}`, i))
					_, err := client.Append(ctx, streamName, [][]byte{data})
					if err != nil {
						t.Fatal(err)
					}
				}
				
				checkResultsWithScenario(t, client, ctx, streamName, 20, "NO_CONSUMER")
			},
		},
		{
			name: "Consumer_Create_Only",
			testFunc: func(t *testing.T) {
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
				
				// Write 20 entries
				for i := 0; i < 20; i++ {
					data := []byte(fmt.Sprintf(`{"id": %d}`, i))
					_, err := client.Append(ctx, streamName, [][]byte{data})
					if err != nil {
						t.Fatal(err)
					}
				}
				
				// Only create consumer, don't read
				consumer := NewConsumer(client, ConsumerOptions{Group: "test-group"})
				defer consumer.Close()
				
				checkResultsWithScenario(t, client, ctx, streamName, 20, "CREATE_ONLY")
			},
		},
		{
			name: "Consumer_Read",
			testFunc: func(t *testing.T) {
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
				
				// Write 20 entries
				for i := 0; i < 20; i++ {
					data := []byte(fmt.Sprintf(`{"id": %d}`, i))
					_, err := client.Append(ctx, streamName, [][]byte{data})
					if err != nil {
						t.Fatal(err)
					}
				}
				
				// Create and read from consumer
				consumer := NewConsumer(client, ConsumerOptions{Group: "test-group"})
				defer consumer.Close()
				
				messages, err := consumer.Read(ctx, []uint32{1}, 5)
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("Consumer returned %d messages", len(messages))
				
				checkResultsWithScenario(t, client, ctx, streamName, 20, "READ")
			},
		},
	}
	
	for _, tc := range testCases {
		t.Run(tc.name, tc.testFunc)
	}
}

func checkResultsWithScenario(t *testing.T, client *Client, ctx context.Context, streamName string, expectedCount int, scenario string) {
	// Check shard state
	shard, _ := client.getOrCreateShard(1)
	shard.mu.RLock()
	fileCount := len(shard.index.Files)
	indexCurrent := shard.index.CurrentEntryNumber
	stateLast := int64(-999)
	if shard.state != nil {
		stateLast = shard.state.GetLastEntryNumber()
	}
	shard.mu.RUnlock()

	// ScanAll test
	var scanCount int
	err := client.ScanAll(ctx, streamName, func(ctx context.Context, msg StreamMessage) bool {
		scanCount++
		return true
	})
	if err != nil {
		t.Fatal(err)
	}

	// Log results
	success := scanCount == expectedCount
	t.Logf("%s: FILES=%d INDEX=%d STATE=%d SCAN=%d SUCCESS=%v", 
		scenario, fileCount, indexCurrent, stateLast, scanCount, success)
		
	if !success {
		t.Errorf("Expected %d entries, got %d", expectedCount, scanCount)
	}
}