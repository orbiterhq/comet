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

// TestMethodComparison - compare different write patterns
func TestMethodComparison(t *testing.T) {
	testCases := []struct {
		name string
		testFunc func(*testing.T)
	}{
		{
			name: "Working_Individual_Writes_20",
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
				
				// Individual writes like threshold test
				for i := 0; i < 20; i++ {
					data := []byte(fmt.Sprintf(`{"id": %d}`, i))
					_, err := client.Append(ctx, streamName, [][]byte{data})
					if err != nil {
						t.Fatal(err)
					}
				}
				
				checkResults(t, client, ctx, streamName, 20)
			},
		},
		{
			name: "Original_Failing_Pattern",
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
				
				// Exact pattern from failing TestDataScanning
				for i := 0; i < 20; i++ {
					data := []byte(fmt.Sprintf(`{"id": %d}`, i))
					_, err := client.Append(ctx, streamName, [][]byte{data})
					if err != nil {
						t.Fatal(err)
					}
				}
				t.Logf("Wrote 20 entries")

				// Try consumer read like the failing test
				consumer := NewConsumer(client, ConsumerOptions{Group: "test-group"})
				defer consumer.Close()

				messages, err := consumer.Read(ctx, []uint32{1}, 5)
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("Consumer.Read returned %d messages", len(messages))

				checkResults(t, client, ctx, streamName, 20)
			},
		},
	}
	
	for _, tc := range testCases {
		t.Run(tc.name, tc.testFunc)
	}
}

func checkResults(t *testing.T, client *Client, ctx context.Context, streamName string, expectedCount int) {
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
	t.Logf("RESULT: FILES=%d INDEX=%d STATE=%d SCAN=%d EXPECTED=%d SUCCESS=%v", 
		fileCount, indexCurrent, stateLast, scanCount, expectedCount, success)
}