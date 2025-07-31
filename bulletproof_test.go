//go:build integration
// +build integration

package comet

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestBulletproofMultiProcess runs comprehensive stress tests
func TestBulletproofMultiProcess(t *testing.T) {
	tests := []struct {
		name         string
		workers      int
		entriesEach  int
		expectations int
	}{
		{"2 workers × 50", 2, 50, 101},    // 2×50 + 1 init
		{"3 workers × 100", 3, 100, 301},  // 3×100 + 1 init
		{"8 workers × 25", 8, 25, 201},    // 8×25 + 1 init
		{"10 workers × 10", 10, 10, 101},  // 10×10 + 1 init
		{"5 workers × 200", 5, 200, 1001}, // 5×200 + 1 init (heavy load)
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testDir := t.TempDir()
			config := MultiProcessConfig()

			// Initialize with first entry
			client, err := NewClientWithConfig(testDir, config)
			if err != nil {
				t.Fatal(err)
			}

			_, err = client.Append(context.Background(), "test:v1:shard:0001", [][]byte{
				[]byte(`{"init": true}`),
			})
			if err != nil {
				t.Fatal(err)
			}
			client.Close()

			// Launch concurrent writers
			var wg sync.WaitGroup

			for i := 0; i < test.workers; i++ {
				wg.Add(1)
				go func(workerID int) {
					defer wg.Done()

					workerClient, err := NewClientWithConfig(testDir, config)
					if err != nil {
						t.Errorf("Worker %d failed to create client: %v", workerID, err)
						return
					}
					defer workerClient.Close()

					successfulWrites := 0
					for j := 0; j < test.entriesEach; j++ {
						data := fmt.Sprintf(`{"worker":%d,"seq":%d,"data":"stress-test-%d-%d"}`,
							workerID, j, workerID, j)

						_, err := workerClient.Append(context.Background(), "test:v1:shard:0001", [][]byte{
							[]byte(data),
						})
						if err == nil {
							successfulWrites++
						} else {
							t.Errorf("Worker %d write %d failed: %v", workerID, j, err)
						}
					}

					if successfulWrites != test.entriesEach {
						t.Errorf("Worker %d: only %d/%d writes succeeded",
							workerID, successfulWrites, test.entriesEach)
					}
				}(i)
			}

			wg.Wait()

			// Verify all entries are readable
			verifyClient, err := NewClientWithConfig(testDir, config)
			if err != nil {
				t.Fatal(err)
			}
			defer verifyClient.Close()

			// Wait a moment for index updates
			time.Sleep(10 * time.Millisecond)

			consumer := NewConsumer(verifyClient, ConsumerOptions{Group: fmt.Sprintf("verify-%d", time.Now().UnixNano())})
			defer consumer.Close()

			// Read all entries
			messages, err := consumer.Read(context.Background(), []uint32{1}, test.expectations+100)
			if err != nil {
				t.Fatal(err)
			}

			// Filter non-empty messages
			var validMessages []StreamMessage
			for _, msg := range messages {
				if len(msg.Data) > 0 {
					validMessages = append(validMessages, msg)
				}
			}

			if len(validMessages) != test.expectations {
				t.Errorf("%s: Expected %d entries, got %d",
					test.name, test.expectations, len(validMessages))
			}
		})
	}
}

// TestRapidStartupShutdown tests rapid process creation/destruction
func TestRapidStartupShutdown(t *testing.T) {
	testDir := t.TempDir()
	config := MultiProcessConfig()

	// Initialize
	client, err := NewClientWithConfig(testDir, config)
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.Append(context.Background(), "test:v1:shard:0001", [][]byte{
		[]byte(`{"init": true}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	client.Close()

	// Rapidly create and destroy clients while writing
	var wg sync.WaitGroup

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// Rapid client lifecycle
			for cycle := 0; cycle < 5; cycle++ {
				workerClient, err := NewClientWithConfig(testDir, config)
				if err != nil {
					t.Errorf("Worker %d cycle %d: failed to create client: %v", workerID, cycle, err)
					return
				}

				// Write a few entries quickly
				for j := 0; j < 3; j++ {
					data := fmt.Sprintf(`{"worker":%d,"cycle":%d,"seq":%d}`, workerID, cycle, j)
					_, err := workerClient.Append(context.Background(), "test:v1:shard:0001", [][]byte{
						[]byte(data),
					})
					_ = err // Ignore errors for rapid cycling test
				}

				workerClient.Close()
			}
		}(i)
	}

	wg.Wait()

	// Verify data integrity after rapid cycling
	verifyClient, err := NewClientWithConfig(testDir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer verifyClient.Close()

	time.Sleep(20 * time.Millisecond) // Allow for index updates

	consumer := NewConsumer(verifyClient, ConsumerOptions{Group: fmt.Sprintf("verify-%d", time.Now().UnixNano())})
	defer consumer.Close()

	messages, err := consumer.Read(context.Background(), []uint32{1}, 400) // 20×5×3 + buffer
	if err != nil {
		t.Fatal(err)
	}

	// Count valid messages
	validCount := 0
	for _, msg := range messages {
		if len(msg.Data) > 0 {
			validCount++
		}
	}

	t.Logf("Rapid startup/shutdown: wrote entries, read %d valid messages", validCount)

	// Should have at least most entries (allowing for some race conditions in rapid cycling)
	minExpected := 250 // Allow some tolerance for rapid cycling
	if validCount < minExpected {
		t.Errorf("Expected at least %d entries, got %d", minExpected, validCount)
	}
}

// TestTimeBasedStress tests sustained load over time
func TestTimeBasedStress(t *testing.T) {
	testDir := t.TempDir()
	config := MultiProcessConfig()

	// Initialize
	client, err := NewClientWithConfig(testDir, config)
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.Append(context.Background(), "test:v1:shard:0001", [][]byte{
		[]byte(`{"init": true}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	client.Close()

	// Run for 2 seconds with concurrent writers
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var wg sync.WaitGroup

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			workerClient, err := NewClientWithConfig(testDir, config)
			if err != nil {
				t.Errorf("Worker %d failed to create client: %v", workerID, err)
				return
			}
			defer workerClient.Close()

			seq := 0
			for {
				select {
				case <-ctx.Done():
					return
				default:
					data := fmt.Sprintf(`{"worker":%d,"seq":%d,"timestamp":%d}`,
						workerID, seq, time.Now().UnixNano())

					_, err := workerClient.Append(context.Background(), "test:v1:shard:0001", [][]byte{
						[]byte(data),
					})
					if err == nil {
						seq++
					}

					// Small delay to avoid overwhelming
					time.Sleep(100 * time.Microsecond)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify data integrity
	verifyClient, err := NewClientWithConfig(testDir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer verifyClient.Close()

	time.Sleep(50 * time.Millisecond) // Allow for index updates

	consumer := NewConsumer(verifyClient, ConsumerOptions{Group: fmt.Sprintf("verify-%d", time.Now().UnixNano())})
	defer consumer.Close()

	messages, err := consumer.Read(context.Background(), []uint32{1}, 2000) // Large buffer
	if err != nil {
		t.Fatal(err)
	}

	// Count valid messages
	validCount := 0
	for _, msg := range messages {
		if len(msg.Data) > 0 {
			validCount++
		}
	}

	t.Logf("Time-based stress: read %d valid messages", validCount)

	// Should have at least 100 entries (very conservative)
	if validCount < 100 {
		t.Errorf("Expected at least 100 entries, got %d", validCount)
	}
}
