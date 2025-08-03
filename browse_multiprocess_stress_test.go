//go:build integration
// +build integration

package comet

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestBrowseMultiProcessStress tests browse operations under extreme multi-process conditions
func TestBrowseMultiProcessStress(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	// Check if we're a subprocess
	if role := os.Getenv("COMET_BROWSE_STRESS_ROLE"); role != "" {
		runBrowseStressWorker(t, role)
		return
	}

	dir := t.TempDir()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}

	// Run stress test with many processes
	duration := "10s"
	if os.Getenv("CI") != "" {
		duration = "3s"
	}

	numShards := 8
	numWriters := 5
	numConsumerProcesses := 4
	numBrowsers := 6

	t.Logf("Starting stress test: %d writers, %d consumer processes, %d browsers, %d shards for %s",
		numWriters, numConsumerProcesses, numBrowsers, numShards, duration)

	var wg sync.WaitGroup

	// Start hammer writers
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			cmd := exec.Command(executable, "-test.run", "^TestBrowseMultiProcessStress$", "-test.v")
			cmd.Env = append(os.Environ(),
				"COMET_BROWSE_STRESS_ROLE=hammer-writer",
				fmt.Sprintf("COMET_BROWSE_STRESS_DIR=%s", dir),
				fmt.Sprintf("COMET_BROWSE_STRESS_WORKER_ID=writer%d", id),
				fmt.Sprintf("COMET_BROWSE_STRESS_DURATION=%s", duration),
				fmt.Sprintf("COMET_BROWSE_STRESS_NUM_SHARDS=%d", numShards),
			)
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Logf("Writer %d failed: %v", id, err)
			}
			t.Logf("Writer %d: %s", id, strings.TrimSpace(string(output)))
		}(i)
	}

	// Start aggressive consumer processes (each with multiple groups)
	for i := 0; i < numConsumerProcesses; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			time.Sleep(200 * time.Millisecond) // Let writers start
			cmd := exec.Command(executable, "-test.run", "^TestBrowseMultiProcessStress$", "-test.v")
			cmd.Env = append(os.Environ(),
				"COMET_BROWSE_STRESS_ROLE=aggressive-consumer",
				fmt.Sprintf("COMET_BROWSE_STRESS_DIR=%s", dir),
				fmt.Sprintf("COMET_BROWSE_STRESS_GROUP_PREFIX=proc%d", id),
				fmt.Sprintf("COMET_BROWSE_STRESS_DURATION=%s", duration),
				fmt.Sprintf("COMET_BROWSE_STRESS_NUM_SHARDS=%d", numShards),
				"COMET_BROWSE_STRESS_NUM_GROUPS=3",
			)
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Logf("Consumer process %d failed: %v", id, err)
			}
			t.Logf("Consumer process %d:\n%s", id, string(output))
		}(i)
	}

	// Start browse storm processes
	for i := 0; i < numBrowsers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			time.Sleep(400 * time.Millisecond) // Let writers and consumers start
			cmd := exec.Command(executable, "-test.run", "^TestBrowseMultiProcessStress$", "-test.v")
			cmd.Env = append(os.Environ(),
				"COMET_BROWSE_STRESS_ROLE=browse-storm",
				fmt.Sprintf("COMET_BROWSE_STRESS_DIR=%s", dir),
				fmt.Sprintf("COMET_BROWSE_STRESS_WORKER_ID=browser%d", id),
				fmt.Sprintf("COMET_BROWSE_STRESS_DURATION=%s", duration),
				fmt.Sprintf("COMET_BROWSE_STRESS_NUM_SHARDS=%d", numShards),
			)
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Logf("Browser %d failed: %v", id, err)
			}
			t.Logf("Browser %d: %s", id, strings.TrimSpace(string(output)))
		}(i)
	}

	// Start chaos actor
	if !testing.Short() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(1 * time.Second) // Let others establish
			cmd := exec.Command(executable, "-test.run", "^TestBrowseMultiProcessStress$", "-test.v")
			cmd.Env = append(os.Environ(),
				"COMET_BROWSE_STRESS_ROLE=chaos-actor",
				fmt.Sprintf("COMET_BROWSE_STRESS_DIR=%s", dir),
				fmt.Sprintf("COMET_BROWSE_STRESS_DURATION=%s", duration),
			)
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Logf("Chaos actor failed: %v", err)
			}
			t.Logf("Chaos: %s", string(output))
		}()
	}

	// Run validator periodically
	validatorTicker := time.NewTicker(2 * time.Second)
	validatorDone := make(chan struct{})
	go func() {
		for {
			select {
			case <-validatorTicker.C:
				cmd := exec.Command(executable, "-test.run", "^TestBrowseMultiProcessStress$", "-test.v")
				cmd.Env = append(os.Environ(),
					"COMET_BROWSE_STRESS_ROLE=validator",
					fmt.Sprintf("COMET_BROWSE_STRESS_DIR=%s", dir),
					fmt.Sprintf("COMET_BROWSE_STRESS_NUM_SHARDS=%d", numShards),
				)
				output, err := cmd.CombinedOutput()
				if err != nil {
					t.Logf("Validator error: %v\nOutput: %s", err, output)
				} else {
					t.Logf("Validator: %s", strings.TrimSpace(string(output)))
				}
			case <-validatorDone:
				return
			}
		}
	}()

	// Wait for all workers
	wg.Wait()
	close(validatorDone)
	validatorTicker.Stop()

	// Final validation
	t.Log("Running final validation...")
	finalCmd := exec.Command(executable, "-test.run", "^TestBrowseMultiProcessStress$", "-test.v")
	finalCmd.Env = append(os.Environ(),
		"COMET_BROWSE_STRESS_ROLE=validator",
		fmt.Sprintf("COMET_BROWSE_STRESS_DIR=%s", dir),
		fmt.Sprintf("COMET_BROWSE_STRESS_NUM_SHARDS=%d", numShards),
	)
	finalOutput, err := finalCmd.CombinedOutput()
	if err != nil {
		t.Errorf("Final validation failed: %v\nOutput: %s", err, finalOutput)
	} else {
		t.Logf("Final validation: %s", string(finalOutput))
	}
}

// runBrowseStressWorker handles stress test subprocess execution
func runBrowseStressWorker(t *testing.T, role string) {
	dir := os.Getenv("COMET_BROWSE_STRESS_DIR")
	if dir == "" {
		t.Fatal("COMET_BROWSE_STRESS_DIR not set")
	}

	config := MultiProcessConfig()
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	switch role {
	case "hammer-writer":
		// Write as fast as possible to create maximum contention
		workerID := os.Getenv("COMET_BROWSE_STRESS_WORKER_ID")
		duration, _ := time.ParseDuration(os.Getenv("COMET_BROWSE_STRESS_DURATION"))
		numShards, _ := strconv.Atoi(os.Getenv("COMET_BROWSE_STRESS_NUM_SHARDS"))

		var totalWrites int64
		var errors int64
		start := time.Now()

		for time.Since(start) < duration {
			shardID := rand.Intn(numShards) + 1
			streamName := fmt.Sprintf("test:v1:shard:%04d", shardID)

			// Variable batch sizes to stress different code paths
			batchSize := rand.Intn(100) + 1
			batch := make([][]byte, batchSize)

			for i := 0; i < batchSize; i++ {
				// Mix small and large messages
				size := rand.Intn(50*1024) + 100
				data := make(map[string]interface{})
				data["writer"] = workerID
				data["seq"] = atomic.AddInt64(&totalWrites, 1)
				data["size"] = size
				data["time"] = time.Now().UnixNano()

				if size > 1024 {
					// Large payload to stress memory and force rotations
					data["payload"] = make([]byte, size)
				}

				batch[i], _ = json.Marshal(data)
			}

			_, err := client.Append(ctx, streamName, batch)
			if err != nil {
				atomic.AddInt64(&errors, 1)
			}

			// No sleep - maximum pressure
		}

		fmt.Printf("Writer %s: writes=%d errors=%d rate=%.0f/sec\n",
			workerID, totalWrites, errors, float64(totalWrites)/time.Since(start).Seconds())

	case "aggressive-consumer":
		// Multiple consumer groups fighting for data
		groupPrefix := os.Getenv("COMET_BROWSE_STRESS_GROUP_PREFIX")
		duration, _ := time.ParseDuration(os.Getenv("COMET_BROWSE_STRESS_DURATION"))
		numShards, _ := strconv.Atoi(os.Getenv("COMET_BROWSE_STRESS_NUM_SHARDS"))
		numGroups, _ := strconv.Atoi(os.Getenv("COMET_BROWSE_STRESS_NUM_GROUPS"))

		shards := make([]uint32, numShards)
		for i := 0; i < numShards; i++ {
			shards[i] = uint32(i + 1)
		}

		var wg sync.WaitGroup

		// Multiple consumer groups in same process
		for g := 0; g < numGroups; g++ {
			wg.Add(1)
			go func(groupID int) {
				defer wg.Done()

				consumer := NewConsumer(client, ConsumerOptions{
					Group: fmt.Sprintf("%s-%d", groupPrefix, groupID),
				})
				defer consumer.Close()

				var reads, acks, errors int64
				start := time.Now()

				for time.Since(start) < duration {
					messages, err := consumer.Read(ctx, shards, rand.Intn(200)+10)
					if err != nil {
						atomic.AddInt64(&errors, 1)
						continue
					}

					atomic.AddInt64(&reads, int64(len(messages)))

					// Various ACK patterns to stress offset management
					switch groupID % 4 {
					case 0: // ACK all immediately
						ids := make([]MessageID, len(messages))
						for i, msg := range messages {
							ids[i] = msg.ID
						}
						if err := consumer.Ack(ctx, ids...); err == nil {
							atomic.AddInt64(&acks, int64(len(ids)))
						}
					case 1: // ACK individually with delay
						for _, msg := range messages {
							if err := consumer.Ack(ctx, msg.ID); err == nil {
								atomic.AddInt64(&acks, 1)
							}
							if rand.Float32() < 0.1 {
								time.Sleep(time.Microsecond)
							}
						}
					case 2: // ACK only some
						for i, msg := range messages {
							if i%2 == 0 {
								if err := consumer.Ack(ctx, msg.ID); err == nil {
									atomic.AddInt64(&acks, 1)
								}
							}
						}
					case 3: // No ACK - create lag
					}
				}

				fmt.Printf("Consumer %s-%d: reads=%d acks=%d errors=%d\n",
					groupPrefix, groupID, reads, acks, errors)
			}(g)
		}

		wg.Wait()

	case "browse-storm":
		// All browse operations at maximum rate
		workerID := os.Getenv("COMET_BROWSE_STRESS_WORKER_ID")
		duration, _ := time.ParseDuration(os.Getenv("COMET_BROWSE_STRESS_DURATION"))
		numShards, _ := strconv.Atoi(os.Getenv("COMET_BROWSE_STRESS_NUM_SHARDS"))

		var listOps, scanOps int64
		var listErrors, scanErrors int64
		var totalBrowsed int64

		start := time.Now()
		var wg sync.WaitGroup

		// ListRecent storm
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for time.Since(start) < duration {
					shardID := rand.Intn(numShards) + 1
					streamName := fmt.Sprintf("test:v1:shard:%04d", shardID)

					messages, err := client.ListRecent(ctx, streamName, rand.Intn(200)+10)
					if err != nil {
						atomic.AddInt64(&listErrors, 1)
					} else {
						atomic.AddInt64(&listOps, 1)
						atomic.AddInt64(&totalBrowsed, int64(len(messages)))
					}
				}
			}()
		}

		// ScanAll storm
		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for time.Since(start) < duration {
					shardID := rand.Intn(numShards) + 1
					streamName := fmt.Sprintf("test:v1:shard:%04d", shardID)

					limit := rand.Intn(1000) + 100
					count := 0
					err := client.ScanAll(ctx, streamName, func(ctx context.Context, msg StreamMessage) bool {
						count++
						atomic.AddInt64(&totalBrowsed, 1)
						return count < limit
					})

					if err != nil {
						atomic.AddInt64(&scanErrors, 1)
					} else {
						atomic.AddInt64(&scanOps, 1)
					}
				}
			}()
		}

		wg.Wait()

		elapsed := time.Since(start)
		fmt.Printf("Browser %s: list=%d/%d scan=%d/%d total=%d rate=%.0f/sec\n",
			workerID, listOps, listErrors, scanOps, scanErrors,
			totalBrowsed, float64(totalBrowsed)/elapsed.Seconds())

	case "chaos-actor":
		// Randomly kills other processes, forces rotations, etc.
		duration, _ := time.ParseDuration(os.Getenv("COMET_BROWSE_STRESS_DURATION"))
		start := time.Now()

		for time.Since(start) < duration {
			action := rand.Intn(3)
			switch action {
			case 0:
				// Force file rotation by writing large data
				shardID := rand.Intn(8) + 1
				streamName := fmt.Sprintf("test:v1:shard:%04d", shardID)

				hugeData := make([]byte, 5*1024*1024) // 5MB
				for i := range hugeData {
					hugeData[i] = byte(rand.Intn(256))
				}

				_, err := client.Append(ctx, streamName, [][]byte{hugeData})
				if err != nil {
					fmt.Fprintf(os.Stderr, "Chaos: rotation write failed: %v\n", err)
				} else {
					fmt.Printf("Chaos: forced rotation on shard %d\n", shardID)
				}

			case 1:
				// Consume all messages from random consumer group
				group := fmt.Sprintf("chaos-consumer-%d", rand.Intn(10))
				consumer := NewConsumer(client, ConsumerOptions{Group: group})

				shardID := uint32(rand.Intn(8) + 1)
				messages, _ := consumer.Read(ctx, []uint32{shardID}, 10000)

				// ACK everything to advance offset dramatically
				for _, msg := range messages {
					consumer.Ack(ctx, msg.ID)
				}

				consumer.Close()
				fmt.Printf("Chaos: consumed %d messages from shard %d as %s\n",
					len(messages), shardID, group)

			case 2:
				// Just create a client and close it (test connection churn)
				newClient, err := NewClientWithConfig(dir, config)
				if err == nil {
					newClient.Close()
				}
			}

			time.Sleep(time.Duration(rand.Intn(500)+100) * time.Millisecond)
		}

		fmt.Printf("Chaos actor completed\n")

	case "validator":
		// Validates browse operations return consistent data
		time.Sleep(500 * time.Millisecond) // Let others start

		numShards, _ := strconv.Atoi(os.Getenv("COMET_BROWSE_STRESS_NUM_SHARDS"))

		inconsistencies := 0
		for shardID := 1; shardID <= numShards; shardID++ {
			streamName := fmt.Sprintf("test:v1:shard:%04d", shardID)

			// Get data via different methods
			recent, err1 := client.ListRecent(ctx, streamName, 100)

			var scanFirst100 []StreamMessage
			var scanCount int
			err2 := client.ScanAll(ctx, streamName, func(ctx context.Context, msg StreamMessage) bool {
				if scanCount < 100 {
					scanFirst100 = append(scanFirst100, msg)
				}
				scanCount++
				return scanCount < 1000
			})

			if err1 != nil || err2 != nil {
				fmt.Fprintf(os.Stderr, "Validator: errors on shard %d: list=%v scan=%v\n",
					shardID, err1, err2)
				continue
			}

			// Recent should be the newest entries
			if len(recent) > 0 && scanCount > len(recent) {
				// The newest entry from recent should have higher ID than oldest scan entry
				newestRecent := recent[0].ID.EntryNumber
				if len(scanFirst100) > 0 {
					oldestScan := scanFirst100[0].ID.EntryNumber
					if newestRecent < oldestScan {
						inconsistencies++
						fmt.Fprintf(os.Stderr, "Validator: inconsistency on shard %d: "+
							"newest recent %d < oldest scan %d\n",
							shardID, newestRecent, oldestScan)
					}
				}
			}

			fmt.Printf("Validator: shard %d has %d entries, recent=%d\n",
				shardID, scanCount, len(recent))
		}

		if inconsistencies > 0 {
			fmt.Fprintf(os.Stderr, "Validator: found %d inconsistencies!\n", inconsistencies)
			os.Exit(1)
		}

		fmt.Printf("Validator: all shards consistent\n")
	}
}
