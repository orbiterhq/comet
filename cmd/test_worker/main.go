package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/orbiterhq/comet"
)

func main() {
	// Check if first argument is a command
	if len(os.Args) > 1 && os.Args[1] == "retention-test" {
		runRetentionTest()
		return
	}

	if len(os.Args) > 1 && os.Args[1] == "index-rebuild-test" {
		runIndexRebuildTest()
		return
	}

	if len(os.Args) > 1 && os.Args[1] == "retention-debug" {
		runRetentionDebug()
		return
	}

	var (
		mode     = flag.String("mode", "writer", "Mode: writer, reader, or benchmark")
		dir      = flag.String("dir", "", "Data directory")
		id       = flag.String("id", "", "Process ID")
		duration = flag.Duration("duration", 10*time.Second, "How long to run")
	)
	flag.Parse()

	if *dir == "" {
		log.Fatal("--dir is required")
	}

	switch *mode {
	case "writer":
		runWriter(*dir, *id, *duration)
	case "reader":
		runReader(*dir, *id, *duration)
	case "benchmark":
		runBenchmark(*dir, *id, *duration)
	default:
		log.Fatalf("unknown mode: %s", *mode)
	}
}

func runWriter(dir, id string, duration time.Duration) {
	config := comet.MultiProcessConfig()
	client, err := comet.NewClientWithConfig(dir, config)
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	streamName := "test:v1:shard:0001"
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	count := 0
	for {
		select {
		case <-ctx.Done():
			log.Printf("Writer %s completed: wrote %d entries", id, count)
			return
		case <-ticker.C:
			// Write a batch of entries
			batch := make([][]byte, 10)
			for i := 0; i < 10; i++ {
				entry := map[string]interface{}{
					"writer_id": id,
					"sequence":  count + i,
					"timestamp": time.Now().UnixNano(),
					"data":      fmt.Sprintf("test data from %s entry %d", id, count+i),
				}
				data, _ := json.Marshal(entry)
				batch[i] = data
			}

			if _, err := client.Append(ctx, streamName, batch); err != nil {
				log.Printf("Writer %s append error: %v", id, err)
			} else {
				count += 10
			}
		}
	}
}

func runReader(dir, id string, duration time.Duration) {
	config := comet.MultiProcessConfig()
	client, err := comet.NewClientWithConfig(dir, config)
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	consumer := comet.NewConsumer(client, comet.ConsumerOptions{
		Group: fmt.Sprintf("reader-%s", id),
	})
	defer consumer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	totalRead := 0
	latencies := []time.Duration{}

	for ctx.Err() == nil {
		start := time.Now()
		messages, err := consumer.Read(ctx, []uint32{1}, 100)
		if err != nil {
			log.Printf("Reader %s error: %v", id, err)
			continue
		}

		latency := time.Since(start)
		if len(messages) > 0 {
			latencies = append(latencies, latency)
			totalRead += len(messages)

			// Verify data integrity
			for _, msg := range messages {
				var data map[string]interface{}
				if err := json.Unmarshal(msg.Data, &data); err != nil {
					log.Printf("Reader %s: corrupted message: %v", id, err)
				}
			}

			// Ack messages
			for _, msg := range messages {
				consumer.Ack(ctx, msg.ID)
			}
		} else {
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Calculate average latency
	var avgLatency time.Duration
	if len(latencies) > 0 {
		var total time.Duration
		for _, l := range latencies {
			total += l
		}
		avgLatency = total / time.Duration(len(latencies))
	}

	log.Printf("Reader %s completed: read %d entries, avg latency: %v", id, totalRead, avgLatency)
}

func runBenchmark(dir, id string, duration time.Duration) {
	config := comet.MultiProcessConfig()
	client, err := comet.NewClientWithConfig(dir, config)
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	streamName := fmt.Sprintf("bench:v1:shard:%04x", os.Getpid()%16)

	// Prepare batch
	batch := make([][]byte, 100)
	for i := 0; i < 100; i++ {
		entry := map[string]interface{}{
			"writer_id": id,
			"sequence":  i,
			"timestamp": time.Now().UnixNano(),
			"data":      fmt.Sprintf("benchmark data entry %d with some padding to make it realistic size", i),
		}
		data, _ := json.Marshal(entry)
		batch[i] = data
	}

	count := 0
	start := time.Now()

	for ctx.Err() == nil {
		if _, err := client.Append(ctx, streamName, batch); err != nil {
			log.Printf("Benchmark %s append error: %v", id, err)
		} else {
			count += len(batch)
		}
	}

	elapsed := time.Since(start)
	rate := float64(count) / elapsed.Seconds()
	log.Printf("Benchmark %s completed: wrote %d entries in %v (%.0f entries/sec)", id, count, elapsed, rate)
}

func runRetentionTest() {
	// Parse retention test specific flags
	retentionCmd := flag.NewFlagSet("retention-test", flag.ExitOnError)
	dir := retentionCmd.String("dir", "", "Data directory")
	workerID := retentionCmd.Int("worker-id", 0, "Worker ID")
	streamName := retentionCmd.String("stream", "", "Stream name")

	// Skip the command name
	retentionCmd.Parse(os.Args[2:])

	if *dir == "" || *streamName == "" {
		log.Fatal("--dir and --stream are required")
	}

	// Use same config as test
	config := comet.MultiProcessConfig()
	config.Retention.MaxAge = 100 * time.Millisecond
	config.Retention.MinFilesToKeep = 0

	client, err := comet.NewClientWithConfig(*dir, config)
	if err != nil {
		log.Fatalf("Worker %d: failed to create client: %v", *workerID, err)
	}
	defer client.Close()

	// Don't write any data - writing would create a new file and interfere with retention testing
	// But we need to read to load the shard
	consumer := comet.NewConsumer(client, comet.ConsumerOptions{
		Group: fmt.Sprintf("retention-worker-%d", *workerID),
	})

	// Read one message to ensure shard is loaded
	ctx := context.Background()
	_, err = consumer.Read(ctx, []uint32{1}, 1)
	if err != nil && err.Error() != "no messages available" {
		log.Printf("Worker %d: read error (expected): %v", *workerID, err)
	}
	consumer.Close()

	log.Printf("Worker %d: triggering retention cleanup", *workerID)

	// Get retention stats before - should now show files
	statsBefore := client.GetRetentionStats()
	log.Printf("Worker %d: files before retention: %d", *workerID, statsBefore.TotalFiles)

	client.ForceRetentionCleanup()

	// Get retention stats after
	statsAfter := client.GetRetentionStats()
	log.Printf("Worker %d: files after retention: %d (deleted %d)", *workerID, statsAfter.TotalFiles, statsBefore.TotalFiles-statsAfter.TotalFiles)

	log.Printf("Worker %d: retention cleanup completed", *workerID)
}

func runIndexRebuildTest() {
	// Parse index rebuild test specific flags
	rebuildCmd := flag.NewFlagSet("index-rebuild-test", flag.ExitOnError)
	dir := rebuildCmd.String("dir", "", "Data directory")
	streamName := rebuildCmd.String("stream", "", "Stream name")
	initialFiles := rebuildCmd.Int("initial-files", 0, "Expected initial file count")
	initialEntries := rebuildCmd.Int("initial-entries", 0, "Expected initial entry count")

	// Skip the command name
	rebuildCmd.Parse(os.Args[2:])

	if *dir == "" || *streamName == "" {
		log.Fatal("--dir and --stream are required")
	}

	log.Printf("Starting index rebuild test in separate process")
	log.Printf("Expected: %d files, %d entries", *initialFiles, *initialEntries)

	config := comet.MultiProcessConfig()
	client, err := comet.NewClientWithConfig(*dir, config)
	if err != nil {
		log.Fatalf("Failed to create client (this should trigger index rebuild): %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Access shard to verify the index was rebuilt
	// This will trigger the getOrCreateShard call which should rebuild if needed
	time.Sleep(100 * time.Millisecond) // Give a moment for initialization

	// Verify we can read the existing data
	consumer := comet.NewConsumer(client, comet.ConsumerOptions{
		Group: "rebuild-test-worker",
	})
	defer consumer.Close()

	messages, err := consumer.Read(ctx, []uint32{1}, 50)
	if err != nil {
		log.Fatalf("Failed to read after index rebuild: %v", err)
	}

	log.Printf("Successfully read %d messages after index rebuild", len(messages))

	// Verify we can write new data after rebuild
	for i := 0; i < 3; i++ {
		data := []byte(fmt.Sprintf(`{"id": %d, "test": "post_rebuild_write", "process": "worker"}`, i))
		_, err := client.Append(ctx, *streamName, [][]byte{data})
		if err != nil {
			log.Fatalf("Failed to write after index rebuild: %v", err)
		}
	}

	log.Printf("Successfully wrote new data after index rebuild")

	fmt.Printf("Index rebuild test completed successfully: read %d messages, wrote 3 new entries\n", len(messages))
}

func runRetentionDebug() {
	// Parse retention debug flags
	debugCmd := flag.NewFlagSet("retention-debug", flag.ExitOnError)
	dir := debugCmd.String("dir", "", "Data directory")
	streamName := debugCmd.String("stream", "", "Stream name")

	// Skip the command name
	debugCmd.Parse(os.Args[2:])

	if *dir == "" || *streamName == "" {
		log.Fatal("--dir and --stream are required")
	}

	log.Printf("=== RETENTION DEBUG WORKER ===")

	config := comet.MultiProcessConfig()
	config.Retention.MaxAge = 100 * time.Millisecond
	config.Retention.MinFilesToKeep = 0
	config.Retention.ProtectUnconsumed = false

	client, err := comet.NewClientWithConfig(*dir, config)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Write a trigger entry to ensure shard is loaded
	ctx := context.Background()
	data := []byte(`{"trigger": "retention-debug"}`)
	_, err = client.Append(ctx, *streamName, [][]byte{data})
	if err != nil {
		log.Printf("Failed to write trigger entry: %v", err)
	}

	// Get retention stats
	statsBefore := client.GetRetentionStats()
	log.Printf("Stats before retention:")
	log.Printf("  - TotalFiles: %d", statsBefore.TotalFiles)
	log.Printf("  - TotalSizeBytes: %d", statsBefore.TotalSizeBytes)

	// Run retention
	log.Printf("Triggering retention cleanup...")
	client.ForceRetentionCleanup()

	// Get retention stats after
	statsAfter := client.GetRetentionStats()
	log.Printf("Stats after retention:")
	log.Printf("  - TotalFiles: %d (deleted: %d)", statsAfter.TotalFiles, statsBefore.TotalFiles-statsAfter.TotalFiles)
	log.Printf("  - TotalSizeBytes: %d", statsAfter.TotalSizeBytes)
}
