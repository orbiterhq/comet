package comet

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// BenchmarkShardStress tests extreme shard/file size combinations
// Each test is limited to ~3GB total memory mapped capacity to ensure fair comparison
func BenchmarkShardStress(b *testing.B) {
	const maxTotalCapacityGB = 3 // 3GB max capacity per test

	// Build configs with proportional file sizes to stay within memory budget
	shardCounts := []int{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024}

	configs := make([]struct {
		name     string
		shards   int
		fileSize int64
		desc     string
	}, 0)

	for _, shards := range shardCounts {
		// Calculate file size to keep total capacity at 3GB
		fileSizeMB := (maxTotalCapacityGB * 1024) / shards
		if fileSizeMB < 4 {
			fileSizeMB = 4 // Minimum 4MB files
		}
		if fileSizeMB > 1024 {
			fileSizeMB = 1024 // Cap at 1GB per file
		}

		name := fmt.Sprintf("%d_shards_%dMB", shards, fileSizeMB)
		desc := fmt.Sprintf("%d shards with %dMB files (%.1fGB total capacity)",
			shards, fileSizeMB, float64(shards*fileSizeMB)/1024)

		configs = append(configs, struct {
			name     string
			shards   int
			fileSize int64
			desc     string
		}{
			name:     name,
			shards:   shards,
			fileSize: int64(fileSizeMB) << 20,
			desc:     desc,
		})
	}

	// Add special case that user specifically requested
	configs = append(configs, struct {
		name     string
		shards   int
		fileSize int64
		desc     string
	}{
		name:     "256_shards_10MB_user_requested",
		shards:   256,
		fileSize: 10 << 20,
		desc:     "256 shards with 10MB files (2.5GB total) - user requested",
	})

	threadCounts := []int{1, 16, 64, 256}

	for _, cfg := range configs {
		for _, threads := range threadCounts {
			if threads > cfg.shards*2 && threads > 64 {
				continue // Skip unrealistic thread counts
			}

			name := fmt.Sprintf("%s/threads_%d", cfg.name, threads)
			b.Run(name, func(b *testing.B) {
				dir := b.TempDir()
				config := DefaultCometConfig()
				config.Storage.MaxFileSize = cfg.fileSize
				config.Storage.CheckpointInterval = 60 * time.Second // Disable time-based

				// Scale checkpoint/flush with file size
				entriesPerMB := 1024 // ~1KB entries
				entriesPerFile := int(cfg.fileSize>>20) * entriesPerMB
				config.Storage.FlushEntries = entriesPerFile / 20      // Flush every 5% of file
				config.Storage.CheckpointEntries = entriesPerFile / 10 // Checkpoint every 10% of file

				// For very small files, ensure reasonable minimums
				if config.Storage.FlushEntries < 1000 {
					config.Storage.FlushEntries = 1000
				}
				if config.Storage.CheckpointEntries < 2000 {
					config.Storage.CheckpointEntries = 2000
				}

				// Adjust reader config for memory budget
				// Allow up to 2 files per shard but cap based on memory
				config.Reader.MaxMappedFiles = cfg.shards * 2
				maxPossibleFiles := int(maxTotalCapacityGB<<30) / int(cfg.fileSize)
				if config.Reader.MaxMappedFiles > maxPossibleFiles {
					config.Reader.MaxMappedFiles = maxPossibleFiles
				}
				if config.Reader.MaxMappedFiles > 1000 {
					config.Reader.MaxMappedFiles = 1000 // Hard cap
				}

				// Set memory budget to match our 3GB limit
				config.Reader.MaxMemoryBytes = maxTotalCapacityGB << 30

				client, err := NewClient(dir, config)
				if err != nil {
					b.Fatal(err)
				}
				defer client.Close()

				ctx := context.Background()
				entry := []byte(`{"id":1,"msg":"test data that is realistic in size"}`)

				// Pre-create some shards
				preCreateCount := cfg.shards
				if preCreateCount > 100 {
					preCreateCount = 100 // Don't pre-create too many
				}
				for i := 0; i < preCreateCount; i++ {
					streamName := ShardStreamName("test:v1", uint32(i))
					client.Append(ctx, streamName, [][]byte{entry})
				}

				b.ResetTimer()
				b.ReportAllocs()

				// Run concurrent workload
				start := time.Now()
				var wg sync.WaitGroup
				opsPerThread := b.N / threads
				if opsPerThread < 1 {
					opsPerThread = 1
				}

				for t := 0; t < threads; t++ {
					wg.Add(1)
					go func(threadID int) {
						defer wg.Done()
						for i := 0; i < opsPerThread; i++ {
							// Distribute across shards
							key := fmt.Sprintf("t%d-op%d", threadID, i)
							shardID := client.PickShard(key, uint32(cfg.shards))
							streamName := ShardStreamName("test:v1", shardID)

							if _, err := client.Append(ctx, streamName, [][]byte{entry}); err != nil {
								b.Error(err)
								return
							}
						}
					}(t)
				}

				wg.Wait()
				duration := time.Since(start)

				// Calculate and report metrics
				totalOps := threads * opsPerThread
				opsPerSec := float64(totalOps) / duration.Seconds()
				latencyUs := duration.Microseconds() / int64(totalOps)

				b.ReportMetric(opsPerSec, "ops/sec")
				b.ReportMetric(float64(latencyUs), "latency_us")
				b.ReportMetric(float64(cfg.shards), "shards")
				b.ReportMetric(float64(cfg.fileSize)/1e6, "file_MB")
				b.ReportMetric(float64(threads), "threads")

				// Report system metrics
				stats := client.GetStats()
				b.ReportMetric(float64(stats.FileRotations), "rotations")
				b.ReportMetric(float64(config.Reader.MaxMappedFiles), "max_mapped")

				// Calculate overhead metrics
				avgOpsPerShard := float64(totalOps) / float64(cfg.shards)
				b.ReportMetric(avgOpsPerShard, "ops_per_shard")
			})
		}
	}
}

// BenchmarkShardCreationOverhead tests the cost of creating many shards
func BenchmarkShardCreationOverhead(b *testing.B) {
	shardCounts := []int{1, 10, 100, 1000}

	for _, count := range shardCounts {
		b.Run(fmt.Sprintf("create_%d_shards", count), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				dir := b.TempDir()
				config := DefaultCometConfig()
				config.Storage.MaxFileSize = 10 << 20 // 10MB files

				client, err := NewClient(dir, config)
				if err != nil {
					b.Fatal(err)
				}

				ctx := context.Background()
				entry := []byte("test")

				b.StartTimer()

				// Time how long it takes to create N shards
				start := time.Now()
				for shard := 0; shard < count; shard++ {
					streamName := ShardStreamName("test:v1", uint32(shard))
					if _, err := client.Append(ctx, streamName, [][]byte{entry}); err != nil {
						b.Fatal(err)
					}
				}
				createTime := time.Since(start)

				b.StopTimer()
				client.Close()

				b.ReportMetric(float64(createTime.Microseconds())/float64(count), "us_per_shard")
				b.ReportMetric(float64(count), "shards")
			}
		})
	}
}
