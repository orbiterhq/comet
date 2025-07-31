# Comet Performance Guide

This guide helps you achieve maximum performance with Comet for your specific use case. For the architectural reasons behind these optimizations, see [ARCHITECTURE.md](ARCHITECTURE.md).

## Quick Performance Numbers

| Operation        | Single-Process | Multi-Process | Entries/Second |
| ---------------- | -------------- | ------------- | -------------- |
| Single Write     | 1.66μs         | 2.42μs        | 602K / 413K    |
| 10-Entry Batch   | 0.50μs/entry   | 0.30μs/entry  | 2M / 3.3M      |
| 100-Entry Batch  | 0.25μs/entry   | 0.25μs/entry  | 4M / 4M        |
| 1000-Entry Batch | 0.10μs/entry   | 0.10μs/entry  | 10M / 10M      |

### Compression Impact (for ~800 byte JSON logs)

| Operation       | Without Compression | With Compression | Slowdown | Storage Savings |
| --------------- | ------------------- | ---------------- | -------- | --------------- |
| Single Write    | 2.2μs               | 7.1μs            | 3.2x     | ~37%            |
| 10-Entry Batch  | 3.2μs/entry         | 6.2μs/entry      | 1.9x     | ~37%            |
| 100-Entry Batch | 0.78μs/entry        | 5.7μs/entry      | 7.4x     | ~37%            |

_Benchmarked on: macOS 15.3.2 (24D81), Apple M2 MacBook Air, Go 1.24.5_

**Compression defaults to OFF for entries <4KB** to maintain low latency for typical logs.

**Key Insight**: Batching is critical. Going from single writes to 1000-entry batches provides a 17x improvement.

## Performance Profiles

### Ultra-Low Latency Profile (<2μs)

For real-time systems requiring minimum latency:

```go
config := comet.DefaultCometConfig()
config.Compression.MinCompressSize = 1<<20  // 1MB - effectively disable
config.Storage.CheckpointTime = 10000       // 10 seconds
config.Indexing.BoundaryInterval = 1000     // Less frequent indexing

// Use small batches for responsiveness
consumer.Process(ctx, handler,
    comet.WithBatchSize(10),
    comet.WithPollInterval(1*time.Millisecond),
)
```

**Trade-offs**: Higher disk usage, more memory for index

### Maximum Throughput Profile (>10M entries/sec)

For batch processing and high-volume ingestion:

```go
config := comet.HighThroughputConfig()  // Pre-configured for throughput

// Use large batches
consumer.Process(ctx, handler,
    comet.WithBatchSize(1000),
    comet.WithPollInterval(100*time.Millisecond),
)
```

**Best practices**:

- Batch writes in groups of 1000+
- Use multiple shards (16-32)
- Enable compression for network-sourced data

### Storage Efficiency Profile

For edge devices with limited storage:

```go
config := comet.HighCompressionConfig()     // Aggressive compression
config.Retention.MaxAge = 2 * time.Hour     // Shorter retention
config.Retention.MaxShardSize = 100 << 20   // 100MB per shard
config.Compression.MinCompressSize = 256    // Compress everything >256B

// Moderate batching for compression efficiency
consumer.Process(ctx, handler,
    comet.WithBatchSize(100),
)
```

**Compression trade-offs**:

- Storage savings: ~37% for typical JSON logs
- Latency impact: 2-7x slower depending on batch size
- Default threshold: 4KB (most logs bypass compression)
- Enable for: Cold storage, bandwidth-constrained environments
- Avoid for: Real-time processing, latency-sensitive workloads

### Multi-Process Profile

For prefork web servers and shared deployments:

```go
config := comet.MultiProcessConfig()
// Additional tuning for multi-process
config.Storage.CheckpointTime = 100  // Async checkpoints every 100ms

// Each process handles different shards
consumer.Process(ctx, handler,
    comet.WithStream("events:v1:shard:*"),
    comet.WithConsumerAssignment(processID, totalProcesses),
)
```

**Multi-Process Performance Evolution**:

| Implementation             | Write Latency | vs Single-Process | Key Technology             |
| -------------------------- | ------------- | ----------------- | -------------------------- |
| Original (sync checkpoint) | 7.6ms         | 4,575x slower     | File locks + sync I/O      |
| Async checkpointing        | 3.2ms         | 1,940x slower     | Deferred index persistence |
| Memory-mapped I/O          | 32μs          | 20x slower        | Lock-free atomics + mmap   |

The latest multi-process mode achieves **32μs write latency** through:

- **Lock-free coordination**: Atomic operations for sequence allocation
- **Memory-mapped data files**: Direct memory writes bypass syscalls
- **Async index updates**: Index persistence happens in background
- **Zero-copy writes**: Data goes directly to mapped memory

This is a **237x improvement** over the original multi-process implementation!

## Benchmarking Your Workload

### Running Benchmarks

```bash
# Basic benchmarks
go test -bench=. -benchmem -benchtime=10s

# CPU profiling
go test -bench=BenchmarkBatch1000 -cpuprofile=cpu.prof
go tool pprof cpu.prof

# Memory profiling
go test -bench=BenchmarkBatch1000 -memprofile=mem.prof
go tool pprof mem.prof

# Specific scenarios
go test -bench=BenchmarkSingleWrite    # Test write latency
go test -bench=BenchmarkCompression    # Test compression impact
go test -bench=BenchmarkConsumer       # Test read throughput
```

### Writing Custom Benchmarks

```go
func BenchmarkYourWorkload(b *testing.B) {
    client, _ := comet.NewClient("/tmp/bench")
    defer client.Close()

    // Your typical data
    data := generateTypicalEntry()

    b.ResetTimer()
    b.ReportAllocs()

    for i := 0; i < b.N; i++ {
        client.Append(ctx, "stream:0000", [][]byte{data})
    }
}
```

### Interpreting Results

Look for:

- **ns/op**: Lower is better (nanoseconds per operation)
- **B/op**: Bytes allocated per operation (aim for 0)
- **allocs/op**: Number of allocations (aim for 0)

## The Counter-Intuitive Truth: Syscall-Bound is Perfect

Most engineers see "97% syscalls" and think "performance problem."
For Comet, it means we've eliminated all unnecessary work:

✅ **97% syscalls** = Actually moving data to disk
✅ **2% CPU** = Only essential operations
✅ **0% wasted cycles** = No hot loops or algorithm overhead

This is **exactly** what you want in a storage engine.

### Comet is Syscall-Bound (By Design)

CPU profiling reveals that Comet's performance profile varies by workload:

```
Single writes:     97.96% syscalls, 2% CPU work
Batch writes:      56.86% syscalls, 29% GC, 14% encoding
Compression:       92.00% syscalls, 7% compression, 1% runtime
Sequential reads:  95.28% syscalls (mostly file stat operations)
```

This workload-dependent profile is exactly what we want! Here's why:

**Single Writes** (Ultra-low latency):

- **~98%** syscalls - Minimal overhead, maximum efficiency
- **~2%** CPU work - Just essential operations

**Batch Writes** (High throughput):

- **~57%** syscalls - Still I/O bound
- **~29%** GC overhead - From larger allocations during batching
- **~14%** encoding work - Binary index operations scale with batch size

**Compression** (Storage efficiency):

- **~92%** syscalls - I/O still dominates
- **~7%** compression - zstd is highly optimized
- **~1%** runtime - Minimal Go overhead

**Reads** (Consumer workloads):

- **~95%** syscalls - Mostly file stat calls to check growth
- **~5%** runtime - Memory-mapped reads are nearly free

### Memory Profile

Memory allocations are dominated by:

- **Compression buffers** (52%) - Reused via pools
- **Test infrastructure** (38%) - Not present in production
- **Actual operations** (<10%) - Excellent efficiency

In production, Comet achieves **zero allocations** for most operations.

## Performance Tuning Checklist

### 1. Identify Your Bottleneck

**CPU Bound**:

- High compression ratio with small entries
- Many small writes instead of batches
- Excessive shard count

**I/O Bound**:

- Large uncompressed entries
- Slow storage (HDD vs SSD)
- Insufficient batching

**Lock Contention**:

- Multiple writers to same shard
- Single-entry writes
- Multi-process without proper sharding

### 2. Measure Before Optimizing

```go
// Add metrics to identify bottlenecks
stats := client.GetStats()
fmt.Printf("Compression ratio: %.2f\n", stats.CompressionRatio)
fmt.Printf("Write latency avg: %v\n", stats.AvgWriteLatency)
fmt.Printf("Entries/sec: %.0f\n", stats.EntriesPerSecond)
```

### 3. Apply Targeted Optimizations

**For CPU bottlenecks**:

```go
config.Compression.MinCompressSize = 10240  // Only compress large entries
config.Indexing.BoundaryInterval = 1000     // Less frequent indexing
```

**For I/O bottlenecks**:

```go
config.Storage.MaxFileSize = 10 << 30       // 10GB files (fewer files)
config.Compression.MinCompressSize = 512    // More aggressive compression
```

**For lock contention**:

```go
// Use more shards
numShards := uint32(32)
shardID := comet.PickShard(key, numShards)

// Batch aggressively
entries := make([][]byte, 0, 1000)
// ... collect entries ...
client.Append(ctx, stream, entries)
```

## Hardware Considerations

### CPU

- **Compression workers**: Default 4 is good for 4-8 cores
- **More cores**: Increase shard count for parallelism
- **Weak CPU**: Disable compression or increase threshold

### Memory

- **Index size**: ~200 bytes per boundary × MaxIndexEntries
- **16 shards**: ~32MB for indexes (with defaults)
- **Compression buffers**: ~10MB with 4 workers
- **Total estimate**: 50-100MB for typical deployment

### Storage

**SSD Recommended**:

- Sequential write: 500MB/s+
- Random read: High IOPS for consumer seeks
- Low latency: <0.1ms

**HDD Considerations**:

```go
config.Storage.MaxFileSize = 10 << 30       // Larger files
config.Storage.CheckpointTime = 5000        // Less frequent syncs
config.Compression.MinCompressSize = 256    // Aggressive compression
```

**NVMe Optimizations**:

```go
// Can handle more aggressive settings
config.Storage.CheckpointTime = 500         // More frequent checkpoints
config.Indexing.MaxIndexEntries = 100000    // Larger index OK
```

## Common Performance Pitfalls

### 1. Single-Entry Writes

```go
// SLOW - 1.66μs per entry
for _, entry := range entries {
    client.Append(ctx, stream, [][]byte{entry})
}

// FAST - 0.098μs per entry
client.Append(ctx, stream, entries)
```

### 2. Inefficient Shard Selection

```go
// POOR - All writes to one shard
stream := "events:v1:shard:0000"

// GOOD - Distribute across shards
stream := comet.PickShardStream(userID, "events", "v1", 16)
```

### 3. Synchronous Consumer Processing

```go
// SLOW - Process one at a time
consumer.Process(ctx, func(messages []StreamMessage) error {
    for _, msg := range messages {
        slowDatabaseWrite(msg)  // Blocks batch
    }
    return nil
})

// FAST - Pipeline processing
consumer.Process(ctx, func(messages []StreamMessage) error {
    // Send to channel for async processing
    for _, msg := range messages {
        processingChan <- msg
    }
    return nil  // ACK immediately
})
```

### 4. Compression Threshold Too Low

```go
// SLOW - Compressing tiny entries
config.Compression.MinCompressSize = 10

// GOOD - Only compress where beneficial
config.Compression.MinCompressSize = 2048
```

### 5. Over-Sharding

```go
// INEFFICIENT - Too many shards for low volume
numShards := uint32(256)  // Overkill for <1M entries/sec

// EFFICIENT - Right-sized
numShards := uint32(16)   // Good for most workloads
```

## Performance Monitoring

### Key Metrics to Track

```go
stats := client.GetStats()

// Write performance
writeLatency := stats.AvgWriteLatency
writeThroughput := stats.TotalEntries / time.Since(startTime).Seconds()

// Compression efficiency
compressionRatio := stats.CompressionRatio
savedBytes := stats.TotalBytes - stats.TotalCompressed

// Consumer health
lag, _ := consumer.GetLag(ctx, shardID)
consumerThroughput := processedCount / time.Since(startTime).Seconds()

// System health
fileRotations := stats.FileRotations
errorRate := stats.ErrorCount / stats.TotalEntries
```

### Performance SLOs

Suggested Service Level Objectives:

| Metric            | Good   | Acceptable | Investigate |
| ----------------- | ------ | ---------- | ----------- |
| Write p50 latency | <5μs   | <10μs      | >10μs       |
| Write p99 latency | <50μs  | <100μs     | >100μs      |
| Compression ratio | >10:1  | >5:1       | <5:1        |
| Consumer lag      | <1000  | <10000     | >10000      |
| Error rate        | <0.01% | <0.1%      | >0.1%       |

## Understanding the Syscall Bottleneck

Since Comet is syscall-bound, traditional CPU optimizations won't help much. Instead:

### What Won't Help

- **More CPU cores** - Already minimal CPU usage
- **SIMD/Vectorization** - Not CPU-bound
- **Fancy data structures** - Syscalls dominate
- **Threading tricks** - I/O is the limit

### What Will Help

- **Faster storage** - NVMe > SSD > HDD
- **Batching** - Amortize syscall overhead
- **io_uring** - Reduce syscall overhead (future)
- **Larger writes** - Fewer syscalls per byte
- **File system tuning** - noatime, nodiratime

### Platform-Specific Considerations

**Linux**:

- Lower syscall overhead than macOS
- io_uring potential (future)
- Better file locking performance

**macOS** (current profiling platform):

- Higher syscall overhead
- No io_uring equivalent
- Still achieves 1.66μs latency

**Real-world Linux performance** is typically 20-30% better than macOS numbers shown.

### Performance vs. Alternatives

| System    | Write Latency | Explanation                                   |
| --------- | ------------- | --------------------------------------------- |
| Comet     | 1.66μs        | Lock-free reads, batched writes, binary index |
| Kafka     | 1-5ms         | Network + consensus + replication overhead    |
| SQLite    | 100μs+        | B-tree updates, transaction overhead, WAL     |
| Raw Files | 50μs+         | No indexing, manual coordination              |

_Why the huge difference?_ Most systems optimize for ACID or distributed consensus.
Comet optimizes purely for append-only throughput.

## Advanced Optimizations

### Custom Compression Dictionary

For domain-specific data with repetitive patterns:

```go
// Future feature - not yet implemented
config.Compression.Dictionary = buildDictionaryFromSamples(samples)
```

### NUMA Awareness

For large multi-socket systems:

```go
// Pin compression workers to NUMA nodes
// Use taskset or numactl when launching process
```

### Huge Pages

For very high throughput (>100M entries/sec):

```bash
# Enable transparent huge pages
echo always > /sys/kernel/mm/transparent_hugepage/enabled
```

### io_uring (Future)

Potentially 20-30% improvement for high-volume workloads, but adds complexity.

## Performance Recipe Book

### Recipe: 1 Million Events/Second

```go
config := comet.HighThroughputConfig()
client, _ := comet.NewClientWithConfig("/data", config)

// Use 16 shards
entries := make([][]byte, 1000)
go func() {
    for {
        // Collect 1000 entries
        stream := comet.PickShardStream(key, "events", "v1", 16)
        client.Append(ctx, stream, entries)
    }
}()
```

### Recipe: Sub-Millisecond p99 Latency

```go
config := comet.DefaultCometConfig()
config.Compression.MinCompressSize = 1<<20  // Disable compression
config.Storage.CheckpointTime = 30000       // 30 seconds

// Single shard for consistency
client.Append(ctx, "events:v1:shard:0000", [][]byte{data})
```

### Recipe: Minimum Storage Usage

```go
config := comet.HighCompressionConfig()
config.Retention.MaxAge = 1 * time.Hour
config.Compression.MinCompressSize = 128    // Aggressive

// Batch for better compression
batch := make([][]byte, 0, 100)
// ... collect 100 entries ...
client.Append(ctx, stream, batch)
```

## Conclusion

Comet's performance is highly tunable. The key principles:

1. **Batch everything** - Single largest performance win
2. **Shard appropriately** - Distribute load, but don't overdo it
3. **Compress wisely** - Balance CPU vs storage
4. **Monitor always** - Track your key metrics
5. **Benchmark specifically** - Test with your actual workload

Remember: Comet is already fast by default. Only tune if you need to optimize for specific constraints.

For architectural details on why these optimizations work, see [ARCHITECTURE.md](ARCHITECTURE.md).
