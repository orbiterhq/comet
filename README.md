# ☄️ Comet

High-performance embedded segmented log for edge observability. Built for single-digit microsecond latency and bounded resources.

[**Architecture Guide**](ARCHITECTURE.md) | [**Performance Guide**](PERFORMANCE.md) | [**Troubleshooting**](TROUBLESHOOTING.md) | [**Security**](SECURITY.md) | [**API Reference**](https://pkg.go.dev/github.com/orbiterhq/comet)

## What is Comet?

Comet is a segmented append-only log optimized for observability data (metrics, logs, traces) at edge locations. It implements the same pattern as Kafka's storage engine - append-only segments with time/size-based retention - but embedded directly in your service with aggressive deletion policies for resource-constrained environments.

Each shard maintains a series of immutable segment files that are rotated at size boundaries and deleted based on retention policies, ensuring predictable resource usage without the complexity of circular buffers or in-place overwrites.

Comet requires local filesystems (ext4, xfs, etc.) for its microsecond latency guarantees. It is unapologetically _local_. If you need distributed storage,
use a proper distributed system like NATS JetStream or Kafka instead.

### The Edge Storage Problem

Edge deployments need local observability buffering, but other solutions fall short:

- **Kafka**: Requires clusters, complex ops, ~1-5ms latency
- **RocksDB**: Single-threaded writes, 50-200μs writes, 100ms+ during compaction stalls
- **Redis**: Requires separate server, memory-only without persistence config
- **Ring buffers**: No persistence, no compression, data loss on overflow
- **Files + rotation**: No indexing, no consumer tracking, manual everything

**The gap**: No embedded solution with Kafka's reliability at microsecond latencies.

## Features

- **Ultra-low latency**: 1.7μs single-process, 33μs multi-process writes
- **Predictable performance**: No compaction stalls or write amplification like LSM-trees
- **True multi-process support**: Hybrid coordination (mmap + file locks), crash-safe rotation, real OS processes
- **O(log n) lookups**: Binary searchable index with bounded memory usage
- **Lock-free reads**: Atomic pointers, zero-copy via mmap with memory safety
- **Automatic retention**: Time and size-based cleanup, protects unconsumed data
- **Production ready**: Crash recovery, built-in metrics, extensive testing
- **Smart sharding**: Consistent hashing, automatic discovery, batch optimizations
- **Optional zstd compression**: ~37% storage savings when needed

## Multi-Process Coordination

Unlike other embedded solutions, Comet enables **true multi-process coordination**.
Perfect for prefork web servers like Go Fiber.

- **Hybrid coordination strategy** - Memory-mapped atomics for high-frequency operations, OS file locks for critical sections
- **Crash-safe rotation** - File locks provide automatic cleanup when processes crash
- **Real process testing** - Spawns actual OS processes, not just goroutines
- **33μs write latency** - vs. 4,470x with traditional file locking

## How Does Comet Compare?

| Feature              | Comet                      | Kafka               | Redis Streams      | RocksDB            | Proof                                         |
| -------------------- | -------------------------- | ------------------- | ------------------ | ------------------ | --------------------------------------------- |
| **Write Latency**    | 1.7μs (33μs multi-process) | 1-5ms               | 50-100μs           | 50-200μs           | [Code](benchmarks_test.go#L22)                |
| **Multi-Process**    | ✅ Real OS processes       | ✅ Distributed      | ❌ Single process  | ⚠️ Mutex locks      | [Test](multiprocess_simple_test.go#L101)      |
| **Resource Bounds**  | ✅ Time & size limits      | ⚠️ JVM heap          | ⚠️ Memory only      | ⚠️ Manual compact   | [Retention](retention.go#L144-L196)           |
| **Crash Recovery**   | ✅ Automatic               | ✅ Replicas         | ⚠️ AOF/RDB          | ✅ WAL             | [Test](multiprocess_integration_test.go#L154) |
| **Zero Copy Reads**  | ✅ mmap                    | ❌ Network          | ❌ Serialization   | ❌ Deserialization | [Code](reader.go#L89)                         |
| **Storage Overhead** | ~12 bytes/entry            | ~50 bytes/entry     | ~20 bytes/entry    | ~30 bytes/entry    | [Format](ARCHITECTURE.md#wire-format)         |
| **Sharding**         | ✅ Built-in                | ✅ Partitions       | ❌ Manual          | ❌ Manual          | [Code](client.go#L776)                        |
| **Compression**      | ✅ Optional zstd           | ✅ Multiple codecs  | ❌ None            | ✅ Multiple        | [Code](benchmarks_test.go#L283)               |
| **Embedded**         | ✅ Native                  | ❌ Requires cluster | ❌ Requires server | ✅ Native          | -                                             |

## Quick Start

### The Easy Way™

**Step 1: Create a client**

```go
client, err := comet.NewClient("/var/lib/comet")
defer client.Close()
```

**Step 2: Write your events**

```go
// Pick which shard to write to based on a key (for consistent routing)
// High cardinality keys (e.g. uuid) are recommended for consistent routing
stream := comet.PickShardStream(event.ID, "events", "v1", 16)
// This returns something like "events:v1:shard:0007" based on hash(userID) % 16

ids, err := client.Append(ctx, stream, [][]byte{
    []byte(event.ToJSON()),
})
```

**Step 3: Process events**

```go
consumer := comet.NewConsumer(client, comet.ConsumerOptions{
    Group: "my-processor",
})

// Process() is the main API - it handles everything for you!
// By default, it discovers and processes ALL shards automatically
err = consumer.Process(ctx, func(ctx context.Context, messages []comet.StreamMessage) error {
    for _, msg := range messages {
        processEvent(msg.Data)  // Your logic here
    }
    return nil  // Success = automatic progress tracking
})
```

### That's it! Comet handles:

- ✅ **Compression** - Large events compressed automatically
- ✅ **Sharding** - Load distributed across 16 shards
- ✅ **Retries** - Failed batches retry automatically
- ✅ **Progress** - Consumer offsets tracked per shard
- ✅ **Cleanup** - Old data deleted automatically
- ✅ **Recovery** - Crash? Picks up where it left off

### Want more control? Scale horizontally:

```go
// Deploy this same code across 3 processes:
err = consumer.Process(ctx, processEvents,
    comet.WithStream("events:v1:shard:*"),
    comet.WithConsumerAssignment(workerID, numWorkers),  // This worker + total count
)
// Each worker processes different shards automatically!
// No coordination needed - Comet handles it
```

### Production-Ready Example

```go
// Define your processing function with context support
processEvents := func(ctx context.Context, messages []comet.StreamMessage) error {
    for _, msg := range messages {
        // Check for cancellation
        if ctx.Err() != nil {
            return ctx.Err()
        }
        // Process each message
        if err := handleEvent(msg.Data); err != nil {
            return err // Will trigger retry
        }
    }
    return nil
}

err = consumer.Process(ctx, processEvents,
    comet.WithStream("events:v1:shard:*"),
    comet.WithBatchSize(1000),
    comet.WithPollInterval(50 * time.Millisecond),

    // Optional: Add observability
    comet.WithErrorHandler(func(err error, retryCount int) {
        metrics.Increment("comet.errors", 1)
        log.Printf("Retry %d: %v", retryCount, err)
    }),
    comet.WithBatchCallback(func(size int, duration time.Duration) {
        metrics.Histogram("comet.batch.size", float64(size))
        metrics.Histogram("comet.batch.duration_ms", duration.Milliseconds())
    }),
)
```

### Need to tweak something?

```go
// Only override what you need:
config := comet.DefaultCometConfig()
config.Retention.MaxAge = 24 * time.Hour  // Keep data longer
client, err := comet.NewClientWithConfig("/var/lib/comet", config)

// Or use a preset:
config = comet.HighCompressionConfig()      // Optimize for storage
config = comet.MultiProcessConfig()         // For prefork deployments
config = comet.HighThroughputConfig()       // For maximum write speed
```

#### Configuration Structure

```go
type CometConfig struct {
    Compression CompressionConfig  // Controls compression behavior
    Indexing    IndexingConfig     // Controls indexing and lookup
    Storage     StorageConfig      // Controls file storage
    Concurrency ConcurrencyConfig  // Controls multi-process behavior
    Retention   RetentionConfig    // Controls data retention
}
```

## Architecture

```
┌─────────────────┐     ┌─────────────────┐
│   Your Service  │     │  Your Service   │
│                 │     │                 │
│  ┌───────────┐  │     │  ┌───────────┐  │
│  │   Comet   │  │     │  │   Comet   │  │
│  │  Writer   │  │     │  │  Reader   │  │
│  └─────┬─────┘  │     │  └─────┬─────┘  │
└────────┼────────┘     └────────┼────────┘
         │                       │
         ▼                       ▼
    ┌──────────────────────────────────┐
    │      Segmented Log Storage       │
    │                                  │
    │  Shard 0: [seg0][seg1][seg2]→    │
    │  Shard 1: [seg0][seg1]→          │
    │  ...                             │
    │                                  │
    │  ↓ segments deleted by retention │
    └──────────────────────────────────┘
```

## Performance Optimizations

Comet achieves microsecond-level latency through careful optimization:

1. **Lock-Free Reads**: Memory-mapped files with atomic pointers and defensive copying for memory safety
2. **I/O Outside Locks**: Compression and disk writes happen outside critical sections
3. **Binary Searchable Index**: O(log n) entry lookups instead of linear scans
4. **Vectored I/O**: Batches multiple writes into single syscalls
5. **Batch ACKs**: Groups acknowledgments by shard to minimize lock acquisitions
6. **Pre-allocated Buffers**: Reuses buffers to minimize allocations
7. **Concurrent Shards**: Each shard has independent locks for parallel operations
8. **Memory-Mapped Multi-Process Coordination**: Direct memory writes with atomic sequence allocation

## How It Works

1. **Append-Only Segments**: Data is written to segment files that grow up to `MaxFileSize`
2. **Segment Rotation**: When a segment reaches max size, it's closed and a new one starts
3. **Binary Index**: Entry locations are indexed for O(log n) lookups with bounded memory
4. **Retention**: Old segments are deleted based on age (`MaxAge`) or total size limits
5. **Sharding**: Load is distributed across multiple independent segmented logs
6. **Index Limits**: Index memory is capped by `MaxIndexEntries` - older entries are pruned

## Resource Usage

With default settings:

- **Memory**: ~2MB per shard (10k index entries × ~200 bytes/entry)
- **Disk**: Bounded by retention policy (MaxShardSize × shard count)
- **CPU**: Minimal - compression happens outside locks

Example: 16 shards × 2MB index = 32MB memory for indexes
Example: 16 shards × 1GB/shard = 16GB max disk usage

## Sharding

Comet uses deterministic sharding for load distribution. Here's how it works:

### Writing

```go
// The key (event ID, user ID, tenant, etc.) determines which shard gets the data
stream := comet.PickShardStream(event.ID, "events", "v1", 16)
// Returns "events:v1:shard:0007" (hash("user-123") % 16 = 7)

// The key is ONLY used for routing - it's not stored anywhere!
client.Append(ctx, stream, data)
```

### Reading

```go
// Process ALL shards (recommended)
err = consumer.Process(ctx, handler,
    comet.WithStream("events:v1:shard:*"))  // The * wildcard finds all shards

// Process specific shards only
err = consumer.Process(ctx, handler,
    comet.WithShards(0, 1, 2))  // Only process shards 0, 1, and 2

// Process with advanced options
err = consumer.Process(ctx, handler,
    comet.WithStream("events:v1:shard:*"),
    comet.WithBatchSize(1000),
    comet.WithConsumerAssignment(workerID, 3))  // This is worker 'workerID' of 3 total workers
```

## Use Cases

✅ **Right tool for:**

- Edge deployments with limited storage
- High-frequency observability data (metrics, logs, traces)
- Recent data access patterns (debugging last N hours)
- Local buffering before shipping to cloud
- Multi-service nodes requiring predictable resource usage

❌ **Not for:**

- Network filesystems (NFS, CIFS, etc.)
- Long-term storage (use S3/GCS)
- Transactional data requiring ACID
- Random access patterns
- Complex queries or aggregations

## Performance

Benchmarked on Apple M2 with SSD (see [Performance Guide](PERFORMANCE.md) for detailed analysis):

### Single-Process Mode (default)

Optimized for single-process deployments with best performance:

- **Single entry**: 1.7μs latency (594k entries/sec)
- **10-entry batch**: 2.7μs per batch (3.6M entries/sec)
- **100-entry batch**: 9.0μs per batch (11.1M entries/sec)
- **1000-entry batch**: 112μs per batch (8.9M entries/sec)
- **10000-entry batch**: 548μs per batch (18.2M entries/sec)

### Multi-Process Mode

For prefork/multi-process deployments with memory-mapped coordination:

- **Single entry**: 33μs latency (30k entries/sec) - ultra-fast for multi-process!
- **10-entry batch**: 35μs per batch (283k entries/sec)
- **100-entry batch**: 56μs per batch (1.8M entries/sec)
- **1000-entry batch**: 170μs per batch (5.9M entries/sec)
- **10000-entry batch**: 2.0ms per batch (5.0M entries/sec)

**Note on Multi-Process Latency**: While single-entry writes are ~19x slower in multi-process mode (33μs vs 1.7μs), this difference is often irrelevant in production:

- **With async batching**: If you're buffering writes (like most ingest services), the latency is hidden from your request path
- **With large batches**: At 1000-entry batches, multi-process is only ~1.5x slower per batch (170μs vs 112μs)
- **With prefork benefits**: You gain linear CPU scaling, process isolation, and crash resilience

**When the 33μs matters**: Direct, synchronous writes where every microsecond counts
**When it doesn't**: HTTP APIs, batched ingestion, async workers, or any pattern that decouples the write from the request

### Other Performance Metrics

- **ACK performance**: 30ns per ACK (34M ACKs/sec) with batch optimization
- **Memory efficiency**: Zero allocations for ACKs, 5 allocations per write batch
- **Multi-process coordination**: Memory-mapped atomic operations for lock-free sequence allocation
- **Storage overhead**: 12 bytes per entry (4-byte length + 8-byte timestamp)

## Configuration

### Single-Process vs Multi-Process Mode

Comet defaults to single-process mode for optimal single-entry performance. Enable multi-process mode when needed:

```go
// Single-process mode (default) - fastest performance
client, err := comet.NewClient("/data/streams")

// Multi-process mode (EXPERIMENTAL) - for prefork/multi-process deployments
config := comet.DefaultCometConfig()
config.Concurrency.EnableMultiProcessMode = true
client, err := comet.NewClientWithConfig("/data/streams", config)
```

**⚠️ IMPORTANT: Multi-Process Mode Limitations**

1. **Experimental Status**: Multi-process mode is experimental and may have edge cases
2. **Mode Switching**: You CANNOT switch between single-process and multi-process modes on the same data directory. They use incompatible on-disk formats
3. **Performance Trade-off**: 19x slower for single writes (33μs vs 1.7μs)
4. **Known Issues**: Some race conditions exist in aggressive retention scenarios

**When to use multi-process mode:**

- Process isolation is critical
- You're already batching writes (reduces the latency impact)
- Using prefork web servers and need true process separation
- Can tolerate experimental features

**When to use single-process mode (recommended):**

- Need the lowest possible latency
- Want maximum reliability
- Don't need process-level isolation
- Writing less than 500k entries/second

## License

MIT
