# ☄️ Comet

High-performance embedded segmented log for edge observability. Built for single-digit microsecond latency and bounded resources.

[**Architecture Guide**](ARCHITECTURE.md) | [**Performance Guide**](PERFORMANCE.md) | [**Troubleshooting**](TROUBLESHOOTING.md) | [**Security**](SECURITY.md) | [**API Reference**](https://pkg.go.dev/github.com/orbiterhq/comet)

## What is Comet?

Comet is a segmented append-only log optimized for observability data (metrics, logs, traces) at edge locations. It implements the same pattern as Kafka's storage engine - append-only segments with time/size-based retention - but embedded directly in your service with aggressive deletion policies for resource-constrained environments.

Each shard maintains a series of immutable segment files that are rotated at size boundaries and deleted based on retention policies, ensuring predictable resource usage without the complexity of circular buffers or in-place overwrites.

### The Edge Storage Problem

Edge deployments need local observability buffering, but:

- **Kafka**: Requires clusters, complex ops, ~1-5ms latency
- **RocksDB**: Single-threaded writes, 10-50μs best case, 100ms during stalls
- **SQLite**: Unbounded growth, no sharding, ~100μs+ latency
- **Ring buffers**: No persistence, no compression, data loss on overflow
- **Files + rotation**: No indexing, no consumers, manual everything

**The gap**: No embedded solution with Kafka's reliability at ring buffer speeds.

## Features

- **Ultra-low latency** writes (1.7μs single-process mode, 32μs multi-process mode)
- **Multi-process coordination** with lock-free mmap-based coordination and atomic operations
- **Zero allocations** in the hot path
- **O(log n) entry lookup** with binary searchable index
- **Lock-free reads** with atomic memory-mapped segments
- **Crash resilient** with automatic recovery and durable consumer offsets
- **Resource bounded** with configurable retention policies
- **Built-in metrics** for monitoring and observability
- **Memory-efficient** with binary index format (8.8x smaller than JSON)
- **Smart sharding** with consistent hashing for load distribution
- **Batch optimizations** for high-throughput scenarios
- **Optional compression** with zstd (saves ~37% storage, disabled by default for speed)

## Multi-Process Coordination

Unlike other embedded solutions, Comet enables **true multi-process coordination**.
Perfect for prefork web servers like Go Fiber.

- **Memory-mapped coordination** - Lock-free atomic operations for sequence allocation
- **Zero-copy writes** - Direct memory writes to mapped files bypass syscalls
- **32μs write latency** - 237x faster than original multi-process implementation
- **Real process testing** - Spawns actual OS processes, not just goroutines

## Key Benefits

**1. Drop-in simplicity** - No configuration needed. No external dependencies. No operational burden.

**2. Predictable performance** - Bounded memory usage, consistent microsecond latency, automatic resource cleanup.

**3. Edge-optimized** - Designed for resource-constrained environments where every byte and microsecond matters.

**4. Battle-tested patterns** - Implements Kafka's proven segmented log design, adapted for embedded use cases.

## How Does Comet Compare?

| Feature              | Comet                      | Kafka               | Redis Streams      | SQLite             | Proof                                         |
| -------------------- | -------------------------- | ------------------- | ------------------ | ------------------ | --------------------------------------------- |
| **Write Latency**    | 1.7μs (32μs multi-process) | 1-5ms               | 50-100μs           | 100μs+             | [Code](benchmarks_test.go#L22)                |
| **Multi-Process**    | ✅ Real OS processes       | ✅ Distributed      | ❌ Single process  | ❌ File locks only | [Test](multiprocess_simple_test.go#L101)      |
| **Resource Bounds**  | ✅ Time & size limits      | ⚠️ JVM heap          | ⚠️ Memory only      | ❌ Unbounded       | [Retention](retention.go#L144-L196)           |
| **Crash Recovery**   | ✅ Automatic               | ✅ Replicas         | ⚠️ AOF/RDB          | ✅ WAL             | [Test](multiprocess_integration_test.go#L154) |
| **Zero Copy Reads**  | ✅ mmap                    | ❌ Network          | ❌ Serialization   | ❌ SQL parsing     | [Code](reader.go#L89)                         |
| **Storage Overhead** | ~12 bytes/entry            | ~50 bytes/entry     | ~20 bytes/entry    | ~100 bytes/row     | [Format](ARCHITECTURE.md#wire-format)         |
| **Embedded**         | ✅ Native                  | ❌ Requires cluster | ❌ Requires server | ✅ Native          | -                                             |
| **Sharding**         | ✅ Built-in                | ✅ Partitions       | ❌ Manual          | ❌ Manual          | [Code](client.go#L776)                        |
| **Compression**      | ✅ Optional zstd           | ✅ Multiple codecs  | ❌ None            | ❌ None            | [Code](benchmarks_test.go#L283)               |

## Quick Start

### The Easy Way™

**Step 1: Create a client**

```go
client, err := comet.NewClient("/var/lib/comet")
defer client.Close()
```

**Step 2: Write your events** (sharding handled automatically)

```go
// Comet automatically shards by user ID for optimal performance
stream := comet.PickShardStream(event.UserID, "events", "v1", 16)
ids, err := client.Append(ctx, stream, [][]byte{
    []byte(event.ToJSON()),
})
```

**Step 3: Process events** (everything automatic!)

```go
consumer := comet.NewConsumer(client, comet.ConsumerOptions{
    Group: "my-processor",
})

// This is it! Auto-discovery, auto-retry, auto-ACK, auto-everything!
err = consumer.Process(ctx, func(messages []comet.StreamMessage) error {
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

1. **Lock-Free Reads**: Memory-mapped files with atomic pointers enable concurrent reads without locks
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

Comet uses deterministic sharding for load distribution:

```go
// Smart sharding by key (consistent distribution)
stream := comet.PickShardStream("user-123", "events", "v1", 16)

// Read from all shards
shards := comet.AllShardsRange(16)
messages, err := consumer.Read(ctx, shards, 1000)

for _, msg := range messages {
    fmt.Printf("Shard: %d, Entry: %d\n", msg.ID.ShardID, msg.ID.EntryNumber)
}

// Batch acknowledgments (automatically grouped by shard)
var messageIDs []comet.MessageID
for _, msg := range messages {
    messageIDs = append(messageIDs, msg.ID)
}
consumer.Ack(ctx, messageIDs...)
```

## Delightful Features

Beyond the core functionality, Comet includes thoughtful features that make it a joy to use:

- **Auto-discovery** - Consumers automatically find all shards matching a pattern
- **Smart batching** - Automatically groups operations by shard for efficiency
- **Graceful degradation** - Continues operating even when some shards are unavailable
- **Zero-allocation hot paths** - Critical paths optimized to avoid garbage collection
- **Instant startup** - Memory-mapped files enable sub-millisecond startup times
- **Transparent compression** - Automatically compresses large entries with zstd
- **Progress visibility** - Built-in lag tracking shows exactly how far behind consumers are

## Performance

Benchmarked on Apple M2 with SSD (see [Performance Guide](PERFORMANCE.md) for detailed analysis):

### Single-Process Mode (default)

Optimized for single-process deployments with best performance:

- **Single entry**: 1.7μs latency (588k entries/sec)
- **10-entry batch**: 0.50μs per entry (2M entries/sec)
- **100-entry batch**: 0.24μs per entry (4.1M entries/sec)
- **1000-entry batch**: 0.098μs per entry (10.2M entries/sec)

#### Compression Impact

Compression trades latency for storage savings (benchmarked with ~800 byte JSON logs):

| Operation       | Without Compression | With Compression | Slowdown | Storage Savings |
| --------------- | ------------------- | ---------------- | -------- | --------------- |
| Single Write    | 2.2μs               | 7.1μs            | 3.2x     | ~37%            |
| 10-Entry Batch  | 3.2μs/entry         | 6.2μs/entry      | 1.9x     | ~37%            |
| 100-Entry Batch | 0.78μs/entry        | 5.7μs/entry      | 7.4x     | ~37%            |

_Compression is OFF by default (threshold: 4KB) to maintain ultra-low latency._

### Multi-Process Mode

For prefork/multi-process deployments with memory-mapped coordination:

- **Single entry**: 32μs latency (31k entries/sec) - ultra-fast for multi-process!
- **10-entry batch**: 3.2μs per entry (312k entries/sec)
- **100-entry batch**: 0.32μs per entry (3.1M entries/sec)
- **1000-entry batch**: 0.032μs per entry (31M entries/sec)

### Other Performance Metrics

- **ACK performance**: 29ns per ACK (34M ACKs/sec) with batch optimization
- **Memory efficiency**: Zero allocations for ACKs, 5 allocations per write batch
- **Multi-process coordination**: Memory-mapped atomic operations for lock-free sequence allocation
- **Storage overhead**: 12 bytes per entry (4-byte length + 8-byte timestamp)

## Use Cases

✅ **Right fit for:**

- Edge deployments with limited storage
- High-frequency observability data (metrics, logs, traces)
- Recent data access patterns (debugging last N hours)
- Local buffering before shipping to cloud
- Multi-service nodes requiring predictable resource usage

❌ **Not for:**

- Long-term storage (use S3/GCS)
- Transactional data requiring ACID
- Random access patterns
- Complex queries or aggregations

## Configuration

### Single-Process vs Multi-Process Mode

Comet defaults to single-process mode for optimal performance. Enable multi-process mode when needed:

```go
// Single-process mode (default) - fastest performance
client, err := comet.NewClient("/data/streams")

// Multi-process mode - for prefork/multi-process deployments
config := comet.DefaultCometConfig()
config.Concurrency.EnableMultiProcessMode = true
client, err := comet.NewClientWithConfig("/data/streams", config)
```

**When to use multi-process mode:**

- Fiber prefork mode or similar multi-process web servers
- Multiple processes writing to the same stream files
- Distributed workers on the same machine
- When you need cross-process coordination

**When to use single-process mode (default):**

- Single application instance
- Microservices with dedicated storage
- Maximum performance requirements
- Containerized deployments with process isolation

## License

MIT
