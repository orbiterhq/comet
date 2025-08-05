# ☄️ Comet

High-performance embedded segmented log for edge observability. Built for single-digit microsecond latency and bounded resources.

[**Architecture Guide**](ARCHITECTURE.md) | [**Performance Guide**](PERFORMANCE.md) | [**Troubleshooting**](TROUBLESHOOTING.md) | [**Security**](SECURITY.md) | [**API Reference**](https://pkg.go.dev/github.com/orbiterhq/comet)

> [!NOTE]
> This is very much an experiment in vibe coding. While the ideas are sound and the test coverage is robust, you may want to keep that in mind before using it for now.

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

- **Ultra-low latency**: Up to 12,634,200 entries/sec with batching
  - Comet uses periodic checkpoints (default: every 1000 writes or 1 second) to persist data to disk. Between checkpoints, writes are acknowledged after being written to the OS page cache.
- **Predictable performance**: No compaction stalls or write amplification like LSM-trees
- **True multi-process support**: Hybrid coordination (mmap + file locks), crash-safe rotation, real OS processes
- **O(log n) lookups**: Binary searchable index with bounded memory usage
- **Lock-free reads**: Atomic pointers, zero-copy via mmap with memory safety
- **Automatic retention**: Time and size-based cleanup, protects unconsumed data
- **Production ready**: Crash recovery, built-in metrics, extensive testing
- **Smart sharding**: Consistent hashing, automatic discovery, batch optimizations
- **Optional zstd compression**: ~37% storage savings when needed

## Multi-Process Coordination

Unlike other embedded solutions, Comet enables **true multi-process coordination** through memory-mapped state files. Perfect for prefork web servers and multi-process deployments.

- **Automatic shard ownership** - Each process owns specific shards based on `shardID % processCount == processID`
- **Per-shard state files** - Each shard has its own `comet.state` for metrics and recovery
- **Memory-mapped coordination** - Lock-free operations through atomic memory access
- **Crash-safe design** - State files enable automatic recovery on restart

## How Does Comet Compare?

| Feature              | Comet                      | Kafka               | Redis Streams      | RocksDB            | Proof                                 |
| -------------------- | -------------------------- | ------------------- | ------------------ | ------------------ | ------------------------------------- |
| **Write Latency**    | 1.7μs (33μs multi-process) | 1-5ms               | 50-100μs           | 50-200μs           | [Benchmarks](benchmarks_test.go)      |
| **Multi-Process**    | ✅ Real OS processes       | ✅ Distributed      | ❌ Single process  | ⚠️ Mutex locks      | [Tests](multiprocess_test.go)         |
| **Resource Bounds**  | ✅ Time & size limits      | ⚠️ JVM heap          | ⚠️ Memory only      | ⚠️ Manual compact   | [Retention](retention.go)             |
| **Crash Recovery**   | ✅ Automatic               | ✅ Replicas         | ⚠️ AOF/RDB          | ✅ WAL             | [Recovery](state_recovery.go)         |
| **Zero Copy Reads**  | ✅ mmap                    | ❌ Network          | ❌ Serialization   | ❌ Deserialization | [Reader](reader.go)                   |
| **Storage Overhead** | ~12 bytes/entry            | ~50 bytes/entry     | ~20 bytes/entry    | ~30 bytes/entry    | [Format](ARCHITECTURE.md#wire-format) |
| **Sharding**         | ✅ Built-in                | ✅ Partitions       | ❌ Manual          | ❌ Manual          | [Client](client.go)                   |
| **Compression**      | ✅ Optional zstd           | ✅ Multiple codecs  | ❌ None            | ✅ Multiple        | [Config](comet.go)                    |
| **Embedded**         | ✅ Native                  | ❌ Requires cluster | ❌ Requires server | ✅ Native          | -                                     |

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
client, err := comet.NewClient("/var/lib/comet", config)
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
│    Process 0    │     │    Process 1    │
│  ┌───────────┐  │     │  ┌───────────┐  │
│  │   Comet   │  │     │  │   Comet   │  │
│  │  Client   │  │     │  │  Client   │  │
│  └─────┬─────┘  │     │  └─────┬─────┘  │
└────────┼────────┘     └────────┼────────┘
         │                       │
         ▼                       ▼
    ┌──────────────────────────────────┐
    │      Segmented Log Storage       │
    │                                  │
    │  Shard 0: [seg0][seg1][seg2]→    │ ← Process 0
    │           [comet.state]          │
    │  Shard 1: [seg0][seg1]→          │ ← Process 1
    │           [comet.state]          │
    │  Shard 2: [seg0][seg1][seg2]→    │ ← Process 0
    │           [comet.state]          │
    │  Shard 3: [seg0][seg1]→          │ ← Process 1
    │           [comet.state]          │
    │  ...                             │
    │                                  │
    │  ↓ segments deleted by retention │
    └──────────────────────────────────┘
```

## Performance Optimizations

Comet achieves microsecond-level latency through careful optimization:

1. **Lock-Free Reads**: Memory-mapped files with atomic pointers and defensive copying for memory safety
1. **Binary Searchable Index**: O(log n) entry lookups instead of linear scans
1. **Vectored I/O**: Batches multiple writes into single syscalls
1. **Batch ACKs**: Groups acknowledgments by shard to minimize lock acquisitions
1. **Pre-allocated Buffers**: Reuses buffers to minimize allocations
1. **Concurrent Shards**: Each shard has independent locks for parallel operations

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

## Performance notes

**Durability Note**: Comet uses periodic checkpoints (default: every 1000 writes or 1 second) to persist metadata to disk. Between checkpoints, writes are acknowledged after being written to the OS page cache. This provides excellent performance while maintaining durability through:

- OS page cache (typically synced within 30 seconds)
- Explicit fsync on file rotation
- Crash recovery that rebuilds state from data files

### Performance Metrics

- **ACK performance**: 201ns per ACK (5M ACKs/sec) for single ACKs
- **Memory efficiency**: 7 allocations per write batch, 4 allocations per ACK
- **Storage overhead**: 12 bytes per entry (4-byte length + 8-byte timestamp)

## Configuration

### Single-Process vs Multi-Process Mode

Comet supports both single-process and multi-process deployments with a unified state management system:

```go
// Single-process mode (default) - fastest performance
client, err := comet.NewClient("/data/streams")

// Multi-process mode - for prefork/multi-process deployments
client, err := comet.NewMultiProcessClient("/data/streams")
```

**How multi-process mode works:**

- Each process owns specific shards based on: `shardID % processCount == processID`
- Processes automatically coordinate through memory-mapped state files
- No configuration needed - just use `NewMultiProcessClient()`
- Automatic process ID assignment from a pool

**When to use multi-process mode:**

- Process isolation is critical
- Using prefork web servers (e.g., Go Fiber with prefork)
- Need independent process scaling
- You're already batching writes (reduces the latency impact)

**When to use single-process mode (default):**

- Single service deployment
- Don't need process-level isolation

## License

MIT
