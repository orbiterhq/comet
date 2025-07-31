# Comet Architecture

This document provides an in-depth look at Comet's architecture, explaining the design decisions that enable single-digit microsecond latency while maintaining reliability and resource efficiency.

## Table of Contents

1. [Overview](#overview)
2. [Core Design Principles](#core-design-principles)
3. [Storage Architecture](#storage-architecture)
4. [Performance Optimizations](#performance-optimizations)
5. [Concurrency Model](#concurrency-model)
6. [Memory Management](#memory-management)
7. [Compression Strategy](#compression-strategy)
8. [Index Design](#index-design)
9. [Multi-Process Coordination](#multi-process-coordination)
10. [Consumer Architecture](#consumer-architecture)
11. [Retention Management](#retention-management)
12. [Crash Recovery](#crash-recovery)
13. [Benchmarking Results](#benchmarking-results)

## Overview

Comet is an embedded segmented log designed for edge observability workloads. It achieves 1.7μs write latency (32μs multi-process) by combining:

- Lock-free reads via memory-mapped files
- Compression outside critical sections
- Binary searchable indexes with bounded memory
- Intelligent batching and vectored I/O
- Separate locks for index and data operations

## Core Design Principles

### 1. Minimum Effective Abstraction

We avoid unnecessary layers. Every abstraction must prove its worth through benchmarks. This principle led to:

- Direct memory-mapped file access instead of buffered I/O
- sync.Map for reader tracking (30x faster than RWMutex for our read-heavy workload)
- Atomic pointers for lock-free reads

### 2. I/O Outside Locks

All expensive operations happen outside critical sections. Compression, disk writes, and other CPU-intensive operations are performed without holding locks. Only the minimal critical section for updating shared state is protected.

### 3. Entry-Based Addressing

Unlike byte-based systems, we use entry numbers for addressing. This provides:

- Compression transparency (consumers don't care if data is compressed)
- Simple consumer offset tracking
- Natural batch boundaries

### 4. Predictable Resource Usage

- Bounded index size via `MaxIndexEntries`
- Automatic file rotation at `MaxFileSize`
- Time and size-based retention policies

## Storage Architecture

### Segmented Log Design

```
Shard Directory Structure:
/data/streams/0001/
├── log-0000000000000001.comet   # First segment file (sequential)
├── log-0000000000000002.comet   # Second segment file
├── log-0000000000000003.comet   # Current write file
├── index.bin                    # Binary index file
├── index.state                  # 8-byte index change notification (multi-process mode)
├── sequence.state               # 8-byte sequence counter (multi-process mode)  
├── coordination.state           # 256-byte mmap coordination state (multi-process mode)
├── index.lock                   # Index write lock (multi-process mode)
└── shard.lock                   # Data write lock (multi-process mode)
```

### Wire Format

Each entry consists of:

```
[4 bytes] Length (uint32, little endian)
[8 bytes] Timestamp (uint64, little endian, nanoseconds)
[N bytes] Data (possibly compressed)
```

This fixed 12-byte header enables:

- Single read to determine entry size
- Efficient scanning without parsing data
- Natural alignment for modern CPUs

### File Rotation

Files rotate when they exceed `MaxFileSize` (default 1GB). This:

- Bounds memory usage for mmap
- Enables efficient retention management
- Prevents file system limitations

## Performance Optimizations

### 1. Lock-Free Reads

Readers use atomic pointers to access memory-mapped data. This allows multiple readers to access data concurrently without any locking overhead. The atomic pointer ensures readers always see a consistent view of the data.

### 2. sync.Map for Reader Cache

Benchmarks showed sync.Map is 30x faster than RWMutex for our access pattern:

- Many concurrent readers
- Infrequent updates (only on file rotation)
- Natural sharding reduces contention

### 3. Header Pool

Pre-allocated header buffers reduce allocations. By reusing buffers for the 12-byte headers, we eliminate allocation overhead in the hot path. The pool is sized for common batch sizes.

### 4. Vectored I/O

Batch writes use vectored I/O to minimize syscalls. Instead of one syscall per entry, we build an iovec array and write multiple entries in a single operation. This dramatically reduces kernel transitions.

### 5. Binary Searchable Index

O(log n) lookups instead of linear scans. The index stores checkpoints every N entries (configurable via BoundaryInterval). To find an entry, we binary search to the nearest checkpoint, then perform a short linear scan. This balances memory usage with lookup performance.

### 6. Compression Workers

Parallel compression with bounded queues. Multiple worker goroutines handle compression in parallel, preventing compression from becoming a bottleneck. The queue size is bounded to prevent memory bloat.

### 7. Instant Change Detection

8-byte mmap for multi-process coordination. Writers update a timestamp after modifying the index. Readers check this timestamp before each batch to detect changes. This provides instant coordination without locks or system calls.

## Concurrency Model

### Write Path

1. **Compression** (parallel, outside lock)
2. **Lock acquisition** (minimal scope)
3. **Write to file** (sequential)
4. **Index update** (in-memory)
5. **Lock release**
6. **Checkpoint** (periodic, separate lock)

### Read Path

1. **Check mmap state** (atomic, no lock)
2. **Access reader** (sync.Map, usually no lock)
3. **Read from mmap** (atomic pointer, no lock)
4. **Decompress** (parallel, no lock)

### Lock Hierarchy

To prevent deadlocks:

1. Shard lock (data operations)
2. Index lock (index operations)
3. Never hold both simultaneously

## Memory Management

### Bounded Index Memory

The binary searchable index limits memory usage. When the index reaches MaxIndexEntries, we remove the oldest half of nodes. This keeps memory bounded while preserving recent entries for fast access.

### Reader Lifecycle

Readers are cached and reused. The fast path retrieves an existing reader from the cache. New readers are only created when files rotate. This amortizes the cost of opening and mapping files.

### Memory-Mapped Files

Benefits:

- OS manages page cache
- Automatic memory pressure handling
- Zero-copy reads
- Shared between processes

Careful management:

- Unmap old files after rotation
- Bounded number of mapped files
- Graceful handling of memory pressure

## Compression Strategy

### Adaptive Compression

Only compress entries above threshold. Small entries aren't worth compressing due to overhead. We also skip compression if it doesn't provide at least 10% savings. This ensures we only pay the CPU cost when there's real benefit.

### Compression Outside Locks

Critical insight: compression is CPU-intensive but doesn't need the lock:

1. Compress while unlocked
2. Acquire lock only for write
3. 10x improvement in p99 latency

### Zstd Configuration

Chosen for:

- Best compression ratio (37:1 on structured logs)
- Good speed/ratio balance
- Streaming support
- Dictionary potential (future)

## Index Design

### Three-Level Index Structure

1. **In-Memory Boundaries** (recent entries)
2. **Binary Searchable Index** (checkpoint nodes)
3. **Consumer Offsets** (per consumer group)

### Entry Position Tracking

Each entry's location is tracked by file index and byte offset within that file. This two-level addressing scheme handles file rotation naturally and enables efficient seeking.

### Binary Search Algorithm

The index uses a standard binary search to find the checkpoint just before the target entry. We then scan forward from that checkpoint. This two-phase approach minimizes both memory usage and lookup time.

## Multi-Process Coordination

Comet's multi-process coordination has evolved through three generations to achieve ultra-fast 32μs write latency:

### Evolution of Multi-Process Performance

| Generation                 | Write Latency | vs Single-Process | Key Technology               |
| -------------------------- | ------------- | ----------------- | ---------------------------- |
| Original (sync checkpoint) | 7.6ms         | 4,575x slower     | File locks + sync I/O        |
| Async checkpointing        | 3.2ms         | 1,940x slower     | Deferred index persistence   |
| **Memory-mapped I/O**      | **32μs**      | **20x slower**    | **Lock-free atomics + mmap** |

### Memory-Mapped Writer (Current Implementation)

The latest implementation uses memory-mapped files for ultra-fast multi-process writes:

#### Coordination State Structure

The `coordination.state` file contains atomic coordination data:

```go
type MmapCoordinationState struct {
    ActiveFileIndex    atomic.Int64  // Current file being written to
    WriteOffset        atomic.Int64  // Current write position in active file
    FileSize           atomic.Int64  // Current size of active file
    RotationInProgress atomic.Int64  // 1 if rotation is happening, 0 otherwise
    LastWriteNanos     atomic.Int64  // Timestamp of last write
    TotalWrites        atomic.Int64  // Total number of writes
}
```

#### Lock-Free Sequence Allocation

Processes coordinate through atomic operations on shared memory:

1. **Sequence Allocation**: `atomic.Add()` on sequence counter to reserve entry numbers
2. **Write Space Allocation**: `atomic.Add()` on write offset to reserve file space
3. **Direct Memory Writes**: Data written directly to memory-mapped region
4. **Atomic File Growth**: File extended atomically when needed

#### Zero-Copy Architecture

- **Direct Memory Writes**: Data goes directly to mapped memory, bypassing syscalls
- **Atomic Coordination**: All coordination through lock-free atomic operations
- **No File Locking**: Traditional file locks eliminated for data writes
- **Background Index Updates**: Index persistence happens asynchronously

### Legacy File Locking (Fallback)

For non-mmap mode, we use advisory file locks (flock) to coordinate writes between processes. The lock is held only during the actual write operation, not during compression or other preparation.

### Separate Index Lock

A separate lock for index updates prevents reader starvation. Writers can update the index without blocking data reads. This separation is crucial for maintaining low read latency.

### Memory-Mapped State File (index.state)

The `index.state` file enables instant change detection without locks or system calls.

#### Structure

The file contains a single 64-bit integer representing Unix nanoseconds of the last index update. This timestamp serves as a change notification mechanism.

#### Why 8 Bytes?

- Single atomic operation on 64-bit systems
- No torn reads/writes
- Cache-line friendly
- Minimal memory overhead

#### Writer Operation

After updating the index, writers atomically update the timestamp in index.state. This signals to all readers that the index has changed.

#### Reader Operation

Before each read batch, readers check the index.state timestamp. If it differs from their last check, they reload the index. This check is extremely fast (single atomic load) and requires no locks.

#### Clock Skew Immunity

We use `!=` comparison instead of `>` to handle clock skew:

- Works even if system clock goes backwards
- Robust across different processes
- No dependency on synchronized clocks

#### Performance Impact

- **Check cost**: ~2ns (single atomic load)
- **No system calls**: Direct memory access
- **No lock contention**: Readers never block writers
- **Cache efficient**: 8 bytes stay in L1 cache

## Consumer Architecture

### Consumer Groups

Kafka-style consumer groups with offset tracking. Each consumer group maintains its own offset per shard, enabling multiple independent consumers to process the same stream at their own pace.

### Batch Processing

Optimized for throughput. The consumer reads messages in batches, processes them, and acknowledges them as a group. This amortizes the cost of offset updates and maximizes throughput.

### Functional Options Pattern

Clean, extensible API using functional options. This pattern allows users to configure only what they need while maintaining backward compatibility as new options are added.

### Smart Sharding

Automatic shard assignment for distributed processing. Given a worker ID and total worker count, the system automatically assigns shards using modulo distribution. This enables easy horizontal scaling.

## Retention Management

### Time-Based Retention

Delete files older than MaxAge. Files are deleted based on their end timestamp, ensuring we only remove data that has aged out completely.

### Size-Based Retention

Keep total size under limits. When space limits are exceeded, we delete the oldest files first, but always maintain MinFilesToKeep to prevent deleting everything.

### Consumer Protection

Never delete unread data (unless forced). Before deleting a file, we check all consumer offsets. If any consumer hasn't processed the file yet, we skip deletion. ForceDeleteAfter provides an escape hatch for stuck consumers.

## Crash Recovery

### Startup Recovery

1. Scan all shard directories
2. Validate index against data files
3. Find actual EOF in last file
4. Rebuild index if corrupted
5. Resume from last checkpoint

### Partial Write Detection

On startup, we scan backwards from EOF to find the last complete entry. Any partial write at the end is discarded. This ensures crash consistency without complex journaling.

### Consumer Offset Recovery

Offsets are persisted in the index:

- Durable on checkpoint
- Recovered on startup
- No data loss for acknowledged messages

## Memory Alignment Strategy

Every struct is carefully designed for optimal memory alignment:

### Field Ordering Rules

1. **64-bit fields first** - Prevents padding on 32-bit systems
2. **Pointers next** - Natural 8-byte alignment
3. **Smaller fields last** - Minimizes trailing padding
4. **Explicit size tracking** - Comments indicate field sizes

This strategy eliminates unnecessary padding and improves cache line utilization. The alignment_test.go verifies all structs maintain optimal layout.

## Wire Format Details

### Entry Layout

The 12-byte header uses little-endian encoding for cross-platform compatibility:

- **Bytes 0-3**: Length (uint32) - Data size, excluding header
- **Bytes 4-11**: Timestamp (uint64) - Unix nanoseconds
- **Bytes 12+**: Data - Possibly compressed

### Design Rationale

- **Fixed header** - Enables single read to determine entry size
- **Little-endian** - Consistent across architectures
- **Nanosecond precision** - Sufficient for observability data
- **Natural alignment** - 4-byte and 8-byte boundaries

## Binary Index Format

### Structure

The binary index uses a magic number "COMT" (0x434F4D54) for format identification:

1. **Header** - Magic number and version
2. **Metadata** - Current entry number, write offset
3. **Consumer offsets** - Encoded as key-length-value
4. **Boundaries** - Entry-to-position mappings
5. **Binary nodes** - Checkpoint data for O(log n) lookup

### Space Efficiency

Binary format provides optimal space efficiency:

- Fixed-size encoding for numbers
- No string overhead for field names
- Compact representation of boundaries
- Direct offset calculations without parsing

## Allocation Optimizations

### Zero-Allocation Hot Path

Critical paths achieve minimal allocations through:

1. **Preallocated buffers** - Sized for common batch sizes
2. **Direct slice operations** - Avoid copies where possible
3. **Atomic operations** - No allocation for metrics
4. **Buffer pools for decompression** - Reuse large buffers

### Measurement

Every optimization is verified with allocation benchmarks. The goal is zero allocations for common operations.

## Edge Case Handling

### File Boundaries

Entries that span file boundaries are handled seamlessly:

- Consumer reads continue across files
- Position tracking accounts for file transitions
- No data loss or duplication

### Concurrent Operations

Protection against edge cases:

- **Rotation storms** - Only one rotation proceeds
- **Clock skew** - Using != comparison for robustness
- **Partial writes** - Detected and discarded on recovery
- **Empty shards** - Handled gracefully

### Resource Exhaustion

Bounded resources prevent system overload:

- Compression queue limited to 256 entries
- Index entries capped at MaxIndexEntries
- File size limited to MaxFileSize
- Number of mapped files bounded

## Testing Strategy

### Benchmark-Driven Development

Every optimization must prove itself:

- Baseline measurement before changes
- Multiple benchmark scenarios
- Both latency and throughput metrics
- Real-world data patterns

### Edge Case Testing

Comprehensive test coverage for:

- Concurrent readers and writers
- File rotation boundaries
- Multi-process coordination
- Crash recovery scenarios
- Clock skew handling

### Fuzz Testing Potential

The fixed wire format and bounded operations make Comet suitable for fuzz testing, though not currently implemented.

## Configuration Philosophy

### Sensible Defaults

Default configuration optimized for common use cases:

- 1GB file size - Good for edge deployments
- 2KB compression threshold - Balanced CPU vs storage
- 100-entry boundaries - Memory-efficient indexing
- 4-hour retention - Typical debugging window

### Progressive Disclosure

Configuration complexity hidden behind:

1. Simple constructor for defaults
2. Specialized configs (HighCompression, MultiProcess, etc.)
3. Full customization via CometConfig

### Migration Support

Automatic migration from old config structure ensures smooth upgrades without breaking existing deployments.

## API Design Principles

### Functional Options

Consumer API uses functional options for flexibility:

- Clean default path
- Optional configuration
- Backward compatible extensions
- Type-safe at compile time

### Consistent Naming

Stream naming follows a hierarchical pattern:

- `namespace:version:shard:XXXX`
- Enables wildcard discovery
- Natural grouping
- Version migration support

## Monitoring and Observability

### Metrics Design

Comprehensive metrics without performance impact:

- Atomic counters for thread safety
- Latency histograms for percentiles
- Error tracking with categories
- Compression efficiency metrics

### Zero-Cost When Disabled

Metrics collection uses atomic operations that compile to simple stores when not actively monitored.

## Durability Guarantees

### Write Path

1. **Data written** - To OS buffer cache
2. **Index updated** - In memory
3. **Periodic checkpoint** - Index to disk
4. **Explicit sync** - Forces durability

### Consumer Offsets

Consumer acknowledgments are immediately durable through index persistence. This prevents data loss but may impact ACK latency.

### Trade-offs

The system favors throughput over immediate durability, suitable for observability data where some loss is acceptable.

## Benchmarking Results

### Single-Process Mode (Default)

```
BenchmarkSingleWrite-8          588,235 ops/s    1.7μs/op     202 B/op    6 allocs/op
BenchmarkBatch10-8            2,000,000 ops/s    0.50μs/op    1309 B/op   5 allocs/op
BenchmarkBatch100-8           4,100,000 ops/s    0.24μs/op    13090 B/op  5 allocs/op
BenchmarkBatch1000-8         10,200,000 ops/s    0.098μs/op   130900 B/op 5 allocs/op
```

### Multi-Process Mode (Memory-Mapped)

```
BenchmarkMmapWriter-8            31,250 ops/s    32μs/op      6664 B/op   49 allocs/op
BenchmarkMmapBatch10-8          312,500 ops/s    3.2μs/op     66640 B/op  49 allocs/op
BenchmarkMmapBatch100-8       3,125,000 ops/s    0.32μs/op    666400 B/op 49 allocs/op  
BenchmarkMmapBatch1000-8     31,250,000 ops/s    0.032μs/op   6664000 B/op 49 allocs/op
```

### Key Insights

1. **Batching is critical**: 10x improvement from single to 1000-entry batches
2. **Lock overhead is manageable**: Only 45% slower with file locking
3. **Zero allocations**: Careful memory management pays off
4. **Compression is worth it**: 37:1 ratio saves enormous disk I/O

## Lock Hierarchy and Deadlock Prevention

### Lock Acquisition Order

To prevent deadlocks, locks must be acquired in a strict order:

1. **Shard data lock** (`shard.mu`) - For data operations
2. **Index write lock** (`indexLock`) - For index updates
3. **Never hold both** - Release one before acquiring the other

### Lock Scopes

Each lock protects specific operations:

- **Data lock**: File writes, rotation, shard state
- **Index lock**: Index updates, consumer offsets
- **No lock**: Reads (use atomic pointers)

## Error Handling Philosophy

### Write Failures

On write failure, the system:

1. Rolls back index state
2. Preserves previous valid state
3. Returns error to caller
4. Updates error metrics

### Read Failures

Read errors are handled gracefully:

- Truncated files allow continued operation
- Missing files trigger re-creation
- Corrupt index triggers rebuild attempt

### Partial State Recovery

The system prioritizes availability:

- Partial data better than no data
- Continue operation when possible
- Log errors for debugging

## Resource Management

### File Descriptor Limits

Careful management of file descriptors:

- Lazy file opening
- Aggressive closing on rotation
- Bounded number of open files
- mmap reduces fd pressure

### Memory Bounds

Multiple mechanisms limit memory usage:

- `MaxIndexEntries` caps index size
- Compression queue bounded at 256
- Header pool sized for typical batches
- Old mmap regions unmapped promptly

### CPU Utilization

CPU usage controlled through:

- 4 compression workers (configurable)
- Batch processing amortization
- Lock-free read paths
- Minimal critical sections

## Preallocation Strategy

### File Preallocation

New files preallocated to 128MB:

- Reduces fragmentation
- Improves write performance
- Amortizes allocation cost
- Falls back gracefully if space limited

### Benefits

- Sequential disk layout
- Fewer metadata updates
- Better SSD wear leveling
- Predictable performance

## Consumer Group Semantics

### Offset Management

Each consumer group maintains:

- Per-shard offset tracking
- Entry-based positioning (not bytes)
- Immediate persistence on ACK
- Independent progress tracking

### Delivery Guarantees

At-least-once delivery through:

- Explicit acknowledgment required
- Offset advanced only on ACK
- Retry on processing failure
- No automatic offset advancement

### Lag Monitoring

Consumer lag calculated as:

- Current entry number minus consumer offset
- Per-shard granularity
- Real-time calculation
- Useful for alerting

## Shard Discovery

### Pattern-Based Discovery

Shards discovered through pattern matching:

- Wildcard support (`stream:*`)
- Automatic shard detection
- Up to 32 shards by default
- Extensible to more if needed

### Load Distribution

Automatic shard assignment using:

- Modulo distribution
- Consistent assignment
- No coordination required
- Deterministic allocation

## Compression Trade-offs

### Threshold Tuning

Compression threshold (default 2KB) based on:

- Overhead of compression headers
- CPU cost vs space savings
- Typical entry sizes
- Measured break-even point

### Adaptive Compression

Only compress if beneficial:

- Minimum 10% size reduction required
- Skip incompressible data
- Preserve CPU for small gains
- Configurable thresholds

## Future Optimizations

### Under Consideration

1. **io_uring** - Could eliminate syscall overhead, but has its own limitations and complexities

### Rejected Ideas

1. **Circular Buffers** - Too complex for marginal gains
1. **B-Tree Indexes** - Binary search is simpler and sufficient
1. **Write-Ahead Log** - Segmented log is simpler and faster

## Performance Characteristics by Operation Size

### Single Entry Writes

- **Latency**: 1.66μs (single-process), 2.42μs (multi-process)
- **Bottleneck**: Lock acquisition and index update
- **Optimization**: Batch for better performance

### Small Batches (10 entries)

- **Latency**: 0.50μs per entry
- **Improvement**: 3x over single writes
- **Sweet spot**: Balances latency and throughput

### Medium Batches (100 entries)

- **Latency**: 0.24μs per entry
- **Improvement**: 7x over single writes
- **Recommended**: Default batch size

### Large Batches (1000 entries)

- **Latency**: 0.098μs per entry
- **Improvement**: 17x over single writes
- **Trade-off**: Higher memory usage

## System Integration Considerations

### File System Choice

- **ext4/xfs**: Well-tested, good performance
- **btrfs**: Works but COW may impact performance
- **tmpfs**: Excellent for testing
- **NFS**: Not recommended due to locking

### Deployment Patterns

#### Single Process

- Default configuration
- Best performance
- Simplest operation
- No coordination overhead

#### Prefork/Multi-Process

- Enable file locking
- 45% performance penalty
- Instant coordination via mmap
- Suitable for web servers with prefork

#### Containerized

- Mount persistent volume
- Consider memory limits
- File locking if sharing volume
- Monitor file descriptor usage

### Resource Planning

#### Memory Requirements

- **Base**: ~2MB per shard for index
- **Compression workers**: ~10MB
- **mmap regions**: Managed by OS
- **Total**: ~50MB for 16 shards

#### Disk Requirements

- **Active data**: MaxShardSize × shard count
- **Retention buffer**: ~20% overhead
- **Preallocation**: 128MB per active file
- **Total**: Plan for 2x active data size

#### CPU Requirements

- **Compression**: 4 cores can saturate
- **I/O**: Minimal CPU usage
- **Lock contention**: Negligible with batching
- **Total**: 2-4 cores typically sufficient

## Operational Insights

### Monitoring Key Metrics

Critical metrics to watch:

1. **Write latency** - Indicates system health
2. **Compression ratio** - Storage efficiency
3. **Consumer lag** - Processing delays
4. **File rotations** - I/O patterns
5. **Error rate** - System issues

### Tuning Guidelines

#### For Latency

- Disable compression
- Smaller batch sizes
- Single-process mode
- Fast storage (NVMe)

#### For Throughput

- Larger batches (1000+)
- Enable compression
- Multiple shards
- Parallel consumers

#### For Storage Efficiency

- Lower compression threshold
- Aggressive retention
- Larger file sizes
- Higher compression ratio

### Common Issues and Solutions

#### High Write Latency

- **Cause**: Lock contention
- **Solution**: Increase batch size

#### Excessive Memory Usage

- **Cause**: Unbounded index
- **Solution**: Set MaxIndexEntries

#### Consumer Falling Behind

- **Cause**: Slow processing
- **Solution**: Add parallel consumers

#### Disk Space Exhaustion

- **Cause**: Retention not aggressive enough
- **Solution**: Reduce MaxAge or MaxShardSize

## Design Trade-offs Explained

### Why Not Memory-Mapped Writes?

- mmap for writes adds complexity
- Crash recovery harder
- Performance gain minimal
- Direct I/O more predictable

### Why Entry-Based Addressing?

- Compression transparent to consumers
- Simple offset tracking
- Natural batch boundaries
- Easy consumer coordination

### Why Not Circular Buffers?

- Complex wrap-around logic
- Hard to handle variable-size entries
- Retention policy complicated
- Marginal performance gain

### Why Binary Index Format?

- 2-3x space savings
- Predictable memory usage
- Fast serialization
- Backward compatible

## Conclusion

Comet achieves microsecond latency through:

1. **Architectural simplicity** - Segmented logs with minimal abstractions
2. **Lock-free reads** - Atomic pointers and memory mapping
3. **Intelligent batching** - Amortizes costs across operations
4. **Careful optimization** - Every decision backed by benchmarks
5. **Resource bounds** - Predictable memory and disk usage
6. **Production focus** - Built for real-world edge deployments

The result is a storage engine that combines the simplicity of append-only logs with the performance of specialized databases, perfect for edge observability workloads where every microsecond counts.
