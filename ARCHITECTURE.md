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

Comet is an embedded segmented log designed for edge observability workloads. It achieves 1.7μs write latency (33μs multi-process) by combining:

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
/data/streams/shard-0001/
├── log-0000000000000001.comet   # First segment file (sequential)
├── log-0000000000000002.comet   # Second segment file
├── log-0000000000000003.comet   # Current write file
├── index.bin                    # Binary index file
├── comet.state                  # 1024-byte unified state (multi-process mode)
├── index.lock                   # Index write lock (multi-process mode)
├── retention.lock               # Retention coordination lock (multi-process mode)
├── rotation.lock                # File rotation coordination lock (multi-process mode)
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

Files rotate when they exceed `MaxFileSize` (default 256MB). This:

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

Unified state for multi-process coordination. Writers update the `LastIndexUpdate` timestamp in comet.state after modifying the index. Readers check this timestamp before each batch to detect changes. This provides instant coordination without locks or system calls.

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

### Multi-Process Lock Files

In multi-process mode, Comet uses specialized lock files for different coordination needs:

| Lock File        | Purpose                    | Frequency | Blocking | Notes                                |
| ---------------- | -------------------------- | --------- | -------- | ------------------------------------ |
| `shard.lock`     | Data write coordination    | High      | Yes      | Ensures only one process writes data |
| `index.lock`     | Index modification safety  | Medium    | Yes      | Prevents concurrent index corruption |
| `rotation.lock`  | File rotation coordination | Low       | No       | Non-blocking to prevent deadlocks    |
| `retention.lock` | Retention operation safety | Low       | No       | Non-blocking to prevent hanging      |

**Non-blocking locks** use `LOCK_NB` flag - if another process holds the lock, the operation is skipped rather than waiting. This prevents deadlocks in scenarios where operations can be safely deferred.

### Lock Hierarchy

To prevent deadlocks:

1. Shard lock (data operations)
2. Index lock (index operations)
3. Never hold both simultaneously

## Memory Management

### Bounded Reader Cache

Comet's memory management addresses production memory exhaustion issues with long retention periods through intelligent bounded memory management:

#### Smart File Mapping

The reader system supports two operating modes:

1. **MapAllFiles=true**: Maps all files immediately for maximum read performance
2. **MapAllFiles=false** (default): Only maps files containing unACKed data, dramatically reducing memory usage

#### Memory Bounds Enforcement

- **MaxMappedFiles**: Limits the number of simultaneously mapped files (default: reasonable based on system memory)
- **MaxMemoryBytes**: Hard limit on memory-mapped regions (default: bounded to prevent exhaustion)
- **LRU Eviction**: Automatically evicts oldest non-recent files when limits are exceeded
- **Memory Tracking**: Atomic counters track exact memory usage across all mapped files

#### Recent File Cache

A sophisticated LRU cache system optimizes performance:

- **Recent Access Tracking**: Frequently accessed files are protected from eviction
- **Automatic Eviction**: Oldest unused files are unmapped when memory limits are reached
- **Smart Mapping**: Files are mapped on-demand when accessed, unmapped when no longer needed
- **Cross-Process Coordination**: Memory limits enforced consistently across all processes

#### Production Benefits

This design solves the critical production issue where long retention periods (days/weeks) would cause memory exhaustion:

- **Before**: All files mapped → 100GB+ memory usage with long retention
- **After**: Only active files mapped → <1GB memory usage regardless of retention period
- **Performance**: Hot files maintain microsecond read latency, cold files mapped on-demand
- **Reliability**: Graceful handling of memory pressure without crashes

### Bounded Index Memory

The binary searchable index limits memory usage. When the index reaches MaxIndexEntries, we remove the oldest half of nodes. This keeps memory bounded while preserving recent entries for fast access.

### Reader Lifecycle Management

The reader system provides sophisticated lifecycle management:

- **Lazy Initialization**: Readers created only when needed
- **Automatic Cleanup**: Resources freed when readers are closed
- **Memory Tracking**: Precise tracking of mapped memory usage
- **Graceful Degradation**: System continues operating under memory pressure

### Memory-Mapped Files

Enhanced memory mapping with intelligent bounds:

Benefits:

- OS manages page cache efficiently
- Automatic memory pressure handling
- Zero-copy reads for maximum performance
- Shared between processes

Careful management:

- **Bounded Mapping**: Only essential files mapped
- **Dynamic Remapping**: Files mapped/unmapped based on access patterns
- **Memory Limits**: Hard limits prevent system memory exhaustion
- **Graceful Fallback**: System degrades gracefully under memory pressure

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

Comet's multi-process coordination has evolved through three generations to achieve ultra-fast 33μs write latency:

### Evolution of Multi-Process Performance

| Generation                 | Write Latency | vs Single-Process | Key Technology               |
| -------------------------- | ------------- | ----------------- | ---------------------------- |
| Original (sync checkpoint) | 7.6ms         | 4,575x slower     | File locks + sync I/O        |
| Async checkpointing        | 3.2ms         | 1,940x slower     | Deferred index persistence   |
| **Memory-mapped I/O**      | **33μs**      | **19x slower**    | **Lock-free atomics + mmap** |

### Memory-Mapped Writer (Current Implementation)

The latest implementation uses memory-mapped files for ultra-fast multi-process writes:

#### Unified State File (comet.state)

The `comet.state` file is the heart of Comet's ultra-fast multi-process coordination. This 1024-byte memory-mapped file provides comprehensive metrics and coordination state in a single, cache-aligned structure.

##### Structure

```go
type CometState struct {
    // Cache line 1: Core metrics (0-63 bytes)
    Version          uint64  // Format version
    WriteOffset      uint64  // Current write position in active file
    LastEntryNumber  int64   // Last allocated entry number (-1 = uninitialized)
    LastIndexUpdate  int64   // Timestamp of last index update
    ActiveFileIndex  uint64  // Current file being written to
    FileSize         uint64  // Current size of active file
    LastFileSequence uint64  // Last file sequence number
    _padding1        uint64  // Padding for cache alignment
    
    // Cache line 2: Write metrics (64-127 bytes)
    TotalEntries     int64   // Total entries written
    TotalBytes       uint64  // Total uncompressed bytes
    TotalWrites      uint64  // Total write operations
    LastWriteNanos   int64   // Timestamp of last write
    // ... continues with more metrics organized by cache lines
}
```

The structure is carefully designed with:

- 64-byte cache line alignment for optimal CPU performance
- Atomic-safe fields using raw int64/uint64 types
- Comprehensive metrics replacing multiple fragmented state files

##### How It Works

1. **Memory-Mapped Shared State**: All processes map this file into their address space
2. **Lock-Free Operations**: Atomic operations ensure consistency without locks
3. **Write Coordination**: Processes atomically reserve space in the current data file
4. **File Rotation**: Coordinated through OS file locking (rotation.lock)
5. **Staleness Detection**: TotalWrites helps detect when indexes need rebuilding

##### Performance Impact

This design enables:

- **33μs multi-process writes** (vs 7.6ms with file locking)
- **Zero system calls** for coordination (just atomic CPU operations)
- **No lock contention** between writers
- **Instant visibility** of changes across all processes

##### Coordination Strategy: Mmap vs File Locks

Comet uses a hybrid approach that leverages the strengths of both memory-mapped coordination and OS file locking:

**Memory-Mapped Coordination** (comet.state):

- **Best for**: High-frequency atomic operations (write offsets, counters, timestamps)
- **Performance**: Zero system calls, just atomic CPU operations
- **Reliability**: Excellent for data sharing, but problematic for critical sections
- **Use cases**: Write coordination, sequence allocation, change detection, comprehensive metrics

**OS File Locking** (*.lock files):

- **Best for**: Critical section coordination (rotation, retention, index updates)
- **Performance**: Requires system calls, but used infrequently
- **Reliability**: Automatic cleanup on process crash, prevents deadlocks
- **Use cases**: File rotation, retention operations, index modifications

**Key Insight**: Memory-mapped atomics are perfect for lock-free data sharing but terrible for critical section locks because they lack OS-level cleanup when processes crash. File locks provide the robustness needed for infrequent but critical operations.

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

### Unified State Architecture

Comet consolidates all multi-process coordination into a single memory-mapped state file:

#### comet.state (1024 bytes)

The unified `comet.state` file replaces the previous three-file design (coordination.state, sequence.state, index.state) with a single, comprehensive structure that provides:

1. **Write Coordination**: Atomic space allocation, file management, sequence tracking
2. **Change Detection**: Index update timestamps for instant reader notification
3. **Comprehensive Metrics**: Write latencies, compression ratios, error counts, and more
4. **Cache Optimization**: 64-byte aligned sections minimize CPU cache misses

#### Key Benefits of Unified State

- **Single Memory Map**: One mmap operation instead of three
- **Atomic Updates**: Related fields updated together without coordination
- **Better Cache Locality**: Related data in the same cache lines
- **Simplified Code**: One state structure to manage instead of three
- **Comprehensive Visibility**: All metrics available in one place

#### State Access Patterns

**Writers** use atomic operations to:

- Allocate write space (`AddWriteOffset`)
- Track entry numbers (`IncrementLastEntryNumber`)
- Update metrics (`AddTotalBytes`, `UpdateWriteLatency`)
- Signal changes (`SetLastIndexUpdate`)

**Readers** use atomic loads to:

- Detect index changes (`GetLastIndexUpdate`)
- Check write progress (`GetTotalWrites`)
- Monitor system health (`GetErrorCount`)

#### Performance Characteristics

- **Check cost**: ~2ns (single atomic load)
- **Update cost**: ~10ns (atomic store with memory barrier)
- **No system calls**: Direct memory access
- **No lock contention**: Lock-free atomic operations
- **Cache efficient**: Hot fields in first cache line

#### Race Condition Handling

Comet includes comprehensive retry logic to handle multi-process race conditions:

- **EOF errors**: Retries when index files are being written by another process
- **Index file too small**: Handles partial index file writes during coordination
- **Exponential backoff**: 1ms, 2ms delays to allow conflicting operations to complete
- **Bounded retries**: Maximum 3 attempts to prevent infinite loops

This ensures 100% data consistency even under extreme multi-process contention.

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

### Bounded Reader Testing Strategy

The bounded reader cache implementation includes extensive test coverage across multiple dimensions:

#### Unit Tests

- Basic functionality in both MapAllFiles modes
- Memory bounds enforcement with various limits
- LRU cache behavior and eviction policies
- Concurrent access and thread safety
- Error handling for corrupted/missing files

#### Comprehensive Tests

- Compressed data handling and decompression
- UpdateFiles functionality for dynamic file lists
- Empty file handling and edge cases
- Large offset handling and overflow protection

#### Edge Case Coverage

- Nil/empty index handling
- Invalid file indices and byte offsets
- Permission errors and file system issues
- Zero memory limits and resource exhaustion
- Reader lifecycle (creation, use, cleanup)

#### Multi-Process Integration Tests

- Real OS process spawning (not simulated)
- Cross-process data visibility and coordination
- Memory bounds enforcement across processes
- Index synchronization and rebuilding

This multi-layered testing approach ensures the bounded reader cache works correctly under all conditions, from normal operation to extreme edge cases and multi-process coordination scenarios.

### Fuzz Testing Potential

The fixed wire format and bounded operations make Comet suitable for fuzz testing, though not currently implemented.

## Configuration Philosophy

### Sensible Defaults

Default configuration optimized for common use cases:

- 256MB file size - Balanced memory usage and performance
- 2KB compression threshold - Balanced CPU vs storage
- 100-entry boundaries - Memory-efficient indexing
- 4-hour retention - Typical debugging window
- Bounded file mapping - Smart memory management for long retention

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
BenchmarkWrite_SingleEntry-8         594k ops/s    1.7μs/op     203 B/op    6 allocs/op
BenchmarkWrite_SmallBatch-8       230k batches/s   2.7μs/batch  1310 B/op   5 allocs/op (10 entries)
BenchmarkWrite_LargeBatch-8        70k batches/s   9.0μs/batch  13k B/op    5 allocs/op (100 entries)
BenchmarkWrite_HugeBatch-8         12k batches/s   112μs/batch  383k B/op   7 allocs/op (1K entries)
BenchmarkWrite_MegaBatch-8        1.2k batches/s   548μs/batch  1.6M B/op   7 allocs/op (10K entries)
```

### Multi-Process Mode (Memory-Mapped)

```
BenchmarkMultiProcessThroughput/SingleEntry-8    30k ops/s     33μs/op      5k B/op    51 allocs/op
BenchmarkMultiProcessThroughput/SmallBatch-8     16k batches/s 35μs/batch   9k B/op    54 allocs/op (10 entries)
BenchmarkMultiProcessThroughput/LargeBatch-8     10k batches/s 56μs/batch   65k B/op   65 allocs/op (100 entries)
BenchmarkMultiProcessThroughput/HugeBatch-8      6k batches/s  170μs/batch  363k B/op  120 allocs/op (1K entries)
BenchmarkMultiProcessThroughput/MegaBatch-8      312 batches/s 2.0ms/batch  2.9M B/op  856 allocs/op (10K entries)
```

### Key Insights

1. **Batching is critical**: ~66x improvement from single entry to 1000-entry batches (1.7μs → 0.11μs per entry)
2. **Multi-process overhead decreases with batch size**: 19x slower for single entries, only 1.5x slower for 1K batches
3. **Memory-mapped coordination is efficient**: 33μs latency is exceptional for multi-process writes
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

- **Latency**: 1.7μs (single-process), 33μs (multi-process)
- **Bottleneck**: Multi-process coordination overhead
- **Optimization**: Batch for better performance

### Small Batches (10 entries)

- **Latency**: 0.27μs per entry (single-process), 3.5μs per entry (multi-process)
- **Improvement**: 6x over single writes (single-process)
- **Sweet spot**: Balances latency and throughput

### Medium Batches (100 entries)

- **Latency**: 0.09μs per entry (single-process), 0.56μs per entry (multi-process)
- **Improvement**: 19x over single writes (single-process)
- **Recommended**: Default batch size

### Large Batches (1000 entries)

- **Latency**: 0.11μs per entry (single-process), 0.17μs per entry (multi-process)
- **Improvement**: 15x over single writes (single-process)
- **Trade-off**: Higher memory usage but excellent throughput

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
