# Comet Architecture

This document provides an in-depth look at Comet's architecture, explaining the design decisions that enable single-digit microsecond latency while maintaining reliability and resource efficiency.

## Overview

Comet is a high-performance embedded segmented log designed for edge observability. It provides:

- Single-digit microsecond write latency
- Bounded resource usage with predictable memory overhead
- Multi-process safety with shared memory coordination
- Automatic data lifecycle management
- Zero-copy reads through memory-mapped files
- Horizontal scaling through sharding

## Core Components

### 1. Client

The `Client` is the main entry point that coordinates all operations:

```
┌─────────────────────────────────────────────┐
│                   Client                    │
├─────────────────────────────────────────────┤
│ - Shard management                          │
│ - Write routing                             │
│ - Global retention                          │
│ - Configuration                             │
│ - Metrics aggregation                       │
└─────────────────────────────────────────────┘
                      │
                      ▼
        ┌─────────────┴─────────────┐
        │                           │
    Shard 0                     Shard N
```

Key responsibilities:

- **Thread-safe operations**: All public methods use mutex protection
- **Shard routing**: Determines target shard for writes/reads
- **Resource management**: Coordinates cleanup and retention
- **Configuration**: Hierarchical config with sensible defaults

### 2. Shard

Each shard represents an independent data stream partition:

```
┌──────────────────────────────────────┐
│               Shard                  │
├──────────────────────────────────────┤
│ - Buffered file writer               │
│ - Binary searchable index            │
│ - Memory-mapped state                │
│ - Compression engine                 │
│ - File rotation logic                │
└──────────────────────────────────────┘
         │              │
         ▼              ▼
    Data Files     Index File
   (.comet.data)  (.comet.index)
```

Design decisions:

- **Memory alignment**: Structure optimized with 64-bit fields first
- **Atomic operations**: Lock-free updates to shared state
- **Buffered writes**: Reduces syscall overhead
- **Automatic rotation**: Files capped at configurable size (default 1GB)

### 3. Consumer

High-level reading interface with consumer group semantics:

```
┌──────────────────────────────────────┐
│             Consumer                 │
├──────────────────────────────────────┤
│ - Consumer groups                    │
│ - Offset management                  │
│ - Batch processing                   │
│ - ACK tracking                       │
│ - Shard assignment                   │
└──────────────────────────────────────┘
         │
         ▼
   ┌─────┴─────┐
   │           │
Reader 0    Reader N
```

Key features:

- **Exactly-once semantics**: Through ACK tracking
- **Deterministic assignment**: Consistent hashing for multi-consumer
- **In-memory offsets**: Fast read-after-ACK consistency
- **Batch operations**: Amortizes overhead across messages

### 4. Reader

Low-level memory-mapped file reader:

```
┌──────────────────────────────────────┐
│              Reader                  │
├──────────────────────────────────────┤
│ - Memory-mapped files                │
│ - LRU file cache                     │
│ - Atomic remapping                   │
│ - Decompression                      │
└──────────────────────────────────────┘
         │
         ▼
   MappedFile Cache
  (Bounded by memory)
```

Performance optimizations:

- **Zero-copy reads**: Direct memory access
- **Lock-free updates**: Atomic pointer swaps
- **Bounded cache**: Automatic eviction under pressure
- **Lazy mapping**: Files mapped on demand

## Data Flow

### Write Path

```
Write Request
     │
     ▼
Client.Write()
     │
     ├─→ Select shard (hash/round-robin)
     │
     ▼
Shard.Write()
     │
     ├─→ Compress if > threshold
     ├─→ Write to buffered file
     ├─→ Update memory-mapped state
     ├─→ Add to index (periodic)
     └─→ Rotate file if > max size
```

Latency breakdown:

- Hash computation: ~10ns
- Compression check: ~5ns
- Buffer write: ~100ns
- State update: ~50ns
- **Total: < 1μs typical**

### Read Path

```
Read Request
     │
     ▼
Consumer.ReadBatch()
     │
     ├─→ Determine assigned shards
     │
     ▼
For each shard:
     │
     ├─→ Get/create Reader
     ├─→ Memory-map files
     ├─→ Binary search index
     ├─→ Read entries
     └─→ Decompress if needed
```

### Retention Path

```
Retention Timer
     │
     ▼
Client.cleanupShard()
     │
     ├─→ List files
     ├─→ Apply retention rules
     ├─→ Check consumer offsets
     ├─→ Update index atomically
     └─→ Delete eligible files
```

## Memory-Mapped State

Each shard maintains a 1KB state file with cache-line aligned sections:

```
┌─────────────────────────┐ Line 0 (0-63): Header
│ Version (8B)            │ Format version
│ WriteOffset (8B)        │ Current write position
│ LastEntryNumber (8B)    │ Sequence counter
│ LastIndexUpdate (8B)    │ Index modification time
│ ActiveFileIndex (8B)    │ Current file index
│ FileSize (8B)           │ Active file size
│ LastFileSequence (8B)   │ File naming counter
├─────────────────────────┤ Line 1 (64-127): Write metrics
│ TotalEntries (8B)       │ Total entries written
│ TotalBytes (8B)         │ Uncompressed bytes
│ TotalWrites (8B)        │ Write operations
│ LastWriteNanos (8B)     │ Last write timestamp
│ CurrentBatchSize (8B)   │ Current batch size
│ TotalBatches (8B)       │ Total batches
│ FailedWrites (8B)       │ Write failures
├─────────────────────────┤ Line 2 (128-191): Compression
│ TotalCompressed (8B)    │ Compressed bytes
│ CompressedEntries (8B)  │ Compressed count
│ SkippedCompression (8B) │ Too small to compress
│ CompressionRatio (8B)   │ Average ratio * 100
│ CompressionTime (8B)    │ Total compress time
├─────────────────────────┤ Line 3 (192-255): Latency
│ WriteLatencySum (8B)    │ Sum for averaging
│ WriteLatencyCount (8B)  │ Count for averaging
│ MinWriteLatency (8B)    │ Minimum seen
│ MaxWriteLatency (8B)    │ Maximum seen
│ SyncLatencyNanos (8B)   │ Time in fsync
├─────────────────────────┤ Line 4 (256-319): File ops
│ FilesCreated (8B)       │ Total files created
│ FilesDeleted (8B)       │ Removed by retention
│ FileRotations (8B)      │ Successful rotations
│ RotationTime (8B)       │ Time rotating
│ CurrentFiles (8B)       │ Current file count
│ TotalFileBytes (8B)     │ Total disk size
│ FailedRotations (8B)    │ Rotation failures
│ SyncCount (8B)          │ Total sync operations
├─────────────────────────┤ Line 5 (320-383): Checkpoint
│ CheckpointCount (8B)    │ Total checkpoints
│ LastCheckpoint (8B)     │ Last checkpoint time
│ CheckpointTime (8B)     │ Total checkpoint time
│ IndexPersistCount (8B)  │ Index saves
│ IndexPersistErrors (8B) │ Failed index saves
│ IndexSizeBytes (8B)     │ Current index size
│ BinaryIndexNodes (8B)   │ Nodes in index
├─────────────────────────┤ Line 6 (384-447): Consumers
│ ActiveReaders (8B)      │ Current readers
│ TotalReaders (8B)       │ Total readers created
│ MaxConsumerLag (8B)     │ Max entries behind
│ TotalEntriesRead (8B)   │ Read operations
│ ConsumerGroups (8B)     │ Active groups
│ AckedEntries (8B)       │ Acknowledged entries
│ ReaderCacheHits (8B)    │ Cache hits
├─────────────────────────┤ Line 7 (448-511): Errors
│ ErrorCount (8B)         │ Total errors
│ LastErrorNanos (8B)     │ Last error time
│ CorruptionDetected (8B) │ Corrupted entries
│ RecoveryAttempts (8B)   │ Recovery attempts
│ RecoverySuccesses (8B)  │ Successful recoveries
│ PartialWrites (8B)      │ Incomplete writes
│ ReadErrors (8B)         │ Read failures
├─────────────────────────┤ Lines 8-9 (512-639): Retention
│ RetentionRuns (8B)      │ Cleanup executions
│ LastRetention (8B)      │ Last cleanup time
│ RetentionTime (8B)      │ Total cleanup time
│ EntriesDeleted (8B)     │ Entries removed
│ BytesReclaimed (8B)     │ Space freed
│ OldestEntry (8B)        │ Oldest timestamp
│ RetentionErrors (8B)    │ Cleanup failures
│ ProtectedByConsumers(8B)│ Files kept for readers
├─────────────────────────┤ Line 10 (768-831): Reader cache
│ ReaderFileMaps (8B)     │ Files mapped
│ ReaderFileUnmaps (8B)   │ Files unmapped
│ ReaderCacheBytes (8B)   │ Current cache size
│ ReaderMappedFiles (8B)  │ Current mapped count
│ ReaderFileRemaps (8B)   │ Remaps due to growth
│ ReaderCacheEvicts (8B)  │ Evictions
├─────────────────────────┤ Lines 13-15 (832-1023):
│ Reserved (192B)         │ Future expansion
└─────────────────────────┘
```

Benefits:

- **Cache-line aligned**: Prevents false sharing
- **Atomic access**: Lock-free updates
- **Fixed size**: Predictable memory usage

## Wire Format

Each entry follows a simple, efficient format:

```
┌──────────────┬──────────────┬─────────────┐
│ Length (4B)  │ Timestamp(8B)│ Data (var)  │
└──────────────┴──────────────┴─────────────┘
```

- **Length**: Data size only (excludes 12-byte header)
- **Timestamp**: Unix nanoseconds
- **Data**: Raw bytes (optionally compressed)

Compression:

- Automatic for entries > threshold (configurable)
- Zstd compression for high ratio with low CPU overhead
- Detected by zstd magic bytes (0x28B52FFD) at start of data
- No compression flag needed - transparent to readers

## Index Format

Binary format for fast lookups and persistence:

```
┌────────────────────────────┐
│ Magic number (4B)          │ 0x434F4D54 "COMT"
├────────────────────────────┤
│ Version (4B)               │ Currently 1
├────────────────────────────┤
│ Current entry number (8B)  │
├────────────────────────────┤
│ Current write offset (8B)  │
├────────────────────────────┤
│ Consumer count (4B)        │
├────────────────────────────┤
│ Index node count (4B)      │
├────────────────────────────┤
│ File count (4B)            │
├────────────────────────────┤
│ Consumer offsets           │ For each consumer:
│ - Group name length (1B)   │ - Length of group name
│ - Group name (N bytes)     │ - UTF-8 group name
│ - Offset (8B)              │ - Consumer offset
├────────────────────────────┤
│ Binary search nodes        │ For each node (20B):
│ - Entry number (8B)        │ - Entry number
│ - File index (4B)          │ - Index into files array
│ - Byte offset (8B)         │ - Offset within file
├────────────────────────────┤
│ File info array            │ For each file:
│ - Path length (2B)         │ - Length of file path
│ - Path (N bytes)           │ - File path string
│ - Start offset (8B)        │ - Starting byte offset
│ - End offset (8B)          │ - Ending byte offset
│ - Start entry (8B)         │ - First entry number
│ - Entry count (8B)         │ - Number of entries
│ - Start time (8B)          │ - Unix nano timestamp
│ - End time (8B)            │ - Unix nano timestamp
└────────────────────────────┘
```

## Key Design Decisions

### 1. Entry-Based Addressing

- Uses sequential entry numbers instead of byte offsets
- Simplifies consumer offset management
- Enables consistent ordering across files

### 2. Bounded Resources

- Configurable limits on:
  - File sizes (rotation threshold)
  - Memory usage (reader cache)
  - Retention (time and size)
- Predictable resource consumption

### 3. Lock-Free Operations

- Atomic operations for hot paths
- Memory-mapped state for coordination
- Minimal mutex usage (only Client level)

### 4. Zero-Copy Reads

- Memory-mapped files for direct access
- No intermediate buffers
- Automatic remapping as files grow

### 5. Multi-Process Safety

- Shared memory state per shard
- Process-based shard ownership
- Process slot management

## Performance Characteristics

### Write Performance

- **Latency**: < 1μs typical, < 10μs p99
- **Throughput**: > 1M entries/sec per shard
- **Bottlenecks**: File I/O, compression

### Read Performance

- **Latency**: < 100μs for recent data
- **Throughput**: Limited by memory bandwidth
- **Scaling**: Linear with number of readers

### Resource Usage

- **Memory**: ~1KB state + configurable cache
- **Disk**: Efficient compression, automatic cleanup
- **CPU**: Minimal (compression optional)
