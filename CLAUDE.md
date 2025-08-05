# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with
code in this repository.

## Package overview

High-performance embedded segmented log for edge observability. Built for single-digit microsecond latency and bounded resources.

## Key Components

### Core Components

- **Client** - The main entry point for the package. Manages shards, handles writes/reads, and coordinates retention policies. Thread-safe for concurrent access.

- **Shard** - Represents a single data stream partition. Each shard has its own set of files, index, and manages its own writes. Shards enable horizontal scaling and parallel processing.

- **Consumer** - Provides high-level reading interface with consumer groups, automatic offset management, and batch processing capabilities. Supports exactly-once semantics through ACK tracking.

- **Reader** - Low-level memory-mapped file reader with lock-free concurrent access. Uses atomic operations for dynamic file remapping as files grow.

### Storage Components

- **ShardIndex** - Maintains metadata for each shard including file locations, consumer offsets, and a binary searchable index for O(log n) entry lookups.

- **BinarySearchableIndex** - Enables fast entry lookups by maintaining periodic checkpoints (every N entries). Reduces memory usage while providing efficient access patterns.

- **FileInfo** - Tracks individual data files including byte ranges, entry counts, and timestamps. Used for file rotation and retention management.

- **CometState** - Memory-mapped state file (1KB) per shard providing lock-free metrics and coordination. Cache-line aligned structure with comprehensive metrics across 15 cache lines.

### Supporting Components

- **StateRecovery** - Validates and recovers from corrupted state files. Provides automatic recovery with synchronization between state and index.

- **ProcessID** - Manages process slot assignment for multi-process deployments. Supports automatic dead process detection and slot reclamation.

- **Retention** - Background goroutine for automated data lifecycle management. Implements time and size-based retention with consumer protection.

### Data Format

- **Wire Format** - Each entry consists of: `[uint32 length][uint64 timestamp][byte[] data]` where length is data size only (excludes 12-byte header)
- **Compression** - Automatic zstd compression for entries above configurable threshold. Detected by magic bytes (0x28B52FFD) at start of data
- **File Rotation** - Automatic rotation when files exceed size limits (default: 1GB)

### Advanced Features

- **Smart Sharding** - Built-in support for consistent hash-based sharding with helper functions for shard selection and distribution
- **Multi-Process Safety** - Process-based shard ownership for safe concurrent access from multiple processes
- **Retention Management** - Configurable time and size-based retention with protection for unconsumed data
- **Metrics** - Comprehensive metrics tracking including write latency, compression ratios, and consumer lag

## Configuration

The package uses a hierarchical configuration structure:

```go
config := comet.DefaultCometConfig()
config.Compression.MinCompressSize = 1024      // Compression settings
config.Indexing.BoundaryInterval = 100          // Indexing behavior
config.Storage.MaxFileSize = 1 << 30            // File storage
config.Retention.MaxAge = 4 * time.Hour         // Retention policy
```

Convenience constructors are available:

- `HighCompressionConfig()` - Optimizes for storage efficiency
- `MultiProcessConfig()` - Enables multi-process coordination
- `HighThroughputConfig()` - Optimizes for write performance

## Common Development Commands

After making any significant change, run the following commands:

```bash
mise run lint      # Run all linting (staticcheck, go vet, deadcode)
mise run test      # Run tests with race detection
mise run bench     # Run benchmarks
```

Or the individual commands:

```bash
go vet ./...
go test ./... -race
go test ./... -bench=. -benchmem
deadcode -test ./...
```

**IMPORTANT**: Always use the `-race` flag when running tests to detect data races and ensure thread safety.

## Code Style

- Generally speaking, do not write any code comments unless it's for documenting the function, e.g. jsdoc or godoc comments.
- Do not worry about formatting. We use `dprint` for this.
- Prefer to use modern Golang, e.g. `any` over `interface{}`. We are using the latest versions of Go.
- Try to add tests to existing test files if possible. If it really doesn't fit or files are getting too big, then you can create a new one.
- To avoid hanging tests/deadlocks, use the `-timeout` flag with a reasonable value, e.g. `go test -timeout 10s ./...`.
- If you're writing Go code with structs that will be heavily used, make sure they are properly memory-aligned / padded. We have a test for this now.
- Our philosophy is "minimum effective abstraction". Keep things simple and avoid unnecessary complexity. Justify your decisions _with benchmarks_ aka _data_ and ensure that the code is easy to understand and maintain. Software should _elegant_.

## Other Tips

- If you want to know if some Go code is likely to compile, use `go vet ./...` instead of `go build ./...`
- If you're writing tests and the tests are failing, before changing the test code, make sure it's the _test_ that needs fixing and not the _implementation_.
- Tests should be comprehensive and cover failure modes, resource exhaustion, and the _critical_ edge cases.
- To run the benchmarks we care _most_ about not regressing, use `mise run bench:core`
- Run `go vet ./...`, `go test ./...`, `go test ./... -bench=. -benchmem` after significant changes.
