package comet

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"github.com/klauspost/compress/zstd"
)

// Wire format for each entry:
// [uint32 length][uint64 timestamp][byte[] data]

// MmapSharedState provides instant multi-process coordination
// Just 8 bytes - a timestamp indicating when the index was last modified
type MmapSharedState struct {
	LastUpdateNanos int64 // Nanosecond timestamp of last index change
}

// SequenceState is a memory-mapped structure for atomic entry number generation
type SequenceState struct {
	LastEntryNumber int64 // Last allocated entry number
}

// DURABILITY SEMANTICS:
//
// 1. Write Acknowledgment:
//    - Add() returns successfully after data is written to OS buffers
//    - Direct I/O bypasses page cache when available (Linux)
//    - Not guaranteed durable until explicit Sync() or checkpoint
//
// 2. Checkpointing (automatic durability):
//    - Index persisted every 10,000 writes OR 5 seconds
//    - Data file sync'd during checkpoint
//    - Consumer offsets persisted immediately on ACK
//
// 3. Crash Recovery:
//    - Files scanned from last checkpoint to find actual EOF
//    - Partial writes at EOF are discarded
//    - Consumer offsets recovered from last persisted index
//
// 4. Consistency Guarantees:
//    - Entry-based addressing ensures compression compatibility
//    - Atomic index updates prevent torn reads
//    - Memory barriers ensure proper ordering
//
// 5. Performance vs Durability Trade-offs:
//    - Fast writes: Data buffered, periodic sync
//    - Crash safety: Index checkpoints provide recovery points
//    - Consumer safety: ACKs are immediately durable

const (
	headerSize              = 12      // 4 bytes length + 8 bytes timestamp
	defaultBufSize          = 1 << 20 // 1MB buffer
	checkpointWrites        = 10000
	maxFileSize             = 1 << 30   // 1GB per file
	preallocSize            = 128 << 20 // 128MB preallocation
	minCompressSize         = 256       // Don't compress entries smaller than this
	vectorIOBatch           = 64        // Number of entries to batch for vectored I/O
	defaultBoundaryInterval = 100       // Store boundaries every N entries for memory efficiency
	compressionWorkers      = 4         // Number of parallel compression workers
	compressionQueueSize    = 256       // Buffer size for compression queue
	defaultShardCount       = 16        // Default number of shards for load distribution
)

// Smart Sharding Helpers

// ShardStreamName generates a stream name for a specific shard
func ShardStreamName(namespace string, version string, shardID uint32) string {
	return fmt.Sprintf("%s:%s:shard:%04d", namespace, version, shardID)
}

// PickShard selects a shard based on a key using consistent hashing
// Returns a shard ID in the range [0, shardCount)
func PickShard(key string, shardCount uint32) uint32 {
	if shardCount == 0 {
		shardCount = defaultShardCount
	}

	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32() % shardCount
}

// PickShardStream combines PickShard and ShardStreamName for convenience
// Example: PickShardStream("user123", "events", "v1", 16) -> "events:v1:shard:0007"
func PickShardStream(key, namespace, version string, shardCount uint32) string {
	shardID := PickShard(key, shardCount)
	return ShardStreamName(namespace, version, shardID)
}

// AllShardsRange returns a slice of shard IDs from 0 to shardCount-1
// Useful for consumers that need to read from all shards
func AllShardsRange(shardCount uint32) []uint32 {
	if shardCount == 0 {
		shardCount = defaultShardCount
	}

	shards := make([]uint32, shardCount)
	for i := uint32(0); i < shardCount; i++ {
		shards[i] = i
	}
	return shards
}

// AllShardStreams returns stream names for all shards in a namespace
func AllShardStreams(namespace, version string, shardCount uint32) []string {
	if shardCount == 0 {
		shardCount = defaultShardCount
	}

	streams := make([]string, shardCount)
	for i := uint32(0); i < shardCount; i++ {
		streams[i] = ShardStreamName(namespace, version, i)
	}
	return streams
}

// CometStats tracks key metrics for monitoring comet performance
type CometStats struct {
	// Write metrics
	TotalEntries     uint64 `json:"total_entries"`
	TotalBytes       uint64 `json:"total_bytes"`
	TotalCompressed  uint64 `json:"total_compressed_bytes"`
	WriteLatencyNano uint64 `json:"avg_write_latency_nano"`
	MinWriteLatency  uint64 `json:"min_write_latency_nano"`
	MaxWriteLatency  uint64 `json:"max_write_latency_nano"`

	// Compression metrics
	CompressionRatio   uint64 `json:"compression_ratio_x100"` // x100 for fixed point
	CompressedEntries  uint64 `json:"compressed_entries"`
	SkippedCompression uint64 `json:"skipped_compression"`

	// File management
	TotalFiles      uint64 `json:"total_files"`
	FileRotations   uint64 `json:"file_rotations"`
	CheckpointCount uint64 `json:"checkpoint_count"`
	LastCheckpoint  uint64 `json:"last_checkpoint_nano"`

	// Consumer metrics
	ActiveReaders uint64 `json:"active_readers"`
	ConsumerLag   uint64 `json:"max_consumer_lag_bytes"`

	// Error tracking
	ErrorCount         uint64 `json:"error_count"`
	LastErrorNano      uint64 `json:"last_error_nano"`
	IndexPersistErrors uint64 `json:"index_persist_errors"`

	// Compression metrics
	CompressionWaitNano uint64 `json:"compression_wait_nano"`
}

// ClientMetrics holds atomic counters for thread-safe metrics tracking
type ClientMetrics struct {
	TotalEntries       atomic.Uint64
	TotalBytes         atomic.Uint64
	TotalCompressed    atomic.Uint64
	WriteLatencyNano   atomic.Uint64
	MinWriteLatency    atomic.Uint64
	MaxWriteLatency    atomic.Uint64
	CompressionRatio   atomic.Uint64
	CompressedEntries  atomic.Uint64
	SkippedCompression atomic.Uint64
	TotalFiles         atomic.Uint64
	FileRotations      atomic.Uint64
	CheckpointCount    atomic.Uint64
	LastCheckpoint     atomic.Uint64
	ActiveReaders      atomic.Uint64
	ConsumerLag        atomic.Uint64
	ErrorCount         atomic.Uint64
	LastErrorNano      atomic.Uint64
	CompressionWait    atomic.Uint64 // Time waiting for compression
	IndexPersistErrors atomic.Uint64 // Failed index persistence attempts
}

// RetentionConfig defines data retention policies
type RetentionConfig struct {
	// Time-based retention
	MaxAge time.Duration `json:"max_age"` // Delete files older than this

	// Size-based retention
	MaxTotalSize int64 `json:"max_total_size"` // Total size limit across all shards
	MaxShardSize int64 `json:"max_shard_size"` // Size limit per shard

	// Cleanup behavior
	CleanupInterval   time.Duration `json:"cleanup_interval"`   // How often to run cleanup
	MinFilesToKeep    int           `json:"min_files_to_keep"`  // Always keep at least N files
	ProtectUnconsumed bool          `json:"protect_unconsumed"` // Don't delete unread data
	ForceDeleteAfter  time.Duration `json:"force_delete_after"` // Delete even if unread after this time
}

// CompressionConfig controls compression behavior
type CompressionConfig struct {
	MinCompressSize int `json:"min_compress_size"` // Don't compress entries smaller than this
}

// IndexingConfig controls indexing behavior
type IndexingConfig struct {
	BoundaryInterval int `json:"boundary_interval"` // Store boundaries every N entries
	MaxIndexEntries  int `json:"max_index_entries"` // Max boundary entries per shard (0 = unlimited)
}

// StorageConfig controls file storage behavior
type StorageConfig struct {
	MaxFileSize    int64 `json:"max_file_size"`      // Maximum size per file before rotation
	CheckpointTime int   `json:"checkpoint_time_ms"` // Checkpoint every N milliseconds
}

// ConcurrencyConfig controls multi-process behavior
type ConcurrencyConfig struct {
	EnableMultiProcessMode bool `json:"enable_file_locking"` // Enable file-based locking for multi-process safety
}

// CometConfig holds configuration for comet client
type CometConfig struct {
	// Compression settings
	Compression CompressionConfig `json:"compression"`

	// Indexing settings
	Indexing IndexingConfig `json:"indexing"`

	// Storage settings
	Storage StorageConfig `json:"storage"`

	// Concurrency settings
	Concurrency ConcurrencyConfig `json:"concurrency"`

	// Retention policy
	Retention RetentionConfig `json:"retention"`
}

// DefaultCometConfig returns sensible defaults optimized for logging workloads
func DefaultCometConfig() CometConfig {
	return CometConfig{
		// Compression - optimized for logging workloads
		Compression: CompressionConfig{
			MinCompressSize: 4096, // Only compress entries >4KB to avoid latency hit on typical logs
		},

		// Indexing - memory efficient boundary tracking
		Indexing: IndexingConfig{
			BoundaryInterval: 100,   // Store boundaries every 100 entries
			MaxIndexEntries:  10000, // Limit index memory growth
		},

		// Storage - optimized for 1GB files
		Storage: StorageConfig{
			MaxFileSize:    1 << 30, // 1GB per file
			CheckpointTime: 2000,    // Checkpoint every 2 seconds
		},

		// Concurrency - disabled by default for performance
		// Enable this for multi-process deployments (e.g., prefork mode)
		Concurrency: ConcurrencyConfig{
			EnableMultiProcessMode: false, // Single-process mode is faster
		},

		// Retention - defaults for edge observability
		Retention: RetentionConfig{
			MaxAge:            4 * time.Hour,    // Keep 4 hours of data
			MaxTotalSize:      10 << 30,         // 10GB total
			MaxShardSize:      1 << 30,          // 1GB per shard
			CleanupInterval:   15 * time.Minute, // Check every 15 minutes
			MinFilesToKeep:    2,                // Keep at least 2 files
			ProtectUnconsumed: true,             // Don't delete unread data by default
			ForceDeleteAfter:  24 * time.Hour,   // Force delete after 24 hours
		},
	}
}

// HighCompressionConfig returns a config optimized for high compression ratios
// Suitable for text-heavy logs and structured data
func HighCompressionConfig() CometConfig {
	cfg := DefaultCometConfig()
	cfg.Compression.MinCompressSize = 256 // Compress almost everything
	return cfg
}

// MultiProcessConfig returns a config suitable for multi-process deployments
// Enable this for prefork servers or when multiple processes write to the same stream
func MultiProcessConfig() CometConfig {
	cfg := DefaultCometConfig()
	cfg.Concurrency.EnableMultiProcessMode = true
	cfg.Storage.CheckpointTime = 100 // More frequent checkpoints for multi-process coordination
	return cfg
}

// HighThroughputConfig returns a config optimized for high write throughput
// Trades some memory for better performance
func HighThroughputConfig() CometConfig {
	cfg := DefaultCometConfig()
	cfg.Indexing.BoundaryInterval = 1000    // Less frequent indexing
	cfg.Indexing.MaxIndexEntries = 100000   // Allow larger index
	cfg.Storage.CheckpointTime = 5000       // Less frequent checkpoints
	cfg.Compression.MinCompressSize = 10240 // Only compress very large entries
	return cfg
}

// migrateConfig migrates deprecated fields to new structure
func migrateConfig(cfg *CometConfig) {
	// Currently no migrations needed since we removed all deprecated fields
}

// validateConfig ensures configuration values are reasonable
func validateConfig(cfg *CometConfig) error {
	// Storage validation
	if cfg.Storage.MaxFileSize < 100 {
		return fmt.Errorf("MaxFileSize must be at least 100 bytes, got %d", cfg.Storage.MaxFileSize)
	}
	if cfg.Storage.MaxFileSize > 10<<30 { // 10GB
		return fmt.Errorf("MaxFileSize cannot exceed 10GB, got %d", cfg.Storage.MaxFileSize)
	}
	if cfg.Storage.CheckpointTime < 0 {
		return fmt.Errorf("CheckpointTime cannot be negative, got %d", cfg.Storage.CheckpointTime)
	}
	if cfg.Storage.CheckpointTime > 300000 { // 5 minutes
		return fmt.Errorf("CheckpointTime cannot exceed 5 minutes (300000ms), got %d", cfg.Storage.CheckpointTime)
	}

	// Compression validation
	if cfg.Compression.MinCompressSize < 0 {
		return fmt.Errorf("MinCompressSize cannot be negative, got %d", cfg.Compression.MinCompressSize)
	}
	// Note: MinCompressSize can be larger than MaxFileSize to effectively disable compression

	// Indexing validation
	if cfg.Indexing.BoundaryInterval < 1 {
		return fmt.Errorf("BoundaryInterval must be at least 1, got %d", cfg.Indexing.BoundaryInterval)
	}
	if cfg.Indexing.BoundaryInterval > 10000 {
		return fmt.Errorf("BoundaryInterval cannot exceed 10000, got %d", cfg.Indexing.BoundaryInterval)
	}
	if cfg.Indexing.MaxIndexEntries < 0 {
		return fmt.Errorf("MaxIndexEntries cannot be negative, got %d", cfg.Indexing.MaxIndexEntries)
	}
	if cfg.Indexing.MaxIndexEntries > 0 && cfg.Indexing.MaxIndexEntries < 10 {
		return fmt.Errorf("MaxIndexEntries must be 0 (unlimited) or at least 10, got %d", cfg.Indexing.MaxIndexEntries)
	}

	// Retention validation
	if cfg.Retention.MaxShardSize < 0 {
		return fmt.Errorf("MaxShardSize cannot be negative, got %d", cfg.Retention.MaxShardSize)
	}
	if cfg.Retention.MaxShardSize > 0 && cfg.Retention.MaxShardSize < cfg.Storage.MaxFileSize {
		return fmt.Errorf("MaxShardSize must be 0 (unlimited) or at least MaxFileSize (%d), got %d",
			cfg.Storage.MaxFileSize, cfg.Retention.MaxShardSize)
	}
	if cfg.Retention.MaxAge < 0 {
		return fmt.Errorf("MaxAge cannot be negative, got %v", cfg.Retention.MaxAge)
	}

	return nil
}

// Client implements a local file-based stream client with append-only storage
type Client struct {
	dataDir     string
	config      CometConfig
	shards      map[uint32]*Shard
	metrics     ClientMetrics
	mu          sync.RWMutex
	closed      bool
	retentionWg sync.WaitGroup
	stopCh      chan struct{}
}

// Shard represents a single stream shard with its own files
// Fields ordered for optimal memory alignment (64-bit words first)
type Shard struct {
	// 64-bit aligned fields first (8 bytes each)
	readerCount     int64     // Lock-free reader tracking
	lastCheckpoint  time.Time // 64-bit on most systems
	lastMmapCheck   int64     // Last mmap timestamp we saw (for change detection)
	sequenceCounter *int64    // Memory-mapped sequence counter for file naming

	// Pointers (8 bytes each on 64-bit)
	dataFile      *os.File         // Data file handle
	writer        *bufio.Writer    // Buffered writer
	compressor    *zstd.Encoder    // Compression engine
	index         *ShardIndex      // Shard metadata
	lockFile      *os.File         // File lock for multi-writer safety
	indexLockFile *os.File         // Separate lock for index writes
	mmapState     *MmapSharedState // Memory-mapped coordination state
	sequenceState *SequenceState   // Memory-mapped sequence state for entry numbers
	sequenceFile  *os.File         // File handle for sequence counter
	mmapWriter    *MmapWriter      // Memory-mapped writer for ultra-fast multi-process writes

	// Strings (24 bytes: ptr + len + cap)
	indexPath         string // Path to index file
	indexStatePath    string // Path to index state file
	indexLockPath     string // Path to index lock file
	sequenceStatePath string // Path to sequence counter file
	indexStateData    []byte // Memory-mapped index state data (slice header: 24 bytes)
	sequenceStateData []byte // Memory-mapped sequence state data (slice header: 24 bytes)

	// Mutex (platform-specific, often 24 bytes)
	mu      sync.RWMutex
	writeMu sync.Mutex // Protects DirectWriter from concurrent writes
	indexMu sync.Mutex // Protects index file writes

	// Synchronization for background operations
	wg sync.WaitGroup // Tracks background goroutines

	// Smaller fields last
	writesSinceCheckpoint int    // 8 bytes
	shardID               uint32 // 4 bytes
	// 4 bytes padding will be added automatically for 8-byte alignment
}

// EntryIndexNode represents a node in the binary searchable index
type EntryIndexNode struct {
	EntryNumber int64         `json:"entry_number"` // Entry number this node covers
	Position    EntryPosition `json:"position"`     // Position in files
}

// BinarySearchableIndex provides O(log n) entry lookups
type BinarySearchableIndex struct {
	// Sorted slice of index nodes for binary search
	Nodes []EntryIndexNode `json:"nodes"`
	// Interval between indexed entries (default: 1000)
	IndexInterval int `json:"index_interval"`
	// Maximum number of nodes to keep (0 = unlimited)
	MaxNodes int `json:"max_nodes"`
}

// ShardIndex tracks files and consumer offsets
// Fixed to use entry-based addressing instead of byte offsets
// Fields ordered for optimal memory alignment
type ShardIndex struct {
	// 64-bit aligned fields first (8 bytes each)
	CurrentEntryNumber int64 `json:"current_entry_number"` // Entry-based tracking (not byte offsets!)
	CurrentWriteOffset int64 `json:"current_write_offset"` // Still track for file management

	// Maps (8 bytes pointer each)
	ConsumerOffsets map[string]int64 `json:"consumer_entry_offsets"` // Consumer tracking by entry number (not bytes!)

	// Composite types
	BinaryIndex BinarySearchableIndex `json:"binary_index"` // Binary searchable index for O(log n) lookups

	// Slices (24 bytes: ptr + len + cap)
	Files []FileInfo `json:"files"` // File management

	// Strings (24 bytes: ptr + len + cap)
	CurrentFile string `json:"current_file"`

	// Smaller fields last
	BoundaryInterval int `json:"boundary_interval"` // 8 bytes
}

// AddIndexNode adds a new entry to the binary searchable index
func (bi *BinarySearchableIndex) AddIndexNode(entryNumber int64, position EntryPosition) {
	// Only index entries at the specified interval to limit memory usage
	if bi.IndexInterval == 0 {
		bi.IndexInterval = 1000 // Default interval
	}

	if entryNumber%int64(bi.IndexInterval) == 0 || entryNumber == 0 {
		node := EntryIndexNode{
			EntryNumber: entryNumber,
			Position:    position,
		}

		// Use binary search to find insertion point to maintain sorted order
		idx, found := slices.BinarySearchFunc(bi.Nodes, node, func(a, b EntryIndexNode) int {
			if a.EntryNumber < b.EntryNumber {
				return -1
			}
			if a.EntryNumber > b.EntryNumber {
				return 1
			}
			return 0
		})

		if !found {
			// Insert at the correct position to maintain sorted order
			bi.Nodes = slices.Insert(bi.Nodes, idx, node)

			// Check if we need to prune old nodes
			if bi.MaxNodes > 0 && len(bi.Nodes) > bi.MaxNodes {
				// Remove the oldest (first) node
				bi.Nodes = bi.Nodes[1:]
			}
		} else {
			// Update existing node
			bi.Nodes[idx] = node
		}
	}
}

// FindEntry uses binary search to locate an entry position
func (bi *BinarySearchableIndex) FindEntry(entryNumber int64) (EntryPosition, bool) {
	if len(bi.Nodes) == 0 {
		return EntryPosition{}, false
	}

	// Find the largest indexed entry <= target
	target := EntryIndexNode{EntryNumber: entryNumber}
	idx, found := slices.BinarySearchFunc(bi.Nodes, target, func(a, b EntryIndexNode) int {
		if a.EntryNumber < b.EntryNumber {
			return -1
		}
		if a.EntryNumber > b.EntryNumber {
			return 1
		}
		return 0
	})

	if found {
		// Exact match
		return bi.Nodes[idx].Position, true
	}

	if idx == 0 {
		// Target is before the first indexed entry
		return EntryPosition{}, false
	}

	// Return the position of the largest indexed entry before target
	// The caller will need to scan forward from this position
	return bi.Nodes[idx-1].Position, true
}

// GetScanStartPosition returns the best starting position for scanning to find an entry
func (bi *BinarySearchableIndex) GetScanStartPosition(entryNumber int64) (EntryPosition, int64, bool) {
	if len(bi.Nodes) == 0 {
		return EntryPosition{}, 0, false
	}

	// Find the largest indexed entry <= target
	target := EntryIndexNode{EntryNumber: entryNumber}
	idx, found := slices.BinarySearchFunc(bi.Nodes, target, func(a, b EntryIndexNode) int {
		if a.EntryNumber < b.EntryNumber {
			return -1
		}
		if a.EntryNumber > b.EntryNumber {
			return 1
		}
		return 0
	})

	if found {
		// Exact match
		return bi.Nodes[idx].Position, bi.Nodes[idx].EntryNumber, true
	}

	if idx == 0 {
		// Target is before the first indexed entry, start from beginning
		return EntryPosition{FileIndex: 0, ByteOffset: 0}, 0, true
	}

	// Return the position of the largest indexed entry before target
	node := bi.Nodes[idx-1]
	return node.Position, node.EntryNumber, true
}

// EntryPosition tracks where an entry is located
// Fields ordered for optimal memory alignment
type EntryPosition struct {
	ByteOffset int64 `json:"byte_offset"` // Byte position within that file (8 bytes)
	FileIndex  int   `json:"file_index"`  // Which file in Files array (8 bytes)
}

// FileInfo tracks a single data file
// Fields ordered for optimal memory alignment
type FileInfo struct {
	// 64-bit aligned fields first
	StartOffset int64     `json:"start_offset"`
	EndOffset   int64     `json:"end_offset"`
	StartEntry  int64     `json:"start_entry"` // First entry number in this file
	Entries     int64     `json:"entries"`
	StartTime   time.Time `json:"start_time"`
	EndTime     time.Time `json:"end_time"`

	// String last (will use remaining space efficiently)
	Path string `json:"path"`
}

// NewClient creates a new comet client for local file-based streaming with default config
func NewClient(dataDir string) (*Client, error) {
	return NewClientWithConfig(dataDir, DefaultCometConfig())
}

// NewClientWithConfig creates a new comet client with custom configuration
func NewClientWithConfig(dataDir string, config CometConfig) (*Client, error) {
	// Migrate deprecated fields to new structure
	migrateConfig(&config)

	// Validate configuration
	if err := validateConfig(&config); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	c := &Client{
		dataDir: dataDir,
		config:  config,
		shards:  make(map[uint32]*Shard),
		stopCh:  make(chan struct{}),
	}

	// Start retention manager if configured
	c.startRetentionManager()

	return c, nil
}

// Append adds entries to a stream shard (append-only semantics)
func (c *Client) Append(ctx context.Context, stream string, entries [][]byte) ([]MessageID, error) {
	// Extract shard from stream name (e.g., "events:v1:shard:0042" -> 42)
	shardID, err := parseShardFromStream(stream)
	if err != nil {
		return nil, fmt.Errorf("invalid stream name %s: %w", stream, err)
	}

	shard, err := c.getOrCreateShard(shardID)
	if err != nil {
		return nil, err
	}

	return shard.appendEntries(entries, &c.metrics, &c.config)
}

// Len returns the number of entries in a stream shard
func (c *Client) Len(ctx context.Context, stream string) (int64, error) {
	shardID, err := parseShardFromStream(stream)
	if err != nil {
		return 0, fmt.Errorf("invalid stream name %s: %w", stream, err)
	}

	shard, err := c.getOrCreateShard(shardID)
	if err != nil {
		return 0, err
	}

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	// If file locking is enabled, reload index to get latest state from other processes
	// Only reload if the index file exists and is newer than our last checkpoint
	if c.config.Concurrency.EnableMultiProcessMode && shard.lockFile != nil {
		if indexStat, err := os.Stat(shard.indexPath); err == nil {
			if indexStat.ModTime().After(shard.lastCheckpoint) {
				// Acquire shared lock for reading
				err := syscall.Flock(int(shard.lockFile.Fd()), syscall.LOCK_SH)
				if err != nil {
					return 0, fmt.Errorf("failed to acquire shared lock for Len: %w", err)
				}
				defer syscall.Flock(int(shard.lockFile.Fd()), syscall.LOCK_UN)

				// Reload index to get current state
				if err := shard.loadIndex(); err != nil {
					return 0, fmt.Errorf("failed to reload index for Len: %w", err)
				}
			}
		}
	}

	// In multi-process mode, check if we need to rebuild index AFTER reloading
	if shard.mmapState != nil {
		// Need write lock for rebuild
		shard.mu.RUnlock()
		shard.mu.Lock()
		shard.lazyRebuildIndexIfNeeded(c.config, filepath.Join(c.dataDir, fmt.Sprintf("shard-%04d", shard.shardID)))
		shard.mu.Unlock()
		shard.mu.RLock()
	}

	var total int64
	for _, file := range shard.index.Files {
		total += file.Entries
	}

	return total, nil
}

// Sync ensures all buffered data is durably written to disk
func (c *Client) Sync(ctx context.Context) error {
	c.mu.RLock()
	shards := make([]*Shard, 0, len(c.shards))
	for _, shard := range c.shards {
		shards = append(shards, shard)
	}
	c.mu.RUnlock()

	for _, shard := range shards {
		shard.mu.Lock()

		// Flush and sync writer
		if shard.writer != nil {
			shard.writeMu.Lock()
			err := shard.writer.Flush()
			if err == nil && shard.dataFile != nil {
				err = shard.dataFile.Sync()
			}
			shard.writeMu.Unlock()
			if err != nil {
				shard.mu.Unlock()
				return fmt.Errorf("failed to sync shard %d: %w", shard.shardID, err)
			}
		}

		// Force checkpoint
		// Clone index while holding lock
		indexCopy := shard.cloneIndex()
		shard.writesSinceCheckpoint = 0
		shard.lastCheckpoint = time.Now()
		shard.mu.Unlock()

		// Persist outside of lock
		// For multi-process safety, use the separate index lock if enabled
		var err error
		if c.config.Concurrency.EnableMultiProcessMode && shard.indexLockFile != nil {
			// Acquire exclusive lock for index writes
			if err := syscall.Flock(int(shard.indexLockFile.Fd()), syscall.LOCK_EX); err != nil {
				return fmt.Errorf("failed to acquire index lock for shard %d: %w", shard.shardID, err)
			}

			shard.indexMu.Lock()
			// Reload index from disk to merge with other processes' changes
			if _, statErr := os.Stat(shard.indexPath); statErr == nil {
				// Index exists, load it to get latest state
				if diskIndex, loadErr := shard.loadBinaryIndex(); loadErr == nil {
					// Merge our changes with the disk state
					// Important: We need to merge carefully to avoid losing entries
					// The disk state represents entries written by other processes

					// Always use the highest entry number and write offset
					if diskIndex.CurrentEntryNumber > indexCopy.CurrentEntryNumber {
						indexCopy.CurrentEntryNumber = diskIndex.CurrentEntryNumber
					}
					if diskIndex.CurrentWriteOffset > indexCopy.CurrentWriteOffset {
						indexCopy.CurrentWriteOffset = diskIndex.CurrentWriteOffset
					}

					// Merge consumer offsets - keep the highest offset for each consumer
					for group, offset := range diskIndex.ConsumerOffsets {
						if currentOffset, exists := indexCopy.ConsumerOffsets[group]; !exists || offset > currentOffset {
							indexCopy.ConsumerOffsets[group] = offset
						}
					}

					// Merge file info - this is critical for multi-process coordination
					// The disk version should have the most complete file information
					if len(diskIndex.Files) > 0 {
						// Always use the disk version as it represents the actual files
						indexCopy.Files = diskIndex.Files

						// Update the current file info if we have one
						if len(indexCopy.Files) > 0 && indexCopy.CurrentFile != "" {
							lastFile := &indexCopy.Files[len(indexCopy.Files)-1]
							// Update the last file's end offset and entry count based on our writes
							if lastFile.Path == indexCopy.CurrentFile {
								lastFile.EndOffset = indexCopy.CurrentWriteOffset
								// Calculate actual entries in the file
								if diskIndex.CurrentEntryNumber > 0 {
									entriesInOtherFiles := int64(0)
									for i := 0; i < len(indexCopy.Files)-1; i++ {
										entriesInOtherFiles += indexCopy.Files[i].Entries
									}
									lastFile.Entries = indexCopy.CurrentEntryNumber - entriesInOtherFiles
								}
							}
						}
					}

					// Merge binary index nodes if needed
					if diskIndex.BinaryIndex.Nodes != nil && len(diskIndex.BinaryIndex.Nodes) > len(indexCopy.BinaryIndex.Nodes) {
						indexCopy.BinaryIndex = diskIndex.BinaryIndex
					}
				}
			}

			err = shard.saveBinaryIndex(indexCopy)
			shard.indexMu.Unlock()

			// Update mmap state BEFORE releasing lock
			if err == nil {
				shard.updateMmapState()
			}

			// Release lock immediately after index save
			syscall.Flock(int(shard.indexLockFile.Fd()), syscall.LOCK_UN)
		} else {
			shard.indexMu.Lock()
			err = shard.saveBinaryIndex(indexCopy)
			shard.indexMu.Unlock()

			if err == nil {
				shard.updateMmapState()
			}
		}

		if err != nil {
			return fmt.Errorf("failed to persist index for shard %d: %w", shard.shardID, err)
		}
	}

	return nil
}

// getOrCreateShard returns an existing shard or creates a new one
func (c *Client) getOrCreateShard(shardID uint32) (*Shard, error) {
	c.mu.RLock()
	shard, exists := c.shards[shardID]
	c.mu.RUnlock()

	if exists {
		return shard, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check after acquiring write lock
	if shard, exists = c.shards[shardID]; exists {
		return shard, nil
	}

	// Create new shard
	shardDir := filepath.Join(c.dataDir, fmt.Sprintf("shard-%04d", shardID))
	if err := os.MkdirAll(shardDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create shard directory: %w", err)
	}

	shard = &Shard{
		shardID:           shardID,
		indexPath:         filepath.Join(shardDir, "index.bin"),
		indexStatePath:    filepath.Join(shardDir, "index.state"),
		indexLockPath:     filepath.Join(shardDir, "index.lock"),
		sequenceStatePath: filepath.Join(shardDir, "sequence.state"),
		index: &ShardIndex{
			BoundaryInterval: c.config.Indexing.BoundaryInterval,
			ConsumerOffsets:  make(map[string]int64),
			BinaryIndex: BinarySearchableIndex{
				IndexInterval: c.config.Indexing.BoundaryInterval,
				MaxNodes:      c.config.Indexing.MaxIndexEntries, // Use full limit for binary index
			},
		},
		lastCheckpoint: time.Now(),
	}

	// Create lock files for multi-writer coordination if enabled
	if c.config.Concurrency.EnableMultiProcessMode {
		// Data write lock
		lockPath := filepath.Join(shardDir, "shard.lock")
		lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return nil, fmt.Errorf("failed to create lock file: %w", err)
		}
		shard.lockFile = lockFile

		// Separate index write lock for better concurrency
		indexLockFile, err := os.OpenFile(shard.indexLockPath, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			lockFile.Close()
			return nil, fmt.Errorf("failed to create index lock file: %w", err)
		}
		shard.indexLockFile = indexLockFile
	}

	// Initialize mmap shared state only if file locking is enabled
	if c.config.Concurrency.EnableMultiProcessMode {
		if err := shard.initMmapState(); err != nil {
			return nil, fmt.Errorf("failed to initialize mmap state: %w", err)
		}
		if err := shard.initSequenceState(); err != nil {
			return nil, fmt.Errorf("failed to initialize sequence state: %w", err)
		}

		// Initialize memory-mapped writer for ultra-fast writes
		mmapWriter, err := NewMmapWriter(shardDir, c.config.Storage.MaxFileSize, shard.index, &c.metrics)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize mmap writer: %w", err)
		}
		shard.mmapWriter = mmapWriter
	}

	// Load existing index if present
	if err := shard.loadIndex(); err != nil {
		return nil, err
	}

	// If we have an mmap writer, sync the index with the coordination state
	if shard.mmapWriter != nil {
		coordState := shard.mmapWriter.CoordinationState()
		writeOffset := coordState.WriteOffset.Load()
		if writeOffset > shard.index.CurrentWriteOffset {
			shard.index.CurrentWriteOffset = writeOffset
		}
	}

	// Open or create current data file
	if err := shard.openDataFileWithConfig(shardDir, &c.config); err != nil {
		return nil, err
	}

	// Recover from crash if needed
	if err := shard.recoverFromCrash(); err != nil {
		return nil, err
	}

	c.shards[shardID] = shard
	return shard, nil
}

// CompressedEntry represents a pre-compressed entry ready for writing
type CompressedEntry struct {
	Data           []byte
	OriginalSize   uint64
	CompressedSize uint64
	WasCompressed  bool
}

// preCompressEntries compresses entries outside of any locks to reduce contention
func (s *Shard) preCompressEntries(entries [][]byte, config *CometConfig) []CompressedEntry {
	compressed := make([]CompressedEntry, len(entries))

	for i, data := range entries {
		originalSize := uint64(len(data))

		if len(data) >= config.Compression.MinCompressSize && s.compressor != nil {
			// Compress the data
			compressedData := s.compressor.EncodeAll(data, nil)
			compressed[i] = CompressedEntry{
				Data:           compressedData,
				OriginalSize:   originalSize,
				CompressedSize: uint64(len(compressedData)),
				WasCompressed:  true,
			}
		} else {
			// Use original data directly (zero-copy)
			compressed[i] = CompressedEntry{
				Data:           data,
				OriginalSize:   originalSize,
				CompressedSize: originalSize,
				WasCompressed:  false,
			}
		}
	}

	return compressed
}

// WriteRequest represents a batch write operation
// Fields ordered for optimal memory alignment
type WriteRequest struct {
	UpdateFunc   func() error // Function to update index state after successful write (8 bytes)
	WriteBuffers [][]byte     // Buffers to write (24 bytes)
	IDs          []MessageID  // Message IDs for the batch (24 bytes)
}

// appendEntries adds raw entry bytes to the shard with I/O outside locks
func (s *Shard) appendEntries(entries [][]byte, clientMetrics *ClientMetrics, config *CometConfig) ([]MessageID, error) {
	startTime := time.Now()

	// Pre-compress entries OUTSIDE the lock to reduce contention
	compressedEntries := s.preCompressEntries(entries, config)

	// Prepare write request while holding lock
	var writeReq WriteRequest
	var totalOriginalBytes, totalCompressedBytes uint64
	var compressedCount, skippedCount uint64

	// Critical section: prepare write data and update indices
	func() {
		s.mu.Lock()
		defer s.mu.Unlock()

		// Check mmap state for instant change detection (no lock needed for read)
		if config.Concurrency.EnableMultiProcessMode && s.mmapState != nil {
			currentTimestamp := atomic.LoadInt64(&s.mmapState.LastUpdateNanos)
			if currentTimestamp != s.lastMmapCheck {
				// Index changed - reload it with retry for EOF errors
				if err := s.loadIndexWithRetry(); err != nil {
					panic(fmt.Errorf("failed to reload index after detecting mmap change: %w", err))
				}
				s.lastMmapCheck = currentTimestamp
			}
		}

		writeReq.IDs = make([]MessageID, len(entries))
		now := startTime.UnixNano()

		// Build write buffers from pre-compressed data (minimal work under lock)
		writeReq.WriteBuffers = make([][]byte, 0, len(entries)*2) // headers + data

		// Allocate header buffer for this specific request (no sharing)
		requiredSize := len(entries) * headerSize
		allHeaders := make([]byte, requiredSize)

		// Store initial state for rollback if write fails
		initialEntryNumber := s.index.CurrentEntryNumber
		initialWriteOffset := s.index.CurrentWriteOffset
		initialWritesSinceCheckpoint := s.writesSinceCheckpoint

		// Single-process mode: fast path with immediate updates
		if !config.Concurrency.EnableMultiProcessMode {
			for i, compressedEntry := range compressedEntries {
				totalOriginalBytes += compressedEntry.OriginalSize
				totalCompressedBytes += compressedEntry.CompressedSize

				if compressedEntry.WasCompressed {
					compressedCount++
				} else {
					skippedCount++
				}

				// Use slice of pre-allocated header buffer
				headerStart := i * headerSize
				header := allHeaders[headerStart : headerStart+headerSize]
				binary.LittleEndian.PutUint32(header[0:4], uint32(len(compressedEntry.Data)))
				binary.LittleEndian.PutUint64(header[4:12], uint64(now))

				// Add to vectored write batch
				writeReq.WriteBuffers = append(writeReq.WriteBuffers, header, compressedEntry.Data)

				// Track entry in binary index - only store at intervals for memory efficiency
				entryNumber := s.index.CurrentEntryNumber
				entrySize := int64(headerSize + len(compressedEntry.Data))

				// Calculate the correct file index and offset for this entry
				fileIndex := len(s.index.Files) - 1
				byteOffset := s.index.CurrentWriteOffset

				// If this entry will cause rotation and we're not at the start of a file,
				// it will be written to the NEXT file at offset 0
				willCauseRotation := s.index.CurrentWriteOffset+entrySize > config.Storage.MaxFileSize
				if willCauseRotation && s.index.CurrentWriteOffset > 0 {
					fileIndex = len(s.index.Files) // Will be the next file after rotation
					byteOffset = 0
				}

				position := EntryPosition{
					FileIndex:  fileIndex,
					ByteOffset: byteOffset,
				}

				// Add to binary searchable index (it handles intervals and pruning internally)
				s.index.BinaryIndex.AddIndexNode(entryNumber, position)

				// Generate ID for this entry
				writeReq.IDs[i] = MessageID{EntryNumber: entryNumber, ShardID: s.shardID}

				// Update index state (will be rolled back if write fails)
				s.index.CurrentEntryNumber++
				s.index.CurrentWriteOffset += entrySize
				s.writesSinceCheckpoint++
			}
		} else {
			// Multi-process mode: use atomic sequence for entry numbers
			// Track binary index updates for deferred application
			type binaryIndexUpdate struct {
				entryNumber int64
				position    EntryPosition
			}
			var binaryIndexUpdates []binaryIndexUpdate

			// Pre-allocate entry numbers atomically
			entryNumbers := make([]int64, len(entries))
			if s.sequenceState != nil {
				for i := range entries {
					oldVal := atomic.LoadInt64(&s.sequenceState.LastEntryNumber)
					newVal := atomic.AddInt64(&s.sequenceState.LastEntryNumber, 1)
					entryNumbers[i] = newVal - 1
					if false { // Enable for debugging
						log.Printf("Shard %d: seq %d->%d, allocated entry %d",
							s.shardID, oldVal, newVal, entryNumbers[i])
					}
				}
			} else {
				// Fallback if sequence state not initialized
				baseEntry := s.index.CurrentEntryNumber
				for i := range entries {
					entryNumbers[i] = baseEntry + int64(i)
				}
			}

			// Track write position
			writeOffset := s.index.CurrentWriteOffset

			for i, compressedEntry := range compressedEntries {
				totalOriginalBytes += compressedEntry.OriginalSize
				totalCompressedBytes += compressedEntry.CompressedSize

				if compressedEntry.WasCompressed {
					compressedCount++
				} else {
					skippedCount++
				}

				// Use slice of pre-allocated header buffer
				headerStart := i * headerSize
				header := allHeaders[headerStart : headerStart+headerSize]
				binary.LittleEndian.PutUint32(header[0:4], uint32(len(compressedEntry.Data)))
				binary.LittleEndian.PutUint64(header[4:12], uint64(now))

				// Add to vectored write batch
				writeReq.WriteBuffers = append(writeReq.WriteBuffers, header, compressedEntry.Data)

				// Track entry in binary index - only store at intervals for memory efficiency
				entrySize := int64(headerSize + len(compressedEntry.Data))
				entryNumber := entryNumbers[i]

				// Calculate the correct file index and offset for this entry
				fileIndex := len(s.index.Files) - 1
				byteOffset := writeOffset

				// If this entry will cause rotation and we're not at the start of a file,
				// it will be written to the NEXT file at offset 0
				willCauseRotation := writeOffset+entrySize > config.Storage.MaxFileSize
				if willCauseRotation && writeOffset > 0 {
					fileIndex = len(s.index.Files) // Will be the next file after rotation
					byteOffset = 0
				}

				position := EntryPosition{
					FileIndex:  fileIndex,
					ByteOffset: byteOffset,
				}

				// Store for later update
				binaryIndexUpdates = append(binaryIndexUpdates, binaryIndexUpdate{
					entryNumber: entryNumber,
					position:    position,
				})

				// Generate ID for this entry
				writeReq.IDs[i] = MessageID{EntryNumber: entryNumber, ShardID: s.shardID}

				// Track write position for next entry
				writeOffset += entrySize
			}

			// Multi-process mode: set up deferred update function
			// Calculate final entry number (highest allocated + 1)
			finalEntryNumber := int64(0)
			if len(entryNumbers) > 0 {
				finalEntryNumber = entryNumbers[len(entryNumbers)-1] + 1
			}
			finalWriteOffset := writeOffset
			writeReq.UpdateFunc = func() error {
				s.mu.Lock()
				defer s.mu.Unlock()

				// Apply the binary index updates
				for _, update := range binaryIndexUpdates {
					s.index.BinaryIndex.AddIndexNode(update.entryNumber, update.position)
				}

				// Apply the index updates
				s.index.CurrentEntryNumber = finalEntryNumber
				s.index.CurrentWriteOffset = finalWriteOffset
				s.writesSinceCheckpoint = initialWritesSinceCheckpoint + len(entries)

				// Update the current file's entry count and end offset
				if len(s.index.Files) > 0 {
					s.index.Files[len(s.index.Files)-1].Entries += int64(len(entries))
					s.index.Files[len(s.index.Files)-1].EndOffset = finalWriteOffset
				}

				// Update mmap state to notify readers
				if s.mmapState != nil {
					s.updateMmapState()
				}

				// Schedule async checkpoint instead of synchronous
				if s.writesSinceCheckpoint > 0 && time.Since(s.lastCheckpoint) > 10*time.Millisecond {
					s.scheduleAsyncCheckpoint(clientMetrics, config)
				}

				return nil
			}
		}

		// Define rollback function in case write fails (only for single-process mode)
		if !config.Concurrency.EnableMultiProcessMode {
			writeReq.UpdateFunc = func() error {
				// On failure, rollback index state
				s.mu.Lock()
				defer s.mu.Unlock()
				s.index.CurrentEntryNumber = initialEntryNumber
				s.index.CurrentWriteOffset = initialWriteOffset
				s.writesSinceCheckpoint = initialWritesSinceCheckpoint
				return nil
			}
		}
	}() // End of critical section

	// Perform I/O OUTSIDE the lock
	var writeErr error

	// Use memory-mapped writer for ultra-fast multi-process writes if available
	if config.Concurrency.EnableMultiProcessMode && s.mmapWriter != nil {
		// Extract entry numbers from IDs
		entryNumbers := make([]int64, len(writeReq.IDs))
		for i, id := range writeReq.IDs {
			entryNumbers[i] = id.EntryNumber
		}

		// Extract raw data from write buffers (skip headers, they will be recreated)
		rawEntries := make([][]byte, len(writeReq.IDs))
		for i := 0; i < len(writeReq.IDs); i++ {
			// WriteBuffers contains [header, data, header, data, ...]
			// Skip the header (index i*2) and get the data (index i*2+1)
			rawEntries[i] = writeReq.WriteBuffers[i*2+1]
		}

		// Memory-mapped write (handles its own coordination)
		writeErr = s.mmapWriter.Write(rawEntries, entryNumbers)

		// Handle rotation needed error
		if writeErr != nil && strings.Contains(writeErr.Error(), "rotation needed") {
			// Must acquire mutex before rotating to avoid race conditions
			s.mu.Lock()
			rotErr := s.rotateFile(clientMetrics, config)
			s.mu.Unlock()

			if rotErr != nil {
				return nil, fmt.Errorf("failed to rotate file: %w", rotErr)
			}

			// Retry the write after rotation
			writeErr = s.mmapWriter.Write(rawEntries, entryNumbers)
		}

		// Update index after successful mmap write - must be protected by mutex
		if writeErr == nil {
			s.mu.Lock()
			s.index.CurrentWriteOffset = s.mmapWriter.CoordinationState().WriteOffset.Load()
			s.mu.Unlock()
		}
	} else {
		// Regular write path with file locking
		if config.Concurrency.EnableMultiProcessMode && s.lockFile != nil {
			if err := syscall.Flock(int(s.lockFile.Fd()), syscall.LOCK_EX); err != nil {
				return nil, fmt.Errorf("failed to acquire shard lock for write: %w", err)
			}
			defer syscall.Flock(int(s.lockFile.Fd()), syscall.LOCK_UN)
		}

		s.writeMu.Lock()
		// Write all buffers
		for _, buf := range writeReq.WriteBuffers {
			if _, err := s.writer.Write(buf); err != nil {
				writeErr = err
				break
			}
		}
		if writeErr == nil {
			writeErr = s.writer.Flush()
			// In multi-process mode, sync to ensure data hits disk before releasing lock
			if writeErr == nil && config.Concurrency.EnableMultiProcessMode && s.dataFile != nil {
				writeErr = s.dataFile.Sync()
			}
		}
		s.writeMu.Unlock()
	}

	// Handle write error
	if writeErr != nil {
		// Track error
		clientMetrics.ErrorCount.Add(1)
		clientMetrics.LastErrorNano.Store(uint64(time.Now().UnixNano()))

		// Rollback index state
		if writeReq.UpdateFunc != nil {
			writeReq.UpdateFunc()
		}

		return nil, fmt.Errorf("failed write: %w", writeErr)
	}

	// Apply deferred updates for multi-process mode BEFORE post-write operations
	if config.Concurrency.EnableMultiProcessMode && writeReq.UpdateFunc != nil {
		if err := writeReq.UpdateFunc(); err != nil {
			return nil, fmt.Errorf("failed to update index after write: %w", err)
		}
	}

	// Track metrics
	writeLatency := uint64(time.Since(startTime).Nanoseconds())
	clientMetrics.TotalEntries.Add(uint64(len(entries)))
	clientMetrics.TotalBytes.Add(totalOriginalBytes)
	clientMetrics.TotalCompressed.Add(totalCompressedBytes)
	clientMetrics.CompressedEntries.Add(compressedCount)
	clientMetrics.SkippedCompression.Add(skippedCount)

	// Update latency metrics using EMA
	if totalOriginalBytes > 0 && totalCompressedBytes > 0 {
		ratio := (totalOriginalBytes * 100) / totalCompressedBytes // x100 for fixed point
		clientMetrics.CompressionRatio.Store(ratio)
	}

	// Track write latency with min/max
	clientMetrics.WriteLatencyNano.Store(writeLatency)
	for {
		currentMin := clientMetrics.MinWriteLatency.Load()
		if currentMin == 0 || writeLatency < currentMin {
			if clientMetrics.MinWriteLatency.CompareAndSwap(currentMin, writeLatency) {
				break
			}
		} else {
			break
		}
	}
	for {
		currentMax := clientMetrics.MaxWriteLatency.Load()
		if writeLatency > currentMax {
			if clientMetrics.MaxWriteLatency.CompareAndSwap(currentMax, writeLatency) {
				break
			}
		} else {
			break
		}
	}

	// Post-write operations
	func() {
		s.mu.Lock()
		defer s.mu.Unlock()

		// In single-process mode, update file entry count after successful write
		// (Multi-process mode updates this in the UpdateFunc)
		if !config.Concurrency.EnableMultiProcessMode && len(s.index.Files) > 0 {
			currentFile := &s.index.Files[len(s.index.Files)-1]
			currentFile.Entries += int64(len(entries))
		}

		// In multi-process mode, update mmap state immediately for visibility
		if config.Concurrency.EnableMultiProcessMode && s.mmapState != nil {
			// Fast path: Update coordination state immediately (~10ns total)
			atomic.StoreInt64(&s.mmapState.LastUpdateNanos, time.Now().UnixNano())
			if s.sequenceState != nil {
				atomic.StoreInt64(&s.sequenceState.LastEntryNumber, s.index.CurrentEntryNumber)
			}

			// Note: Async checkpointing happens in UpdateFunc now
		} else {
			// Single-process mode: use regular checkpointing
			s.maybeCheckpoint(clientMetrics, config)
		}

		// Check if we need to rotate file
		if s.index.CurrentWriteOffset > config.Storage.MaxFileSize {
			if err := s.rotateFile(clientMetrics, config); err != nil {
				panic(err) // Will be caught by deferred recover
			}
		}
	}()

	return writeReq.IDs, nil
}

// maybeCheckpoint persists the index if needed
func (s *Shard) maybeCheckpoint(clientMetrics *ClientMetrics, config *CometConfig) {
	if time.Since(s.lastCheckpoint) > time.Duration(config.Storage.CheckpointTime)*time.Millisecond {

		// Flush and sync writer
		if s.writer != nil {
			s.writeMu.Lock()
			s.writer.Flush()
			if s.dataFile != nil {
				s.dataFile.Sync()
			}
			s.writeMu.Unlock()
		}

		// Update current file info
		if len(s.index.Files) > 0 {
			current := &s.index.Files[len(s.index.Files)-1]
			current.EndOffset = s.index.CurrentWriteOffset
			current.EndTime = time.Now()
		}

		// Clone index while holding lock (caller holds the lock)
		indexCopy := s.cloneIndex()
		s.writesSinceCheckpoint = 0
		s.lastCheckpoint = time.Now()

		// Persist index in background to avoid blocking
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()

			// For multi-process safety, use the separate index lock if enabled
			var err error
			if config.Concurrency.EnableMultiProcessMode && s.indexLockFile != nil {
				// Acquire exclusive lock for index writes
				if err := syscall.Flock(int(s.indexLockFile.Fd()), syscall.LOCK_EX); err != nil {
					if clientMetrics != nil {
						clientMetrics.IndexPersistErrors.Add(1)
						clientMetrics.ErrorCount.Add(1)
						clientMetrics.LastErrorNano.Store(uint64(time.Now().UnixNano()))
					}
					return
				}

				// Reload index from disk to merge with other processes' changes
				s.indexMu.Lock()
				if _, statErr := os.Stat(s.indexPath); statErr == nil {
					// Index exists, load it to get latest state
					if diskIndex, loadErr := s.loadBinaryIndex(); loadErr == nil {
						// Merge our changes with the disk state
						// Keep the highest entry number and write offset
						if diskIndex.CurrentEntryNumber > indexCopy.CurrentEntryNumber {
							indexCopy.CurrentEntryNumber = diskIndex.CurrentEntryNumber
						}
						if diskIndex.CurrentWriteOffset > indexCopy.CurrentWriteOffset {
							indexCopy.CurrentWriteOffset = diskIndex.CurrentWriteOffset
						}

						// Merge consumer offsets - keep the highest offset for each consumer
						for group, offset := range diskIndex.ConsumerOffsets {
							if currentOffset, exists := indexCopy.ConsumerOffsets[group]; !exists || offset > currentOffset {
								indexCopy.ConsumerOffsets[group] = offset
							}
						}

						// Merge file info - use the disk version as base
						if len(diskIndex.Files) > 0 {
							indexCopy.Files = diskIndex.Files
						}

						// Merge binary index nodes if needed
						if diskIndex.BinaryIndex.Nodes != nil && len(diskIndex.BinaryIndex.Nodes) > len(indexCopy.BinaryIndex.Nodes) {
							indexCopy.BinaryIndex = diskIndex.BinaryIndex
						}
					}
				}

				// Now save the merged index
				err = s.saveBinaryIndex(indexCopy)
				s.indexMu.Unlock()

				// Update mmap state BEFORE releasing lock
				if err == nil {
					s.updateMmapState()
				}

				// Release lock immediately
				syscall.Flock(int(s.indexLockFile.Fd()), syscall.LOCK_UN)
			} else {
				// No file locking - just use process-local mutex
				s.indexMu.Lock()
				err = s.saveBinaryIndex(indexCopy)
				s.indexMu.Unlock()

				if err == nil {
					s.updateMmapState()
				}
			}

			if err != nil {
				// Track error in metrics - next checkpoint will retry
				if clientMetrics != nil {
					clientMetrics.IndexPersistErrors.Add(1)
				}
			}
		}()

		// Track checkpoint metrics (only if metrics are available)
		if clientMetrics != nil {
			clientMetrics.CheckpointCount.Add(1)
			clientMetrics.LastCheckpoint.Store(uint64(time.Now().UnixNano()))
		}
	}
}

// loadIndexWithRetry loads the index with retry logic for EOF errors
// This handles the race condition between file writes and mmap state updates
func (s *Shard) loadIndexWithRetry() error {
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		err = s.loadIndex()
		if err == nil {
			return nil
		}
		// Retry on EOF and file size errors to handle race conditions
		if (strings.Contains(err.Error(), "unexpected EOF") ||
			strings.Contains(err.Error(), "index file too small")) && attempt < 2 {
			time.Sleep(time.Duration(attempt+1) * time.Millisecond) // 1ms, 2ms
			continue
		}
		break
	}
	return err
}

// lazyRebuildIndexIfNeeded checks if the index needs rebuilding based on file sizes
// This is called on read path in multi-process mode to ensure consistency
// Caller must hold the shard lock
func (s *Shard) lazyRebuildIndexIfNeeded(config CometConfig, shardDir string) {
	if len(s.index.Files) == 0 {
		return
	}

	// Check if any file has grown beyond what the index knows
	needsRebuild := false
	for _, file := range s.index.Files {
		if stat, err := os.Stat(file.Path); err == nil {
			actualSize := stat.Size()
			if actualSize > file.EndOffset {
				needsRebuild = true
				break
			}
		}
	}

	if !needsRebuild {
		return
	}

	// Rebuild the index by scanning ALL files
	// In multi-process mode, we can't trust entry counts from partial indexes
	totalEntries := int64(0)
	newBinaryNodes := make([]EntryIndexNode, 0)
	currentEntryNum := int64(0)

	for i := range s.index.Files {
		file := &s.index.Files[i]

		// Get actual file size
		stat, err := os.Stat(file.Path)
		if err != nil {
			continue
		}
		actualSize := stat.Size()

		// In multi-process mode, read the shared coordination state to get actual data size
		scanSize := actualSize
		if config.Concurrency.EnableMultiProcessMode && i == len(s.index.Files)-1 {
			// For the current file, check the coordination state
			coordPath := filepath.Join(shardDir, "coordination.state")
			if coordFile, err := os.Open(coordPath); err == nil {
				defer coordFile.Close()

				// Map the coordination state temporarily to read it
				const stateSize = 256
				if data, err := syscall.Mmap(int(coordFile.Fd()), 0, stateSize,
					syscall.PROT_READ, syscall.MAP_SHARED); err == nil {
					defer syscall.Munmap(data)

					// Read the write offset from the shared state
					// In multi-process scenarios, retry a few times to handle timing issues
					coordState := (*MmapCoordinationState)(unsafe.Pointer(&data[0]))
					var writeOffset int64
					maxRetries := 3

					for retry := 0; retry < maxRetries; retry++ {
						writeOffset = coordState.WriteOffset.Load()
						lastWrite := coordState.LastWriteNanos.Load()
						totalWrites := coordState.TotalWrites.Load()

						// Check if we expect more writes than we see in the offset
						// If TotalWrites is significantly higher than what the offset suggests, wait for coordination to stabilize
						averageBytesPerWrite := int64(50) // Rough estimate
						expectedMinOffset := totalWrites * averageBytesPerWrite

						if totalWrites > 100 && writeOffset < expectedMinOffset/2 {
							time.Sleep(100 * time.Millisecond)
							continue
						}

						// If we see a recent write timestamp, the offset should be stable
						if lastWrite > 0 && time.Since(time.Unix(0, lastWrite)) < 100*time.Millisecond {
							break
						}

						// If this is our first try and offset seems small, wait a bit
						if retry == 0 && writeOffset < 1000 { // Less than 1KB suggests incomplete writes
							time.Sleep(50 * time.Millisecond)
							continue
						}

						break
					}

					// Get the latest coordination state values for stabilization check
					latestTotalWrites := coordState.TotalWrites.Load()

					// Wait for coordination state to stabilize if we're in a high-contention scenario
					if latestTotalWrites > 100 {

						// Wait for a brief period to let any pending writes complete
						stableTotalWrites := latestTotalWrites
						stableWriteOffset := writeOffset

						for attempts := 0; attempts < 10; attempts++ {
							time.Sleep(50 * time.Millisecond)
							currentWrites := coordState.TotalWrites.Load()
							currentOffset := coordState.WriteOffset.Load()

							if currentWrites == stableTotalWrites && currentOffset == stableWriteOffset {
								// Coordination state is stable
								break
							}

							stableTotalWrites = currentWrites
							stableWriteOffset = currentOffset
						}

						// Use the stabilized values
						writeOffset = stableWriteOffset
					}

					// In multi-process mode, if we have a large pre-allocated file (>100KB),
					// scan the entire file rather than trusting coordination state for scanning bounds
					// This handles cases where coordination state updates are delayed
					if actualSize > 100*1024 { // File is larger than 100KB - likely pre-allocated
						scanSize = actualSize
					} else if writeOffset > 0 && writeOffset < actualSize {
						scanSize = writeOffset
					} else if writeOffset >= actualSize {
						// Use file scan - this handles large pre-allocated files
					}
				}
			} else if s.mmapWriter != nil {
				// Fallback to local mmapWriter if available
				coordState := s.mmapWriter.CoordinationState()
				writeOffset := coordState.WriteOffset.Load()
				if writeOffset > 0 && writeOffset < actualSize {
					scanSize = writeOffset
				}
			}
		}

		// Always scan the entire file in multi-process mode
		// Don't trust the entry count from the partial index
		scanResult := s.scanFileForEntries(file.Path, scanSize, i, currentEntryNum)

		if scanResult.entryCount > 0 {
			file.Entries = scanResult.entryCount
			file.EndOffset = actualSize
			newBinaryNodes = append(newBinaryNodes, scanResult.indexNodes...)
			totalEntries += scanResult.entryCount
			currentEntryNum += scanResult.entryCount
		}
	}

	// Update index state
	if totalEntries > s.index.CurrentEntryNumber {
		s.index.CurrentEntryNumber = totalEntries
	}

	// Update binary index if we found new nodes
	if len(newBinaryNodes) > 0 {
		// Merge with existing nodes
		nodeMap := make(map[int64]EntryIndexNode)
		for _, node := range s.index.BinaryIndex.Nodes {
			nodeMap[node.EntryNumber] = node
		}
		for _, node := range newBinaryNodes {
			nodeMap[node.EntryNumber] = node
		}

		// Convert back to sorted slice
		s.index.BinaryIndex.Nodes = make([]EntryIndexNode, 0, len(nodeMap))
		for _, node := range nodeMap {
			s.index.BinaryIndex.Nodes = append(s.index.BinaryIndex.Nodes, node)
		}
		sort.Slice(s.index.BinaryIndex.Nodes, func(i, j int) bool {
			return s.index.BinaryIndex.Nodes[i].EntryNumber < s.index.BinaryIndex.Nodes[j].EntryNumber
		})
	}

	// Update write offset to match last file
	if len(s.index.Files) > 0 {
		lastFile := &s.index.Files[len(s.index.Files)-1]
		s.index.CurrentWriteOffset = lastFile.EndOffset
	}
}

// scanFileResult holds the results of scanning a data file
type scanFileResult struct {
	entryCount int64
	indexNodes []EntryIndexNode
}

// scanFileForEntries scans a data file to count entries and rebuild index
// This is used in multi-process mode to ensure we capture all entries
func (s *Shard) scanFileForEntries(filePath string, fileSize int64, fileIndex int, startEntryNum int64) scanFileResult {
	f, err := os.Open(filePath)
	if err != nil {
		return scanFileResult{}
	}
	defer f.Close()

	result := scanFileResult{
		indexNodes: make([]EntryIndexNode, 0),
	}

	var offset int64
	var entryNum int64 = startEntryNum
	interval := s.index.BinaryIndex.IndexInterval
	if interval <= 0 {
		interval = 100 // Default interval
	}

	for offset < fileSize {
		// Read header
		headerBuf := make([]byte, headerSize)
		n, err := f.ReadAt(headerBuf, offset)
		if err != nil || n != headerSize {
			break
		}

		// Parse header
		length := binary.LittleEndian.Uint32(headerBuf[0:4])
		timestamp := binary.LittleEndian.Uint64(headerBuf[4:12])

		// Check for uninitialized memory (zeros) - AGGRESSIVE GAP SKIPPING
		if length == 0 && timestamp == 0 {

			found := false

			// NUCLEAR APPROACH: Search every 4 bytes until we find a valid header
			// This ensures we NEVER miss entries due to gaps
			for searchOffset := offset + 4; searchOffset <= fileSize-headerSize; searchOffset += 4 {
				searchBuf := make([]byte, headerSize)
				if n, err := f.ReadAt(searchBuf, searchOffset); err == nil && n == headerSize {
					searchLength := binary.LittleEndian.Uint32(searchBuf[0:4])
					searchTimestamp := binary.LittleEndian.Uint64(searchBuf[4:12])

					// More lenient validation for gap recovery
					if searchLength > 0 && searchLength <= 10*1024*1024 && searchTimestamp > 0 {
						// Found a valid entry!
						offset = searchOffset
						found = true
						break
					}
				}
			}

			if !found {
				break
			}

			// Continue scanning from the recovered position
			continue
		}

		// Validate entry bounds
		if length == 0 || length > 100*1024*1024 { // Sanity check: max 100MB per entry
			break
		}

		// Check if full entry fits in file
		nextOffset := offset + headerSize + int64(length)
		if nextOffset > fileSize {
			break
		}

		// Add to binary index at intervals
		if entryNum%int64(interval) == 0 {
			result.indexNodes = append(result.indexNodes, EntryIndexNode{
				EntryNumber: entryNum,
				Position: EntryPosition{
					FileIndex:  fileIndex,
					ByteOffset: offset,
				},
			})
		}

		result.entryCount++
		entryNum++
		offset = nextOffset
	}

	return result
}

// scheduleAsyncCheckpoint schedules an asynchronous checkpoint for multi-process mode
// This allows writes to return immediately while index persistence happens in background
func (s *Shard) scheduleAsyncCheckpoint(clientMetrics *ClientMetrics, config *CometConfig) {
	// Clone index while holding lock (caller holds the lock)
	indexCopy := s.cloneIndex()
	s.writesSinceCheckpoint = 0
	s.lastCheckpoint = time.Now()

	// Persist index in background to avoid blocking writes
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		// For multi-process safety, use the separate index lock
		if s.indexLockFile != nil {
			// Acquire exclusive lock for index writes
			if err := syscall.Flock(int(s.indexLockFile.Fd()), syscall.LOCK_EX); err != nil {
				if clientMetrics != nil {
					clientMetrics.IndexPersistErrors.Add(1)
					clientMetrics.ErrorCount.Add(1)
					clientMetrics.LastErrorNano.Store(uint64(time.Now().UnixNano()))
				}
				return
			}
			defer syscall.Flock(int(s.indexLockFile.Fd()), syscall.LOCK_UN)
		}

		// Serialize index writes to prevent file corruption
		s.indexMu.Lock()
		err := s.saveBinaryIndex(indexCopy)
		s.indexMu.Unlock()

		if err != nil {
			// Track error in metrics - next checkpoint will retry
			if clientMetrics != nil {
				clientMetrics.IndexPersistErrors.Add(1)
				clientMetrics.ErrorCount.Add(1)
				clientMetrics.LastErrorNano.Store(uint64(time.Now().UnixNano()))
			}
		} else {
			// Track checkpoint metrics
			if clientMetrics != nil {
				clientMetrics.CheckpointCount.Add(1)
				clientMetrics.LastCheckpoint.Store(uint64(time.Now().UnixNano()))
			}
		}
	}()
}

// cloneIndex creates a deep copy of the index for safe serialization
// IMPORTANT: Caller must hold the shard mutex (either read or write lock)
func (s *Shard) cloneIndex() *ShardIndex {
	// Create a deep copy of the index
	clone := &ShardIndex{
		CurrentEntryNumber: s.index.CurrentEntryNumber,
		CurrentWriteOffset: s.index.CurrentWriteOffset,
		BoundaryInterval:   s.index.BoundaryInterval,
		CurrentFile:        s.index.CurrentFile,
	}

	// Deep copy maps

	clone.ConsumerOffsets = make(map[string]int64, len(s.index.ConsumerOffsets))
	for k, v := range s.index.ConsumerOffsets {
		clone.ConsumerOffsets[k] = v
	}

	// Deep copy binary index
	clone.BinaryIndex.IndexInterval = s.index.BinaryIndex.IndexInterval
	clone.BinaryIndex.MaxNodes = s.index.BinaryIndex.MaxNodes
	clone.BinaryIndex.Nodes = make([]EntryIndexNode, len(s.index.BinaryIndex.Nodes))
	copy(clone.BinaryIndex.Nodes, s.index.BinaryIndex.Nodes)

	// Deep copy files
	clone.Files = make([]FileInfo, len(s.index.Files))
	copy(clone.Files, s.index.Files)

	return clone
}

// persistIndex atomically writes the index to disk
// IMPORTANT: Caller must hold the shard mutex (either read or write lock)
func (s *Shard) persistIndex() error {
	// Clone the index - caller already holds lock
	indexCopy := s.cloneIndex()

	// Use binary format for efficiency
	err := s.saveBinaryIndex(indexCopy)
	if err == nil {
		// Only update mmap state after successful persistence
		s.updateMmapState()
	}
	return err
}

// discoverDataFiles scans the shard directory for all data files
// This is critical for multi-process mode where other processes may have created files
func (s *Shard) discoverDataFiles() error {
	shardDir := filepath.Dir(s.indexPath)
	entries, err := os.ReadDir(shardDir)
	if err != nil {
		return fmt.Errorf("failed to read shard directory: %w", err)
	}

	// Collect all data files
	var dataFiles []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(name, "log-") && strings.HasSuffix(name, ".comet") {
			dataFiles = append(dataFiles, filepath.Join(shardDir, name))
		}
	}

	if len(dataFiles) == 0 {
		return nil // No files yet
	}

	// Sort files by name (which includes sequence number)
	sort.Strings(dataFiles)

	// Build file info for any files not already in our index
	existingFiles := make(map[string]bool)
	for _, f := range s.index.Files {
		existingFiles[f.Path] = true
	}

	// Add any newly discovered files
	for _, filePath := range dataFiles {
		if !existingFiles[filePath] {
			// Get file info
			info, err := os.Stat(filePath)
			if err != nil {
				continue // Skip files we can't stat
			}

			// Add to index with basic info
			// The actual entry count and offsets will be updated during scanning
			s.index.Files = append(s.index.Files, FileInfo{
				Path:        filePath,
				StartOffset: 0,
				EndOffset:   info.Size(),
				StartTime:   info.ModTime(),
				EndTime:     info.ModTime(),
				Entries:     0, // Will be updated during scan
			})
		}
	}

	// Update current file to the latest one
	if len(dataFiles) > 0 {
		s.index.CurrentFile = dataFiles[len(dataFiles)-1]
	}

	return nil
}

// loadIndex loads the index from disk
func (s *Shard) loadIndex() error {
	// In multi-process mode, always scan for new files first
	if s.lockFile != nil { // Multi-process mode indicator
		if err := s.discoverDataFiles(); err != nil {
			return fmt.Errorf("failed to discover data files: %w", err)
		}
	}

	// Check if index file exists
	if _, err := os.Stat(s.indexPath); os.IsNotExist(err) {
		// No index yet, keep the already configured index
		if s.index == nil {
			// Only create default if no index was configured
			s.index = &ShardIndex{
				BoundaryInterval: defaultBoundaryInterval,
				ConsumerOffsets:  make(map[string]int64),
			}
		}
		return nil
	}

	// Save current config values before loading
	boundaryInterval := s.index.BoundaryInterval
	maxIndexEntries := s.index.BinaryIndex.MaxNodes

	// Load binary index
	index, err := s.loadBinaryIndex()
	if err != nil {
		return fmt.Errorf("failed to load binary index: %w", err)
	}

	s.index = index
	// Restore config values
	s.index.BoundaryInterval = boundaryInterval
	s.index.BinaryIndex.IndexInterval = boundaryInterval
	s.index.BinaryIndex.MaxNodes = maxIndexEntries

	// In multi-process mode, verify and update file sizes
	if s.lockFile != nil {
		for i := range s.index.Files {
			file := &s.index.Files[i]
			if info, err := os.Stat(file.Path); err == nil {
				actualSize := info.Size()
				if actualSize > file.EndOffset {
					file.EndOffset = actualSize
				}
			}
		}

		// Don't update current write offset based on file size
		// The index already has the correct write offset from when it was saved
		// For mmap files, the file size doesn't reflect the actual data written
	}

	return nil
}

// initMmapState initializes the memory-mapped shared state for multi-process coordination
func (s *Shard) initMmapState() error {
	// Create or open the mmap state file (8 bytes)
	file, err := os.OpenFile(s.indexStatePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to create mmap state file: %w", err)
	}
	defer file.Close()

	// Ensure file is exactly 8 bytes
	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat mmap state file: %w", err)
	}

	if stat.Size() == 0 {
		// New file - initialize with current timestamp
		now := time.Now().UnixNano()
		if err := binary.Write(file, binary.LittleEndian, now); err != nil {
			return fmt.Errorf("failed to initialize mmap state: %w", err)
		}
	} else if stat.Size() != 8 {
		return fmt.Errorf("mmap state file has invalid size %d, expected 8", stat.Size())
	}

	// Memory-map the file
	data, err := syscall.Mmap(int(file.Fd()), 0, 8, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("failed to mmap state file: %w", err)
	}

	s.indexStateData = data
	s.mmapState = (*MmapSharedState)(unsafe.Pointer(&data[0]))

	// Initialize our last seen timestamp
	s.lastMmapCheck = atomic.LoadInt64(&s.mmapState.LastUpdateNanos)

	return nil
}

// initSequenceState initializes the memory-mapped sequence counter for file naming
func (s *Shard) initSequenceState() error {
	// Create or open the sequence state file (8 bytes)
	file, err := os.OpenFile(s.sequenceStatePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to create sequence state file: %w", err)
	}
	s.sequenceFile = file

	// Ensure file is exactly 8 bytes
	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat sequence state file: %w", err)
	}

	if stat.Size() == 0 {
		// New file - initialize with current entry number from index
		// This ensures multi-process coordination starts from the right point
		initialSeq := s.index.CurrentEntryNumber
		// In multi-process mode, we want entries to start from 1, not 0
		// This matches the behavior of traditional file-based sequence numbering
		if initialSeq == 0 && s.index.CurrentFile == "" {
			initialSeq = 1
		}
		if err := binary.Write(file, binary.LittleEndian, initialSeq); err != nil {
			return fmt.Errorf("failed to initialize sequence state: %w", err)
		}
	} else if stat.Size() != 8 {
		return fmt.Errorf("sequence state file has invalid size %d, expected 8", stat.Size())
	}

	// Memory-map the file
	data, err := syscall.Mmap(int(file.Fd()), 0, 8, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("failed to mmap sequence state file: %w", err)
	}

	s.sequenceStateData = data
	s.sequenceCounter = (*int64)(unsafe.Pointer(&data[0]))

	// Also set up the SequenceState pointer for entry number tracking
	s.sequenceState = (*SequenceState)(unsafe.Pointer(&data[0]))

	// For now, we're using the same counter for files and entries
	// This means entry numbers will start from 1 if a file has been created
	// TODO: Use separate counters for files and entries

	return nil
}

// getNextSequence atomically increments and returns the next sequence number for file naming
func (s *Shard) getNextSequence() int64 {
	if s.sequenceCounter != nil {
		return atomic.AddInt64(s.sequenceCounter, 1)
	}
	// Fallback to timestamp if sequence counter not available (single process mode)
	return time.Now().Unix()
}

// updateMmapState atomically updates the shared timestamp to notify other processes of index changes
func (s *Shard) updateMmapState() {
	// Check if mmapState is still valid (not nil and not unmapped)
	if s.mmapState != nil && s.indexStateData != nil {
		atomic.StoreInt64(&s.mmapState.LastUpdateNanos, time.Now().UnixNano())
	}
}

// openDataFileWithConfig opens the current data file for appending with optional config for multi-process safety
func (s *Shard) openDataFileWithConfig(shardDir string, config *CometConfig) error {
	// For multi-process safety, acquire exclusive lock when creating files
	if config != nil && config.Concurrency.EnableMultiProcessMode && s.lockFile != nil {
		if err := syscall.Flock(int(s.lockFile.Fd()), syscall.LOCK_EX); err != nil {
			return fmt.Errorf("failed to acquire lock for file creation: %w", err)
		}
		defer syscall.Flock(int(s.lockFile.Fd()), syscall.LOCK_UN)
	}

	if s.index.CurrentFile == "" {
		// Create first file with sequential number
		seqNum := s.getNextSequence()
		s.index.CurrentFile = filepath.Join(shardDir, fmt.Sprintf("log-%016d.comet", seqNum))
		s.index.Files = append(s.index.Files, FileInfo{
			Path:        s.index.CurrentFile,
			StartOffset: 0,
			StartEntry:  0,
			StartTime:   time.Now(),
			Entries:     0,
		})
	}

	// Open file for appending
	file, err := os.OpenFile(s.index.CurrentFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open data file: %w", err)
	}

	s.dataFile = file

	// Create buffered writer
	// In multi-process mode, use smaller buffer to reduce conflicts
	bufSize := defaultBufSize
	if s.lockFile != nil { // Multi-process mode
		bufSize = 8192 // 8KB buffer for more frequent flushes
	}
	s.writer = bufio.NewWriterSize(s.dataFile, bufSize)

	// Set up compressor with fastest level for better throughput
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		return fmt.Errorf("failed to create compressor: %w", err)
	}
	s.compressor = enc

	return nil
}

// recoverFromCrash scans from the last checkpoint to find actual EOF
func (s *Shard) recoverFromCrash() error {
	info, err := s.dataFile.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat data file: %w", err)
	}

	actualSize := info.Size()
	if actualSize <= s.index.CurrentWriteOffset {
		s.index.CurrentWriteOffset = actualSize
		return nil
	}

	// Scan forward from index offset to validate entries
	offset := s.index.CurrentWriteOffset
	validEntries := int64(0)
	var lastEntryOffset int64

	// Open file for reading
	f, err := os.Open(s.index.CurrentFile)
	if err != nil {
		return err
	}
	defer f.Close()

	// Scan entries until EOF or corruption
	for offset < actualSize {
		if offset+headerSize > actualSize {
			break // Incomplete header
		}

		header := make([]byte, headerSize)
		if _, err := f.ReadAt(header, offset); err != nil {
			break
		}

		length := binary.LittleEndian.Uint32(header[0:4])
		if offset+headerSize+int64(length) > actualSize {
			break // Incomplete entry
		}

		// Skip zero-length entries (likely uninitialized file regions)
		if length == 0 {
			// Check if this is just zeros (uninitialized data)
			timestamp := binary.LittleEndian.Uint64(header[4:12])
			if timestamp == 0 {
				// This is uninitialized data, not a real entry
				break
			}
		}

		// Entry looks valid
		lastEntryOffset = offset
		offset += headerSize + int64(length)
		validEntries++
	}

	// Update state
	s.index.CurrentWriteOffset = offset
	s.index.CurrentEntryNumber += validEntries

	// Update the current file's entry count
	if len(s.index.Files) > 0 && validEntries > 0 {
		s.index.Files[len(s.index.Files)-1].Entries += validEntries
	}

	// Store the last valid entry position in binary index if needed
	if validEntries > 0 {
		s.index.BinaryIndex.AddIndexNode(s.index.CurrentEntryNumber-1, EntryPosition{
			FileIndex:  len(s.index.Files) - 1,
			ByteOffset: lastEntryOffset,
		})
	}

	return nil
}

// rotateFile closes current file and starts a new one
// NOTE: This method assumes the caller holds s.mu (main shard mutex)
func (s *Shard) rotateFile(clientMetrics *ClientMetrics, config *CometConfig) error {
	// Handle mmap writer rotation
	if s.mmapWriter != nil {
		// Let mmap writer handle its own file rotation
		if err := s.mmapWriter.rotateFile(); err != nil {
			return fmt.Errorf("failed to rotate mmap file: %w", err)
		}

		// Update index to reflect new file from mmap writer
		// The mmap writer will have updated its internal path
		shardDir := filepath.Dir(s.index.CurrentFile)
		seqNum := s.getNextSequence()
		newPath := filepath.Join(shardDir, fmt.Sprintf("log-%016d.comet", seqNum))
		s.index.CurrentFile = newPath

		// Add new file to index
		s.index.Files = append(s.index.Files, FileInfo{
			Path:        newPath,
			StartOffset: 0, // Mmap files always start at 0
			StartEntry:  s.index.CurrentEntryNumber,
			StartTime:   time.Now(),
			Entries:     0,
		})

		// Track file rotation
		clientMetrics.FileRotations.Add(1)
		clientMetrics.TotalFiles.Add(1)

		return nil
	}

	// Regular file writer rotation - acquire write lock to ensure no writes are in progress
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	// Regular file writer rotation
	// Close direct writer
	if s.writer != nil {
		s.writer.Flush()
	}

	// Close compressor
	if s.compressor != nil {
		s.compressor.Close()
	}

	// Close current file (safe now that we hold writeMu)
	if err := s.dataFile.Close(); err != nil {
		return fmt.Errorf("failed to close data file: %w", err)
	}

	// Update final stats for current file
	if len(s.index.Files) > 0 {
		current := &s.index.Files[len(s.index.Files)-1]
		current.EndOffset = s.index.CurrentWriteOffset
		current.EndTime = time.Now()
	}

	// For multi-process safety, acquire exclusive lock when creating new file
	if config.Concurrency.EnableMultiProcessMode && s.lockFile != nil {
		if err := syscall.Flock(int(s.lockFile.Fd()), syscall.LOCK_EX); err != nil {
			return fmt.Errorf("failed to acquire lock for file rotation: %w", err)
		}
		defer syscall.Flock(int(s.lockFile.Fd()), syscall.LOCK_UN)
	}

	// Create new file with sequential number
	shardDir := filepath.Dir(s.index.CurrentFile)
	seqNum := s.getNextSequence()
	s.index.CurrentFile = filepath.Join(shardDir, fmt.Sprintf("log-%016d.comet", seqNum))

	// Add new file to index
	s.index.Files = append(s.index.Files, FileInfo{
		Path:        s.index.CurrentFile,
		StartOffset: s.index.CurrentWriteOffset,
		StartEntry:  s.index.CurrentEntryNumber,
		StartTime:   time.Now(),
		Entries:     0,
	})

	// Track file rotation
	clientMetrics.FileRotations.Add(1)
	clientMetrics.TotalFiles.Add(1)

	// Open with preallocation and setup
	return s.openDataFileWithConfig(shardDir, config)
}

// parseShardFromStream extracts shard ID from stream name
func parseShardFromStream(stream string) (uint32, error) {
	// Expected format: "events:v1:shard:0042"
	// Use strings.LastIndex to find the last colon, then parse what follows
	lastColonIdx := -1
	for i := len(stream) - 1; i >= 0; i-- {
		if stream[i] == ':' {
			lastColonIdx = i
			break
		}
	}

	if lastColonIdx == -1 || lastColonIdx == len(stream)-1 {
		return 0, fmt.Errorf("invalid stream format: missing shard number")
	}

	// Parse the number after the last colon
	numStr := stream[lastColonIdx+1:]
	var shardID uint32

	// Manual parsing to avoid fmt.Sscanf overhead
	for _, char := range numStr {
		if char < '0' || char > '9' {
			return 0, fmt.Errorf("invalid shard number: contains non-digit")
		}
		digit := uint32(char - '0')
		if shardID > (^uint32(0)-digit)/10 {
			return 0, fmt.Errorf("invalid shard number: overflow")
		}
		shardID = shardID*10 + digit
	}

	return shardID, nil
}

// Close gracefully shuts down the client
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true

	// Stop retention manager
	if c.stopCh != nil {
		close(c.stopCh)
		c.retentionWg.Wait()
	}

	// Close all shards
	for _, shard := range c.shards {
		shard.mu.Lock()

		// Final checkpoint - pass nil for metrics since we're shutting down
		// Use the actual config for final checkpoint
		shard.maybeCheckpoint(nil, &c.config)

		// Acquire write lock to ensure no writes are in progress
		shard.writeMu.Lock()

		// Close direct writer
		if shard.writer != nil {
			shard.writer.Flush()
		}

		// Close compressor
		if shard.compressor != nil {
			shard.compressor.Close()
		}

		// Close file (safe now that we hold writeMu)
		if shard.dataFile != nil {
			shard.dataFile.Close()
		}

		// Close memory-mapped writer
		if shard.mmapWriter != nil {
			shard.mmapWriter.Close()
		}

		shard.writeMu.Unlock()
		shard.mu.Unlock()

		// Wait for background operations to complete BEFORE closing files
		shard.wg.Wait()

		// Now safe to close lock files (no more background goroutines using them)
		shard.mu.Lock()
		if shard.lockFile != nil {
			shard.lockFile.Close()
		}
		if shard.indexLockFile != nil {
			shard.indexLockFile.Close()
		}
		if shard.sequenceFile != nil {
			shard.sequenceFile.Close()
		}
		shard.mu.Unlock()

		// Now safe to unmap shared state
		if shard.indexStateData != nil {
			syscall.Munmap(shard.indexStateData)
			shard.indexStateData = nil
			shard.mmapState = nil
		}
		if shard.sequenceStateData != nil {
			syscall.Munmap(shard.sequenceStateData)
			shard.sequenceStateData = nil
			shard.sequenceCounter = nil
		}
	}

	return nil
}

// GetStats returns current metrics for monitoring
func (c *Client) GetStats() CometStats {
	var totalReaders uint64
	var maxLag uint64
	var totalFiles uint64

	// Aggregate stats from all shards
	c.mu.RLock()
	for _, shard := range c.shards {
		readerCount := atomic.LoadInt64(&shard.readerCount)
		totalReaders += uint64(readerCount)

		shard.mu.RLock()
		totalFiles += uint64(len(shard.index.Files))

		// Calculate max consumer lag in ENTRIES (not bytes!)
		for _, consumerEntry := range shard.index.ConsumerOffsets {
			lag := uint64(shard.index.CurrentEntryNumber - consumerEntry)
			if lag > maxLag {
				maxLag = lag
			}
		}
		shard.mu.RUnlock()
	}
	c.mu.RUnlock()

	// Update aggregated metrics
	c.metrics.ActiveReaders.Store(totalReaders)
	c.metrics.ConsumerLag.Store(maxLag) // This is now entry lag, not byte lag
	c.metrics.TotalFiles.Store(totalFiles)

	return CometStats{
		TotalEntries:        c.metrics.TotalEntries.Load(),
		TotalBytes:          c.metrics.TotalBytes.Load(),
		TotalCompressed:     c.metrics.TotalCompressed.Load(),
		WriteLatencyNano:    c.metrics.WriteLatencyNano.Load(),
		MinWriteLatency:     c.metrics.MinWriteLatency.Load(),
		MaxWriteLatency:     c.metrics.MaxWriteLatency.Load(),
		CompressionRatio:    c.metrics.CompressionRatio.Load(),
		CompressedEntries:   c.metrics.CompressedEntries.Load(),
		SkippedCompression:  c.metrics.SkippedCompression.Load(),
		TotalFiles:          totalFiles,
		FileRotations:       c.metrics.FileRotations.Load(),
		CheckpointCount:     c.metrics.CheckpointCount.Load(),
		LastCheckpoint:      c.metrics.LastCheckpoint.Load(),
		ActiveReaders:       totalReaders,
		ConsumerLag:         maxLag,
		ErrorCount:          c.metrics.ErrorCount.Load(),
		LastErrorNano:       c.metrics.LastErrorNano.Load(),
		IndexPersistErrors:  c.metrics.IndexPersistErrors.Load(),
		CompressionWaitNano: c.metrics.CompressionWait.Load(),
	}
}
