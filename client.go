package comet

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"hash/fnv"
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

// NOTE: MmapSharedState and SequenceState have been replaced by CometState
// which provides comprehensive metrics and coordination in a single memory-mapped structure

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

// Health represents the health status of the Comet client
type Health struct {
	Status        string    `json:"status"`    // "healthy", "degraded", "unhealthy"
	Healthy       bool      `json:"healthy"`   // Simple boolean for quick checks
	WritesOK      bool      `json:"writes_ok"` // Can we write data?
	ReadsOK       bool      `json:"reads_ok"`  // Can we read data?
	LastWriteTime time.Time `json:"last_write_time"`
	LastErrorTime time.Time `json:"last_error_time"`
	ErrorCount    uint64    `json:"error_count"`
	ActiveShards  int       `json:"active_shards"`
	TotalFiles    uint64    `json:"total_files"`
	Uptime        string    `json:"uptime"`
	Details       string    `json:"details,omitempty"` // Optional details about issues
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
	// Logging configuration
	Log LogConfig `json:"log"`
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
	logger      Logger
	shards      map[uint32]*Shard
	metrics     ClientMetrics
	mu          sync.RWMutex
	closed      bool
	retentionWg sync.WaitGroup
	stopCh      chan struct{}
	startTime   time.Time
}

// Shard represents a single stream shard with its own files
// Fields ordered for optimal memory alignment (64-bit words first)
type Shard struct {
	// 64-bit aligned fields first (8 bytes each)
	readerCount    int64     // Lock-free reader tracking
	lastCheckpoint time.Time // 64-bit on most systems
	lastMmapCheck  int64     // Last mmap timestamp we saw (for change detection)

	// Pointers (8 bytes each on 64-bit)
	dataFile          *os.File      // Data file handle
	writer            *bufio.Writer // Buffered writer
	compressor        *zstd.Encoder // Compression engine
	index             *ShardIndex   // Shard metadata
	lockFile          *os.File      // File lock for multi-writer safety
	indexLockFile     *os.File      // Separate lock for index writes
	retentionLockFile *os.File      // Separate lock for retention operations
	rotationLockFile  *os.File      // Separate lock for file rotation operations
	// NOTE: mmapState and sequenceState replaced by state
	mmapWriter *MmapWriter // Memory-mapped writer for ultra-fast multi-process writes
	state      *CometState // NEW: Unified memory-mapped state for all metrics and coordination
	logger     Logger      // Logger for this shard

	// Strings (24 bytes: ptr + len + cap)
	indexPath         string // Path to index file
	indexLockPath     string // Path to index lock file
	retentionLockPath string // Path to retention lock file
	rotationLockPath  string // Path to rotation lock file
	statePath         string // Path to unified state file
	stateData         []byte // Memory-mapped unified state data (slice header: 24 bytes)

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
	// Validate configuration
	if err := validateConfig(&config); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	// Create logger based on config
	logger := createLogger(config.Log)

	// Set debug mode from config
	if config.Log.EnableDebug {
		SetDebug(true)
	}

	c := &Client{
		dataDir:   dataDir,
		config:    config,
		logger:    logger,
		shards:    make(map[uint32]*Shard),
		stopCh:    make(chan struct{}),
		startTime: time.Now(),
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
	if shard.state != nil {
		// Check if rebuild needed while holding read lock
		needsRebuild := shard.checkIfRebuildNeeded()
		shard.mu.RUnlock()

		if needsRebuild {
			// Acquire write lock for rebuild
			shard.mu.Lock()
			// Double-check after acquiring write lock
			if shard.checkIfRebuildNeeded() {
				shard.lazyRebuildIndexIfNeeded(c.config, filepath.Join(c.dataDir, fmt.Sprintf("shard-%04d", shard.shardID)))
			}
			shard.mu.Unlock()
		}

		// Re-acquire read lock
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
										// Skip files with corrupted entry counts
										if indexCopy.Files[i].Entries >= 0 {
											entriesInOtherFiles += indexCopy.Files[i].Entries
										}
									}
									calculatedEntries := indexCopy.CurrentEntryNumber - entriesInOtherFiles

									// Prevent negative entry counts which cause corruption
									if calculatedEntries >= 0 {
										lastFile.Entries = calculatedEntries
									}
									// If calculation would be negative, keep the existing value
									// This prevents the uint64 overflow issue during serialization
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
		logger:            c.logger.WithFields("shard", shardID),
		indexPath:         filepath.Join(shardDir, "index.bin"),
		indexLockPath:     filepath.Join(shardDir, "index.lock"),
		retentionLockPath: filepath.Join(shardDir, "retention.lock"),
		rotationLockPath:  filepath.Join(shardDir, "rotation.lock"),
		statePath:         filepath.Join(shardDir, "comet.state"),
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

	// Initialize unified state (memory-mapped in multi-process mode, in-memory otherwise)
	if err := shard.initCometState(c.config.Concurrency.EnableMultiProcessMode); err != nil {
		return nil, fmt.Errorf("failed to initialize unified state: %w", err)
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

		// Separate retention lock for coordinating retention operations
		retentionLockFile, err := os.OpenFile(shard.retentionLockPath, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			lockFile.Close()
			indexLockFile.Close()
			return nil, fmt.Errorf("failed to create retention lock file: %w", err)
		}
		shard.retentionLockFile = retentionLockFile

		// Separate rotation lock for coordinating file rotation operations
		rotationLockFile, err := os.OpenFile(shard.rotationLockPath, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			lockFile.Close()
			indexLockFile.Close()
			retentionLockFile.Close()
			return nil, fmt.Errorf("failed to create rotation lock file: %w", err)
		}
		shard.rotationLockFile = rotationLockFile
	}

	// NOTE: Old mmapState and sequenceState initialization removed - replaced by CometState
	if c.config.Concurrency.EnableMultiProcessMode {
		// CometState initialization already handled above

		// Initialize memory-mapped writer for ultra-fast writes
		mmapWriter, err := NewMmapWriter(shardDir, c.config.Storage.MaxFileSize, shard.index, shard.state, shard.rotationLockFile)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize mmap writer: %w", err)
		}
		shard.mmapWriter = mmapWriter
	}

	// Load existing index if present
	if err := shard.loadIndex(); err != nil {
		return nil, err
	}

	// If we have an mmap writer, sync the index with the state
	if shard.mmapWriter != nil && shard.mmapWriter.state != nil {
		writeOffset := shard.mmapWriter.state.GetWriteOffset()
		if writeOffset > uint64(shard.index.CurrentWriteOffset) {
			shard.index.CurrentWriteOffset = int64(writeOffset)
		}
	}

	// Open or create current data file
	if err := shard.openDataFileWithConfig(shardDir, &c.config); err != nil {
		return nil, err
	}

	// Recover from crash if needed
	// Track recovery attempt
	if shard.state != nil {
		atomic.AddUint64(&shard.state.RecoveryAttempts, 1)
	}
	if err := shard.recoverFromCrash(); err != nil {
		return nil, err
	}
	// Recovery successful if we got here
	if shard.state != nil {
		atomic.AddUint64(&shard.state.RecoverySuccesses, 1)
	}

	c.shards[shardID] = shard

	// Debug log shard creation
	if Debug && c.logger != nil {
		c.logger.Debug("Created new shard",
			"shardID", shardID,
			"path", shardDir,
			"multiProcess", c.config.Concurrency.EnableMultiProcessMode)
	}

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
			compressionStart := time.Now()
			compressedData := s.compressor.EncodeAll(data, nil)
			compressionDuration := time.Since(compressionStart)

			// Track compression time
			if s.state != nil {
				atomic.AddInt64(&s.state.CompressionTimeNanos, compressionDuration.Nanoseconds())
			}

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
	var criticalErr error
	func() {
		s.mu.Lock()
		defer s.mu.Unlock()

		// Check mmap state for instant change detection (no lock needed for read)
		if config.Concurrency.EnableMultiProcessMode && s.state != nil {
			currentTimestamp := s.state.GetLastIndexUpdate()
			if currentTimestamp != s.lastMmapCheck {
				// Index changed - reload it with retry for EOF errors
				if err := s.loadIndexWithRetry(); err != nil {
					// Try to handle missing directory gracefully
					if s.handleMissingShardDirectory(err) {
						s.lastMmapCheck = currentTimestamp
					} else {
						criticalErr = fmt.Errorf("failed to reload index after detecting mmap change: %w", err)
						return
					}
				} else {
					s.lastMmapCheck = currentTimestamp
				}
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
			if s.state != nil {
				for i := range entries {
					// Use CometState for entry number allocation
					entryNumbers[i] = s.state.IncrementLastEntryNumber() - 1

					if Debug && s.logger != nil {
						s.logger.Debug("Allocated entry",
							"shard", s.shardID,
							"entryNumber", entryNumbers[i])
					}
				}
			} else {
				// Fallback if sequence state not initialized
				baseEntry := s.index.CurrentEntryNumber
				for i := range entries {
					entryNumbers[i] = baseEntry + int64(i)

					// Track in CometState if available
					if s.state != nil {
						s.state.IncrementLastEntryNumber()
					}
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

			// DEBUG: Log entry number calculation
			if Debug && s.logger != nil {
				s.logger.Debug("Entry number calculation",
					"shard", s.shardID,
					"entryNumbers", entryNumbers,
					"finalEntryNumber", finalEntryNumber,
					"initialEntryNumber", initialEntryNumber)
			}
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
				if s.state != nil {
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
		entryNumbers := make([]uint64, len(writeReq.IDs))
		for i, id := range writeReq.IDs {
			entryNumbers[i] = uint64(id.EntryNumber)
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

		// Handle missing directory errors in mmap mode
		if writeErr != nil && s.handleMissingShardDirectory(writeErr) {
			// Directory was recovered, need to reinitialize the mmap writer
			s.mu.Lock()
			if err := s.initializeMmapWriter(config); err != nil {
				s.mu.Unlock()
				return nil, fmt.Errorf("failed to reinitialize mmap writer after recovery: %w", err)
			}
			s.mu.Unlock()

			// Retry the write operation once after recovery
			writeErr = s.mmapWriter.Write(rawEntries, entryNumbers)
		}

		// Update index after successful mmap write - must be protected by mutex
		if writeErr == nil {
			s.mu.Lock()
			if s.mmapWriter.state != nil {
				s.index.CurrentWriteOffset = int64(s.mmapWriter.state.GetWriteOffset())
			}
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
		// Check if this is a missing directory error that we can recover from
		if s.handleMissingShardDirectory(writeErr) {
			// Directory was recovered, need to reinitialize the writer
			s.mu.Lock()
			if err := s.ensureWriter(config); err != nil {
				s.mu.Unlock()
				return nil, fmt.Errorf("failed to reinitialize writer after recovery: %w", err)
			}
			s.mu.Unlock()

			// Retry the write operation once after recovery
			s.writeMu.Lock()
			writeErr = nil // Reset error
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

		// If write still failed after recovery attempt, return error
		if writeErr != nil {
			// Track error
			clientMetrics.ErrorCount.Add(1)
			clientMetrics.LastErrorNano.Store(uint64(time.Now().UnixNano()))

			// Track in CometState if available
			if s.state != nil {
				atomic.AddUint64(&s.state.ErrorCount, 1)
				atomic.StoreInt64(&s.state.LastErrorNanos, time.Now().UnixNano())
				// Track failed write batch
				atomic.AddUint64(&s.state.FailedWrites, 1)
			}

			// Rollback index state
			if writeReq.UpdateFunc != nil {
				writeReq.UpdateFunc()
			}

			return nil, fmt.Errorf("failed write: %w", writeErr)
		}
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

	// Track unified state metrics (if available)
	if s.state != nil {
		atomic.AddInt64(&s.state.TotalEntries, int64(len(entries)))
		atomic.AddUint64(&s.state.TotalBytes, totalOriginalBytes)
		atomic.AddUint64(&s.state.TotalWrites, 1)
		atomic.StoreInt64(&s.state.LastWriteNanos, time.Now().UnixNano())
		atomic.AddUint64(&s.state.TotalCompressed, totalCompressedBytes)
		atomic.AddUint64(&s.state.CompressedEntries, compressedCount)
		atomic.AddUint64(&s.state.SkippedCompression, skippedCount)

		// Batch metrics
		atomic.StoreUint64(&s.state.CurrentBatchSize, uint64(len(entries)))
		atomic.AddUint64(&s.state.TotalBatches, 1)
	}

	// Update latency metrics using EMA
	if totalOriginalBytes > 0 && totalCompressedBytes > 0 {
		ratio := (totalOriginalBytes * 100) / totalCompressedBytes // x100 for fixed point
		clientMetrics.CompressionRatio.Store(ratio)
		// Also update in shard state
		if s.state != nil {
			atomic.StoreUint64(&s.state.CompressionRatio, ratio)
		}
	}

	// Track write latency with min/max
	clientMetrics.WriteLatencyNano.Store(writeLatency)

	// Update unified state latency metrics (if available)
	if s.state != nil {
		s.state.UpdateWriteLatency(writeLatency)
	}
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

		// In multi-process mode, update unified state immediately for visibility
		if config.Concurrency.EnableMultiProcessMode && s.state != nil {
			// Fast path: Update coordination state immediately (~10ns total)
			s.state.SetLastIndexUpdate(time.Now().UnixNano())
			// LastEntryNumber should be the last allocated entry, not the next one
			// CurrentEntryNumber is the next entry to be written
			if s.index.CurrentEntryNumber > 0 {
				// Sync CometState with current index if needed
				currentLastEntry := s.state.GetLastEntryNumber()
				expectedLastEntry := s.index.CurrentEntryNumber - 1
				if currentLastEntry < expectedLastEntry {
					// Catch up to the expected value
					for s.state.GetLastEntryNumber() < expectedLastEntry {
						s.state.IncrementLastEntryNumber()
					}
				}
			}

			// Note: Async checkpointing happens in UpdateFunc now
		} else {
			// Single-process mode: use regular checkpointing
			s.maybeCheckpoint(clientMetrics, config)
		}

		// Check if we need to rotate file
		if s.index.CurrentWriteOffset > config.Storage.MaxFileSize {
			if err := s.rotateFile(clientMetrics, config); err != nil {
				criticalErr = fmt.Errorf("failed to rotate file: %w", err)
				return
			}
		}
	}()

	// Check if critical section encountered an error
	if criticalErr != nil {
		return nil, criticalErr
	}

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

					// Track in CometState if available
					if s.state != nil {
						atomic.AddUint64(&s.state.IndexPersistErrors, 1)
						atomic.AddUint64(&s.state.ErrorCount, 1)
						atomic.StoreInt64(&s.state.LastErrorNanos, time.Now().UnixNano())
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

		// Track in CometState if available
		if s.state != nil {
			atomic.AddUint64(&s.state.CheckpointCount, 1)
			atomic.StoreInt64(&s.state.LastCheckpointNanos, time.Now().UnixNano())
		}
	}
}

// handleMissingShardDirectory handles the case where a shard directory was manually deleted
// Returns true if the error was handled, false if it should be propagated
func (s *Shard) handleMissingShardDirectory(err error) bool {
	// Check if this is a missing directory error
	if strings.Contains(err.Error(), "no such file or directory") ||
		strings.Contains(err.Error(), "failed to read shard directory") {
		// Shard directory was deleted - recreate it and reset state
		shardDir := filepath.Dir(s.indexPath)
		if mkdirErr := os.MkdirAll(shardDir, 0755); mkdirErr != nil {
			// If we can't recreate the directory, this is a serious error
			return false
		}
		// Reset shard state to empty - it will be rebuilt as needed
		s.index.Files = nil
		s.index.CurrentFile = ""
		s.index.CurrentEntryNumber = 0
		s.index.CurrentWriteOffset = 0
		return true
	}
	return false
}

// loadIndexWithRecovery loads the index and handles missing directory gracefully
// Returns nil if the index was loaded or missing directory was recovered
func (s *Shard) loadIndexWithRecovery() error {
	if err := s.loadIndexWithRetry(); err != nil {
		if s.handleMissingShardDirectory(err) {
			return nil // Successfully recovered from missing directory
		}
		return err // Other error, propagate it
	}
	return nil
}

// ensureWriter ensures the writer is properly initialized after recovery
// This function should be called with the shard mutex held
func (s *Shard) ensureWriter(config *CometConfig) error {
	// Get the shard directory from the index path
	shardDir := filepath.Dir(s.indexPath)

	// Make sure the shard directory exists
	if err := os.MkdirAll(shardDir, 0755); err != nil {
		return fmt.Errorf("failed to create shard directory during recovery: %w", err)
	}

	// Close existing writer and data file if they exist
	if s.writer != nil {
		s.writer.Flush() // Ignore error since we're recovering
		s.writer = nil
	}
	if s.dataFile != nil {
		s.dataFile.Close() // Ignore error since we're recovering
		s.dataFile = nil
	}

	// Reinitialize the data file and writer
	return s.openDataFileWithConfig(shardDir, config)
}

// initializeMmapWriter initializes the memory-mapped writer after recovery
// This function should be called with the shard mutex held
func (s *Shard) initializeMmapWriter(config *CometConfig) error {
	// Get the shard directory from the index path
	shardDir := filepath.Dir(s.indexPath)

	// Make sure the shard directory exists
	if err := os.MkdirAll(shardDir, 0755); err != nil {
		return fmt.Errorf("failed to create shard directory during mmap recovery: %w", err)
	}

	// Close existing mmap writer if it exists
	if s.mmapWriter != nil {
		s.mmapWriter.Close() // Ignore error since we're recovering
		s.mmapWriter = nil
	}

	// Create new mmap writer - use existing state
	mmapWriter, err := NewMmapWriter(shardDir, config.Storage.MaxFileSize, s.index, s.state, s.rotationLockFile)
	if err != nil {
		return fmt.Errorf("failed to create mmap writer: %w", err)
	}

	s.mmapWriter = mmapWriter
	return nil
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
// checkIfRebuildNeeded checks if the index needs rebuilding without acquiring locks
// Must be called while holding at least a read lock
func (s *Shard) checkIfRebuildNeeded() bool {
	if s.mmapWriter == nil || s.mmapWriter.state == nil {
		return false
	}

	currentTotalWrites := s.mmapWriter.state.GetTotalWrites()

	// Check if total writes in state exceed what we have in index
	return currentTotalWrites > uint64(s.index.CurrentEntryNumber)
}

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
			statePath := filepath.Join(shardDir, "comet.state")
			if stateFile, err := os.Open(statePath); err == nil {
				defer stateFile.Close()

				// Map the state temporarily to read it
				if data, err := syscall.Mmap(int(stateFile.Fd()), 0, CometStateSize,
					syscall.PROT_READ, syscall.MAP_SHARED); err == nil {
					defer syscall.Munmap(data)

					// Read the write offset from the shared state
					// In multi-process scenarios, retry a few times to handle timing issues
					state := (*CometState)(unsafe.Pointer(&data[0]))
					var writeOffset int64
					var totalWrites int64
					maxRetries := 3

					for retry := 0; retry < maxRetries; retry++ {
						writeOffset = int64(atomic.LoadUint64(&state.WriteOffset))
						lastWrite := atomic.LoadInt64(&state.LastWriteNanos)
						totalWrites = int64(atomic.LoadUint64(&state.TotalWrites))

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

					// Get the latest state values for stabilization check
					latestTotalWrites := totalWrites

					// Wait for state to stabilize if we're in a high-contention scenario
					if latestTotalWrites > 100 {

						// Wait for a brief period to let any pending writes complete
						stableTotalWrites := latestTotalWrites
						stableWriteOffset := writeOffset

						for attempts := 0; attempts < 10; attempts++ {
							time.Sleep(50 * time.Millisecond)
							currentWrites := int64(atomic.LoadUint64(&state.TotalWrites))
							currentOffset := int64(atomic.LoadUint64(&state.WriteOffset))

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
				if s.mmapWriter.state != nil {
					writeOffset := s.mmapWriter.state.GetWriteOffset()
					if writeOffset > 0 && writeOffset < uint64(actualSize) {
						scanSize = int64(writeOffset)
					}
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

			// Track in CometState if available
			if s.state != nil {
				atomic.AddUint64(&s.state.IndexPersistErrors, 1)
				atomic.AddUint64(&s.state.ErrorCount, 1)
				atomic.StoreInt64(&s.state.LastErrorNanos, time.Now().UnixNano())
			}
		} else {
			// Track checkpoint metrics
			if clientMetrics != nil {
				clientMetrics.CheckpointCount.Add(1)
				clientMetrics.LastCheckpoint.Store(uint64(time.Now().UnixNano()))
			}

			// Track in CometState if available
			if s.state != nil {
				atomic.AddUint64(&s.state.CheckpointCount, 1)
				atomic.StoreInt64(&s.state.LastCheckpointNanos, time.Now().UnixNano())
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

	// Calculate total file bytes while we have the index
	if s.state != nil {
		var totalBytes uint64
		for _, file := range s.index.Files {
			totalBytes += uint64(file.EndOffset - file.StartOffset)
		}
		atomic.StoreUint64(&s.state.TotalFileBytes, totalBytes)
		// Also update current files count
		atomic.StoreUint64(&s.state.CurrentFiles, uint64(len(s.index.Files)))
	}

	// Use binary format for efficiency
	err := s.saveBinaryIndex(indexCopy)
	if err == nil {
		// Only update mmap state after successful persistence
		s.updateMmapState()

		// Track successful index persistence
		if s.state != nil {
			atomic.AddUint64(&s.state.IndexPersistCount, 1)
			atomic.StoreInt64(&s.state.LastIndexUpdate, time.Now().UnixNano())
			// Update binary index node count
			atomic.StoreUint64(&s.state.BinaryIndexNodes, uint64(len(s.index.BinaryIndex.Nodes)))
		}
	} else if s.state != nil {
		// Track failed index persistence
		atomic.AddUint64(&s.state.IndexPersistErrors, 1)
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
		// No index file - attempt to rebuild from data files
		if s.index == nil {
			// Create default index structure
			s.index = &ShardIndex{
				BoundaryInterval: defaultBoundaryInterval,
				ConsumerOffsets:  make(map[string]int64),
			}
		}

		// Try to rebuild index from existing data files
		shardDir := filepath.Dir(s.indexPath)
		// Track recovery attempt
		if s.state != nil {
			atomic.AddUint64(&s.state.RecoveryAttempts, 1)
		}
		if err := s.rebuildIndexFromDataFiles(shardDir); err != nil {
			// Log the error but don't fail - empty index is better than nothing
			// This allows new shards to start fresh
			if len(s.index.Files) > 0 {
				// Only return error if we found files but couldn't read them
				return fmt.Errorf("failed to rebuild index from data files: %w", err)
			}
		}

		// Persist the rebuilt index with proper locking in multi-process mode
		if len(s.index.Files) > 0 {
			var persistErr error
			if s.indexLockFile != nil {
				// Multi-process mode: acquire index lock before persisting
				if err := syscall.Flock(int(s.indexLockFile.Fd()), syscall.LOCK_EX); err != nil {
					return fmt.Errorf("failed to acquire index lock for rebuild: %w", err)
				}
				// Check if another process already rebuilt and saved the index
				if _, statErr := os.Stat(s.indexPath); statErr == nil {
					// Index file exists now - another process beat us to it
					// Release lock and reload the index
					syscall.Flock(int(s.indexLockFile.Fd()), syscall.LOCK_UN)
					// Load the index that was created by another process
					newIndex, err := s.loadBinaryIndex()
					if err != nil {
						return fmt.Errorf("failed to load index created by another process: %w", err)
					}
					s.index = newIndex
					return nil
				}
				persistErr = s.persistIndex()
				syscall.Flock(int(s.indexLockFile.Fd()), syscall.LOCK_UN)
			} else {
				// Single-process mode: just persist
				persistErr = s.persistIndex()
			}

			if persistErr != nil {
				return fmt.Errorf("failed to persist rebuilt index: %w", persistErr)
			}
			// Track recovery success if we successfully rebuilt from files
			if s.state != nil {
				atomic.AddUint64(&s.state.RecoverySuccesses, 1)
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

// NOTE: initMmapState and initSequenceState removed - replaced by initCometState

// Global sequence counter for single-process mode
var globalSequenceCounter int64

// getNextSequence atomically increments and returns the next sequence number for file naming
func (s *Shard) getNextSequence() int64 {
	if s.state != nil {
		// Multi-process mode: use unified state's file sequence
		return int64(s.state.AddLastFileSequence(1))
	}
	// Single-process mode: use global atomic counter
	return atomic.AddInt64(&globalSequenceCounter, 1)
}

// rebuildIndexFromDataFiles scans all data files and rebuilds the index from scratch
// This is used for disaster recovery when the index file is lost or corrupted
func (s *Shard) rebuildIndexFromDataFiles(shardDir string) error {
	// Find all data files
	entries, err := os.ReadDir(shardDir)
	if err != nil {
		return fmt.Errorf("failed to read shard directory: %w", err)
	}

	// Collect and sort data files by sequence number
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
		return nil // No files to rebuild from
	}

	// Sort files by name (which includes sequence number)
	sort.Strings(dataFiles)

	// Reset index state
	s.index.Files = make([]FileInfo, 0, len(dataFiles))
	s.index.BinaryIndex.Nodes = make([]EntryIndexNode, 0)
	s.index.CurrentEntryNumber = 0
	s.index.CurrentWriteOffset = 0

	// Scan each file to rebuild the index
	for _, filePath := range dataFiles {
		fileInfo, err := s.scanDataFile(filePath)
		if err != nil {
			// Skip corrupted files but continue with others
			continue
		}

		// Add to index
		s.index.Files = append(s.index.Files, *fileInfo)

		// Update current file if this is the last one
		if filePath == dataFiles[len(dataFiles)-1] {
			s.index.CurrentFile = filePath
			s.index.CurrentWriteOffset = fileInfo.EndOffset
		}
	}

	// Update total entry count
	if len(s.index.Files) > 0 {
		lastFile := s.index.Files[len(s.index.Files)-1]
		s.index.CurrentEntryNumber = lastFile.StartEntry + lastFile.Entries
	}

	// Track recovery success
	if s.state != nil {
		atomic.AddUint64(&s.state.RecoverySuccesses, 1)
	}

	return nil
}

// scanDataFile reads a data file and extracts metadata for index rebuilding
func (s *Shard) scanDataFile(filePath string) (*FileInfo, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open data file: %w", err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	fileInfo := &FileInfo{
		Path:        filePath,
		StartOffset: 0,
		EndOffset:   stat.Size(),
		StartTime:   stat.ModTime(), // Use modification time as approximation
		EndTime:     stat.ModTime(),
	}

	// Determine starting entry number
	if len(s.index.Files) > 0 {
		prevFile := s.index.Files[len(s.index.Files)-1]
		fileInfo.StartEntry = prevFile.StartEntry + prevFile.Entries
	} else {
		fileInfo.StartEntry = 0
	}

	// Scan through the file to count entries and build index nodes
	offset := int64(0)
	entryCount := int64(0)
	buffer := make([]byte, 12) // Header size

	for offset < stat.Size() {
		// Read header
		n, err := file.ReadAt(buffer, offset)
		if err != nil || n < 12 {
			break // End of file or corrupted
		}

		// Parse header
		length := binary.LittleEndian.Uint32(buffer[0:4])
		timestamp := binary.LittleEndian.Uint64(buffer[4:12])

		// Validate entry
		if length > 100*1024*1024 { // 100MB max
			// Track corruption detection
			if s.state != nil {
				atomic.AddUint64(&s.state.CorruptionDetected, 1)
			}
			break // Corrupted entry
		}

		// Update timestamps
		entryTime := time.Unix(0, int64(timestamp))
		if entryCount == 0 {
			fileInfo.StartTime = entryTime
		}
		fileInfo.EndTime = entryTime

		// Add to binary index at intervals
		if entryCount%int64(s.index.BoundaryInterval) == 0 {
			s.index.BinaryIndex.AddIndexNode(fileInfo.StartEntry+entryCount, EntryPosition{
				FileIndex:  len(s.index.Files), // Current file index
				ByteOffset: offset,
			})
		}

		// Move to next entry
		entrySize := int64(12 + length)
		offset += entrySize
		entryCount++
	}

	fileInfo.Entries = entryCount
	fileInfo.EndOffset = offset

	return fileInfo, nil
}

// updateMmapState atomically updates the shared timestamp to notify other processes of index changes
func (s *Shard) updateMmapState() {
	// Check if state is still valid
	if s.state != nil {
		s.state.SetLastIndexUpdate(time.Now().UnixNano())
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

	// Track file creation if this is a new file
	if s.state != nil {
		// Check if file was newly created
		fileInfo, err := file.Stat()
		if err == nil && fileInfo.Size() == 0 {
			// New file created
			atomic.AddUint64(&s.state.FilesCreated, 1)
		}
		// Update current file count
		atomic.StoreUint64(&s.state.CurrentFiles, uint64(len(s.index.Files)))
	}

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
	partialWriteDetected := false

	// Open file for reading
	f, err := os.Open(s.index.CurrentFile)
	if err != nil {
		return err
	}
	defer f.Close()

	// Scan entries until EOF or corruption
	for offset < actualSize {
		if offset+headerSize > actualSize {
			partialWriteDetected = true
			break // Incomplete header
		}

		header := make([]byte, headerSize)
		if _, err := f.ReadAt(header, offset); err != nil {
			break
		}

		length := binary.LittleEndian.Uint32(header[0:4])
		if offset+headerSize+int64(length) > actualSize {
			partialWriteDetected = true
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

	// Track partial write if detected
	if partialWriteDetected && s.state != nil {
		atomic.AddUint64(&s.state.PartialWrites, 1)
	}

	return nil
}

// rotateFile closes current file and starts a new one
// NOTE: This method assumes the caller holds s.mu (main shard mutex)
func (s *Shard) rotateFile(clientMetrics *ClientMetrics, config *CometConfig) error {
	// Handle mmap writer rotation
	if s.mmapWriter != nil {
		// Update final stats for current file BEFORE rotation
		if len(s.index.Files) > 0 {
			current := &s.index.Files[len(s.index.Files)-1]
			current.EndOffset = s.index.CurrentWriteOffset
			current.EndTime = time.Now()
		}

		// Ensure the shard directory exists before rotation
		shardDir := filepath.Dir(s.index.CurrentFile)
		if err := os.MkdirAll(shardDir, 0755); err != nil {
			return fmt.Errorf("failed to create shard directory during mmap rotation: %w", err)
		}

		// Let mmap writer handle its own file rotation
		if err := s.mmapWriter.rotateFile(); err != nil {
			return fmt.Errorf("failed to rotate mmap file: %w", err)
		}

		// Update index to reflect new file from mmap writer
		// The mmap writer will have updated its internal path
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
		if s.state != nil {
			atomic.AddUint64(&s.state.FileRotations, 1)
		}

		// Debug log file rotation
		if Debug && s.logger != nil {
			var oldFile string
			if len(s.index.Files) > 1 {
				oldFile = s.index.Files[len(s.index.Files)-2].Path
			}
			s.logger.Debug("File rotated",
				"oldFile", filepath.Base(oldFile),
				"newFile", filepath.Base(newPath),
				"totalFiles", len(s.index.Files))
		}

		// Don't persist index on every rotation - it will be persisted during checkpoints
		// This avoids performance issues with frequent rotations

		// But update the metrics that would have been updated by persistIndex
		if s.state != nil {
			// Update file count metric
			atomic.StoreUint64(&s.state.CurrentFiles, uint64(len(s.index.Files)))

			// Calculate and update total file size
			var totalBytes uint64
			for _, file := range s.index.Files {
				totalBytes += uint64(file.EndOffset - file.StartOffset)
			}
			atomic.StoreUint64(&s.state.TotalFileBytes, totalBytes)
		}

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
	if s.state != nil {
		atomic.AddUint64(&s.state.FileRotations, 1)
	}

	// Ensure the shard directory exists before trying to create the new file
	if err := os.MkdirAll(shardDir, 0755); err != nil {
		return fmt.Errorf("failed to create shard directory during rotation: %w", err)
	}

	// Open with preallocation and setup
	if err := s.openDataFileWithConfig(shardDir, config); err != nil {
		return err
	}

	// Don't persist index on every rotation - it will be persisted during checkpoints
	// This avoids performance issues with frequent rotations

	// But update the metrics that would have been updated by persistIndex
	if s.state != nil {
		// Update file count metric
		atomic.StoreUint64(&s.state.CurrentFiles, uint64(len(s.index.Files)))

		// Calculate and update total file size
		var totalBytes uint64
		for _, file := range s.index.Files {
			totalBytes += uint64(file.EndOffset - file.StartOffset)
		}
		atomic.StoreUint64(&s.state.TotalFileBytes, totalBytes)
	}

	return nil
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
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true

	// Stop retention manager - close the channel while holding lock to prevent races
	var shouldWait bool
	if c.stopCh != nil {
		close(c.stopCh)
		shouldWait = true
	}
	c.mu.Unlock()

	// Wait for retention manager to finish AFTER releasing the lock
	// This prevents deadlock where retention goroutine needs RLock while we hold Lock
	if shouldWait {
		c.retentionWg.Wait()
	}

	// Re-acquire lock for shard cleanup
	c.mu.Lock()
	defer c.mu.Unlock()

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
		if shard.retentionLockFile != nil {
			shard.retentionLockFile.Close()
		}
		if shard.rotationLockFile != nil {
			shard.rotationLockFile.Close()
		}
		// NOTE: sequenceFile removed - file sequence now tracked in CometState
		shard.mu.Unlock()

		// Now safe to unmap unified state
		if shard.stateData != nil {
			syscall.Munmap(shard.stateData)
			shard.stateData = nil
			shard.state = nil
		}
	}

	return nil
}

// getAllShards returns all shards for testing purposes
func (c *Client) getAllShards() map[uint32]*Shard {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make(map[uint32]*Shard, len(c.shards))
	for k, v := range c.shards {
		result[k] = v
	}
	return result
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

// Health returns the current health status of the Comet client
func (c *Client) Health() Health {
	c.mu.RLock()
	shardCount := len(c.shards)
	c.mu.RUnlock()

	// Get current stats
	stats := c.GetStats()

	// Calculate uptime
	uptime := time.Since(c.startTime)
	uptimeStr := uptime.Truncate(time.Second).String()

	// Determine health status
	health := Health{
		Healthy:      true,
		Status:       "healthy",
		WritesOK:     true,
		ReadsOK:      true,
		ActiveShards: shardCount,
		TotalFiles:   stats.TotalFiles,
		ErrorCount:   stats.ErrorCount,
		Uptime:       uptimeStr,
	}

	// Check last write time across all shards
	var lastWriteNano int64
	c.mu.RLock()
	for _, shard := range c.shards {
		if shard.state != nil {
			shardLastWrite := atomic.LoadInt64(&shard.state.LastWriteNanos)
			if shardLastWrite > lastWriteNano {
				lastWriteNano = shardLastWrite
			}
		}
	}
	c.mu.RUnlock()

	if lastWriteNano > 0 {
		health.LastWriteTime = time.Unix(0, lastWriteNano)
	}

	// Check last error time
	if stats.LastErrorNano > 0 {
		health.LastErrorTime = time.Unix(0, int64(stats.LastErrorNano))

		// If error was recent (within 1 minute), mark as degraded
		if time.Since(health.LastErrorTime) < time.Minute {
			health.Status = "degraded"
			health.Details = "Recent errors detected"
		}
	}

	// Check for high error rate
	if stats.TotalEntries > 0 && stats.ErrorCount > 0 {
		errorRate := float64(stats.ErrorCount) / float64(stats.TotalEntries)
		if errorRate > 0.01 { // More than 1% error rate
			health.Status = "degraded"
			health.Healthy = false
			health.WritesOK = false
			if health.Details != "" {
				health.Details += "; "
			}
			health.Details += fmt.Sprintf("High error rate: %.2f%%", errorRate*100)
		}
	}

	// Check if we've written recently (within 5 minutes)
	if health.LastWriteTime.IsZero() || time.Since(health.LastWriteTime) > 5*time.Minute {
		// No recent writes, but this might be normal for low-traffic systems
		// Don't mark as unhealthy, just note it
		if stats.TotalEntries == 0 {
			if health.Details != "" {
				health.Details += "; "
			}
			health.Details += "No data written yet"
		}
	}

	// Check for index persist errors
	if stats.IndexPersistErrors > 0 {
		health.Status = "degraded"
		if health.Details != "" {
			health.Details += "; "
		}
		health.Details += fmt.Sprintf("Index persist errors: %d", stats.IndexPersistErrors)
	}

	// Simple ping test - try to access a shard
	if shardCount > 0 {
		// Try to get any shard to verify basic functionality
		c.mu.RLock()
		for _, shard := range c.shards {
			// Just accessing the shard and checking if we can read its state
			if shard.state == nil {
				health.ReadsOK = false
				health.Status = "unhealthy"
				health.Healthy = false
				if health.Details != "" {
					health.Details += "; "
				}
				health.Details += "Shard state unavailable"
			}
			break // Just check one shard
		}
		c.mu.RUnlock()
	}

	// Set final health status
	if health.Status == "unhealthy" {
		health.Healthy = false
	}

	return health
}

// initCometState initializes the unified state for metrics and coordination
// In multi-process mode, it's memory-mapped to a file for sharing between processes
// In single-process mode, it's allocated in regular memory
func (s *Shard) initCometState(multiProcessMode bool) error {
	if multiProcessMode {
		return s.initCometStateMmap()
	} else {
		return s.initCometStateMemory()
	}
}

// initCometStateMemory initializes unified state in regular memory (single-process mode)
func (s *Shard) initCometStateMemory() error {
	s.state = &CometState{}

	// Initialize version
	atomic.StoreUint64(&s.state.Version, CometStateVersion1)

	// Initialize with -1 to indicate "not yet set" for LastEntryNumber
	atomic.StoreInt64(&s.state.LastEntryNumber, -1)

	return nil
}

// initCometStateMmap initializes unified state via memory mapping (multi-process mode)
func (s *Shard) initCometStateMmap() error {
	// Create or open the unified state file
	file, err := os.OpenFile(s.statePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to open unified state file: %w", err)
	}
	defer file.Close()

	// Ensure file is the correct size
	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat unified state file: %w", err)
	}

	if stat.Size() == 0 {
		// New file - initialize with zeros and set version
		if err := file.Truncate(CometStateSize); err != nil {
			return fmt.Errorf("failed to set unified state file size: %w", err)
		}
	} else if stat.Size() != CometStateSize {
		return fmt.Errorf("unified state file has wrong size: got %d, expected %d", stat.Size(), CometStateSize)
	}

	// Memory map the file
	data, err := syscall.Mmap(int(file.Fd()), 0, CometStateSize,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("failed to mmap unified state file: %w", err)
	}

	s.stateData = data
	s.state = (*CometState)(unsafe.Pointer(&data[0]))

	// Initialize version if this is a new file
	if stat.Size() == 0 {
		atomic.StoreUint64(&s.state.Version, CometStateVersion1)
		// Initialize with -1 to indicate "not yet set" for LastEntryNumber
		atomic.StoreInt64(&s.state.LastEntryNumber, -1)
	} else {
		// Validate existing state file
		if err := s.validateAndRecoverState(); err != nil {
			return fmt.Errorf("state validation failed: %w", err)
		}
	}

	return nil
}
