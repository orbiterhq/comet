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
	// Process-level shard ownership (simplifies multi-process coordination)
	// When ProcessCount > 1, multi-process mode is automatically enabled
	ProcessID    int `json:"process_id"`    // This process's ID (0-based)
	ProcessCount int `json:"process_count"` // Total number of processes (0 or 1 = single-process mode)
}

// IsMultiProcess returns true if running in multi-process mode
func (c ConcurrencyConfig) IsMultiProcess() bool {
	return c.ProcessCount > 1
}

// Owns returns true if this process owns the given shard
func (c ConcurrencyConfig) Owns(shardID uint32) bool {
	if c.ProcessCount <= 1 {
		return true // Single-process mode - own all shards
	}
	return int(shardID%uint32(c.ProcessCount)) == c.ProcessID
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

	// Reader settings
	Reader ReaderConfig `json:"reader"`
}

// DefaultCometConfig returns sensible defaults optimized for logging workloads
func DefaultCometConfig() CometConfig {
	maxFileSize := int64(256 << 20) // 256MB per file

	cfg := CometConfig{
		// Compression - optimized for logging workloads
		Compression: CompressionConfig{
			MinCompressSize: 4096, // Only compress entries >4KB to avoid latency hit on typical logs
		},

		// Indexing - memory efficient boundary tracking
		Indexing: IndexingConfig{
			BoundaryInterval: 100,   // Store boundaries every 100 entries
			MaxIndexEntries:  10000, // Limit index memory growth
		},

		// Storage - optimized for 256MB files
		Storage: StorageConfig{
			MaxFileSize:    maxFileSize,
			CheckpointTime: 2000, // Checkpoint every 2 seconds
		},

		// Concurrency - single-process mode by default
		// Set ProcessCount > 1 for multi-process deployments (e.g., prefork mode)
		Concurrency: ConcurrencyConfig{
			ProcessID:    0, // Default to process 0
			ProcessCount: 0, // 0 = single-process mode
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

		// Reader - dynamic defaults based on storage settings
		Reader: ReaderConfigForStorage(maxFileSize),
	}

	return cfg
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
// Deprecated: Set ProcessCount > 1 instead
func MultiProcessConfig() CometConfig {
	cfg := DefaultCometConfig()
	cfg.Concurrency.ProcessCount = 2 // Default to 2 processes
	cfg.Concurrency.ProcessID = 0    // Caller should set this appropriately
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
	dataDir        string
	config         CometConfig
	logger         Logger
	shards         map[uint32]*Shard
	metrics        ClientMetrics
	mu             sync.RWMutex
	closed         bool
	retentionWg    sync.WaitGroup
	stopCh         chan struct{}
	startTime      time.Time
	activeGroups   map[string]bool // Track consumer groups used by this client
	activeGroupsMu sync.RWMutex    // Protects activeGroups
}

// isShardOwnedByProcess checks if this process owns a specific shard
// isShardOwnedByProcess removed - processes always own their shards exclusively

// Shard represents a single stream shard with its own files
// Fields ordered for optimal memory alignment (64-bit words first)
type Shard struct {
	// 64-bit aligned fields first (8 bytes each)
	readerCount    int64     // Lock-free reader tracking
	lastCheckpoint time.Time // 64-bit on most systems
	// lastMmapCheck removed - processes own their shards exclusively

	// Pointers (8 bytes each on 64-bit)
	dataFile   *os.File      // Data file handle
	writer     *bufio.Writer // Buffered writer
	compressor *zstd.Encoder // Compression engine
	index      *ShardIndex   // Shard metadata
	// Lock files removed - processes own their shards exclusively
	mmapWriter *MmapWriter // Memory-mapped writer for ultra-fast multi-process writes
	state      *CometState // Unified memory-mapped state for all metrics and coordination
	logger     Logger      // Logger for this shard

	// Strings (24 bytes: ptr + len + cap)
	indexPath string // Path to index file
	statePath string // Path to unified state file
	stateData []byte // Memory-mapped unified state data (slice header: 24 bytes)

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
		dataDir:      dataDir,
		config:       config,
		logger:       logger,
		shards:       make(map[uint32]*Shard),
		stopCh:       make(chan struct{}),
		startTime:    time.Now(),
		activeGroups: make(map[string]bool),
	}

	// Start retention manager if configured
	c.startRetentionManager()

	return c, nil
}

// registerConsumerGroup registers a consumer group as active for this client
func (c *Client) registerConsumerGroup(group string) {
	if group == "" {
		group = "default"
	}

	c.activeGroupsMu.Lock()
	c.activeGroups[group] = true
	c.activeGroupsMu.Unlock()
}

// deregisterConsumerGroup removes a consumer group from the active set
func (c *Client) deregisterConsumerGroup(group string) {
	if group == "" {
		group = "default"
	}

	c.activeGroupsMu.Lock()
	delete(c.activeGroups, group)
	c.activeGroupsMu.Unlock()
}

// getActiveGroups returns a copy of active consumer groups for this client
func (c *Client) getActiveGroups() map[string]bool {
	c.activeGroupsMu.RLock()
	defer c.activeGroupsMu.RUnlock()

	groups := make(map[string]bool, len(c.activeGroups))
	for group := range c.activeGroups {
		groups[group] = true
	}

	if IsDebug() && c.logger != nil {
		c.logger.Debug("getActiveGroups called",
			"activeGroups", groups,
			"numActiveGroups", len(groups))
	}

	return groups
}

// Append adds entries to a stream shard (append-only semantics)
func (c *Client) Append(ctx context.Context, stream string, entries [][]byte) ([]MessageID, error) {
	// Extract shard from stream name (e.g., "events:v1:shard:0042" -> 42)
	shardID, err := parseShardFromStream(stream)
	if err != nil {
		return nil, fmt.Errorf("invalid stream name %s: %w", stream, err)
	}

	// Check process ownership for writes
	if !c.config.Concurrency.Owns(shardID) {
		return nil, fmt.Errorf("shard %d is not owned by process %d (assigned to process %d)",
			shardID, c.config.Concurrency.ProcessID, int(shardID%uint32(c.config.Concurrency.ProcessCount)))
	}

	shard, err := c.getOrCreateShard(shardID)
	if err != nil {
		return nil, err
	}

	return shard.appendEntries(entries, &c.metrics, &c.config, c.getActiveGroups())
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

	// Handle potential index rebuild in multi-process mode
	if shard.state != nil && shard.checkIfRebuildNeeded() {
		shard.mu.Lock()
		if shard.checkIfRebuildNeeded() {
			shard.lazyRebuildIndexIfNeeded(c.config, filepath.Join(c.dataDir, fmt.Sprintf("shard-%04d", shard.shardID)))
		}
		shard.mu.Unlock()
	}

	// Now get the length with read lock
	shard.mu.RLock()
	defer shard.mu.RUnlock()

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

		// Update CurrentWriteOffset and EndOffset to match actual file sizes
		// This ensures GetShardStats() returns correct TotalBytes after sync
		if shard.dataFile != nil {
			if stat, err := shard.dataFile.Stat(); err == nil {
				actualSize := stat.Size()
				if actualSize > shard.index.CurrentWriteOffset {
					shard.index.CurrentWriteOffset = actualSize
				}

				// Update current file EndOffset to match actual file size
				if len(shard.index.Files) > 0 {
					current := &shard.index.Files[len(shard.index.Files)-1]
					current.EndOffset = shard.index.CurrentWriteOffset
					current.EndTime = time.Now()
				}
			}
		}

		// Force checkpoint
		// Clone index while holding lock
		indexCopy := shard.cloneIndex()
		shard.writesSinceCheckpoint = 0
		shard.lastCheckpoint = time.Now()
		shard.mu.Unlock()

		// Persist the index - no complex merging needed since processes own their shards
		shard.indexMu.Lock()
		err := shard.saveBinaryIndex(indexCopy)
		shard.indexMu.Unlock()

		if err == nil {
			shard.updateMmapState()
		}

		if err != nil {
			return fmt.Errorf("failed to persist index for shard %d: %w", shard.shardID, err)
		}
	}

	return nil
}

// loadExistingShard loads a shard that was created by another process
// getOrCreateShard returns an existing shard or creates a new one
func (c *Client) getOrCreateShard(shardID uint32) (*Shard, error) {
	processID := os.Getpid()

	if IsDebug() && c.logger != nil {
		c.logger.Debug("TRACE: getOrCreateShard entry",
			"shardID", shardID,
			"pid", processID)
	}

	c.mu.RLock()
	shard, exists := c.shards[shardID]
	c.mu.RUnlock()

	if exists {
		if c.logger != nil {
			c.logger.Debug("getOrCreateShard: shard already exists in memory",
				"shardID", shardID,
				"pid", processID)
		}
		return shard, nil
	}

	if c.logger != nil {
		c.logger.Debug("getOrCreateShard: creating new shard",
			"shardID", shardID,
			"pid", processID)
	}

	// For multi-process mode, we need to ensure only one process initializes the shard
	shardDir := filepath.Join(c.dataDir, fmt.Sprintf("shard-%04d", shardID))

	// Check if shard already exists and detect mode mismatch
	if stat, err := os.Stat(shardDir); err == nil && stat.IsDir() {
		// Since we now always use state files, we don't need to check for mode mismatches
		// The state file will exist for both single and multi-process modes
	}

	// Create shard directory if it doesn't exist
	if err := os.MkdirAll(shardDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create shard directory: %w", err)
	}

	// Since processes own their shards exclusively, no init lock needed

	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check after acquiring write lock
	if shard, exists = c.shards[shardID]; exists {
		return shard, nil
	}

	if c.logger != nil {
		c.logger.Debug("getOrCreateShard: initializing new shard",
			"shardID", shardID,
			"pid", processID)
	}

	shard = &Shard{
		shardID:   shardID,
		logger:    c.logger.WithFields("shard", shardID),
		indexPath: filepath.Join(shardDir, "index.bin"),
		statePath: filepath.Join(shardDir, "comet.state"),
		index: &ShardIndex{
			CurrentEntryNumber: 0, // Explicitly initialize to prevent garbage values
			CurrentWriteOffset: 0, // Explicitly initialize to prevent garbage values
			BoundaryInterval:   c.config.Indexing.BoundaryInterval,
			ConsumerOffsets:    make(map[string]int64),
			Files:              make([]FileInfo, 0),
			BinaryIndex: BinarySearchableIndex{
				IndexInterval: c.config.Indexing.BoundaryInterval,
				MaxNodes:      c.config.Indexing.MaxIndexEntries, // Use full limit for binary index
				Nodes:         make([]EntryIndexNode, 0),
			},
		},
		lastCheckpoint: time.Now(),
	}

	// Initialize unified state (memory-mapped in multi-process mode, in-memory otherwise)
	if err := shard.initCometState(c.config.Concurrency.IsMultiProcess()); err != nil {
		return nil, fmt.Errorf("failed to initialize unified state: %w", err)
	}

	// Since each process owns its shards exclusively in multi-process mode,
	// we don't need lock files for coordination. Processes can directly write
	// to their owned shards without locking.

	// Load existing index if present
	// IMPORTANT: Must load index BEFORE initializing mmapWriter to allow rebuild from data files
	if c.logger != nil {
		c.logger.Debug("getOrCreateShard: before loadIndex",
			"shardID", shardID,
			"pid", processID,
			"currentEntryNumber", shard.index.CurrentEntryNumber)
	}

	// Load the index with recovery support - handles rebuild if index is missing/corrupted
	if err := shard.loadIndexWithRecovery(); err != nil {
		return nil, err
	}

	if c.logger != nil {
		c.logger.Debug("getOrCreateShard: after loadIndex",
			"shardID", shardID,
			"pid", processID,
			"currentEntryNumber", shard.index.CurrentEntryNumber)
	}

	// Synchronize state with index if index has data and state is uninitialized
	// This handles the case where an index file exists from a previous session
	// but the state is fresh (single-process mode)
	if state := shard.state; state != nil && shard.index.CurrentEntryNumber > 0 && len(shard.index.Files) > 0 {
		currentLastEntryNumber := atomic.LoadInt64(&state.LastEntryNumber)
		if currentLastEntryNumber == -1 {
			// Set LastEntryNumber to the last allocated entry (CurrentEntryNumber - 1)
			// If index says CurrentEntryNumber=3, we have entries 0,1,2, so LastEntryNumber=2
			newLastEntryNumber := shard.index.CurrentEntryNumber - 1
			atomic.StoreInt64(&state.LastEntryNumber, newLastEntryNumber)

			if c.logger != nil {
				c.logger.Debug("getOrCreateShard: synchronized state with existing index",
					"shardID", shardID,
					"indexCurrentEntryNumber", shard.index.CurrentEntryNumber,
					"stateLastEntryNumber", newLastEntryNumber)
			}
		}
	}

	// If we have an mmap writer, sync the index with the state
	if shard.mmapWriter != nil && shard.mmapWriter.state != nil {
		writeOffset := shard.mmapWriter.state.GetWriteOffset()
		if writeOffset > uint64(shard.index.CurrentWriteOffset) {
			shard.index.CurrentWriteOffset = int64(writeOffset)
		}
	}

	// Initialize mmapWriter AFTER loading index to ensure rebuild works correctly
	if c.config.Concurrency.IsMultiProcess() {
		// Initialize memory-mapped writer for ultra-fast writes
		// Pass nil for rotation lock since processes own their shards exclusively
		mmapWriter, err := NewMmapWriter(shardDir, c.config.Storage.MaxFileSize, shard.index, shard.state, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize mmap writer: %w", err)
		}
		shard.mmapWriter = mmapWriter
	}

	// Open or create current data file
	if err := shard.openDataFileWithConfig(shardDir, &c.config); err != nil {
		return nil, err
	}

	// Recover from crash if needed
	// Track recovery attempt
	if state := shard.state; state != nil {
		atomic.AddUint64(&state.RecoveryAttempts, 1)
	}
	if err := shard.recoverFromCrash(); err != nil {
		return nil, err
	}
	// Recovery successful if we got here
	if state := shard.state; state != nil {
		atomic.AddUint64(&state.RecoverySuccesses, 1)
	}

	c.shards[shardID] = shard

	// Debug log shard creation
	if IsDebug() && c.logger != nil {
		c.logger.Debug("Created new shard",
			"shardID", shardID,
			"path", shardDir,
			"multiProcess", c.config.Concurrency.IsMultiProcess())
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

// storeState atomically stores the state pointer
func (s *Shard) storeState(state *CometState) {
	s.state = state
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
			if state := s.state; state != nil {
				atomic.AddInt64(&state.CompressionTimeNanos, compressionDuration.Nanoseconds())
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
func (s *Shard) appendEntries(entries [][]byte, clientMetrics *ClientMetrics, config *CometConfig, activeGroups map[string]bool) ([]MessageID, error) {
	startTime := time.Now()

	// Pre-compress entries OUTSIDE the lock to reduce contention
	compressedEntries := s.preCompressEntries(entries, config)

	// Prepare write request while holding lock
	writeReq, criticalErr := s.prepareWriteRequest(compressedEntries, startTime, config, clientMetrics)
	if criticalErr != nil {
		return nil, criticalErr
	}

	// Perform I/O OUTSIDE the lock
	writeErr := s.performWrite(writeReq, config, clientMetrics, activeGroups)
	if writeErr != nil {
		return nil, writeErr
	}

	// Apply deferred updates for multi-process mode
	if config.Concurrency.IsMultiProcess() && writeReq.UpdateFunc != nil {
		if err := writeReq.UpdateFunc(); err != nil {
			return nil, fmt.Errorf("failed to update index after write: %w", err)
		}
	}

	// Track metrics after successful write
	s.trackWriteMetrics(startTime, entries, compressedEntries, clientMetrics)

	// Post-write operations (checkpointing, rotation check)
	if err := s.performPostWriteOperations(config, clientMetrics, activeGroups, len(entries)); err != nil {
		return nil, err
	}

	return writeReq.IDs, nil
}

// prepareWriteRequest builds the write request under lock
func (s *Shard) prepareWriteRequest(compressedEntries []CompressedEntry, startTime time.Time, config *CometConfig, clientMetrics *ClientMetrics) (WriteRequest, error) {
	var writeReq WriteRequest
	var criticalErr error

	s.mu.Lock()
	defer s.mu.Unlock()

	// Since processes own their shards exclusively, no need to check for changes

	writeReq.IDs = make([]MessageID, len(compressedEntries))
	now := startTime.UnixNano()

	// Build write buffers from pre-compressed data (minimal work under lock)
	writeReq.WriteBuffers = make([][]byte, 0, len(compressedEntries)*2) // headers + data

	// Allocate header buffer for this specific request (no sharing)
	requiredSize := len(compressedEntries) * headerSize
	allHeaders := make([]byte, requiredSize)

	// Store initial state for potential rollback
	initialState := s.captureInitialState()

	// Pre-allocate entry numbers
	entryNumbers := s.allocateEntryNumbers(len(compressedEntries), config)

	// Build write request based on process mode
	if config.Concurrency.IsMultiProcess() {
		writeReq = s.buildMultiProcessWriteRequest(compressedEntries, entryNumbers, now, allHeaders, initialState, config, clientMetrics)
	} else {
		writeReq = s.buildSingleProcessWriteRequest(compressedEntries, entryNumbers, now, allHeaders)
	}

	return writeReq, criticalErr
}

// captureInitialState saves the current state for potential rollback
func (s *Shard) captureInitialState() struct {
	entryNumber           int64
	writeOffset           int64
	writesSinceCheckpoint int
} {
	return struct {
		entryNumber           int64
		writeOffset           int64
		writesSinceCheckpoint int
	}{
		entryNumber:           s.index.CurrentEntryNumber,
		writeOffset:           s.index.CurrentWriteOffset,
		writesSinceCheckpoint: s.writesSinceCheckpoint,
	}
}

// allocateEntryNumbers reserves entry numbers for the batch
func (s *Shard) allocateEntryNumbers(count int, config *CometConfig) []int64 {
	entryNumbers := make([]int64, count)

	if config.Concurrency.IsMultiProcess() {
		// Multi-process mode: use atomic state operations for thread safety
		state := s.state
		if state != nil {
			for i := range entryNumbers {
				entryNumbers[i] = state.IncrementLastEntryNumber()
			}
		} else {
			// Fallback if state not available (shouldn't happen in normal operation)
			if IsDebug() && s.logger != nil {
				s.logger.Warn("State not available, using index fallback",
					"shard", s.shardID)
			}
			baseEntry := s.index.CurrentEntryNumber
			for i := range entryNumbers {
				entryNumbers[i] = baseEntry + int64(i)
			}
		}
	} else {
		// Single-process mode: direct index access for maximum performance
		baseEntry := s.index.CurrentEntryNumber
		for i := range entryNumbers {
			entryNumbers[i] = baseEntry + int64(i)
		}
	}

	return entryNumbers
}

// buildSingleProcessWriteRequest builds the write request for single-process mode
func (s *Shard) buildSingleProcessWriteRequest(compressedEntries []CompressedEntry, entryNumbers []int64, now int64, allHeaders []byte) WriteRequest {
	var writeReq WriteRequest
	writeReq.IDs = make([]MessageID, len(compressedEntries))
	writeReq.WriteBuffers = make([][]byte, 0, len(compressedEntries)*2)

	for i, compressedEntry := range compressedEntries {
		// Use slice of pre-allocated header buffer
		headerStart := i * headerSize
		header := allHeaders[headerStart : headerStart+headerSize]
		binary.LittleEndian.PutUint32(header[0:4], uint32(len(compressedEntry.Data)))
		binary.LittleEndian.PutUint64(header[4:12], uint64(now))

		// Add to vectored write batch
		writeReq.WriteBuffers = append(writeReq.WriteBuffers, header, compressedEntry.Data)

		// Track entry in binary index
		entryNumber := entryNumbers[i]
		entrySize := int64(headerSize + len(compressedEntry.Data))

		// Calculate the correct file index and offset for this entry
		position := s.calculateEntryPosition(entrySize)

		// Add to binary searchable index (it handles intervals and pruning internally)
		s.index.BinaryIndex.AddIndexNode(entryNumber, position)

		// Update metric if state is available
		if state := s.state; state != nil {
			atomic.StoreUint64(&state.BinaryIndexNodes, uint64(len(s.index.BinaryIndex.Nodes)))
		}

		// Generate ID for this entry
		writeReq.IDs[i] = MessageID{EntryNumber: entryNumber, ShardID: s.shardID}

		s.index.CurrentEntryNumber = entryNumber + 1
		s.index.CurrentWriteOffset += entrySize
		s.writesSinceCheckpoint++
	}

	return writeReq
}

// buildMultiProcessWriteRequest builds the write request for multi-process mode
func (s *Shard) buildMultiProcessWriteRequest(compressedEntries []CompressedEntry, entryNumbers []int64, now int64, allHeaders []byte, initialState struct {
	entryNumber           int64
	writeOffset           int64
	writesSinceCheckpoint int
}, config *CometConfig, clientMetrics *ClientMetrics) WriteRequest {
	var writeReq WriteRequest
	writeReq.IDs = make([]MessageID, len(compressedEntries))
	writeReq.WriteBuffers = make([][]byte, 0, len(compressedEntries)*2)

	// Track binary index updates for deferred application
	type binaryIndexUpdate struct {
		entryNumber int64
		position    EntryPosition
	}
	var binaryIndexUpdates []binaryIndexUpdate

	// Track write position
	writeOffset := s.index.CurrentWriteOffset

	for i, compressedEntry := range compressedEntries {
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

		// Store for later update - only at boundary intervals for memory efficiency
		if entryNumber%int64(s.index.BoundaryInterval) == 0 || entryNumber == 0 {
			binaryIndexUpdates = append(binaryIndexUpdates, binaryIndexUpdate{
				entryNumber: entryNumber,
				position:    position,
			})
		}

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
	if IsDebug() && s.logger != nil {
		s.logger.Debug("Entry number calculation",
			"shard", s.shardID,
			"entryNumbers", entryNumbers,
			"finalEntryNumber", finalEntryNumber,
			"initialEntryNumber", initialState.entryNumber)
	}

	writeReq.UpdateFunc = func() error {
		s.mu.Lock()
		defer s.mu.Unlock()

		// Apply the binary index updates
		for _, update := range binaryIndexUpdates {
			s.index.BinaryIndex.AddIndexNode(update.entryNumber, update.position)
		}

		// Update metric if state is available
		if state := s.state; state != nil {
			atomic.StoreUint64(&state.BinaryIndexNodes, uint64(len(s.index.BinaryIndex.Nodes)))
		}

		// Apply the index updates
		if IsDebug() && s.logger != nil {
			s.logger.Debug("TRACE: Setting CurrentEntryNumber to final in multiprocess",
				"location", "client.go:1530",
				"oldValue", s.index.CurrentEntryNumber,
				"newValue", finalEntryNumber,
				"shardID", s.shardID)
		}
		s.index.CurrentEntryNumber = finalEntryNumber
		s.index.CurrentWriteOffset = finalWriteOffset
		s.writesSinceCheckpoint = initialState.writesSinceCheckpoint + len(compressedEntries)

		// Update the current file's end offset and entry count
		// Since processes own shards exclusively, we can safely update entry counts
		if len(s.index.Files) > 0 {
			s.index.Files[len(s.index.Files)-1].EndOffset = finalWriteOffset
			s.index.Files[len(s.index.Files)-1].Entries += int64(len(compressedEntries))
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

	return writeReq
}

// calculateEntryPosition calculates the file index and byte offset for an entry
func (s *Shard) calculateEntryPosition(entrySize int64) EntryPosition {
	fileIndex := len(s.index.Files) - 1
	byteOffset := s.index.CurrentWriteOffset

	// If this entry will cause rotation and we're not at the start of a file,
	// it will be written to the NEXT file at offset 0
	maxFileSize := int64(1 << 30) // Default 1GB, should come from config
	willCauseRotation := s.index.CurrentWriteOffset+entrySize > maxFileSize
	if willCauseRotation && s.index.CurrentWriteOffset > 0 {
		fileIndex = len(s.index.Files) // Will be the next file after rotation
		byteOffset = 0
	}

	return EntryPosition{
		FileIndex:  fileIndex,
		ByteOffset: byteOffset,
	}
}

// performWrite performs the actual I/O operation
func (s *Shard) performWrite(writeReq WriteRequest, config *CometConfig, clientMetrics *ClientMetrics, activeGroups map[string]bool) error {
	var writeErr error

	// Check if this shard is owned by the current process
	isOwnedByProcess := config.Concurrency.Owns(s.shardID)

	// Use memory-mapped writer for ultra-fast multi-process writes if available
	if config.Concurrency.IsMultiProcess() && s.mmapWriter != nil {
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
			rotErr := s.rotateFile(clientMetrics, config, activeGroups)
			s.mu.Unlock()

			if rotErr != nil {
				return fmt.Errorf("failed to rotate file: %w", rotErr)
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
				return fmt.Errorf("failed to reinitialize mmap writer after recovery: %w", err)
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

			// Add binary index nodes for entries that were written
			for _, entryNum := range entryNumbers {
				if int64(entryNum)%int64(s.index.BoundaryInterval) == 0 || entryNum == 0 {
					// Calculate position for this entry
					// In mmap mode, we don't track exact positions, so use entry number
					position := EntryPosition{
						FileIndex:  0,               // Will be updated during scan
						ByteOffset: int64(entryNum), // Use entry number as placeholder
					}
					s.index.BinaryIndex.AddIndexNode(int64(entryNum), position)
				}
			}

			// Update metric if state is available
			if state := s.state; state != nil {
				atomic.StoreUint64(&state.BinaryIndexNodes, uint64(len(s.index.BinaryIndex.Nodes)))
			}

			s.mu.Unlock()
		}
	} else {
		// Regular write path - no locking needed since processes own their shards

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
			// In multi-process mode, sync to ensure data hits disk before releasing lock (skip for owned shards)
			if writeErr == nil && config.Concurrency.IsMultiProcess() && s.dataFile != nil && !isOwnedByProcess {
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
				return fmt.Errorf("failed to reinitialize writer after recovery: %w", err)
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
				// In multi-process mode, sync to ensure data hits disk before releasing lock (skip for owned shards)
				if writeErr == nil && config.Concurrency.IsMultiProcess() && s.dataFile != nil && !isOwnedByProcess {
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
			if state := s.state; state != nil {
				atomic.AddUint64(&state.ErrorCount, 1)
				atomic.StoreInt64(&state.LastErrorNanos, time.Now().UnixNano())
				// Track failed write batch
				atomic.AddUint64(&state.FailedWrites, 1)
			}

			// Log the error
			if IsDebug() && s.logger != nil {
				s.logger.Debug("Write failed",
					"shard", s.shardID,
					"error", writeErr)
			}

			return fmt.Errorf("failed write: %w", writeErr)
		}
	}

	return nil
}

// trackWriteMetrics tracks metrics after a successful write
func (s *Shard) trackWriteMetrics(startTime time.Time, entries [][]byte, compressedEntries []CompressedEntry, clientMetrics *ClientMetrics) {
	var totalOriginalBytes, totalCompressedBytes uint64
	var compressedCount, skippedCount uint64

	for _, entry := range compressedEntries {
		totalOriginalBytes += entry.OriginalSize
		totalCompressedBytes += entry.CompressedSize
		if entry.WasCompressed {
			compressedCount++
		} else {
			skippedCount++
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
	if state := s.state; state != nil {
		atomic.AddInt64(&state.TotalEntries, int64(len(entries)))
		atomic.AddUint64(&state.TotalBytes, totalOriginalBytes)
		atomic.AddUint64(&state.TotalWrites, 1)
		atomic.StoreInt64(&state.LastWriteNanos, time.Now().UnixNano())
		atomic.AddUint64(&state.TotalCompressed, totalCompressedBytes)
		atomic.AddUint64(&state.CompressedEntries, compressedCount)
		atomic.AddUint64(&state.SkippedCompression, skippedCount)

		// Batch metrics
		atomic.StoreUint64(&state.CurrentBatchSize, uint64(len(entries)))
		atomic.AddUint64(&state.TotalBatches, 1)
	}

	// Update latency metrics using EMA
	if totalOriginalBytes > 0 && totalCompressedBytes > 0 {
		ratio := (totalOriginalBytes * 100) / totalCompressedBytes // x100 for fixed point
		clientMetrics.CompressionRatio.Store(ratio)
		// Also update in shard state
		if state := s.state; state != nil {
			atomic.StoreUint64(&state.CompressionRatio, ratio)
		}
	}

	// Track write latency with min/max
	clientMetrics.WriteLatencyNano.Store(writeLatency)

	// Update unified state latency metrics (if available)
	if state := s.state; state != nil {
		state.UpdateWriteLatency(writeLatency)
	}
	// Since processes own their shards exclusively, no CAS needed for client metrics
	currentMin := clientMetrics.MinWriteLatency.Load()
	if currentMin == 0 || writeLatency < currentMin {
		clientMetrics.MinWriteLatency.Store(writeLatency)
	}

	currentMax := clientMetrics.MaxWriteLatency.Load()
	if writeLatency > currentMax {
		clientMetrics.MaxWriteLatency.Store(writeLatency)
	}
}

// performPostWriteOperations handles post-write tasks like checkpointing and rotation
func (s *Shard) performPostWriteOperations(config *CometConfig, clientMetrics *ClientMetrics, activeGroups map[string]bool, entryCount int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Update file entry count after successful write
	// In multi-process mode, this is already done in UpdateFunc
	if !config.Concurrency.IsMultiProcess() && len(s.index.Files) > 0 {
		currentFile := &s.index.Files[len(s.index.Files)-1]
		currentFile.Entries += int64(entryCount)
	}

	// In multi-process mode, update unified state immediately for visibility
	if config.Concurrency.IsMultiProcess() {
		if state := s.state; state != nil {
			// Fast path: Update coordination state immediately (~10ns total)
			state.SetLastIndexUpdate(time.Now().UnixNano())
		}
		// LastEntryNumber should be the last allocated entry, not the next one
		// CurrentEntryNumber is the next entry to be written
		if s.index.CurrentEntryNumber > 0 {
			// Sync CometState with current index if needed
			if state := s.state; state != nil {
				currentLastEntry := state.GetLastEntryNumber()
				expectedLastEntry := s.index.CurrentEntryNumber - 1
				if currentLastEntry < expectedLastEntry {
					// Catch up to the expected value
					for state.GetLastEntryNumber() < expectedLastEntry {
						state.IncrementLastEntryNumber()
					}
				}
			}
		}

		// Note: Async checkpointing happens in UpdateFunc now
	} else {
		// Single-process mode: use regular checkpointing
		s.maybeCheckpoint(clientMetrics, config, activeGroups)
	}

	// Check if we need to rotate file
	if s.index.CurrentWriteOffset > config.Storage.MaxFileSize {
		if err := s.rotateFile(clientMetrics, config, activeGroups); err != nil {
			return fmt.Errorf("failed to rotate file: %w", err)
		}
	}

	return nil
}

// maybeCheckpoint persists the index if needed
func (s *Shard) maybeCheckpoint(clientMetrics *ClientMetrics, config *CometConfig, activeGroups map[string]bool) {
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

			// In single-process mode, sync CurrentWriteOffset with actual file size
			// This ensures EndOffset reflects the true file size after writes
			if !config.Concurrency.IsMultiProcess() && s.dataFile != nil {
				if stat, err := s.dataFile.Stat(); err == nil {
					actualSize := stat.Size()
					if actualSize > s.index.CurrentWriteOffset {
						s.index.CurrentWriteOffset = actualSize
					}
				}
			}

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
			if config.Concurrency.IsMultiProcess() && false {
				// Acquire exclusive lock for index writes
				if false { // Lock removed - processes own shards
					if clientMetrics != nil {
						clientMetrics.IndexPersistErrors.Add(1)
						clientMetrics.ErrorCount.Add(1)
						clientMetrics.LastErrorNano.Store(uint64(time.Now().UnixNano()))
					}

					// Track in CometState if available
					if state := s.state; state != nil {
						atomic.AddUint64(&state.IndexPersistErrors, 1)
						atomic.AddUint64(&state.ErrorCount, 1)
						atomic.StoreInt64(&state.LastErrorNanos, time.Now().UnixNano())
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
							if IsDebug() && s.logger != nil {
								s.logger.Debug("TRACE: Setting CurrentEntryNumber from diskIndex (2)",
									"location", "client.go:1927",
									"oldValue", indexCopy.CurrentEntryNumber,
									"newValue", diskIndex.CurrentEntryNumber,
									"shardID", s.shardID)
							}
							indexCopy.CurrentEntryNumber = diskIndex.CurrentEntryNumber
						}
						if diskIndex.CurrentWriteOffset > indexCopy.CurrentWriteOffset {
							indexCopy.CurrentWriteOffset = diskIndex.CurrentWriteOffset
						}

						// Merge consumer offsets - only for groups actively used by this client
						for group, offset := range diskIndex.ConsumerOffsets {
							// Only merge offsets for consumer groups actively used by this client
							if activeGroups != nil && activeGroups[group] {
								if currentOffset, exists := indexCopy.ConsumerOffsets[group]; !exists || offset > currentOffset {
									indexCopy.ConsumerOffsets[group] = offset
								}
							}
						}

						// Merge file info - combine files from disk and memory
						// Create a map of existing files from disk
						fileMap := make(map[string]FileInfo)
						for _, f := range diskIndex.Files {
							fileMap[f.Path] = f
						}

						// Add or update files from our in-memory copy
						for _, f := range indexCopy.Files {
							// Use our version which should have more up-to-date info
							fileMap[f.Path] = f
						}

						// Convert back to slice, sorted by path
						indexCopy.Files = make([]FileInfo, 0, len(fileMap))
						for _, f := range fileMap {
							indexCopy.Files = append(indexCopy.Files, f)
						}

						// Sort files by path for consistency
						sort.Slice(indexCopy.Files, func(i, j int) bool {
							return indexCopy.Files[i].Path < indexCopy.Files[j].Path
						})

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
				// Lock removed - processes own shards
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
		if state := s.state; state != nil {
			atomic.AddUint64(&state.CheckpointCount, 1)
			atomic.StoreInt64(&state.LastCheckpointNanos, time.Now().UnixNano())
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

		// If index loading failed due to corruption, attempt rebuild
		if strings.Contains(err.Error(), "index file too small") ||
			strings.Contains(err.Error(), "unexpected EOF") ||
			strings.Contains(err.Error(), "invalid header") {

			// Get shard directory for rebuild
			shardDir := filepath.Dir(s.indexPath)

			if s.logger != nil {
				s.logger.Warn("Index corrupted, attempting rebuild",
					"error", err, "shardDir", shardDir)
			}

			// Attempt to rebuild from data files
			if rebuildErr := s.rebuildIndexFromDataFiles(shardDir); rebuildErr != nil {
				if s.logger != nil {
					s.logger.Error("Index rebuild failed", "rebuildError", rebuildErr, "originalError", err)
				}
				return fmt.Errorf("index corrupt and rebuild failed: original=%w, rebuild=%w", err, rebuildErr)
			}

			if s.logger != nil {
				s.logger.Info("Successfully rebuilt index after corruption")
			}
			return nil // Rebuild succeeded
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
	mmapWriter, err := NewMmapWriter(shardDir, config.Storage.MaxFileSize, s.index, s.state, nil)
	if err != nil {
		return fmt.Errorf("failed to create mmap writer: %w", err)
	}

	s.mmapWriter = mmapWriter
	return nil
}

// loadIndexWithRetry loads the index with retry logic for EOF errors
// This handles the race condition between file writes and mmap state updates
// loadIndexWithRetry attempts to load the index with retries for EOF errors.
// IMPORTANT: This function calls loadIndex() and must be called with s.mu held.
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
		if config.Concurrency.IsMultiProcess() && i == len(s.index.Files)-1 {
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
						// In stress test scenarios, reduce wait time
						maxAttempts := 10
						waitTime := 50 * time.Millisecond

						// If we're in a test environment, reduce wait times
						if os.Getenv("CI") != "" || os.Getenv("TEST_FAST_MODE") != "" {
							maxAttempts = 3
							waitTime = 10 * time.Millisecond
						}

						// Wait for a brief period to let any pending writes complete
						stableTotalWrites := latestTotalWrites
						stableWriteOffset := writeOffset

						for attempts := 0; attempts < maxAttempts; attempts++ {
							time.Sleep(waitTime)
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

	// Remove files with 0 entries from the index - they shouldn't be tracked
	// This prevents consumer offset adjustment logic from seeing fake files
	validFiles := make([]FileInfo, 0, len(s.index.Files))
	for _, file := range s.index.Files {
		if file.Entries > 0 {
			validFiles = append(validFiles, file)
		}
	}
	s.index.Files = validFiles

	// Recalculate StartEntry values for remaining files to ensure they're correct
	// This is critical for consumer offset logic to work properly
	runningEntryCount := int64(0)
	for i := range s.index.Files {
		s.index.Files[i].StartEntry = runningEntryCount
		runningEntryCount += s.index.Files[i].Entries
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
		if IsDebug() && s.logger != nil {
			s.logger.Debug("TRACE: Setting CurrentWriteOffset in rebuildIndexFromDataFiles",
				"shardID", s.shardID,
				"oldWriteOffset", s.index.CurrentWriteOffset,
				"newWriteOffset", lastFile.EndOffset,
				"lastFilePath", lastFile.Path)
		}
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

		// Check for uninitialized memory (zeros) - LIMITED GAP SKIPPING
		if length == 0 && timestamp == 0 {

			found := false

			// BOUNDED APPROACH: Search up to 64KB for next valid header
			// This balances recovery capability with performance
			maxSearchDistance := int64(64 * 1024) // 64KB limit
			searchLimit := offset + maxSearchDistance
			if searchLimit > fileSize-headerSize {
				searchLimit = fileSize - headerSize
			}

			for searchOffset := offset + 4; searchOffset <= searchLimit; searchOffset += 4 {
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
			if state := s.state; state != nil {
				atomic.AddUint64(&state.IndexPersistErrors, 1)
				atomic.AddUint64(&state.ErrorCount, 1)
				atomic.StoreInt64(&state.LastErrorNanos, time.Now().UnixNano())
			}
		} else {
			// Track checkpoint metrics
			if clientMetrics != nil {
				clientMetrics.CheckpointCount.Add(1)
				clientMetrics.LastCheckpoint.Store(uint64(time.Now().UnixNano()))
			}

			// Track in CometState if available
			if state := s.state; state != nil {
				atomic.AddUint64(&state.CheckpointCount, 1)
				atomic.StoreInt64(&state.LastCheckpointNanos, time.Now().UnixNano())
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
	// Since processes own their shards exclusively, we don't need complex merging
	// Just clone and save the index
	indexCopy := s.cloneIndex()

	// Create a copy to avoid concurrent modifications
	if IsDebug() && s.logger != nil {
		s.logger.Debug("persistIndex called",
			"shardID", s.shardID,
			"originalNodes", len(s.index.BinaryIndex.Nodes),
			"copyNodes", len(indexCopy.BinaryIndex.Nodes),
			"consumerGroups", len(indexCopy.ConsumerOffsets))
	}

	// Calculate total file bytes while we have the index
	if state := s.state; state != nil {
		var totalBytes uint64
		for _, file := range s.index.Files {
			totalBytes += uint64(file.EndOffset - file.StartOffset)
		}
		atomic.StoreUint64(&state.TotalFileBytes, totalBytes)
		// Also update current files count
		atomic.StoreUint64(&state.CurrentFiles, uint64(len(s.index.Files)))
	}

	// Serialize index writes to prevent file corruption
	s.indexMu.Lock()
	err := s.saveBinaryIndex(indexCopy)
	s.indexMu.Unlock()

	if err == nil {
		// Only update mmap state after successful persistence
		s.updateMmapState()

		// Track successful index persistence
		if state := s.state; state != nil {
			atomic.AddUint64(&state.IndexPersistCount, 1)
			atomic.StoreInt64(&state.LastIndexUpdate, time.Now().UnixNano())
			// Update binary index node count
			nodeCount := uint64(len(indexCopy.BinaryIndex.Nodes))
			atomic.StoreUint64(&state.BinaryIndexNodes, nodeCount)
			if IsDebug() && s.logger != nil {
				s.logger.Debug("persistIndex updated BinaryIndexNodes metric",
					"shardID", s.shardID,
					"nodeCount", nodeCount)
			}
		} else {
			if IsDebug() && s.logger != nil {
				s.logger.Debug("persistIndex loadState() returned nil", "shardID", s.shardID)
			}
		}
	} else if state := s.state; state != nil {
		// Track failed index persistence
		atomic.AddUint64(&state.IndexPersistErrors, 1)
	}
	return err
}

// discoverDataFiles scans the shard directory for all data files
// This is critical for multi-process mode where other processes may have created files
// loadIndex loads the index from disk
// loadIndex loads the shard index from disk.
// IMPORTANT: This function modifies s.index and must be called with s.mu held.
func (s *Shard) loadIndex() error {
	// Since processes own shards exclusively, no need to discover files or acquire locks
	// The process that owns this shard is the only one writing to it

	// Check if index file exists
	if _, err := os.Stat(s.indexPath); os.IsNotExist(err) {
		// No index file - attempt to rebuild from data files
		if s.index == nil {
			// Create default index structure with properly initialized fields
			// Note: Assignment to s.index requires caller to hold s.mu
			s.index = &ShardIndex{
				CurrentEntryNumber: 0, // Explicitly initialize to prevent garbage values
				CurrentWriteOffset: 0, // Explicitly initialize to prevent garbage values
				BoundaryInterval:   defaultBoundaryInterval,
				ConsumerOffsets:    make(map[string]int64),
				Files:              make([]FileInfo, 0),
				BinaryIndex: BinarySearchableIndex{
					IndexInterval: defaultBoundaryInterval,
					MaxNodes:      1000,
					Nodes:         make([]EntryIndexNode, 0),
				},
			}
		}

		// Try to rebuild index from existing data files only if we don't have files already
		// Since processes own their shards exclusively, if mmap writer added files, keep them
		if len(s.index.Files) == 0 {
			shardDir := filepath.Dir(s.indexPath)
			if s.logger != nil {
				s.logger.Info("loadIndex: No index file exists, attempting rebuild",
					"shardDir", shardDir,
					"indexPath", s.indexPath)
			}
			// Track recovery attempt
			if state := s.state; state != nil {
				atomic.AddUint64(&state.RecoveryAttempts, 1)
			}
			if err := s.rebuildIndexFromDataFiles(shardDir); err != nil {
				// Log the error but don't fail - empty index is better than nothing
				// This allows new shards to start fresh
				if len(s.index.Files) > 0 {
					// Only return error if we found files but couldn't read them
					return fmt.Errorf("failed to rebuild index from data files: %w", err)
				}
			}
		}

		// Since each process owns its shards exclusively, we can simply persist without complex locking
		persistErr := s.persistIndex()

		if persistErr != nil {
			return fmt.Errorf("failed to persist index: %w", persistErr)
		}

		// Track recovery success if we successfully rebuilt from files
		if len(s.index.Files) > 0 {
			if state := s.state; state != nil {
				atomic.AddUint64(&state.RecoverySuccesses, 1)
			}
		}

		return nil
	}

	// Save current config values before loading
	boundaryInterval := s.index.BoundaryInterval
	maxIndexEntries := s.index.BinaryIndex.MaxNodes

	// Load binary index - simplified since only one process owns this shard
	index, err := s.loadBinaryIndexWithConfig(boundaryInterval, maxIndexEntries)
	if err != nil {
		// Check if this is the 341 corruption pattern
		if strings.Contains(err.Error(), "corrupted index detected") {
			if s.logger != nil {
				s.logger.Warn("Index corruption detected, rebuilding from data files",
					"shardID", s.shardID,
					"indexPath", s.indexPath,
					"error", err)
			}

			// Backup corrupted index for investigation
			corruptedPath := s.indexPath + fmt.Sprintf(".corrupted.%d", time.Now().Unix())
			os.Rename(s.indexPath, corruptedPath)

			// Rebuild index from data files
			shardDir := filepath.Dir(s.indexPath)
			if rebuildErr := s.rebuildIndexFromDataFiles(shardDir); rebuildErr != nil {
				return fmt.Errorf("failed to rebuild corrupted index: %w", rebuildErr)
			}

			// Persist the rebuilt index
			if persistErr := s.persistIndex(); persistErr != nil {
				return fmt.Errorf("failed to persist rebuilt index: %w", persistErr)
			}

			// Track corruption recovery
			if state := s.state; state != nil {
				atomic.AddUint64(&state.CorruptionDetected, 1)
				atomic.AddUint64(&state.RecoverySuccesses, 1)
			}

			if s.logger != nil {
				s.logger.Debug("Successfully rebuilt corrupted index",
					"shardID", s.shardID,
					"indexPath", s.indexPath,
					"backupPath", corruptedPath,
					"newEntryCount", s.index.CurrentEntryNumber)
			}

			return nil
		}
		return fmt.Errorf("failed to load binary index: %w", err)
	}

	if IsDebug() && s.logger != nil {
		s.logger.Debug("loadIndex: about to assign loaded index",
			"shardID", s.shardID,
			"beforeAssign", s.index.CurrentEntryNumber,
			"loadedIndex", index.CurrentEntryNumber)
	}

	// Since processes own their shards exclusively, they own ALL consumer groups
	// No need to filter - just load all consumer offsets

	// Update index fields in place to preserve mmap writer's reference
	// This is safe since processes own their shards exclusively
	if s.index == nil {
		s.index = index
	} else {
		// Update fields in place to preserve the pointer that mmap writer has
		s.index.CurrentEntryNumber = index.CurrentEntryNumber
		s.index.CurrentWriteOffset = index.CurrentWriteOffset
		s.index.CurrentFile = index.CurrentFile
		s.index.BoundaryInterval = index.BoundaryInterval

		// Update consumer offsets
		s.index.ConsumerOffsets = index.ConsumerOffsets

		// Update binary index
		s.index.BinaryIndex = index.BinaryIndex

		// Update files array - but preserve any files added by mmap writer if index has none
		if len(index.Files) > 0 || len(s.index.Files) == 0 {
			// Only update if loaded index has files, or if we have no files
			s.index.Files = index.Files
		}
		// Otherwise keep existing files that mmap writer may have added
	}

	// Since processes own their shards exclusively, we trust what's on disk
	// No complex merging needed!

	if IsDebug() && s.logger != nil {
		s.logger.Debug("Index loaded successfully",
			"shardID", s.shardID,
			"nodes", len(s.index.BinaryIndex.Nodes),
			"files", len(s.index.Files),
			"currentEntryNumber", s.index.CurrentEntryNumber)
	}
	// Restore config values
	s.index.BoundaryInterval = boundaryInterval
	s.index.BinaryIndex.IndexInterval = boundaryInterval
	s.index.BinaryIndex.MaxNodes = maxIndexEntries

	// In multi-process mode, always trust state over index for CurrentEntryNumber
	if s.mmapWriter != nil && s.state != nil {
		state := s.state
		stateLastEntry := atomic.LoadInt64(&state.LastEntryNumber)
		if stateLastEntry >= 0 {
			expectedCurrentEntry := stateLastEntry + 1
			if s.index.CurrentEntryNumber != expectedCurrentEntry {
				if IsDebug() && s.logger != nil {
					s.logger.Debug("loadIndex: correcting CurrentEntryNumber with state",
						"shardID", s.shardID,
						"indexCurrentEntry", s.index.CurrentEntryNumber,
						"stateLastEntry", stateLastEntry,
						"correctedCurrentEntry", expectedCurrentEntry)
				}
				s.index.CurrentEntryNumber = expectedCurrentEntry
			}
		}
	}

	// In multi-process mode, verify and update file sizes
	if s.mmapWriter != nil {
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

	// Update state metrics after loading index
	if state := s.state; state != nil {
		// Calculate total file bytes from loaded index
		var totalBytes uint64
		for _, file := range s.index.Files {
			totalBytes += uint64(file.EndOffset - file.StartOffset)
		}
		atomic.StoreUint64(&state.TotalFileBytes, totalBytes)
		atomic.StoreUint64(&state.CurrentFiles, uint64(len(s.index.Files)))
	}

	// IMPORTANT: If we have an mmap writer, update its index pointer since we replaced the index
	// This ensures the mmap writer sees the same index as the shard
	if s.mmapWriter != nil {
		s.mmapWriter.mu.Lock()
		s.mmapWriter.index = s.index
		s.mmapWriter.mu.Unlock()
	}

	return nil
}

// NOTE: initMmapState and initSequenceState removed - replaced by initCometState

// Global sequence counter for single-process mode
var globalSequenceCounter int64

// getNextSequence atomically increments and returns the next sequence number for file naming
func (s *Shard) getNextSequence() int64 {
	if state := s.state; state != nil {
		// Multi-process mode: use unified state's file sequence
		return int64(state.AddLastFileSequence(1))
	}
	// Single-process mode: use global atomic counter
	return atomic.AddInt64(&globalSequenceCounter, 1)
}

// rebuildIndexFromDataFiles scans data files to rebuild the index
// Since processes own shards exclusively, this is only used for disaster recovery
func (s *Shard) rebuildIndexFromDataFiles(shardDir string) error {
	if s.logger != nil {
		s.logger.Info("rebuildIndexFromDataFiles: Starting rebuild", "shardDir", shardDir)
	}
	// Find all data files
	entries, err := os.ReadDir(shardDir)
	if err != nil {
		if s.logger != nil {
			s.logger.Error("rebuildIndexFromDataFiles: Failed to read directory",
				"shardDir", shardDir,
				"error", err)
		}
		return fmt.Errorf("failed to read shard directory: %w", err)
	}

	if s.logger != nil {
		s.logger.Info("rebuildIndexFromDataFiles: Read directory",
			"shardDir", shardDir,
			"entryCount", len(entries))
	}

	var dataFiles []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), "log-") && strings.HasSuffix(entry.Name(), ".comet") {
			dataFiles = append(dataFiles, filepath.Join(shardDir, entry.Name()))
		}
	}

	if len(dataFiles) == 0 {
		return nil
	}

	sort.Strings(dataFiles)

	if s.logger != nil {
		s.logger.Debug("rebuildIndexFromDataFiles: found data files",
			"shardDir", shardDir,
			"fileCount", len(dataFiles),
			"files", dataFiles)
	}

	// Reset index
	s.index.Files = make([]FileInfo, 0, len(dataFiles))
	s.index.BinaryIndex.Nodes = make([]EntryIndexNode, 0)
	s.index.CurrentEntryNumber = 0
	s.index.CurrentWriteOffset = 0
	s.index.ConsumerOffsets = make(map[string]int64)

	// Scan files
	currentEntryNum := int64(0)
	for i, filePath := range dataFiles {
		if s.logger != nil {
			s.logger.Debug("rebuildIndexFromDataFiles: scanning file",
				"fileIndex", i,
				"filePath", filePath,
				"currentEntryNum", currentEntryNum)
		}

		fileInfo, err := s.scanDataFileForRebuild(filePath, currentEntryNum)
		if err != nil {
			if s.logger != nil {
				s.logger.Warn("rebuildIndexFromDataFiles: failed to scan file",
					"filePath", filePath,
					"error", err)
			}
			continue // Skip corrupted files
		}

		if s.logger != nil {
			s.logger.Debug("rebuildIndexFromDataFiles: scanned file",
				"filePath", filePath,
				"entries", fileInfo.Entries,
				"startEntry", fileInfo.StartEntry,
				"endOffset", fileInfo.EndOffset)
		}

		// Always add files to index, even if they appear empty
		// They might have entries we couldn't scan due to corruption
		s.index.Files = append(s.index.Files, *fileInfo)
		if fileInfo.Entries > 0 {
			currentEntryNum = fileInfo.StartEntry + fileInfo.Entries
		}

		// Last file becomes current
		if filePath == dataFiles[len(dataFiles)-1] {
			s.index.CurrentFile = filePath
			s.index.CurrentWriteOffset = fileInfo.EndOffset
		}
	}

	// Set CurrentEntryNumber based on what we found
	s.index.CurrentEntryNumber = currentEntryNum

	// If state exists and has a higher count, trust it (could have entries not yet synced to disk)
	if state := s.state; state != nil {
		lastEntryFromState := atomic.LoadInt64(&state.LastEntryNumber)
		if s.logger != nil {
			s.logger.Debug("rebuildIndexFromDataFiles: After scanning files",
				"filesScanned", len(s.index.Files),
				"currentEntryNum", currentEntryNum,
				"lastEntryFromState", lastEntryFromState)
		}
		// Only use state if it's higher than what we found (state might have pending entries)
		if lastEntryFromState >= 0 && lastEntryFromState+1 > currentEntryNum {
			if s.logger != nil {
				s.logger.Debug("rebuildIndexFromDataFiles: using state for CurrentEntryNumber",
					"shardID", s.shardID,
					"stateLastEntry", lastEntryFromState)
			}
			s.index.CurrentEntryNumber = lastEntryFromState + 1
		}
	}

	// Track recovery success
	if state := s.state; state != nil {
		atomic.AddUint64(&state.RecoverySuccesses, 1)
	}

	// CRITICAL: Persist the rebuilt and sorted index to disk
	// This ensures subsequent loadIndex() calls get the correct sorted order
	if err := s.persistIndex(); err != nil {
		if s.logger != nil {
			s.logger.Warn("Failed to persist rebuilt index", "error", err)
		}
		// Don't fail the rebuild, but log the issue
	}

	return nil
}

// scanDataFileForRebuild is like scanDataFile but takes startEntry as parameter to avoid using corrupted index state
func (s *Shard) scanDataFileForRebuild(filePath string, startEntry int64) (*FileInfo, error) {

	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open data file: %w", err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	if s.logger != nil {
		s.logger.Debug("REBUILD_SCAN: Starting file scan",
			"filePath", filePath,
			"fileSize", stat.Size(),
			"startEntry", startEntry)
	}

	fileInfo := &FileInfo{
		Path:        filePath,
		StartOffset: 0,
		EndOffset:   stat.Size(),
		StartEntry:  startEntry, // Use provided startEntry, not corrupted index data
		StartTime:   stat.ModTime(),
		EndTime:     stat.ModTime(),
	}

	// Scan through the file to count entries and build index nodes
	offset := int64(0)
	entryCount := int64(0)
	buffer := make([]byte, 12) // Header size

	for offset < stat.Size() {
		// Read header
		n, err := file.ReadAt(buffer, offset)
		if err != nil || n < 12 {
			if s.logger != nil {
				s.logger.Debug("REBUILD_SCAN: EOF or corrupted header",
					"offset", offset,
					"fileSize", stat.Size(),
					"bytesRead", n,
					"error", err)
			}
			break // End of file or corrupted
		}

		// Parse header
		length := binary.LittleEndian.Uint32(buffer[0:4])
		timestamp := binary.LittleEndian.Uint64(buffer[4:12])

		// Check for uninitialized memory (all zeros) - this indicates end of valid data
		allZeros := true
		for _, b := range buffer {
			if b != 0 {
				allZeros = false
				break
			}
		}
		if allZeros {
			break // Stop scanning when we hit uninitialized memory
		}

		// Validate entry
		if length > 100*1024*1024 { // 100MB max
			if s.logger != nil {
				s.logger.Error("REBUILD_SCAN: Corrupted entry detected",
					"entryNumber", entryCount,
					"offset", offset,
					"length", length,
					"headerBytes", fmt.Sprintf("%x", buffer))
			}
			// Track corruption detection
			if state := s.state; state != nil {
				atomic.AddUint64(&state.CorruptionDetected, 1)
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
			s.index.BinaryIndex.AddIndexNode(startEntry+entryCount, EntryPosition{
				FileIndex:  len(s.index.Files), // Current file index
				ByteOffset: offset,
			})

			// Update metric if state is available
			if state := s.state; state != nil {
				atomic.StoreUint64(&state.BinaryIndexNodes, uint64(len(s.index.BinaryIndex.Nodes)))
			}
		}

		// Move to next entry
		entrySize := int64(12 + length)
		offset += entrySize
		entryCount++

		if IsDebug() && s.logger != nil {
			s.logger.Debug("REBUILD_SCAN: Found entry",
				"entryNumber", entryCount-1,
				"offset", offset-entrySize,
				"length", length,
				"timestamp", timestamp)
		}

		// Safety check - if we're reading way too many entries, something is wrong
		if entryCount > 1000 {
			if s.logger != nil {
				s.logger.Error("REBUILD_SCAN: Too many entries, stopping scan",
					"entryCount", entryCount,
					"fileSize", stat.Size(),
					"currentOffset", offset)
			}
			break
		}
	}

	fileInfo.Entries = entryCount
	fileInfo.EndOffset = offset

	return fileInfo, nil
}

// updateMmapState updates the timestamp (kept for metrics/debugging)
func (s *Shard) updateMmapState() {
	// Since processes own their shards exclusively, this is just for metrics
	if state := s.state; state != nil {
		state.SetLastIndexUpdate(time.Now().UnixNano())
	}
}

// openDataFileWithConfig opens the current data file for appending with optional config for multi-process safety
func (s *Shard) openDataFileWithConfig(shardDir string, config *CometConfig) error {
	// No locking needed since processes own their shards exclusively

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

		if IsDebug() && s.logger != nil {
			s.logger.Debug("Created first data file",
				"shard", s.shardID,
				"filePath", s.index.CurrentFile,
				"fileCount", len(s.index.Files))
		}
	}

	// Open file for appending
	file, err := os.OpenFile(s.index.CurrentFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open data file: %w", err)
	}

	s.dataFile = file

	// Track file creation if this is a new file
	if state := s.state; state != nil {
		// Check if file was newly created
		fileInfo, err := file.Stat()
		if err == nil && fileInfo.Size() == 0 {
			// New file created
			atomic.AddUint64(&state.FilesCreated, 1)
		}
		// Update current file count
		atomic.StoreUint64(&state.CurrentFiles, uint64(len(s.index.Files)))
	}

	// Create buffered writer
	// In multi-process mode, use smaller buffer for more frequent flushes
	bufSize := defaultBufSize
	if s.mmapWriter != nil { // Multi-process mode
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

	// Check if index and state are in sync AND file size matches write offset
	// If the file is larger than the write offset, there may be unindexed entries
	if state := s.state; state != nil {
		stateLastEntry := atomic.LoadInt64(&state.LastEntryNumber)
		expectedCurrentEntry := stateLastEntry + 1
		if s.index.CurrentEntryNumber == expectedCurrentEntry && actualSize <= s.index.CurrentWriteOffset {
			// Index and state are in sync and file size matches, no recovery needed
			if IsDebug() && s.logger != nil {
				s.logger.Debug("TRACE: recoverFromCrash - index and state in sync, no recovery needed",
					"shardID", s.shardID,
					"stateLastEntry", stateLastEntry,
					"indexCurrentEntry", s.index.CurrentEntryNumber,
					"actualSize", actualSize,
					"oldWriteOffset", s.index.CurrentWriteOffset)
			}
			s.index.CurrentWriteOffset = actualSize
			return nil
		}
	}

	if IsDebug() && s.logger != nil {
		s.logger.Debug("TRACE: recoverFromCrash values",
			"shardID", s.shardID,
			"actualSize", actualSize,
			"indexCurrentWriteOffset", s.index.CurrentWriteOffset,
			"needsRecovery", actualSize > s.index.CurrentWriteOffset)
	}

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

	if IsDebug() && s.logger != nil {
		s.logger.Debug("TRACE: recoverFromCrash found entries",
			"shardID", s.shardID,
			"validEntries", validEntries,
			"oldCurrentEntryNumber", s.index.CurrentEntryNumber,
			"newCurrentEntryNumber", s.index.CurrentEntryNumber+validEntries)
	}

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

		// Update metric if state is available
		if state := s.state; state != nil {
			atomic.StoreUint64(&state.BinaryIndexNodes, uint64(len(s.index.BinaryIndex.Nodes)))
		}
	}

	// Track partial write if detected
	if partialWriteDetected {
		if state := s.state; state != nil {
			atomic.AddUint64(&state.PartialWrites, 1)
		}
	}

	return nil
}

// rotateFile closes current file and starts a new one
// NOTE: This method assumes the caller holds s.mu (main shard mutex)
func (s *Shard) rotateFile(clientMetrics *ClientMetrics, config *CometConfig, activeGroups map[string]bool) error {
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
		// Use the actual path that mmap writer created
		newPath := s.mmapWriter.dataPath
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
		if state := s.state; state != nil {
			atomic.AddUint64(&state.FileRotations, 1)
		}

		// Debug log file rotation
		if IsDebug() && s.logger != nil {
			var oldFile string
			if len(s.index.Files) > 1 {
				oldFile = s.index.Files[len(s.index.Files)-2].Path
			}
			s.logger.Debug("File rotated",
				"oldFile", filepath.Base(oldFile),
				"newFile", filepath.Base(newPath),
				"totalFiles", len(s.index.Files))
		}

		// Force a checkpoint after rotation to ensure index is persisted
		// This is critical in multi-process mode so other processes see the new files
		s.lastCheckpoint = time.Time{} // Reset to force checkpoint
		s.maybeCheckpoint(clientMetrics, config, activeGroups)

		// Update the metrics that would have been updated by persistIndex
		if state := s.state; state != nil {
			// Update file count metric
			atomic.StoreUint64(&state.CurrentFiles, uint64(len(s.index.Files)))

			// Calculate and update total file size
			var totalBytes uint64
			for _, file := range s.index.Files {
				totalBytes += uint64(file.EndOffset - file.StartOffset)
			}
			atomic.StoreUint64(&state.TotalFileBytes, totalBytes)
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

	// For multi-process safety, acquire exclusive lock when creating new file (skip for owned shards)
	// Since processes own their shards exclusively, no locking needed for rotation

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
	if state := s.state; state != nil {
		atomic.AddUint64(&state.FileRotations, 1)
	}

	// Ensure the shard directory exists before trying to create the new file
	if err := os.MkdirAll(shardDir, 0755); err != nil {
		return fmt.Errorf("failed to create shard directory during rotation: %w", err)
	}

	// Open with preallocation and setup
	if err := s.openDataFileWithConfig(shardDir, config); err != nil {
		return err
	}

	// In multi-process mode, we MUST persist the index after rotation
	// to ensure other processes can see the new file structure
	// In single-process mode, skip persist for performance
	// It will be persisted during checkpoints

	// Update the metrics that would have been updated by persistIndex
	if state := s.state; state != nil {
		// Update file count metric
		atomic.StoreUint64(&state.CurrentFiles, uint64(len(s.index.Files)))

		// Calculate and update total file size
		var totalBytes uint64
		for _, file := range s.index.Files {
			totalBytes += uint64(file.EndOffset - file.StartOffset)
		}
		atomic.StoreUint64(&state.TotalFileBytes, totalBytes)
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
		// Force checkpoint on close in multi-process mode to ensure index is persisted
		if c.config.Concurrency.IsMultiProcess() && shard.mmapWriter != nil {
			// Always persist index on close to ensure files are visible to next process
			if err := shard.persistIndex(); err != nil {
				if c.logger != nil {
					c.logger.Warn("Failed to persist index on close", "shard", shard.shardID, "error", err)
				}
			}
		} else {
			shard.maybeCheckpoint(nil, &c.config, c.getActiveGroups())
		}

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

		// Lock files removed - processes own their shards exclusively

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
		if state := shard.state; state != nil {
			shardLastWrite := atomic.LoadInt64(&state.LastWriteNanos)
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

// ListRecent returns the most recent N messages from a stream without affecting consumer state
// It reads directly from the underlying files, bypassing consumer groups entirely
func (c *Client) ListRecent(ctx context.Context, streamName string, limit int) ([]StreamMessage, error) {
	if limit <= 0 {
		return nil, nil
	}

	shardID, err := parseShardFromStream(streamName)
	if err != nil {
		return nil, fmt.Errorf("invalid stream name: %w", err)
	}

	shard, err := c.getOrCreateShard(shardID)
	if err != nil {
		return nil, err
	}

	// In multi-process mode, reload index to get latest state
	if c.config.Concurrency.IsMultiProcess() {
		shard.mu.Lock()
		if err := shard.loadIndex(); err != nil {
			shard.mu.Unlock()
			return nil, fmt.Errorf("failed to reload index: %w", err)
		}
		// Debug log after loading
		if c.logger != nil {
			c.logger.WithFields(
				"currentEntryNumber", shard.index.CurrentEntryNumber,
				"numFiles", len(shard.index.Files),
				"currentWriteOffset", shard.index.CurrentWriteOffset,
			).Debug("ListRecent: Index loaded")
		}
		shard.mu.Unlock()
	}

	// Create a reader for direct access
	shard.mu.RLock()
	if shard.index == nil {
		shard.mu.RUnlock()
		return nil, fmt.Errorf("shard index not initialized")
	}
	indexCopy := shard.cloneIndex() // Create proper deep copy to avoid race conditions
	totalEntries := shard.index.CurrentEntryNumber
	shard.mu.RUnlock()

	if totalEntries == 0 {
		return nil, nil
	}

	// Debug log in multi-process mode
	if c.config.Concurrency.IsMultiProcess() && c.logger != nil {
		c.logger.WithFields("totalEntries", totalEntries, "limit", limit).Debug("ListRecent scanning entries")
	}

	reader, err := NewReader(shardID, indexCopy, c.config.Reader)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	// Calculate starting position
	startFrom := totalEntries - int64(limit)
	if startFrom < 0 {
		startFrom = 0
	}

	var messages []StreamMessage
	for i := startFrom; i < totalEntries && len(messages) < limit; i++ {
		select {
		case <-ctx.Done():
			return messages, ctx.Err()
		default:
		}

		entry, err := reader.ReadEntryByNumber(i)
		if err != nil {
			continue // Skip bad entries
		}

		messages = append(messages, StreamMessage{
			Stream: streamName,
			ID:     MessageID{ShardID: shardID, EntryNumber: i},
			Data:   entry,
		})
	}

	return messages, nil
}

// ScanAll scans all entries in a stream, calling fn for each message
// Return false from fn to stop scanning early
// This operation bypasses consumer groups and doesn't affect offsets
func (c *Client) ScanAll(ctx context.Context, streamName string, fn func(context.Context, StreamMessage) bool) error {
	shardID, err := parseShardFromStream(streamName)
	if err != nil {
		return fmt.Errorf("invalid stream name: %w", err)
	}

	shard, err := c.getOrCreateShard(shardID)
	if err != nil {
		return err
	}

	// In multi-process mode, reload index to get latest state
	if c.config.Concurrency.IsMultiProcess() {
		shard.mu.Lock()
		if err := shard.loadIndex(); err != nil {
			shard.mu.Unlock()
			return fmt.Errorf("failed to reload index: %w", err)
		}
		shard.mu.Unlock()
	}

	// Create a reader for direct access
	shard.mu.RLock()
	if shard.index == nil {
		shard.mu.RUnlock()
		return fmt.Errorf("shard index not initialized")
	}
	indexCopy := shard.cloneIndex() // Create proper deep copy to avoid race conditions
	totalEntries := shard.index.CurrentEntryNumber
	shard.mu.RUnlock()

	if totalEntries == 0 {
		return nil
	}

	// Debug log in multi-process mode
	if c.config.Concurrency.IsMultiProcess() && c.logger != nil {
		c.logger.WithFields("totalEntries", totalEntries, "numFiles", len(indexCopy.Files)).Debug("ScanAll starting")
	}

	reader, err := NewReader(shardID, indexCopy, c.config.Reader)
	if err != nil {
		return err
	}
	defer reader.Close()

	for i := int64(0); i < totalEntries; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		entry, err := reader.ReadEntryByNumber(i)
		if err != nil {
			continue // Skip bad entries
		}

		msg := StreamMessage{
			Stream: streamName,
			ID:     MessageID{ShardID: shardID, EntryNumber: i},
			Data:   entry,
		}

		if !fn(ctx, msg) {
			return nil // User requested stop
		}
	}

	return nil
}

// initCometState initializes the unified state for metrics and coordination
// In multi-process mode, it's memory-mapped to a file for sharing between processes
// In single-process mode, it's allocated in regular memory
func (s *Shard) initCometState(multiProcessMode bool) error {
	// Always use memory-mapped state for both single and multi-process modes
	// This provides crash recovery and simplifies the code
	return s.initCometStateMmap()
}

// initCometStateMmap initializes unified state via memory mapping
// Used for both single-process and multi-process modes for consistency and crash recovery
func (s *Shard) initCometStateMmap() error {
	processID := os.Getpid()

	if s.logger != nil {
		s.logger.Debug("initCometStateMmap: starting",
			"shardID", s.shardID,
			"pid", processID,
			"statePath", s.statePath)
	}

	// Since processes own their shards exclusively, simple create or open
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

	isNewFile := stat.Size() == 0

	if isNewFile {
		// Since processes own their shards exclusively, no locking needed
		// Set the size atomically
		if err := file.Truncate(CometStateSize); err != nil {
			return fmt.Errorf("failed to set state file size: %w", err)
		}

		// Sync to ensure size change is on disk
		if err := file.Sync(); err != nil {
			return fmt.Errorf("failed to sync state file after truncate: %w", err)
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

	// Log the state values immediately after mapping
	state := s.state
	if s.logger != nil {
		s.logger.Debug("initCometStateMmap: state after mmap",
			"shardID", s.shardID,
			"pid", processID,
			"isNewFile", isNewFile,
			"version", atomic.LoadUint64(&state.Version),
			"lastEntryNumber", atomic.LoadInt64(&state.LastEntryNumber),
			"writeOffset", atomic.LoadUint64(&state.WriteOffset))
	}

	// Initialize version if this is a new file
	if isNewFile {
		if s.logger != nil {
			s.logger.Debug("initCometStateMmap: initializing new state file",
				"shardID", s.shardID,
				"pid", processID)
		}

		atomic.StoreUint64(&state.Version, CometStateVersion1)
		// Initialize with -1 to indicate "not yet set" for LastEntryNumber
		// NOTE: This -1 is different from index.CurrentEntryNumber which starts at 0.
		// The synchronization code in getOrCreateShard handles this difference.
		// -1 means "no entries allocated yet", while after first write it becomes 0, 1, 2...
		atomic.StoreInt64(&state.LastEntryNumber, -1)

		if s.logger != nil {
			s.logger.Debug("initCometStateMmap: after initialization",
				"shardID", s.shardID,
				"pid", processID,
				"version", atomic.LoadUint64(&state.Version),
				"lastEntryNumber", atomic.LoadInt64(&state.LastEntryNumber))
		}

		// Also run validation to catch any initialization issues
		if err := s.validateAndRecoverState(); err != nil {
			return fmt.Errorf("state validation failed: %w", err)
		}
	} else {
		if s.logger != nil {
			s.logger.Debug("initCometStateMmap: validating existing state file",
				"shardID", s.shardID,
				"pid", processID)
		}

		// Validate existing state file
		if err := s.validateAndRecoverState(); err != nil {
			return fmt.Errorf("state validation failed: %w", err)
		}
	}

	return nil
}
