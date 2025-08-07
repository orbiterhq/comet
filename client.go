package comet

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"github.com/klauspost/compress/zstd"
)

// shardIndexPool reuses ShardIndex objects to reduce allocations
var shardIndexPool = sync.Pool{
	New: func() any {
		return &ShardIndex{
			ConsumerOffsets: make(map[string]int64),
			Files:           make([]FileInfo, 0),
			BinaryIndex: BinarySearchableIndex{
				Nodes: make([]EntryIndexNode, 0),
			},
		}
	},
}

// EntryPosition represents the location of an entry in the file system
// EntryPosition represents the location of an entry in the file system
type EntryPosition struct {
	FileIndex  int   `json:"file_index"`  // Index in the Files array
	ByteOffset int64 `json:"byte_offset"` // Byte offset within the file
}

// EntryIndexMetadata represents a stored index node
type EntryIndexMetadata struct {
	EntryNumber int64         `json:"entry_number"`
	Position    EntryPosition `json:"position"`
}

// WriteMode determines how data is written to disk
type WriteMode int

const (
	WriteModeDirect   WriteMode = iota // Direct I/O with O_SYNC
	WriteModeBuffered                  // Standard buffered writes (fastest)
	WriteModeFSync                     // Buffered writes + explicit fsync

	// Constants for entry format
	headerSize   = 4 + 8             // uint32 size + uint64 timestamp
	maxEntrySize = 128 * 1024 * 1024 // 128MB max entry size
)

// CompressionConfig controls compression behavior
type CompressionConfig struct {
	MinCompressSize int `json:"min_compress_size"` // Minimum size to compress (bytes)
}

// IndexingConfig controls indexing behavior
type IndexingConfig struct {
	BoundaryInterval int `json:"boundary_interval"` // Store boundary every N entries
	MaxIndexEntries  int `json:"max_index_entries"` // Max boundary entries per shard (0 = unlimited)
}

// StorageConfig controls file storage behavior
type StorageConfig struct {
	MaxFileSize        int64 `json:"max_file_size"`      // Maximum size per file before rotation
	CheckpointInterval int   `json:"checkpoint_time_ms"` // Checkpoint every N milliseconds
	CheckpointEntries  int   `json:"checkpoint_entries"` // Checkpoint every N entries (default: 100000)
	FlushInterval      int   `json:"flush_interval_ms"`  // Flush to OS cache every N ms (memory management, not durability)
	FlushEntries       int   `json:"flush_entries"`      // Flush to OS cache every N entries (memory management, not durability)
}

// ConcurrencyConfig controls multi-process behavior
type ConcurrencyConfig struct {
	// Process-level shard ownership (simplifies multi-process coordination)
	// When ProcessCount > 1, multi-process mode is automatically enabled
	ProcessID    int    `json:"process_id"`    // This process's ID (0-based)
	ProcessCount int    `json:"process_count"` // Total number of processes (0 = single-process)
	SHMFile      string `json:"shm_file"`      // Shared memory file path (empty = os.TempDir()/comet-worker-slots-shm)
}

// Owns checks if this process owns a particular shard
func (c ConcurrencyConfig) Owns(shardID uint32) bool {
	if c.ProcessCount <= 1 {
		return true // Single process owns all shards
	}
	return int(shardID%uint32(c.ProcessCount)) == c.ProcessID
}

// IsMultiProcess returns true if running in multi-process mode
func (c ConcurrencyConfig) IsMultiProcess() bool {
	return c.ProcessCount > 1
}

// RetentionConfig controls data retention policies
type RetentionConfig struct {
	MaxAge            time.Duration `json:"max_age"`             // Delete files older than this
	MaxBytes          int64         `json:"max_bytes"`           // Delete oldest files if total size exceeds
	MaxTotalSize      int64         `json:"max_total_size"`      // Alias for MaxBytes for compatibility
	MaxShardSize      int64         `json:"max_shard_size"`      // Maximum size per shard
	CheckUnconsumed   bool          `json:"check_unconsumed"`    // Protect unconsumed messages
	ProtectUnconsumed bool          `json:"protect_unconsumed"`  // Alias for CheckUnconsumed
	CleanupInterval   time.Duration `json:"cleanup_interval"`    // How often to run cleanup (0 = disabled)
	FileGracePeriod   time.Duration `json:"file_grace_period"`   // Don't delete files newer than this
	ForceDeleteAfter  time.Duration `json:"force_delete_after"`  // Force delete files older than this
	SafetyMargin      float64       `json:"safety_margin"`       // Keep this fraction of space free (0.0-1.0)
	MinFilesToRetain  int           `json:"min_files_to_retain"` // Always keep at least N files per shard
	MinFilesToKeep    int           `json:"min_files_to_keep"`   // Alias for MinFilesToRetain
}

// CometConfig represents the complete comet configuration
type CometConfig struct {
	// Write mode
	WriteMode WriteMode `json:"write_mode"`

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
			MaxFileSize:        maxFileSize,
			CheckpointInterval: 2000,   // Checkpoint every 2 seconds
			CheckpointEntries:  100000, // Checkpoint every 100k entries
			FlushEntries:       50000,  // Flush every 50k entries (~5MB) - optimal for large batches
			FlushInterval:      1000,   // Flush and make data visible every 1 second
		},

		// Concurrency - single-process mode by default
		// Set ProcessCount > 1 for multi-process deployments (e.g., prefork mode)
		Concurrency: ConcurrencyConfig{
			ProcessID:    0, // Default to process 0
			ProcessCount: 0, // 0 = single-process mode
		},

		// Retention - Keep 1 week of data by default
		Retention: RetentionConfig{
			MaxAge:           7 * 24 * time.Hour,
			MaxBytes:         10 << 30, // 10GB max per shard
			CheckUnconsumed:  true,     // Protect unconsumed data
			CleanupInterval:  1 * time.Hour,
			FileGracePeriod:  5 * time.Minute,
			SafetyMargin:     0.1, // Keep 10% free
			MinFilesToRetain: 2,   // Always keep at least 2 files
		},

		// Logging
		Log: LogConfig{
			Level: func() string {
				if os.Getenv("COMET_DEBUG") != "" && os.Getenv("COMET_DEBUG") != "0" && strings.ToLower(os.Getenv("COMET_DEBUG")) != "false" {
					return "debug"
				}
				return "info"
			}(),
		},
	}

	if isFiberChild == envPreforkChildVal {
		// We're in a Fiber worker - enable multi-process coordination
		processID := GetProcessID() // Use default shared memory
		if processID >= 0 {
			cfg.Concurrency.ProcessID = processID
			cfg.Concurrency.ProcessCount = runtime.NumCPU()
		}
	}

	return cfg
}

// DeprecatedMultiProcessConfig creates a config for multi-process mode with N processes
func DeprecatedMultiProcessConfig(processID, processCount int) CometConfig {
	cfg := DefaultCometConfig()
	cfg.Concurrency.ProcessID = processID
	cfg.Concurrency.ProcessCount = processCount
	return cfg
}

// HighCompressionConfig returns a config optimized for compression ratio
func HighCompressionConfig() CometConfig {
	cfg := DefaultCometConfig()
	cfg.Compression.MinCompressSize = 512 // Compress smaller entries
	return cfg
}

// HighThroughputConfig returns a config optimized for write throughput
func HighThroughputConfig() CometConfig {
	cfg := DefaultCometConfig()
	cfg.Storage.CheckpointInterval = 10000        // Less frequent checkpoints
	cfg.Storage.CheckpointEntries = 200000        // Checkpoint every 200k entries (infrequent syncs)
	cfg.Storage.FlushEntries = 100000             // Flush every 100k entries for large batch throughput
	cfg.Compression.MinCompressSize = 1024 * 1024 // Only compress very large entries
	cfg.Indexing.BoundaryInterval = 1000          // Less frequent index entries
	// Reader config is set to defaults in DefaultReaderConfig()
	return cfg
}

// OptimizedConfig returns a configuration optimized for a specific number of shards.
// Based on benchmarking, it adjusts file size and other parameters for optimal performance.
//
// Recommended shard counts:
//   - 16 shards: Good for small deployments
//   - 64 shards: Balanced performance
//   - 256 shards: Optimal for high throughput (2.4M ops/sec)
//
// Example:
//
//	config := comet.OptimizedConfig(256, 3072) // For 256 shards with 3GB memory budget
//	client, err := comet.NewClient("/data", config)
func OptimizedConfig(shardCount int, memoryBudget int) CometConfig {
	cfg := DefaultCometConfig()

	// Calculate optimal file size based on 3GB memory budget
	fileSizeMB := memoryBudget / shardCount

	// Clamp file size to reasonable bounds
	if fileSizeMB < 10 {
		fileSizeMB = 10 // Minimum 10MB
	} else if fileSizeMB > 1024 {
		fileSizeMB = 1024 // Maximum 1GB
	}

	cfg.Storage.MaxFileSize = int64(fileSizeMB) << 20

	// Use our benchmarked optimal values for flush and checkpoint
	// These were proven to provide best performance in our tests
	cfg.Storage.FlushEntries = 50_000       // Optimal for batch throughput
	cfg.Storage.CheckpointEntries = 100_000 // Reduced from default, better for high throughput

	// For high shard counts, reduce checkpoint time to avoid memory pressure
	if shardCount >= 256 {
		cfg.Storage.CheckpointInterval = 5000 // 5 seconds for many shards
	}

	return cfg
}

// validateConfig validates the configuration and sets defaults
func validateConfig(cfg *CometConfig) error {
	if cfg.Storage.MaxFileSize <= 0 {
		cfg.Storage.MaxFileSize = 256 << 20 // 256MB default
	}
	if cfg.Storage.CheckpointInterval <= 0 {
		cfg.Storage.CheckpointInterval = 2000 // 2 seconds default
	}
	if cfg.Storage.CheckpointEntries <= 0 {
		cfg.Storage.CheckpointEntries = 100000 // 100k entries default
	}
	if cfg.Storage.FlushEntries < 0 {
		cfg.Storage.FlushEntries = 50000 // 50k entries default
	}
	// FlushEntries of 0 means no entry-based flushing (rely on buffer size)

	// Validate constraint: checkpoint should be >= flush (if both are set)
	if cfg.Storage.FlushEntries > 0 && cfg.Storage.CheckpointEntries < cfg.Storage.FlushEntries {
		// Auto-adjust checkpoint to be at least 2x flush for efficiency
		cfg.Storage.CheckpointEntries = cfg.Storage.FlushEntries * 2
	}
	if cfg.Compression.MinCompressSize < 0 {
		cfg.Compression.MinCompressSize = 4096 // 4KB default
	}
	if cfg.Indexing.BoundaryInterval <= 0 {
		cfg.Indexing.BoundaryInterval = 100 // Default interval
	}
	if cfg.Concurrency.ProcessCount < 0 {
		return fmt.Errorf("process count cannot be negative")
	}
	if cfg.Concurrency.ProcessCount > 0 && (cfg.Concurrency.ProcessID < 0 || cfg.Concurrency.ProcessID >= cfg.Concurrency.ProcessCount) {
		return fmt.Errorf("process ID %d is out of range [0, %d)", cfg.Concurrency.ProcessID, cfg.Concurrency.ProcessCount)
	}

	// Retention validation
	if cfg.Retention.SafetyMargin < 0 || cfg.Retention.SafetyMargin > 1 {
		cfg.Retention.SafetyMargin = 0.1 // Default 10%
	}
	if cfg.Retention.MinFilesToRetain < 1 {
		cfg.Retention.MinFilesToRetain = 1
	}

	// Reader validation - use defaults from DefaultReaderConfig()
	if cfg.Reader.MaxMappedFiles <= 0 {
		cfg.Reader.MaxMappedFiles = 10
	}
	if cfg.Reader.MaxMemoryBytes <= 0 {
		cfg.Reader.MaxMemoryBytes = 2 * 1024 * 1024 * 1024
	}
	if cfg.Reader.CleanupInterval <= 0 {
		cfg.Reader.CleanupInterval = 5000
	}

	return nil
}

// CometStats provides runtime statistics
type CometStats struct {
	TotalEntries        int64            `json:"total_entries"`
	TotalBytes          int64            `json:"total_bytes"`
	TotalCompressed     int64            `json:"total_compressed"`
	CompressedEntries   int64            `json:"compressed_entries"`
	SkippedCompression  int64            `json:"skipped_compression"`
	FileRotations       int64            `json:"file_rotations"`
	CompressionWaitNano int64            `json:"compression_wait_nano"`
	ConsumerGroups      int              `json:"consumer_groups"`
	ConsumerOffsets     map[string]int64 `json:"consumer_offsets"`
	WriteThroughput     float64          `json:"write_throughput_mbps"`
	CompressionRatio    float64          `json:"compression_ratio"`
	OpenReaders         int64            `json:"open_readers"`
	MaxLag              int64            `json:"max_lag"`
	FileCount           int              `json:"file_count"`
	IndexSize           int64            `json:"index_size"`
	UptimeSeconds       int64            `json:"uptime_seconds"`
}

// ClientMetrics tracks global client metrics
type ClientMetrics struct {
	// Write metrics
	WritesTotal        atomic.Uint64
	BytesWritten       atomic.Uint64
	WriteErrors        atomic.Uint64
	WriteLatencyNanos  atomic.Uint64
	CompressionSaves   atomic.Uint64
	CompressionSkipped atomic.Uint64

	// Read metrics
	ReadsTotal       atomic.Uint64
	BytesRead        atomic.Uint64
	ReadErrors       atomic.Uint64
	ConsumerLagTotal atomic.Uint64

	// File metrics
	FileRotations atomic.Uint64
	FilesCreated  atomic.Uint64

	// Additional metrics
	LastErrorNano      atomic.Int64
	TotalEntries       atomic.Uint64
	FilesDeleted       atomic.Uint64
	CheckpointsWritten atomic.Uint64

	// Process coordination metrics
	ShardConflicts    atomic.Uint64
	LockWaitNanos     atomic.Uint64
	RecoveryAttempts  atomic.Uint64
	RecoverySuccesses atomic.Uint64

	// Index persistence errors
	IndexPersistErrors atomic.Uint64
	ErrorCount         atomic.Uint64

	// Consumer metrics
	AckCount         atomic.Uint64
	ActiveConsumers  atomic.Uint64
	ConsumerTimeouts atomic.Uint64
	ConsumerResets   atomic.Uint64

	// Retention metrics
	RetentionRuns    atomic.Uint64
	FilesCleanedUp   atomic.Uint64
	BytesCleanedUp   atomic.Uint64
	RetentionErrors  atomic.Uint64
	RetentionSkipped atomic.Uint64
}

var (
	isFiberChild       = os.Getenv("FIBER_PREFORK_CHILD")
	envPreforkChildVal = "1"
)

// MultiProcessConfig returns a configuration optimized for multi-process deployments.
// It automatically acquires a unique process ID and configures the client for multi-process mode.
// The process ID is automatically released when the client is closed.
func MultiProcessConfig(sharedMemoryFile ...string) CometConfig {
	// Acquire process ID
	processID := GetProcessID(sharedMemoryFile...)
	if processID < 0 {
		panic("failed to acquire process ID - all slots may be taken")
	}

	// Create multi-process config
	config := DeprecatedMultiProcessConfig(processID, runtime.NumCPU())

	// Set the shared memory file
	if len(sharedMemoryFile) > 0 && sharedMemoryFile[0] != "" {
		config.Concurrency.SHMFile = sharedMemoryFile[0]
	}

	return config
}

// Client implements a local file-based stream client with append-only storage
type Client struct {
	dataDir          string
	config           CometConfig
	logger           Logger
	shards           map[uint32]*Shard
	metrics          ClientMetrics
	mu               sync.RWMutex
	closed           bool
	retentionWg      sync.WaitGroup
	stopCh           chan struct{}
	startTime        time.Time
	sharedMemoryFile string // For automatic process ID cleanup

	// Multi-process optimization: cache owned shards per shard count
	ownedShardsCache sync.Map // map[uint32][]uint32 - shardCount -> owned shard IDs
}

// isShardOwnedByProcess checks if this process owns a specific shard
// isShardOwnedByProcess removed - processes always own their shards exclusively

// Shard represents a single stream shard with its own files
// Fields ordered for optimal memory alignment (64-bit words first)
type Shard struct {
	// 64-bit aligned fields first (8 bytes each)
	readerCount        int64     // Lock-free reader tracking
	nextEntryNumber    int64     // Next entry number to assign (includes pending)
	pendingWriteOffset int64     // Current write offset including pending writes
	lastCheckpoint     time.Time // 64-bit on most systems
	lastIndexReload    time.Time // Last time index was reloaded from disk
	// lastMmapCheck removed - processes own their shards exclusively

	// Pointers (8 bytes each on 64-bit)
	dataFile   *os.File      // Data file handle
	writer     *bufio.Writer // Buffered writer
	compressor *zstd.Encoder // Compression engine
	index      *ShardIndex   // Shard metadata
	// Lock files removed - processes own their shards exclusively
	state  *CometState // Unified memory-mapped state for all metrics and coordination
	logger Logger      // Logger for this shard

	// Strings (24 bytes: ptr + len + cap)
	indexPath string // Path to index file
	statePath string // Path to unified state file
	stateData []byte // Memory-mapped unified state data (slice header: 24 bytes)

	// Mutex (platform-specific, often 24 bytes)
	mu      sync.RWMutex
	writeMu sync.Mutex // Protects DirectWriter from concurrent writes
	indexMu sync.Mutex // Protects index file writes

	// Synchronization for background operations
	wg        sync.WaitGroup // Tracks background goroutines
	stopFlush chan struct{}  // Signal to stop periodic flush

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
	BoundaryInterval int `json:"boundary_interval"` // Store boundaries every N entries (4 bytes)
}

// FileInfo represents a data file in the shard
// Fields ordered for optimal memory alignment
type FileInfo struct {
	// 64-bit aligned fields first (8 bytes each)
	StartOffset int64 `json:"start_offset"` // Byte offset in virtual stream
	EndOffset   int64 `json:"end_offset"`   // Last byte offset + 1
	StartEntry  int64 `json:"start_entry"`  // First entry number
	Entries     int64 `json:"entries"`      // Number of entries

	// Time fields (24 bytes each on 64-bit due to location pointer)
	StartTime time.Time `json:"start_time"` // File creation time
	EndTime   time.Time `json:"end_time"`   // Last write time

	// String last (will use remaining space efficiently)
	Path string `json:"path"`
}

// NewMultiProcessClient creates a new comet client with automatic multi-process coordination.
// It uses the default shared memory file for process ID coordination.
// The process ID is automatically released when the client is closed.
//
// Example usage:
//
//	client, err := comet.NewMultiProcessClient("./data")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer client.Close() // Automatically releases process ID
func NewMultiProcessClient(dataDir string, cfg ...CometConfig) (*Client, error) {
	var config CometConfig

	if len(cfg) > 0 && cfg[0].Concurrency.IsMultiProcess() {
		// Use the provided multi-process config
		config = cfg[0]
	} else {
		// Create a new multi-process config
		var shmFile string
		if len(cfg) > 0 && cfg[0].Concurrency.SHMFile != "" {
			shmFile = cfg[0].Concurrency.SHMFile
		}

		if shmFile == "" {
			config = MultiProcessConfig()
		} else {
			config = MultiProcessConfig(shmFile)
		}

		// Copy other settings from provided config
		if len(cfg) > 0 {
			providedCfg := cfg[0]
			config.Compression = providedCfg.Compression
			config.Indexing = providedCfg.Indexing
			config.Storage = providedCfg.Storage
			config.Retention = providedCfg.Retention
			config.Reader = providedCfg.Reader
			config.Log = providedCfg.Log
		}
	}

	client, err := NewClient(dataDir, config)
	if err != nil {
		// Release process ID on failure
		if config.Concurrency.SHMFile != "" {
			ReleaseProcessID(config.Concurrency.SHMFile)
		} else {
			ReleaseProcessID()
		}
		return nil, err
	}

	// Mark that this client should auto-release the process ID
	client.sharedMemoryFile = config.Concurrency.SHMFile

	return client, nil
}

// NewClient creates a new comet client with custom configuration
func NewClient(dataDir string, config ...CometConfig) (*Client, error) {
	cfg := DefaultCometConfig()
	if len(config) > 0 {
		cfg = config[0]
	}
	if cfg.Reader.MaxMemoryBytes == 0 && cfg.Reader.CleanupInterval == 0 && cfg.Reader.MaxMappedFiles == 0 {
		cfg.Reader = ReaderConfigForStorage(cfg.Storage.MaxFileSize)
	}
	// Validate configuration
	if err := validateConfig(&cfg); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	// Create logger based on config
	logger := createLogger(cfg.Log)

	c := &Client{
		dataDir:   dataDir,
		config:    cfg,
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

	// Check process ownership for writes
	if !c.config.Concurrency.Owns(shardID) {
		return nil, fmt.Errorf("shard %d is not owned by process %d (assigned to process %d)",
			shardID, c.config.Concurrency.ProcessID, int(shardID%uint32(c.config.Concurrency.ProcessCount)))
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

	// Handle potential index rebuild in multi-process mode
	if shard.state != nil && shard.checkIfRebuildNeeded() {
		shard.mu.Lock()
		needsRebuild := shard.checkIfRebuildNeeded()
		shard.mu.Unlock()

		if needsRebuild {
			// Call rebuild without holding the lock to avoid deadlock
			shard.lazyRebuildIndexIfNeeded(c.config, filepath.Join(c.dataDir, fmt.Sprintf("shard-%04d", shard.shardID)))
		}
	}

	// Now get the length with read lock
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	// Return nextEntryNumber which tracks all assigned entries (both flushed and pending)
	return shard.nextEntryNumber, nil
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
				// Track sync latency
				syncStart := time.Now()
				err = shard.dataFile.Sync()
				if err == nil && shard.state != nil {
					syncDuration := time.Since(syncStart).Nanoseconds()
					atomic.AddInt64(&shard.state.SyncLatencyNanos, syncDuration)
					atomic.AddUint64(&shard.state.SyncCount, 1)
				}
			}
			shard.writeMu.Unlock()
			if err != nil {
				shard.mu.Unlock()
				return fmt.Errorf("failed to sync shard %d: %w", shard.shardID, err)
			}
		}

		// Update index to reflect what's now persisted to disk
		// CurrentEntryNumber should match nextEntryNumber after all pending writes are flushed
		shard.index.CurrentEntryNumber = shard.nextEntryNumber

		// Update CurrentWriteOffset and EndOffset to match actual file sizes
		// This ensures GetShardStats() returns correct TotalBytes after sync
		if shard.dataFile != nil {
			if stat, err := shard.dataFile.Stat(); err == nil {
				actualSize := stat.Size()
				shard.index.CurrentWriteOffset = actualSize
				shard.pendingWriteOffset = actualSize // Sync pending offset with actual file size

				// Update current file EndOffset and Entries count
				if len(shard.index.Files) > 0 {
					current := &shard.index.Files[len(shard.index.Files)-1]
					current.EndOffset = shard.index.CurrentWriteOffset
					current.EndTime = time.Now()
					// Calculate actual entry count for this file
					current.Entries = shard.index.CurrentEntryNumber - current.StartEntry
				}
			}
		}

		// Force checkpoint
		// Update mmap state while holding the lock
		shard.updateMmapState()

		shard.writesSinceCheckpoint = 0
		shard.lastCheckpoint = time.Now()
		shard.mu.Unlock()

		// Persist the index - this will also update metrics
		if err := shard.persistIndex(); err != nil {
			return fmt.Errorf("failed to persist index for shard %d: %w", shard.shardID, err)
		}
	}

	return nil
}

// loadExistingShard loads a shard that was created by another process

// getOrCreateShard returns an existing shard or creates a new one
// getShard retrieves an existing shard without creating it if it doesn't exist.
// Returns nil if the shard doesn't exist.
func (c *Client) getShard(shardID uint32) *Shard {
	c.mu.RLock()
	shard := c.shards[shardID]
	c.mu.RUnlock()
	return shard
}

// getMaxShardID returns the highest shard ID that this client knows about.
// Used by consumers for efficient shard discovery without filesystem scanning.
func (c *Client) getMaxShardID() uint32 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var maxShardID uint32 = 0
	for shardID := range c.shards {
		if shardID > maxShardID {
			maxShardID = shardID
		}
	}
	return maxShardID
}

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
		if c.logger != nil && IsDebug() {
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
		stopFlush: make(chan struct{}),
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
	if err := shard.initCometState(); err != nil {
		return nil, fmt.Errorf("failed to initialize unified state: %w", err)
	}

	// Since each process owns its shards exclusively in multi-process mode,
	// we don't need lock files for coordination. Processes can directly write
	// to their owned shards without locking.

	// Load existing index if present
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

	// Initialize nextEntryNumber from index (what's on disk)
	shard.nextEntryNumber = shard.index.CurrentEntryNumber
	shard.pendingWriteOffset = shard.index.CurrentWriteOffset

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

	// Initialize file metrics by calculating size of all existing files
	if state := shard.state; state != nil && len(shard.index.Files) > 0 {
		var totalBytes int64
		for _, fileInfo := range shard.index.Files {
			totalBytes += fileInfo.EndOffset - fileInfo.StartOffset
		}
		atomic.StoreUint64(&state.TotalFileBytes, uint64(totalBytes))
		atomic.StoreUint64(&state.CurrentFiles, uint64(len(shard.index.Files)))

		// If FilesCreated is 0 but we have files, initialize it
		if atomic.LoadUint64(&state.FilesCreated) == 0 {
			atomic.StoreUint64(&state.FilesCreated, uint64(len(shard.index.Files)))
		}
	}

	// Open or create current data file
	if err := shard.openDataFileWithConfig(shardDir); err != nil {
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

	// Start periodic flush goroutine for this shard
	shard.startPeriodicFlush(&c.config)

	// Debug log shard creation
	if IsDebug() && c.logger != nil {
		c.logger.Debug("Created new shard",
			"shardID", shardID,
			"path", shardDir,
		)
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

// preCompressEntries compresses entries outside of any locks to reduce contention
func (s *Shard) preCompressEntries(entries [][]byte, config *CometConfig) []CompressedEntry {
	// Fast path: check if any entry needs compression
	needsCompression := false
	if s.compressor != nil {
		for _, data := range entries {
			if len(data) >= config.Compression.MinCompressSize {
				needsCompression = true
				break
			}
		}
	}

	// If no compression needed, track skipped compressions and return nil to avoid allocation
	if !needsCompression {
		if state := s.state; state != nil {
			atomic.AddUint64(&state.SkippedCompression, uint64(len(entries)))
		}
		return nil
	}

	// Slow path: at least one entry needs compression
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
	CurrentWriteOffset int64       // Snapshot of write offset at request time (8 bytes)
	WriteBuffers       [][]byte    // Buffers to write (24 bytes)
	IDs                []MessageID // Message IDs for the batch (24 bytes)
	ShouldFlush        bool        // Whether to flush after this write (1 byte)
}

// appendEntries adds raw entry bytes to the shard with I/O outside locks
func (s *Shard) appendEntries(entries [][]byte, clientMetrics *ClientMetrics, config *CometConfig) ([]MessageID, error) {
	startTime := time.Now()

	// Pre-compress entries OUTSIDE the lock to reduce contention
	compressedEntries := s.preCompressEntries(entries, config)

	// Prepare write request while holding lock
	writeReq, criticalErr := s.prepareWriteRequest(entries, compressedEntries, startTime, config)
	if criticalErr != nil {
		// Track failure metrics
		if state := s.state; state != nil {
			atomic.AddUint64(&state.FailedWrites, 1)
			atomic.AddUint64(&state.ErrorCount, 1)
			atomic.StoreInt64(&state.LastErrorNanos, time.Now().UnixNano())
		}
		return nil, criticalErr
	}

	// Perform I/O OUTSIDE the lock
	writeErr := s.performWrite(writeReq, config)
	if writeErr != nil {
		// Track failure metrics
		if state := s.state; state != nil {
			atomic.AddUint64(&state.FailedWrites, 1)
			atomic.AddUint64(&state.ErrorCount, 1)
			atomic.StoreInt64(&state.LastErrorNanos, time.Now().UnixNano())
		}
		return nil, writeErr
	}

	// Track metrics after successful write
	s.trackWriteMetrics(startTime, entries, compressedEntries, clientMetrics)

	// Post-write operations (checkpointing, rotation check)
	if err := s.performPostWriteOperations(config, clientMetrics); err != nil {
		return nil, err
	}

	return writeReq.IDs, nil
}

// prepareWriteRequest builds the write request under lock
func (s *Shard) prepareWriteRequest(entries [][]byte, compressedEntries []CompressedEntry, startTime time.Time, config *CometConfig) (WriteRequest, error) {
	var writeReq WriteRequest
	var criticalErr error

	s.mu.Lock()
	defer s.mu.Unlock()

	// Since processes own their shards exclusively, no need to check for changes

	numEntries := len(entries)
	writeReq.IDs = make([]MessageID, numEntries)
	now := startTime.UnixNano()

	// Build write buffers from pre-compressed data (minimal work under lock)
	writeReq.WriteBuffers = make([][]byte, 0, numEntries*2) // headers + data

	// Allocate header buffer for all entries at once
	// Go's runtime will handle large allocations efficiently
	allHeaders := make([]byte, numEntries*headerSize)

	// Pre-allocate entry numbers
	entryNumbers := s.allocateEntryNumbers(numEntries)

	// Build write request
	if compressedEntries != nil {
		// Use compressed entries
		s.buildWriteRequest(&writeReq, compressedEntries, entryNumbers, now, allHeaders)
	} else {
		// No compression needed - build from original entries
		s.buildWriteRequestDirect(&writeReq, entries, entryNumbers, now, allHeaders)
	}

	// Capture current write offset for performWrite to use (avoids race condition)
	writeReq.CurrentWriteOffset = s.index.CurrentWriteOffset

	// Determine if we should flush after this write based on entry count from config
	flushThreshold := int64(config.Storage.FlushEntries)
	if flushThreshold > 0 {
		// Use nextEntryNumber (which includes pending) to determine flush needs
		writeReq.ShouldFlush = (s.nextEntryNumber % flushThreshold) >= flushThreshold
	}

	return writeReq, criticalErr
}

// allocateEntryNumbers reserves entry numbers for the batch
// MUST be called with s.mu held
func (s *Shard) allocateEntryNumbers(count int) []int64 {
	entryNumbers := make([]int64, count)

	// Use nextEntryNumber which tracks all assigned entries (both flushed and pending)
	// Caller must hold the lock
	baseEntry := s.nextEntryNumber
	s.nextEntryNumber += int64(count)

	// Fill in the entry numbers
	for i := range entryNumbers {
		entryNumbers[i] = baseEntry + int64(i)
	}

	// Update state for multi-process coordination if available
	if s.state != nil {
		// Update LastEntryNumber to the highest assigned entry
		lastEntry := baseEntry + int64(count) - 1
		atomic.StoreInt64(&s.state.LastEntryNumber, lastEntry)
	}

	return entryNumbers
}

// buildWriteRequestDirect builds the write request directly from uncompressed entries
func (s *Shard) buildWriteRequestDirect(writeReq *WriteRequest, entries [][]byte, entryNumbers []int64, now int64, allHeaders []byte) {
	writeOffset := s.pendingWriteOffset
	totalBytes := uint64(0)

	for i, data := range entries {
		// Use pre-allocated header buffer
		headerStart := i * headerSize
		header := allHeaders[headerStart : headerStart+headerSize]
		binary.LittleEndian.PutUint32(header[0:4], uint32(len(data)))
		binary.LittleEndian.PutUint64(header[4:12], uint64(now))

		// Add to vectored write batch
		writeReq.WriteBuffers = append(writeReq.WriteBuffers, header, data)

		// Track entry in binary index
		entryNumber := entryNumbers[i]
		entrySize := int64(headerSize + len(data))
		totalBytes += uint64(len(data))

		// Calculate position - simple since we use buffered writes
		fileIndex := len(s.index.Files) - 1
		byteOffset := writeOffset
		position := EntryPosition{FileIndex: fileIndex, ByteOffset: byteOffset}

		// Update binary index
		if entryNumber%int64(s.index.BoundaryInterval) == 0 || entryNumber == 0 {
			s.index.BinaryIndex.AddIndexNode(entryNumber, position)
		}

		// Generate ID for this entry
		writeReq.IDs[i] = MessageID{EntryNumber: entryNumber, ShardID: s.shardID}

		// Update tracking
		writeOffset += entrySize
	}

	// Update metrics
	if state := s.state; state != nil {
		atomic.AddUint64(&state.TotalBytes, totalBytes)
		atomic.StoreUint64(&state.WriteOffset, uint64(writeOffset))
		atomic.StoreUint64(&state.BinaryIndexNodes, uint64(len(s.index.BinaryIndex.Nodes)))
	}

	// NOTE: Index updates moved to Sync() - index should only track persisted data
	// Track writes for checkpoint purposes and rotation decisions
	if len(entryNumbers) > 0 {
		s.writesSinceCheckpoint += len(entries)
		s.pendingWriteOffset = writeOffset // Track actual write position
	}
}

// buildWriteRequest builds the write request (unified for both modes)
func (s *Shard) buildWriteRequest(writeReq *WriteRequest, compressedEntries []CompressedEntry, entryNumbers []int64, now int64, allHeaders []byte) {

	writeOffset := s.pendingWriteOffset

	for i, compressedEntry := range compressedEntries {
		// Use pre-allocated header buffer
		headerStart := i * headerSize
		header := allHeaders[headerStart : headerStart+headerSize]
		binary.LittleEndian.PutUint32(header[0:4], uint32(len(compressedEntry.Data)))
		binary.LittleEndian.PutUint64(header[4:12], uint64(now))

		// Add to vectored write batch
		writeReq.WriteBuffers = append(writeReq.WriteBuffers, header, compressedEntry.Data)

		// Track entry in binary index
		entryNumber := entryNumbers[i]
		entrySize := int64(headerSize + len(compressedEntry.Data))

		// Calculate position - simple since we use buffered writes
		fileIndex := len(s.index.Files) - 1
		byteOffset := writeOffset
		position := EntryPosition{FileIndex: fileIndex, ByteOffset: byteOffset}

		// Update binary index
		if entryNumber%int64(s.index.BoundaryInterval) == 0 || entryNumber == 0 {
			s.index.BinaryIndex.AddIndexNode(entryNumber, position)
		}

		// Generate ID for this entry
		writeReq.IDs[i] = MessageID{EntryNumber: entryNumber, ShardID: s.shardID}

		// Update tracking
		writeOffset += entrySize
	}

	// NOTE: Index updates moved to Sync() - index should only track persisted data
	// Track writes for checkpoint purposes and rotation decisions
	if len(entryNumbers) > 0 {
		s.writesSinceCheckpoint += len(compressedEntries)
		s.pendingWriteOffset = writeOffset // Track actual write position
	}

	// Update state metrics
	if state := s.state; state != nil {
		atomic.StoreUint64(&state.BinaryIndexNodes, uint64(len(s.index.BinaryIndex.Nodes)))
		// Update mmap state
		s.updateMmapState()
	}
}

// performWrite performs the actual I/O operation
func (s *Shard) performWrite(writeReq WriteRequest, config *CometConfig) error {
	if IsDebug() && s.logger != nil {
		s.logger.Debug("performWrite: entry", "shard", s.shardID, "buffers", len(writeReq.WriteBuffers))
	}

	var writeErr error

	// Unified buffered write path for both single and multi-process modes
	s.writeMu.Lock()

	if IsDebug() && s.logger != nil {
		s.logger.Debug("performWrite: checking writer", "shard", s.shardID, "writer_nil", s.writer == nil)
	}

	// Ensure writer exists before attempting to write
	if s.writer == nil {
		if IsDebug() && s.logger != nil {
			s.logger.Debug("performWrite: writer is nil, initializing", "shard", s.shardID)
		}
		// Get shard directory
		shardDir := filepath.Dir(s.indexPath)
		if err := s.openDataFileForAppend(shardDir); err != nil {
			s.writeMu.Unlock()
			if IsDebug() && s.logger != nil {
				s.logger.Debug("performWrite: failed to initialize writer", "shard", s.shardID, "error", err)
			}
			return fmt.Errorf("failed to initialize writer: %w", err)
		}
		if IsDebug() && s.logger != nil {
			s.logger.Debug("performWrite: writer initialized", "shard", s.shardID, "writer_nil", s.writer == nil)
		}
	}

	// Speed-optimized: only flush when absolutely necessary
	// Check BEFORE writing if this would exceed file size
	if s.writer != nil {
		// Calculate total bytes to write
		bytesToWrite := int64(0)
		for _, buf := range writeReq.WriteBuffers {
			bytesToWrite += int64(len(buf))
		}

		// Flush if this write would exceed max file size
		if writeReq.CurrentWriteOffset+bytesToWrite > config.Storage.MaxFileSize {
			if err := s.writer.Flush(); err != nil {
				writeErr = err
			}
		}
	}

	if writeErr == nil {
		// Perform vectored writes
		for _, buf := range writeReq.WriteBuffers {
			if _, err := s.writer.Write(buf); err != nil {
				writeErr = err
				break
			}
		}
	}

	// Flush if requested (based on entry count threshold)
	if writeErr == nil && writeReq.ShouldFlush && s.writer != nil {
		writeErr = s.writer.Flush()
	}

	s.writeMu.Unlock()

	return writeErr
}

// trackWriteMetrics updates metrics after successful write
func (s *Shard) trackWriteMetrics(startTime time.Time, entries [][]byte, compressedEntries []CompressedEntry, clientMetrics *ClientMetrics) {
	// Track state metrics
	if state := s.state; state != nil {
		// Track write metrics
		atomic.AddInt64(&state.TotalEntries, int64(len(entries)))
		atomic.AddUint64(&state.TotalWrites, uint64(len(entries)))
		atomic.StoreInt64(&state.LastWriteNanos, time.Now().UnixNano())

		// Track batch metrics
		atomic.StoreUint64(&state.CurrentBatchSize, uint64(len(entries)))
		atomic.AddUint64(&state.TotalBatches, 1)

		// Track compression metrics
		totalOriginal := uint64(0)
		totalCompressed := uint64(0)
		compressedCount := uint64(0)
		skippedCount := uint64(0)

		for _, entry := range compressedEntries {
			totalOriginal += entry.OriginalSize
			totalCompressed += entry.CompressedSize
			if entry.WasCompressed {
				compressedCount++
			} else {
				skippedCount++
			}
		}

		atomic.AddUint64(&state.TotalBytes, totalOriginal)
		atomic.AddUint64(&state.TotalCompressed, totalCompressed)
		atomic.AddUint64(&state.CompressedEntries, compressedCount)
		atomic.AddUint64(&state.SkippedCompression, skippedCount)

		// Calculate compression ratio (as percentage * 100 for precision)
		if totalOriginal > 0 && compressedCount > 0 {
			saved := totalOriginal - totalCompressed
			ratio := (saved * 10000) / totalOriginal // Ratio as basis points
			atomic.StoreUint64(&state.CompressionRatio, ratio)
		}

		// Track write latency
		latency := time.Since(startTime)
		latencyNanos := uint64(latency.Nanoseconds())

		// Update latency metrics
		atomic.AddUint64(&state.WriteLatencySum, latencyNanos)
		atomic.AddUint64(&state.WriteLatencyCount, 1)

		// Update min/max latency using CAS loop
		for {
			current := atomic.LoadUint64(&state.MinWriteLatency)
			if current != 0 && current <= latencyNanos {
				break
			}
			if atomic.CompareAndSwapUint64(&state.MinWriteLatency, current, latencyNanos) {
				break
			}
		}

		for {
			current := atomic.LoadUint64(&state.MaxWriteLatency)
			if current >= latencyNanos {
				break
			}
			if atomic.CompareAndSwapUint64(&state.MaxWriteLatency, current, latencyNanos) {
				break
			}
		}

	}

	// Update client metrics
	if clientMetrics != nil {
		clientMetrics.WritesTotal.Add(uint64(len(entries)))

		totalBytes := uint64(0)
		for _, data := range entries {
			totalBytes += uint64(len(data))
		}
		clientMetrics.BytesWritten.Add(totalBytes)

		// Track compression savings
		for _, entry := range compressedEntries {
			if entry.WasCompressed {
				saved := entry.OriginalSize - entry.CompressedSize
				clientMetrics.CompressionSaves.Add(saved)
			} else {
				clientMetrics.CompressionSkipped.Add(1)
			}
		}

		// Track write latency
		latency := time.Since(startTime)
		clientMetrics.WriteLatencyNanos.Add(uint64(latency.Nanoseconds()))
	}
}

// performPostWriteOperations handles checkpointing and rotation
func (s *Shard) performPostWriteOperations(config *CometConfig, clientMetrics *ClientMetrics) error {
	s.mu.RLock()
	shouldCheckpoint := s.shouldCheckpoint(config)
	shouldRotate := s.shouldRotateFile(config)
	s.mu.RUnlock()

	if shouldCheckpoint {
		s.maybeCheckpoint(clientMetrics, config)
	}

	if shouldRotate {
		if err := s.rotateFile(config); err != nil {
			return fmt.Errorf("failed to rotate file: %w", err)
		}
	}

	return nil
}

// shouldCheckpoint determines if checkpoint is needed
func (s *Shard) shouldCheckpoint(config *CometConfig) bool {
	// Time-based checkpoint
	if time.Since(s.lastCheckpoint) > time.Duration(config.Storage.CheckpointInterval)*time.Millisecond {
		return true
	}

	// Entry-based checkpoint
	if s.writesSinceCheckpoint >= config.Storage.CheckpointEntries {
		return true
	}

	return false
}

// shouldRotateFile determines if file rotation is needed
func (s *Shard) shouldRotateFile(config *CometConfig) bool {
	// Use pendingWriteOffset which includes buffered writes
	return s.pendingWriteOffset >= config.Storage.MaxFileSize
}

// maybeCheckpoint conditionally saves the index if needed
func (s *Shard) maybeCheckpoint(clientMetrics *ClientMetrics, config *CometConfig) {
	s.mu.Lock()
	if !s.shouldCheckpoint(config) {
		s.mu.Unlock()
		return
	}

	// Track checkpoint timing
	checkpointStart := time.Now()

	// Clone index while holding lock
	indexCopy := s.cloneIndex()
	s.writesSinceCheckpoint = 0
	s.lastCheckpoint = time.Now()
	s.mu.Unlock()

	// Return index to pool after use
	defer returnIndexToPool(indexCopy)

	s.indexMu.Lock()
	err := s.saveBinaryIndex(indexCopy)
	s.indexMu.Unlock()

	if err != nil && clientMetrics != nil {
		clientMetrics.IndexPersistErrors.Add(1)
		clientMetrics.ErrorCount.Add(1)
	} else {
		if clientMetrics != nil {
			clientMetrics.CheckpointsWritten.Add(1)
		}
		// Track checkpoint metrics in state
		if state := s.state; state != nil {
			atomic.AddUint64(&state.CheckpointCount, 1)
			atomic.StoreInt64(&state.LastCheckpointNanos, time.Now().UnixNano())
			checkpointDuration := time.Since(checkpointStart).Nanoseconds()
			atomic.AddInt64(&state.CheckpointTimeNanos, checkpointDuration)
			
			// CRITICAL: Update LastIndexUpdate so consumers know to reload
			state.SetLastIndexUpdate(time.Now().UnixNano())
		}
	}
}

// rotateFile creates a new data file when size limit is reached
func (s *Shard) rotateFile(config *CometConfig) error {
	// Track rotation time
	rotationStart := time.Now()

	// Quick check without lock
	s.mu.RLock()
	needsRotation := s.pendingWriteOffset >= config.Storage.MaxFileSize
	currentWriteOffset := s.pendingWriteOffset
	s.mu.RUnlock()

	if !needsRotation {
		return nil
	}

	// Prepare new file BEFORE taking locks
	shardDir := filepath.Dir(s.indexPath)

	// Ensure shard directory exists (in case it was deleted)
	if err := os.MkdirAll(shardDir, 0755); err != nil {
		// Track failed rotation
		if state := s.state; state != nil {
			atomic.AddUint64(&state.FailedRotations, 1)
		}
		return fmt.Errorf("failed to create shard directory: %w", err)
	}

	// Use sequence counter for consistent file naming
	var fileSequence uint64
	if state := s.state; state != nil {
		fileSequence = state.AddLastFileSequence(1)
	} else {
		// Fallback for single-process mode - use timestamp
		fileSequence = uint64(time.Now().UnixNano())
	}
	newFilePath := filepath.Join(shardDir, fmt.Sprintf("log-%016d.comet", fileSequence))
	newFile, err := os.OpenFile(newFilePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		// Track failed rotation
		if state := s.state; state != nil {
			atomic.AddUint64(&state.FailedRotations, 1)
		}
		return fmt.Errorf("failed to create new file: %w", err)
	}

	// Take locks for the critical section
	s.mu.Lock()
	defer s.mu.Unlock()

	s.writeMu.Lock()

	// Double-check rotation is still needed
	if s.pendingWriteOffset < config.Storage.MaxFileSize {
		s.writeMu.Unlock()
		newFile.Close()
		os.Remove(newFilePath)
		return nil
	}

	// Track rotation
	if state := s.state; state != nil {
		atomic.AddUint64(&state.FileRotations, 1)
	}

	// Save references to old file and writer
	oldFile := s.dataFile
	oldWriter := s.writer

	// Quick swap to new file (fast operation)
	s.dataFile = newFile
	bufferSize := 64 * 1024 // 64KB buffer
	s.writer = bufio.NewWriterSize(newFile, bufferSize)

	// Update index metadata (fast in-memory operations)
	if len(s.index.Files) > 0 {
		current := &s.index.Files[len(s.index.Files)-1]
		current.EndTime = time.Now()
		current.EndOffset = currentWriteOffset
		// Calculate actual entry count for this file
		current.Entries = s.nextEntryNumber - current.StartEntry
	}

	// Add new file to index
	newFileInfo := FileInfo{
		Path:       newFilePath,
		StartEntry: s.nextEntryNumber, // Use nextEntryNumber for pending writes
		StartTime:  time.Now(),
		Entries:    0,
		EndOffset:  0,
	}
	s.index.Files = append(s.index.Files, newFileInfo)
	s.index.CurrentWriteOffset = 0
	s.pendingWriteOffset = 0 // Reset pending offset for new file
	s.index.CurrentFile = newFilePath

	// Track new file
	if state := s.state; state != nil {
		atomic.AddUint64(&state.FilesCreated, 1)
		atomic.StoreUint64(&state.CurrentFiles, uint64(len(s.index.Files)))

		// Calculate total file size across all files
		var totalBytes int64
		for _, fileInfo := range s.index.Files {
			totalBytes += fileInfo.EndOffset - fileInfo.StartOffset
		}
		atomic.StoreUint64(&state.TotalFileBytes, uint64(totalBytes))
	}

	// Release write lock ASAP
	s.writeMu.Unlock()

	// Do slow I/O operations AFTER releasing write lock (still holding main lock)
	if oldWriter != nil {
		if err := oldWriter.Flush(); err != nil {
			// Log but don't fail - data is already written
			if s.logger != nil {
				s.logger.Warn("Failed to flush old writer during rotation", "error", err)
			}
		}
	}

	if oldFile != nil {
		// For now, do this synchronously to fix tests
		// TODO: Make async once we handle file references properly
		syncStart := time.Now()
		if err := oldFile.Sync(); err != nil {
			if s.logger != nil {
				s.logger.Warn("Failed to sync old file during rotation", "error", err)
			}
		} else if state := s.state; state != nil {
			// Track sync latency during rotation
			syncDuration := time.Since(syncStart).Nanoseconds()
			atomic.AddInt64(&state.SyncLatencyNanos, syncDuration)
			atomic.AddUint64(&state.SyncCount, 1)
		}
		if err := oldFile.Close(); err != nil {
			if s.logger != nil {
				s.logger.Warn("Failed to close old file during rotation", "error", err)
			}
		}
	}

	// Skip checkpoint during rotation to avoid deadlock
	// The periodic flush and regular checkpoints will handle persistence

	// Track rotation time
	if state := s.state; state != nil {
		rotationDuration := time.Since(rotationStart).Nanoseconds()
		atomic.AddInt64(&state.RotationTimeNanos, rotationDuration)
	}

	return nil
}

// cloneIndex creates a deep copy of the shard index for safe persistence
func (s *Shard) cloneIndex() *ShardIndex {
	// Get object from pool
	clone := shardIndexPool.Get().(*ShardIndex)

	// Reset and populate fields
	clone.CurrentEntryNumber = s.index.CurrentEntryNumber
	clone.CurrentWriteOffset = s.index.CurrentWriteOffset
	clone.CurrentFile = s.index.CurrentFile
	clone.BoundaryInterval = s.index.BoundaryInterval

	// Clear and repopulate consumer offsets
	for k := range clone.ConsumerOffsets {
		delete(clone.ConsumerOffsets, k)
	}

	maps.Copy(clone.ConsumerOffsets, s.index.ConsumerOffsets)

	// Resize files slice if needed
	if cap(clone.Files) < len(s.index.Files) {
		clone.Files = make([]FileInfo, len(s.index.Files))
	} else {
		clone.Files = clone.Files[:len(s.index.Files)]
	}
	copy(clone.Files, s.index.Files)

	// Update binary index
	clone.BinaryIndex.IndexInterval = s.index.BinaryIndex.IndexInterval
	clone.BinaryIndex.MaxNodes = s.index.BinaryIndex.MaxNodes

	// Resize nodes slice if needed
	if cap(clone.BinaryIndex.Nodes) < len(s.index.BinaryIndex.Nodes) {
		clone.BinaryIndex.Nodes = make([]EntryIndexNode, len(s.index.BinaryIndex.Nodes))
	} else {
		clone.BinaryIndex.Nodes = clone.BinaryIndex.Nodes[:len(s.index.BinaryIndex.Nodes)]
	}
	copy(clone.BinaryIndex.Nodes, s.index.BinaryIndex.Nodes)

	return clone
}

// returnIndexToPool returns a ShardIndex to the pool for reuse
func returnIndexToPool(index *ShardIndex) {
	if index != nil {
		shardIndexPool.Put(index)
	}
}

// updateMmapState updates shared state after index changes
func (s *Shard) updateMmapState() {
	state := s.state
	if state == nil {
		return
	}

	// Update metrics in mmap state
	atomic.StoreUint64(&state.WriteOffset, uint64(s.index.CurrentWriteOffset))

	// Update file count and file size
	if len(s.index.Files) > 0 {
		atomic.StoreUint64(&state.CurrentFiles, uint64(len(s.index.Files)))
		// Update FileSize to the current file's end offset
		currentFile := &s.index.Files[len(s.index.Files)-1]
		atomic.StoreUint64(&state.FileSize, uint64(currentFile.EndOffset-currentFile.StartOffset))
		atomic.StoreUint64(&state.ActiveFileIndex, uint64(len(s.index.Files)-1))
	}

	// Update timestamp to signal state change
	state.SetLastIndexUpdate(time.Now().UnixNano())

	// For compatibility with the simplified state update
	s.updateLastEntryState()
}

// updateLastEntryState updates the LastEntryNumber in the mmap state
func (s *Shard) updateLastEntryState() {
	state := s.state
	if state == nil {
		return
	}

	// The last entry number that was actually written (not the next one to allocate)
	// If CurrentEntryNumber is 5, we've written entries 0,1,2,3,4, so LastEntryNumber is 4
	if s.index.CurrentEntryNumber > 0 {
		lastWritten := s.index.CurrentEntryNumber - 1
		atomic.StoreInt64(&state.LastEntryNumber, lastWritten)
		if IsDebug() && s.logger != nil {
			s.logger.Debug("Updated LastEntryNumber in state",
				"shard", s.shardID,
				"currentEntryNumber", s.index.CurrentEntryNumber,
				"lastEntryNumber", lastWritten)
		}
	}
}

// openDataFileForAppend opens a new data file for appending
func (s *Shard) openDataFileForAppend(shardDir string) error {
	if IsDebug() && s.logger != nil {
		s.logger.Debug("openDataFileForAppend: entry", "shard", s.shardID, "dir", shardDir)
	}

	// Ensure shard directory exists before creating file
	if err := os.MkdirAll(shardDir, 0755); err != nil {
		return fmt.Errorf("failed to create shard directory: %w", err)
	}

	// Use sequence counter for consistent file naming
	var fileSequence uint64
	if state := s.state; state != nil {
		fileSequence = state.AddLastFileSequence(1)
	} else {
		// Fallback for single-process mode - use timestamp
		fileSequence = uint64(time.Now().UnixMicro())
	}
	filename := fmt.Sprintf("log-%016d.comet", fileSequence)
	filePath := filepath.Join(shardDir, filename)

	if IsDebug() && s.logger != nil {
		s.logger.Debug("openDataFileForAppend: creating file", "shard", s.shardID, "file", filePath)
	}

	// Open file based on write mode
	flags := os.O_CREATE | os.O_WRONLY | os.O_APPEND

	file, err := os.OpenFile(filePath, flags, 0644)
	if err != nil {
		if IsDebug() && s.logger != nil {
			s.logger.Debug("openDataFileForAppend: failed to open file", "shard", s.shardID, "error", err)
		}
		return fmt.Errorf("failed to create data file: %w", err)
	}

	// Create buffered writer
	bufferSize := 64 * 1024 // 64KB buffer
	writer := bufio.NewWriterSize(file, bufferSize)

	// Store file and writer
	s.dataFile = file
	s.writer = writer

	if IsDebug() && s.logger != nil {
		s.logger.Debug("openDataFileForAppend: set writer", "shard", s.shardID, "writer_nil", s.writer == nil)
	}

	// Add to index
	s.index.Files = append(s.index.Files, FileInfo{
		Path:        filePath,
		StartOffset: s.index.CurrentWriteOffset,
		EndOffset:   s.index.CurrentWriteOffset,
		StartEntry:  s.nextEntryNumber, // Use nextEntryNumber for pending writes
		Entries:     0,
		StartTime:   time.Now(),
		EndTime:     time.Now(),
	})
	s.index.CurrentFile = filePath

	if IsDebug() && s.logger != nil {
		s.logger.Debug("openDataFileForAppend: success", "shard", s.shardID, "files", len(s.index.Files))
	}

	return nil
}

// openDataFileWithConfig opens or creates the current data file
func (s *Shard) openDataFileWithConfig(shardDir string) error {
	if IsDebug() && s.logger != nil {
		s.logger.Debug("openDataFileWithConfig: entry", "shard", s.shardID, "files", len(s.index.Files))
	}

	// Initialize compressor
	var err error
	s.compressor, err = zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedDefault),
		zstd.WithEncoderConcurrency(1))
	if err != nil {
		return fmt.Errorf("failed to create compressor: %w", err)
	}

	// Find or create data file
	if len(s.index.Files) == 0 {
		if IsDebug() && s.logger != nil {
			s.logger.Debug("openDataFileWithConfig: no files, creating first one", "shard", s.shardID)
		}
		// No files yet, create first one
		if err := s.openDataFileForAppend(shardDir); err != nil {
			return fmt.Errorf("failed to create initial data file: %w", err)
		}
		if IsDebug() && s.logger != nil {
			s.logger.Debug("openDataFileWithConfig: created first file", "shard", s.shardID, "writer_nil", s.writer == nil)
		}
		return nil
	}

	// Open last file for append
	lastFile := s.index.Files[len(s.index.Files)-1]
	s.index.CurrentFile = lastFile.Path

	if IsDebug() && s.logger != nil {
		s.logger.Debug("openDataFileWithConfig: opening existing file", "shard", s.shardID, "file", lastFile.Path)
	}

	// Open file based on write mode
	flags := os.O_WRONLY | os.O_APPEND

	file, err := os.OpenFile(lastFile.Path, flags, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			// File was deleted, create new one
			if s.logger != nil {
				s.logger.Warn("Data file missing, creating new file",
					"missing", lastFile.Path,
					"shard", s.shardID)
			}
			// Reset write offset and create new file
			s.index.CurrentWriteOffset = 0
			s.index.Files = s.index.Files[:0] // Clear files list
			if err := s.openDataFileForAppend(shardDir); err != nil {
				return fmt.Errorf("failed to create replacement data file: %w", err)
			}
			if IsDebug() && s.logger != nil {
				s.logger.Debug("openDataFileWithConfig: created replacement file", "shard", s.shardID, "writer_nil", s.writer == nil)
			}
			return nil
		}
		return fmt.Errorf("failed to open data file: %w", err)
	}

	// Create buffered writer
	bufferSize := 64 * 1024 // 64KB buffer
	writer := bufio.NewWriterSize(file, bufferSize)

	s.dataFile = file
	s.writer = writer

	if IsDebug() && s.logger != nil {
		s.logger.Debug("openDataFileWithConfig: initialized writer", "shard", s.shardID, "writer_nil", s.writer == nil)
	}

	// Verify file size matches index
	stat, err := file.Stat()
	if err == nil && stat.Size() != s.index.CurrentWriteOffset {
		if s.logger != nil {
			s.logger.Warn("File size mismatch",
				"file", lastFile.Path,
				"actual", stat.Size(),
				"expected", s.index.CurrentWriteOffset,
				"shard", s.shardID)
		}

		// If file is larger than expected, scan to count actual entries
		if stat.Size() > s.index.CurrentWriteOffset {
			// File has extra data, need to scan to update entry count
			// Open file for reading to scan entries
			readFile, err := os.Open(lastFile.Path)
			if err == nil {
				entryCount, scanErr := s.scanFileEntries(readFile)
				readFile.Close()
				if scanErr == nil {
					// Update the file info in our index
					for i := range s.index.Files {
						if s.index.Files[i].Path == lastFile.Path {
							s.index.Files[i].Entries = entryCount
							break
						}
					}
					s.index.CurrentEntryNumber = lastFile.StartEntry + entryCount
					// Also update nextEntryNumber to stay in sync
					s.nextEntryNumber = s.index.CurrentEntryNumber
					if s.logger != nil {
						s.logger.Info("Updated file entry count after crash recovery",
							"file", lastFile.Path,
							"entries", entryCount,
							"shard", s.shardID)
					}
				}
			}
		}

		// Update index to match reality
		s.index.CurrentWriteOffset = stat.Size()
		lastFile.EndOffset = stat.Size()
	}

	// Ensure writer was properly initialized
	if s.writer == nil {
		return fmt.Errorf("writer was not properly initialized")
	}

	if IsDebug() && s.logger != nil {
		s.logger.Debug("openDataFileWithConfig: success", "shard", s.shardID)
	}

	return nil
}

// loadIndex loads the shard index from disk
func (s *Shard) loadIndex() error {
	// Check if index file exists
	if _, err := os.Stat(s.indexPath); os.IsNotExist(err) {
		// Index doesn't exist yet, use defaults
		return nil
	}

	// Load using binary format
	index, err := s.loadBinaryIndexWithConfig(s.index.BoundaryInterval, int(s.index.BinaryIndex.MaxNodes))
	if err != nil {
		return fmt.Errorf("failed to load binary index: %w", err)
	}

	// Preserve certain fields that should not be overwritten
	index.BoundaryInterval = s.index.BoundaryInterval
	if index.BinaryIndex.IndexInterval == 0 {
		index.BinaryIndex.IndexInterval = s.index.BinaryIndex.IndexInterval
	}
	if index.BinaryIndex.MaxNodes == 0 {
		index.BinaryIndex.MaxNodes = s.index.BinaryIndex.MaxNodes
	}

	s.index = index

	// Validate entry numbers after loading
	if s.logger != nil && IsDebug() {
		s.logger.Debug("loadIndex: after unmarshal",
			"currentEntryNumber", s.index.CurrentEntryNumber,
			"numFiles", len(s.index.Files),
			"currentWriteOffset", s.index.CurrentWriteOffset)
	}

	return nil
}

// persistIndex saves the shard index to disk
func (s *Shard) persistIndex() error {
	// Clone the index while holding the lock to prevent concurrent modification
	s.mu.RLock()
	indexCopy := s.cloneIndex()
	state := s.state // Capture state reference while holding lock
	s.mu.RUnlock()

	// Return index to pool after use
	defer returnIndexToPool(indexCopy)

	// Use binary format for persistence
	s.indexMu.Lock()
	err := s.saveBinaryIndex(indexCopy)
	s.indexMu.Unlock()

	if err != nil {
		if state != nil {
			atomic.AddUint64(&state.IndexPersistErrors, 1)
		}
		return fmt.Errorf("failed to persist index: %w", err)
	}

	// Update mmap timestamp to signal index change to other processes
	if state != nil {
		state.SetLastIndexUpdate(time.Now().UnixNano())
		atomic.AddUint64(&state.IndexPersistCount, 1)
	}

	return nil
}

// recoverFromCrash handles crash recovery by scanning data files
func (s *Shard) recoverFromCrash() error {
	// Skip if no files
	if len(s.index.Files) == 0 {
		return nil
	}

	// Check last file
	lastFile := &s.index.Files[len(s.index.Files)-1]
	file, err := os.Open(lastFile.Path)
	if err != nil {
		if os.IsNotExist(err) {
			// File doesn't exist, remove from index
			s.index.Files = s.index.Files[:len(s.index.Files)-1]
			s.index.CurrentWriteOffset = 0
			if len(s.index.Files) > 0 {
				s.index.CurrentWriteOffset = s.index.Files[len(s.index.Files)-1].EndOffset
			}
			return nil
		}
		return err
	}
	defer file.Close()

	// Get actual file size
	stat, err := file.Stat()
	if err != nil {
		return err
	}

	actualSize := stat.Size()
	if actualSize < lastFile.EndOffset {
		// File is smaller than expected, scan to find actual entries
		if s.logger != nil {
			s.logger.Warn("File truncated, recovering",
				"file", lastFile.Path,
				"expected", lastFile.EndOffset,
				"actual", actualSize,
				"shard", s.shardID)
		}

		// Track truncation as partial write
		if state := s.state; state != nil {
			atomic.AddUint64(&state.PartialWrites, 1)
		}

		// Scan file to count entries
		entryCount, err := s.scanFileEntries(file)
		if err != nil {
			return err
		}

		// Update index
		lastFile.EndOffset = actualSize
		lastFile.Entries = entryCount
		s.index.CurrentWriteOffset = actualSize
		s.index.CurrentEntryNumber = lastFile.StartEntry + entryCount
		// Keep nextEntryNumber in sync
		s.nextEntryNumber = s.index.CurrentEntryNumber
	}

	return nil
}

// scanFileEntries counts valid entries in a file
func (s *Shard) scanFileEntries(file *os.File) (int64, error) {
	var count int64
	offset := int64(0)

	for {
		// Read header
		var header [headerSize]byte
		n, err := file.ReadAt(header[:], offset)
		if err == io.EOF {
			break
		}
		if err != nil {
			return count, err
		}
		if n < headerSize {
			// Partial header detected
			if state := s.state; state != nil {
				atomic.AddUint64(&state.PartialWrites, 1)
			}
			break
		}

		// Parse header
		size := binary.LittleEndian.Uint32(header[0:4])
		if size == 0 || size > maxEntrySize {
			// Invalid header, likely incomplete write
			if size == 0 && s.state != nil {
				// Zero size likely means partial write
				atomic.AddUint64(&s.state.PartialWrites, 1)
			}
			break // Invalid entry
		}

		// Skip to next entry
		offset += headerSize + int64(size)
		count++
	}

	return count, nil
}

// GetConsumerOffset returns the current offset for a consumer group
func (s *Shard) GetConsumerOffset(group string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.index.ConsumerOffsets[group]
}

// UpdateConsumerOffset updates the offset for a consumer group
func (s *Shard) UpdateConsumerOffset(group string, offset int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	oldOffset := s.index.ConsumerOffsets[group]
	s.index.ConsumerOffsets[group] = offset

	// Update consumer group count if this is a new group
	if oldOffset == 0 && offset > 0 {
		if state := s.state; state != nil {
			// Count unique consumer groups
			groupCount := uint64(len(s.index.ConsumerOffsets))
			atomic.StoreUint64(&state.ConsumerGroups, groupCount)
		}
	}

	// Track consumer lag
	if state := s.state; state != nil && s.index.CurrentEntryNumber > offset {
		lag := uint64(s.index.CurrentEntryNumber - offset)
		// Update max lag if this is higher
		for {
			current := atomic.LoadUint64(&state.MaxConsumerLag)
			if current >= lag {
				break
			}
			if atomic.CompareAndSwapUint64(&state.MaxConsumerLag, current, lag) {
				break
			}
		}
	}
}

// AddIndexNode adds a node to the binary searchable index
func (idx *BinarySearchableIndex) AddIndexNode(entryNumber int64, position EntryPosition) {
	// Only add at specified intervals
	if idx.IndexInterval > 0 && entryNumber%int64(idx.IndexInterval) != 0 && entryNumber != 0 {
		return
	}

	// Create new node
	node := EntryIndexNode{
		EntryNumber: entryNumber,
		Position:    position,
	}

	// Add to nodes
	idx.Nodes = append(idx.Nodes, node)

	// Prune if needed
	if idx.MaxNodes > 0 && len(idx.Nodes) > idx.MaxNodes {
		// Keep every Nth node to maintain coverage
		pruneInterval := len(idx.Nodes) / idx.MaxNodes
		if pruneInterval > 1 {
			pruned := make([]EntryIndexNode, 0, idx.MaxNodes)
			for i := 0; i < len(idx.Nodes); i += pruneInterval {
				pruned = append(pruned, idx.Nodes[i])
			}
			idx.Nodes = pruned
		}
	}
}

// FindEntryPosition uses binary search to find the position of an entry
func (idx *BinarySearchableIndex) FindEntryPosition(targetEntry int64) (EntryPosition, bool) {
	if len(idx.Nodes) == 0 {
		return EntryPosition{}, false
	}

	// Binary search for the highest node <= targetEntry
	left, right := 0, len(idx.Nodes)-1
	result := -1

	for left <= right {
		mid := (left + right) / 2
		if idx.Nodes[mid].EntryNumber <= targetEntry {
			result = mid
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	if result == -1 {
		return EntryPosition{}, false
	}

	// Return the position of the found node
	// The caller will need to scan forward from this position
	return idx.Nodes[result].Position, true
}

// parseShardFromStream extracts shard ID from stream name
func parseShardFromStream(stream string) (uint32, error) {
	// Expected formats:
	// New format: "prefix:NNNN" (4-digit decimal)
	// Legacy format: "name:version:shard:NNNN" (4-digit decimal)

	// Find the last colon
	lastColon := strings.LastIndex(stream, ":")
	if lastColon == -1 || lastColon == len(stream)-1 {
		return 0, fmt.Errorf("invalid stream format, expected prefix:NNNN or name:version:shard:NNNN")
	}

	shardStr := stream[lastColon+1:]

	// Check if this might be legacy format by looking for "shard:" before the last colon
	if lastColon >= 6 && stream[lastColon-6:lastColon] == ":shard" {
		// Legacy format: parse as decimal
		shard, err := strconv.ParseUint(shardStr, 10, 32)
		if err != nil {
			return 0, fmt.Errorf("invalid shard number: %s", shardStr)
		}
		return uint32(shard), nil
	}

	// New format: parse as decimal (4-digit)
	if len(shardStr) != 4 {
		return 0, fmt.Errorf("invalid shard format, expected 4-digit decimal value")
	}

	shard, err := strconv.ParseUint(shardStr, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid shard number: %s", shardStr)
	}

	return uint32(shard), nil
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
		// Stop periodic flush first before acquiring any locks
		if shard.stopFlush != nil {
			close(shard.stopFlush)
		}

		// Wait for background operations to complete BEFORE acquiring locks
		shard.wg.Wait()

		shard.mu.Lock()

		// Final checkpoint - do it directly since we already hold the lock
		// Always checkpoint on close to ensure index is persisted
		if shard.shouldCheckpoint(&c.config) || true { // Force checkpoint on close
			// Track checkpoint timing
			checkpointStart := time.Now()

			// Clone index while holding lock
			indexCopy := shard.cloneIndex()
			shard.writesSinceCheckpoint = 0
			shard.lastCheckpoint = time.Now()

			// Persist index outside lock
			shard.mu.Unlock() // Release for the persistence operation

			// Return index to pool after use
			defer returnIndexToPool(indexCopy)

			shard.indexMu.Lock()
			err := shard.saveBinaryIndex(indexCopy)
			shard.indexMu.Unlock()
			shard.mu.Lock() // Re-acquire for cleanup

			if err != nil {
				c.metrics.IndexPersistErrors.Add(1)
				c.metrics.ErrorCount.Add(1)
			} else {
				c.metrics.CheckpointsWritten.Add(1)
				// Track checkpoint metrics in state
				if state := shard.state; state != nil {
					atomic.AddUint64(&state.CheckpointCount, 1)
					atomic.StoreInt64(&state.LastCheckpointNanos, time.Now().UnixNano())
					checkpointDuration := time.Since(checkpointStart).Nanoseconds()
					atomic.AddInt64(&state.CheckpointTimeNanos, checkpointDuration)
					
					// CRITICAL: Update LastIndexUpdate so consumers know to reload
					state.SetLastIndexUpdate(time.Now().UnixNano())
				}
			}
		}

		// Acquire write lock to ensure no writes are in progress
		shard.writeMu.Lock()

		// Close direct writer and sync to disk
		if shard.writer != nil {
			shard.writer.Flush()
		}

		// Ensure data is persisted to disk before closing
		if shard.dataFile != nil {
			syncStart := time.Now()
			if err := shard.dataFile.Sync(); err == nil && shard.state != nil {
				// Track sync latency during close
				syncDuration := time.Since(syncStart).Nanoseconds()
				atomic.AddInt64(&shard.state.SyncLatencyNanos, syncDuration)
				atomic.AddUint64(&shard.state.SyncCount, 1)
			}
		}

		// Close compressor
		if shard.compressor != nil {
			shard.compressor.Close()
		}

		// Close file (safe now that we hold writeMu)
		if shard.dataFile != nil {
			shard.dataFile.Close()
		}

		shard.writeMu.Unlock()
		shard.mu.Unlock()

		// Lock files removed - processes own their shards exclusively

		// Now safe to unmap unified state
		if shard.stateData != nil {
			// The memory-mapped state is MAP_SHARED, so changes are automatically
			// written back to the file. We don't need explicit msync on most systems.
			// The OS will ensure data is written when we unmap or when the file is closed.
			syscall.Munmap(shard.stateData)
			shard.stateData = nil
			shard.state = nil
		}
	}

	// Release process ID if this client was created with NewMultiProcessClient
	if c.sharedMemoryFile != "" {
		ReleaseProcessID(c.sharedMemoryFile)
	} else if c.config.Concurrency.IsMultiProcess() {
		// Check if this might be an auto-managed process ID
		ReleaseProcessID()
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

// CometHealth represents the health status of the client
type CometHealth struct {
	Healthy       bool      `json:"healthy"`
	Status        string    `json:"status"`
	ActiveShards  int       `json:"active_shards"`
	Details       string    `json:"details"`
	Uptime        int64     `json:"uptime_seconds"`
	WritesOK      bool      `json:"writes_ok"`
	ReadsOK       bool      `json:"reads_ok"`
	LastWriteTime time.Time `json:"last_write_time"`
	ErrorCount    int64     `json:"error_count"`
}

// Health returns basic health status
func (c *Client) Health() CometHealth {
	c.mu.RLock()
	defer c.mu.RUnlock()

	healthy := true
	status := "healthy"
	details := "Operating normally"
	writesOK := true
	readsOK := true

	if len(c.shards) == 0 {
		details = "No data written yet"
	}

	// Get error count and last write time
	errorCount := c.metrics.WriteErrors.Load()
	writeErrors := c.metrics.WriteErrors.Load()
	readErrors := c.metrics.ReadErrors.Load()

	// Check if writes/reads are OK based on recent error rates
	if writeErrors > 0 {
		writesOK = false
	}
	if readErrors > 0 {
		readsOK = false
	}

	// Get last write time (approximate from start time + total writes)
	lastWriteTime := c.startTime
	if c.metrics.WritesTotal.Load() > 0 {
		lastWriteTime = time.Now() // Approximate - we don't track exact last write time
	}

	// Check for recent errors
	if lastError := c.metrics.LastErrorNano.Load(); lastError > 0 {
		errorAge := time.Since(time.Unix(0, lastError)).Seconds()
		if errorAge < 60 { // Errors in last minute affect health
			healthy = false
			status = "unhealthy"
			details = fmt.Sprintf("Recent error %d seconds ago", int(errorAge))
		}
	}

	return CometHealth{
		Healthy:       healthy,
		Status:        status,
		ActiveShards:  len(c.shards),
		Details:       details,
		Uptime:        int64(time.Since(c.startTime).Seconds()),
		WritesOK:      writesOK,
		ReadsOK:       readsOK,
		LastWriteTime: lastWriteTime,
		ErrorCount:    int64(errorCount),
	}
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

		// Calculate max lag
		for group, offset := range shard.index.ConsumerOffsets {
			lag := shard.index.CurrentEntryNumber - offset
			if lag > 0 && uint64(lag) > maxLag {
				maxLag = uint64(lag)
			}
			_ = group // avoid unused variable warning
		}
		shard.mu.RUnlock()
	}
	c.mu.RUnlock()

	uptime := time.Since(c.startTime).Seconds()

	// Calculate write throughput
	bytesWritten := c.metrics.BytesWritten.Load()
	writeThroughput := float64(bytesWritten) / uptime / (1024 * 1024) // MB/s

	// Calculate compression ratio
	compressionSaves := c.metrics.CompressionSaves.Load()
	compressionRatio := 1.0
	if bytesWritten > 0 {
		compressionRatio = float64(bytesWritten+compressionSaves) / float64(bytesWritten)
	}

	// Aggregate compression and rotation stats from shard states
	var totalCompressed, compressedEntries, skippedCompression, fileRotations, compressionWaitNano int64
	c.mu.RLock()
	for _, shard := range c.shards {
		if shard.state != nil {
			totalCompressed += int64(atomic.LoadUint64(&shard.state.TotalCompressed))
			compressedEntries += int64(atomic.LoadUint64(&shard.state.CompressedEntries))
			skippedCompression += int64(atomic.LoadUint64(&shard.state.SkippedCompression))
			fileRotations += int64(atomic.LoadUint64(&shard.state.FileRotations))
			compressionWaitNano += atomic.LoadInt64(&shard.state.CompressionTimeNanos)
		}
	}
	c.mu.RUnlock()

	return CometStats{
		TotalEntries:        int64(c.metrics.WritesTotal.Load()),
		TotalBytes:          int64(bytesWritten),
		TotalCompressed:     totalCompressed,
		CompressedEntries:   compressedEntries,
		SkippedCompression:  skippedCompression,
		FileRotations:       fileRotations,
		CompressionWaitNano: compressionWaitNano,
		WriteThroughput:     writeThroughput,
		CompressionRatio:    compressionRatio,
		OpenReaders:         int64(totalReaders),
		MaxLag:              int64(maxLag),
		FileCount:           int(totalFiles),
		UptimeSeconds:       int64(uptime),
	}
}

// Smart Sharding helper functions

// PickShard selects a shard ID based on consistent hashing of the key
func (c *Client) PickShard(key string, shardCount uint32) uint32 {
	if shardCount == 0 {
		shardCount = 16
	}

	// Use FNV-1a hash for consistent distribution
	h := uint32(2166136261) // FNV offset basis
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619 // FNV prime
	}

	return h % shardCount
}

// getOwnedShards returns the list of shard IDs owned by this process for the given total shard count
func (c *Client) getOwnedShards(shardCount uint32) []uint32 {
	// Check cache first
	if cached, ok := c.ownedShardsCache.Load(shardCount); ok {
		return cached.([]uint32)
	}

	// Build the list of owned shards
	ownedShards := make([]uint32, 0, shardCount/uint32(c.config.Concurrency.ProcessCount)+1)
	for i := uint32(0); i < shardCount; i++ {
		if c.config.Concurrency.Owns(i) {
			ownedShards = append(ownedShards, i)
		}
	}

	// Store in cache
	c.ownedShardsCache.Store(shardCount, ownedShards)
	return ownedShards
}

// PickShardStream returns a complete stream name for the shard picked by key
// Example: client.PickShardStream("events:v1", "user123", 256) returns "events:v1:0255"
// In multi-process mode, this will only pick from shards owned by this client
func (c *Client) PickShardStream(prefix string, key string, shardCount uint32) string {
	if shardCount == 0 {
		shardCount = 16
	}

	// In multi-process mode, only pick from owned shards
	if c.config.Concurrency.IsMultiProcess() {
		ownedShards := c.getOwnedShards(shardCount)
		if len(ownedShards) == 0 {
			panic("no shards owned by this process")
		}

		// Hash the key and pick from owned shards
		h := uint32(2166136261) // FNV offset basis
		for i := 0; i < len(key); i++ {
			h ^= uint32(key[i])
			h *= 16777619 // FNV prime
		}

		shardID := ownedShards[h%uint32(len(ownedShards))]
		return ShardStreamName(prefix, shardID)
	}

	// Single-process mode: use regular shard picking
	shardID := c.PickShard(key, shardCount)
	return ShardStreamName(prefix, shardID)
}

// ShardStreamName constructs a stream name from prefix and shard ID
// Example: ShardStreamName("events:v1", 255) returns "events:v1:0011"
func ShardStreamName(prefix string, shardID uint32) string {
	return fmt.Sprintf("%s:%04d", prefix, shardID)
}

// AllShardsRange returns a slice containing all shard IDs from 0 to shardCount-1
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

// AllShardStreams returns all stream names for the given prefix and shard count
func AllShardStreams(prefix string, shardCount uint32) []string {
	if shardCount == 0 {
		shardCount = defaultShardCount
	}
	streams := make([]string, shardCount)
	for i := uint32(0); i < shardCount; i++ {
		streams[i] = ShardStreamName(prefix, i)
	}
	return streams
}

// Default shard count for tests
const defaultShardCount = uint32(16)

// GetShardStats returns detailed statistics for a specific shard
func (c *Client) GetShardStats(shardID uint32) (map[string]any, error) {
	c.mu.RLock()
	shard, exists := c.shards[shardID]
	c.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("shard %d not found", shardID)
	}

	stats := make(map[string]any)

	// Reader count
	stats["readers"] = atomic.LoadInt64(&shard.readerCount)

	// Index stats
	shard.mu.RLock()
	stats["entries"] = shard.index.CurrentEntryNumber
	stats["bytes"] = shard.index.CurrentWriteOffset
	stats["files"] = len(shard.index.Files)
	stats["consumer_groups"] = len(shard.index.ConsumerOffsets)
	stats["binary_index_nodes"] = len(shard.index.BinaryIndex.Nodes)

	// File details
	files := make([]map[string]any, len(shard.index.Files))
	for i, f := range shard.index.Files {
		files[i] = map[string]any{
			"path":        f.Path,
			"size":        f.EndOffset - f.StartOffset,
			"entries":     f.Entries,
			"start_entry": f.StartEntry,
			"created":     f.StartTime,
			"modified":    f.EndTime,
		}
	}
	stats["file_details"] = files

	// Consumer group details
	groups := make(map[string]any)
	for group, offset := range shard.index.ConsumerOffsets {
		lag := shard.index.CurrentEntryNumber - offset
		groups[group] = map[string]any{
			"offset": offset,
			"lag":    lag,
		}
	}
	stats["consumer_details"] = groups
	shard.mu.RUnlock()

	// Add state metrics if available
	if state := shard.state; state != nil {
		stateMetrics := make(map[string]any)

		// Basic metrics
		stateMetrics["total_writes"] = atomic.LoadUint64(&state.TotalWrites)
		stateMetrics["total_bytes"] = atomic.LoadUint64(&state.TotalBytes)
		stateMetrics["write_offset"] = atomic.LoadUint64(&state.WriteOffset)

		// Compression metrics
		stateMetrics["compressed_entries"] = atomic.LoadUint64(&state.CompressedEntries)
		stateMetrics["skipped_compression"] = atomic.LoadUint64(&state.SkippedCompression)
		stateMetrics["compression_ratio_bp"] = atomic.LoadUint64(&state.CompressionRatio) // basis points

		// Latency metrics
		count := atomic.LoadUint64(&state.WriteLatencyCount)
		if count > 0 {
			sum := atomic.LoadUint64(&state.WriteLatencySum)
			stateMetrics["avg_write_latency_us"] = float64(sum/count) / 1000
			stateMetrics["min_write_latency_us"] = float64(atomic.LoadUint64(&state.MinWriteLatency)) / 1000
			stateMetrics["max_write_latency_us"] = float64(atomic.LoadUint64(&state.MaxWriteLatency)) / 1000
		}

		// File metrics
		stateMetrics["file_rotations"] = atomic.LoadUint64(&state.FileRotations)
		stateMetrics["files_created"] = atomic.LoadUint64(&state.FilesCreated)

		// Error metrics
		stateMetrics["error_count"] = atomic.LoadUint64(&state.ErrorCount)
		stateMetrics["failed_writes"] = atomic.LoadUint64(&state.FailedWrites)
		stateMetrics["read_errors"] = atomic.LoadUint64(&state.ReadErrors)

		// Recovery metrics
		stateMetrics["recovery_attempts"] = atomic.LoadUint64(&state.RecoveryAttempts)
		stateMetrics["recovery_successes"] = atomic.LoadUint64(&state.RecoverySuccesses)

		stats["state_metrics"] = stateMetrics
	}

	return stats, nil
}

// ListRecent returns the N most recent messages from a stream
// This is a browse operation that doesn't affect consumer offsets
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

	// Create a reader for direct access
	shard.mu.RLock()
	if shard.index == nil {
		shard.mu.RUnlock()
		return nil, fmt.Errorf("shard index not initialized")
	}
	indexCopy := shard.cloneIndex() // Create proper deep copy to avoid race conditions
	totalEntries := shard.index.CurrentEntryNumber
	shard.mu.RUnlock()

	// Return index to pool after reader is done with it
	defer returnIndexToPool(indexCopy)

	if totalEntries == 0 {
		return nil, nil
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

	// Read messages
	messages := make([]StreamMessage, 0, limit)
	for i := startFrom; i < totalEntries && len(messages) < limit; i++ {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return messages, ctx.Err()
		default:
		}

		entry, err := reader.ReadEntryByNumber(i)
		if err != nil {
			// Log error but continue
			if c.logger != nil {
				c.logger.WithFields(
					"entryNumber", i,
					"error", err,
				).Debug("Failed to read entry in ListRecent")
			}
			continue
		}

		// Convert to StreamMessage
		msg := StreamMessage{
			ID: MessageID{
				ShardID:     shardID,
				EntryNumber: i,
			},
			Data:   entry, // entry is []byte from ReadEntryByNumber
			Stream: streamName,
		}
		messages = append(messages, msg)
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
	shard.mu.Lock()
	if err := shard.loadIndex(); err != nil {
		shard.mu.Unlock()
		return fmt.Errorf("failed to reload index: %w", err)
	}
	shard.mu.Unlock()

	// Create a reader for direct access
	shard.mu.RLock()
	if shard.index == nil {
		shard.mu.RUnlock()
		return fmt.Errorf("shard index not initialized")
	}
	indexCopy := shard.cloneIndex() // Create proper deep copy to avoid race conditions
	totalEntries := shard.index.CurrentEntryNumber
	shard.mu.RUnlock()

	// Return index to pool after reader is done with it
	defer returnIndexToPool(indexCopy)

	if totalEntries == 0 {
		return nil
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

		// Convert to StreamMessage
		msg := StreamMessage{
			ID: MessageID{
				ShardID:     shardID,
				EntryNumber: i,
			},
			Data:   entry, // entry is []byte from ReadEntryByNumber
			Stream: streamName,
		}

		// Call user function
		if !fn(ctx, msg) {
			break // User requested stop
		}
	}

	return nil
}

// checkIfRebuildNeeded checks if the index needs to be rebuilt
// Returns true if there's a significant mismatch between state and index
func (s *Shard) checkIfRebuildNeeded() bool {
	if s.state == nil {
		return false
	}

	stateLastEntry := atomic.LoadInt64(&s.state.LastEntryNumber)
	indexCurrentEntry := s.index.CurrentEntryNumber

	// If state shows we have entries but index is empty, rebuild needed
	if stateLastEntry >= 0 && indexCurrentEntry == 0 && len(s.index.Files) == 0 {
		if s.logger != nil {
			s.logger.Warn("Index rebuild needed: state shows entries but index is empty",
				"shard", s.shardID,
				"stateLastEntry", stateLastEntry,
				"indexCurrentEntry", indexCurrentEntry)
		}
		return true
	}

	// If there's a significant discrepancy (more than 1000 entries), rebuild
	// Note: stateLastEntry is the last written entry (0-based)
	// indexCurrentEntry is the next entry to write
	// So normally: indexCurrentEntry = stateLastEntry + 1
	expectedCurrent := stateLastEntry + 1
	if stateLastEntry >= 0 && abs(indexCurrentEntry-expectedCurrent) > 1000 {
		if s.logger != nil {
			s.logger.Warn("Index rebuild needed: significant entry count mismatch",
				"shard", s.shardID,
				"stateLastEntry", stateLastEntry,
				"indexCurrentEntry", indexCurrentEntry,
				"expectedCurrent", expectedCurrent)
		}
		return true
	}

	return false
}

// lazyRebuildIndexIfNeeded rebuilds the index by scanning data files
// This is called lazily when we detect the index is out of sync
func (s *Shard) lazyRebuildIndexIfNeeded(config CometConfig, shardDir string) error {
	// Double-check under lock
	if !s.checkIfRebuildNeeded() {
		return nil
	}

	return s.doRebuildIndex(config, shardDir)
}

// doRebuildIndex performs the actual index rebuild without checking conditions
func (s *Shard) doRebuildIndex(config CometConfig, shardDir string) error {
	if s.logger != nil {
		s.logger.Info("Rebuilding index from data files",
			"shard", s.shardID,
			"dir", shardDir)
	}

	// Track rebuild attempt
	if state := s.state; state != nil {
		atomic.AddUint64(&state.RecoveryAttempts, 1)
	}

	// List all data files
	files, err := filepath.Glob(filepath.Join(shardDir, "log-*.comet"))
	if err != nil {
		return fmt.Errorf("failed to list data files: %w", err)
	}

	if len(files) == 0 {
		// No data files but state shows entries - data loss
		if s.logger != nil {
			s.logger.Error("No data files found but state shows entries",
				"shard", s.shardID,
				"stateLastEntry", atomic.LoadInt64(&s.state.LastEntryNumber))
		}
		// Reset state to match reality
		atomic.StoreInt64(&s.state.LastEntryNumber, -1)
		return nil
	}

	// Sort files by name (which includes timestamp)
	sort.Strings(files)

	// Rebuild index
	newIndex := &ShardIndex{
		CurrentEntryNumber: 0,
		CurrentWriteOffset: 0,
		BoundaryInterval:   s.index.BoundaryInterval,
		ConsumerOffsets:    make(map[string]int64),
		Files:              make([]FileInfo, 0),
		BinaryIndex: BinarySearchableIndex{
			IndexInterval: s.index.BinaryIndex.IndexInterval,
			MaxNodes:      s.index.BinaryIndex.MaxNodes,
			Nodes:         make([]EntryIndexNode, 0),
		},
	}

	// Preserve consumer offsets
	maps.Copy(newIndex.ConsumerOffsets, s.index.ConsumerOffsets)

	totalEntries := int64(0)
	totalBytes := int64(0)

	// Scan each file
	for _, filePath := range files {
		fileInfo, err := scanDataFile(filePath, totalEntries, 0, newIndex.BoundaryInterval)
		if err != nil {
			if s.logger != nil {
				s.logger.Error("Failed to scan data file",
					"file", filePath,
					"error", err)
			}
			continue
		}

		if fileInfo.Entries > 0 {
			// Adjust offsets to be global instead of file-relative
			fileInfo.StartOffset = totalBytes
			fileInfo.EndOffset = totalBytes + (fileInfo.EndOffset - 0) // EndOffset from scan is file size

			newIndex.Files = append(newIndex.Files, fileInfo)
			totalEntries += fileInfo.Entries
			totalBytes = fileInfo.EndOffset

			// Rebuild binary index with accurate positions by scanning the file
			fileIndex := len(newIndex.Files) - 1
			if err := s.rebuildBinaryIndexForFile(fileIndex, fileInfo, newIndex); err != nil {
				if s.logger != nil {
					s.logger.Warn("Failed to rebuild binary index for file",
						"file", fileInfo.Path,
						"error", err)
				}
			}
		} else if s.logger != nil {
			s.logger.Warn("Scanned file with 0 entries",
				"file", filePath,
				"size", fileInfo.EndOffset)
		}
	}

	// Update index
	newIndex.CurrentEntryNumber = totalEntries
	newIndex.CurrentWriteOffset = totalBytes
	if len(newIndex.Files) > 0 {
		newIndex.CurrentFile = newIndex.Files[len(newIndex.Files)-1].Path
	}

	// Replace index under write lock
	s.mu.Lock()
	s.index = newIndex
	s.mu.Unlock()

	// Update state to match
	if s.state != nil && totalEntries > 0 {
		atomic.StoreInt64(&s.state.LastEntryNumber, totalEntries-1)
		atomic.StoreUint64(&s.state.WriteOffset, uint64(totalBytes))
		atomic.AddUint64(&s.state.RecoverySuccesses, 1)
	}

	// Persist rebuilt index (this will take its own read lock as needed)
	if err := s.persistIndex(); err != nil {
		if s.logger != nil {
			s.logger.Error("Failed to persist rebuilt index",
				"error", err)
		}
	}

	if s.logger != nil {
		s.logger.Info("Index rebuild complete",
			"shard", s.shardID,
			"files", len(newIndex.Files),
			"entries", totalEntries,
			"bytes", totalBytes)
	}

	return nil
}

// rebuildBinaryIndexForFile rebuilds binary index entries for a single file by scanning it
func (s *Shard) rebuildBinaryIndexForFile(fileIndex int, fileInfo FileInfo, index *ShardIndex) error {
	// Open the file for scanning
	file, err := os.Open(fileInfo.Path)
	if err != nil {
		return fmt.Errorf("failed to open file for binary index rebuild: %w", err)
	}
	defer file.Close()

	// Get file size
	stat, err := file.Stat()
	if err != nil {
		return err
	}

	if stat.Size() == 0 {
		// Empty file, nothing to index
		return nil
	}

	// Read the entire file into memory for scanning
	data := make([]byte, stat.Size())
	if _, err := file.Read(data); err != nil {
		return fmt.Errorf("failed to read file for scanning: %w", err)
	}

	// Scan through the file to find entry positions
	offset := int64(0)
	entryCount := int64(0)

	for offset < int64(len(data)) {
		// Check if we have enough data for a header
		if offset+12 > int64(len(data)) {
			break // Incomplete entry at end of file
		}

		// Read entry header
		length := binary.LittleEndian.Uint32(data[offset : offset+4])

		// Validate the length is reasonable
		if length > 10*1024*1024 { // 10MB max entry size
			break // Likely corrupted data
		}

		// Calculate entry number for this entry
		entryNum := fileInfo.StartEntry + entryCount

		// Add to binary index if this entry is at a boundary
		if entryNum%int64(index.BinaryIndex.IndexInterval) == 0 {
			pos := EntryPosition{
				FileIndex:  fileIndex,
				ByteOffset: offset,
			}
			index.BinaryIndex.AddIndexNode(entryNum, pos)
		}

		// Move to next entry
		entrySize := 12 + int64(length) // header + data
		offset += entrySize
		entryCount++

		// Safety check to prevent infinite loops
		if offset > int64(len(data)) {
			break
		}
	}

	return nil
}

// scanDataFile scans a single data file and returns its metadata
func scanDataFile(filePath string, startEntry, startOffset int64, boundaryInterval int) (FileInfo, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return FileInfo{}, err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return FileInfo{}, err
	}

	// Debug logging
	if IsDebug() {
		fmt.Printf("scanDataFile: scanning %s (size=%d, startEntry=%d, startOffset=%d)\n",
			filePath, stat.Size(), startEntry, startOffset)
	}

	info := FileInfo{
		Path:        filePath,
		StartOffset: startOffset,
		EndOffset:   startOffset,
		StartEntry:  startEntry,
		Entries:     0,
		StartTime:   stat.ModTime(), // Best approximation
		EndTime:     stat.ModTime(),
	}

	offset := int64(0)
	for offset < stat.Size() {
		// Read header
		var header [headerSize]byte
		n, err := file.ReadAt(header[:], offset)
		if err == io.EOF || n < headerSize {
			break
		}
		if err != nil {
			return info, err
		}

		// Parse header
		size := binary.LittleEndian.Uint32(header[0:4])
		if size == 0 || size > maxEntrySize {
			// Log the issue if we have partial data
			if info.Entries > 0 || offset > 0 {
				// File might be truncated or corrupted after this point
				// Continue with what we have
			}
			break // Invalid entry, stop scanning
		}

		// Update offset and count
		offset += headerSize + int64(size)
		info.Entries++
	}

	info.EndOffset = startOffset + offset
	return info, nil
}

// Helper function for absolute value
func abs(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}

// loadIndexWithRecovery attempts to load the index with automatic recovery
func (s *Shard) loadIndexWithRecovery() error {
	// Ensure shard directory exists first
	shardDir := filepath.Dir(s.indexPath)
	if err := os.MkdirAll(shardDir, 0755); err != nil {
		return fmt.Errorf("failed to create shard directory: %w", err)
	}

	// First try to load normally
	err := s.loadIndex()
	if err == nil {
		// Check if we need to rebuild despite successful load
		// This handles the case where index is missing but data files exist
		dataFiles, _ := filepath.Glob(filepath.Join(shardDir, "log-*.comet"))
		if len(dataFiles) > 0 && len(s.index.Files) == 0 {
			// We have data files but no index - rebuild needed
			if s.logger != nil {
				s.logger.Info("Index is empty but data files exist, rebuilding",
					"shard", s.shardID,
					"dataFiles", len(dataFiles))
			}
			// Fall through to rebuild logic
		} else {
			return nil
		}
	} else if os.IsNotExist(err) {
		// If it's just a missing file and no data files, that's OK for a new shard
		dataFiles, _ := filepath.Glob(filepath.Join(shardDir, "log-*.comet"))
		if len(dataFiles) == 0 {
			return nil
		}
		// Fall through to rebuild if we have data files
	}

	// Log the error
	if s.logger != nil {
		s.logger.Warn("Failed to load index, attempting recovery",
			"shard", s.shardID,
			"error", err)
	}

	// Track recovery attempt
	if state := s.state; state != nil {
		atomic.AddUint64(&state.RecoveryAttempts, 1)
	}

	// Try to rebuild from data files
	if err := s.doRebuildIndex(CometConfig{
		Indexing: IndexingConfig{
			BoundaryInterval: s.index.BoundaryInterval,
		},
	}, shardDir); err != nil {
		return fmt.Errorf("failed to rebuild index: %w", err)
	}

	// Track successful recovery
	if state := s.state; state != nil {
		atomic.AddUint64(&state.RecoverySuccesses, 1)
	}

	return nil
}

// initCometState initializes the unified state structure
func (s *Shard) initCometState() error {
	// Always use mmap state for consistency and entry number persistence
	return s.initCometStateMmap()
}

// initCometStateMmap initializes memory-mapped state for multi-process mode
func (s *Shard) initCometStateMmap() error {
	processID := os.Getpid()

	// Check if state file exists
	_, err := os.Stat(s.statePath)
	isNewFile := os.IsNotExist(err)

	if !isNewFile {
		// Open existing file
		file, err := os.OpenFile(s.statePath, os.O_RDWR, 0644)
		if err != nil {
			return fmt.Errorf("failed to open state file: %w", err)
		}
		defer file.Close()

		// Check size
		stat, err := file.Stat()
		if err != nil {
			return fmt.Errorf("failed to stat state file: %w", err)
		}

		if stat.Size() != CometStateSize {
			// Size mismatch - recreate
			if s.logger != nil {
				s.logger.Warn("State file size mismatch, recreating",
					"expected", CometStateSize,
					"actual", stat.Size())
			}
			file.Close()
			os.Remove(s.statePath)
			isNewFile = true
		} else {
			// Map existing file
			data, err := syscall.Mmap(int(file.Fd()), 0, CometStateSize,
				syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
			if err != nil {
				return fmt.Errorf("failed to mmap state file: %w", err)
			}

			s.stateData = data
			s.state = (*CometState)(unsafe.Pointer(&data[0]))
		}
	}

	if isNewFile {
		// Create new file
		file, err := os.OpenFile(s.statePath, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0644)
		if err != nil {
			return fmt.Errorf("failed to create state file: %w", err)
		}
		defer file.Close()

		// Extend to required size
		if err := file.Truncate(CometStateSize); err != nil {
			return fmt.Errorf("failed to set state file size: %w", err)
		}

		// Map it
		data, err := syscall.Mmap(int(file.Fd()), 0, CometStateSize,
			syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
		if err != nil {
			return fmt.Errorf("failed to mmap new state file: %w", err)
		}

		s.stateData = data
		s.state = (*CometState)(unsafe.Pointer(&data[0]))

		// Initialize new state
		if s.logger != nil {
			s.logger.Debug("initCometStateMmap: initializing new state file",
				"shardID", s.shardID,
				"pid", processID)
		}
		atomic.StoreUint64(&s.state.Version, CometStateVersion1)
		// Initialize with -1 to indicate "not yet set" for LastEntryNumber
		// NOTE: This -1 is different from index.CurrentEntryNumber which starts at 0.
		// The synchronization code in getOrCreateShard handles this difference.
		// -1 means "no entries allocated yet", while after first write it becomes 0, 1, 2...
		atomic.StoreInt64(&s.state.LastEntryNumber, -1)

		if s.logger != nil {
			s.logger.Debug("initCometStateMmap: after initialization",
				"shardID", s.shardID,
				"pid", processID,
				"version", atomic.LoadUint64(&s.state.Version),
				"lastEntryNumber", atomic.LoadInt64(&s.state.LastEntryNumber))
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

// startPeriodicFlush starts a goroutine that periodically flushes buffered writes
func (s *Shard) startPeriodicFlush(config *CometConfig) {
	// Determine flush interval
	flushInterval := config.Storage.FlushInterval
	if flushInterval <= 0 {
		// Default to checkpoint interval if not specified
		flushInterval = config.Storage.CheckpointInterval
	}
	if flushInterval <= 0 {
		// No periodic flush needed
		return
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		ticker := time.NewTicker(time.Duration(flushInterval) * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// Perform both data flush AND index update to make data visible to consumers
				s.mu.Lock()

				// Flush buffered writes first
				if s.writer != nil {
					s.writeMu.Lock()
					if err := s.writer.Flush(); err != nil {
						s.writeMu.Unlock()
						s.mu.Unlock()
						if s.logger != nil {
							s.logger.Error("Periodic flush failed",
								"shard", s.shardID,
								"error", err)
						}
						continue
					}
					s.writeMu.Unlock()
				}

				// Critical: Update index to make flushed data visible to consumers
				// This is what was missing - periodic flush wasn't updating the index!
				oldCurrentEntry := s.index.CurrentEntryNumber
				s.index.CurrentEntryNumber = s.nextEntryNumber

				// Update offsets to reflect actual file state
				if s.dataFile != nil {
					if stat, err := s.dataFile.Stat(); err == nil {
						actualSize := stat.Size()
						s.index.CurrentWriteOffset = actualSize
						s.pendingWriteOffset = actualSize

						// Update current file metadata
						if len(s.index.Files) > 0 {
							current := &s.index.Files[len(s.index.Files)-1]
							current.EndOffset = s.index.CurrentWriteOffset
							current.EndTime = time.Now()
							current.Entries = s.index.CurrentEntryNumber - current.StartEntry
						}
					}
				}

				// Update memory-mapped state to notify other processes
				s.updateMmapState()

				// Capture the current entry count while holding the lock to avoid race conditions
				currentEntryNumber := s.index.CurrentEntryNumber

				s.mu.Unlock()

				// Persist index if entries were made durable
				if currentEntryNumber > oldCurrentEntry {
					if err := s.persistIndex(); err != nil {
						if s.logger != nil {
							s.logger.Error("Periodic index persistence failed",
								"shard", s.shardID,
								"error", err)
						}
					}
				}

			case <-s.stopFlush:
				// Shutdown requested
				return
			}
		}
	}()
}

// Test helper methods

// scanFileForEntries is a test helper that scans a file for entry information
func (s *Shard) scanFileForEntries(filePath string, fileSize int64, startOffset int64, maxEntries int64) struct {
	entryCount int64
	lastEntry  int64
	endOffset  int64
	indexNodes []EntryIndexNode
} {
	result := struct {
		entryCount int64
		lastEntry  int64
		endOffset  int64
		indexNodes []EntryIndexNode
	}{}

	file, err := os.Open(filePath)
	if err != nil {
		return result
	}
	defer file.Close()

	offset := startOffset
	entryCount := int64(0)

	for offset < fileSize && (maxEntries == 0 || entryCount < maxEntries) {
		var header [headerSize]byte
		n, err := file.ReadAt(header[:], offset)
		if err != nil || n < headerSize {
			break
		}

		size := binary.LittleEndian.Uint32(header[0:4])
		if size == 0 || int64(size) > maxEntrySize {
			break
		}

		// Create index node every 10 entries for testing
		if entryCount%10 == 0 {
			node := EntryIndexNode{
				EntryNumber: entryCount,
				Position: EntryPosition{
					FileIndex:  0,
					ByteOffset: offset,
				},
			}
			result.indexNodes = append(result.indexNodes, node)
		}

		entryCount++
		result.lastEntry = entryCount - 1
		offset += headerSize + int64(size)
	}

	result.entryCount = entryCount
	result.endOffset = offset
	return result
}

// ensureWriter is a test helper that ensures the writer is properly initialized
func (s *Shard) ensureWriter() error {
	// This is called during recovery - just ensure we have a valid writer
	if s.writer == nil {
		// Create a new buffered writer if needed
		if s.dataFile != nil {
			s.writer = bufio.NewWriter(s.dataFile)
		}
	}
	return nil
}
