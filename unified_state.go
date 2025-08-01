package comet

import (
	"fmt"
	"sync/atomic"
	"unsafe"
)

// UnifiedState consolidates ALL mmap state and comprehensive metrics
// Stored in: comet.state (1KB file per shard)
// Total size: 1024 bytes (1KB) for plenty of room to grow
//
// Design principles:
// 1. Group related metrics in same cache line
// 2. Hot path metrics in early cache lines
// 3. Generous reserved space for future additions
// 4. All fields are atomic-safe for multi-process access
// 5. CRITICAL: Use raw int64/uint64 fields with atomic operations, NOT atomic.Int64
type UnifiedState struct {
	// ======== Header (0-63): Version and core state ========
	Version          uint64  // 0-7:   Format version (start with 1)
	WriteOffset      uint64  // 8-15:  Current write position in active file
	LastEntryNumber  int64   // 16-23: Sequence counter for entry IDs
	LastIndexUpdate  int64   // 24-31: Timestamp of last index modification
	ActiveFileIndex  uint64  // 32-39: Current file being written to
	FileSize         uint64  // 40-47: Current size of active file
	LastFileSequence uint64  // 48-55: Sequence counter for file naming
	_pad0            [8]byte // 56-63: Padding

	// ======== Cache Line 1 (64-127): Write metrics (hot path) ========
	TotalEntries     int64   // 64-71:  Total entries written
	TotalBytes       uint64  // 72-79:  Total uncompressed bytes
	TotalWrites      uint64  // 80-87:  Total write operations
	LastWriteNanos   int64   // 88-95:  Timestamp of last write
	CurrentBatchSize uint64  // 96-103: Current batch being built
	TotalBatches     uint64  // 104-111: Total batches written
	FailedWrites     uint64  // 112-119: Write failures
	_pad1            [8]byte // 120-127: Padding

	// ======== Cache Line 2 (128-191): Compression metrics ========
	TotalCompressed      uint64  // 128-135: Total compressed bytes
	CompressedEntries    uint64  // 136-143: Number of compressed entries
	SkippedCompression   uint64  // 144-151: Entries too small to compress
	CompressionRatio     uint64  // 152-159: Average ratio * 100
	CompressionTimeNanos int64   // 160-167: Total time compressing
	BestCompression      uint64  // 168-175: Best ratio seen * 100
	WorstCompression     uint64  // 176-183: Worst ratio seen * 100
	_pad2                [8]byte // 184-191: Padding

	// ======== Cache Line 3 (192-255): Latency metrics ========
	WriteLatencySum   uint64  // 192-199: Sum for averaging
	WriteLatencyCount uint64  // 200-207: Count for averaging
	MinWriteLatency   uint64  // 208-215: Minimum seen
	MaxWriteLatency   uint64  // 216-223: Maximum seen
	P50WriteLatency   uint64  // 224-231: Median estimate
	P99WriteLatency   uint64  // 232-239: 99th percentile estimate
	SyncLatencyNanos  int64   // 240-247: Time spent in fsync
	_pad3             [8]byte // 248-255: Padding

	// ======== Cache Line 4 (256-319): File operation metrics ========
	FilesCreated      uint64  // 256-263: Total files created
	FilesDeleted      uint64  // 264-271: Files removed by retention
	FileRotations     uint64  // 272-279: Successful rotations
	RotationTimeNanos int64   // 280-287: Time spent rotating
	CurrentFiles      uint64  // 288-295: Current file count
	TotalFileBytes    uint64  // 296-303: Total size on disk
	FailedRotations   uint64  // 304-311: Rotation failures
	_pad4             [8]byte // 312-319: Padding

	// ======== Cache Line 5 (320-383): Checkpoint/Index metrics ========
	CheckpointCount     uint64  // 320-327: Total checkpoints
	LastCheckpointNanos int64   // 328-335: Last checkpoint time
	CheckpointTimeNanos int64   // 336-343: Total checkpoint time
	IndexPersistCount   uint64  // 344-351: Index saves
	IndexPersistErrors  uint64  // 352-359: Failed index saves
	IndexSizeBytes      uint64  // 360-367: Current index size
	BinaryIndexNodes    uint64  // 368-375: Nodes in binary index
	_pad5               [8]byte // 376-383: Padding

	// ======== Cache Line 6 (384-447): Consumer metrics ========
	ActiveReaders    uint64  // 384-391: Current reader count
	TotalReaders     uint64  // 392-399: Total readers created
	MaxConsumerLag   uint64  // 400-407: Max entries behind
	TotalEntriesRead uint64  // 408-415: Total read operations
	ConsumerGroups   uint64  // 416-423: Active consumer groups
	AckedEntries     uint64  // 424-431: Acknowledged entries
	ReaderCacheHits  uint64  // 432-439: Cache hit count
	_pad6            [8]byte // 440-447: Padding

	// ======== Cache Line 7 (448-511): Error/Recovery metrics ========
	ErrorCount         uint64  // 448-455: Total errors
	LastErrorNanos     int64   // 456-463: Last error timestamp
	CorruptionDetected uint64  // 464-471: Corrupted entries found
	RecoveryAttempts   uint64  // 472-479: Auto-recovery attempts
	RecoverySuccesses  uint64  // 480-487: Successful recoveries
	PartialWrites      uint64  // 488-495: Incomplete write detected
	ReadErrors         uint64  // 496-503: Read failures
	_pad7              [8]byte // 504-511: Padding

	// ======== Cache Lines 8-9 (512-639): Retention metrics ========
	RetentionRuns        uint64   // 512-519: Cleanup executions
	LastRetentionNanos   int64    // 520-527: Last cleanup time
	RetentionTimeNanos   int64    // 528-535: Total cleanup time
	EntriesDeleted       uint64   // 536-543: Entries removed
	BytesReclaimed       uint64   // 544-551: Space freed
	OldestEntryNanos     int64    // 552-559: Oldest data timestamp
	RetentionErrors      uint64   // 560-567: Cleanup failures
	ProtectedByConsumers uint64   // 568-575: Files kept for consumers
	_pad8                [64]byte // 576-639: Full line padding

	// ======== Cache Lines 10-11 (640-767): Multi-process coordination ========
	ProcessCount         uint64   // 640-647: Active processes
	LastProcessHeartbeat int64    // 648-655: Latest heartbeat
	ContentionCount      uint64   // 656-663: Lock contentions
	LockWaitNanos        int64    // 664-671: Time waiting for locks
	MMAPRemapCount       uint64   // 672-679: File remappings
	FalseShareCount      uint64   // 680-687: Detected false sharing
	_pad9                [80]byte // 688-767: Padding

	// ======== Cache Lines 12-15 (768-1023): Reserved for future ========
	_reserved [256]byte // 768-1023: Future expansion space
}

// Compile-time checks
const unifiedStateSize = unsafe.Sizeof(UnifiedState{})

func init() {
	if unifiedStateSize != UnifiedStateSize {
		panic(fmt.Sprintf("UnifiedState must be exactly %d bytes, got %d", UnifiedStateSize, unifiedStateSize))
	}
	if unifiedStateSize%64 != 0 {
		panic("UnifiedState must be 64-byte aligned")
	}
}

// Helper methods for non-atomic version field
func (s *UnifiedState) GetVersion() uint64 {
	return atomic.LoadUint64(&s.Version)
}

func (s *UnifiedState) SetVersion(v uint64) {
	atomic.StoreUint64(&s.Version, v)
}

// Helper methods for atomic operations on uint64 fields
func (s *UnifiedState) GetLastEntryNumber() int64 {
	val := atomic.LoadInt64(&s.LastEntryNumber)
	if false { // Set to true for extreme debugging
		fmt.Printf("DEBUG GetLastEntryNumber: val=%d, ptr=%p, structPtr=%p\n", val, &s.LastEntryNumber, s)
	}
	return val
}

func (s *UnifiedState) IncrementLastEntryNumber() int64 {
	oldVal := atomic.LoadInt64(&s.LastEntryNumber)
	newVal := atomic.AddInt64(&s.LastEntryNumber, 1)
	afterVal := atomic.LoadInt64(&s.LastEntryNumber)

	if false {
		fmt.Printf("DEBUG IncrementLastEntryNumber: old=%d, returned=%d, after=%d, ptr=%p, fieldPtr=%p\n",
			oldVal, newVal, afterVal, s, &s.LastEntryNumber)
	}
	return newVal
}

func (s *UnifiedState) GetLastIndexUpdate() int64 {
	return atomic.LoadInt64(&s.LastIndexUpdate)
}

func (s *UnifiedState) SetLastIndexUpdate(nanos int64) {
	atomic.StoreInt64(&s.LastIndexUpdate, nanos)
}

// WriteOffset methods
func (s *UnifiedState) GetWriteOffset() uint64 {
	return atomic.LoadUint64(&s.WriteOffset)
}

func (s *UnifiedState) AddWriteOffset(delta uint64) uint64 {
	return atomic.AddUint64(&s.WriteOffset, delta)
}

func (s *UnifiedState) StoreWriteOffset(val uint64) {
	atomic.StoreUint64(&s.WriteOffset, val)
}

// ActiveFileIndex methods
func (s *UnifiedState) GetActiveFileIndex() uint64 {
	return atomic.LoadUint64(&s.ActiveFileIndex)
}

func (s *UnifiedState) AddActiveFileIndex(delta uint64) uint64 {
	return atomic.AddUint64(&s.ActiveFileIndex, delta)
}

func (s *UnifiedState) StoreActiveFileIndex(val uint64) {
	atomic.StoreUint64(&s.ActiveFileIndex, val)
}

// FileSize methods
func (s *UnifiedState) GetFileSize() uint64 {
	return atomic.LoadUint64(&s.FileSize)
}

func (s *UnifiedState) StoreFileSize(val uint64) {
	atomic.StoreUint64(&s.FileSize, val)
}

// LastFileSequence methods
func (s *UnifiedState) AddLastFileSequence(delta uint64) uint64 {
	return atomic.AddUint64(&s.LastFileSequence, delta)
}

// TotalWrites methods
func (s *UnifiedState) GetTotalWrites() uint64 {
	return atomic.LoadUint64(&s.TotalWrites)
}

func (s *UnifiedState) AddTotalWrites(delta uint64) uint64 {
	return atomic.AddUint64(&s.TotalWrites, delta)
}

// LastWriteNanos methods
func (s *UnifiedState) GetLastWriteNanos() int64 {
	return atomic.LoadInt64(&s.LastWriteNanos)
}

func (s *UnifiedState) StoreLastWriteNanos(val int64) {
	atomic.StoreInt64(&s.LastWriteNanos, val)
}

// TotalEntries methods
func (s *UnifiedState) AddTotalEntries(delta int64) int64 {
	return atomic.AddInt64(&s.TotalEntries, delta)
}

// TotalBytes methods
func (s *UnifiedState) AddTotalBytes(delta uint64) uint64 {
	return atomic.AddUint64(&s.TotalBytes, delta)
}

// FileRotations methods
func (s *UnifiedState) AddFileRotations(delta uint64) uint64 {
	return atomic.AddUint64(&s.FileRotations, delta)
}

// FilesCreated methods
func (s *UnifiedState) AddFilesCreated(delta uint64) uint64 {
	return atomic.AddUint64(&s.FilesCreated, delta)
}

// MinWriteLatency methods
func (s *UnifiedState) GetMinWriteLatency() uint64 {
	return atomic.LoadUint64(&s.MinWriteLatency)
}

func (s *UnifiedState) CompareAndSwapMinWriteLatency(old, new uint64) bool {
	return atomic.CompareAndSwapUint64(&s.MinWriteLatency, old, new)
}

// MaxWriteLatency methods
func (s *UnifiedState) GetMaxWriteLatency() uint64 {
	return atomic.LoadUint64(&s.MaxWriteLatency)
}

func (s *UnifiedState) CompareAndSwapMaxWriteLatency(old, new uint64) bool {
	return atomic.CompareAndSwapUint64(&s.MaxWriteLatency, old, new)
}

// WriteLatencySum methods
func (s *UnifiedState) AddWriteLatencySum(delta uint64) uint64 {
	return atomic.AddUint64(&s.WriteLatencySum, delta)
}

// WriteLatencyCount methods
func (s *UnifiedState) AddWriteLatencyCount(delta uint64) uint64 {
	return atomic.AddUint64(&s.WriteLatencyCount, delta)
}

// Computed metrics helpers
func (s *UnifiedState) GetAverageWriteLatency() uint64 {
	count := atomic.LoadUint64(&s.WriteLatencyCount)
	if count == 0 {
		return 0
	}
	return atomic.LoadUint64(&s.WriteLatencySum) / count
}

func (s *UnifiedState) GetCompressionRatioFloat() float64 {
	compressed := atomic.LoadUint64(&s.TotalCompressed)
	original := atomic.LoadUint64(&s.TotalBytes)
	if original == 0 {
		return 1.0
	}
	return float64(compressed) / float64(original)
}

func (s *UnifiedState) GetErrorRate() float64 {
	errors := atomic.LoadUint64(&s.ErrorCount)
	writes := atomic.LoadUint64(&s.TotalWrites)
	if writes == 0 {
		return 0.0
	}
	return float64(errors) / float64(writes)
}

// UpdateWriteLatency updates latency metrics with a new sample
// Note: nanos should be uint64 since latencies are always positive
func (s *UnifiedState) UpdateWriteLatency(nanos uint64) {
	atomic.AddUint64(&s.WriteLatencySum, nanos)
	atomic.AddUint64(&s.WriteLatencyCount, 1)

	// Update min
	for {
		min := atomic.LoadUint64(&s.MinWriteLatency)
		if min > 0 && min <= nanos {
			break
		}
		if atomic.CompareAndSwapUint64(&s.MinWriteLatency, min, nanos) {
			break
		}
	}

	// Update max
	for {
		max := atomic.LoadUint64(&s.MaxWriteLatency)
		if max >= nanos {
			break
		}
		if atomic.CompareAndSwapUint64(&s.MaxWriteLatency, max, nanos) {
			break
		}
	}
}

// Constants for the unified state
const (
	UnifiedStateVersion1 = 1
	UnifiedStateSize     = 1024
)
