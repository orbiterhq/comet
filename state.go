package comet

import (
	"fmt"
	"sync/atomic"
	"unsafe"
)

// CometState consolidates ALL mmap state and comprehensive metrics
// Stored in: comet.state (1KB file per shard)
// Total size: 1024 bytes (1KB) for plenty of room to grow
//
// Design principles:
// 1. Group related metrics in same cache line
// 2. Hot path metrics in early cache lines
// 3. Generous reserved space for future additions
// 4. All fields are atomic-safe for multi-process access
// 5. CRITICAL: Use raw int64/uint64 fields with atomic operations, NOT atomic.Int64
type CometState struct {
	// ======== Header (0-63): Version and core state ========
	Version          uint64  // 0-7:   Format version (start with 1)
	WriteOffset      uint64  // 8-15:  Current write position in active file
	LastEntryNumber  int64   // 16-23: Sequence counter for entry IDs
	LastIndexUpdate  int64   // 24-31: Timestamp of last index modification
	_pad0b           [8]byte // 32-39: Removed ActiveFileIndex - never used
	_pad0c           [8]byte // 40-47: Removed FileSize - never used
	LastFileSequence uint64  // 48-55: Sequence counter for file naming
	_pad0            [8]byte // 56-63: Padding

	// ======== Cache Line 1 (64-127): Write metrics (hot path) ========
	_pad0a           [8]byte // 64-71:  Removed TotalEntries - use index.CurrentEntryNumber
	TotalBytes       uint64  // 72-79:  Total uncompressed bytes
	TotalWrites      uint64  // 80-87:  Total write operations
	LastWriteNanos   int64   // 88-95:  Timestamp of last write
	CurrentBatchSize uint64  // 96-103: Current batch being built
	TotalBatches     uint64  // 104-111: Total batches written
	FailedWrites     uint64  // 112-119: Write failures
	_pad1            [8]byte // 120-127: Padding

	// ======== Cache Line 2 (128-191): Compression metrics ========
	TotalCompressed      uint64   // 128-135: Total compressed bytes
	CompressedEntries    uint64   // 136-143: Number of compressed entries
	SkippedCompression   uint64   // 144-151: Entries too small to compress
	CompressionRatio     uint64   // 152-159: Average ratio * 100
	CompressionTimeNanos int64    // 160-167: Total time compressing
	_pad2                [24]byte // 168-191: Padding

	// ======== Cache Line 3 (192-255): Latency metrics ========
	WriteLatencySum   uint64   // 192-199: Sum for averaging
	WriteLatencyCount uint64   // 200-207: Count for averaging
	MinWriteLatency   uint64   // 208-215: Minimum seen
	MaxWriteLatency   uint64   // 216-223: Maximum seen
	SyncLatencyNanos  int64    // 224-231: Time spent in fsync
	_pad3             [24]byte // 232-255: Padding

	// ======== Cache Line 4 (256-319): File operation metrics ========
	FilesCreated      uint64 // 256-263: Total files created
	FilesDeleted      uint64 // 264-271: Files removed by retention
	FileRotations     uint64 // 272-279: Successful rotations
	RotationTimeNanos int64  // 280-287: Time spent rotating
	CurrentFiles      uint64 // 288-295: Current file count
	TotalFileBytes    uint64 // 296-303: Total size on disk
	FailedRotations   uint64 // 304-311: Rotation failures
	SyncCount         uint64 // 312-319: Total sync operations

	// ======== Cache Line 5 (320-383): Checkpoint/Index metrics ========
	CheckpointCount     uint64   // 320-327: Total checkpoints
	LastCheckpointNanos int64    // 328-335: Last checkpoint time
	CheckpointTimeNanos int64    // 336-343: Total checkpoint time
	IndexPersistCount   uint64   // 344-351: Index saves
	IndexPersistErrors  uint64   // 352-359: Failed index saves
	BinaryIndexNodes    uint64   // 360-367: Nodes in binary index
	_pad5               [16]byte // 368-383: Padding

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

	// ======== Cache Lines 10-11 (640-767): Reader cache metrics ========
	ReaderFileMaps    uint64   // 640-647: Files mapped into memory
	ReaderFileUnmaps  uint64   // 648-655: Files unmapped from memory
	ReaderCacheBytes  uint64   // 656-663: Current cache memory usage
	ReaderMappedFiles uint64   // 664-671: Current number of mapped files
	ReaderFileRemaps  uint64   // 672-679: File remappings due to growth
	ReaderCacheEvicts uint64   // 680-687: Files evicted due to pressure
	_pad9             [80]byte // 688-767: Padding to end of cache line 11

	// ======== Cache Lines 12-15 (768-1023): Global coordination ========
	_reserved [256]byte // 776-1023: Future expansion space
}

// Compile-time checks
const stateSize = unsafe.Sizeof(CometState{})

func init() {
	if stateSize != CometStateSize {
		panic(fmt.Sprintf("CometState must be exactly %d bytes, got %d", CometStateSize, stateSize))
	}
	if stateSize%64 != 0 {
		panic("CometState must be 64-byte aligned")
	}
}

// Helper methods for non-atomic version field
func (s *CometState) GetVersion() uint64 {
	return atomic.LoadUint64(&s.Version)
}

func (s *CometState) SetVersion(v uint64) {
	atomic.StoreUint64(&s.Version, v)
}

// Helper methods for atomic operations on uint64 fields
func (s *CometState) GetLastEntryNumber() int64 {
	val := atomic.LoadInt64(&s.LastEntryNumber)
	return val
}

func (s *CometState) IncrementLastEntryNumber() int64 {
	// Since processes own their shards exclusively, simple atomic add is sufficient
	return atomic.AddInt64(&s.LastEntryNumber, 1)
}

// AllocateEntryNumbers atomically reserves a batch of entry numbers
func (s *CometState) AllocateEntryNumbers(count int) int64 {
	// Reserve 'count' entry numbers and return the first one
	// This does a single atomic operation instead of count operations
	return atomic.AddInt64(&s.LastEntryNumber, int64(count)) - int64(count) + 1
}

func (s *CometState) GetLastIndexUpdate() int64 {
	return atomic.LoadInt64(&s.LastIndexUpdate)
}

func (s *CometState) SetLastIndexUpdate(nanos int64) {
	atomic.StoreInt64(&s.LastIndexUpdate, nanos)
}

// WriteOffset methods
func (s *CometState) GetWriteOffset() uint64 {
	return atomic.LoadUint64(&s.WriteOffset)
}

func (s *CometState) AddWriteOffset(delta uint64) uint64 {
	return atomic.AddUint64(&s.WriteOffset, delta)
}

func (s *CometState) StoreWriteOffset(val uint64) {
	atomic.StoreUint64(&s.WriteOffset, val)
}

// ActiveFileIndex methods removed - field not used
// FileSize methods removed - field not used

// LastFileSequence methods
func (s *CometState) AddLastFileSequence(delta uint64) uint64 {
	return atomic.AddUint64(&s.LastFileSequence, delta)
}

// TotalWrites methods - simple atomics since processes own shards exclusively
func (s *CometState) GetTotalWrites() uint64 {
	return atomic.LoadUint64(&s.TotalWrites)
}

func (s *CometState) AddTotalWrites(delta uint64) uint64 {
	// Since processes own their shards exclusively, simple atomic add is sufficient
	return atomic.AddUint64(&s.TotalWrites, delta)
}

// LastWriteNanos methods
func (s *CometState) GetLastWriteNanos() int64 {
	return atomic.LoadInt64(&s.LastWriteNanos)
}

func (s *CometState) StoreLastWriteNanos(val int64) {
	atomic.StoreInt64(&s.LastWriteNanos, val)
}

// TotalBytes methods - simple atomics since processes own shards exclusively
func (s *CometState) AddTotalBytes(delta uint64) uint64 {
	// Since processes own their shards exclusively, simple atomic add is sufficient
	return atomic.AddUint64(&s.TotalBytes, delta)
}

// FileRotations methods - simple atomics since processes own shards exclusively
func (s *CometState) AddFileRotations(delta uint64) uint64 {
	// Since processes own their shards exclusively, simple atomic add is sufficient
	return atomic.AddUint64(&s.FileRotations, delta)
}

// FilesCreated methods - simple atomics since processes own shards exclusively
func (s *CometState) AddFilesCreated(delta uint64) uint64 {
	// Since processes own their shards exclusively, simple atomic add is sufficient
	return atomic.AddUint64(&s.FilesCreated, delta)
}

// MinWriteLatency methods
func (s *CometState) GetMinWriteLatency() uint64 {
	return atomic.LoadUint64(&s.MinWriteLatency)
}

// MaxWriteLatency methods
func (s *CometState) GetMaxWriteLatency() uint64 {
	return atomic.LoadUint64(&s.MaxWriteLatency)
}

// WriteLatencySum methods
func (s *CometState) AddWriteLatencySum(delta uint64) uint64 {
	return atomic.AddUint64(&s.WriteLatencySum, delta)
}

// WriteLatencyCount methods
func (s *CometState) AddWriteLatencyCount(delta uint64) uint64 {
	return atomic.AddUint64(&s.WriteLatencyCount, delta)
}

// Computed metrics helpers
func (s *CometState) GetAverageWriteLatency() uint64 {
	count := atomic.LoadUint64(&s.WriteLatencyCount)
	if count == 0 {
		return 0
	}
	return atomic.LoadUint64(&s.WriteLatencySum) / count
}

func (s *CometState) GetCompressionRatioFloat() float64 {
	compressed := atomic.LoadUint64(&s.TotalCompressed)
	original := atomic.LoadUint64(&s.TotalBytes)
	if original == 0 {
		return 1.0
	}
	return float64(compressed) / float64(original)
}

// ErrorCount methods - simple atomics since processes own shards exclusively
func (s *CometState) AddErrorCount(delta uint64) uint64 {
	// Since processes own their shards exclusively, simple atomic add is sufficient
	return atomic.AddUint64(&s.ErrorCount, delta)
}

// FailedWrites methods - simple atomics since processes own shards exclusively
func (s *CometState) AddFailedWrites(delta uint64) uint64 {
	// Since processes own their shards exclusively, simple atomic add is sufficient
	return atomic.AddUint64(&s.FailedWrites, delta)
}

func (s *CometState) GetErrorRate() float64 {
	errors := atomic.LoadUint64(&s.ErrorCount)
	writes := atomic.LoadUint64(&s.TotalWrites)
	if writes == 0 {
		return 0.0
	}
	return float64(errors) / float64(writes)
}

// Constants for the unified state
const (
	CometStateVersion1 = 1
	CometStateSize     = 1024
)
