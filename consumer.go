package comet

import (
	"context"
	"encoding/binary"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// MessageID represents a structured message ID
// Fields ordered for optimal memory alignment: int64 first, then uint32
type MessageID struct {
	EntryNumber int64  `json:"entry_number"`
	ShardID     uint32 `json:"shard_id"`
}

// String returns the string representation of the ID (ShardID-EntryNumber format)
func (id MessageID) String() string {
	return fmt.Sprintf("%d-%d", id.ShardID, id.EntryNumber)
}

// ParseMessageID parses a string ID back to MessageID
func ParseMessageID(str string) (MessageID, error) {
	var shardID uint32
	var entryNumber int64
	if _, err := fmt.Sscanf(str, "%d-%d", &shardID, &entryNumber); err != nil {
		return MessageID{}, fmt.Errorf("invalid message ID format: %w", err)
	}
	return MessageID{EntryNumber: entryNumber, ShardID: shardID}, nil
}

// StreamMessage represents a message read from a stream
type StreamMessage struct {
	Stream string    // Stream name/identifier
	ID     MessageID // Unique message ID
	Data   []byte    // Raw message data
}

// Consumer reads from comet stream shards
// Fields ordered for optimal memory alignment
type Consumer struct {
	// Pointers first (8 bytes on 64-bit)
	client *Client

	// Composite types
	readers sync.Map // Cached readers per shard (optimized for read-heavy workload)

	// Strings last
	group string
}

// ConsumerOptions configures a consumer
type ConsumerOptions struct {
	Group string
}

// ProcessFunc handles a batch of messages, returning error to trigger retry
type ProcessFunc func(messages []StreamMessage) error

// ProcessOption configures the Process method
type ProcessOption func(*processConfig)

// processConfig holds internal configuration built from options
type processConfig struct {
	// Core processing
	handler ProcessFunc
	autoAck bool

	// Callbacks
	onError func(err error, retryCount int)
	onBatch func(size int, duration time.Duration)

	// Behavior
	batchSize    int
	maxRetries   int
	pollInterval time.Duration
	retryDelay   time.Duration

	// Sharding
	stream        string
	shards        []uint32
	consumerID    int
	consumerCount int
}

// WithStream specifies a stream pattern for shard discovery
func WithStream(pattern string) ProcessOption {
	return func(cfg *processConfig) {
		cfg.stream = pattern
	}
}

// WithShards specifies explicit shards to process
func WithShards(shards ...uint32) ProcessOption {
	return func(cfg *processConfig) {
		cfg.shards = shards
	}
}

// WithBatchSize sets the number of messages to read at once
func WithBatchSize(size int) ProcessOption {
	return func(cfg *processConfig) {
		cfg.batchSize = size
	}
}

// WithMaxRetries sets the number of retry attempts for failed batches
func WithMaxRetries(retries int) ProcessOption {
	return func(cfg *processConfig) {
		cfg.maxRetries = retries
	}
}

// WithPollInterval sets how long to wait when no messages are available
func WithPollInterval(interval time.Duration) ProcessOption {
	return func(cfg *processConfig) {
		cfg.pollInterval = interval
	}
}

// WithRetryDelay sets the base delay between retries
func WithRetryDelay(delay time.Duration) ProcessOption {
	return func(cfg *processConfig) {
		cfg.retryDelay = delay
	}
}

// WithAutoAck controls automatic acknowledgment (default: true)
func WithAutoAck(enabled bool) ProcessOption {
	return func(cfg *processConfig) {
		cfg.autoAck = enabled
	}
}

// WithErrorHandler sets a callback for processing errors
func WithErrorHandler(handler func(err error, retryCount int)) ProcessOption {
	return func(cfg *processConfig) {
		cfg.onError = handler
	}
}

// WithBatchCallback sets a callback after each batch completes
func WithBatchCallback(callback func(size int, duration time.Duration)) ProcessOption {
	return func(cfg *processConfig) {
		cfg.onBatch = callback
	}
}

// WithConsumerAssignment configures distributed processing
func WithConsumerAssignment(id, total int) ProcessOption {
	return func(cfg *processConfig) {
		cfg.consumerID = id
		cfg.consumerCount = total
	}
}

// buildProcessConfig applies options and defaults
func buildProcessConfig(handler ProcessFunc, opts []ProcessOption) *processConfig {
	cfg := &processConfig{
		handler:       handler,
		autoAck:       true,
		batchSize:     100,
		maxRetries:    3,
		pollInterval:  100 * time.Millisecond,
		retryDelay:    time.Second,
		consumerCount: 1,
		consumerID:    0,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	// If no stream or shards specified, discover all shards
	if cfg.stream == "" && len(cfg.shards) == 0 {
		cfg.stream = "*:*:*:*" // Match any stream pattern
	}

	return cfg
}

// NewConsumer creates a new consumer for comet streams
func NewConsumer(client *Client, opts ConsumerOptions) *Consumer {
	return &Consumer{
		client: client,
		group:  opts.Group,
		// readers sync.Map is zero-initialized and ready to use
	}
}

// Close closes the consumer and releases all resources
func (c *Consumer) Close() error {
	// Close all cached readers
	c.readers.Range(func(key, value any) bool {
		if reader, ok := value.(*Reader); ok {
			reader.Close()
		}
		return true
	})
	return nil
}

// Read reads up to count entries from the specified shards
func (c *Consumer) Read(ctx context.Context, shards []uint32, count int) ([]StreamMessage, error) {
	var messages []StreamMessage
	remaining := count

	for _, shardID := range shards {
		if remaining <= 0 {
			break
		}

		shard, err := c.client.getOrCreateShard(shardID)
		if err != nil {
			return nil, fmt.Errorf("failed to get shard %d: %w", shardID, err)
		}

		shardMessages, err := c.readFromShard(ctx, shard, remaining)
		if err != nil {
			return nil, fmt.Errorf("failed to read from shard %d: %w", shardID, err)
		}

		messages = append(messages, shardMessages...)
		remaining -= len(shardMessages)
	}

	return messages, nil
}

// Process continuously reads and processes messages from shards.
// The simplest usage processes all discoverable shards:
//
//	err := consumer.Process(ctx, handleMessages)
//
// With options:
//
//	err := consumer.Process(ctx, handleMessages,
//	    comet.WithStream("events:v1:shard:*"),
//	    comet.WithBatchSize(1000),
//	    comet.WithErrorHandler(logError),
//	)
//
// For distributed processing:
//
//	err := consumer.Process(ctx, handleMessages,
//	    comet.WithStream("events:v1:shard:*"),
//	    comet.WithConsumerAssignment(workerID, totalWorkers),
//	)
func (c *Consumer) Process(ctx context.Context, handler ProcessFunc, opts ...ProcessOption) error {
	// Build config from options
	cfg := buildProcessConfig(handler, opts)

	// Validate required fields
	if cfg.handler == nil {
		return fmt.Errorf("handler is required")
	}

	// Determine shards to process
	var shards []uint32
	if len(cfg.shards) > 0 {
		shards = cfg.shards
	} else if cfg.stream != "" {
		// Auto-discover shards from stream pattern
		discoveredShards, err := c.discoverShards(cfg.stream, cfg.consumerID, cfg.consumerCount)
		if err != nil {
			return fmt.Errorf("failed to discover shards: %w", err)
		}
		shards = discoveredShards
	} else {
		return fmt.Errorf("either Shards or Stream must be specified")
	}

	// Apply defaults (already set in buildProcessConfig)
	autoAck := cfg.autoAck

	// Main processing loop
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		start := time.Now()
		messages, err := c.Read(ctx, shards, cfg.batchSize)
		if err != nil {
			if cfg.onError != nil {
				cfg.onError(err, 0)
			}
			// For read errors, sleep and retry
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(cfg.pollInterval):
				continue
			}
		}

		if len(messages) == 0 {
			// No messages, wait before polling again
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(cfg.pollInterval):
				continue
			}
		}

		// Process batch with retries
		var processErr error
		for retry := 0; retry <= cfg.maxRetries; retry++ {
			processErr = cfg.handler(messages)
			if processErr == nil {
				break
			}

			if cfg.onError != nil {
				cfg.onError(processErr, retry)
			}

			if retry < cfg.maxRetries {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(cfg.retryDelay * time.Duration(retry+1)):
					// Exponential backoff
				}
			}
		}

		// Auto-ack if enabled and processing succeeded
		if autoAck && processErr == nil {
			for _, msg := range messages {
				if err := c.Ack(ctx, msg.ID); err != nil && cfg.onError != nil {
					cfg.onError(fmt.Errorf("ack failed for message %s: %w", msg.ID, err), 0)
				}
			}
		}

		// Call batch callback if provided
		if cfg.onBatch != nil {
			cfg.onBatch(len(messages), time.Since(start))
		}
	}
}

// getOrCreateReader gets or creates a reader for a shard
func (c *Consumer) getOrCreateReader(shard *Shard) (*Reader, error) {
	// Fast path: check if reader exists using sync.Map
	if value, ok := c.readers.Load(shard.shardID); ok {
		reader := value.(*Reader)

		// Check if reader is still valid by comparing file lists
		shard.mu.RLock()
		currentFiles := shard.index.Files
		shard.mu.RUnlock()

		reader.mu.RLock()
		readerFiles := reader.files
		reader.mu.RUnlock()

		// Reader is valid only if it has the exact same files as the shard
		// This handles both additions (rotation) and deletions (retention)
		if len(currentFiles) == len(readerFiles) {
			// Check if all files match
			filesMatch := true
			for i := 0; i < len(currentFiles) && filesMatch; i++ {
				if currentFiles[i].Path != readerFiles[i].Path {
					filesMatch = false
				}
			}
			if filesMatch {
				return reader, nil
			}
		}

		// Reader is stale, need to recreate
		// Delete the old one so only one goroutine recreates
		c.readers.Delete(shard.shardID)
		reader.Close()
	}

	// Create new reader with a snapshot of the current index
	shard.mu.RLock()
	// Make a copy of the index to avoid race conditions
	indexCopy := &ShardIndex{
		Files:              make([]FileInfo, len(shard.index.Files)),
		CurrentFile:        shard.index.CurrentFile,
		CurrentWriteOffset: shard.index.CurrentWriteOffset,
		CurrentEntryNumber: shard.index.CurrentEntryNumber,
		ConsumerOffsets:    make(map[string]int64),
		BinaryIndex: BinarySearchableIndex{
			IndexInterval: shard.index.BinaryIndex.IndexInterval,
			MaxNodes:      shard.index.BinaryIndex.MaxNodes,
			Nodes:         make([]EntryIndexNode, len(shard.index.BinaryIndex.Nodes)),
		},
	}
	copy(indexCopy.Files, shard.index.Files)
	copy(indexCopy.BinaryIndex.Nodes, shard.index.BinaryIndex.Nodes)
	maps.Copy(indexCopy.ConsumerOffsets, shard.index.ConsumerOffsets)
	shard.mu.RUnlock()

	newReader, err := NewReader(shard.shardID, indexCopy)
	if err != nil {
		return nil, err
	}

	// Use LoadOrStore to handle race where multiple goroutines try to create
	if actual, loaded := c.readers.LoadOrStore(shard.shardID, newReader); loaded {
		// Another goroutine created a reader, close ours and use theirs
		newReader.Close()
		return actual.(*Reader), nil
	}

	return newReader, nil
}

// findEntryPosition finds the position of an entry using binary searchable index (O(log n))
func (c *Consumer) findEntryPosition(shard *Shard, entryNum int64) (EntryPosition, error) {
	// Check if the requested entry exists
	if entryNum >= shard.index.CurrentEntryNumber {
		return EntryPosition{}, fmt.Errorf("entry %d does not exist (current max: %d)", entryNum, shard.index.CurrentEntryNumber-1)
	}

	// Use the binary searchable index for O(log n) lookup
	if len(shard.index.BinaryIndex.Nodes) > 0 {
		// Get the best starting position for scanning
		if startPos, startEntry, found := shard.index.BinaryIndex.GetScanStartPosition(entryNum); found {
			// Safety check: if the position is invalid, fall back to linear scan
			if startPos.FileIndex < 0 || startPos.FileIndex >= len(shard.index.Files) {
				// Binary index has stale data, fall back to scanning from beginning
			} else {
				if startEntry == entryNum {
					return startPos, nil
				}
				// Scan forward from the closest indexed entry
				return c.scanForwardToEntry(shard, startPos, startEntry, entryNum)
			}
		}
	}

	// If no index nodes exist, start from the beginning
	if len(shard.index.Files) == 0 {
		return EntryPosition{}, fmt.Errorf("no files in shard")
	}

	// Start from entry 0 in the first file
	startPos := EntryPosition{
		FileIndex:  0,
		ByteOffset: 0,
	}

	// In multi-process mode, entries might not start from 0
	// Calculate the first entry number based on total entries written
	totalEntries := int64(0)
	for _, f := range shard.index.Files {
		totalEntries += f.Entries
	}
	firstEntryNum := shard.index.CurrentEntryNumber - totalEntries

	// If looking for an entry before the first one, it doesn't exist
	if entryNum < firstEntryNum {
		return EntryPosition{}, fmt.Errorf("entry %d does not exist (first entry is %d)", entryNum, firstEntryNum)
	}

	// If looking for the first entry, return the start position
	if entryNum == firstEntryNum {
		return startPos, nil
	}

	// Otherwise scan forward from the first entry
	return c.scanForwardToEntry(shard, startPos, firstEntryNum, entryNum)
}

// scanForwardToEntry scans forward from a known position to find the target entry
func (c *Consumer) scanForwardToEntry(shard *Shard, startPos EntryPosition, startEntry, targetEntry int64) (EntryPosition, error) {
	if startEntry >= targetEntry {
		return startPos, nil // No scanning needed
	}

	// Get reader for this shard
	reader, err := c.getOrCreateReader(shard)
	if err != nil {
		return EntryPosition{}, fmt.Errorf("failed to get reader: %w", err)
	}

	// Hold reader lock during the entire scan to prevent races with remapFile
	reader.mu.RLock()
	defer reader.mu.RUnlock()

	// First, find which file contains the target entry
	targetFileIndex := -1
	for i := startPos.FileIndex; i < len(shard.index.Files); i++ {
		fileInfo := shard.index.Files[i]
		if fileInfo.StartEntry <= targetEntry && targetEntry < fileInfo.StartEntry+fileInfo.Entries {
			targetFileIndex = i
			break
		}
	}

	if targetFileIndex == -1 {
		return EntryPosition{}, fmt.Errorf("entry %d not found in any file", targetEntry)
	}

	// Now scan within the target file to find the exact position
	file := reader.files[targetFileIndex]
	fileData := file.data.Load()
	if len(fileData) == 0 {
		return EntryPosition{}, fmt.Errorf("file %d is empty but should contain entry %d", targetFileIndex, targetEntry)
	}

	fileInfo := shard.index.Files[targetFileIndex]
	currentEntry := fileInfo.StartEntry
	currentOffset := int64(0)

	// Scan through the file entry by entry
	for currentEntry <= targetEntry {
		// Check if we've found the target
		if currentEntry == targetEntry {
			return EntryPosition{
				FileIndex:  targetFileIndex,
				ByteOffset: currentOffset,
			}, nil
		}

		// Check if we have enough data for the header
		if currentOffset+12 > int64(len(fileData)) {
			return EntryPosition{}, fmt.Errorf("entry %d header extends beyond file", currentEntry)
		}

		// Read entry header
		header := fileData[currentOffset : currentOffset+12]
		length := binary.LittleEndian.Uint32(header[0:4])

		// Validate entry length
		if length > 100*1024*1024 { // 100MB max
			return EntryPosition{}, fmt.Errorf("entry %d has invalid length %d", currentEntry, length)
		}

		// Check if the full entry fits
		entryEnd := currentOffset + 12 + int64(length)
		if entryEnd > int64(len(fileData)) {
			return EntryPosition{}, fmt.Errorf("entry %d extends beyond file", currentEntry)
		}

		// Move to next entry
		currentEntry++
		currentOffset = entryEnd
	}

	return EntryPosition{}, fmt.Errorf("entry %d not found in file %d", targetEntry, targetFileIndex)
}

// readFromShard reads entries from a single shard using entry-based positioning
func (c *Consumer) readFromShard(ctx context.Context, shard *Shard, maxCount int) ([]StreamMessage, error) {
	// Lock-free reader tracking
	atomic.AddInt64(&shard.readerCount, 1)
	defer atomic.AddInt64(&shard.readerCount, -1)

	// Check unified state for instant change detection
	if shard.state != nil {
		currentTimestamp := shard.state.GetLastIndexUpdate()
		if currentTimestamp != shard.lastMmapCheck {
			// Index changed - reload it under write lock
			shard.mu.Lock()
			// Double-check after acquiring lock
			if currentTimestamp != shard.lastMmapCheck {
				if err := shard.loadIndexWithRecovery(); err != nil {
					shard.mu.Unlock()
					return nil, fmt.Errorf("failed to reload index after detecting mmap change: %w", err)
				}
				shard.lastMmapCheck = currentTimestamp

				// In multi-process mode, check if we need to rebuild index from files
				// We can tell we're in multi-process mode if mmapState exists
				if shard.state != nil {
					shardDir := filepath.Join(c.client.dataDir, fmt.Sprintf("shard-%04d", shard.shardID))
					shard.lazyRebuildIndexIfNeeded(c.client.config, shardDir)
				}

				// Also invalidate any cached readers since the index changed
				c.readers.Range(func(key, value any) bool {
					if key.(uint32) == shard.shardID {
						if reader, ok := value.(*Reader); ok {
							reader.Close()
						}
						c.readers.Delete(key)
						return false // Stop after finding this shard's reader
					}
					return true
				})
			}
			shard.mu.Unlock()
		}
	}

	shard.mu.RLock()
	// Get consumer entry offset (not byte offset!)
	startEntryNum, exists := shard.index.ConsumerOffsets[c.group]
	if !exists {
		startEntryNum = 0
	}

	// After retention, the requested start entry might no longer exist
	// Adjust to the earliest available entry if needed
	if len(shard.index.Files) > 0 {
		earliestEntry := shard.index.Files[0].StartEntry
		if startEntryNum < earliestEntry {
			startEntryNum = earliestEntry
		}
	}

	endEntryNum := shard.index.CurrentEntryNumber
	fileCount := len(shard.index.Files)
	shard.mu.RUnlock()

	// In multi-process mode, check if index might be stale by comparing with state
	if shard.state != nil && shard.mmapWriter != nil && shard.mmapWriter.state != nil {
		totalWrites := shard.mmapWriter.state.GetTotalWrites()
		if totalWrites > uint64(endEntryNum) {
			// Need write lock for rebuild
			shard.mu.Lock()
			shardDir := filepath.Join(c.client.dataDir, fmt.Sprintf("shard-%04d", shard.shardID))
			// Force a full rebuild by manually updating the rebuild trigger
			// Temporarily modify the file end offset to trigger a rebuild
			oldEndOffset := int64(0)
			if len(shard.index.Files) > 0 {
				oldEndOffset = shard.index.Files[len(shard.index.Files)-1].EndOffset
				// Set end offset to 0 to force rebuild detection
				shard.index.Files[len(shard.index.Files)-1].EndOffset = 0
			}

			shard.lazyRebuildIndexIfNeeded(c.client.config, shardDir)

			// Restore if rebuild didn't happen (shouldn't occur)
			if len(shard.index.Files) > 0 && shard.index.Files[len(shard.index.Files)-1].EndOffset == 0 {
				shard.index.Files[len(shard.index.Files)-1].EndOffset = oldEndOffset
			}
			// Reload the updated values
			startEntryNum, exists = shard.index.ConsumerOffsets[c.group]
			if !exists {
				startEntryNum = 0
			}

			// After retention, adjust to earliest available entry if needed
			if len(shard.index.Files) > 0 {
				earliestEntry := shard.index.Files[0].StartEntry
				if startEntryNum < earliestEntry {
					startEntryNum = earliestEntry
				}
			}

			endEntryNum = shard.index.CurrentEntryNumber
			fileCount = len(shard.index.Files)
			shard.mu.Unlock()
		}
	}

	if startEntryNum >= endEntryNum {
		return nil, nil // No new data
	}

	// Safety check: if we have entries but no files, something is wrong
	if endEntryNum > 0 && fileCount == 0 {
		return nil, fmt.Errorf("shard %d has %d entries but no data files", shard.shardID, endEntryNum)
	}

	// Preallocate slice capacity based on available entries and maxCount
	availableEntries := endEntryNum - startEntryNum
	expectedCount := availableEntries
	if maxCount > 0 && expectedCount > int64(maxCount) {
		expectedCount = int64(maxCount)
	}
	messages := make([]StreamMessage, 0, expectedCount)

	// Read entries by entry number, looking up positions from index
	for entryNum := startEntryNum; entryNum < endEntryNum && len(messages) < maxCount; entryNum++ {
		// Check context
		if ctx.Err() != nil {
			return messages, ctx.Err()
		}

		// Find where this entry is stored using interval-based boundaries
		shard.mu.RLock()
		position, err := c.findEntryPosition(shard, entryNum)
		shard.mu.RUnlock()

		if err != nil {
			// In multi-process mode, if index-based lookup fails, try direct file scanning
			// This handles the case where the index is incomplete but the data exists in files
			if shard.state != nil && !strings.Contains(err.Error(), "no files in shard") {
				position, err = c.scanDataFilesForEntry(shard, entryNum)
			}

			if err != nil {
				// In multi-process mode, some entries might not exist due to gaps in the sequence
				// Skip these entries instead of failing the entire read
				if strings.Contains(err.Error(), "does not exist") {
					continue
				}
				return nil, fmt.Errorf("failed to find entry %d position in shard %d: %w", entryNum, shard.shardID, err)
			}
		}

		// Safety check for invalid position
		if position.FileIndex < 0 {
			return nil, fmt.Errorf("invalid position for entry %d in shard %d: FileIndex=%d, ByteOffset=%d", entryNum, shard.shardID, position.FileIndex, position.ByteOffset)
		}

		// Get reader for this shard
		reader, err := c.getOrCreateReader(shard)
		if err != nil {
			return nil, fmt.Errorf("failed to get reader for shard %d: %w", shard.shardID, err)
		}

		// Read the specific entry
		data, err := reader.ReadEntryAtPosition(position)
		if err != nil {
			// If we get a "file not memory mapped" error, it might mean the file is empty
			// or hasn't been flushed yet. Try to force a flush and retry once.
			if strings.Contains(err.Error(), "file not memory mapped") {
				// Force persist the index to ensure file metadata is up to date
				shard.persistIndex()
				// Retry the read
				data, err = reader.ReadEntryAtPosition(position)
			}
			if err != nil {
				return nil, fmt.Errorf("failed to read entry %d from shard %d: %w", entryNum, shard.shardID, err)
			}
		}

		message := StreamMessage{
			Stream: fmt.Sprintf("shard:%04d", shard.shardID),
			ID:     MessageID{EntryNumber: entryNum, ShardID: shard.shardID},
			Data:   data,
		}

		messages = append(messages, message)
	}

	return messages, nil
}

// scanDataFilesForEntry directly scans data files to find an entry when index is incomplete
func (c *Consumer) scanDataFilesForEntry(shard *Shard, targetEntry int64) (EntryPosition, error) {
	// This is the fallback when index-based lookup fails in multi-process mode
	// We scan the actual append-only data files directly

	shard.mu.RLock()
	files := make([]FileInfo, len(shard.index.Files))
	copy(files, shard.index.Files)
	shard.mu.RUnlock()

	if len(files) == 0 {
		return EntryPosition{}, fmt.Errorf("no files to scan")
	}

	// Get reader for file access
	reader, err := c.getOrCreateReader(shard)
	if err != nil {
		return EntryPosition{}, fmt.Errorf("failed to get reader: %w", err)
	}

	reader.mu.RLock()
	defer reader.mu.RUnlock()

	currentEntry := int64(0)

	// Scan each file sequentially
	for fileIdx := range files {
		if fileIdx >= len(reader.files) {
			break // Reader doesn't have this file yet
		}

		file := reader.files[fileIdx]
		fileData := file.data.Load()
		if fileData == nil {
			continue
		}

		offset := int64(0)
		fileSize := int64(len(fileData))

		// Scan entries in this file
		for offset < fileSize {
			// Check if we have enough data for header
			if offset+12 > fileSize {
				break
			}

			// Read entry header
			header := fileData[offset : offset+12]
			length := binary.LittleEndian.Uint32(header[0:4])
			timestamp := binary.LittleEndian.Uint64(header[4:12])

			// Validate entry
			if length == 0 || length > 100*1024*1024 || timestamp == 0 {
				// Invalid entry - try to find next valid entry
				offset += 4
				continue
			}

			// Check if full entry is available
			entryEnd := offset + 12 + int64(length)
			if entryEnd > fileSize {
				break // Entry extends beyond file
			}

			// Found a valid entry - check if it's the target
			if currentEntry == targetEntry {
				return EntryPosition{
					FileIndex:  fileIdx,
					ByteOffset: offset,
				}, nil
			}

			// Move to next entry
			currentEntry++
			offset = entryEnd
		}
	}

	return EntryPosition{}, fmt.Errorf("entry %d not found in data files (scanned %d entries)", targetEntry, currentEntry)
}

// Ack acknowledges one or more processed messages and updates consumer offset
func (c *Consumer) Ack(ctx context.Context, messageIDs ...MessageID) error {
	if len(messageIDs) == 0 {
		return nil
	}

	// Single message fast path
	if len(messageIDs) == 1 {
		messageID := messageIDs[0]
		shard, err := c.client.getOrCreateShard(messageID.ShardID)
		if err != nil {
			return err
		}

		shard.mu.Lock()
		// Update consumer offset to the next entry number
		// This ensures we won't re-read this entry
		nextEntry := messageID.EntryNumber + 1
		shard.index.ConsumerOffsets[c.group] = nextEntry
		// Mark that we need a checkpoint
		shard.writesSinceCheckpoint++
		shard.mu.Unlock()

		// Don't persist immediately - let periodic checkpoint handle it
		return nil
	}

	// Multiple messages - use batch logic
	return c.ackBatch(messageIDs)
}

// ackBatch is a helper for batch acknowledgments
func (c *Consumer) ackBatch(messageIDs []MessageID) error {
	// Group messages by shard
	shardGroups := make(map[uint32][]MessageID)
	for _, id := range messageIDs {
		shardGroups[id.ShardID] = append(shardGroups[id.ShardID], id)
	}

	// Process each shard group
	for shardID, ids := range shardGroups {
		shard, err := c.client.getOrCreateShard(shardID)
		if err != nil {
			return err
		}

		shard.mu.Lock()

		// Find the highest entry number in this shard's batch
		var maxEntry int64 = -1
		for _, id := range ids {
			if id.EntryNumber > maxEntry {
				maxEntry = id.EntryNumber
			}
		}

		// Update to one past the highest ACK'd entry
		if maxEntry >= 0 {
			shard.index.ConsumerOffsets[c.group] = maxEntry + 1
			// Mark that we need a checkpoint
			shard.writesSinceCheckpoint++
		}

		shard.mu.Unlock()
	}

	return nil
}

// GetLag returns how many entries behind this consumer group is
func (c *Consumer) GetLag(ctx context.Context, shardID uint32) (int64, error) {
	shard, err := c.client.getOrCreateShard(shardID)
	if err != nil {
		return 0, err
	}

	// Check unified state for instant change detection
	if shard.state != nil {
		currentTimestamp := shard.state.GetLastIndexUpdate()
		if currentTimestamp != shard.lastMmapCheck {
			// Index changed - reload it under write lock
			shard.mu.Lock()
			// Double-check after acquiring lock
			if currentTimestamp != shard.lastMmapCheck {
				if err := shard.loadIndexWithRecovery(); err != nil {
					shard.mu.Unlock()
					return 0, fmt.Errorf("failed to reload index after detecting mmap change: %w", err)
				}
				shard.lastMmapCheck = currentTimestamp
			}
			shard.mu.Unlock()
		}
	}

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	consumerEntry, exists := shard.index.ConsumerOffsets[c.group]
	if !exists {
		consumerEntry = 0
	}

	// Entry-based lag calculation
	lag := shard.index.CurrentEntryNumber - consumerEntry
	return lag, nil
}

// ResetOffset sets the consumer offset to a specific entry number
func (c *Consumer) ResetOffset(ctx context.Context, shardID uint32, entryNumber int64) error {
	shard, err := c.client.getOrCreateShard(shardID)
	if err != nil {
		return err
	}

	shard.mu.Lock()
	if entryNumber < 0 {
		// Negative means from end
		entryNumber = shard.index.CurrentEntryNumber + entryNumber
		if entryNumber < 0 {
			entryNumber = 0
		}
	}

	shard.index.ConsumerOffsets[c.group] = entryNumber
	// Mark that we need a checkpoint
	shard.writesSinceCheckpoint++
	shard.mu.Unlock()

	// Don't persist immediately - let periodic checkpoint handle it
	return nil
}

// AckRange acknowledges all messages in a contiguous range for a shard
// This is more efficient than individual acks for bulk processing
func (c *Consumer) AckRange(ctx context.Context, shardID uint32, fromEntry, toEntry int64) error {
	if fromEntry > toEntry {
		return fmt.Errorf("invalid range: from %d > to %d", fromEntry, toEntry)
	}

	shard, err := c.client.getOrCreateShard(shardID)
	if err != nil {
		return err
	}

	shard.mu.Lock()
	// Update to one past the end of the range
	newOffset := toEntry + 1
	currentOffset, exists := shard.index.ConsumerOffsets[c.group]

	// Only update if this advances the offset (no going backwards)
	if !exists || newOffset > currentOffset {
		shard.index.ConsumerOffsets[c.group] = newOffset
		// Mark that we need a checkpoint
		shard.writesSinceCheckpoint++
	}
	shard.mu.Unlock()

	return nil
}

// StreamStats returns statistics about a shard
// Fields ordered for optimal memory alignment
type StreamStats struct {
	// 64-bit aligned fields first
	TotalEntries int64
	TotalBytes   int64
	OldestEntry  time.Time
	NewestEntry  time.Time

	// Composite types
	ConsumerOffsets map[string]int64

	// Smaller fields last
	FileCount int    // 8 bytes
	ShardID   uint32 // 4 bytes
	// 4 bytes padding
}

// GetShardStats returns statistics for a specific shard
func (c *Consumer) GetShardStats(ctx context.Context, shardID uint32) (*StreamStats, error) {
	shard, err := c.client.getOrCreateShard(shardID)
	if err != nil {
		return nil, err
	}

	// Check unified state for instant change detection
	if shard.state != nil {
		currentTimestamp := shard.state.GetLastIndexUpdate()
		if currentTimestamp != shard.lastMmapCheck {
			// Index changed - reload it under write lock
			shard.mu.Lock()
			// Double-check after acquiring lock
			if currentTimestamp != shard.lastMmapCheck {
				if err := shard.loadIndexWithRecovery(); err != nil {
					shard.mu.Unlock()
					return nil, fmt.Errorf("failed to reload index after detecting mmap change: %w", err)
				}
				shard.lastMmapCheck = currentTimestamp
			}
			shard.mu.Unlock()
		}
	}

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	stats := &StreamStats{
		ShardID:         shardID,
		FileCount:       len(shard.index.Files),
		ConsumerOffsets: make(map[string]int64),
	}

	// Copy consumer offsets
	for group, offset := range shard.index.ConsumerOffsets {
		stats.ConsumerOffsets[group] = offset
	}

	// Calculate totals from files
	for _, file := range shard.index.Files {
		stats.TotalEntries += file.Entries
		stats.TotalBytes += file.EndOffset - file.StartOffset

		if stats.OldestEntry.IsZero() || file.StartTime.Before(stats.OldestEntry) {
			stats.OldestEntry = file.StartTime
		}

		if file.EndTime.After(stats.NewestEntry) {
			stats.NewestEntry = file.EndTime
		}
	}

	return stats, nil
}

// discoverShards discovers available shards based on stream pattern and consumer assignment
func (c *Consumer) discoverShards(streamPattern string, consumerID, consumerCount int) ([]uint32, error) {
	// Apply defaults
	if consumerCount <= 0 {
		consumerCount = 1
	}
	if consumerID < 0 || consumerID >= consumerCount {
		return nil, fmt.Errorf("invalid consumerID %d for consumerCount %d", consumerID, consumerCount)
	}

	// Parse stream pattern (e.g., "events:v1:shard:*")
	if !strings.HasSuffix(streamPattern, ":*") {
		return nil, fmt.Errorf("stream pattern must end with :* (e.g., events:v1:shard:*)")
	}

	baseStream := strings.TrimSuffix(streamPattern, "*")

	// Discover all available shards
	// Check up to 32 shards by default (can be increased if needed)
	maxShards := uint32(32)
	var allShards []uint32

	// Try to discover shards in parallel for better performance
	type result struct {
		shardID uint32
		exists  bool
	}

	results := make(chan result, maxShards)
	var wg sync.WaitGroup

	for shardID := uint32(0); shardID < maxShards; shardID++ {
		wg.Add(1)
		go func(id uint32) {
			defer wg.Done()
			streamName := fmt.Sprintf("%s%04d", baseStream, id)
			_, err := c.client.Len(context.Background(), streamName)
			results <- result{shardID: id, exists: err == nil}
		}(shardID)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	for res := range results {
		if res.exists {
			allShards = append(allShards, res.shardID)
		}
	}

	if len(allShards) == 0 {
		return nil, fmt.Errorf("no shards found for pattern %s", streamPattern)
	}

	// Sort shards for predictable assignment
	slices.Sort(allShards)

	// Assign shards to this consumer using modulo distribution
	var assignedShards []uint32
	for _, shardID := range allShards {
		if int(shardID)%consumerCount == consumerID {
			assignedShards = append(assignedShards, shardID)
		}
	}

	return assignedShards, nil
}
