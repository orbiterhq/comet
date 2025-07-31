package comet

import (
	"context"
	"encoding/binary"
	"fmt"
	"maps"
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

	currentPos := startPos
	currentEntry := startEntry
	maxScanEntries := targetEntry - startEntry + 100 // Safety limit
	scannedEntries := int64(0)

	// Scan forward entry by entry until we reach the target
	for currentEntry < targetEntry {
		// Safety check to prevent infinite loops
		scannedEntries++
		if scannedEntries > maxScanEntries {
			return EntryPosition{}, fmt.Errorf("scan limit exceeded: scanned %d entries looking for entry %d from %d", scannedEntries, targetEntry, startEntry)
		}

		// Validate file index under lock
		if currentPos.FileIndex >= len(reader.files) {
			return EntryPosition{}, fmt.Errorf("file index %d out of range (have %d files)", currentPos.FileIndex, len(reader.files))
		}

		file := reader.files[currentPos.FileIndex]
		fileData := file.data.Load() // Get data atomically
		if fileData == nil {
			return EntryPosition{}, fmt.Errorf("file %d has no data", currentPos.FileIndex)
		}

		// Check if we have enough data for the header
		if currentPos.ByteOffset+12 > int64(len(fileData)) {
			// Move to next file if available
			if currentPos.FileIndex+1 < len(reader.files) {
				currentPos.FileIndex++
				currentPos.ByteOffset = 0
				continue
			} else {
				return EntryPosition{}, fmt.Errorf("entry header extends beyond final file (fileData len=%d, offset=%d)", len(fileData), currentPos.ByteOffset)
			}
		}

		// Read length from header (safe under reader lock)
		header := fileData[currentPos.ByteOffset : currentPos.ByteOffset+12]
		length := binary.LittleEndian.Uint32(header[0:4])

		// Validate entry length is reasonable
		if length > 100*1024*1024 { // 100MB max entry size
			return EntryPosition{}, fmt.Errorf("entry %d has invalid length %d at file %d offset %d", currentEntry, length, currentPos.FileIndex, currentPos.ByteOffset)
		}

		// Check if the full entry fits in current file
		entryEnd := currentPos.ByteOffset + 12 + int64(length)
		if entryEnd > int64(len(fileData)) {
			// Move to next file if available
			if currentPos.FileIndex+1 < len(reader.files) {
				currentPos.FileIndex++
				currentPos.ByteOffset = 0
				continue
			} else {
				return EntryPosition{}, fmt.Errorf("entry %d data extends beyond final file", currentEntry)
			}
		}

		// Move to next entry
		currentEntry++
		currentPos.ByteOffset = entryEnd

		// Check if we've moved beyond the current file's data
		if currentPos.ByteOffset >= int64(len(fileData)) {
			// Move to next file if one exists
			if currentPos.FileIndex+1 < len(reader.files) {
				currentPos.FileIndex++
				currentPos.ByteOffset = 0
			} else {
				// We've reached the actual end
				break
			}
		}
	}

	return currentPos, nil
}

// readFromShard reads entries from a single shard using entry-based positioning
func (c *Consumer) readFromShard(ctx context.Context, shard *Shard, maxCount int) ([]StreamMessage, error) {
	// Lock-free reader tracking
	atomic.AddInt64(&shard.readerCount, 1)
	defer atomic.AddInt64(&shard.readerCount, -1)

	// Check mmap state for instant change detection
	if shard.mmapState != nil {
		currentTimestamp := atomic.LoadInt64(&shard.mmapState.LastUpdateNanos)
		if currentTimestamp != shard.lastMmapCheck {
			// Index changed - reload it under write lock
			shard.mu.Lock()
			// Double-check after acquiring lock
			if currentTimestamp != shard.lastMmapCheck {
				if err := shard.loadIndex(); err != nil {
					shard.mu.Unlock()
					return nil, fmt.Errorf("failed to reload index after detecting mmap change: %w", err)
				}
				shard.lastMmapCheck = currentTimestamp

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
	endEntryNum := shard.index.CurrentEntryNumber
	fileCount := len(shard.index.Files)
	shard.mu.RUnlock()

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
			// In multi-process mode, some entries might not exist due to gaps in the sequence
			// Skip these entries instead of failing the entire read
			if strings.Contains(err.Error(), "does not exist") {
				continue
			}
			return nil, fmt.Errorf("failed to find entry %d position in shard %d: %w", entryNum, shard.shardID, err)
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
			return nil, fmt.Errorf("failed to read entry %d from shard %d: %w", entryNum, shard.shardID, err)
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

	// Check mmap state for instant change detection
	if shard.mmapState != nil {
		currentTimestamp := atomic.LoadInt64(&shard.mmapState.LastUpdateNanos)
		if currentTimestamp != shard.lastMmapCheck {
			// Index changed - reload it under write lock
			shard.mu.Lock()
			// Double-check after acquiring lock
			if currentTimestamp != shard.lastMmapCheck {
				if err := shard.loadIndex(); err != nil {
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

	// Check mmap state for instant change detection
	if shard.mmapState != nil {
		currentTimestamp := atomic.LoadInt64(&shard.mmapState.LastUpdateNanos)
		if currentTimestamp != shard.lastMmapCheck {
			// Index changed - reload it under write lock
			shard.mu.Lock()
			// Double-check after acquiring lock
			if currentTimestamp != shard.lastMmapCheck {
				if err := shard.loadIndex(); err != nil {
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
