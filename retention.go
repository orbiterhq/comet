package comet

import (
	"maps"
	"os"
	"sort"
	"sync/atomic"
	"syscall"
	"time"
)

// RetentionStats provides type-safe retention statistics
// Fields ordered for optimal memory alignment
type RetentionStats struct {
	// 8-byte aligned fields first
	TotalSizeBytes int64         `json:"total_size_bytes"`
	TotalSizeGB    float64       `json:"total_size_gb"`
	MaxTotalSizeGB float64       `json:"max_total_size_gb"`
	RetentionAge   time.Duration `json:"retention_age"`
	TotalFiles     int           `json:"total_files"`

	// Pointer fields (8 bytes)
	ShardStats map[uint32]ShardRetentionStats `json:"shard_stats,omitempty"`

	// Larger composite types last (time.Time is 24 bytes)
	OldestData time.Time `json:"oldest_data"`
	NewestData time.Time `json:"newest_data"`
}

// ShardRetentionStats provides retention stats for a single shard
// Fields ordered for optimal memory alignment
type ShardRetentionStats struct {
	// 8-byte aligned fields first
	SizeBytes int64 `json:"size_bytes"`
	Files     int   `json:"files"`

	// Pointer field (8 bytes)
	ConsumerLag map[string]int64 `json:"consumer_lag,omitempty"`

	// Larger composite types last (time.Time is 24 bytes)
	OldestEntry time.Time `json:"oldest_entry"`
	NewestEntry time.Time `json:"newest_entry"`
}

// startRetentionManager starts the background retention process
func (c *Client) startRetentionManager() {
	if c.config.Retention.CleanupInterval <= 0 {
		// Retention disabled
		return
	}

	c.retentionWg.Add(1)
	go func() {
		defer c.retentionWg.Done()

		ticker := time.NewTicker(c.config.Retention.CleanupInterval)
		defer ticker.Stop()

		// Run initial cleanup after short delay
		// Use cleanup interval as initial delay to avoid hardcoded 10s
		initialDelay := c.config.Retention.CleanupInterval
		if initialDelay > 10*time.Second {
			initialDelay = 10 * time.Second
		}
		initialTimer := time.NewTimer(initialDelay)
		select {
		case <-initialTimer.C:
			c.runRetentionCleanup()
		case <-c.stopCh:
			initialTimer.Stop()
			return
		}

		// Run periodic cleanup
		for {
			select {
			case <-ticker.C:
				c.runRetentionCleanup()
			case <-c.stopCh:
				return
			}
		}
	}()
}

// runRetentionCleanup performs a single cleanup pass
func (c *Client) runRetentionCleanup() {
	start := time.Now()

	c.mu.RLock()
	shards := make([]*Shard, 0, len(c.shards))
	for _, shard := range c.shards {
		shards = append(shards, shard)
		// Track retention run in each shard's metrics
		if shard.state != nil {
			atomic.AddUint64(&shard.state.RetentionRuns, 1)
			atomic.StoreInt64(&shard.state.LastRetentionNanos, start.UnixNano())
		}
	}
	c.mu.RUnlock()

	var totalSize int64
	for _, shard := range shards {
		size := c.cleanupShard(shard)
		totalSize += size
	}

	// Check total size limit
	if c.config.Retention.MaxTotalSize > 0 && totalSize > c.config.Retention.MaxTotalSize {
		// Need to delete more files to get under total limit
		c.enforceGlobalSizeLimit(shards, totalSize)
	}

	// Update retention time metrics
	duration := time.Since(start)
	for _, shard := range shards {
		if shard.state != nil {
			atomic.AddInt64(&shard.state.RetentionTimeNanos, duration.Nanoseconds())
		}
	}

	// Debug log retention summary
	if Debug && c.logger != nil {
		var totalFiles, deletedFiles int
		for _, shard := range shards {
			shard.mu.RLock()
			totalFiles += len(shard.index.Files)
			shard.mu.RUnlock()
			if shard.state != nil {
				deletedFiles += int(atomic.LoadUint64(&shard.state.FilesDeleted))
			}
		}
		c.logger.Debug("Retention cleanup completed",
			"shards", len(shards),
			"totalFiles", totalFiles,
			"totalSizeMB", totalSize/(1<<20),
			"duration", duration,
			"maxAge", c.config.Retention.MaxAge,
			"maxSizeMB", c.config.Retention.MaxTotalSize/(1<<20))
	}
}

// cleanupShard cleans up old files in a single shard
func (c *Client) cleanupShard(shard *Shard) int64 {
	// For multi-process safety, acquire retention lock first
	if c.config.Concurrency.EnableMultiProcessMode && shard.retentionLockFile != nil {
		// Use non-blocking try-lock to avoid hanging if another process is doing retention
		if err := syscall.Flock(int(shard.retentionLockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
			// Another process is doing retention - skip this cleanup
			// Return current shard size without cleanup
			shard.mu.RLock()
			var currentSize int64
			for _, file := range shard.index.Files {
				currentSize += file.EndOffset - file.StartOffset
			}
			shard.mu.RUnlock()
			return currentSize
		}
		// Ensure we release the lock when done
		defer syscall.Flock(int(shard.retentionLockFile.Fd()), syscall.LOCK_UN)
	}

	shard.mu.RLock()
	files := make([]FileInfo, len(shard.index.Files))
	copy(files, shard.index.Files)
	currentFile := shard.index.CurrentFile
	consumerOffsets := make(map[string]int64)
	maps.Copy(consumerOffsets, shard.index.ConsumerOffsets)
	shard.mu.RUnlock()

	// Calculate current state
	var shardSize int64
	var oldestProtectedEntry int64 = -1
	now := time.Now()

	// Find oldest consumer position if protecting unconsumed
	if c.config.Retention.ProtectUnconsumed {
		for _, offset := range consumerOffsets {
			if oldestProtectedEntry == -1 || offset < oldestProtectedEntry {
				oldestProtectedEntry = offset
			}
		}
	}

	// Analyze files for deletion
	filesToDelete := []FileInfo{}
	filesToKeep := []FileInfo{}

	// Count total non-current files first
	totalNonCurrentFiles := 0
	for _, file := range files {
		if file.Path != currentFile {
			totalNonCurrentFiles++
		}
	}

	for i, file := range files {
		fileSize := file.EndOffset - file.StartOffset
		shardSize += fileSize

		// Never delete the current file
		if file.Path == currentFile {
			filesToKeep = append(filesToKeep, file)
			continue
		}

		// Check if we should delete this file
		shouldDelete := false

		// Time-based deletion
		if c.config.Retention.MaxAge > 0 && now.Sub(file.EndTime) > c.config.Retention.MaxAge {
			shouldDelete = true
		}

		// Force delete after time
		if c.config.Retention.ForceDeleteAfter > 0 && now.Sub(file.EndTime) > c.config.Retention.ForceDeleteAfter {
			shouldDelete = true
			oldestProtectedEntry = -1 // Ignore consumer protection
		}

		// Check if file has active readers
		if shouldDelete && atomic.LoadInt64(&shard.readerCount) > 0 {
			// Skip files that might have active readers
			// This is conservative - we could track per-file readers for more precision
			if i == 0 || i == len(files)-1 {
				shouldDelete = false
			}
		}

		// Check consumer protection
		if shouldDelete && oldestProtectedEntry >= 0 {
			// Check if any consumer still needs this file
			fileLastEntry := file.StartEntry + file.Entries - 1
			if fileLastEntry >= oldestProtectedEntry {
				shouldDelete = false
				// Track files protected by consumers
				if shard.state != nil {
					atomic.AddUint64(&shard.state.ProtectedByConsumers, 1)
				}
			}
		}

		// Enforce minimum files - check against total files, not just files already processed
		remainingFiles := totalNonCurrentFiles - len(filesToDelete)
		if shouldDelete && remainingFiles <= c.config.Retention.MinFilesToKeep {
			shouldDelete = false
		}

		if shouldDelete {
			filesToDelete = append(filesToDelete, file)
		} else {
			filesToKeep = append(filesToKeep, file)
		}
	}

	// Size-based deletion (if we're over the shard limit)
	if c.config.Retention.MaxShardSize > 0 && shardSize > c.config.Retention.MaxShardSize {
		// Sort kept files by age (oldest first)
		sort.Slice(filesToKeep, func(i, j int) bool {
			return filesToKeep[i].EndTime.Before(filesToKeep[j].EndTime)
		})

		targetSize := shardSize
		for i := 0; i < len(filesToKeep) && targetSize > c.config.Retention.MaxShardSize; i++ {
			file := filesToKeep[i]

			// Skip current file and minimum required files
			if file.Path == currentFile || len(filesToKeep)-i <= c.config.Retention.MinFilesToKeep {
				continue
			}

			// Move to delete list
			filesToDelete = append(filesToDelete, file)
			targetSize -= (file.EndOffset - file.StartOffset)

			// Remove from keep list
			filesToKeep = append(filesToKeep[:i], filesToKeep[i+1:]...)
			i-- // Adjust index after removal
		}
	}

	// Actually delete the files
	if len(filesToDelete) > 0 {
		c.deleteFiles(shard, filesToDelete, &c.metrics)
	}

	// Update oldest entry timestamp
	if shard.state != nil {
		if len(filesToKeep) > 0 {
			// Find the oldest file that remains
			oldestTime := filesToKeep[0].StartTime
			for _, file := range filesToKeep {
				if file.StartTime.Before(oldestTime) && !file.StartTime.IsZero() {
					oldestTime = file.StartTime
				}
			}
			// Only set if we found a valid timestamp
			if !oldestTime.IsZero() {
				atomic.StoreInt64(&shard.state.OldestEntryNanos, oldestTime.UnixNano())
			}
		} else if len(shard.index.Files) > 0 {
			// If we're keeping all files (no deletion), still update the metric
			oldestTime := time.Time{}
			for _, file := range shard.index.Files {
				if !file.StartTime.IsZero() && (oldestTime.IsZero() || file.StartTime.Before(oldestTime)) {
					oldestTime = file.StartTime
				}
			}
			// Only set if we found a valid timestamp
			if !oldestTime.IsZero() {
				atomic.StoreInt64(&shard.state.OldestEntryNanos, oldestTime.UnixNano())
			}
		}
	}

	// Return remaining size
	var remainingSize int64
	for _, file := range filesToKeep {
		remainingSize += file.EndOffset - file.StartOffset
	}
	return remainingSize
}

// deleteFiles removes files from disk and updates the shard index
// CRITICAL: Updates index BEFORE deleting files to prevent readers from accessing deleted files
func (c *Client) deleteFiles(shard *Shard, files []FileInfo, metrics *ClientMetrics) {
	if len(files) == 0 {
		return
	}

	// STEP 1: Update the shard index FIRST to prevent readers from accessing files we're about to delete
	// For multi-process safety, use the index lock
	if c.config.Concurrency.EnableMultiProcessMode && shard.indexLockFile != nil {
		// Acquire exclusive lock for index writes
		if err := syscall.Flock(int(shard.indexLockFile.Fd()), syscall.LOCK_EX); err != nil {
			// Failed to acquire index lock - skip deletion to avoid corruption
			return
		}
		defer syscall.Flock(int(shard.indexLockFile.Fd()), syscall.LOCK_UN)
	}

	shard.mu.Lock()

	// Create a map of files to delete for quick lookup
	deletedMap := make(map[string]bool)
	for _, file := range files {
		deletedMap[file.Path] = true
	}

	// Filter out files to be deleted from the index
	newFiles := make([]FileInfo, 0, len(shard.index.Files))
	for _, file := range shard.index.Files {
		if !deletedMap[file.Path] {
			newFiles = append(newFiles, file)
		}
	}

	shard.index.Files = newFiles

	// Clean up entry boundaries for deleted files
	// Clean up binary index to remove entries that reference deleted files
	// Must be done while holding the lock!
	if len(files) > 0 {
		minDeletedEntry := files[0].StartEntry

		// Filter binary index nodes
		newNodes := make([]EntryIndexNode, 0)
		for _, node := range shard.index.BinaryIndex.Nodes {
			if node.EntryNumber < minDeletedEntry {
				newNodes = append(newNodes, node)
			}
		}
		shard.index.BinaryIndex.Nodes = newNodes
	}

	// Persist the updated index while still holding the lock
	shard.persistIndex()

	// Now we can release the lock - all index modifications are complete
	shard.mu.Unlock()

	// STEP 2: Now delete the physical files - readers can no longer find them in the index
	deletedCount := 0
	var bytesReclaimed uint64
	var entriesDeleted uint64

	for _, file := range files {
		err := os.Remove(file.Path)
		if err != nil && !os.IsNotExist(err) {
			// Log error but continue - file may have been deleted by another process
			// Track retention errors
			if shard.state != nil {
				atomic.AddUint64(&shard.state.RetentionErrors, 1)
			}
			continue
		}
		deletedCount++
		bytesReclaimed += uint64(file.EndOffset - file.StartOffset)
		entriesDeleted += uint64(file.Entries)
	}

	// Update retention metrics
	if shard.state != nil && deletedCount > 0 {
		atomic.AddUint64(&shard.state.FilesDeleted, uint64(deletedCount))
		atomic.AddUint64(&shard.state.BytesReclaimed, bytesReclaimed)
		atomic.AddUint64(&shard.state.EntriesDeleted, entriesDeleted)
	}
}

// enforceGlobalSizeLimit deletes files across all shards to meet total size limit
func (c *Client) enforceGlobalSizeLimit(shards []*Shard, currentTotal int64) {
	if currentTotal <= c.config.Retention.MaxTotalSize {
		return
	}

	// Collect all files from all shards with their metadata
	type FileWithShard struct {
		shard *Shard
		file  FileInfo
	}

	var allFiles []FileWithShard
	for _, shard := range shards {
		shard.mu.RLock()
		for _, file := range shard.index.Files {
			if file.Path != shard.index.CurrentFile {
				allFiles = append(allFiles, FileWithShard{shard: shard, file: file})
			}
		}
		shard.mu.RUnlock()
	}

	// Sort by age (oldest first)
	sort.Slice(allFiles, func(i, j int) bool {
		return allFiles[i].file.EndTime.Before(allFiles[j].file.EndTime)
	})

	// Delete oldest files until we're under the limit
	bytesToDelete := currentTotal - c.config.Retention.MaxTotalSize
	deletionMap := make(map[*Shard][]FileInfo)

	for _, fw := range allFiles {
		if bytesToDelete <= 0 {
			break
		}

		fileSize := fw.file.EndOffset - fw.file.StartOffset
		deletionMap[fw.shard] = append(deletionMap[fw.shard], fw.file)
		bytesToDelete -= fileSize
	}

	// Perform deletions
	for shard, files := range deletionMap {
		c.deleteFiles(shard, files, &c.metrics)
	}
}

// ForceRetentionCleanup forces an immediate retention cleanup pass
// This is primarily useful for testing retention behavior
func (c *Client) ForceRetentionCleanup() {
	if c.config.Retention.CleanupInterval <= 0 {
		// Retention is disabled
		return
	}
	c.runRetentionCleanup()
}

// GetRetentionStats returns current retention statistics
func (c *Client) GetRetentionStats() *RetentionStats {
	stats := &RetentionStats{
		RetentionAge:   c.config.Retention.MaxAge,
		MaxTotalSizeGB: float64(c.config.Retention.MaxTotalSize) / (1 << 30),
		ShardStats:     make(map[uint32]ShardRetentionStats),
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	for shardID, shard := range c.shards {
		shard.mu.RLock()

		shardStat := ShardRetentionStats{
			Files:       len(shard.index.Files),
			ConsumerLag: make(map[string]int64),
		}

		// Calculate shard size and time range
		for _, file := range shard.index.Files {
			size := file.EndOffset - file.StartOffset
			shardStat.SizeBytes += size
			stats.TotalSizeBytes += size

			if shardStat.OldestEntry.IsZero() || file.StartTime.Before(shardStat.OldestEntry) {
				shardStat.OldestEntry = file.StartTime
			}
			// For the current file, use current time as end time
			endTime := file.EndTime
			if file.Path == shard.index.CurrentFile && (endTime.IsZero() || endTime.Before(file.StartTime)) {
				endTime = time.Now()
			}

			if endTime.After(shardStat.NewestEntry) {
				shardStat.NewestEntry = endTime
			}

			// Update global oldest/newest
			if stats.OldestData.IsZero() || file.StartTime.Before(stats.OldestData) {
				stats.OldestData = file.StartTime
			}
			if endTime.After(stats.NewestData) {
				stats.NewestData = endTime
			}
		}

		// Calculate consumer lag for this shard
		for group, offset := range shard.index.ConsumerOffsets {
			lag := shard.index.CurrentEntryNumber - offset
			if lag > 0 {
				shardStat.ConsumerLag[group] = lag
			}
		}

		stats.TotalFiles += shardStat.Files
		stats.ShardStats[shardID] = shardStat

		shard.mu.RUnlock()
	}

	stats.TotalSizeGB = float64(stats.TotalSizeBytes) / (1 << 30)

	return stats
}
