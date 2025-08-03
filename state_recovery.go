package comet

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"
)

// validateAndRecoverState validates the state file and recovers from corruption
func (s *Shard) validateAndRecoverState() error {
	processID := os.Getpid()

	if s.loadState() == nil {
		return fmt.Errorf("state is nil")
	}

	state := s.loadState()
	if state == nil {
		return fmt.Errorf("state is nil after loadState")
	}

	// Validate critical fields for sanity
	lastEntry := atomic.LoadInt64(&state.LastEntryNumber)
	
	// Debug logging for CurrentEntryNumber changes
	if Debug && s.logger != nil {
		var indexBefore int64 = -1
		if s.index != nil {
			indexBefore = s.index.CurrentEntryNumber
		}
		s.logger.Debug("validateAndRecoverState: ENTRY",
			"shardID", s.shardID,
			"indexCurrentEntryNumber", indexBefore,
			"stateLastEntryNumber", lastEntry)
	}

	if s.logger != nil {
		// Safely access index fields (index may be nil during recovery)
		var indexCurrentEntryNumber int64 = -1
		var indexFileCount int = 0
		if s.index != nil {
			indexCurrentEntryNumber = s.index.CurrentEntryNumber
			indexFileCount = len(s.index.Files)
		}
		
		s.logger.Debug("validateAndRecoverState: checking state",
			"shardID", s.shardID,
			"pid", processID,
			"lastEntryNumber", lastEntry,
			"version", atomic.LoadUint64(&state.Version),
			"indexCurrentEntryNumber", indexCurrentEntryNumber,
			"indexFiles", indexFileCount)
	}

	// Check version
	version := atomic.LoadUint64(&state.Version)
	if version == 0 || version > CometStateVersion1 {
		// Version 0 means uninitialized or corrupted
		// Version > current means newer format we don't understand
		return s.recoverCorruptedState(fmt.Sprintf("invalid state version: %d", version))
	}

	// Validate critical fields for sanity
	writeOffset := atomic.LoadUint64(&state.WriteOffset)
	fileSize := atomic.LoadUint64(&state.FileSize)

	// Basic sanity checks
	if writeOffset > fileSize && fileSize > 0 {
		return s.recoverCorruptedState(fmt.Sprintf("write offset (%d) exceeds file size (%d)", writeOffset, fileSize))
	}

	// Check for impossible values that indicate corruption
	if writeOffset > 1<<40 { // 1TB - unreasonably large
		return s.recoverCorruptedState(fmt.Sprintf("write offset unreasonably large: %d", writeOffset))
	}

	// LastEntryNumber should be -1 (uninitialized) or >= 0
	if lastEntry < -1 {
		return s.recoverCorruptedState(fmt.Sprintf("invalid last entry number: %d", lastEntry))
	}

	// Check for suspicious patterns that suggest uninitialized memory
	// Common patterns: 0x55 (85), 0xAA (170), 0x155 (341), etc.
	// These are often used by memory allocators or debugging tools
	if lastEntry > 0 && (s.index == nil || len(s.index.Files) == 0) {
		// We have a non-zero entry count but no data files in our index
		// In multi-process mode, this might just mean the index hasn't been reloaded yet
		if s.statePath != "" { // Multi-process mode
			if s.logger != nil {
				s.logger.Debug("State shows entries but index has no files, reloading index",
					"lastEntry", lastEntry,
					"shard", s.shardID,
					"pid", processID)
			}

			// Try to reload the index from disk
			// Note: loadIndex requires s.mu to be held
			s.mu.Lock()
			err := s.loadIndex()
			s.mu.Unlock()
			if err != nil {
				if s.logger != nil {
					s.logger.Warn("Failed to reload index",
						"error", err,
						"shard", s.shardID)
				}
			}

			// Check again after reloading
			if s.index == nil || len(s.index.Files) == 0 {
				// Not a known pattern, but still suspicious
				if s.logger != nil {
					s.logger.Warn("Suspicious state after index reload: LastEntryNumber > 0 but no data files",
						"lastEntry", lastEntry,
						"shard", s.shardID)
				}
				// For now, trust the state in multi-process mode as another process might be writing
				// This is a trade-off for correctness over strict validation
			}
		} else {
			// Single-process mode - this is definitely suspicious
			if s.logger != nil {
				s.logger.Warn("Suspicious state: LastEntryNumber > 0 but no data files",
					"lastEntry", lastEntry,
					"shard", s.shardID)
			}
			return s.recoverCorruptedState(fmt.Sprintf("suspicious LastEntryNumber %d with no data files", lastEntry))
		}
	}

	// Validate metrics are within reasonable bounds
	totalWrites := atomic.LoadUint64(&state.TotalWrites)
	totalEntries := atomic.LoadInt64(&state.TotalEntries)

	// TotalWrites should be >= TotalEntries (can have failed writes)
	if totalEntries > 0 && totalWrites == 0 {
		// This is suspicious but not necessarily corruption
		// Log it but don't recover
	}

	// Synchronize state with index when they're out of sync
	
	// Case 1: State is uninitialized (-1) but index has data - initialize state from index
	if s.index != nil && lastEntry == -1 && s.index.CurrentEntryNumber > 0 && len(s.index.Files) > 0 {
		if s.logger != nil {
			s.logger.Info("validateAndRecoverState: initializing state from existing index",
				"shardID", s.shardID,
				"pid", processID,
				"indexCurrentEntryNumber", s.index.CurrentEntryNumber,
				"stateLastEntryNumber", lastEntry)
		}
		
		// Set state LastEntryNumber to index CurrentEntryNumber - 1
		// If index says CurrentEntryNumber=3, we have entries 0,1,2, so LastEntryNumber=2
		state := s.loadState()
		atomic.StoreInt64(&state.LastEntryNumber, s.index.CurrentEntryNumber-1)
		
		if Debug && s.logger != nil {
			s.logger.Debug("TRACE: Setting LastEntryNumber from index",
				"location", "state_recovery.go:154",
				"indexCurrentEntryNumber", s.index.CurrentEntryNumber,
				"newStateLastEntryNumber", s.index.CurrentEntryNumber-1,
				"shardID", s.shardID)
		}
	}
	
	// Case 2: State has data but index is out of sync - update index from state
	if s.index != nil && lastEntry >= 0 && s.index.CurrentEntryNumber != lastEntry + 1 && len(s.index.Files) > 0 {
		if s.logger != nil {
			s.logger.Warn("validateAndRecoverState: synchronizing index with state",
				"shardID", s.shardID,
				"pid", processID,
				"indexCurrentEntryNumber", s.index.CurrentEntryNumber,
				"stateLastEntryNumber", lastEntry,
				"indexFiles", len(s.index.Files))
		}

		// Trust the state over the index since state is updated atomically
		// State.LastEntryNumber is the last allocated entry, index.CurrentEntryNumber is the next to allocate
		if Debug && s.logger != nil {
			s.logger.Debug("TRACE: Setting CurrentEntryNumber from state recovery",
				"location", "state_recovery.go:156",
				"oldValue", s.index.CurrentEntryNumber,
				"newValue", lastEntry + 1,
				"shardID", s.shardID)
		}
		s.index.CurrentEntryNumber = lastEntry + 1

		// Also update file entries if needed
		if len(s.index.Files) > 0 {
			// Calculate what the last file's entry count should be
			lastFile := &s.index.Files[len(s.index.Files)-1]
			expectedEntries := lastEntry - lastFile.StartEntry + 1
			if expectedEntries >= 0 && expectedEntries != lastFile.Entries {
				lastFile.Entries = expectedEntries
			}
		}
	}

	// Debug logging for CurrentEntryNumber changes
	if Debug && s.logger != nil {
		var indexAfter int64 = -1
		if s.index != nil {
			indexAfter = s.index.CurrentEntryNumber
		}
		s.logger.Debug("validateAndRecoverState: EXIT",
			"shardID", s.shardID,
			"indexCurrentEntryNumber", indexAfter,
			"stateLastEntryNumber", lastEntry)
	}

	return nil
}

// recoverCorruptedState handles corrupted state by resetting to safe defaults
func (s *Shard) recoverCorruptedState(reason string) error {
	// Log the corruption
	if s.logger != nil {
		s.logger.Warn("Corrupted state detected, recovering...",
			"shard", s.shardID,
			"indexPath", s.indexPath,
			"reason", reason)
	}

	// Store recovery counts to restore after reinit
	var prevRecoveryAttempts uint64
	var prevCorruptionDetected uint64
	if state := s.loadState(); state != nil {
		prevRecoveryAttempts = atomic.LoadUint64(&state.RecoveryAttempts)
		prevCorruptionDetected = atomic.LoadUint64(&state.CorruptionDetected)
	}

	// Close and unmap current state
	if s.loadState() != nil && s.stateData != nil {
		// In mmap mode, unmap the memory
		if len(s.stateData) > 0 {
			syscall.Munmap(s.stateData)
			s.stateData = nil
		}
		s.storeState(nil)
	}

	// Rename corrupted file for investigation
	shardDir := filepath.Dir(s.indexPath)
	statePath := shardDir + "/comet.state"
	backupPath := fmt.Sprintf("%s.corrupted.%d", statePath, time.Now().Unix())
	os.Rename(statePath, backupPath)

	// Reinitialize state from scratch
	// Determine if we're in multi-process mode based on statePath
	multiProcessMode := s.statePath != ""
	if err := s.initCometState(multiProcessMode); err != nil {
		return fmt.Errorf("failed to reinitialize state after corruption: %w", err)
	}

	// Try to recover some information from the index
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.index != nil && s.loadState() != nil {
		// Restore what we can from the index
		state := s.loadState()

		// Set LastEntryNumber to the last allocated entry (CurrentEntryNumber - 1)
		// If CurrentEntryNumber=3, we have entries 0,1,2, so LastEntryNumber=2
		if s.index.CurrentEntryNumber > 0 {
			atomic.StoreInt64(&state.LastEntryNumber, s.index.CurrentEntryNumber-1)
		} else {
			atomic.StoreInt64(&state.LastEntryNumber, -1)  // No entries allocated yet
		}
		atomic.StoreUint64(&state.WriteOffset, uint64(s.index.CurrentWriteOffset))
		atomic.StoreUint64(&state.CurrentFiles, uint64(len(s.index.Files)))

		// Set file index based on current file
		if s.index.CurrentFile != "" {
			// Extract file index from filename
			base := filepath.Base(s.index.CurrentFile)
			var fileIndex uint64
			if _, err := fmt.Sscanf(base, "log-%016d.comet", &fileIndex); err == nil {
				atomic.StoreUint64(&state.ActiveFileIndex, fileIndex)
			}
		}

		// Restore and increment recovery metrics
		atomic.StoreUint64(&state.RecoveryAttempts, prevRecoveryAttempts+1)
		atomic.StoreUint64(&state.CorruptionDetected, prevCorruptionDetected+1)
		atomic.AddUint64(&state.RecoverySuccesses, 1)
	}

	return nil
}

// migrateStateVersion handles upgrading state from older versions
func (s *Shard) migrateStateVersion(fromVersion, toVersion uint64) error {
	// For now, we only have version 1
	// Future versions would add migration logic here

	switch fromVersion {
	case 0:
		// Version 0 -> 1: Initialize all fields to defaults
		return s.recoverCorruptedState("migrating from version 0")
	default:
		return fmt.Errorf("unknown state version for migration: %d", fromVersion)
	}
}
