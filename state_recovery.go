package comet

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"
)

// validateAndRecoverState validates the state file and recovers from corruption
func (s *Shard) validateAndRecoverState() error {
	if s.state == nil {
		return fmt.Errorf("state is nil")
	}

	state := s.state
	lastEntry := atomic.LoadInt64(&state.LastEntryNumber)

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

	// Since processes own their shards exclusively, we can trust the state
	// If there's a mismatch between state and index, it will be resolved below

	// Since processes own shards exclusively, we don't need complex metric validation

	// Synchronize state with index when they're out of sync

	// Case 1: State is uninitialized (-1) but index has data - initialize state from index
	if s.index != nil && lastEntry == -1 && s.index.CurrentEntryNumber > 0 && len(s.index.Files) > 0 {
		if s.logger != nil {
			s.logger.Debug("validateAndRecoverState: initializing state from existing index",
				"shardID", s.shardID,
				"indexCurrentEntryNumber", s.index.CurrentEntryNumber,
				"stateLastEntryNumber", lastEntry)
		}

		// Set state LastEntryNumber to index CurrentEntryNumber - 1
		// If index says CurrentEntryNumber=3, we have entries 0,1,2, so LastEntryNumber=2
		state := s.state
		atomic.StoreInt64(&state.LastEntryNumber, s.index.CurrentEntryNumber-1)

		if IsDebug() && s.logger != nil {
			s.logger.Debug("TRACE: Setting LastEntryNumber from index",
				"location", "state_recovery.go:154",
				"indexCurrentEntryNumber", s.index.CurrentEntryNumber,
				"newStateLastEntryNumber", s.index.CurrentEntryNumber-1,
				"shardID", s.shardID)
		}
	}

	// Case 2: State has data but index is out of sync - update index from state
	if s.index != nil && lastEntry >= 0 && s.index.CurrentEntryNumber != lastEntry+1 && len(s.index.Files) > 0 {
		if s.logger != nil {
			s.logger.Warn("validateAndRecoverState: synchronizing index with state",
				"shardID", s.shardID,
				"indexCurrentEntryNumber", s.index.CurrentEntryNumber,
				"stateLastEntryNumber", lastEntry,
				"indexFiles", len(s.index.Files))
		}

		// Trust the state over the index since state is updated atomically
		// State.LastEntryNumber is the last allocated entry, index.CurrentEntryNumber is the next to allocate
		if IsDebug() && s.logger != nil {
			s.logger.Debug("TRACE: Setting CurrentEntryNumber from state recovery",
				"location", "state_recovery.go:156",
				"oldValue", s.index.CurrentEntryNumber,
				"newValue", lastEntry+1,
				"shardID", s.shardID)
		}
		s.index.CurrentEntryNumber = lastEntry + 1

		// Trust the state - no need to adjust file entries
	}

	return nil
}

// recoverCorruptedState handles corrupted state by resetting to safe defaults
func (s *Shard) recoverCorruptedState(reason string) error {
	if s.logger != nil {
		s.logger.Warn("Corrupted state detected, recovering...",
			"shard", s.shardID,
			"reason", reason)
	}

	// Close and unmap current state
	if s.state != nil && s.stateData != nil {
		if len(s.stateData) > 0 {
			syscall.Munmap(s.stateData)
			s.stateData = nil
		}
		s.state = nil
	}

	// Rename corrupted file for investigation
	shardDir := filepath.Dir(s.indexPath)
	statePath := shardDir + "/comet.state"
	backupPath := fmt.Sprintf("%s.corrupted.%d", statePath, time.Now().Unix())
	os.Rename(statePath, backupPath)

	// Reinitialize state from scratch
	multiProcessMode := s.statePath != ""
	if err := s.initCometState(multiProcessMode); err != nil {
		return fmt.Errorf("failed to reinitialize state after corruption: %w", err)
	}

	// Since processes own shards exclusively, we can trust the index to restore state
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.index != nil && s.state != nil {
		state := s.state
		// Restore critical fields from index
		if s.index.CurrentEntryNumber > 0 {
			atomic.StoreInt64(&state.LastEntryNumber, s.index.CurrentEntryNumber-1)
		} else {
			atomic.StoreInt64(&state.LastEntryNumber, -1)
		}
		atomic.StoreUint64(&state.WriteOffset, uint64(s.index.CurrentWriteOffset))

		// Update recovery metrics
		atomic.AddUint64(&state.RecoveryAttempts, 1)
		atomic.AddUint64(&state.RecoverySuccesses, 1)
		atomic.AddUint64(&state.CorruptionDetected, 1)

		// Restore ActiveFileIndex from index if we have files
		if len(s.index.Files) > 0 && s.index.CurrentFile != "" {
			// Extract file index from last file name (log-NNNNNNNNNNNNNNNN.comet)
			lastFile := s.index.Files[len(s.index.Files)-1].Path
			if matches := regexp.MustCompile(`log-(\d+)\.comet`).FindStringSubmatch(lastFile); len(matches) > 1 {
				if fileIndex, err := strconv.ParseUint(matches[1], 10, 64); err == nil {
					atomic.StoreUint64(&state.ActiveFileIndex, fileIndex)
				}
			}
		}
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
