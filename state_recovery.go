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
	if s.state == nil {
		return fmt.Errorf("state is nil")
	}

	// Check version
	version := atomic.LoadUint64(&s.state.Version)
	if version == 0 || version > CometStateVersion1 {
		// Version 0 means uninitialized or corrupted
		// Version > current means newer format we don't understand
		return s.recoverCorruptedState(fmt.Sprintf("invalid state version: %d", version))
	}

	// Validate critical fields for sanity
	writeOffset := atomic.LoadUint64(&s.state.WriteOffset)
	fileSize := atomic.LoadUint64(&s.state.FileSize)
	lastEntry := atomic.LoadInt64(&s.state.LastEntryNumber)

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

	// Validate metrics are within reasonable bounds
	totalWrites := atomic.LoadUint64(&s.state.TotalWrites)
	totalEntries := atomic.LoadInt64(&s.state.TotalEntries)

	// TotalWrites should be >= TotalEntries (can have failed writes)
	if totalEntries > 0 && totalWrites == 0 {
		// This is suspicious but not necessarily corruption
		// Log it but don't recover
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
	if s.state != nil {
		prevRecoveryAttempts = atomic.LoadUint64(&s.state.RecoveryAttempts)
		prevCorruptionDetected = atomic.LoadUint64(&s.state.CorruptionDetected)
	}

	// Close and unmap current state
	if s.state != nil && s.stateData != nil {
		// In mmap mode, unmap the memory
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
	// Determine if we're in multi-process mode based on statePath
	multiProcessMode := s.statePath != ""
	if err := s.initCometState(multiProcessMode); err != nil {
		return fmt.Errorf("failed to reinitialize state after corruption: %w", err)
	}

	// Try to recover some information from the index
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.index != nil && s.state != nil {
		// Restore what we can from the index
		atomic.StoreInt64(&s.state.LastEntryNumber, s.index.CurrentEntryNumber)
		atomic.StoreUint64(&s.state.WriteOffset, uint64(s.index.CurrentWriteOffset))
		atomic.StoreUint64(&s.state.CurrentFiles, uint64(len(s.index.Files)))

		// Set file index based on current file
		if s.index.CurrentFile != "" {
			// Extract file index from filename
			base := filepath.Base(s.index.CurrentFile)
			var fileIndex uint64
			if _, err := fmt.Sscanf(base, "log-%016d.comet", &fileIndex); err == nil {
				atomic.StoreUint64(&s.state.ActiveFileIndex, fileIndex)
			}
		}

		// Restore and increment recovery metrics
		atomic.StoreUint64(&s.state.RecoveryAttempts, prevRecoveryAttempts+1)
		atomic.StoreUint64(&s.state.CorruptionDetected, prevCorruptionDetected+1)
		atomic.AddUint64(&s.state.RecoverySuccesses, 1)
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
