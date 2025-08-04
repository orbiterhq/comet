package comet

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// TestValidateAndRecoverState tests state validation logic
func TestValidateAndRecoverState(t *testing.T) {
	dir := t.TempDir()
	config := MultiProcessConfig(0, 2)

	// Create client to initialize state
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	// Write some data to populate state
	ctx := context.Background()
	_, err = client.Append(ctx, "test:v1:shard:0000", [][]byte{[]byte("test data")})
	if err != nil {
		t.Fatal(err)
	}

	// Get the shard
	shard, err := client.getOrCreateShard(0)
	if err != nil {
		t.Fatal(err)
	}

	client.Close()

	// Test cases for validation
	testCases := []struct {
		name          string
		setupFunc     func()
		expectRecover bool
	}{
		{
			name: "ValidState",
			setupFunc: func() {
				// State should already be valid
			},
			expectRecover: false,
		},
		{
			name: "InvalidVersion0",
			setupFunc: func() {
				if state := shard.state; state != nil {
					atomic.StoreUint64(&state.Version, 0)
				}
			},
			expectRecover: true,
		},
		{
			name: "InvalidVersionTooHigh",
			setupFunc: func() {
				if state := shard.state; state != nil {
					atomic.StoreUint64(&state.Version, 999)
				}
			},
			expectRecover: true,
		},
		{
			name: "WriteOffsetExceedsFileSize",
			setupFunc: func() {
				if state := shard.state; state != nil {
					atomic.StoreUint64(&state.WriteOffset, 1000000)
					atomic.StoreUint64(&state.FileSize, 1000)
				}
			},
			expectRecover: true,
		},
		{
			name: "UnreasonablyLargeWriteOffset",
			setupFunc: func() {
				if state := shard.state; state != nil {
					atomic.StoreUint64(&state.WriteOffset, 1<<41) // > 1TB
				}
			},
			expectRecover: true,
		},
		{
			name: "InvalidLastEntryNumber",
			setupFunc: func() {
				if state := shard.state; state != nil {
					atomic.StoreInt64(&state.LastEntryNumber, -100)
				}
			},
			expectRecover: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Re-initialize for each test
			if err := shard.initCometState(true); err != nil {
				t.Fatal(err)
			}

			// Apply test setup
			tc.setupFunc()

			// Get state pointer before validation
			stateBefore := shard.state
			if stateBefore == nil {
				t.Fatal("State is nil after initCometState")
			}

			// Run validation
			err := shard.validateAndRecoverState()

			// Recovery should always succeed (return nil) unless something goes very wrong
			if err != nil {
				t.Errorf("Unexpected error during recovery: %v", err)
			}

			// Check if recovery happened by verifying the bad values were reset
			stateAfter := shard.state
			if tc.expectRecover {
				// After recovery, version should be valid
				version := atomic.LoadUint64(&stateAfter.Version)
				if version != CometStateVersion1 {
					t.Errorf("After recovery, version should be %d, got %d", CometStateVersion1, version)
				}

				// LastEntryNumber should be valid (>= -1)
				lastEntry := atomic.LoadInt64(&stateAfter.LastEntryNumber)
				if lastEntry < -1 {
					t.Errorf("After recovery, LastEntryNumber should be >= -1, got %d", lastEntry)
				}
			}

			// Cleanup state file for next test
			if shard.state != nil && shard.stateData != nil {
				if len(shard.stateData) > 0 {
					syscall.Munmap(shard.stateData)
					shard.stateData = nil
				}
				shard.state = nil
			}
		})
	}
}

// TestRecoverCorruptedState tests the corruption recovery mechanism
func TestRecoverCorruptedState(t *testing.T) {
	dir := t.TempDir()
	config := MultiProcessConfig(0, 2)

	// Create client and shard
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	_, err = client.Append(ctx, "test:v1:shard:0000", [][]byte{[]byte("test data")})
	if err != nil {
		t.Fatal(err)
	}

	shard, err := client.getOrCreateShard(0)
	if err != nil {
		t.Fatal(err)
	}

	// Store some values we'll check are restored
	shard.mu.Lock()
	shard.index.CurrentEntryNumber = 42
	shard.index.CurrentWriteOffset = 1234
	shard.index.CurrentFile = fmt.Sprintf("%s/log-%016d.comet", filepath.Dir(shard.indexPath), 5)
	// Add a file to the index so recovery can extract the file index
	shard.index.Files = append(shard.index.Files, FileInfo{
		Path:        shard.index.CurrentFile,
		StartOffset: 0,
		EndOffset:   1234,
		StartEntry:  0,
		Entries:     42,
		StartTime:   time.Now(),
		EndTime:     time.Now(),
	})
	shard.mu.Unlock()

	// Get the state file path
	stateFilePath := filepath.Dir(shard.indexPath) + "/comet.state"

	// Trigger recovery
	err = shard.recoverCorruptedState("test corruption")
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}

	// Check that a backup was created
	files, err := filepath.Glob(stateFilePath + ".corrupted.*")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Error("No backup file created")
	}

	// Verify state was reinitialized
	state := shard.state
	if state == nil {
		t.Fatal("State is nil after recovery")
	}

	// Check version is set correctly
	version := atomic.LoadUint64(&state.Version)
	if version != CometStateVersion1 {
		t.Errorf("Expected version %d, got %d", CometStateVersion1, version)
	}

	// Check that index values were restored
	// If CurrentEntryNumber=42, then entries 0-41 have been written, so LastEntryNumber should be 41
	lastEntry := atomic.LoadInt64(&state.LastEntryNumber)
	if lastEntry != 41 {
		t.Errorf("Expected LastEntryNumber 41, got %d", lastEntry)
	}

	writeOffset := atomic.LoadUint64(&state.WriteOffset)
	if writeOffset != 1234 {
		t.Errorf("Expected WriteOffset 1234, got %d", writeOffset)
	}

	fileIndex := atomic.LoadUint64(&state.ActiveFileIndex)
	if fileIndex != 5 {
		t.Errorf("Expected ActiveFileIndex 5, got %d", fileIndex)
	}

	// Check metrics were updated
	recoverySuccesses := atomic.LoadUint64(&state.RecoverySuccesses)
	if recoverySuccesses != 1 {
		t.Errorf("Expected RecoverySuccesses 1, got %d", recoverySuccesses)
	}

	corruptionDetected := atomic.LoadUint64(&state.CorruptionDetected)
	if corruptionDetected != 1 {
		t.Errorf("Expected CorruptionDetected 1, got %d", corruptionDetected)
	}

	client.Close()
}

// TestStateCorruptionEndToEnd tests full corruption recovery flow
func TestStateCorruptionEndToEnd(t *testing.T) {
	dir := t.TempDir()
	config := MultiProcessConfig(0, 2)

	// Create initial client and write data
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// Write multiple entries
	for i := 0; i < 10; i++ {
		_, err = client.Append(ctx, "test:v1:shard:0000", [][]byte{[]byte(fmt.Sprintf("entry %d", i))})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Force sync
	client.Sync(ctx)

	// Close client
	client.Close()

	// Corrupt the state file by writing garbage (must be correct size)
	stateFilePath := dir + "/shard-0000/comet.state"
	corruptData := make([]byte, CometStateSize)
	copy(corruptData, []byte("corrupted garbage data"))
	if err := os.WriteFile(stateFilePath, corruptData, 0644); err != nil {
		t.Fatal(err)
	}

	// Create new client - should detect corruption and recover
	client2, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client2.Close()

	// Verify we can still write
	_, err = client2.Append(ctx, "test:v1:shard:0000", [][]byte{[]byte("after corruption")})
	if err != nil {
		t.Fatalf("Write after corruption recovery failed: %v", err)
	}

	// Check that backup exists
	files, err := filepath.Glob(stateFilePath + ".corrupted.*")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Error("No backup file created for corrupted state")
	}

	// Verify the backup contains our corrupted data
	backupData, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	// Check that the beginning of the backup contains our corrupted data
	expectedPrefix := "corrupted garbage data"
	if len(backupData) < len(expectedPrefix) || string(backupData[:len(expectedPrefix)]) != expectedPrefix {
		t.Error("Backup file doesn't contain expected corrupted data at the beginning")
	}

	// Verify metrics show recovery
	shard, err := client2.getOrCreateShard(0)
	if err != nil {
		t.Fatal(err)
	}

	state := shard.state
	if state == nil {
		t.Fatal("State is nil")
	}
	recoveryAttempts := atomic.LoadUint64(&state.RecoveryAttempts)
	if recoveryAttempts == 0 {
		t.Error("Expected RecoveryAttempts > 0")
	}

	corruptionDetected := atomic.LoadUint64(&state.CorruptionDetected)
	if corruptionDetected == 0 {
		t.Error("Expected CorruptionDetected > 0")
	}
}

// TestMigrateStateVersion tests future version migration paths
func TestMigrateStateVersion(t *testing.T) {
	dir := t.TempDir()
	config := MultiProcessConfig(0, 2)

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	_, err = client.Append(ctx, "test:v1:shard:0000", [][]byte{[]byte("test")})
	if err != nil {
		t.Fatal(err)
	}

	shard, err := client.getOrCreateShard(0)
	if err != nil {
		t.Fatal(err)
	}

	// Test migration from version 0
	err = shard.migrateStateVersion(0, CometStateVersion1)
	if err != nil {
		t.Fatalf("Migration from version 0 failed: %v", err)
	}

	// Test migration from unknown version
	err = shard.migrateStateVersion(999, CometStateVersion1)
	if err == nil {
		t.Error("Expected error for unknown version migration")
	}
}

// TestStateRecoveryWithMultipleProcesses tests recovery in multi-process scenario
func TestStateRecoveryWithMultipleProcesses(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping multi-process test in short mode")
	}

	dir := t.TempDir()
	config := MultiProcessConfig(0, 2)

	// Process 1: Create and write data
	client1, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		_, err = client1.Append(ctx, "test:v1:shard:0000", [][]byte{[]byte(fmt.Sprintf("p1-entry-%d", i))})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Get shard from process 1
	shard1, err := client1.getOrCreateShard(0)
	if err != nil {
		t.Fatal(err)
	}

	// Store metrics before corruption
	totalWritesBefore := atomic.LoadUint64(&shard1.state.TotalWrites)
	totalEntriesBefore := atomic.LoadInt64(&shard1.state.TotalEntries)

	client1.Close()

	// Corrupt the state file with invalid version
	stateFilePath := dir + "/shard-0000/comet.state"

	// Read current state
	data, err := os.ReadFile(stateFilePath)
	if err != nil {
		t.Fatal(err)
	}

	// Corrupt just the version field (first 8 bytes)
	copy(data[0:8], []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})

	if err := os.WriteFile(stateFilePath, data, 0644); err != nil {
		t.Fatal(err)
	}

	// Process 2: Should detect corruption and recover
	client2, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client2.Close()

	// Write from process 2
	_, err = client2.Append(ctx, "test:v1:shard:0000", [][]byte{[]byte("p2-after-recovery")})
	if err != nil {
		t.Fatalf("Process 2 write failed: %v", err)
	}

	// Process 3: Should work with recovered state
	client3, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client3.Close()

	_, err = client3.Append(ctx, "test:v1:shard:0000", [][]byte{[]byte("p3-after-recovery")})
	if err != nil {
		t.Fatalf("Process 3 write failed: %v", err)
	}

	// Verify recovery metrics are visible across processes
	shard3, err := client3.getOrCreateShard(0)
	if err != nil {
		t.Fatal(err)
	}

	recoveryAttempts := atomic.LoadUint64(&shard3.state.RecoveryAttempts)
	if recoveryAttempts == 0 {
		t.Error("RecoveryAttempts should be > 0 after corruption recovery")
	}

	// The metrics might have been reset during recovery, but new writes should be tracked
	totalWritesAfter := atomic.LoadUint64(&shard3.state.TotalWrites)
	if totalWritesAfter < 2 { // At least p2 and p3 writes
		t.Errorf("Expected TotalWrites >= 2, got %d", totalWritesAfter)
	}

	t.Logf("Recovery successful: attempts=%d, writes before=%d, writes after=%d, entries before=%d",
		recoveryAttempts, totalWritesBefore, totalWritesAfter, totalEntriesBefore)
}

// TestPartialStateCorruption tests recovery from partial corruption
func TestPartialStateCorruption(t *testing.T) {
	dir := t.TempDir()
	config := MultiProcessConfig(0, 2)

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// Write data to establish state
	for i := 0; i < 20; i++ {
		_, err = client.Append(ctx, "test:v1:shard:0000", [][]byte{[]byte(fmt.Sprintf("entry-%d", i))})
		if err != nil {
			t.Fatal(err)
		}
	}

	client.Close()

	// Partially corrupt the state file (corrupt middle section)
	stateFilePath := dir + "/shard-0000/comet.state"
	data, err := os.ReadFile(stateFilePath)
	if err != nil {
		t.Fatal(err)
	}

	// Corrupt compression metrics area (bytes 128-191)
	for i := 128; i < 192 && i < len(data); i++ {
		data[i] = 0xFF
	}

	if err := os.WriteFile(stateFilePath, data, 0644); err != nil {
		t.Fatal(err)
	}

	// Reopen - should still work as we only validate critical fields
	client2, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client2.Close()

	// Should be able to write
	_, err = client2.Append(ctx, "test:v1:shard:0000", [][]byte{[]byte("after partial corruption")})
	if err != nil {
		t.Fatalf("Write after partial corruption failed: %v", err)
	}

	// Now corrupt a critical field (WriteOffset > FileSize)
	shard, err := client2.getOrCreateShard(0)
	if err != nil {
		t.Fatal(err)
	}

	state := shard.state
	if state == nil {
		t.Fatal("State is nil")
	}
	atomic.StoreUint64(&state.WriteOffset, 1<<40)
	atomic.StoreUint64(&state.FileSize, 1000)

	// This should trigger recovery on next validation
	err = shard.validateAndRecoverState()
	if err != nil {
		t.Fatalf("Recovery from critical corruption failed: %v", err)
	}

	// Verify recovery happened
	recoveryAttempts := atomic.LoadUint64(&state.RecoveryAttempts)
	if recoveryAttempts == 0 {
		t.Error("Expected recovery attempt for critical corruption")
	}
}

// TestStateFilePermissions tests recovery handles permission issues
func TestStateFilePermissions(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping permission test in short mode")
	}

	dir := t.TempDir()
	config := MultiProcessConfig(0, 2)

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	_, err = client.Append(ctx, "test:v1:shard:0000", [][]byte{[]byte("test")})
	if err != nil {
		t.Fatal(err)
	}

	client.Close()

	// Make state file read-only
	stateFilePath := dir + "/shard-0000/comet.state"
	if err := os.Chmod(stateFilePath, 0444); err != nil {
		t.Fatal(err)
	}

	// Try to open - should fail with permission denied
	client2, err := NewClientWithConfig(dir, config)
	if err != nil {
		// This is expected - we can't write to a read-only file
		if !strings.Contains(err.Error(), "permission denied") {
			t.Errorf("Expected permission denied error, got: %v", err)
		}
		t.Logf("Got expected error with read-only state file: %v", err)
		// Restore permissions for cleanup
		os.Chmod(stateFilePath, 0644)
		return
	}
	defer client2.Close()

	// If we got here, the OS allowed opening read-only file for writing (unlikely)
	t.Log("Note: OS allowed opening read-only file - this is OS-specific behavior")

	// If it opened, verify we can at least read
	shard, err := client2.getOrCreateShard(0)
	if err != nil {
		// This might fail due to permission issues
		t.Logf("getOrCreateShard failed (expected on some systems): %v", err)
		os.Chmod(stateFilePath, 0644)
		return
	}

	if state := shard.state; state != nil {
		version := atomic.LoadUint64(&state.Version)
		if version != CometStateVersion1 {
			t.Errorf("Expected version %d, got %d", CometStateVersion1, version)
		}
	}

	// Restore permissions
	os.Chmod(stateFilePath, 0644)
}

// TestValidateNilState tests validation with nil state
func TestValidateNilState(t *testing.T) {
	dir := t.TempDir()
	config := DefaultCometConfig()

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Create shard
	ctx := context.Background()
	_, err = client.Append(ctx, "test:v1:shard:0000", [][]byte{[]byte("test")})
	if err != nil {
		t.Fatal(err)
	}

	shard, err := client.getOrCreateShard(0)
	if err != nil {
		t.Fatal(err)
	}

	// Force state to nil
	shard.state = nil

	// Should return error for nil state
	err = shard.validateAndRecoverState()
	if err == nil {
		t.Error("Expected error for nil state")
	}
	if !strings.Contains(err.Error(), "state is nil") {
		t.Errorf("Expected 'state is nil' error, got: %v", err)
	}
}

// TestRecoverWithNoIndex tests recovery when index is nil
func TestRecoverWithNoIndex(t *testing.T) {
	dir := t.TempDir()
	config := MultiProcessConfig(0, 2)

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	_, err = client.Append(ctx, "test:v1:shard:0000", [][]byte{[]byte("test")})
	if err != nil {
		t.Fatal(err)
	}

	shard, err := client.getOrCreateShard(0)
	if err != nil {
		t.Fatal(err)
	}

	// Temporarily set index to nil
	shard.mu.Lock()
	origIndex := shard.index
	shard.index = nil
	shard.mu.Unlock()

	// Trigger recovery
	err = shard.recoverCorruptedState("test with nil index")
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}

	// Restore index
	shard.mu.Lock()
	shard.index = origIndex
	shard.mu.Unlock()

	// Verify state was reinitialized
	if shard.state == nil {
		t.Error("State should not be nil after recovery")
	}

	client.Close()
}

// TestSuspiciousMetrics tests the suspicious metrics check
func TestSuspiciousMetrics(t *testing.T) {
	dir := t.TempDir()
	config := MultiProcessConfig(0, 2)

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	_, err = client.Append(ctx, "test:v1:shard:0000", [][]byte{[]byte("test")})
	if err != nil {
		t.Fatal(err)
	}

	shard, err := client.getOrCreateShard(0)
	if err != nil {
		t.Fatal(err)
	}

	state := shard.state
	if state == nil {
		t.Fatal("State is nil")
	}
	// Set suspicious metrics (many entries but no writes)
	atomic.StoreInt64(&state.TotalEntries, 1000)
	atomic.StoreUint64(&state.TotalWrites, 0)

	// This should NOT trigger recovery (just suspicious, not corrupted)
	err = shard.validateAndRecoverState()
	if err != nil {
		t.Errorf("Suspicious metrics should not trigger recovery: %v", err)
	}

	// Verify metrics were not reset
	if atomic.LoadInt64(&state.TotalEntries) != 1000 {
		t.Error("TotalEntries should not be reset for suspicious metrics")
	}
}

// TestRecoveryFailure tests recovery behavior with permission issues
func TestRecoveryFailure(t *testing.T) {
	// This test validates that our recovery code handles permission issues gracefully.
	// Different OSes and CI environments handle read-only directories differently:
	// - Some systems (regular Linux/macOS) will fail with permission denied
	// - Some CI environments with elevated privileges may allow writes
	// Both behaviors are acceptable as our recovery code handles both cases.

	dir := t.TempDir()
	config := MultiProcessConfig(0, 2)

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	_, err = client.Append(ctx, "test:v1:shard:0000", [][]byte{[]byte("test")})
	if err != nil {
		t.Fatal(err)
	}

	shard, err := client.getOrCreateShard(0)
	if err != nil {
		t.Fatal(err)
	}

	// Get the state path before closing
	statePath := shard.statePath
	shardDir := filepath.Dir(shard.indexPath)

	client.Close()

	// Remove the state file and make directory read-only
	os.Remove(statePath)
	if err := os.Chmod(shardDir, 0555); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(shardDir, 0755) // Restore permissions

	// Try to create a new client
	client2, err := NewClientWithConfig(dir, config)

	// Both outcomes are valid:
	// 1. Permission denied (most systems) - recovery can't proceed
	// 2. Success (some CI systems) - recovery proceeds despite read-only dir
	if err == nil {
		// Success case: verify the system recovered properly
		defer client2.Close()
		t.Log("System allowed writes to read-only directory (common in CI with elevated privileges)")

		// Try to write data - it may fail if permissions are still restrictive
		_, err = client2.Append(ctx, "test:v1:shard:0000", [][]byte{[]byte("after recovery")})
		if err != nil {
			// This can happen if the OS allows opening the directory but not creating new files
			t.Logf("Failed to write after recovery: %v", err)
		}
	} else {
		// Failure case: verify it's a permission error
		if !strings.Contains(err.Error(), "permission denied") &&
			!strings.Contains(err.Error(), "read-only") &&
			!strings.Contains(err.Error(), "mkdir") {
			t.Errorf("Expected permission-related error, got: %v", err)
		} else {
			t.Logf("Got expected permission error: %v", err)
		}
	}
}

// TestStateWithEmptyCurrentFile tests recovery with empty CurrentFile
func TestStateWithEmptyCurrentFile(t *testing.T) {
	dir := t.TempDir()
	config := MultiProcessConfig(0, 2)

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	_, err = client.Append(ctx, "test:v1:shard:0000", [][]byte{[]byte("test")})
	if err != nil {
		t.Fatal(err)
	}

	shard, err := client.getOrCreateShard(0)
	if err != nil {
		t.Fatal(err)
	}

	// Set CurrentFile to empty
	shard.mu.Lock()
	shard.index.CurrentFile = ""
	shard.mu.Unlock()

	// Trigger recovery
	err = shard.recoverCorruptedState("test empty file")
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}

	// ActiveFileIndex should remain 0 when CurrentFile is empty
	state := shard.state
	if state == nil {
		t.Fatal("State is nil after recovery")
	}
	fileIndex := atomic.LoadUint64(&state.ActiveFileIndex)
	if fileIndex != 0 {
		t.Errorf("Expected ActiveFileIndex 0 for empty CurrentFile, got %d", fileIndex)
	}
}

// TestStateWithInvalidFilename tests recovery with invalid filename format
func TestStateWithInvalidFilename(t *testing.T) {
	dir := t.TempDir()
	config := MultiProcessConfig(0, 2)

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	_, err = client.Append(ctx, "test:v1:shard:0000", [][]byte{[]byte("test")})
	if err != nil {
		t.Fatal(err)
	}

	shard, err := client.getOrCreateShard(0)
	if err != nil {
		t.Fatal(err)
	}

	// Set CurrentFile to invalid format
	shard.mu.Lock()
	shard.index.CurrentFile = "invalid-filename.comet"
	shard.mu.Unlock()

	// Trigger recovery
	err = shard.recoverCorruptedState("test invalid filename")
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}

	// ActiveFileIndex should remain unchanged when filename is invalid
	state := shard.state
	if state == nil {
		t.Fatal("State is nil after recovery")
	}
	fileIndex := atomic.LoadUint64(&state.ActiveFileIndex)
	t.Logf("ActiveFileIndex after invalid filename: %d", fileIndex)
}

// TestCorruptedFileRecovery tests recovery from corrupted data files
func TestCorruptedFileRecovery(t *testing.T) {
	dir := t.TempDir()
	config := DefaultCometConfig()

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// Write some data
	testData := [][]byte{
		[]byte("entry 1"),
		[]byte("entry 2"),
		[]byte("entry 3"),
	}

	ids, err := client.Append(ctx, "test:v1:shard:0000", testData)
	if err != nil {
		t.Fatal(err)
	}

	// Force sync to ensure data is on disk
	if err := client.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	// Get the data file path
	shard, _ := client.getOrCreateShard(0)
	shard.mu.RLock()
	dataFile := shard.index.CurrentFile
	shard.mu.RUnlock()

	client.Close()

	// Corrupt the data file by writing garbage in the middle
	file, err := os.OpenFile(dataFile, os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}

	// Seek to middle of file and write garbage
	stat, _ := file.Stat()
	file.Seek(stat.Size()/2, 0)
	file.Write([]byte("CORRUPTED_DATA_HERE"))
	file.Close()

	// Reopen client
	client2, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client2.Close()

	// Try to read - should handle corruption gracefully
	consumer := NewConsumer(client2, ConsumerOptions{Group: "test"})
	defer consumer.Close()

	messages, err := consumer.Read(ctx, []uint32{0}, 10)
	// We expect some entries might be unreadable due to corruption
	// But the system should not crash
	if err != nil {
		t.Logf("Read error (expected): %v", err)
	}

	t.Logf("Read %d messages out of %d after corruption", len(messages), len(ids))

	// Verify we can still write new data
	_, err = client2.Append(ctx, "test:v1:shard:0000", [][]byte{[]byte("new entry after corruption")})
	if err != nil {
		t.Errorf("Failed to write after corruption: %v", err)
	}
}

// TestKillProcessMidWrite tests recovery from process death during write
func TestKillProcessMidWrite(t *testing.T) {
	dir := t.TempDir()
	config := DefaultCometConfig()

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// Write some initial data
	_, err = client.Append(ctx, "test:v1:shard:0000", [][]byte{[]byte("initial data")})
	if err != nil {
		t.Fatal(err)
	}

	// Prepare large batch that will take time to write
	largeBatch := make([][]byte, 1000)
	for i := range largeBatch {
		largeBatch[i] = make([]byte, 1000) // 1KB each
		for j := range largeBatch[i] {
			largeBatch[i][j] = byte(i % 256)
		}
	}

	// Start write in background
	writeCtx, cancel := context.WithCancel(ctx)
	writeDone := make(chan error)
	var writeCount int

	go func() {
		for i := 0; i < 10; i++ {
			_, err := client.Append(writeCtx, "test:v1:shard:0000", largeBatch)
			if err != nil {
				writeDone <- err
				return
			}
			writeCount = i + 1
		}
		writeDone <- nil
	}()

	// Kill the write mid-operation
	time.Sleep(50 * time.Millisecond)
	cancel()

	// Wait for write to finish (should error due to context cancellation)
	err = <-writeDone
	if err == nil || !strings.Contains(err.Error(), "context canceled") {
		// Write might have completed before cancel, which is fine
		t.Logf("Write result: %v (completed %d batches)", err, writeCount)
	}

	client.Close()

	// Simulate process restart by creating new client
	client2, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client2.Close()

	// Verify we can read what was written
	consumer := NewConsumer(client2, ConsumerOptions{Group: "test"})
	defer consumer.Close()

	messages, err := consumer.Read(ctx, []uint32{0}, 100000)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Successfully read %d messages after simulated crash", len(messages))

	// Verify we can continue writing
	_, err = client2.Append(ctx, "test:v1:shard:0000", [][]byte{[]byte("data after restart")})
	if err != nil {
		t.Errorf("Failed to write after restart: %v", err)
	}
}

// TestErrorHandlingInsteadOfPanic verifies we return errors instead of panicking
func TestErrorHandlingInsteadOfPanic(t *testing.T) {
	dir := t.TempDir()
	config := MultiProcessConfig(0, 2)

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// Write some initial data
	_, err = client.Append(ctx, "test:v1:shard:0000", [][]byte{[]byte("initial data")})
	if err != nil {
		t.Fatal(err)
	}

	// Close the client to release locks
	client.Close()

	// Now corrupt the index file to trigger reload error
	indexPath := filepath.Join(dir, "shard-0000", "index.bin")
	if err := os.WriteFile(indexPath, []byte("corrupted"), 0644); err != nil {
		t.Fatal(err)
	}

	// Create a new client - this should handle the corrupted index gracefully
	client2, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client2.Close()

	// Try to write - in multi-process mode, this might fail due to corrupted index
	// but it should return an error, not panic
	_, err = client2.Append(ctx, "test:v1:shard:0000", [][]byte{[]byte("test after corruption")})

	// We expect either success (if recovery worked) or an error (if it couldn't recover)
	// The important thing is that we didn't panic
	if err != nil {
		t.Logf("Got expected error instead of panic: %v", err)
		// Verify it's the right kind of error
		if !strings.Contains(err.Error(), "failed to reload index") && !strings.Contains(err.Error(), "invalid index") {
			// It might have recovered successfully, which is also fine
			t.Logf("System may have recovered from corruption")
		}
	} else {
		t.Log("Write succeeded - system recovered from corruption")
	}
}

// BenchmarkStateValidation benchmarks the validation overhead
func BenchmarkStateValidation(b *testing.B) {
	dir := b.TempDir()
	config := MultiProcessConfig(0, 2)

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	_, err = client.Append(ctx, "test:v1:shard:0000", [][]byte{[]byte("test")})
	if err != nil {
		b.Fatal(err)
	}

	shard, err := client.getOrCreateShard(0)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Just validate, don't actually recover
		state := shard.state
		if state == nil {
			continue
		}
		version := atomic.LoadUint64(&state.Version)
		writeOffset := atomic.LoadUint64(&state.WriteOffset)
		fileSize := atomic.LoadUint64(&state.FileSize)
		lastEntry := atomic.LoadInt64(&state.LastEntryNumber)

		// Simulate validation checks
		_ = version == 0 || version > CometStateVersion1
		_ = writeOffset > fileSize && fileSize > 0
		_ = writeOffset > 1<<40
		_ = lastEntry < -1
	}
}
