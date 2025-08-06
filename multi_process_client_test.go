package comet

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestDebugMultiProcessConfig(t *testing.T) {
	tempDir := t.TempDir()
	shmFile := filepath.Join(tempDir, "debug-slots")

	// Clean up any existing file
	os.Remove(shmFile)

	// Try to get a process ID directly
	id := GetProcessID(shmFile)
	t.Logf("Direct GetProcessID returned: %d", id)

	if id >= 0 {
		// Release it
		ReleaseProcessID(shmFile)
		t.Log("Released process ID")
	}

	// Now try MultiProcessConfig
	config := MultiProcessConfig(shmFile)
	t.Logf("MultiProcessConfig returned ProcessID: %d, ProcessCount: %d",
		config.Concurrency.ProcessID, config.Concurrency.ProcessCount)

	// Clean up
	ReleaseProcessID(shmFile)
}

func TestNewMultiProcessClient_Basic(t *testing.T) {
	// Test basic creation and cleanup
	tempDir := t.TempDir()
	shmFile := filepath.Join(tempDir, "test-basic-slots")

	// Clean up any existing file
	os.Remove(shmFile)

	client, err := NewMultiProcessClient(tempDir+"/data", MultiProcessConfig(shmFile))
	if err != nil {
		t.Fatalf("Failed to create multi-process client: %v", err)
	}

	// Verify it's in multi-process mode
	if !client.config.Concurrency.IsMultiProcess() {
		t.Error("Expected client to be in multi-process mode")
	}

	// Process ID should be 0 (first process)
	if client.config.Concurrency.ProcessID != 0 {
		t.Errorf("Expected process ID 0, got %d", client.config.Concurrency.ProcessID)
	}

	// Process count should be NumCPU
	expectedCount := runtime.NumCPU()
	if client.config.Concurrency.ProcessCount != expectedCount {
		t.Errorf("Expected process count %d, got %d", expectedCount, client.config.Concurrency.ProcessCount)
	}

	// Test basic operations
	ctx := context.Background()
	data := [][]byte{[]byte("test message")}
	ids, err := client.Append(ctx, "test:v1:shard:0000", data)
	if err != nil {
		t.Fatalf("Failed to append data: %v", err)
	}

	if len(ids) != 1 {
		t.Errorf("Expected 1 message ID, got %d", len(ids))
	}

	// Close should automatically release process ID
	err = client.Close()
	if err != nil {
		t.Errorf("Failed to close client: %v", err)
	}
}

func TestNewMultiProcessClientWithFile(t *testing.T) {
	tempDir := t.TempDir()
	shmFile := filepath.Join(tempDir, "custom-slots")

	// Clean up any existing file
	os.Remove(shmFile)

	client, err := NewMultiProcessClient(tempDir+"/data", MultiProcessConfig(shmFile))
	if err != nil {
		t.Fatalf("Failed to create multi-process client: %v", err)
	}
	defer client.Close()

	// Verify it's in multi-process mode
	if !client.config.Concurrency.IsMultiProcess() {
		t.Error("Expected client to be in multi-process mode")
	}

	// Verify shared memory file is stored
	if client.config.Concurrency.SHMFile != shmFile {
		t.Errorf("Expected shared memory file %s, got %s", shmFile, client.config.Concurrency.SHMFile)
	}
}

func TestNewMultiProcessClient_DefaultOptions(t *testing.T) {
	tempDir := t.TempDir()
	shmFile := filepath.Join(tempDir, "test-default-slots")

	// Clean up any existing file
	os.Remove(shmFile)

	client, err := NewMultiProcessClient(tempDir+"/data", MultiProcessConfig(shmFile))
	if err != nil {
		t.Fatalf("Failed to create multi-process client: %v", err)
	}
	defer client.Close()

	// Should use default values
	expectedCount := runtime.NumCPU()
	if client.config.Concurrency.ProcessCount != expectedCount {
		t.Errorf("Expected default process count %d, got %d", expectedCount, client.config.Concurrency.ProcessCount)
	}

	// Should use the specified shared memory file
	if client.config.Concurrency.SHMFile != shmFile {
		t.Errorf("Expected SHMFile %s, got %s", shmFile, client.config.Concurrency.SHMFile)
	}
}

func TestNewMultiProcessClient_FailureCleanup(t *testing.T) {
	// Test that process ID is released even if client creation fails
	tempDir := t.TempDir()
	shmFile := filepath.Join(tempDir, "slots")

	// Clean up any existing file
	os.Remove(shmFile)

	// First, create a config that will work for process ID acquisition
	config := MultiProcessConfig(shmFile)

	// Try to create client with invalid data directory (read-only)
	invalidDir := "/proc/invalid" // This should fail on most systems
	client, err := NewMultiProcessClient(invalidDir, config)
	if err == nil {
		client.Close()
		t.Fatal("Expected client creation to fail with invalid directory")
	}

	// The process ID should have been released automatically
	// We can test this by creating another client - it should get the same slot
	client2, err := NewMultiProcessClient(tempDir+"/data", MultiProcessConfig(shmFile))
	if err != nil {
		t.Fatalf("Failed to create second client after first failed: %v", err)
	}
	defer client2.Close()

	// Should get process ID 0 (the released slot)
	if client2.config.Concurrency.ProcessID != 0 {
		t.Errorf("Expected process ID 0 after failure cleanup, got %d", client2.config.Concurrency.ProcessID)
	}
}

func TestMultiProcessCometConfig(t *testing.T) {
	tempDir := t.TempDir()
	shmFile := filepath.Join(tempDir, "config-test-slots")

	// Clean up any existing file
	os.Remove(shmFile)

	// Test the config creation function
	config := MultiProcessConfig(shmFile)

	// Should be in multi-process mode
	if !config.Concurrency.IsMultiProcess() {
		t.Error("Expected config to be in multi-process mode")
	}

	// Should have acquired process ID 0
	if config.Concurrency.ProcessID != 0 {
		t.Errorf("Expected process ID 0, got %d", config.Concurrency.ProcessID)
	}

	// Should have NumCPU process count
	expectedCount := runtime.NumCPU()
	if config.Concurrency.ProcessCount != expectedCount {
		t.Errorf("Expected process count %d, got %d", expectedCount, config.Concurrency.ProcessCount)
	}

	// Manual cleanup since config creation doesn't track for auto-cleanup
	ReleaseProcessID(shmFile)
}
