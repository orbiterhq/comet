package comet

import (
	"runtime"
	"testing"
	"time"
)

// cleanupClient ensures a client is properly closed and waits for background goroutines
func cleanupClient(t *testing.T, client *Client) {
	t.Helper()
	if client == nil {
		return
	}

	// Close the client
	if err := client.Close(); err != nil {
		t.Logf("Warning: error closing client: %v", err)
	}

	// Give background goroutines time to finish
	// This is particularly important for the consumer's ACK flush goroutine
	time.Sleep(10 * time.Millisecond)
}

// cleanupConsumer ensures a consumer is properly closed and all background goroutines finish
func cleanupConsumer(t *testing.T, consumer *Consumer) {
	t.Helper()
	if consumer == nil {
		return
	}

	// Close the consumer (this waits for the background flush goroutine)
	if err := consumer.Close(); err != nil {
		t.Logf("Warning: error closing consumer: %v", err)
	}

	// Extra safety - give any remaining goroutines time to finish
	time.Sleep(10 * time.Millisecond)
}

// withCleanup runs a test with automatic client/consumer cleanup
func withCleanup(t *testing.T, f func(t *testing.T)) {
	t.Helper()

	// Record initial goroutine count
	initialGoroutines := runtime.NumGoroutine()

	// Run the test
	f(t)

	// Give goroutines time to finish
	time.Sleep(50 * time.Millisecond)

	// Check for goroutine leaks
	finalGoroutines := runtime.NumGoroutine()
	if finalGoroutines > initialGoroutines+2 { // Allow some tolerance
		t.Logf("Warning: possible goroutine leak. Started with %d, ended with %d goroutines",
			initialGoroutines, finalGoroutines)
	}
}

// ensureDirectoryCleanup helps ensure temp directories are cleaned up
func ensureDirectoryCleanup(t *testing.T, dir string) {
	t.Helper()
	t.Cleanup(func() {
		// Give any file operations time to complete
		time.Sleep(10 * time.Millisecond)
		// The directory should already be cleaned up by t.TempDir()
		// This is just to ensure any lingering file handles are released
	})
}
