package comet

import (
	"context"
	"strings"
	"testing"
)

// TestModeSwitchDetection verifies that we cannot switch between single/multi-process modes
func TestModeSwitchDetection(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// Test 1: Create shard in single-process mode, try to open in multi-process mode
	t.Run("SingleToMulti", func(t *testing.T) {
		// Create in single-process mode
		config1 := DefaultCometConfig()
		config1.Concurrency.EnableMultiProcessMode = false
		client1, err := NewClientWithConfig(dir, config1)
		if err != nil {
			t.Fatalf("failed to create single-process client: %v", err)
		}

		// Write some data
		_, err = client1.Append(ctx, "test:v1:shard:0001", [][]byte{[]byte("test")})
		if err != nil {
			t.Fatalf("failed to write data: %v", err)
		}
		client1.Close()

		// Try to open in multi-process mode
		config2 := DefaultCometConfig()
		config2.Concurrency.EnableMultiProcessMode = true
		client2, err := NewClientWithConfig(dir, config2)
		if err != nil {
			t.Fatalf("failed to create multi-process client: %v", err)
		}
		defer client2.Close()

		// This should fail with mode mismatch error
		_, err = client2.Append(ctx, "test:v1:shard:0001", [][]byte{[]byte("test2")})
		if err == nil {
			t.Fatal("expected mode mismatch error, got nil")
		}
		if !contains(err.Error(), "mode switching is not supported") {
			t.Errorf("expected mode switching error, got: %v", err)
		}
	})

	// Test 2: Create shard in multi-process mode, try to open in single-process mode
	t.Run("MultiToSingle", func(t *testing.T) {
		dir2 := t.TempDir()
		
		// Create in multi-process mode
		config1 := DefaultCometConfig()
		config1.Concurrency.EnableMultiProcessMode = true
		client1, err := NewClientWithConfig(dir2, config1)
		if err != nil {
			t.Fatalf("failed to create multi-process client: %v", err)
		}

		// Write some data
		_, err = client1.Append(ctx, "test:v1:shard:0002", [][]byte{[]byte("test")})
		if err != nil {
			t.Fatalf("failed to write data: %v", err)
		}
		client1.Close()

		// Try to open in single-process mode
		config2 := DefaultCometConfig()
		config2.Concurrency.EnableMultiProcessMode = false
		client2, err := NewClientWithConfig(dir2, config2)
		if err != nil {
			t.Fatalf("failed to create single-process client: %v", err)
		}
		defer client2.Close()

		// This should fail with mode mismatch error
		_, err = client2.Append(ctx, "test:v1:shard:0002", [][]byte{[]byte("test2")})
		if err == nil {
			t.Fatal("expected mode mismatch error, got nil")
		}
		if !contains(err.Error(), "mode switching is not supported") {
			t.Errorf("expected mode switching error, got: %v", err)
		}
	})
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && strings.Contains(s, substr))
}