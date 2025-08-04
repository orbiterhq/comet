package comet

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

// DiagnoseMultiProcessIssues helps debug common multi-process problems
func DiagnoseMultiProcessIssues(ctx context.Context, dataDir string) error {
	fmt.Println("=== Multi-Process Diagnostics ===")

	// Check for common issues
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return fmt.Errorf("failed to read data directory: %w", err)
	}

	mixedModeShards := []string{}
	for _, entry := range entries {
		if !entry.IsDir() || len(entry.Name()) < 6 || entry.Name()[:6] != "shard-" {
			continue
		}

		shardDir := filepath.Join(dataDir, entry.Name())

		// Check for mixed mode indicators
		stateFile := filepath.Join(shardDir, "state.mmap")
		dataFiles, _ := filepath.Glob(filepath.Join(shardDir, "*.comet"))

		hasState := false
		if _, err := os.Stat(stateFile); err == nil {
			hasState = true
		}

		if len(dataFiles) > 0 && !hasState {
			mixedModeShards = append(mixedModeShards, entry.Name())
		}
	}

	if len(mixedModeShards) > 0 {
		fmt.Printf("⚠️  WARNING: Found shards created in single-process mode: %v\n", mixedModeShards)
		fmt.Println("   These shards cannot be used in multi-process mode.")
		fmt.Println("   Solution: Delete these shard directories or migrate the data.")
	} else {
		fmt.Println("✓ No mixed-mode shard issues detected")
	}

	return nil
}

// ValidateMultiProcessSetup checks if multi-process mode is properly configured
func ValidateMultiProcessSetup(config CometConfig) []string {
	var issues []string

	if config.Concurrency.EnableMultiProcessMode {
		if config.Storage.CheckpointTime == 0 {
			issues = append(issues, "Multi-process mode works better with periodic checkpointing enabled")
		}
	}

	return issues
}
