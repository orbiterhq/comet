package comet

import (
	"reflect"
	"testing"
)

// TestStructAlignment verifies that structs are optimally aligned
func TestStructAlignment(t *testing.T) {
	tests := []struct {
		name string
		obj  any
	}{
		// Core structs
		{"MessageID", MessageID{}},
		{"RetentionStats", RetentionStats{}},
		{"ShardRetentionStats", ShardRetentionStats{}},
		{"EntryPosition", EntryPosition{}},
		{"WriteRequest", WriteRequest{}},
		{"StreamMessage", StreamMessage{}},
		{"Consumer", Consumer{}},
		{"Reader", Reader{}},
		{"MappedFile", MappedFile{}},
		{"StreamStats", StreamStats{}},
		{"FileInfo", FileInfo{}},
		{"ShardIndex", ShardIndex{}},
		{"Shard", Shard{}},
		{"Client", Client{}},
		// Frequently allocated structs
		{"AtomicSlice", AtomicSlice{}},
		{"CometState", CometState{}},
		{"EntryIndexNode", EntryIndexNode{}},
		{"BinarySearchableIndex", BinarySearchableIndex{}},
		// Hot path structs
		{"CompressedEntry", CompressedEntry{}},
		{"CometStats", CometStats{}},
		{"ClientMetrics", ClientMetrics{}},
		// Configuration structs
		{"CometConfig", CometConfig{}},
		{"RetentionConfig", RetentionConfig{}},
		{"CompressionConfig", CompressionConfig{}},
		{"IndexingConfig", IndexingConfig{}},
		{"StorageConfig", StorageConfig{}},
		{"ConcurrencyConfig", ConcurrencyConfig{}},
		{"ConsumerOptions", ConsumerOptions{}},
		{"ReaderConfig", ReaderConfig{}},
		// Reader internal structs
		{"recentFileCache", recentFileCache{}},
		{"cacheItem", cacheItem{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			typ := reflect.TypeOf(tt.obj)

			// Check if struct is properly aligned
			if !isOptimallyAligned(typ) {
				t.Errorf("%s is not optimally aligned", tt.name)
				suggestOptimalFieldOrder(t, typ)
			}

			// Log the actual size and alignment
			t.Logf("%s: size=%d, align=%d", tt.name, typ.Size(), typ.Align())
		})
	}
}

// isOptimallyAligned checks if a struct's fields are ordered for minimal padding
func isOptimallyAligned(typ reflect.Type) bool {
	if typ.Kind() != reflect.Struct {
		return true
	}

	// Calculate actual size with current field order
	actualSize := typ.Size()

	// Calculate minimal possible size
	minSize := calculateMinimalSize(typ)

	// Allow for some reasonable padding (up to 8 bytes)
	// This accounts for alignment requirements that might require some padding
	return actualSize <= minSize+8
}

// calculateMinimalSize calculates the minimal possible size for a struct
func calculateMinimalSize(typ reflect.Type) uintptr {
	if typ.Kind() != reflect.Struct {
		return typ.Size()
	}

	var totalSize uintptr
	maxAlign := uintptr(1)

	// Sum up all field sizes
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		fieldType := field.Type
		fieldSize := fieldType.Size()
		fieldAlign := uintptr(fieldType.Align())

		totalSize += fieldSize
		if fieldAlign > maxAlign {
			maxAlign = fieldAlign
		}
	}

	// Account for struct alignment
	if totalSize%maxAlign != 0 {
		totalSize = (totalSize/maxAlign + 1) * maxAlign
	}

	return totalSize
}

// suggestOptimalFieldOrder suggests a better field order for alignment
func suggestOptimalFieldOrder(t *testing.T, typ reflect.Type) {
	type fieldInfo struct {
		name  string
		size  uintptr
		align uintptr
		typ   reflect.Type
	}

	var fields []fieldInfo
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		fields = append(fields, fieldInfo{
			name:  field.Name,
			size:  field.Type.Size(),
			align: uintptr(field.Type.Align()),
			typ:   field.Type,
		})
	}

	// Sort by alignment (descending) then by size (descending)
	// This generally produces optimal packing
	t.Logf("Suggested field order for %s:", typ.Name())
	t.Logf("Current size: %d bytes", typ.Size())

	// Group by alignment size
	groups := make(map[uintptr][]fieldInfo)
	for _, f := range fields {
		groups[f.align] = append(groups[f.align], f)
	}

	// Print groups from largest alignment to smallest
	for align := uintptr(8); align >= 1; align /= 2 {
		if fields, ok := groups[align]; ok {
			t.Logf("  // %d-byte aligned fields", align)
			for _, f := range fields {
				t.Logf("  %s %s // size=%d", f.name, f.typ.String(), f.size)
			}
		}
	}
}

// TestSpecificStructPadding tests specific structs for padding issues
func TestSpecificStructPadding(t *testing.T) {
	// Test RetentionStats specifically
	t.Run("RetentionStats", func(t *testing.T) {
		typ := reflect.TypeOf(RetentionStats{})
		logStructLayout(t, typ)
	})
}

// BenchmarkStructAllocation benchmarks allocation of aligned vs unaligned structs
func BenchmarkStructAllocation(b *testing.B) {
	b.Run("RetentionStats", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = &RetentionStats{
				TotalSizeBytes: 1024,
				TotalFiles:     10,
				ShardStats:     make(map[uint32]ShardRetentionStats),
			}
		}
	})
}

// TestFieldAlignmentDetails provides detailed alignment information
func TestFieldAlignmentDetails(t *testing.T) {
	t.Run("RetentionStats", func(t *testing.T) {
		typ := reflect.TypeOf(RetentionStats{})
		logStructLayout(t, typ)
	})

	t.Run("Consumer", func(t *testing.T) {
		typ := reflect.TypeOf(Consumer{})
		logStructLayout(t, typ)
	})

	t.Run("CompressedEntry", func(t *testing.T) {
		typ := reflect.TypeOf(CompressedEntry{})
		logStructLayout(t, typ)
	})

	t.Run("EntryIndexNode", func(t *testing.T) {
		typ := reflect.TypeOf(EntryIndexNode{})
		logStructLayout(t, typ)
	})

	t.Run("CometState", func(t *testing.T) {
		typ := reflect.TypeOf(CometState{})
		logStructLayout(t, typ)
	})

	t.Run("ReaderConfig", func(t *testing.T) {
		typ := reflect.TypeOf(ReaderConfig{})
		logStructLayout(t, typ)
	})

	t.Run("recentFileCache", func(t *testing.T) {
		typ := reflect.TypeOf(recentFileCache{})
		logStructLayout(t, typ)
	})
}

// logStructLayout logs detailed struct layout information
func logStructLayout(t *testing.T, typ reflect.Type) {
	t.Logf("\n%s layout (size=%d, align=%d):", typ.Name(), typ.Size(), typ.Align())

	var offset uintptr
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)

		// Calculate padding
		padding := field.Offset - offset
		if i > 0 && padding > 0 {
			t.Logf("  [%d bytes padding]", padding)
		}

		t.Logf("  %3d: %-20s %-30s (size=%d, align=%d)",
			field.Offset,
			field.Name,
			field.Type.String(),
			field.Type.Size(),
			field.Type.Align())

		offset = field.Offset + field.Type.Size()
	}

	// Final padding
	finalPadding := typ.Size() - offset
	if finalPadding > 0 {
		t.Logf("  [%d bytes final padding]", finalPadding)
	}
}
