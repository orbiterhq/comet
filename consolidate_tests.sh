#!/bin/bash
# Script to consolidate test files

# Create backup directory
mkdir -p test_backup
cp *_test.go test_backup/

# Start consolidating edge cases
echo "Creating consolidated edge_cases_test.go..."

# Header for edge_cases_test.go
cat > edge_cases_consolidated.go << 'EOF'
package comet

// Edge case tests consolidated from multiple files:
// - edge_cases_test.go (original)
// - critical_bugs_test.go
// - file_boundary_test.go  
// - index_limit_test.go
// - io_outside_lock_test.go

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

EOF

# Append test functions from each file (excluding package and imports)
echo "// ========== From original edge_cases_test.go ==========" >> edge_cases_consolidated.go
sed -n '/^func Test/,/^}/p' edge_cases_test.go >> edge_cases_consolidated.go

echo -e "\n// ========== From critical_bugs_test.go ==========" >> edge_cases_consolidated.go
sed -n '/^func Test/,/^}/p' critical_bugs_test.go >> edge_cases_consolidated.go

echo -e "\n// ========== From file_boundary_test.go ==========" >> edge_cases_consolidated.go
sed -n '/^func Test/,/^}/p' file_boundary_test.go >> edge_cases_consolidated.go

echo -e "\n// ========== From index_limit_test.go ==========" >> edge_cases_consolidated.go
sed -n '/^func Test/,/^}/p' index_limit_test.go >> edge_cases_consolidated.go

echo -e "\n// ========== From io_outside_lock_test.go ==========" >> edge_cases_consolidated.go
sed -n '/^func Test/,/^}/p' io_outside_lock_test.go >> edge_cases_consolidated.go

# Replace old edge_cases_test.go
mv edge_cases_consolidated.go edge_cases_test.go

echo "Edge cases consolidation complete!"