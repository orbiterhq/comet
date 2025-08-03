#!/bin/bash

# Fix direct state nil checks in Go files
# This script replaces shard.state != nil with shard.loadState() != nil

echo "Fixing direct state nil checks..."

# Common patterns to fix
patterns=(
    's/\([a-zA-Z0-9_]*\)\.state != nil/\1.loadState() != nil/g'
    's/\([a-zA-Z0-9_]*\)\.state == nil/\1.loadState() == nil/g'
    's/if \([a-zA-Z0-9_]*\)\.state {/if \1.loadState() != nil {/g'
    's/if !\([a-zA-Z0-9_]*\)\.state {/if \1.loadState() == nil {/g'
)

# Find all Go files
find . -name "*.go" -type f | while read file; do
    # Skip vendor and tools directories
    if [[ "$file" == *"/vendor/"* ]] || [[ "$file" == *"/tools/"* ]]; then
        continue
    fi
    
    # Check if file contains state references
    if grep -q "\.state [!=]= nil\|if.*\.state {" "$file"; then
        echo "Fixing: $file"
        # Create backup
        cp "$file" "$file.bak"
        
        # Apply all patterns
        for pattern in "${patterns[@]}"; do
            sed -i '' "$pattern" "$file"
        done
        
        # Remove backup if successful
        rm "$file.bak"
    fi
done

echo "Done! Running linter to verify..."
cd tools/statelint && go run main.go -dir="../.."