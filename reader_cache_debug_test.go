package comet

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// TestReaderCacheDebug creates a focused test with extensive logging to debug reader cache issues
func TestReaderCacheDebug(t *testing.T) {
	dataDir := t.TempDir()

	// Configure for frequent rotations and easy debugging
	config := DefaultCometConfig()
	config.Storage.FlushInterval = 10     // 10ms flush
	config.Storage.MaxFileSize = 1 * 1024 // 1KB files to force quick rotations

	client, err := NewClient(dataDir, config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	stream := "debug:v1:shard:0000"
	shardID := uint32(0)
	ctx := context.Background()

	t.Log("🔧 === PHASE 1: Initial Setup ===")

	// Write initial batch to establish baseline
	batch1 := make([][]byte, 0)
	for i := 0; i < 10; i++ {
		msg := fmt.Sprintf("batch1-msg-%03d-padding-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", i)
		batch1 = append(batch1, []byte(msg))
	}

	_, err = client.Append(ctx, stream, batch1)
	if err != nil {
		t.Fatalf("Failed to write batch1: %v", err)
	}

	err = client.Sync(ctx)
	if err != nil {
		t.Fatalf("Failed to sync batch1: %v", err)
	}

	// Get shard and check initial state
	shard, err := client.getOrCreateShard(shardID)
	if err != nil {
		t.Fatalf("Failed to get shard: %v", err)
	}

	logShardState := func(phase string) {
		shard.mu.RLock()
		fileCount := len(shard.index.Files)
		currentEntry := shard.index.CurrentEntryNumber
		var lastIndexUpdate int64
		if shard.state != nil {
			lastIndexUpdate = shard.state.GetLastIndexUpdate()
		}
		shard.mu.RUnlock()

		t.Logf("📊 [%s] Shard: files=%d, entries=%d, lastIndexUpdate=%d",
			phase, fileCount, currentEntry, lastIndexUpdate)
	}

	logShardState("AFTER BATCH1")

	// Create consumer and read initial messages
	consumer := NewConsumer(client, ConsumerOptions{Group: "debug-test"})
	defer consumer.Close()

	t.Log("📖 Reading initial messages...")
	msgs, err := consumer.Read(ctx, []uint32{shardID}, 5)
	if err != nil {
		t.Fatalf("Failed to read initial messages: %v", err)
	}
	t.Logf("✅ Read %d initial messages (entries %d-%d)",
		len(msgs), msgs[0].ID.EntryNumber, msgs[len(msgs)-1].ID.EntryNumber)

	// ACK the messages to update consumer offset
	t.Log("📝 ACKing initial messages...")
	messageIDs := make([]MessageID, len(msgs))
	for i, msg := range msgs {
		messageIDs[i] = msg.ID
	}
	err = consumer.Ack(ctx, messageIDs...)
	if err != nil {
		t.Fatalf("Failed to ACK initial messages: %v", err)
	}
	t.Log("✅ ACKed initial messages")

	logShardState("AFTER INITIAL READ")

	// Get the reader that was created by the consumer
	readerInterface, exists := consumer.readers.Load(shardID)
	if !exists {
		t.Fatalf("Reader not found for shard %d", shardID)
	}
	reader := readerInterface.(*Reader)

	logReaderState := func(phase string) {
		reader.mappingMu.RLock()
		cachedFiles := len(reader.fileInfos)
		lastKnown := atomic.LoadInt64(&reader.lastKnownIndexUpdate)
		reader.mappingMu.RUnlock()

		var currentUpdate int64
		if reader.state != nil {
			currentUpdate = reader.state.GetLastIndexUpdate()
		}

		t.Logf("🔍 [%s] Reader: cachedFiles=%d, lastKnown=%d, current=%d, stale=%v",
			phase, cachedFiles, lastKnown, currentUpdate, lastKnown < currentUpdate)

		// Log each cached file
		reader.mappingMu.RLock()
		for i, fileInfo := range reader.fileInfos {
			t.Logf("   File[%d]: path=%s, startEntry=%d, entries=%d",
				i, fileInfo.Path, fileInfo.StartEntry, fileInfo.Entries)
		}
		reader.mappingMu.RUnlock()
	}

	logReaderState("AFTER INITIAL READ")

	t.Log("📝 === PHASE 2: Force File Rotation ===")

	// Write large batch to force rotation
	batch2 := make([][]byte, 0)
	for i := 10; i < 30; i++ {
		msg := fmt.Sprintf("batch2-msg-%03d-large-padding-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", i)
		batch2 = append(batch2, []byte(msg))
	}

	_, err = client.Append(ctx, stream, batch2)
	if err != nil {
		t.Fatalf("Failed to write batch2: %v", err)
	}

	err = client.Sync(ctx)
	if err != nil {
		t.Fatalf("Failed to sync batch2: %v", err)
	}

	logShardState("AFTER BATCH2")
	logReaderState("AFTER BATCH2")

	t.Log("🔄 === PHASE 3: Test Reader Cache Refresh ===")

	// Try to read beyond what the reader initially knew about
	t.Log("📖 Attempting to read entry 15 (should be in new file)...")

	// Direct reader test
	data, err := reader.ReadEntryByNumber(15)
	if err != nil {
		t.Logf("❌ Direct reader failed to read entry 15: %v", err)
	} else {
		t.Logf("✅ Direct reader successfully read entry 15: %s", string(data)[:50])
	}

	logReaderState("AFTER DIRECT READ")

	// Consumer read test
	t.Log("📖 Consumer reading more messages...")
	for attempt := 0; attempt < 5; attempt++ {
		msgs, err = consumer.Read(ctx, []uint32{shardID}, 10)
		if err != nil {
			t.Logf("❌ Consumer read attempt %d failed: %v", attempt, err)
			continue
		}

		if len(msgs) == 0 {
			t.Logf("⚠️  Consumer read attempt %d: no messages", attempt)
			time.Sleep(50 * time.Millisecond)
			continue
		}

		t.Logf("✅ Consumer read attempt %d: got %d messages (entries %d-%d)",
			attempt, len(msgs), msgs[0].ID.EntryNumber, msgs[len(msgs)-1].ID.EntryNumber)

		// ACK the messages to advance the offset
		messageIDs := make([]MessageID, len(msgs))
		for i, msg := range msgs {
			messageIDs[i] = msg.ID
		}
		err = consumer.Ack(ctx, messageIDs...)
		if err != nil {
			t.Logf("⚠️  Failed to ACK messages: %v", err)
		}

		// Check if we got messages beyond the original file
		if msgs[len(msgs)-1].ID.EntryNumber >= 10 {
			t.Log("🎉 SUCCESS: Consumer read messages from rotated file!")
			logReaderState("SUCCESS")
			return
		}

		time.Sleep(50 * time.Millisecond)
	}

	t.Error("❌ FAILED: Consumer could not read messages from rotated file")
	logReaderState("FAILED")

	// Final debug: manually check index vs reader state
	t.Log("🔬 === FINAL DEBUG ===")
	shard.mu.RLock()
	liveFiles := shard.index.Files
	t.Logf("Live index has %d files:", len(liveFiles))
	for i, file := range liveFiles {
		t.Logf("  LiveFile[%d]: path=%s, startEntry=%d, entries=%d",
			i, file.Path, file.StartEntry, file.Entries)
	}
	shard.mu.RUnlock()

	// Check if LastIndexUpdate is being updated on file rotation
	t.Log("🕐 LastIndexUpdate timeline:")
	if reader.state != nil {
		current := reader.state.GetLastIndexUpdate()
		known := atomic.LoadInt64(&reader.lastKnownIndexUpdate)
		t.Logf("  Current LastIndexUpdate: %d", current)
		t.Logf("  Reader's known timestamp: %d", known)
		t.Logf("  Difference: %d nanoseconds", current-known)
	}
}
