package comet

import (
	"context"
	"testing"
)

func TestCrashRecoverySimple(t *testing.T) {
	dir := t.TempDir()

	// Write some data
	client1, err := NewClient(dir, DefaultCometConfig())
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	stream := "test:v1:shard:0000"

	// Write 10 messages
	var messages [][]byte
	for i := 0; i < 10; i++ {
		messages = append(messages, []byte("msg-"+string(rune('0'+i))))
	}

	_, err = client1.Append(ctx, stream, messages)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Written 10 messages")

	// Force sync to make data durable
	err = client1.Sync(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Check shard state after sync
	shard := client1.getShard(0)
	if shard != nil {
		shard.mu.RLock()
		t.Logf("After sync: CurrentEntryNumber=%d, nextEntryNumber=%d, lastWrittenEntryNumber=%d",
			shard.index.CurrentEntryNumber, shard.nextEntryNumber, shard.lastWrittenEntryNumber)
		shard.mu.RUnlock()
	}

	client1.Close()

	// Create new client (triggers recovery)
	client2, err := NewClient(dir, DefaultCometConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer client2.Close()

	// Check shard state after recovery
	shard2 := client2.getShard(0)
	if shard2 == nil {
		// Try to trigger shard creation
		consumer := NewConsumer(client2, ConsumerOptions{Group: "test"})
		defer consumer.Close()

		messages, err := consumer.Read(ctx, []uint32{0}, 10)
		if err != nil {
			t.Fatal(err)
		}

		t.Logf("Successfully read %d messages after recovery", len(messages))

		// Get shard now
		shard2 = client2.getShard(0)
	}

	if shard2 != nil {
		shard2.mu.RLock()
		t.Logf("After recovery: CurrentEntryNumber=%d, nextEntryNumber=%d, Files=%d",
			shard2.index.CurrentEntryNumber, shard2.nextEntryNumber, len(shard2.index.Files))
		if len(shard2.index.Files) > 0 {
			for i, f := range shard2.index.Files {
				t.Logf("  File %d: StartEntry=%d, Entries=%d, Path=%s",
					i, f.StartEntry, f.Entries, f.Path)
			}
		}
		shard2.mu.RUnlock()
	}
}

func TestCrashRecoveryNoSync(t *testing.T) {
	dir := t.TempDir()

	// Write some data without syncing (simulate crash)
	client1, err := NewClient(dir, DefaultCometConfig())
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	stream := "test:v1:shard:0000"

	// Write 10 messages
	var messages [][]byte
	for i := 0; i < 10; i++ {
		messages = append(messages, []byte("msg-"+string(rune('0'+i))))
	}

	_, err = client1.Append(ctx, stream, messages)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Written 10 messages without sync")

	// Check shard state before crash (no sync)
	shard := client1.getShard(0)
	if shard != nil {
		shard.mu.RLock()
		t.Logf("Before crash: CurrentEntryNumber=%d, nextEntryNumber=%d, lastWrittenEntryNumber=%d",
			shard.index.CurrentEntryNumber, shard.nextEntryNumber, shard.lastWrittenEntryNumber)
		shard.mu.RUnlock()
	}

	// Simulate crash by closing without sync
	client1.Close()

	// Create new client (triggers recovery)
	client2, err := NewClient(dir, DefaultCometConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer client2.Close()

	// Try to read after recovery
	consumer := NewConsumer(client2, ConsumerOptions{Group: "test"})
	defer consumer.Close()

	messages2, err := consumer.Read(ctx, []uint32{0}, 100)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Successfully read %d messages after crash recovery", len(messages2))

	// Check shard state after recovery
	shard2 := client2.getShard(0)
	if shard2 != nil {
		shard2.mu.RLock()
		t.Logf("After recovery: CurrentEntryNumber=%d, nextEntryNumber=%d, Files=%d",
			shard2.index.CurrentEntryNumber, shard2.nextEntryNumber, len(shard2.index.Files))
		if len(shard2.index.Files) > 0 {
			for i, f := range shard2.index.Files {
				t.Logf("  File %d: StartEntry=%d, Entries=%d, Path=%s",
					i, f.StartEntry, f.Entries, f.Path)
			}
		}
		shard2.mu.RUnlock()
	}
}
