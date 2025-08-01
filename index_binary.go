package comet

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"time"
)

// Binary index format:
// Header:
//   [4] Magic number (0x434F4D54 = "COMT")
//   [4] Version (1)
//   [8] Current entry number
//   [8] Current write offset
//   [4] Consumer count
//   [4] Binary index node count
// Consumers:
//   For each consumer:
//     [1] Group name length
//     [N] Group name (UTF-8)
//     [8] Offset
// Binary index nodes:
//   For each node:
//     [8] Entry number
//     [4] File index
//     [8] Byte offset

const (
	indexMagic   = 0x434F4D54 // "COMT"
	indexVersion = 1
)

// saveBinaryIndex writes the index in binary format
func (s *Shard) saveBinaryIndex(index *ShardIndex) error {
	// Create temp file
	tempPath := s.indexPath + ".tmp"
	f, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("failed to create temp index: %w", err)
	}
	defer f.Close()

	// Write header
	if err := binary.Write(f, binary.LittleEndian, uint32(indexMagic)); err != nil {
		return err
	}
	if err := binary.Write(f, binary.LittleEndian, uint32(indexVersion)); err != nil {
		return err
	}
	if err := binary.Write(f, binary.LittleEndian, uint64(index.CurrentEntryNumber)); err != nil {
		return err
	}
	if err := binary.Write(f, binary.LittleEndian, uint64(index.CurrentWriteOffset)); err != nil {
		return err
	}
	if err := binary.Write(f, binary.LittleEndian, uint32(len(index.ConsumerOffsets))); err != nil {
		return err
	}
	if err := binary.Write(f, binary.LittleEndian, uint32(len(index.BinaryIndex.Nodes))); err != nil {
		return err
	}
	if err := binary.Write(f, binary.LittleEndian, uint32(len(index.Files))); err != nil {
		return err
	}

	// Write consumer offsets
	for group, offset := range index.ConsumerOffsets {
		groupBytes := []byte(group)
		if err := binary.Write(f, binary.LittleEndian, uint8(len(groupBytes))); err != nil {
			return err
		}
		if _, err := f.Write(groupBytes); err != nil {
			return err
		}
		if err := binary.Write(f, binary.LittleEndian, uint64(offset)); err != nil {
			return err
		}
	}

	// Write binary index nodes in a single buffer to reduce syscalls
	nodeCount := len(index.BinaryIndex.Nodes)
	if nodeCount > 0 {
		// Pre-allocate buffer for all nodes (20 bytes per node)
		nodeBuf := make([]byte, nodeCount*20)
		offset := 0
		
		for _, node := range index.BinaryIndex.Nodes {
			// EntryNumber (8 bytes)
			binary.LittleEndian.PutUint64(nodeBuf[offset:], uint64(node.EntryNumber))
			offset += 8
			
			// FileIndex (4 bytes)
			binary.LittleEndian.PutUint32(nodeBuf[offset:], uint32(node.Position.FileIndex))
			offset += 4
			
			// ByteOffset (8 bytes)
			binary.LittleEndian.PutUint64(nodeBuf[offset:], uint64(node.Position.ByteOffset))
			offset += 8
		}
		
		if _, err := f.Write(nodeBuf); err != nil {
			return err
		}
	}

	// Write files info - estimate buffer size and batch writes
	fileCount := len(index.Files)
	if fileCount > 0 {
		// Estimate buffer size: 2 (path len) + avg path length + 48 bytes per file
		estimatedSize := 0
		for _, file := range index.Files {
			estimatedSize += 2 + len(file.Path) + 48
		}
		
		buf := make([]byte, 0, estimatedSize)
		
		for _, file := range index.Files {
			// Write path length (2 bytes)
			pathBytes := []byte(file.Path)
			pathLen := uint16(len(pathBytes))
			buf = append(buf, byte(pathLen), byte(pathLen>>8))
			
			// Write path
			buf = append(buf, pathBytes...)
			
			// Write file metadata (48 bytes total)
			metaBuf := make([]byte, 48)
			binary.LittleEndian.PutUint64(metaBuf[0:], uint64(file.StartOffset))
			binary.LittleEndian.PutUint64(metaBuf[8:], uint64(file.EndOffset))
			binary.LittleEndian.PutUint64(metaBuf[16:], uint64(file.StartEntry))
			binary.LittleEndian.PutUint64(metaBuf[24:], uint64(file.Entries))
			binary.LittleEndian.PutUint64(metaBuf[32:], uint64(file.StartTime.UnixNano()))
			binary.LittleEndian.PutUint64(metaBuf[40:], uint64(file.EndTime.UnixNano()))
			
			buf = append(buf, metaBuf...)
		}
		
		if _, err := f.Write(buf); err != nil {
			return err
		}
	}

	// Note: We don't sync here to avoid performance issues during frequent rotations
	// The index will be synced during periodic checkpoints

	// Atomic rename
	return os.Rename(tempPath, s.indexPath)
}

// loadBinaryIndex reads the index from binary format
func (s *Shard) loadBinaryIndex() (*ShardIndex, error) {
	data, err := os.ReadFile(s.indexPath)
	if err != nil {
		return nil, err
	}

	if len(data) < 32 { // Minimum header size
		return nil, fmt.Errorf("index file too small")
	}

	// Read and verify header
	offset := 0
	magic := binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	if magic != indexMagic {
		return nil, fmt.Errorf("invalid index magic: %x", magic)
	}

	version := binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	if version != indexVersion {
		return nil, fmt.Errorf("unsupported index version: %d", version)
	}

	index := &ShardIndex{
		ConsumerOffsets: make(map[string]int64),
		BinaryIndex: BinarySearchableIndex{
			IndexInterval: s.index.BinaryIndex.IndexInterval,
			MaxNodes:      s.index.BinaryIndex.MaxNodes,
		},
	}

	index.CurrentEntryNumber = int64(binary.LittleEndian.Uint64(data[offset:]))
	offset += 8
	index.CurrentWriteOffset = int64(binary.LittleEndian.Uint64(data[offset:]))
	offset += 8

	consumerCount := binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	nodeCount := binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	fileCount := uint32(0)
	if offset < len(data)-4 {
		fileCount = binary.LittleEndian.Uint32(data[offset:])
		offset += 4
	}

	// Read consumer offsets
	for i := uint32(0); i < consumerCount; i++ {
		if offset >= len(data) {
			return nil, io.ErrUnexpectedEOF
		}

		groupLen := int(data[offset])
		offset++

		if offset+groupLen+8 > len(data) {
			return nil, io.ErrUnexpectedEOF
		}

		group := string(data[offset : offset+groupLen])
		offset += groupLen

		consumerOffset := int64(binary.LittleEndian.Uint64(data[offset:]))
		offset += 8

		index.ConsumerOffsets[group] = consumerOffset
	}

	// Read binary index nodes
	index.BinaryIndex.Nodes = make([]EntryIndexNode, 0, nodeCount)
	for i := uint32(0); i < nodeCount; i++ {
		if offset+20 > len(data) {
			return nil, io.ErrUnexpectedEOF
		}

		node := EntryIndexNode{
			EntryNumber: int64(binary.LittleEndian.Uint64(data[offset:])),
			Position: EntryPosition{
				FileIndex:  int(binary.LittleEndian.Uint32(data[offset+8:])),
				ByteOffset: int64(binary.LittleEndian.Uint64(data[offset+12:])),
			},
		}
		offset += 20

		index.BinaryIndex.Nodes = append(index.BinaryIndex.Nodes, node)
	}

	// Read files info
	index.Files = make([]FileInfo, 0, fileCount)
	for i := uint32(0); i < fileCount; i++ {
		if offset+2 > len(data) {
			return nil, io.ErrUnexpectedEOF
		}

		pathLen := int(binary.LittleEndian.Uint16(data[offset:]))
		offset += 2

		if offset+pathLen > len(data) {
			return nil, io.ErrUnexpectedEOF
		}

		path := string(data[offset : offset+pathLen])
		offset += pathLen

		if offset+48 > len(data) { // 6 uint64 fields
			return nil, io.ErrUnexpectedEOF
		}

		file := FileInfo{
			Path:        path,
			StartOffset: int64(binary.LittleEndian.Uint64(data[offset:])),
			EndOffset:   int64(binary.LittleEndian.Uint64(data[offset+8:])),
			StartEntry:  int64(binary.LittleEndian.Uint64(data[offset+16:])),
			Entries:     int64(binary.LittleEndian.Uint64(data[offset+24:])),
			StartTime:   time.Unix(0, int64(binary.LittleEndian.Uint64(data[offset+32:]))),
			EndTime:     time.Unix(0, int64(binary.LittleEndian.Uint64(data[offset+40:]))),
		}
		offset += 48

		index.Files = append(index.Files, file)
	}

	// Set current file from last file if available
	if len(index.Files) > 0 {
		index.CurrentFile = index.Files[len(index.Files)-1].Path
	} else {
		index.CurrentFile = s.index.CurrentFile
	}
	index.BoundaryInterval = s.index.BoundaryInterval

	return index, nil
}
