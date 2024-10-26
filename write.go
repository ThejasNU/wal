package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/ThejasNU/wal/types"
	"github.com/ThejasNU/wal/utils"
)

// writes in-memory buffer to the segment file in file system
// if fsync is opted, syncs file to the disk
func (wal *WAL) Flush() error {
	if err := wal.bufferWriter.Flush(); err != nil {
		return err
	}

	if wal.shouldFsync {
		if err := wal.currentSegment.Sync(); err != nil {
			return err
		}
	}

	// reset the timer, since we just synced
	wal.resetTimer()

	return nil
}

// writes entry to wal without creating checkpoint
func (wal *WAL) WriteEntry(data []byte) error {
	return wal.writeEntry(data, false)
}

// creates checkpoint entry in the wal
// useful to restore the state of system to this point
func (wal *WAL) CreateCheckpoint(data []byte) error {
	return wal.writeEntry(data, true)
}

func (wal *WAL) writeEntry(data []byte, isCheckpoint bool) error {
	wal.lock.Lock()
	defer wal.lock.Unlock()

	if err := wal.changeLogSegmentIfNeeded(); err != nil {
		return err
	}

	wal.lastSequenceNumber += 1

	newEntry := &types.WAL_Entry{
		LogSequenceNumber: wal.lastSequenceNumber,
		Data:              data,
		CRC:               crc32.ChecksumIEEE(append(data, byte(wal.lastSequenceNumber))),
	}

	if isCheckpoint {
		if err := wal.Flush(); err != nil {
			return fmt.Errorf("error while flushing, could not create checkpoint: %v", err)
		}

		newEntry.IsCheckpoint = &isCheckpoint
	}

	return wal.writeEntryToBuffer(newEntry)
}

func (wal *WAL) changeLogSegmentIfNeeded() error {
	fileInfo, err := wal.currentSegment.Stat()
	if err != nil {
		return err
	}

	if fileInfo.Size()+int64(wal.bufferWriter.Buffered()) >= int64(wal.maxSegmentSize) {
		if err := wal.changeLogSegment(); err != nil {
			return err
		}
	}

	return nil
}

func (wal *WAL) changeLogSegment() error {
	if err := wal.Flush(); err != nil {
		return err
	}

	if err := wal.currentSegment.Close(); err != nil {
		return err
	}

	wal.currentSegmentIndex += 1

	if wal.currentSegmentIndex > wal.maxSegmentsNumber {
		if err := wal.deleteOldestSegment(); err != nil {
			return err
		}
	}

	newFile, err := utils.CreateSegmentFile(wal.directory, int(wal.currentSegmentIndex))
	if err != nil {
		return err
	}

	wal.currentSegment = newFile
	wal.bufferWriter = bufio.NewWriter(newFile)

	return nil
}

func (wal *WAL) deleteOldestSegment() error {
	files, err := filepath.Glob(filepath.Join(wal.directory, fmt.Sprintf("%s*", utils.SegmentPrefix)))
	if err != nil {
		return err
	}

	var oldestSegmentFilePath string
	if len(files) > 0 {
		oldestSegmentFilePath, err = wal.findOldestSegmentFile(files)
		if err != nil {
			return err
		}
	} else {
		return nil
	}

	if err := os.Remove(oldestSegmentFilePath); err != nil {
		return err
	}

	return nil
}

func (wal *WAL) findOldestSegmentFile(files []string) (string, error) {
	var oldestSegmentFilePath string
	oldestSegmentId := math.MaxInt64

	for _, file := range files {
		segmentIdx, err := strconv.Atoi(strings.TrimPrefix(file, filepath.Join(wal.directory, utils.SegmentPrefix)))
		if err != nil {
			return "", err
		}

		if segmentIdx < oldestSegmentId {
			oldestSegmentId = segmentIdx
			oldestSegmentFilePath = file
		}
	}

	return oldestSegmentFilePath, nil
}

// writes size of data of buffer and later the serialized entry
func (wal *WAL) writeEntryToBuffer(entry *types.WAL_Entry) error {
	serializedEntry := utils.MustMarshal(entry)

	size := int32(len(serializedEntry))
	if err := binary.Write(wal.bufferWriter, binary.LittleEndian, size); err != nil {
		return err
	}

	_, err := wal.bufferWriter.Write(serializedEntry)

	return err
}
