package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/ThejasNU/wal/types"
	"github.com/ThejasNU/wal/utils"
)

// reads all entries from the wal
// if checkpoint is true, returns all entries after that checkpoint
// if no checkpoint is found, empty slice is returned
func (wal *WAL) ReadAllFromCurrent(fromCheckpoint bool) ([]*types.WAL_Entry, error) {
	file, err := os.OpenFile(wal.currentSegment.Name(), os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	entries, checkPointLSN, err := readAllEntriesFromFile(file, fromCheckpoint)
	if err != nil {
		return entries, err
	}

	if fromCheckpoint && checkPointLSN <= 0 {
		return make([]*types.WAL_Entry, 0), nil
	}

	return entries, nil
}

// reads segment files starting from the given segment index
// if checkpoint is true, returns all entries after that checkpoint
// if no checkpoint is found, empty slice is returned
func (wal *WAL) ReadAllFromIndex(index int, fromCheckpoint bool) ([]*types.WAL_Entry, error) {
	files, err := filepath.Glob(filepath.Join(wal.directory, fmt.Sprintf("%s*", utils.SegmentPrefix)))
	if err != nil {
		return nil, err
	}

	var entries []*types.WAL_Entry
	var prevCheckPointLSN uint64 = 0

	for _, fileName := range files {
		segmentIdx, err := strconv.Atoi(strings.TrimPrefix(fileName, filepath.Join(wal.directory, utils.SegmentPrefix)))
		if err != nil {
			return nil, err
		}

		if segmentIdx < index {
			continue
		}

		file, err := os.OpenFile(fileName, os.O_RDONLY, 0644)
		if err != nil {
			return nil, err
		}

		entriesFromCurrentSegment, checkPointLSN, err := readAllEntriesFromFile(file, fromCheckpoint)
		if err != nil {
			return entries, err
		}

		if fromCheckpoint && checkPointLSN > prevCheckPointLSN {
			prevCheckPointLSN = checkPointLSN
			entries = entries[:0]
		}

		entries = append(entries, entriesFromCurrentSegment...)
	}

	return entries, nil
}

func readAllEntriesFromFile(file *os.File, fromCheckPoint bool) ([]*types.WAL_Entry, uint64, error) {
	var entries []*types.WAL_Entry
	var checkPointLSN uint64 = 0

	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return entries, checkPointLSN, err
	}

	for {
		var size int32
		if err := binary.Read(file, binary.LittleEndian, &size); err != nil {
			if err == io.EOF {
				break
			}
			return entries, checkPointLSN, err
		}

		data := make([]byte, size)
		if _, err := io.ReadFull(file, data); err != nil {
			return entries, checkPointLSN, err
		}

		entry, err := utils.UnmarshalAndVerifyEntry(data)
		if err != nil {
			return entries, checkPointLSN, err
		}

		if entry.IsCheckpoint != nil && entry.GetIsCheckpoint() && fromCheckPoint {
			checkPointLSN = entry.GetLogSequenceNumber()
			entries = entries[:0]
		}

		entries = append(entries, entry)
	}

	return entries, checkPointLSN, nil
}

// goes through the current log file and returns the LSN
func (wal *WAL) getLastSequenceNumber() (uint64, error) {
	lastEntry, err := wal.getLastEntryInCurrentSegment()
	if err != nil {
		return 0, err
	}

	if lastEntry != nil {
		return lastEntry.GetLogSequenceNumber(), nil
	}

	return 0, err
}

func (wal *WAL) getLastEntryInCurrentSegment() (*types.WAL_Entry, error) {
	file, err := os.OpenFile(wal.currentSegment.Name(), os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var prevSize int32
	var offset int64
	var entry *types.WAL_Entry

	for {
		var size int32
		if err := binary.Read(file, binary.LittleEndian, size); err != nil {
			if err == io.EOF {
				if offset == 0 {
					return entry, nil
				}

				if _, err := file.Seek(offset, io.SeekStart); err != nil {
					return nil, err
				}

				data := make([]byte, prevSize)
				if _, err = io.ReadFull(file, data); err != nil {
					return nil, err
				}

				entry, err = utils.UnmarshalAndVerifyEntry(data)
				if err != nil {
					return nil, err
				}

				return entry, nil
			}

			return nil, err
		}

		offset, err = file.Seek(0, io.SeekCurrent)
		prevSize = size

		if err != nil {
			return nil, err
		}

		if _, err = file.Seek(int64(size), io.SeekCurrent); err != nil {
			return nil, err
		}
	}
}
