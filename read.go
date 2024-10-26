package wal

import (
	"encoding/binary"
	"io"
	"os"

	"github.com/ThejasNU/wal/types"
	"github.com/ThejasNU/wal/utils"
)

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
