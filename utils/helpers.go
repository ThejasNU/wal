package utils

import (
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ThejasNU/wal/types"
)

const (
	SyncInterval  = 200 * time.Millisecond
	SegmentPrefix = "segment-"
)

// finds last segment id in the list of files
func FindLastSegmentId(files []string) (uint, error) {
	var lastSegmentId uint = 0

	for _, file := range files {
		_, fileName := filepath.Split(file)
		curSegmentId, err := strconv.ParseUint(strings.TrimPrefix(fileName, SegmentPrefix), 10, 0)
		if err != nil {
			return uint(curSegmentId), err
		}

		if uint(curSegmentId) > lastSegmentId {
			lastSegmentId = uint(curSegmentId)
		}
	}

	return lastSegmentId, nil
}

// creates new segment file with given id in provided directory and returns it
func CreateSegmentFile(directory string, segmentId int) (*os.File, error) {
	filePath := filepath.Join(directory, fmt.Sprintf("%s%d", SegmentPrefix, segmentId))

	file, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}

	return file, nil
}

// unmarshals the given data into wal entry
// also verifies CRC32, returns only if it is valid
func UnmarshalAndVerifyEntry(data []byte) (*types.WAL_Entry, error) {
	var entry types.WAL_Entry
	MustUnmarshal(data, &entry)

	if !isValidCRC(&entry) {
		return nil, fmt.Errorf("CRC mismatch: Data in the entry maybe be corrupted")
	}

	return &entry, nil
}

func isValidCRC(entry *types.WAL_Entry) bool {
	calculateNewCRC := crc32.ChecksumIEEE(append(entry.GetData(), byte(entry.GetLogSequenceNumber())))

	return calculateNewCRC == entry.GetCRC()
}
