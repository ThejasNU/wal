package wal

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ThejasNU/wal/utils"
)

type WAL struct {
	// directory name in which the wal segments are stored
	directory string

	// pointer to current segment file
	currentSegment *os.File

	// mutex lock to avoid race conditions while writing to wal
	lock sync.Mutex

	// last sequence number of the records in wal
	lastSequenceNumber uint64

	// buffer writer
	bufferWriter *bufio.Writer

	// timer to manage disk flushes
	flushTimer *time.Timer

	// maximum size of each wal segment, once this size is exceeded, new segment is created
	maxSegmentSize uint64

	// maximum number of wal segment, once it is reached, older ones are removed
	maxSegmentsNumber uint

	// current segment number which is being used
	currentSegmentIndex uint

	// controls whether to use fsync api to flush to disk
	shouldFsync bool

	// to control and manage goroutines
	ctx    context.Context
	cancel context.CancelFunc
}

// initialises a new WAL
func GetWAL(directory string, enableFsync bool, maxFileSize uint64, maxSegments uint) (*WAL, error) {
	// create directory if it does not exist
	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, err
	}

	// get list of all wal segment files present
	files, err := filepath.Glob(filepath.Join(directory, fmt.Sprintf("%s*", utils.SegmentPrefix)))
	if err != nil {
		return nil, err
	}

	var lastSegmentId uint = 0
	if len(files) > 0 {
		lastSegmentId, err = utils.FindLastSegmentId(files)
		if err != nil {
			return nil, err
		}
	}

	// open last segment file and seek to it's end
	lastSegmentFilePath := filepath.Join(directory, fmt.Sprintf("%s%d", utils.SegmentPrefix, lastSegmentId))
	lastSegmentFile, err := os.OpenFile(lastSegmentFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	if _, err = lastSegmentFile.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	wal := &WAL{
		directory:           directory,
		currentSegment:      lastSegmentFile,
		lastSequenceNumber:  0,
		bufferWriter:        bufio.NewWriter(lastSegmentFile),
		flushTimer:          time.NewTimer(utils.SyncInterval),
		shouldFsync:         enableFsync,
		maxSegmentSize:      maxFileSize,
		maxSegmentsNumber:   maxSegments,
		currentSegmentIndex: lastSegmentId,
		ctx:                 ctx,
		cancel:              cancel,
	}

	if wal.lastSequenceNumber, err = wal.getLastSequenceNumber(); err != nil {
		return nil, err
	}

	go wal.keepSyncing()

	return wal, nil
}

// keeps checking till timer runs out and flushes data to file
// in case of parent function exiting, this function is also terminated
func (wal *WAL) keepSyncing() {
	for {
		select {
		case <-wal.flushTimer.C:
			wal.lock.Lock()
			err := wal.Flush()
			wal.lock.Unlock()

			if err != nil {
				log.Printf("Error while flushing WAL: %v", err)
			}

		case <-wal.ctx.Done():
			return
		}
	}
}

// resets the synchronization timer
func (wal *WAL) resetTimer() {
	wal.flushTimer.Reset(utils.SyncInterval)
}

// flushes buffers to files and closes current wal instance
func (wal *WAL) Close() error {
	wal.cancel()

	if err := wal.Flush(); err != nil {
		return err
	}

	return wal.currentSegment.Close()
}
