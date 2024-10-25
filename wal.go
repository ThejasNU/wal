package wal

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ThejasNU/wal/types"
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

	if wal.lastSequenceNumber,err=wal.getLastSequenceNumber();err!=nil{
		return nil, err
	}

	go wal.keepSyncing()

	return wal, nil
}

// goes through the current log file and returns the LSN
func (wal *WAL) getLastSequenceNumber()(uint64, error){
	lastEntry, err:= wal.getLastEntryInCurrentSegment()
	if err!=nil{
		return 0, err
	}

	if lastEntry!=nil{
		return lastEntry.GetLogSequenceNumber(), nil
	}

	return 0,err
}

func (wal *WAL) getLastEntryInCurrentSegment() (*types.WAL_Entry, error){
	file, err := os.OpenFile(wal.currentSegment.Name(), os.O_RDONLY, 0644)
	if err!= nil{
		return nil, err
	}
	defer file.Close()

	var prevSize int32
	var offset int64
	var entry *types.WAL_Entry

	for{
		var size int32
		if err:=binary.Read(file, binary.LittleEndian,size); err!=nil{
			if err==io.EOF{
				if offset == 0 {
					return entry, nil
				}

				if _,err := file.Seek(offset, io.SeekStart); err !=nil{
					return nil, err
				}

				data := make([]byte, prevSize)
				if _, err = io.ReadFull(file,data); err!=nil{
					return nil, err
				}

				entry, err = utils.UnmarshalAndVerifyEntry(data)
				if err!= nil{
					return nil, err
				}

				return entry, nil
			}

			return nil, err
		}

		offset, err = file.Seek(0, io.SeekCurrent)
		prevSize = size
		
		if err!=nil{
			return nil, err
		}

		if _, err = file.Seek(int64(size), io.SeekCurrent); err!=nil{
			return nil, err
		}
	}
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

// resets the synchronization timer
func (wal *WAL) resetTimer() {
	wal.flushTimer.Reset(utils.SyncInterval)
}
