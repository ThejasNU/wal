package wal

import (
	"bufio"
	"context"
	"os"
	"sync"
	"time"
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
